package livekit

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

// Config holds LiveKit connection parameters.
type Config struct {
	URL       string
	APIKey    string
	APISecret string
	RoomName  string
}

const (
	frameDuration = 20 * time.Millisecond
	frameBuffer   = 10 // ~200ms of buffered frames
)

// participant represents a single Discord user bridged into LiveKit.
type participant struct {
	room   *lksdk.Room
	track  *lksdk.LocalTrack
	frames chan []byte
	done   chan struct{}
	wg     sync.WaitGroup
}

// paceWriter drains frames at a steady 20ms interval to prevent jitter.
func (p *participant) paceWriter(logger *slog.Logger, userID uint64) {
	defer p.wg.Done()
	ticker := time.NewTicker(frameDuration)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
			select {
			case frame := <-p.frames:
				err := p.track.WriteSample(media.Sample{
					Data:     frame,
					Duration: frameDuration,
				}, nil)
				if err != nil {
					logger.Warn("WriteSample error",
						slog.Uint64("user", userID),
						slog.Any("err", err),
					)
				}
			default:
				// no frame ready — silence gap, skip tick
			}
		}
	}
}

func (p *participant) close() {
	close(p.done)
	p.wg.Wait() // wait for paceWriter to stop before disconnecting
	p.room.Disconnect()
}

// Manager manages LiveKit participants for bridged Discord users.
type Manager struct {
	config       Config
	logger       *slog.Logger
	identityFunc IdentityFunc
	mu           sync.Mutex
	participants map[uint64]*participant
	connecting   map[uint64]bool
}

// NewManager creates a new LiveKit participant manager.
func NewManager(config Config, logger *slog.Logger) *Manager {
	return &Manager{
		config:       config,
		logger:       logger,
		participants: make(map[uint64]*participant),
		connecting:   make(map[uint64]bool),
	}
}

// IdentityFunc maps a Discord user ID to a LiveKit participant identity.
type IdentityFunc func(discordUserID uint64) string

// SetIdentityFunc sets the function used to compute LiveKit participant identities.
// Must be called before any participants are created.
func (m *Manager) SetIdentityFunc(fn IdentityFunc) {
	m.identityFunc = fn
}

func (m *Manager) identity(userID uint64) string {
	if m.identityFunc != nil {
		return m.identityFunc(userID)
	}
	return fmt.Sprintf("discord:%d", userID)
}

// ensureParticipant creates a LiveKit participant if one doesn't exist.
func (m *Manager) ensureParticipant(userID uint64) error {
	m.mu.Lock()
	if _, ok := m.participants[userID]; ok {
		m.mu.Unlock()
		return nil
	}
	if m.connecting[userID] {
		m.mu.Unlock()
		return nil
	}
	m.connecting[userID] = true
	m.mu.Unlock()

	identity := m.identity(userID)

	track, err := lksdk.NewLocalTrack(webrtc.RTPCodecCapability{
		MimeType:  webrtc.MimeTypeOpus,
		ClockRate: 48000,
		Channels:  2,
	})
	if err != nil {
		m.clearConnecting(userID)
		return fmt.Errorf("create track: %w", err)
	}

	room, err := lksdk.ConnectToRoom(m.config.URL, lksdk.ConnectInfo{
		APIKey:              m.config.APIKey,
		APISecret:           m.config.APISecret,
		RoomName:            m.config.RoomName,
		ParticipantIdentity: identity,
		ParticipantName:     identity,
	}, &lksdk.RoomCallback{},
		lksdk.WithRetransmitBufferSize(0),
	)
	if err != nil {
		track.Close()
		m.clearConnecting(userID)
		return fmt.Errorf("connect to room: %w", err)
	}

	_, err = room.LocalParticipant.PublishTrack(track, &lksdk.TrackPublicationOptions{
		Name: "audio",
	})
	if err != nil {
		room.Disconnect()
		track.Close()
		m.clearConnecting(userID)
		return fmt.Errorf("publish track: %w", err)
	}

	p := &participant{
		room:   room,
		track:  track,
		frames: make(chan []byte, frameBuffer),
		done:   make(chan struct{}),
	}
	p.wg.Add(1)
	go p.paceWriter(m.logger, userID)

	m.mu.Lock()
	m.participants[userID] = p
	delete(m.connecting, userID)
	m.mu.Unlock()

	m.logger.Info("LiveKit participant connected",
		slog.String("identity", identity),
		slog.Uint64("discord_user", userID),
	)
	return nil
}

func (m *Manager) clearConnecting(userID uint64) {
	m.mu.Lock()
	delete(m.connecting, userID)
	m.mu.Unlock()
}

// WriteOpus queues a raw Opus frame for paced delivery to LiveKit.
func (m *Manager) WriteOpus(userID uint64, opusFrame []byte) error {
	m.mu.Lock()
	p, ok := m.participants[userID]
	m.mu.Unlock()

	if !ok {
		if err := m.ensureParticipant(userID); err != nil {
			return err
		}
		m.mu.Lock()
		p = m.participants[userID]
		m.mu.Unlock()
		if p == nil {
			return nil
		}
	}

	// Non-blocking send. Called from a single goroutine (bridgeLoop).
	select {
	case p.frames <- opusFrame:
	default:
		// buffer full — drop oldest frame to keep latency low
		select {
		case <-p.frames:
		default:
		}
		select {
		case p.frames <- opusFrame:
		default:
			// still full after drain — drop this frame
		}
	}
	return nil
}

// RemoveParticipant disconnects a user's LiveKit participant.
func (m *Manager) RemoveParticipant(userID uint64) {
	m.mu.Lock()
	p, ok := m.participants[userID]
	if ok {
		delete(m.participants, userID)
	}
	m.mu.Unlock()

	if ok {
		p.close()
		m.logger.Info("LiveKit participant disconnected", slog.Uint64("discord_user", userID))
	}
}

// Close disconnects all participants.
func (m *Manager) Close() {
	m.mu.Lock()
	participants := m.participants
	m.participants = make(map[uint64]*participant)
	m.mu.Unlock()

	for userID, p := range participants {
		p.close()
		m.logger.Info("LiveKit participant disconnected", slog.Uint64("discord_user", userID))
	}
}
