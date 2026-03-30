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

const frameDuration = 20 * time.Millisecond

// Config holds LiveKit connection parameters.
type Config struct {
	URL       string
	APIKey    string
	APISecret string
	RoomName  string
}

// participant represents a single Discord user bridged into LiveKit.
type participant struct {
	room  *lksdk.Room
	track *lksdk.LocalTrack
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

	var roomRef *lksdk.Room
	room, err := lksdk.ConnectToRoom(m.config.URL, lksdk.ConnectInfo{
		APIKey:              m.config.APIKey,
		APISecret:           m.config.APISecret,
		RoomName:            m.config.RoomName,
		ParticipantIdentity: identity,
		ParticipantName:     identity,
	}, &lksdk.RoomCallback{
		OnDisconnected: func() {
			m.mu.Lock()
			if p, ok := m.participants[userID]; ok && p.room == roomRef {
				delete(m.participants, userID)
			}
			m.mu.Unlock()
			m.logger.Warn("LiveKit participant disconnected unexpectedly",
				slog.Uint64("discord_user", userID))
		},
	},
		lksdk.WithRetransmitBufferSize(0),
	)
	roomRef = room
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

	p := &participant{room: room, track: track}

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

// WriteOpus writes a raw Opus frame directly to the user's LiveKit track.
// Creates the participant in the background if it doesn't exist — frames are
// dropped during connect to avoid blocking the IPC read loop.
func (m *Manager) WriteOpus(userID uint64, opusFrame []byte) error {
	m.mu.Lock()
	p, ok := m.participants[userID]
	isConnecting := m.connecting[userID]
	m.mu.Unlock()

	if !ok {
		if isConnecting {
			return nil // drop frames during connect
		}
		go func() {
			if err := m.ensureParticipant(userID); err != nil {
				m.logger.Warn("failed to create LiveKit participant",
					slog.Uint64("discord_user", userID), slog.Any("err", err))
			}
		}()
		return nil // drop first frame
	}

	return p.track.WriteSample(media.Sample{
		Data:     opusFrame,
		Duration: frameDuration,
	}, nil)
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
		p.room.Disconnect()
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
		p.room.Disconnect()
		m.logger.Info("LiveKit participant disconnected", slog.Uint64("discord_user", userID))
	}
}
