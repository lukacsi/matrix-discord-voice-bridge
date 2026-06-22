package livekit

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/pion/webrtc/v4"
	"gopkg.in/hraban/opus.v2"

	msdk "github.com/livekit/media-sdk"
	"github.com/livekit/media-sdk/mixer"
)

const (
	sampleRate      = 48000
	mixChannels     = 1 // mixer only supports mono
	encodeChannels  = 2 // Discord requires stereo Opus
	opusFrameMs     = 20
	samplesPerFrame = sampleRate * opusFrameMs / 1000 // 960

	// Per-input gain applied before mixer to prevent clipping in the mixer's
	// int32→int16 hard-clip. 0.65 means two speakers at full volume sum to
	// 1.3x — soft limited only on simultaneous peaks. Speech rarely peaks
	// simultaneously, so this sounds natural.
	inputGain = 0.65
)

// softLimit is a soft-knee compressor that prevents the media-sdk mixer's
// brick-wall int16 clip from producing audible square-wave distortion.
// Below threshold: passthrough. Above: 10:1 compression.
func softLimit(s int16) int16 {
	const threshold int32 = 8192 // 25% of int16 max — speech stays below this
	const ratio int32 = 10       // 10:1 — gentle enough to sound natural on transients

	x := int32(s)
	abs := x
	if abs < 0 {
		abs = -abs
	}

	if abs <= threshold {
		return s
	}

	excess := abs - threshold
	compressed := threshold + excess/ratio

	if compressed > 32767 {
		compressed = 32767
	}

	if x < 0 {
		return int16(-compressed)
	}
	return int16(compressed)
}

const (
	modePassthrough int32 = 0
	modeMixer       int32 = 1
)

// OpusFrameHandler is called with each encoded Opus frame.
// Must be safe for concurrent use.
type OpusFrameHandler func(opusFrame []byte) error

// trackEntry tracks a subscribed remote track with its own read goroutine.
// The goroutine reads RTP and either forwards raw Opus (passthrough) or
// decodes and feeds the mixer, based on the atomic mode flag.
type trackEntry struct {
	track  *webrtc.TrackRemote
	cancel chan struct{}
	mode   atomic.Int32
	input  *mixer.Input // non-nil when in mixer mode
}

// opusOutput receives mono PCM from the mixer goroutine, converts to stereo,
// encodes Opus, and delivers via handler. All fields (except muted) are only
// accessed from the mixer's single ticker goroutine.
type opusOutput struct {
	encoder *opus.Encoder
	mono    []int16
	stereo  []int16
	outBuf  []byte
	pos     int
	handler OpusFrameHandler
	logger  *slog.Logger
	muted   atomic.Bool // true when in passthrough mode (suppresses mixer output)
}

func newOpusOutput(handler OpusFrameHandler, logger *slog.Logger) (*opusOutput, error) {
	enc, err := opus.NewEncoder(sampleRate, encodeChannels, opus.AppAudio)
	if err != nil {
		return nil, fmt.Errorf("create opus encoder: %w", err)
	}
	if err := enc.SetBitrate(128000); err != nil {
		return nil, fmt.Errorf("set bitrate: %w", err)
	}
	if err := enc.SetComplexity(10); err != nil {
		return nil, fmt.Errorf("set complexity: %w", err)
	}

	return &opusOutput{
		encoder: enc,
		mono:    make([]int16, samplesPerFrame),
		stereo:  make([]int16, samplesPerFrame*encodeChannels),
		outBuf:  make([]byte, 4000),
		handler: handler,
		logger:  logger,
	}, nil
}

func (o *opusOutput) String() string  { return "discord-opus-out" }
func (o *opusOutput) SampleRate() int { return sampleRate }
func (o *opusOutput) Close() error    { return nil }

func (o *opusOutput) WriteSample(sample msdk.PCM16Sample) error {
	if o.muted.Load() {
		return nil
	}

	for i := 0; i < len(sample); {
		n := copy(o.mono[o.pos:], sample[i:])
		o.pos += n
		i += n

		if o.pos >= samplesPerFrame {
			for j := 0; j < samplesPerFrame; j++ {
				s := softLimit(o.mono[j])
				o.stereo[j*2] = s
				o.stereo[j*2+1] = s
			}

			written, err := o.encoder.Encode(o.stereo, o.outBuf)
			if err != nil {
				o.logger.Warn("opus encode error", slog.Any("err", err))
				o.pos = 0
				continue
			}

			frame := make([]byte, written)
			copy(frame, o.outBuf[:written])
			if err := o.handler(frame); err != nil {
				return err
			}
			o.pos = 0
		}
	}
	return nil
}

// Subscriber subscribes to all non-bridge tracks in a LiveKit room
// and delivers Opus frames to Discord. Uses passthrough when a single
// speaker is active (zero quality loss), falls back to mixer for 2+.
type Subscriber struct {
	config  Config
	logger  *slog.Logger
	handler OpusFrameHandler
	room    *lksdk.Room
	mixer   *mixer.Mixer
	opusOut *opusOutput
	mu      sync.Mutex
	tracks  map[string]*trackEntry
}

// NewSubscriber creates a subscriber that delivers Opus frames from LiveKit to Discord.
func NewSubscriber(config Config, handler OpusFrameHandler, slogger *slog.Logger) (*Subscriber, error) {
	out, err := newOpusOutput(handler, slogger)
	if err != nil {
		return nil, err
	}
	// Start muted — unmuted when switching to mixer mode
	out.muted.Store(true)

	mx, err := mixer.NewMixer(out, opusFrameMs*time.Millisecond, nil, mixChannels, 1)
	if err != nil {
		return nil, fmt.Errorf("create mixer: %w", err)
	}

	s := &Subscriber{
		config:  config,
		logger:  slogger,
		handler: handler,
		mixer:   mx,
		opusOut: out,
		tracks:  make(map[string]*trackEntry),
	}

	room, err := lksdk.ConnectToRoom(config.URL, lksdk.ConnectInfo{
		APIKey:              config.APIKey,
		APISecret:           config.APISecret,
		RoomName:            config.RoomName,
		ParticipantIdentity: "discord-bridge-listener",
		ParticipantName:     "Discord Bridge",
	}, &lksdk.RoomCallback{
		ParticipantCallback: lksdk.ParticipantCallback{
			OnTrackSubscribed:   s.onTrackSubscribed,
			OnTrackUnsubscribed: s.onTrackUnsubscribed,
		},
		OnDisconnected: func() {
			slogger.Warn("subscriber disconnected from LiveKit")
		},
	},
		lksdk.WithAutoSubscribe(true),
	)
	if err != nil {
		mx.Stop()
		return nil, fmt.Errorf("connect subscriber: %w", err)
	}

	s.room = room
	slogger.Info("subscriber connected to LiveKit room", slog.String("livekit_room", config.RoomName))
	return s, nil
}

func (s *Subscriber) onTrackSubscribed(track *webrtc.TrackRemote, _ *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) {
	ident := rp.Identity()
	if strings.HasPrefix(ident, "discord:") || strings.HasPrefix(ident, "@discord_") || ident == "discord-bridge-listener" {
		return
	}
	if track.Kind() != webrtc.RTPCodecTypeAudio {
		return
	}

	sid := track.ID()
	s.logger.Info("subscribing to track",
		slog.String("participant", rp.Identity()),
		slog.String("track", sid),
	)

	entry := &trackEntry{
		track:  track,
		cancel: make(chan struct{}),
	}

	s.mu.Lock()
	s.tracks[sid] = entry
	trackCount := len(s.tracks)

	if trackCount == 1 {
		// Single speaker — passthrough mode
		entry.mode.Store(modePassthrough)
		s.opusOut.muted.Store(true)
		s.mu.Unlock()
		s.logger.Debug("single speaker — using Opus passthrough", slog.String("track", sid))
	} else {
		// Multiple speakers — switch everything to mixer
		s.logger.Debug("multiple speakers — switching to mixer", slog.Int("count", trackCount))
		s.activateMixerLocked()
		s.mu.Unlock()
	}

	go s.trackLoop(entry, sid)
}

func (s *Subscriber) onTrackUnsubscribed(track *webrtc.TrackRemote, _ *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) {
	sid := track.ID()

	s.mu.Lock()
	entry, ok := s.tracks[sid]
	if ok {
		delete(s.tracks, sid)
	}
	trackCount := len(s.tracks)

	if ok {
		close(entry.cancel)
		if entry.input != nil {
			s.mixer.RemoveInput(entry.input)
			entry.input = nil
		}
	}

	// If we dropped to 1 track, switch remaining to passthrough
	if trackCount == 1 {
		s.activatePassthroughLocked()
	} else if trackCount == 0 {
		s.opusOut.muted.Store(true)
	}
	s.mu.Unlock()

	if ok {
		s.logger.Info("unsubscribed from track",
			slog.String("participant", rp.Identity()),
			slog.String("track", sid),
		)
	}
}

// trackLoop is the single goroutine per track. It reads RTP packets and
// either forwards raw Opus (passthrough) or decodes+feeds the mixer.
func (s *Subscriber) trackLoop(entry *trackEntry, sid string) {
	dec, err := opus.NewDecoder(sampleRate, encodeChannels)
	if err != nil {
		s.logger.Error("failed to create opus decoder", slog.String("track", sid), slog.Any("err", err))
		return
	}

	stereoBuf := make([]int16, samplesPerFrame*encodeChannels) // 1920 int16
	monoBuf := make([]int16, samplesPerFrame)                  // 960 mono samples
	var frameCount uint64

	for {
		select {
		case <-entry.cancel:
			return
		default:
		}

		pkt, _, err := entry.track.ReadRTP()
		if err != nil {
			s.logger.Debug("track read ended", slog.String("track", sid), slog.Any("err", err))
			return
		}
		if len(pkt.Payload) == 0 {
			continue
		}

		frameCount++
		if frameCount%500 == 1 {
			stereo := "mono"
			if len(pkt.Payload) > 0 && pkt.Payload[0]&0x04 != 0 {
				stereo = "stereo"
			}
			s.logger.Debug("track frame",
				slog.String("track", sid),
				slog.Uint64("frame", frameCount),
				slog.Int("bytes", len(pkt.Payload)),
				slog.String("channels", stereo),
				slog.String("mode", map[int32]string{0: "passthrough", 1: "mixer"}[entry.mode.Load()]),
			)
		}

		if entry.mode.Load() == modePassthrough {
			if err := s.handler(pkt.Payload); err != nil {
				s.logger.Debug("passthrough handler error", slog.String("track", sid), slog.Any("err", err))
				return
			}
			continue
		}

		// Mixer mode: decode Opus → stereo PCM, downmix to mono with gain, write to mixer
		if entry.input == nil {
			continue
		}

		n, err := dec.Decode(pkt.Payload, stereoBuf)
		if err != nil {
			s.logger.Debug("opus decode error", slog.String("track", sid), slog.Any("err", err))
			continue
		}

		// Downmix stereo→mono with per-input gain to prevent mixer clipping
		for i := 0; i < n; i++ {
			mono := (int32(stereoBuf[i*2]) + int32(stereoBuf[i*2+1])) / 2
			monoBuf[i] = int16(float64(mono) * inputGain)
		}

		if err := entry.input.WriteSample(monoBuf[:n]); err != nil {
			s.logger.Debug("mixer input write error", slog.String("track", sid), slog.Any("err", err))
		}
	}
}

// activateMixerLocked switches all tracks to mixer mode. Caller holds s.mu.
func (s *Subscriber) activateMixerLocked() {
	for sid, entry := range s.tracks {
		if entry.input == nil {
			entry.input = s.mixer.NewInput()
			s.logger.Debug("created mixer input", slog.String("track", sid))
		}
		entry.mode.Store(modeMixer)
	}
	s.opusOut.muted.Store(false)
}

// activatePassthroughLocked switches the single remaining track to passthrough. Caller holds s.mu.
func (s *Subscriber) activatePassthroughLocked() {
	for sid, entry := range s.tracks {
		entry.mode.Store(modePassthrough)
		if entry.input != nil {
			s.mixer.RemoveInput(entry.input)
			entry.input = nil
		}
		s.logger.Debug("switched to Opus passthrough", slog.String("track", sid))
	}
	s.opusOut.muted.Store(true)
}

// Close disconnects the subscriber and stops the mixer.
func (s *Subscriber) Close() {
	s.mu.Lock()
	for sid, entry := range s.tracks {
		close(entry.cancel)
		if entry.input != nil {
			s.mixer.RemoveInput(entry.input)
		}
		delete(s.tracks, sid)
	}
	s.mu.Unlock()

	s.mixer.Stop()

	if s.room != nil {
		s.room.Disconnect()
	}
	s.logger.Info("subscriber disconnected")
}
