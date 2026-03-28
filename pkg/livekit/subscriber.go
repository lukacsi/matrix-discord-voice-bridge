package livekit

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lksdk "github.com/livekit/server-sdk-go/v2"
	lkmedia "github.com/livekit/server-sdk-go/v2/pkg/media"
	"github.com/pion/webrtc/v4"
	"gopkg.in/hraban/opus.v2"

	msdk "github.com/livekit/media-sdk"
	"github.com/livekit/media-sdk/mixer"
)

const (
	sampleRate      = 48000
	mixChannels     = 1 // mixer only supports mono
	encodeChannels  = 2 // Discord requires stereo
	opusFrameMs     = 20
	samplesPerFrame = sampleRate * opusFrameMs / 1000 // 960
)

// softLimit applies a soft knee compressor to prevent hard clipping.
// Below the threshold (25% of max), signal passes through unchanged.
// Above threshold, signal is compressed with a 10:1 ratio.
func softLimit(s int16) int16 {
	const threshold int32 = 8192 // 25% of 32767
	const ratio int32 = 10

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

// OpusFrameHandler is called with each encoded Opus frame from the mixer.
// Called from the mixer's goroutine — must be safe for concurrent use.
type OpusFrameHandler func(opusFrame []byte) error

// trackEntry tracks a subscribed remote track and its mixer input.
type trackEntry struct {
	pcmTrack *lkmedia.PCMRemoteTrack
	input    *mixer.Input
}

// opusOutput receives mono PCM from the mixer goroutine, converts to stereo,
// encodes Opus, and delivers via handler. All fields are only accessed from
// the mixer's single ticker goroutine — no mutex needed.
type opusOutput struct {
	encoder *opus.Encoder
	mono    []int16
	stereo  []int16
	outBuf  []byte
	pos     int
	handler OpusFrameHandler
	logger  *slog.Logger
}

func newOpusOutput(handler OpusFrameHandler, logger *slog.Logger) (*opusOutput, error) {
	enc, err := opus.NewEncoder(sampleRate, encodeChannels, opus.AppAudio)
	if err != nil {
		return nil, fmt.Errorf("create opus encoder: %w", err)
	}
	if err := enc.SetBitrate(96000); err != nil {
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
	for i := 0; i < len(sample); {
		n := copy(o.mono[o.pos:], sample[i:])
		o.pos += n
		i += n

		if o.pos >= samplesPerFrame {
			// Mono → stereo with soft limiter
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

// Subscriber subscribes to all non-bridge tracks in a LiveKit room,
// mixes them, encodes to stereo Opus, and delivers frames via a handler.
type Subscriber struct {
	config Config
	logger *slog.Logger
	room   *lksdk.Room
	mixer  *mixer.Mixer
	mu     sync.Mutex
	tracks map[string]*trackEntry
	errors atomic.Int64
}

// NewSubscriber creates a subscriber that mixes LiveKit audio into Opus frames.
func NewSubscriber(config Config, handler OpusFrameHandler, slogger *slog.Logger) (*Subscriber, error) {
	out, err := newOpusOutput(handler, slogger)
	if err != nil {
		return nil, err
	}

	mx, err := mixer.NewMixer(out, opusFrameMs*time.Millisecond, nil, mixChannels, 3)
	if err != nil {
		return nil, fmt.Errorf("create mixer: %w", err)
	}

	s := &Subscriber{
		config: config,
		logger: slogger,
		mixer:  mx,
		tracks: make(map[string]*trackEntry),
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
	},
		lksdk.WithAutoSubscribe(true),
	)
	if err != nil {
		mx.Stop()
		return nil, fmt.Errorf("connect subscriber: %w", err)
	}

	s.room = room
	slogger.Info("subscriber connected to LiveKit room", slog.String("room", config.RoomName))
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

	input := s.mixer.NewInput()

	pcmTrack, err := lkmedia.NewPCMRemoteTrack(track, input)
	if err != nil {
		s.logger.Error("failed to create PCM remote track",
			slog.String("track", sid),
			slog.Any("err", err),
		)
		s.mixer.RemoveInput(input)
		return
	}

	s.mu.Lock()
	s.tracks[sid] = &trackEntry{pcmTrack: pcmTrack, input: input}
	s.mu.Unlock()
}

func (s *Subscriber) onTrackUnsubscribed(track *webrtc.TrackRemote, _ *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) {
	sid := track.ID()

	s.mu.Lock()
	entry, ok := s.tracks[sid]
	if ok {
		delete(s.tracks, sid)
	}
	s.mu.Unlock()

	if ok {
		entry.pcmTrack.Close()
		s.mixer.RemoveInput(entry.input)
		s.logger.Info("unsubscribed from track",
			slog.String("participant", rp.Identity()),
			slog.String("track", sid),
		)
	}
}

// Close disconnects the subscriber and stops the mixer.
func (s *Subscriber) Close() {
	s.mu.Lock()
	for sid, entry := range s.tracks {
		entry.pcmTrack.Close()
		s.mixer.RemoveInput(entry.input)
		delete(s.tracks, sid)
	}
	s.mu.Unlock()

	s.mixer.Stop()

	if s.room != nil {
		s.room.Disconnect()
	}
	s.logger.Info("subscriber disconnected")
}
