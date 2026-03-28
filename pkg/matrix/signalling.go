package matrix

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"log/slog"
	"sync"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

const (
	callMemberEventType = "org.matrix.msc3401.call.member"
	callApplication     = "m.call"
	callScope           = "m.room"
	defaultSlotID       = "m.call#ROOM"
	membershipExpiryMs  = 14400000 // 4 hours
)

// Config holds Matrix connection parameters.
type Config struct {
	HomeserverURL string // e.g. "https://matrix.lukacsi.org"
	ASToken       string // appservice token from registration
	ServerName    string // e.g. "lukacsi.org"
	LKJWTService  string // e.g. "https://lk-jwt.lukacsi.org"
}

// Signaller manages MatrixRTC m.call.member state events for bridged users.
// Uses mautrix-discord's appservice registration to act as virtual users.
type Signaller struct {
	config Config
	as     *appservice.AppService
	logger *slog.Logger
	mu     sync.Mutex
	active map[uint64]memberInfo // Discord user ID → active membership
}

type memberInfo struct {
	mxid     id.UserID
	deviceID string
	roomID   id.RoomID
	stateKey string
}

// NewSignaller creates a Matrix signaller using the given appservice token.
func NewSignaller(config Config, logger *slog.Logger) (*Signaller, error) {
	as, err := appservice.CreateFull(appservice.CreateOpts{
		HomeserverURL:    config.HomeserverURL,
		HomeserverDomain: config.ServerName,
		Registration: &appservice.Registration{
			ID:          "discord",
			AppToken:    config.ASToken,
			ServerToken: config.ASToken,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create appservice: %w", err)
	}

	return &Signaller{
		config: config,
		as:     as,
		logger: logger,
		active: make(map[uint64]memberInfo),
	}, nil
}

// LiveKitIdentity returns the LiveKit participant identity for a Discord user.
// Must match what Element Call computes from the m.call.member event.
// Legacy format: @user:server:DEVICE_ID
func (s *Signaller) LiveKitIdentity(discordUserID uint64) string {
	mxid := s.discordMXID(discordUserID)
	deviceID := s.deviceID(discordUserID)
	return fmt.Sprintf("%s:%s", mxid, deviceID)
}

// LiveKitRoomAlias computes the LiveKit room name from a Matrix room ID.
// Must match lk-jwt-service: base64.StdEncoding without padding.
func LiveKitRoomAlias(roomID string) string {
	data := roomID + "|" + defaultSlotID
	hash := sha256.Sum256([]byte(data))
	return base64.RawStdEncoding.EncodeToString(hash[:])
}

// JoinCall sends an m.call.member state event for a Discord user in the given room.
func (s *Signaller) JoinCall(ctx context.Context, discordUserID uint64, roomID id.RoomID) error {
	mxid := s.discordMXID(discordUserID)
	deviceID := s.deviceID(discordUserID)
	stateKey := fmt.Sprintf("_%s_%s_%s", mxid, deviceID, callApplication)

	intent := s.as.Intent(mxid)

	// Ensure the virtual user is registered and in the room
	if err := intent.EnsureRegistered(ctx); err != nil {
		s.logger.Warn("user registration (may already exist)", slog.Any("err", err))
	}
	if err := intent.EnsureJoined(ctx, roomID); err != nil {
		return fmt.Errorf("join room %s as %s: %w", roomID, mxid, err)
	}

	content := map[string]interface{}{
		"application":   callApplication,
		"call_id":       "",
		"scope":         callScope,
		"device_id":     deviceID,
		"membershipID":  fmt.Sprintf("%s:%s", mxid, deviceID),
		"expires":       membershipExpiryMs,
		"m.call.intent": "audio",
		"focus_active": map[string]interface{}{
			"type":            "livekit",
			"focus_selection": "oldest_membership",
		},
		"foci_preferred": []map[string]interface{}{
			{
				"type":               "livekit",
				"livekit_service_url": s.config.LKJWTService,
				"livekit_alias":      string(roomID),
			},
		},
	}

	_, err := intent.SendStateEvent(ctx, roomID, event.Type{
		Type:  callMemberEventType,
		Class: event.StateEventType,
	}, stateKey, content)
	if err != nil {
		return fmt.Errorf("send m.call.member for %s: %w", mxid, err)
	}

	s.mu.Lock()
	s.active[discordUserID] = memberInfo{
		mxid:     mxid,
		deviceID: deviceID,
		roomID:   roomID,
		stateKey: stateKey,
	}
	s.mu.Unlock()

	s.logger.Info("joined call",
		slog.String("mxid", string(mxid)),
		slog.String("room", string(roomID)),
	)
	return nil
}

// LeaveCall removes a Discord user's m.call.member state event.
func (s *Signaller) LeaveCall(ctx context.Context, discordUserID uint64) error {
	s.mu.Lock()
	info, ok := s.active[discordUserID]
	if ok {
		delete(s.active, discordUserID)
	}
	s.mu.Unlock()

	if !ok {
		return nil
	}

	intent := s.as.Intent(info.mxid)
	_, err := intent.SendStateEvent(ctx, info.roomID, event.Type{
		Type:  callMemberEventType,
		Class: event.StateEventType,
	}, info.stateKey, map[string]interface{}{})
	if err != nil {
		return fmt.Errorf("remove m.call.member for %s: %w", info.mxid, err)
	}

	s.logger.Info("left call",
		slog.String("mxid", string(info.mxid)),
		slog.String("room", string(info.roomID)),
	)
	return nil
}

// LeaveAll removes all active memberships. Call on shutdown.
func (s *Signaller) LeaveAll(ctx context.Context) {
	s.mu.Lock()
	ids := make([]uint64, 0, len(s.active))
	for id := range s.active {
		ids = append(ids, id)
	}
	s.mu.Unlock()

	for _, id := range ids {
		if err := s.LeaveCall(ctx, id); err != nil {
			s.logger.Warn("failed to leave call on shutdown",
				slog.Uint64("user", id),
				slog.Any("err", err),
			)
		}
	}
}

// discordMXID returns the Matrix user ID for a Discord user.
// Matches mautrix-discord's naming: @discord_<snowflake>:server
func (s *Signaller) discordMXID(discordUserID uint64) id.UserID {
	return id.UserID(fmt.Sprintf("@discord_%d:%s", discordUserID, s.config.ServerName))
}

// deviceID returns a stable device ID for a Discord user's voice session.
func (s *Signaller) deviceID(discordUserID uint64) string {
	return fmt.Sprintf("VOICE_%d", discordUserID)
}

// Intent returns a mautrix Intent for direct API calls if needed.
func (s *Signaller) Intent(mxid id.UserID) *appservice.IntentAPI {
	return s.as.Intent(mxid)
}

// Client returns a raw mautrix client for the bot user.
func (s *Signaller) Client() *mautrix.Client {
	return s.as.BotClient()
}
