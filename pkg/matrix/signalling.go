package matrix

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"sync"
	"time"

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
	config         Config
	as             *appservice.AppService
	logger         *slog.Logger
	mu             sync.Mutex
	active         map[uint64]memberInfo // Discord user ID → active membership
	profileChecked map[uint64]bool       // users whose profile we've already verified
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
		config:         config,
		as:             as,
		logger:         logger,
		active:         make(map[uint64]memberInfo),
		profileChecked: make(map[uint64]bool),
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

// sendStateRetry sends a state event with timeout and retry (max 3 attempts, linear backoff).
func (s *Signaller) sendStateRetry(ctx context.Context, intent *appservice.IntentAPI, roomID id.RoomID, evtType event.Type, stateKey string, content interface{}) error {
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err = intent.SendStateEvent(reqCtx, roomID, evtType, stateKey, content)
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt < 3 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}
	}
	return err
}

// JoinCall sends an m.call.member state event for a Discord user in the given room.
func (s *Signaller) JoinCall(ctx context.Context, discordUserID uint64, roomID id.RoomID) error {
	mxid := s.discordMXID(discordUserID)
	deviceID := s.deviceID(discordUserID)
	stateKey := fmt.Sprintf("_%s_%s_%s", mxid, deviceID, callApplication)

	intent := s.as.Intent(mxid)

	regCtx, regCancel := context.WithTimeout(ctx, 10*time.Second)
	if err := intent.EnsureRegistered(regCtx); err != nil {
		s.logger.Debug("user registration (may already exist)",
			slog.String("matrix_user", string(mxid)), slog.Any("err", err))
	}
	regCancel()

	joinCtx, joinCancel := context.WithTimeout(ctx, 10*time.Second)
	err := intent.EnsureJoined(joinCtx, roomID)
	joinCancel()
	if err != nil {
		return fmt.Errorf("join room %s as %s: %w", roomID, mxid, err)
	}

	content := map[string]interface{}{
		"application":   callApplication,
		"call_id":       "",
		"scope":         callScope,
		"device_id":     deviceID,
		"membershipID":  fmt.Sprintf("%s:%s", mxid, deviceID),
		"expires_ts":    time.Now().UnixMilli() + membershipExpiryMs,
		"m.call.intent": "audio",
		"focus_active": map[string]interface{}{
			"type":            "livekit",
			"focus_selection": "oldest_membership",
		},
		"foci_preferred": []map[string]interface{}{
			{
				"type":               "livekit",
				"livekit_service_url": s.config.LKJWTService,
				"livekit_alias":      LiveKitRoomAlias(string(roomID)),
			},
		},
	}

	if err := s.sendStateRetry(ctx, intent, roomID, event.Type{
		Type:  callMemberEventType,
		Class: event.StateEventType,
	}, stateKey, content); err != nil {
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
		slog.String("matrix_user", string(mxid)),
		slog.String("matrix_room", string(roomID)),
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
	if err := s.sendStateRetry(ctx, intent, info.roomID, event.Type{
		Type:  callMemberEventType,
		Class: event.StateEventType,
	}, info.stateKey, map[string]interface{}{}); err != nil {
		return fmt.Errorf("remove m.call.member for %s: %w", info.mxid, err)
	}

	s.logger.Info("left call",
		slog.String("matrix_user", string(info.mxid)),
		slog.String("matrix_room", string(info.roomID)),
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
				slog.Uint64("discord_user", id),
				slog.Any("err", err),
			)
		}
	}
}

// EnsureProfile sets the display name and avatar on a Discord puppet user
// if they haven't been set already (e.g. by mautrix-discord for text channels).
// Only runs once per user per bridge lifetime.
func (s *Signaller) EnsureProfile(ctx context.Context, discordUserID uint64, displayName, avatarHash string) {
	s.mu.Lock()
	if s.profileChecked[discordUserID] {
		s.mu.Unlock()
		return
	}
	s.profileChecked[discordUserID] = true
	s.mu.Unlock()

	mxid := s.discordMXID(discordUserID)
	intent := s.as.Intent(mxid)

	if err := intent.EnsureRegistered(ctx); err != nil {
		s.logger.Debug("user registration (may already exist)", slog.Any("err", err))
	}

	// Check if display name is already set (by mautrix-discord)
	profile, err := intent.GetProfile(ctx, mxid)
	if err == nil && profile.DisplayName != "" {
		return // mautrix-discord already set it
	}

	// Set display name
	if displayName != "" {
		if err := intent.SetDisplayName(ctx, displayName); err != nil {
			s.logger.Warn("failed to set display name",
				slog.String("matrix_user", string(mxid)),
				slog.Any("err", err),
			)
		} else {
			s.logger.Info("set puppet display name",
				slog.String("matrix_user", string(mxid)),
				slog.String("name", displayName),
			)
		}
	}

	// Set avatar from Discord CDN (validate hash to prevent URL manipulation)
	if avatarHash != "" && discordAvatarHashRe.MatchString(avatarHash) {
		avatarURL := fmt.Sprintf("https://cdn.discordapp.com/avatars/%d/%s.png?size=256",
			discordUserID, avatarHash)
		s.uploadAndSetAvatar(ctx, intent, mxid, avatarURL)
	}
}

var discordAvatarHashRe = regexp.MustCompile(`^[0-9a-fA-F]{32}$|^a_[0-9a-fA-F]{32}$`)

var avatarHTTPClient = &http.Client{Timeout: 10 * time.Second}

func (s *Signaller) uploadAndSetAvatar(ctx context.Context, intent *appservice.IntentAPI, mxid id.UserID, url string) {
	resp, err := avatarHTTPClient.Get(url)
	if err != nil {
		s.logger.Warn("failed to download avatar", slog.String("url", url), slog.Any("err", err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024)) // 512KB max
	if err != nil {
		return
	}

	uploaded, err := intent.UploadMedia(ctx, mautrix.ReqUploadMedia{
		ContentBytes:  data,
		ContentType:   "image/png",
		ContentLength: int64(len(data)),
		FileName:      "avatar.png",
	})
	if err != nil {
		s.logger.Warn("failed to upload avatar", slog.Any("err", err))
		return
	}

	if err := intent.SetAvatarURL(ctx, uploaded.ContentURI); err != nil {
		s.logger.Warn("failed to set avatar", slog.Any("err", err))
	}
}

// GetDisplayName returns the display name for a Matrix user, or the localpart as fallback.
func (s *Signaller) GetDisplayName(ctx context.Context, userID id.UserID) string {
	client := s.BotIntent().Client
	profCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	profile, err := client.GetProfile(profCtx, userID)
	if err == nil && profile.DisplayName != "" {
		return profile.DisplayName
	}
	// Fallback: extract localpart from @user:server
	local := string(userID)
	if len(local) > 1 && local[0] == '@' {
		for i, c := range local {
			if c == ':' {
				return local[1:i]
			}
		}
		return local[1:]
	}
	return local
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

// BotIntent returns an intent for the voice bridge bot user.
func (s *Signaller) BotIntent() *appservice.IntentAPI {
	botMXID := id.UserID(fmt.Sprintf("@discord_voice_bridge:%s", s.config.ServerName))
	return s.as.Intent(botMXID)
}

// Client returns a raw mautrix client for the bridge bot user.
func (s *Signaller) Client() *mautrix.Client {
	return s.BotIntent().Client
}

// StartRenewal starts a background goroutine that refreshes active m.call.member
// events before they expire (every 3 hours for a 4-hour expiry window).
// Cancel ctx to stop.
func (s *Signaller) StartRenewal(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(3 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.mu.Lock()
				members := make([]memberInfo, 0, len(s.active))
				for _, info := range s.active {
					members = append(members, info)
				}
				s.mu.Unlock()

				if len(members) == 0 {
					continue
				}

				s.logger.Info("renewing call memberships", slog.Int("count", len(members)))
				for _, info := range members {
					intent := s.as.Intent(info.mxid)
					content := map[string]interface{}{
						"application":   callApplication,
						"call_id":       "",
						"scope":         callScope,
						"device_id":     info.deviceID,
						"membershipID":  fmt.Sprintf("%s:%s", info.mxid, info.deviceID),
						"expires_ts":    time.Now().UnixMilli() + membershipExpiryMs,
						"m.call.intent": "audio",
						"focus_active": map[string]interface{}{
							"type":            "livekit",
							"focus_selection": "oldest_membership",
						},
						"foci_preferred": []map[string]interface{}{
							{
								"type":               "livekit",
								"livekit_service_url": s.config.LKJWTService,
								"livekit_alias":      LiveKitRoomAlias(string(info.roomID)),
							},
						},
					}
					if _, err := intent.SendStateEvent(ctx, info.roomID, event.Type{
						Type:  callMemberEventType,
						Class: event.StateEventType,
					}, info.stateKey, content); err != nil {
						s.logger.Warn("failed to renew membership",
							slog.String("matrix_user", string(info.mxid)),
							slog.Any("err", err),
						)
					}
				}
			}
		}
	}()
}

// CallMemberHandler is called when a m.call.member state event is observed.
// roomID is the Matrix room, userMXID is the sender, joined is true if the event
// indicates an active call membership (false for leave/empty content).
type CallMemberHandler func(ctx context.Context, roomID id.RoomID, userMXID string, joined bool)

// StartSync begins a /sync loop as the voice bridge bot, watching for m.call.member
// state events in voice rooms. Calls onCallMember for each event.
// Runs in a background goroutine; cancel ctx to stop.
func (s *Signaller) StartSync(ctx context.Context, onCallMember CallMemberHandler) error {
	// Create a dedicated client for /sync — the appservice intent client has no Syncer.
	// Must set AppServiceUserID so all requests include ?user_id= for the AS token.
	botMXID := id.UserID(fmt.Sprintf("@discord_voice_bridge:%s", s.config.ServerName))
	client, err := mautrix.NewClient(s.config.HomeserverURL, botMXID, s.config.ASToken)
	if err != nil {
		return fmt.Errorf("create sync client: %w", err)
	}
	client.SetAppServiceUserID = true

	syncer, ok := client.Syncer.(*mautrix.DefaultSyncer)
	if !ok {
		return fmt.Errorf("unexpected Syncer type on mautrix client")
	}

	// Only process events from incremental syncs (since != "").
	// Initial sync contains stale m.call.member state from previous sessions
	// which cannot be reliably distinguished from active calls (expires_ts
	// has a 4-hour window). Users must be actively in a call for the bridge
	// to detect them — rejoin after bridge restart.
	initialSyncDone := false
	syncer.OnSync(func(_ context.Context, _ *mautrix.RespSync, since string) bool {
		if since != "" && !initialSyncDone {
			initialSyncDone = true
			s.logger.Info("initial sync complete, watching for call events")
		}
		return true
	})

	callMemberType := event.Type{Type: callMemberEventType, Class: event.StateEventType}
	syncer.OnEventType(callMemberType, func(_ context.Context, evt *event.Event) {
		if !initialSyncDone || evt.StateKey == nil {
			return
		}
		joined := false
		if raw := evt.Content.Raw; raw != nil {
			if _, hasApp := raw["application"]; hasApp {
				if expiresTs, ok := raw["expires_ts"].(float64); ok {
					if int64(expiresTs) < time.Now().UnixMilli() {
						return // expired
					}
				}
				joined = true
			}
		}
		onCallMember(ctx, evt.RoomID, string(evt.Sender), joined)
	})

	go func() {
		backoff := 500 * time.Millisecond
		for {
			if err := client.SyncWithContext(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				s.logger.Warn("sync error, retrying", slog.Any("err", err), slog.Duration("backoff", backoff))
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				// Exponential backoff: 500ms → 1s → 2s → 4s → 8s (max)
				backoff = min(backoff*2, 8*time.Second)
			} else {
				backoff = 500 * time.Millisecond // reset on success
			}
		}
	}()

	s.logger.Info("started Matrix /sync loop for call member events")
	return nil
}
