// Package bridge manages dynamic voice channel bridging.
// Watches Discord voice states and auto-bridges active channels.
package bridge

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/lukacsi/livekit-discord-bridge/pkg/ipc"
	lk "github.com/lukacsi/livekit-discord-bridge/pkg/livekit"
	mx "github.com/lukacsi/livekit-discord-bridge/pkg/matrix"
)

// channelInfo holds Discord voice channel metadata.
type channelInfo struct {
	name       string
	categoryID uint64
}

// ChannelBridge holds the state for one active voice channel bridge.
type ChannelBridge struct {
	ChannelID   uint64
	Info        channelInfo
	MatrixRoom  id.RoomID
	LKManager   *lk.Manager
	Subscriber  *lk.Subscriber
	mu          sync.Mutex
	JoinedUsers map[uint64]bool
}

// Manager coordinates voice channel bridging for a guild.
type Manager struct {
	conn       *ipc.Conn
	signaller  *mx.Signaller
	lkConfig   lk.Config
	guildID    string
	serverName string
	lkJWTSvc   string
	logger     *slog.Logger

	mu             sync.Mutex
	voiceStates    map[uint64]uint64      // discord user ID → channel ID (0 = not in VC)
	channelInfos   map[uint64]channelInfo // channel ID → metadata (name, category)
	channelRooms   map[uint64]id.RoomID   // channel ID → Matrix room ID (cache)
	activeBridge   *ChannelBridge         // currently bridged channel (nil if none)
	bridgedChannel uint64                 // channel ID being bridged
}

// NewManager creates a voice channel bridge manager.
func NewManager(
	conn *ipc.Conn,
	signaller *mx.Signaller,
	lkConfig lk.Config,
	guildID, serverName, lkJWTSvc string,
	logger *slog.Logger,
) *Manager {
	return &Manager{
		conn:       conn,
		signaller:  signaller,
		lkConfig:   lkConfig,
		guildID:    guildID,
		serverName: serverName,
		lkJWTSvc:   lkJWTSvc,
		logger:     logger,
		voiceStates:  make(map[uint64]uint64),
		channelInfos: make(map[uint64]channelInfo),
		channelRooms: make(map[uint64]id.RoomID),
	}
}

// HandleMessage processes an IPC message from the sidecar.
// Returns true if the message was handled.
func (m *Manager) HandleMessage(ctx context.Context, msg *ipc.Message) bool {
	switch msg.Type {
	case ipc.MsgVoiceState:
		channelID := uint64(0)
		var info channelInfo
		if len(msg.Payload) >= 8 {
			channelID = binary.LittleEndian.Uint64(msg.Payload)
		}
		if len(msg.Payload) >= 18 {
			info.categoryID = binary.LittleEndian.Uint64(msg.Payload[8:16])
			nameLen := binary.LittleEndian.Uint16(msg.Payload[16:18])
			if len(msg.Payload) >= 18+int(nameLen) {
				info.name = string(msg.Payload[18 : 18+nameLen])
			}
		}
		if channelID != 0 {
			m.mu.Lock()
			m.channelInfos[channelID] = info
			m.mu.Unlock()
		}
		m.onVoiceState(ctx, msg.UserID, channelID)
		return true

	case ipc.MsgUserJoin:
		m.mu.Lock()
		bridge := m.activeBridge
		m.mu.Unlock()
		if bridge != nil {
			bridge.mu.Lock()
			alreadyJoined := bridge.JoinedUsers[msg.UserID]
			if !alreadyJoined {
				bridge.JoinedUsers[msg.UserID] = true
			}
			bridge.mu.Unlock()
			if !alreadyJoined {
				if err := m.signaller.JoinCall(ctx, msg.UserID, bridge.MatrixRoom); err != nil {
					m.logger.Error("failed to signal call join",
						slog.Uint64("user", msg.UserID),
						slog.Any("err", err),
					)
				}
			}
		}
		return true

	case ipc.MsgAudioFromDiscord:
		m.mu.Lock()
		bridge := m.activeBridge
		m.mu.Unlock()
		if bridge != nil {
			if err := bridge.LKManager.WriteOpus(msg.UserID, msg.Payload); err != nil {
				// Silently drop — logging every frame would be too noisy
			}
		}
		return true

	default:
		return false
	}
}

// onVoiceState handles a voice state update from Discord.
func (m *Manager) onVoiceState(ctx context.Context, userID, channelID uint64) {
	m.mu.Lock()
	if channelID == 0 {
		delete(m.voiceStates, userID)
	} else {
		m.voiceStates[userID] = channelID
	}

	// Find the channel with the most users
	bestChannel, bestCount := m.bestChannel()
	currentChannel := m.bridgedChannel
	m.mu.Unlock()

	m.logger.Info("voice state",
		slog.Uint64("user", userID),
		slog.Uint64("channel", channelID),
		slog.Uint64("best_channel", bestChannel),
		slog.Int("best_count", bestCount),
	)

	if bestChannel == currentChannel {
		return // no change needed
	}

	if bestCount == 0 {
		// No users in any VC — stop bridging
		m.stopBridge(ctx)
		return
	}

	if currentChannel != 0 {
		// Switch channels — stop current bridge first
		m.stopBridge(ctx)
	}

	// Start bridging the new channel
	m.startBridge(ctx, bestChannel)
}

// bestChannel returns the channel with the most users. Must be called under mu.
func (m *Manager) bestChannel() (uint64, int) {
	counts := make(map[uint64]int)
	for _, ch := range m.voiceStates {
		counts[ch]++
	}

	var bestCh uint64
	var bestCount int
	for ch, count := range counts {
		if count > bestCount {
			bestCh = ch
			bestCount = count
		}
	}
	return bestCh, bestCount
}

// startBridge begins bridging a Discord voice channel.
func (m *Manager) startBridge(ctx context.Context, channelID uint64) {
	m.mu.Lock()
	info := m.channelInfos[channelID]
	m.mu.Unlock()

	// Create Matrix room for this channel
	matrixRoomID, err := m.ensureMatrixRoom(ctx, channelID, info)
	if err != nil {
		m.logger.Error("failed to create Matrix room",
			slog.Uint64("channel", channelID),
			slog.Any("err", err),
		)
		return
	}

	// LiveKit room = raw Matrix room ID
	lkConfig := m.lkConfig
	lkConfig.RoomName = string(matrixRoomID)

	// Create per-channel LiveKit manager
	lkManager := lk.NewManager(lkConfig, m.logger)
	lkManager.SetIdentityFunc(m.signaller.LiveKitIdentity)

	// Create subscriber for reverse path
	sub, err := lk.NewSubscriber(lkConfig, func(opusFrame []byte) error {
		return m.conn.WriteMessage(&ipc.Message{
			Type:    ipc.MsgAudioToDiscord,
			UserID:  0,
			Payload: opusFrame,
		})
	}, m.logger)
	if err != nil {
		m.logger.Warn("subscriber failed — reverse path disabled", slog.Any("err", err))
	}

	bridge := &ChannelBridge{
		ChannelID:   channelID,
		Info:        info,
		MatrixRoom:  matrixRoomID,
		LKManager:   lkManager,
		Subscriber:  sub,
		JoinedUsers: make(map[uint64]bool),
	}

	m.mu.Lock()
	m.activeBridge = bridge
	m.bridgedChannel = channelID
	m.mu.Unlock()

	// Tell sidecar to join the channel
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload, channelID)
	_ = m.conn.WriteMessage(&ipc.Message{
		Type:    ipc.MsgJoinChannel,
		Payload: payload,
	})

	m.logger.Info("started bridge",
		slog.Uint64("channel", channelID),
		slog.String("matrix_room", string(matrixRoomID)),
	)
}

// stopBridge stops bridging the current channel.
func (m *Manager) stopBridge(ctx context.Context) {
	m.mu.Lock()
	bridge := m.activeBridge
	m.activeBridge = nil
	m.bridgedChannel = 0
	m.mu.Unlock()

	if bridge == nil {
		return
	}

	// Tell sidecar to leave
	_ = m.conn.WriteMessage(&ipc.Message{Type: ipc.MsgLeaveChannel})

	// Clean up Matrix memberships
	m.signaller.LeaveAll(ctx)

	// Clean up LiveKit
	if bridge.Subscriber != nil {
		bridge.Subscriber.Close()
	}
	bridge.LKManager.Close()

	m.logger.Info("stopped bridge", slog.Uint64("channel", bridge.ChannelID))
}

// Close stops any active bridge.
func (m *Manager) Close(ctx context.Context) {
	m.stopBridge(ctx)
}

// ensureMatrixRoom creates or finds a Matrix room for a Discord voice channel.
// Places it in mautrix-discord's Space hierarchy if possible.
func (m *Manager) ensureMatrixRoom(ctx context.Context, channelID uint64, info channelInfo) (id.RoomID, error) {
	// Check cache first
	m.mu.Lock()
	if cachedRoom, ok := m.channelRooms[channelID]; ok {
		m.mu.Unlock()
		return cachedRoom, nil
	}
	m.mu.Unlock()

	intent := m.signaller.BotIntent()
	if err := intent.EnsureRegistered(ctx); err != nil {
		m.logger.Warn("bot user registration (may already exist)", slog.Any("err", err))
	}
	client := intent.Client

	// Room name: use Discord channel name or fallback
	roomName := info.name
	if roomName == "" {
		roomName = fmt.Sprintf("Voice %d", channelID)
	}
	roomName = "🔊 " + roomName

	// Create voice room (MSC3417 type makes Cinny show voice UI)
	createResp, err := client.CreateRoom(ctx, &mautrix.ReqCreateRoom{
		Name:   roomName,
		Topic:  fmt.Sprintf("Discord voice channel %d", channelID),
		Preset: "public_chat",
		CreationContent: map[string]interface{}{
			"type": "org.matrix.msc3417.call",
		},
		PowerLevelOverride: &event.PowerLevelsEventContent{
			StateDefaultPtr: func() *int { v := 0; return &v }(),
		},
	})
	if err != nil {
		return "", fmt.Errorf("create room: %w", err)
	}

	roomID := createResp.RoomID

	// Cache for future lookups
	m.mu.Lock()
	m.channelRooms[channelID] = roomID
	m.mu.Unlock()

	m.logger.Info("created Matrix room",
		slog.String("room", string(roomID)),
		slog.String("name", roomName),
	)

	// Invite all Discord users currently in this voice channel
	// For now, also invite the bridge admin (oger) for testing
	_, _ = client.InviteUser(ctx, roomID, &mautrix.ReqInviteUser{
		UserID: id.UserID("@oger:" + m.serverName),
	})

	// Add to mautrix-discord's guild Space hierarchy
	m.addToGuildSpace(ctx, roomID, info.categoryID)

	return roomID, nil
}

// addToGuildSpace adds a room to the guild's Space hierarchy.
// If categoryID is set, adds to the category sub-Space. Otherwise adds to guild Space directly.
func (m *Manager) addToGuildSpace(ctx context.Context, roomID id.RoomID, categoryID uint64) {
	client := m.signaller.Client()

	// Find the guild Space by checking known mautrix-discord alias patterns
	// mautrix-discord doesn't use a consistent alias for guild spaces,
	// so we use the guild Space ID from config or discovery
	guildSpaceAlias := id.RoomAlias(fmt.Sprintf("#discord_%s:%s", m.guildID, m.serverName))

	var targetSpace id.RoomID

	// Try category Space first
	if categoryID != 0 {
		catAlias := id.RoomAlias(fmt.Sprintf("#discord_%s_%d:%s", m.guildID, categoryID, m.serverName))
		if resp, err := client.ResolveAlias(ctx, catAlias); err == nil {
			targetSpace = resp.RoomID
		}
	}

	// Fall back to guild Space
	if targetSpace == "" {
		if resp, err := client.ResolveAlias(ctx, guildSpaceAlias); err == nil {
			targetSpace = resp.RoomID
		}
	}

	if targetSpace == "" {
		m.logger.Warn("could not find guild Space — room created but not added to hierarchy")
		return
	}

	// Add room as child of the Space via m.space.child state event
	_, err := client.SendStateEvent(ctx, targetSpace, event.Type{
		Type:  "m.space.child",
		Class: event.StateEventType,
	}, string(roomID), map[string]interface{}{
		"via": []string{m.serverName},
	})
	if err != nil {
		m.logger.Warn("failed to add room to Space",
			slog.String("room", string(roomID)),
			slog.String("space", string(targetSpace)),
			slog.Any("err", err),
		)
	} else {
		m.logger.Info("added room to Space",
			slog.String("room", string(roomID)),
			slog.String("space", string(targetSpace)),
		)
	}
}
