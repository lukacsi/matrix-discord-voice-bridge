// Package bridge manages dynamic voice channel bridging.
// Watches Discord voice states and auto-bridges active channels.
package bridge

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/lukacsi/livekit-discord-bridge/pkg/ipc"
	lk "github.com/lukacsi/livekit-discord-bridge/pkg/livekit"
	mx "github.com/lukacsi/livekit-discord-bridge/pkg/matrix"
	"github.com/lukacsi/livekit-discord-bridge/pkg/store"
	"github.com/lukacsi/livekit-discord-bridge/pkg/types"
)

const bridgeProtocolID = "discord-voice"

// channelInfo holds Discord voice channel metadata.
type channelInfo struct {
	name       string
	categoryID uint64
}

// SidecarSlot represents one sidecar connection that can bridge one voice channel.
type SidecarSlot struct {
	Conn    *ipc.Conn
	Index   int
	Primary bool
	Token   string // masked bot token for display

	// Protected by Manager.mu
	channelID uint64 // 0 = free
}

// ChannelBridge holds the state for one active voice channel bridge.
type ChannelBridge struct {
	ChannelID   uint64
	Info        channelInfo
	MatrixRoom  id.RoomID
	LKManager   *lk.Manager
	Subscriber  *lk.Subscriber
	SlotIndex   int // which sidecar slot is handling audio
	mu          sync.Mutex
	JoinedUsers map[uint64]bool
}

// Manager coordinates voice channel bridging for a guild.
// Supports N sidecar slots for N concurrent voice channel bridges.
// SidecarConfig holds parameters needed to start a sidecar dynamically.
type SidecarConfig struct {
	Dir        string
	SocketBase string // base socket path (e.g. /tmp/discord-voice-bridge.sock)
	LogLevel   string
}

type Manager struct {
	slots        []*SidecarSlot
	signaller    *mx.Signaller
	store        *store.Store
	lkConfig     lk.Config
	sidecarCfg   SidecarConfig
	guildID      string
	serverName   string
	lkJWTSvc     string
	logger       *slog.Logger
	onSlotReady  func(slot *SidecarSlot) // called when new slot's read loop should start

	mu             sync.Mutex
	voiceStates    map[uint64]uint64              // discord user ID → channel ID
	channelInfos   map[uint64]channelInfo         // channel ID → metadata
	channelRooms   map[uint64]id.RoomID           // channel ID → Matrix room ID
	activeBridges  map[uint64]*ChannelBridge      // channel ID → active bridge
	guildSpace     id.RoomID                      // cached mautrix-discord guild Space
	categorySpaces map[uint64]id.RoomID           // cached category sub-Space IDs
	roomChannels   map[id.RoomID]uint64           // reverse: Matrix room → Discord channel
	matrixUsers    map[uint64]map[string]string   // channelID → {mxid → displayName}
	stopTimers     map[uint64]*time.Timer         // pending bridge stop debounce timers
}

// NewManager creates a voice channel bridge manager with the given sidecar slots.
func NewManager(
	slots []*SidecarSlot,
	signaller *mx.Signaller,
	db *store.Store,
	lkConfig lk.Config,
	guildID, serverName, lkJWTSvc string,
	logger *slog.Logger,
) *Manager {
	return &Manager{
		slots:          slots,
		signaller:      signaller,
		store:          db,
		lkConfig:       lkConfig,
		guildID:        guildID,
		serverName:     serverName,
		lkJWTSvc:       lkJWTSvc,
		logger:         logger,
		voiceStates:    make(map[uint64]uint64),
		channelInfos:   make(map[uint64]channelInfo),
		channelRooms:   make(map[uint64]id.RoomID),
		activeBridges:  make(map[uint64]*ChannelBridge),
		categorySpaces: make(map[uint64]id.RoomID),
		roomChannels:   make(map[id.RoomID]uint64),
		matrixUsers:    make(map[uint64]map[string]string),
		stopTimers:     make(map[uint64]*time.Timer),
	}
}

// SetSidecarConfig sets the config needed for hot-adding sidecars.
func (m *Manager) SetSidecarConfig(cfg SidecarConfig, onReady func(slot *SidecarSlot)) {
	m.sidecarCfg = cfg
	m.onSlotReady = onReady
}

// AddBot hot-adds a new Discord bot token — starts a sidecar and creates a new slot.
func (m *Manager) AddBot(ctx context.Context, token string) (int, error) {
	m.mu.Lock()
	slotIdx := len(m.slots)
	m.mu.Unlock()

	socketPath := fmt.Sprintf("%s.%d", m.sidecarCfg.SocketBase, slotIdx)

	srv, err := ipc.NewServer(socketPath)
	if err != nil {
		return 0, fmt.Errorf("create socket: %w", err)
	}

	cmd, err := ipc.StartSidecar(m.sidecarCfg.Dir, socketPath, token, m.guildID, m.sidecarCfg.LogLevel, false, slotIdx)
	if err != nil {
		srv.Close()
		return 0, fmt.Errorf("start sidecar: %w", err)
	}

	conn, err := srv.Accept(30 * time.Second)
	if err != nil {
		_ = cmd.Process.Kill()
		srv.Close()
		return 0, fmt.Errorf("sidecar connect: %w", err)
	}
	conn.SetWriteTimeout(5 * time.Second)
	srv.Close() // listener no longer needed

	masked := token
	if len(masked) > 10 {
		masked = masked[:10] + "..."
	}

	slot := &SidecarSlot{
		Conn:    conn,
		Index:   slotIdx,
		Primary: false,
		Token:   masked,
	}

	m.mu.Lock()
	m.slots = append(m.slots, slot)
	m.mu.Unlock()

	// Persist to DB
	if m.store != nil {
		if _, err := m.store.AddBot(token, m.guildID); err != nil {
			m.logger.Warn("failed to persist bot token", slog.Any("err", err))
		}
	}

	m.logger.Info("hot-added bot", slog.Int("slot", slotIdx))

	// Start the read loop for this slot
	if m.onSlotReady != nil {
		m.onSlotReady(slot)
	}

	return slotIdx, nil
}

// RemoveBot stops a sidecar slot and marks it dead.
func (m *Manager) RemoveBot(ctx context.Context, slotIdx int) error {
	m.mu.Lock()
	if slotIdx < 0 || slotIdx >= len(m.slots) {
		m.mu.Unlock()
		return fmt.Errorf("slot %d does not exist", slotIdx)
	}
	slot := m.slots[slotIdx]
	if slot.channelID == ^uint64(0) {
		m.mu.Unlock()
		return fmt.Errorf("slot %d is already dead", slotIdx)
	}
	m.mu.Unlock()

	// Stop any active bridge on this slot
	m.HandleSlotDeath(ctx, slotIdx)

	// Close the IPC connection (triggers sidecar shutdown)
	if slot.Conn != nil {
		_ = slot.Conn.WriteMessage(&ipc.Message{Type: ipc.MsgShutdown})
		slot.Conn.Close()
	}

	m.logger.Info("removed bot", slog.Int("slot", slotIdx))
	return nil
}

// RebuildDB re-scans Matrix state events and rebuilds the room database.
func (m *Manager) RebuildDB(ctx context.Context) (int, error) {
	if m.store == nil {
		return 0, fmt.Errorf("no database configured")
	}

	intent := m.signaller.BotIntent()
	resp, err := intent.Client.JoinedRooms(ctx)
	if err != nil {
		return 0, fmt.Errorf("list joined rooms: %w", err)
	}

	bridgeEventType := event.Type{Type: "m.bridge", Class: event.StateEventType}
	prefix := fmt.Sprintf("fi.mau.discord://discord/%s/", m.guildID)
	found := 0

	for _, roomID := range resp.JoinedRooms {
		stateMap, err := intent.Client.State(ctx, roomID)
		if err != nil {
			continue
		}
		bridges, ok := stateMap[bridgeEventType]
		if !ok {
			continue
		}
		for stateKey, evt := range bridges {
			if !strings.HasPrefix(stateKey, prefix) {
				continue
			}
			raw := evt.Content.Raw
			if raw == nil {
				continue
			}
			protocol, _ := raw["protocol"].(map[string]interface{})
			if protocol == nil || protocol["id"] != bridgeProtocolID {
				continue
			}
			channel, _ := raw["channel"].(map[string]interface{})
			if channel == nil {
				continue
			}
			chIDStr, _ := channel["id"].(string)
			chID, err := strconv.ParseUint(chIDStr, 10, 64)
			if err != nil {
				continue
			}
			name, _ := channel["displayname"].(string)

			m.mu.Lock()
			m.channelRooms[chID] = roomID
			m.roomChannels[roomID] = chID
			m.mu.Unlock()

			_ = m.store.UpsertRoom(store.Room{
				DiscordChannel: chID,
				MatrixRoom:     string(roomID),
				Name:           name,
				GuildID:        m.guildID,
			})
			found++
		}
	}

	m.logger.Info("rebuilt database from Matrix state", slog.Int("rooms", found))
	return found, nil
}

// HandleMessage processes an IPC message from a sidecar slot.
// slotIdx identifies which sidecar sent the message.
func (m *Manager) HandleMessage(ctx context.Context, slotIdx int, msg *ipc.Message) bool {
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

	case ipc.MsgChannelList:
		if len(msg.Payload) >= 18 {
			channelID := binary.LittleEndian.Uint64(msg.Payload)
			categoryID := binary.LittleEndian.Uint64(msg.Payload[8:16])
			nameLen := binary.LittleEndian.Uint16(msg.Payload[16:18])
			var name string
			if len(msg.Payload) >= 18+int(nameLen) {
				name = string(msg.Payload[18 : 18+nameLen])
			}
			info := channelInfo{name: name, categoryID: categoryID}
			m.mu.Lock()
			m.channelInfos[channelID] = info
			m.mu.Unlock()
			if _, err := m.ensureMatrixRoom(ctx, channelID, info); err != nil {
				m.logger.Warn("failed to pre-create room",
					slog.Uint64("discord_channel", channelID),
					slog.String("name", name),
					slog.Any("err", err),
				)
			}
		}
		return true

	case ipc.MsgUserJoin:
		// Track user for audio — find the bridge for this slot
		m.mu.Lock()
		bridge := m.bridgeForSlot(slotIdx)
		m.mu.Unlock()
		if bridge != nil {
			bridge.mu.Lock()
			bridge.JoinedUsers[msg.UserID] = true
			bridge.mu.Unlock()
		}
		return true

	case ipc.MsgUserInfo:
		// Set display name + avatar on puppet user if not already set by mautrix-discord
		if m.signaller != nil && len(msg.Payload) >= 2 {
			nameLen := binary.LittleEndian.Uint16(msg.Payload[0:2])
			var displayName, avatarHash string
			if len(msg.Payload) >= 2+int(nameLen) {
				displayName = string(msg.Payload[2 : 2+nameLen])
			}
			offset := 2 + int(nameLen)
			if len(msg.Payload) >= offset+2 {
				avatarLen := binary.LittleEndian.Uint16(msg.Payload[offset : offset+2])
				if len(msg.Payload) >= offset+2+int(avatarLen) {
					avatarHash = string(msg.Payload[offset+2 : offset+2+int(avatarLen)])
				}
			}
			m.signaller.EnsureProfile(ctx, msg.UserID, displayName, avatarHash)
		}
		return true

	case ipc.MsgLeaveChannel:
		// Sidecar-initiated leave (e.g., join failure after retries)
		m.mu.Lock()
		bridge := m.bridgeForSlot(slotIdx)
		m.mu.Unlock()
		if bridge != nil {
			m.logger.Warn("sidecar reported channel leave",
				slog.Uint64("discord_channel", bridge.ChannelID), slog.Int("slot", slotIdx))
			go m.stopBridgeForChannel(ctx, bridge.ChannelID)
		}
		return true

	case ipc.MsgAudioFromDiscord:
		m.mu.Lock()
		bridge := m.bridgeForSlot(slotIdx)
		m.mu.Unlock()
		if bridge != nil && bridge.LKManager != nil {
			if err := bridge.LKManager.WriteOpus(msg.UserID, msg.Payload); err != nil {
				m.logger.Debug("WriteOpus error", slog.Uint64("discord_user", msg.UserID), slog.Any("err", err))
			}
		}
		return true

	default:
		return false
	}
}

// bridgeForSlot returns the active bridge using the given slot. Must be called under mu.
func (m *Manager) bridgeForSlot(slotIdx int) *ChannelBridge {
	for _, b := range m.activeBridges {
		if b.SlotIndex == slotIdx {
			return b
		}
	}
	return nil
}

// onVoiceState handles a voice state update from Discord.
// Signals presence to Matrix rooms (m.call.member) but does NOT start the audio bridge.
// Audio bridging is triggered by Matrix users joining the voice room.
func (m *Manager) onVoiceState(ctx context.Context, userID, channelID uint64) {
	m.mu.Lock()
	oldChannel := m.voiceStates[userID]
	if channelID == 0 {
		delete(m.voiceStates, userID)
	} else {
		m.voiceStates[userID] = channelID
	}
	m.mu.Unlock()

	if m.signaller == nil {
		return
	}

	// Remove from old channel's Matrix room
	if oldChannel != 0 && oldChannel != channelID {
		if err := m.signaller.LeaveCall(ctx, userID); err != nil {
			m.logger.Warn("failed to remove presence",
				slog.Uint64("discord_user", userID),
				slog.Any("err", err),
			)
		}
	}

	// Add to new channel's Matrix room
	if channelID != 0 {
		m.mu.Lock()
		room := m.channelRooms[channelID]
		m.mu.Unlock()
		if room != "" {
			if err := m.signaller.JoinCall(ctx, userID, room); err != nil {
				m.logger.Warn("failed to signal presence",
					slog.Uint64("discord_user", userID),
					slog.Any("err", err),
				)
			}
		}
	}

	m.logger.Debug("voice state",
		slog.Uint64("discord_user", userID),
		slog.Uint64("discord_channel", channelID),
	)
}

// startBridge begins audio bridging for a Discord voice channel.
// Finds a free sidecar slot, starts LiveKit, tells the sidecar to join the VC.
func (m *Manager) startBridge(ctx context.Context, channelID uint64) {
	m.mu.Lock()
	// Already bridging this channel?
	if _, exists := m.activeBridges[channelID]; exists {
		m.mu.Unlock()
		return
	}
	// Find a free sidecar slot (channelID == 0 means free, ^uint64(0) means dead)
	slotIdx := -1
	for i, s := range m.slots {
		if s.channelID == 0 {
			slotIdx = i
			s.channelID = channelID
			break
		}
	}
	if slotIdx == -1 {
		m.mu.Unlock()
		m.logger.Warn("no free sidecar slot — cannot bridge channel",
			slog.Uint64("discord_channel", channelID),
			slog.Int("total_slots", len(m.slots)),
		)
		return
	}
	slot := m.slots[slotIdx]
	matrixRoomID := m.channelRooms[channelID]
	info := m.channelInfos[channelID]

	if matrixRoomID == "" {
		slot.channelID = 0 // release slot
		m.mu.Unlock()
		m.logger.Error("no Matrix room for channel", slog.Uint64("discord_channel", channelID))
		return
	}

	// Insert sentinel so concurrent calls see this channel as claimed
	m.activeBridges[channelID] = &ChannelBridge{ChannelID: channelID, SlotIndex: slotIdx}
	m.mu.Unlock()

	lkConfig := m.lkConfig
	lkConfig.RoomName = mx.LiveKitRoomAlias(string(matrixRoomID))

	// Tell sidecar to join Discord VC IMMEDIATELY (don't wait for LiveKit)
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload, channelID)
	if err := slot.Conn.WriteMessage(&ipc.Message{
		Type:    ipc.MsgJoinChannel,
		Payload: payload,
	}); err != nil {
		m.logger.Error("failed to send JOIN_CHANNEL to sidecar",
			slog.Int("slot", slotIdx), slog.Any("err", err))
	}

	// Connect to LiveKit in parallel with Discord join
	lkManager := lk.NewManager(lkConfig, m.logger)
	lkManager.SetIdentityFunc(m.signaller.LiveKitIdentity)

	sub, err := lk.NewSubscriber(lkConfig, func(opusFrame []byte) error {
		return slot.Conn.WriteMessage(&ipc.Message{
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
		SlotIndex:   slotIdx,
		JoinedUsers: make(map[uint64]bool),
	}

	m.mu.Lock()
	m.activeBridges[channelID] = bridge
	m.mu.Unlock()

	m.logger.Info("started bridge",
		slog.Uint64("discord_channel", channelID),
		slog.String("matrix_room", string(matrixRoomID)),
		slog.Int("slot", slotIdx),
	)
}

// stopBridgeForChannel stops audio bridging for a specific channel.
// Releases the sidecar slot back to the pool.
func (m *Manager) stopBridgeForChannel(ctx context.Context, channelID uint64) {
	m.mu.Lock()
	bridge, exists := m.activeBridges[channelID]
	if !exists {
		m.mu.Unlock()
		return
	}
	delete(m.activeBridges, channelID)
	// Release the sidecar slot (but don't overwrite dead sentinel)
	if bridge.SlotIndex >= 0 && bridge.SlotIndex < len(m.slots) {
		if m.slots[bridge.SlotIndex].channelID != ^uint64(0) {
			m.slots[bridge.SlotIndex].channelID = 0
		}
	}
	m.mu.Unlock()

	// Tell sidecar to leave
	if bridge.SlotIndex >= 0 && bridge.SlotIndex < len(m.slots) {
		slot := m.slots[bridge.SlotIndex]
		if slot.Conn != nil {
			if err := slot.Conn.WriteMessage(&ipc.Message{Type: ipc.MsgLeaveChannel}); err != nil {
				m.logger.Warn("failed to send LEAVE_CHANNEL to sidecar",
					slog.Int("slot", bridge.SlotIndex), slog.Any("err", err))
			}
		}
	}

	if bridge.Subscriber != nil {
		bridge.Subscriber.Close()
	}
	if bridge.LKManager != nil {
		bridge.LKManager.Close()
	}

	m.logger.Info("stopped bridge",
		slog.Uint64("discord_channel", bridge.ChannelID),
		slog.Int("slot", bridge.SlotIndex),
	)
}

// Close stops all active bridges, cancels timers, and cleans up Matrix presence.
func (m *Manager) Close(ctx context.Context) {
	m.mu.Lock()
	// Cancel all pending debounce timers
	for ch, timer := range m.stopTimers {
		timer.Stop()
		delete(m.stopTimers, ch)
	}
	channels := make([]uint64, 0, len(m.activeBridges))
	for ch := range m.activeBridges {
		channels = append(channels, ch)
	}
	m.mu.Unlock()

	for _, ch := range channels {
		m.stopBridgeForChannel(ctx, ch)
	}
	if m.signaller != nil {
		m.signaller.LeaveAll(ctx)
	}
}

// OnMatrixCallMember handles m.call.member events from the Matrix /sync loop.
// When a real Matrix user (not a bridged Discord ghost) joins a voice room,
// starts the audio bridge for the corresponding Discord channel.
func (m *Manager) OnMatrixCallMember(ctx context.Context, roomID id.RoomID, userMXID string, joined bool) {
	if strings.HasPrefix(userMXID, "@discord_") {
		return
	}

	m.mu.Lock()
	channelID, ok := m.roomChannels[roomID]
	m.mu.Unlock()
	if !ok {
		return
	}

	if joined {
		m.mu.Lock()
		// Cancel any pending stop for this channel
		if timer := m.stopTimers[channelID]; timer != nil {
			timer.Stop()
			delete(m.stopTimers, channelID)
		}
		// If joining a DIFFERENT channel, collect channels to stop (can't drop
		// lock inside range loop — concurrent map mutation race).
		var channelsToStop []uint64
		for otherCh, users := range m.matrixUsers {
			if otherCh == channelID {
				continue
			}
			if _, inOther := users[userMXID]; inOther {
				delete(users, userMXID)
				if len(users) == 0 {
					delete(m.matrixUsers, otherCh)
				}
				if timer := m.stopTimers[otherCh]; timer != nil {
					timer.Stop()
					delete(m.stopTimers, otherCh)
				}
				channelsToStop = append(channelsToStop, otherCh)
			}
		}
		m.mu.Unlock()
		for _, ch := range channelsToStop {
			m.stopBridgeForChannel(ctx, ch)
		}
		m.mu.Lock()
		// Check if this is a new join or just a state update (camera toggle, etc.)
		alreadyTracked := false
		if users := m.matrixUsers[channelID]; users != nil {
			_, alreadyTracked = users[userMXID]
		}
		m.mu.Unlock()

		if !alreadyTracked {
			// Resolve display name outside the lock (HTTP call, up to 5s)
			displayName := userMXID
			if m.signaller != nil {
				displayName = m.signaller.GetDisplayName(ctx, id.UserID(userMXID))
			}
			m.mu.Lock()
			if m.matrixUsers[channelID] == nil {
				m.matrixUsers[channelID] = make(map[string]string)
			}
			m.matrixUsers[channelID][userMXID] = displayName
			m.mu.Unlock()
		}

		if !alreadyTracked {
			m.logger.Info("matrix user joined voice room",
				slog.String("matrix_user", userMXID),
				slog.String("matrix_room", string(roomID)),
				slog.Uint64("discord_channel", channelID),
			)
			m.startBridge(ctx, channelID)
			m.pushMatrixUsers(channelID)
		}
	} else {
		m.mu.Lock()
		if users := m.matrixUsers[channelID]; users != nil {
			delete(users, userMXID)
			if len(users) == 0 {
				delete(m.matrixUsers, channelID)
			}
		}
		noUsers := len(m.matrixUsers[channelID]) == 0
		m.mu.Unlock()

		m.logger.Info("matrix user left voice room",
			slog.String("matrix_user", userMXID),
			slog.String("matrix_room", string(roomID)),
			slog.Uint64("discord_channel", channelID),
		)

		m.pushMatrixUsers(channelID)

		// Debounce bridge stop — Element Call refreshes m.call.member by
		// sending leave+join in quick succession. Wait 5s before stopping.
		if noUsers {
			m.mu.Lock()
			if timer := m.stopTimers[channelID]; timer != nil {
				timer.Stop()
			}
			m.stopTimers[channelID] = time.AfterFunc(5*time.Second, func() {
				m.mu.Lock()
				stillEmpty := len(m.matrixUsers[channelID]) == 0
				delete(m.stopTimers, channelID)
				m.mu.Unlock()
				if stillEmpty {
					m.stopBridgeForChannel(ctx, channelID)
				}
			})
			m.mu.Unlock()
		}
	}
}

// HandleSlotDeath cleans up when a sidecar slot dies (process crash, IPC EOF).
// Stops any active bridge on that slot and marks it unavailable.
func (m *Manager) HandleSlotDeath(ctx context.Context, slotIdx int) {
	m.mu.Lock()
	// Guard against double-death (RemoveBot + read loop EOF can both trigger)
	if slotIdx >= 0 && slotIdx < len(m.slots) && m.slots[slotIdx].channelID == ^uint64(0) {
		m.mu.Unlock()
		return
	}
	// Find and stop any bridge using this slot
	var channelToStop uint64
	for ch, b := range m.activeBridges {
		if b.SlotIndex == slotIdx {
			channelToStop = ch
			break
		}
	}
	// Cancel any pending debounce timer for this channel
	if channelToStop != 0 {
		if timer := m.stopTimers[channelToStop]; timer != nil {
			timer.Stop()
			delete(m.stopTimers, channelToStop)
		}
	}
	// Mark slot as dead by setting channelID to max (not 0, which means "free")
	if slotIdx >= 0 && slotIdx < len(m.slots) {
		m.slots[slotIdx].channelID = ^uint64(0) // sentinel: dead slot
	}
	m.mu.Unlock()

	if channelToStop != 0 {
		m.stopBridgeForChannel(ctx, channelToStop)
	}

	m.logger.Warn("sidecar slot died", slog.Int("slot", slotIdx))
}

// Stats returns current bridge statistics.
func (m *Manager) Stats() (activeBridges, busySlots, totalSlots, trackedUsers int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	activeBridges = len(m.activeBridges)
	for _, s := range m.slots {
		if s.channelID != 0 && s.channelID != ^uint64(0) {
			busySlots++
		}
	}
	totalSlots = len(m.slots)
	trackedUsers = len(m.voiceStates)
	return
}

// ListRooms returns all known voice channel → Matrix room mappings.
func (m *Manager) ListRooms() map[uint64]types.RoomInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[uint64]types.RoomInfo, len(m.channelRooms))
	for ch, room := range m.channelRooms {
		info := m.channelInfos[ch]
		result[ch] = types.RoomInfo{Name: info.name, RoomID: room}
	}
	return result
}

// ListSlots returns the status of all sidecar slots.
func (m *Manager) ListSlots() []types.SlotInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]types.SlotInfo, len(m.slots))
	for i, s := range m.slots {
		si := types.SlotInfo{Index: i, Token: s.Token}
		switch {
		case s.channelID == ^uint64(0):
			si.Status = "dead"
		case s.channelID == 0:
			si.Status = "free"
		default:
			si.Status = "busy"
			si.ChannelID = s.channelID
			if info, ok := m.channelInfos[s.channelID]; ok {
				si.ChannelName = info.name
			}
		}
		result[i] = si
	}
	return result
}

// ManualJoin starts a bridge for a channel matched by name.
func (m *Manager) ManualJoin(ctx context.Context, name string) error {
	m.mu.Lock()
	var channelID uint64
	for ch, info := range m.channelInfos {
		if strings.EqualFold(info.name, name) {
			channelID = ch
			break
		}
	}
	m.mu.Unlock()
	if channelID == 0 {
		return fmt.Errorf("channel %q not found", name)
	}
	m.startBridge(ctx, channelID)
	return nil
}

// ManualLeave stops a bridge for a channel matched by name, or all if empty.
func (m *Manager) ManualLeave(ctx context.Context, name string) error {
	if name == "" {
		m.mu.Lock()
		channels := make([]uint64, 0, len(m.activeBridges))
		for ch := range m.activeBridges {
			channels = append(channels, ch)
		}
		m.mu.Unlock()
		for _, ch := range channels {
			m.stopBridgeForChannel(ctx, ch)
		}
		return nil
	}
	m.mu.Lock()
	var channelID uint64
	for ch, info := range m.channelInfos {
		if strings.EqualFold(info.name, name) {
			channelID = ch
			break
		}
	}
	m.mu.Unlock()
	if channelID == 0 {
		return fmt.Errorf("channel %q not found", name)
	}
	m.stopBridgeForChannel(ctx, channelID)
	return nil
}

// pushMatrixUsers sends the current list of Matrix user display names to the
// sidecar handling the given channel. The sidecar updates the bot's nickname.
func (m *Manager) pushMatrixUsers(channelID uint64) {
	m.mu.Lock()
	bridge, exists := m.activeBridges[channelID]
	users := m.matrixUsers[channelID]
	names := make([]string, 0, len(users))
	for _, name := range users {
		names = append(names, name)
	}
	m.mu.Unlock()

	if !exists || bridge == nil {
		return
	}

	// Build payload: count(2) + [nameLen(2) + name(utf8)]*
	size := 2
	for _, name := range names {
		size += 2 + len(name)
	}
	payload := make([]byte, size)
	binary.LittleEndian.PutUint16(payload[0:2], uint16(len(names)))
	offset := 2
	for _, name := range names {
		nameBytes := []byte(name)
		binary.LittleEndian.PutUint16(payload[offset:offset+2], uint16(len(nameBytes)))
		copy(payload[offset+2:], nameBytes)
		offset += 2 + len(nameBytes)
	}

	slot := m.slots[bridge.SlotIndex]
	if slot.Conn != nil {
		if err := slot.Conn.WriteMessage(&ipc.Message{
			Type:    ipc.MsgMatrixUsers,
			Payload: payload,
		}); err != nil {
			m.logger.Warn("failed to push matrix users to sidecar",
				slog.Int("slot", bridge.SlotIndex), slog.Any("err", err))
		}
	}
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

	// Cache for future lookups (both directions) + persist to DB
	m.mu.Lock()
	m.channelRooms[channelID] = roomID
	m.roomChannels[roomID] = channelID
	m.mu.Unlock()

	if m.store != nil {
		_ = m.store.UpsertRoom(store.Room{
			DiscordChannel: channelID,
			MatrixRoom:     string(roomID),
			Name:           info.name,
			GuildID:        m.guildID,
			CategoryID:     info.categoryID,
		})
	}

	m.logger.Info("created Matrix room",
		slog.String("matrix_room", string(roomID)),
		slog.String("name", roomName),
	)

	// Set bridge info state events for persistence and discoverability.
	// Follows mautrix-discord's convention so the room integrates cleanly.
	bridgeStateKey := fmt.Sprintf("fi.mau.discord://discord/%s/%d", m.guildID, channelID)
	botMXID := fmt.Sprintf("@discord_voice_bridge:%s", m.serverName)
	bridgeContent := map[string]interface{}{
		"bridgebot": botMXID,
		"creator":   botMXID,
		"protocol": map[string]interface{}{
			"id":           bridgeProtocolID,
			"displayname":  "Discord Voice",
			"external_url": "https://discord.com/",
		},
		"channel": map[string]interface{}{
			"id":          strconv.FormatUint(channelID, 10),
			"displayname": info.name,
		},
		"network": map[string]interface{}{
			"id": m.guildID,
		},
	}
	for _, evtType := range []string{"m.bridge", "uk.half-shot.bridge"} {
		if _, err := intent.SendStateEvent(ctx, roomID, event.Type{
			Type:  evtType,
			Class: event.StateEventType,
		}, bridgeStateKey, bridgeContent); err != nil {
			m.logger.Warn("failed to set bridge info",
				slog.String("type", evtType),
				slog.String("matrix_room", string(roomID)),
				slog.Any("err", err),
			)
		}
	}

	// Add to mautrix-discord's guild Space hierarchy
	m.addToGuildSpace(ctx, roomID, info.categoryID)

	return roomID, nil
}

// discoverGuildSpace finds mautrix-discord's guild Space by scanning the bridge bot's
// joined rooms for m.bridge state events matching this guild.
// State key format: fi.mau.discord://discord/{guild_id}
// Result is cached after first successful discovery.
func (m *Manager) discoverGuildSpace(ctx context.Context) (id.RoomID, error) {
	m.mu.Lock()
	if m.guildSpace != "" {
		cached := m.guildSpace
		m.mu.Unlock()
		return cached, nil
	}
	m.mu.Unlock()

	// Use mautrix-discord's bot user — it's joined to all bridged rooms including Spaces
	botMXID := id.UserID(fmt.Sprintf("@discordbot:%s", m.serverName))
	botIntent := m.signaller.Intent(botMXID)

	resp, err := botIntent.Client.JoinedRooms(ctx)
	if err != nil {
		return "", fmt.Errorf("list joined rooms as %s: %w", botMXID, err)
	}

	m.logger.Info("scanning for guild Space",
		slog.Int("rooms", len(resp.JoinedRooms)),
		slog.String("guild", m.guildID),
	)

	bridgeEventType := event.Type{Type: "m.bridge", Class: event.StateEventType}
	stateKey := fmt.Sprintf("fi.mau.discord://discord/%s", m.guildID)

	for _, roomID := range resp.JoinedRooms {
		var bridgeInfo map[string]interface{}
		if err := botIntent.StateEvent(ctx, roomID, bridgeEventType, stateKey, &bridgeInfo); err != nil {
			continue
		}
		// Found a room with matching bridge info — verify it's a Space
		var createContent map[string]interface{}
		if err := botIntent.StateEvent(ctx, roomID, event.Type{Type: "m.room.create", Class: event.StateEventType}, "", &createContent); err != nil {
			m.logger.Warn("room has bridge info but can't read create event",
				slog.String("matrix_room", string(roomID)),
				slog.Any("err", err),
			)
			continue
		}
		if createContent["type"] != "m.space" {
			m.logger.Debug("room has bridge info but is not a Space",
				slog.String("matrix_room", string(roomID)),
			)
			continue
		}
		m.mu.Lock()
		m.guildSpace = roomID
		m.mu.Unlock()
		m.logger.Info("discovered guild Space",
			slog.String("matrix_room", string(roomID)),
			slog.String("guild", m.guildID),
		)
		return roomID, nil
	}

	return "", fmt.Errorf("guild Space not found for guild %s", m.guildID)
}

// discoverCategorySpaces finds mautrix-discord's category sub-Spaces by scanning
// children of the guild Space. Each category sub-Space has m.bridge state key
// fi.mau.discord://discord/{guild_id}/{category_id}.
// Must be called after discoverGuildSpace succeeds.
func (m *Manager) discoverCategorySpaces(ctx context.Context) {
	m.mu.Lock()
	gs := m.guildSpace
	m.mu.Unlock()
	if gs == "" {
		return
	}

	botMXID := id.UserID(fmt.Sprintf("@discordbot:%s", m.serverName))
	botIntent := m.signaller.Intent(botMXID)

	// Get guild Space state to find children
	stateMap, err := botIntent.Client.State(ctx, gs)
	if err != nil {
		m.logger.Warn("could not get guild Space state for category discovery",
			slog.Any("err", err),
		)
		return
	}

	spaceChildType := event.Type{Type: "m.space.child", Class: event.StateEventType}
	children, ok := stateMap[spaceChildType]
	if !ok {
		return
	}

	bridgeEventType := event.Type{Type: "m.bridge", Class: event.StateEventType}
	createEventType := event.Type{Type: "m.room.create", Class: event.StateEventType}
	prefix := fmt.Sprintf("fi.mau.discord://discord/%s/", m.guildID)

	for childRoomIDStr := range children {
		childRoomID := id.RoomID(childRoomIDStr)

		childState, err := botIntent.Client.State(ctx, childRoomID)
		if err != nil {
			continue
		}

		// Check if it's a Space
		createEvents, ok := childState[createEventType]
		if !ok {
			continue
		}
		createEvt, ok := createEvents[""]
		if !ok || createEvt.Content.Raw == nil || createEvt.Content.Raw["type"] != "m.space" {
			continue
		}

		// Check for bridge info matching our guild
		bridges, ok := childState[bridgeEventType]
		if !ok {
			continue
		}
		for stateKey := range bridges {
			if !strings.HasPrefix(stateKey, prefix) {
				continue
			}
			catIDStr := strings.TrimPrefix(stateKey, prefix)
			catID, err := strconv.ParseUint(catIDStr, 10, 64)
			if err != nil {
				continue
			}
			m.mu.Lock()
			m.categorySpaces[catID] = childRoomID
			m.mu.Unlock()
			m.logger.Info("discovered category Space",
				slog.Uint64("category", catID),
				slog.String("matrix_room", string(childRoomID)),
			)
		}
	}

	m.mu.Lock()
	count := len(m.categorySpaces)
	m.mu.Unlock()
	m.logger.Info("category discovery complete", slog.Int("found", count))
}

// addToGuildSpace adds a voice room to mautrix-discord's Space hierarchy.
// Places it in the category sub-Space if available, otherwise the guild Space.
// Uses the @discordbot intent for Space modifications (it has the required power level).
func (m *Manager) addToGuildSpace(ctx context.Context, roomID id.RoomID, categoryID uint64) {
	guildSpace, err := m.discoverGuildSpace(ctx)
	if err != nil {
		m.logger.Warn("could not find guild Space — room not added to hierarchy",
			slog.Any("err", err),
		)
		return
	}

	// Discover category sub-Spaces if not done yet
	m.mu.Lock()
	needCategoryDiscovery := len(m.categorySpaces) == 0
	m.mu.Unlock()
	if needCategoryDiscovery {
		m.discoverCategorySpaces(ctx)
	}

	// Prefer category sub-Space, fall back to guild Space
	targetSpace := guildSpace
	if categoryID != 0 {
		m.mu.Lock()
		catSpace, ok := m.categorySpaces[categoryID]
		m.mu.Unlock()
		if ok {
			targetSpace = catSpace
		}
	}

	// Use mautrix-discord's bot for Space state events (it has power level)
	botMXID := id.UserID(fmt.Sprintf("@discordbot:%s", m.serverName))
	botIntent := m.signaller.Intent(botMXID)

	// Add room as child of the target Space
	_, err = botIntent.SendStateEvent(ctx, targetSpace, event.Type{
		Type:  "m.space.child",
		Class: event.StateEventType,
	}, string(roomID), map[string]interface{}{
		"via": []string{m.serverName},
	})
	if err != nil {
		m.logger.Warn("failed to add room to Space",
			slog.String("matrix_room", string(roomID)),
			slog.String("space", string(targetSpace)),
			slog.Any("err", err),
		)
		return
	}

	// Set m.space.parent on the voice room (our bot created it, so it has power)
	voiceBotIntent := m.signaller.BotIntent()
	_, err = voiceBotIntent.SendStateEvent(ctx, roomID, event.Type{
		Type:  "m.space.parent",
		Class: event.StateEventType,
	}, string(targetSpace), map[string]interface{}{
		"via": []string{m.serverName},
	})
	if err != nil {
		m.logger.Warn("failed to set parent Space on voice room",
			slog.String("matrix_room", string(roomID)),
			slog.Any("err", err),
		)
	}

	m.logger.Info("added room to Space",
		slog.String("matrix_room", string(roomID)),
		slog.String("space", string(targetSpace)),
		slog.Uint64("category", categoryID),
	)
}

// DiscoverExistingRooms scans the voice bridge bot's joined rooms for rooms
// previously created by this bridge. Rebuilds the channelRooms cache from
// m.bridge state events so rooms survive restarts without a database.
// Call before processing any CHANNEL_LIST messages from the sidecar.
func (m *Manager) DiscoverExistingRooms(ctx context.Context) error {
	intent := m.signaller.BotIntent()
	if err := intent.EnsureRegistered(ctx); err != nil {
		m.logger.Debug("bot registration (may already exist)", slog.Any("err", err))
	}

	// Try DB first — fast path
	if m.store != nil {
		rooms, err := m.store.ListRooms(m.guildID)
		if err == nil && len(rooms) > 0 {
			m.mu.Lock()
			for _, r := range rooms {
				roomID := id.RoomID(r.MatrixRoom)
				m.channelRooms[r.DiscordChannel] = roomID
				m.roomChannels[roomID] = r.DiscordChannel
				if r.Name != "" {
					m.channelInfos[r.DiscordChannel] = channelInfo{name: r.Name, categoryID: r.CategoryID}
				}
			}
			m.mu.Unlock()
			m.logger.Info("loaded rooms from database", slog.Int("found", len(rooms)))
			return nil
		}
	}

	// Fallback: scan Matrix state events (slow, parallel)
	m.logger.Info("no rooms in database, scanning Matrix state events")
	resp, err := intent.Client.JoinedRooms(ctx)
	if err != nil {
		return fmt.Errorf("list joined rooms: %w", err)
	}

	bridgeEventType := event.Type{Type: "m.bridge", Class: event.StateEventType}
	prefix := fmt.Sprintf("fi.mau.discord://discord/%s/", m.guildID)

	type result struct {
		channelID uint64
		roomID    id.RoomID
	}

	results := make(chan result, len(resp.JoinedRooms))
	sem := make(chan struct{}, 10)
	var wg sync.WaitGroup

	for _, roomID := range resp.JoinedRooms {
		wg.Add(1)
		go func(rid id.RoomID) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			stateMap, err := intent.Client.State(ctx, rid)
			if err != nil {
				return
			}
			bridges, ok := stateMap[bridgeEventType]
			if !ok {
				return
			}
			for stateKey, evt := range bridges {
				if !strings.HasPrefix(stateKey, prefix) {
					continue
				}
				raw := evt.Content.Raw
				if raw == nil {
					continue
				}
				protocol, _ := raw["protocol"].(map[string]interface{})
				if protocol == nil || protocol["id"] != bridgeProtocolID {
					continue
				}
				channel, _ := raw["channel"].(map[string]interface{})
				if channel == nil {
					continue
				}
				chIDStr, _ := channel["id"].(string)
				chID, err := strconv.ParseUint(chIDStr, 10, 64)
				if err != nil {
					continue
				}
				results <- result{channelID: chID, roomID: rid}
			}
		}(roomID)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	discovered := 0
	for r := range results {
		m.mu.Lock()
		m.channelRooms[r.channelID] = r.roomID
		m.roomChannels[r.roomID] = r.channelID
		m.mu.Unlock()
		discovered++

		// Persist to DB for next startup
		if m.store != nil {
			_ = m.store.UpsertRoom(store.Room{
				DiscordChannel: r.channelID,
				MatrixRoom:     string(r.roomID),
				GuildID:        m.guildID,
			})
		}
	}

	m.logger.Info("room discovery complete (Matrix scan)", slog.Int("found", discovered))
	return nil
}
