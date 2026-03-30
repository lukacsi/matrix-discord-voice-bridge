package bridge

import (
	"encoding/binary"
	"log/slog"
	"os"
	"sync"
	"testing"

	"maunium.net/go/mautrix/id"

	"github.com/lukacsi/livekit-discord-bridge/pkg/ipc"
)

func newTestManager() *Manager {
	return &Manager{
		logger:         slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		guildID:        "298535878702137344",
		serverName:     "example.com",
		voiceStates:    make(map[uint64]uint64),
		channelInfos:   make(map[uint64]channelInfo),
		channelRooms:   make(map[uint64]id.RoomID),
		roomChannels:   make(map[id.RoomID]uint64),
		categorySpaces: make(map[uint64]id.RoomID),
		activeBridges:  make(map[uint64]*ChannelBridge),
	}
}

// --- Voice state tracking ---

func TestVoiceStateTracking(t *testing.T) {
	m := newTestManager()

	m.voiceStates[100] = 500
	if m.voiceStates[100] != 500 {
		t.Error("expected user 100 in channel 500")
	}

	m.voiceStates[101] = 500
	count := 0
	for _, ch := range m.voiceStates {
		if ch == 500 {
			count++
		}
	}
	if count != 2 {
		t.Errorf("expected 2 users in channel 500, got %d", count)
	}

	delete(m.voiceStates, 100)
	if _, ok := m.voiceStates[100]; ok {
		t.Error("user 100 should be gone")
	}

	delete(m.voiceStates, 101)
	if len(m.voiceStates) != 0 {
		t.Errorf("expected empty voice states, got %d", len(m.voiceStates))
	}
}

func TestVoiceStateUserSwitchesChannels(t *testing.T) {
	m := newTestManager()

	m.voiceStates[100] = 500
	m.voiceStates[100] = 600

	if m.voiceStates[100] != 600 {
		t.Errorf("expected user 100 in channel 600, got %d", m.voiceStates[100])
	}

	count500 := 0
	for _, ch := range m.voiceStates {
		if ch == 500 {
			count500++
		}
	}
	if count500 != 0 {
		t.Errorf("channel 500 should have 0 users, got %d", count500)
	}
}

// --- Room/channel mapping ---

func TestRoomChannelMapping(t *testing.T) {
	m := newTestManager()
	roomID := id.RoomID("!test:server")
	channelID := uint64(12345)

	m.channelRooms[channelID] = roomID
	m.roomChannels[roomID] = channelID

	if m.channelRooms[channelID] != roomID {
		t.Errorf("channel→room: got %s, want %s", m.channelRooms[channelID], roomID)
	}
	if m.roomChannels[roomID] != channelID {
		t.Errorf("room→channel: got %d, want %d", m.roomChannels[roomID], channelID)
	}
}

func TestRoomChannelMappingMultiple(t *testing.T) {
	m := newTestManager()

	rooms := map[uint64]id.RoomID{
		100: "!room1:server",
		200: "!room2:server",
		300: "!room3:server",
	}

	for ch, room := range rooms {
		m.channelRooms[ch] = room
		m.roomChannels[room] = ch
	}

	for ch, room := range rooms {
		if m.channelRooms[ch] != room {
			t.Errorf("channel %d: got room %s, want %s", ch, m.channelRooms[ch], room)
		}
		if m.roomChannels[room] != ch {
			t.Errorf("room %s: got channel %d, want %d", room, m.roomChannels[room], ch)
		}
	}
}

// --- OnMatrixCallMember ---

func TestOnMatrixCallMemberIgnoresDiscordUsers(t *testing.T) {
	m := newTestManager()
	roomID := id.RoomID("!test:server")
	m.roomChannels[roomID] = 500
	m.channelRooms[500] = roomID

	m.OnMatrixCallMember(nil, roomID, "@discord_123:server", true)

	m.mu.Lock()
	count := len(m.activeBridges)
	m.mu.Unlock()

	if count != 0 {
		t.Error("discord ghost user should not trigger bridge")
	}
}

func TestOnMatrixCallMemberIgnoresUnknownRoom(t *testing.T) {
	m := newTestManager()

	// Room not in roomChannels — should be a no-op
	m.OnMatrixCallMember(nil, "!unknown:server", "@oger:server", true)

	m.mu.Lock()
	count := len(m.activeBridges)
	m.mu.Unlock()

	if count != 0 {
		t.Error("unknown room should not trigger bridge")
	}
}

func TestOnMatrixCallMemberVariousDiscordPatterns(t *testing.T) {
	m := newTestManager()
	roomID := id.RoomID("!test:server")
	m.roomChannels[roomID] = 500
	m.channelRooms[500] = roomID

	discordUsers := []string{
		"@discord_123:server",
		"@discord_voice_bridge:server",
		"@discord_0:server",
	}

	for _, user := range discordUsers {
		m.OnMatrixCallMember(nil, roomID, user, true)
		m.mu.Lock()
		count := len(m.activeBridges)
		m.mu.Unlock()
		if count != 0 {
			t.Errorf("discord user %q should not trigger bridge", user)
		}
	}
}

// --- HandleMessage IPC parsing ---

func TestHandleMessageVoiceState(t *testing.T) {
	m := newTestManager()

	// Build voice state payload: channelID(8) + categoryID(8) + nameLen(2) + name
	name := "General"
	payload := make([]byte, 18+len(name))
	binary.LittleEndian.PutUint64(payload[0:8], 500)
	binary.LittleEndian.PutUint64(payload[8:16], 100)
	binary.LittleEndian.PutUint16(payload[16:18], uint16(len(name)))
	copy(payload[18:], name)

	msg := &ipc.Message{
		Type:    ipc.MsgVoiceState,
		UserID:  42,
		Payload: payload,
	}

	handled := m.HandleMessage(nil, 0, msg)
	if !handled {
		t.Error("MsgVoiceState should be handled")
	}

	m.mu.Lock()
	info := m.channelInfos[500]
	state := m.voiceStates[42]
	m.mu.Unlock()

	if info.name != "General" {
		t.Errorf("channel name = %q, want %q", info.name, "General")
	}
	if info.categoryID != 100 {
		t.Errorf("categoryID = %d, want %d", info.categoryID, 100)
	}
	if state != 500 {
		t.Errorf("voice state = %d, want %d", state, 500)
	}
}

func TestHandleMessageVoiceStateLeave(t *testing.T) {
	m := newTestManager()
	// Don't set a prior voice state — avoids signaller.LeaveCall call
	// Just verify the state tracking for a join then manual removal

	// First join channel 500 (no signaller needed — no room mapped)
	joinPayload := make([]byte, 18)
	binary.LittleEndian.PutUint64(joinPayload[0:8], 500)
	m.HandleMessage(nil, 0, &ipc.Message{
		Type:    ipc.MsgVoiceState,
		UserID:  42,
		Payload: joinPayload,
	})

	m.mu.Lock()
	ch := m.voiceStates[42]
	m.mu.Unlock()
	if ch != 500 {
		t.Fatalf("expected user in channel 500, got %d", ch)
	}

	// Leave: channelID = 0 — no oldChannel→newChannel transition needing signaller
	// because no room is mapped for channel 500
	leavePayload := make([]byte, 8)
	binary.LittleEndian.PutUint64(leavePayload, 0)
	m.HandleMessage(nil, 0, &ipc.Message{
		Type:    ipc.MsgVoiceState,
		UserID:  42,
		Payload: leavePayload,
	})

	m.mu.Lock()
	_, still := m.voiceStates[42]
	m.mu.Unlock()

	if still {
		t.Error("user should have been removed from voice states")
	}
}

func TestChannelListPayloadParsing(t *testing.T) {
	tests := []struct {
		name       string
		channelID  uint64
		categoryID uint64
		chanName   string
	}{
		{"basic", 700, 200, "Gaming VC"},
		{"unicode", 800, 300, "Hangszórók 🎤"},
		{"empty name", 900, 0, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nameBuf := []byte(tt.chanName)
			payload := make([]byte, 18+len(nameBuf))
			binary.LittleEndian.PutUint64(payload[0:8], tt.channelID)
			binary.LittleEndian.PutUint64(payload[8:16], tt.categoryID)
			binary.LittleEndian.PutUint16(payload[16:18], uint16(len(nameBuf)))
			copy(payload[18:], nameBuf)

			// Parse the same way HandleMessage does
			channelID := binary.LittleEndian.Uint64(payload[0:8])
			categoryID := binary.LittleEndian.Uint64(payload[8:16])
			nameLen := binary.LittleEndian.Uint16(payload[16:18])
			var parsedName string
			if len(payload) >= 18+int(nameLen) {
				parsedName = string(payload[18 : 18+nameLen])
			}

			if channelID != tt.channelID {
				t.Errorf("channelID = %d, want %d", channelID, tt.channelID)
			}
			if categoryID != tt.categoryID {
				t.Errorf("categoryID = %d, want %d", categoryID, tt.categoryID)
			}
			if parsedName != tt.chanName {
				t.Errorf("name = %q, want %q", parsedName, tt.chanName)
			}
		})
	}
}

func TestHandleMessageChannelListShortPayload(t *testing.T) {
	m := newTestManager()

	// Payload too short — should be handled but no-op
	msg := &ipc.Message{
		Type:    ipc.MsgChannelList,
		Payload: make([]byte, 10), // less than 18
	}

	handled := m.HandleMessage(nil, 0, msg)
	if !handled {
		t.Error("MsgChannelList should be handled even with short payload")
	}

	m.mu.Lock()
	count := len(m.channelInfos)
	m.mu.Unlock()

	if count != 0 {
		t.Errorf("short payload should not add channel info, got %d", count)
	}
}

func TestHandleMessageUnknownType(t *testing.T) {
	m := newTestManager()

	msg := &ipc.Message{Type: 0xFF}
	if m.HandleMessage(nil, 0, msg) {
		t.Error("unknown message type should not be handled")
	}
}

func TestHandleMessageUserJoinNoBridge(t *testing.T) {
	m := newTestManager()

	msg := &ipc.Message{
		Type:   ipc.MsgUserJoin,
		UserID: 42,
	}

	// No active bridge — should be handled but no-op
	handled := m.HandleMessage(nil, 0, msg)
	if !handled {
		t.Error("MsgUserJoin should be handled")
	}
}

func TestHandleMessageUserJoinWithBridge(t *testing.T) {
	m := newTestManager()
	m.slots = []*SidecarSlot{{Index: 0}}
	b := &ChannelBridge{
		ChannelID:   500,
		SlotIndex:   0,
		JoinedUsers: make(map[uint64]bool),
	}
	m.activeBridges[500] = b

	msg := &ipc.Message{
		Type:   ipc.MsgUserJoin,
		UserID: 42,
	}

	m.HandleMessage(nil, 0, msg)

	b.mu.Lock()
	joined := b.JoinedUsers[42]
	b.mu.Unlock()

	if !joined {
		t.Error("user 42 should be tracked in bridge")
	}
}

// --- Concurrency ---

func TestVoiceStateConcurrency(t *testing.T) {
	m := newTestManager()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(userID uint64) {
			defer wg.Done()
			m.mu.Lock()
			m.voiceStates[userID] = userID % 5
			m.mu.Unlock()

			m.mu.Lock()
			delete(m.voiceStates, userID)
			m.mu.Unlock()
		}(uint64(i))
	}
	wg.Wait()

	if len(m.voiceStates) != 0 {
		t.Errorf("expected empty voice states after concurrent ops, got %d", len(m.voiceStates))
	}
}

func TestRoomChannelMappingConcurrency(t *testing.T) {
	m := newTestManager()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i uint64) {
			defer wg.Done()
			room := id.RoomID("!room" + string(rune('0'+i%10)) + ":server")
			m.mu.Lock()
			m.channelRooms[i] = room
			m.roomChannels[room] = i
			m.mu.Unlock()
		}(uint64(i))
	}
	wg.Wait()
}

// --- Channel info ---

func TestChannelInfoParsing(t *testing.T) {
	info := channelInfo{name: "General Voice", categoryID: 12345}

	if info.name != "General Voice" {
		t.Errorf("name = %q, want %q", info.name, "General Voice")
	}
	if info.categoryID != 12345 {
		t.Errorf("categoryID = %d, want %d", info.categoryID, 12345)
	}
}

func TestChannelInfoEmptyName(t *testing.T) {
	info := channelInfo{categoryID: 999}
	if info.name != "" {
		t.Errorf("name = %q, want empty", info.name)
	}
}

// --- stopBridgeForChannel ---

func TestStopBridgeForChannelWrongChannel(t *testing.T) {
	m := newTestManager()
	m.activeBridges[500] = &ChannelBridge{ChannelID: 500, SlotIndex: -1, JoinedUsers: make(map[uint64]bool)}

	m.stopBridgeForChannel(nil, 600)

	m.mu.Lock()
	_, still := m.activeBridges[500]
	m.mu.Unlock()

	if !still {
		t.Error("bridge for 500 should still be active")
	}
}

func TestStopBridgeForChannelCorrectChannel(t *testing.T) {
	m := newTestManager()
	m.activeBridges[500] = &ChannelBridge{ChannelID: 500, SlotIndex: -1, JoinedUsers: make(map[uint64]bool)}

	m.stopBridgeForChannel(nil, 500)

	m.mu.Lock()
	count := len(m.activeBridges)
	m.mu.Unlock()

	if count != 0 {
		t.Errorf("expected 0 active bridges, got %d", count)
	}
}

func TestStopBridgeForChannelNoBridge(t *testing.T) {
	m := newTestManager()
	m.stopBridgeForChannel(nil, 500)
}
