package bridge

import (
	"testing"

	"maunium.net/go/mautrix/id"
)

func TestBestChannel(t *testing.T) {
	m := &Manager{
		voiceStates: make(map[uint64]uint64),
	}

	tests := []struct {
		name      string
		states    map[uint64]uint64
		wantCh    uint64
		wantCount int
	}{
		{
			name:      "empty",
			states:    map[uint64]uint64{},
			wantCh:    0,
			wantCount: 0,
		},
		{
			name:      "one user one channel",
			states:    map[uint64]uint64{100: 500},
			wantCh:    500,
			wantCount: 1,
		},
		{
			name:      "three users same channel",
			states:    map[uint64]uint64{100: 500, 101: 500, 102: 500},
			wantCh:    500,
			wantCount: 3,
		},
		{
			name:      "two channels pick larger",
			states:    map[uint64]uint64{100: 500, 101: 500, 102: 600},
			wantCh:    500,
			wantCount: 2,
		},
		{
			name:      "two channels equal picks one",
			states:    map[uint64]uint64{100: 500, 101: 600},
			wantCh:    0, // either is valid
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m.voiceStates = tt.states
			gotCh, gotCount := m.bestChannel()

			if gotCount != tt.wantCount {
				t.Errorf("count = %d, want %d", gotCount, tt.wantCount)
			}
			if tt.wantCh != 0 && gotCh != tt.wantCh {
				t.Errorf("channel = %d, want %d", gotCh, tt.wantCh)
			}
			if tt.wantCount > 0 && gotCh == 0 {
				t.Error("expected non-zero channel")
			}
		})
	}
}

func TestVoiceStateTracking(t *testing.T) {
	m := &Manager{
		voiceStates:  make(map[uint64]uint64),
		channelInfos: make(map[uint64]channelInfo),
		channelRooms: make(map[uint64]id.RoomID),
	}

	// User joins channel 500
	m.voiceStates[100] = 500
	ch, count := m.bestChannel()
	if ch != 500 || count != 1 {
		t.Errorf("after join: ch=%d count=%d, want 500/1", ch, count)
	}

	// Second user joins same channel
	m.voiceStates[101] = 500
	ch, count = m.bestChannel()
	if ch != 500 || count != 2 {
		t.Errorf("after second join: ch=%d count=%d, want 500/2", ch, count)
	}

	// First user leaves
	delete(m.voiceStates, 100)
	ch, count = m.bestChannel()
	if ch != 500 || count != 1 {
		t.Errorf("after leave: ch=%d count=%d, want 500/1", ch, count)
	}

	// Last user leaves
	delete(m.voiceStates, 101)
	ch, count = m.bestChannel()
	if ch != 0 || count != 0 {
		t.Errorf("after all leave: ch=%d count=%d, want 0/0", ch, count)
	}
}

func TestChannelInfoParsing(t *testing.T) {
	info := channelInfo{name: "General Voice", categoryID: 12345}

	if info.name != "General Voice" {
		t.Errorf("name = %q, want %q", info.name, "General Voice")
	}
	if info.categoryID != 12345 {
		t.Errorf("categoryID = %d, want %d", info.categoryID, 12345)
	}
}
