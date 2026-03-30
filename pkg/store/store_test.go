package store

import (
	"path/filepath"
	"testing"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestOpenAndMigrate(t *testing.T) {
	s := newTestStore(t)
	if s == nil {
		t.Fatal("store is nil")
	}
}

func TestBotCRUD(t *testing.T) {
	s := newTestStore(t)

	// Add
	id1, err := s.AddBot("token-1", "guild-1")
	if err != nil {
		t.Fatalf("AddBot: %v", err)
	}
	if id1 == 0 {
		t.Error("expected non-zero bot ID")
	}

	// Add duplicate — should error
	_, err = s.AddBot("token-1", "guild-1")
	if err == nil {
		t.Error("expected error on duplicate token")
	}

	// Add second bot
	id2, err := s.AddBot("token-2", "guild-1")
	if err != nil {
		t.Fatalf("AddBot 2: %v", err)
	}

	// List
	bots, err := s.ListBots("guild-1")
	if err != nil {
		t.Fatalf("ListBots: %v", err)
	}
	if len(bots) != 2 {
		t.Fatalf("expected 2 bots, got %d", len(bots))
	}

	// AllActive
	active, err := s.AllActiveBots()
	if err != nil {
		t.Fatalf("AllActiveBots: %v", err)
	}
	if len(active) != 2 {
		t.Fatalf("expected 2 active, got %d", len(active))
	}

	// Remove
	if err := s.RemoveBot(id2); err != nil {
		t.Fatalf("RemoveBot: %v", err)
	}
	bots, _ = s.ListBots("guild-1")
	if len(bots) != 1 {
		t.Errorf("expected 1 bot after remove, got %d", len(bots))
	}

	// List wrong guild
	bots, _ = s.ListBots("guild-999")
	if len(bots) != 0 {
		t.Errorf("expected 0 bots for unknown guild, got %d", len(bots))
	}

	_ = id1
}

func TestRoomCRUD(t *testing.T) {
	s := newTestStore(t)

	r := Room{
		DiscordChannel: 12345,
		MatrixRoom:     "!abc:server",
		Name:           "General",
		GuildID:        "guild-1",
		CategoryID:     100,
	}

	// Upsert
	if err := s.UpsertRoom(r); err != nil {
		t.Fatalf("UpsertRoom: %v", err)
	}

	// Get
	got, err := s.GetRoom(12345)
	if err != nil {
		t.Fatalf("GetRoom: %v", err)
	}
	if got == nil {
		t.Fatal("expected room, got nil")
	}
	if got.Name != "General" || got.MatrixRoom != "!abc:server" {
		t.Errorf("room = %+v, want name=General room=!abc:server", got)
	}

	// Upsert update
	r.Name = "Updated"
	if err := s.UpsertRoom(r); err != nil {
		t.Fatalf("UpsertRoom update: %v", err)
	}
	got, _ = s.GetRoom(12345)
	if got.Name != "Updated" {
		t.Errorf("name = %q, want Updated", got.Name)
	}

	// List
	rooms, err := s.ListRooms("guild-1")
	if err != nil {
		t.Fatalf("ListRooms: %v", err)
	}
	if len(rooms) != 1 {
		t.Errorf("expected 1 room, got %d", len(rooms))
	}

	// Get nonexistent
	got, err = s.GetRoom(99999)
	if err != nil {
		t.Fatalf("GetRoom nonexistent: %v", err)
	}
	if got != nil {
		t.Error("expected nil for nonexistent room")
	}

	// Delete
	if err := s.DeleteRoom(12345); err != nil {
		t.Fatalf("DeleteRoom: %v", err)
	}
	rooms, _ = s.ListRooms("guild-1")
	if len(rooms) != 0 {
		t.Errorf("expected 0 rooms after delete, got %d", len(rooms))
	}
}

func TestGuildUpsert(t *testing.T) {
	s := newTestStore(t)

	if err := s.UpsertGuild("guild-1", "!space:server"); err != nil {
		t.Fatalf("UpsertGuild: %v", err)
	}

	// Upsert again — should not error
	if err := s.UpsertGuild("guild-1", "!space2:server"); err != nil {
		t.Fatalf("UpsertGuild update: %v", err)
	}
}

func TestMultipleRooms(t *testing.T) {
	s := newTestStore(t)

	for i := uint64(1); i <= 10; i++ {
		if err := s.UpsertRoom(Room{
			DiscordChannel: i,
			MatrixRoom:     "!room:server",
			Name:           "Room",
			GuildID:        "guild-1",
		}); err != nil {
			t.Fatalf("UpsertRoom %d: %v", i, err)
		}
	}

	rooms, _ := s.ListRooms("guild-1")
	if len(rooms) != 10 {
		t.Errorf("expected 10 rooms, got %d", len(rooms))
	}
}
