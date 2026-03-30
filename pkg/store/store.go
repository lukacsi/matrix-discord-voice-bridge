// Package store provides SQLite persistence for the bridge.
package store

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

// Store wraps a SQLite database for bridge state persistence.
type Store struct {
	db *sql.DB
}

// Open opens or creates the SQLite database at the given path.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

// Close closes the database.
func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) migrate() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS bots (
			id       INTEGER PRIMARY KEY AUTOINCREMENT,
			token    TEXT NOT NULL UNIQUE,
			guild_id TEXT NOT NULL,
			active   BOOLEAN NOT NULL DEFAULT 1
		);

		CREATE TABLE IF NOT EXISTS rooms (
			discord_channel INTEGER PRIMARY KEY,
			matrix_room     TEXT NOT NULL,
			name            TEXT NOT NULL DEFAULT '',
			guild_id        TEXT NOT NULL,
			category_id     INTEGER NOT NULL DEFAULT 0
		);

		CREATE TABLE IF NOT EXISTS guilds (
			guild_id   TEXT PRIMARY KEY,
			space_room TEXT NOT NULL DEFAULT '',
			last_sync  DATETIME
		);
	`)
	return err
}

// --- Bot operations ---

// Bot represents a stored Discord bot token.
type Bot struct {
	ID      int64
	Token   string
	GuildID string
	Active  bool
}

// AddBot inserts a new bot token. Returns the bot ID.
func (s *Store) AddBot(token, guildID string) (int64, error) {
	res, err := s.db.Exec("INSERT INTO bots (token, guild_id) VALUES (?, ?)", token, guildID)
	if err != nil {
		return 0, fmt.Errorf("add bot: %w", err)
	}
	return res.LastInsertId()
}

// RemoveBot deletes a bot by ID.
func (s *Store) RemoveBot(id int64) error {
	_, err := s.db.Exec("DELETE FROM bots WHERE id = ?", id)
	return err
}

// ListBots returns all bots for a guild.
func (s *Store) ListBots(guildID string) ([]Bot, error) {
	rows, err := s.db.Query("SELECT id, token, guild_id, active FROM bots WHERE guild_id = ?", guildID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var bots []Bot
	for rows.Next() {
		var b Bot
		if err := rows.Scan(&b.ID, &b.Token, &b.GuildID, &b.Active); err != nil {
			return nil, err
		}
		bots = append(bots, b)
	}
	return bots, rows.Err()
}

// AllActiveBots returns all active bot tokens across all guilds.
func (s *Store) AllActiveBots() ([]Bot, error) {
	rows, err := s.db.Query("SELECT id, token, guild_id, active FROM bots WHERE active = 1")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var bots []Bot
	for rows.Next() {
		var b Bot
		if err := rows.Scan(&b.ID, &b.Token, &b.GuildID, &b.Active); err != nil {
			return nil, err
		}
		bots = append(bots, b)
	}
	return bots, rows.Err()
}

// --- Room operations ---

// Room represents a stored voice room mapping.
type Room struct {
	DiscordChannel uint64
	MatrixRoom     string
	Name           string
	GuildID        string
	CategoryID     uint64
}

// UpsertRoom inserts or updates a room mapping.
func (s *Store) UpsertRoom(r Room) error {
	_, err := s.db.Exec(`
		INSERT INTO rooms (discord_channel, matrix_room, name, guild_id, category_id)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(discord_channel) DO UPDATE SET
			matrix_room = excluded.matrix_room,
			name = excluded.name,
			category_id = excluded.category_id
	`, r.DiscordChannel, r.MatrixRoom, r.Name, r.GuildID, r.CategoryID)
	return err
}

// ListRooms returns all rooms for a guild.
func (s *Store) ListRooms(guildID string) ([]Room, error) {
	rows, err := s.db.Query("SELECT discord_channel, matrix_room, name, guild_id, category_id FROM rooms WHERE guild_id = ?", guildID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var rooms []Room
	for rows.Next() {
		var r Room
		if err := rows.Scan(&r.DiscordChannel, &r.MatrixRoom, &r.Name, &r.GuildID, &r.CategoryID); err != nil {
			return nil, err
		}
		rooms = append(rooms, r)
	}
	return rooms, rows.Err()
}

// GetRoom returns a room by Discord channel ID.
func (s *Store) GetRoom(channelID uint64) (*Room, error) {
	r := &Room{}
	err := s.db.QueryRow("SELECT discord_channel, matrix_room, name, guild_id, category_id FROM rooms WHERE discord_channel = ?", channelID).
		Scan(&r.DiscordChannel, &r.MatrixRoom, &r.Name, &r.GuildID, &r.CategoryID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return r, nil
}

// DeleteRoom removes a room mapping.
func (s *Store) DeleteRoom(channelID uint64) error {
	_, err := s.db.Exec("DELETE FROM rooms WHERE discord_channel = ?", channelID)
	return err
}

// --- Guild operations ---

// UpsertGuild inserts or updates a guild record.
func (s *Store) UpsertGuild(guildID, spaceRoom string) error {
	_, err := s.db.Exec(`
		INSERT INTO guilds (guild_id, space_room, last_sync)
		VALUES (?, ?, datetime('now'))
		ON CONFLICT(guild_id) DO UPDATE SET
			space_room = excluded.space_room,
			last_sync = datetime('now')
	`, guildID, spaceRoom)
	return err
}
