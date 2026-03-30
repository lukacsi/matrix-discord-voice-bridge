package config

import (
	"os"
	"path/filepath"
	"testing"
)

func fullConfig() string {
	return `discord:
  bot_token: "test-token"
  guild_id: "123456"
livekit:
  url: "wss://lk.example.com"
  api_key: "key"
  api_secret: "secret"
matrix:
  homeserver_url: "https://matrix.example.com"
  as_token: "as-token"
  server_name: "example.com"
  lk_jwt_service_url: "https://lk-jwt.example.com"
`
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadValidConfig(t *testing.T) {
	path := writeConfig(t, fullConfig())
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Discord.BotToken != "test-token" {
		t.Errorf("bot_token = %q, want %q", cfg.Discord.BotToken, "test-token")
	}
	if cfg.Discord.GuildID != "123456" {
		t.Errorf("guild_id = %q, want %q", cfg.Discord.GuildID, "123456")
	}
	if cfg.Matrix.ServerName != "example.com" {
		t.Errorf("server_name = %q, want %q", cfg.Matrix.ServerName, "example.com")
	}
}

func TestLoadSidecarDefaults(t *testing.T) {
	path := writeConfig(t, fullConfig())
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Sidecar.Dir != "sidecar" {
		t.Errorf("sidecar.dir = %q, want %q", cfg.Sidecar.Dir, "sidecar")
	}
	if cfg.Sidecar.SocketPath != "/tmp/discord-voice-bridge.sock" {
		t.Errorf("sidecar.socket_path = %q", cfg.Sidecar.SocketPath)
	}
}

func TestLoadEnvOverride(t *testing.T) {
	path := writeConfig(t, fullConfig())
	t.Setenv("DISCORD_GUILD_ID", "overridden")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Discord.GuildID != "overridden" {
		t.Errorf("guild_id = %q, want %q", cfg.Discord.GuildID, "overridden")
	}
}

func TestLoadMissingFileWithEnvVars(t *testing.T) {
	t.Setenv("DISCORD_BOT_TOKEN", "tok")
	t.Setenv("DISCORD_GUILD_ID", "gid")
	t.Setenv("LIVEKIT_URL", "wss://lk")
	t.Setenv("LIVEKIT_API_KEY", "k")
	t.Setenv("LIVEKIT_API_SECRET", "s")
	t.Setenv("MATRIX_HOMESERVER_URL", "https://mx")
	t.Setenv("MATRIX_AS_TOKEN", "as")
	t.Setenv("MATRIX_SERVER_NAME", "srv")
	t.Setenv("LK_JWT_SERVICE_URL", "https://jwt")

	cfg, err := Load("/nonexistent/config.yaml")
	if err != nil {
		t.Fatalf("should succeed with env vars only: %v", err)
	}
	if cfg.Discord.BotToken != "tok" {
		t.Errorf("bot_token = %q, want %q", cfg.Discord.BotToken, "tok")
	}
}

func TestLoadInvalidYAML(t *testing.T) {
	path := writeConfig(t, "{{invalid yaml")
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestValidateMissingFields(t *testing.T) {
	tests := []struct {
		name   string
		config string
		errMsg string
	}{
		{
			name:   "missing bot_token",
			config: `discord: {guild_id: "x"}`,
			errMsg: "discord.bot_token or discord.bot_tokens is required",
		},
		{
			name:   "missing guild_id",
			config: `discord: {bot_token: "x"}`,
			errMsg: "discord.guild_id is required",
		},
		{
			name: "missing livekit url",
			config: `discord: {bot_token: "x", guild_id: "x"}
livekit: {api_key: "x", api_secret: "x"}`,
			errMsg: "livekit.url is required",
		},
		{
			name: "missing matrix as_token",
			config: `discord: {bot_token: "x", guild_id: "x"}
livekit: {url: "x", api_key: "x", api_secret: "x"}
matrix: {homeserver_url: "x", server_name: "x", lk_jwt_service_url: "x"}`,
			errMsg: "matrix.as_token is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeConfig(t, tt.config)
			_, err := Load(path)
			if err == nil {
				t.Fatal("expected validation error")
			}
			if err.Error() != tt.errMsg {
				t.Errorf("error = %q, want %q", err.Error(), tt.errMsg)
			}
		})
	}
}
