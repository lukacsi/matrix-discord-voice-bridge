package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Discord  Discord `yaml:"discord"`
	LiveKit  LiveKit `yaml:"livekit"`
	Matrix   Matrix  `yaml:"matrix"`
	Sidecar  Sidecar `yaml:"sidecar"`
	LogLevel string  `yaml:"log_level"` // info (default), debug, trace
}

type Discord struct {
	BotToken  string   `yaml:"bot_token"`
	BotTokens []string `yaml:"bot_tokens"`
	GuildID   string   `yaml:"guild_id"`
}

// Tokens returns the list of bot tokens.
// bot_tokens takes precedence; bot_token is a single-element fallback.
func (d *Discord) Tokens() []string {
	if len(d.BotTokens) > 0 {
		return d.BotTokens
	}
	if d.BotToken != "" {
		return []string{d.BotToken}
	}
	return nil
}

type LiveKit struct {
	URL       string `yaml:"url"`
	APIKey    string `yaml:"api_key"`
	APISecret string `yaml:"api_secret"`
}

type Matrix struct {
	HomeserverURL string `yaml:"homeserver_url"`
	ASToken       string `yaml:"as_token"`
	ServerName    string `yaml:"server_name"`
	LKJWTService  string `yaml:"lk_jwt_service_url"`
}

type Sidecar struct {
	Dir        string `yaml:"dir"`
	SocketPath string `yaml:"socket_path"`
}

// Load reads a YAML config file, then overlays any set environment variables.
func Load(path string) (*Config, error) {
	cfg := &Config{
		Sidecar: Sidecar{
			Dir:        "sidecar",
			SocketPath: "/tmp/discord-voice-bridge.sock",
		},
	}

	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	if data != nil {
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse config %s: %w", path, err)
		}
	}

	// Environment variables override config file
	envOverride(&cfg.Discord.BotToken, "DISCORD_BOT_TOKEN")
	envOverride(&cfg.Discord.GuildID, "DISCORD_GUILD_ID")
	envOverride(&cfg.LiveKit.URL, "LIVEKIT_URL")
	envOverride(&cfg.LiveKit.APIKey, "LIVEKIT_API_KEY")
	envOverride(&cfg.LiveKit.APISecret, "LIVEKIT_API_SECRET")
	envOverride(&cfg.Matrix.HomeserverURL, "MATRIX_HOMESERVER_URL")
	envOverride(&cfg.Matrix.ASToken, "MATRIX_AS_TOKEN")
	envOverride(&cfg.Matrix.ServerName, "MATRIX_SERVER_NAME")
	envOverride(&cfg.Matrix.LKJWTService, "LK_JWT_SERVICE_URL")
	envOverride(&cfg.Sidecar.Dir, "SIDECAR_DIR")
	envOverride(&cfg.Sidecar.SocketPath, "IPC_SOCKET_PATH")
	envOverride(&cfg.LogLevel, "BRIDGE_LOG_LEVEL")

	// DISCORD_BOT_TOKENS env var: comma-separated list
	if v := os.Getenv("DISCORD_BOT_TOKENS"); v != "" {
		cfg.Discord.BotTokens = strings.Split(v, ",")
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	if len(c.Discord.Tokens()) == 0 {
		return fmt.Errorf("discord.bot_token or discord.bot_tokens is required")
	}
	if c.Discord.GuildID == "" {
		return fmt.Errorf("discord.guild_id is required")
	}
	if c.LiveKit.URL == "" {
		return fmt.Errorf("livekit.url is required")
	}
	if c.LiveKit.APIKey == "" {
		return fmt.Errorf("livekit.api_key is required")
	}
	if c.LiveKit.APISecret == "" {
		return fmt.Errorf("livekit.api_secret is required")
	}
	if c.Matrix.HomeserverURL == "" {
		return fmt.Errorf("matrix.homeserver_url is required")
	}
	if c.Matrix.ASToken == "" {
		return fmt.Errorf("matrix.as_token is required")
	}
	if c.Matrix.ServerName == "" {
		return fmt.Errorf("matrix.server_name is required")
	}
	if c.Matrix.LKJWTService == "" {
		return fmt.Errorf("matrix.lk_jwt_service_url is required")
	}
	return nil
}

func envOverride(dst *string, key string) {
	if v := os.Getenv(key); v != "" {
		*dst = v
	}
}
