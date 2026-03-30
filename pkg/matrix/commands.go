package matrix

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/lukacsi/matrix-discord-voice-bridge/pkg/types"

	"maunium.net/go/mautrix/id"
)

// LevelSetter allows runtime log level changes.
type LevelSetter interface {
	Set(slog.Level)
}

// CommandHandler processes admin commands received via Matrix DMs.
type CommandHandler struct {
	manager    types.ManagerAPI
	signaller  *Signaller
	adminUsers map[string]bool
	levelSet   LevelSetter
	logger     *slog.Logger
	startTime  time.Time
	guildID    string
}

// NewCommandHandler creates a command handler.
func NewCommandHandler(
	manager types.ManagerAPI,
	signaller *Signaller,
	adminUsers []string,
	levelSet LevelSetter,
	guildID string,
	logger *slog.Logger,
) *CommandHandler {
	admins := make(map[string]bool, len(adminUsers))
	for _, u := range adminUsers {
		admins[strings.TrimSpace(u)] = true
	}
	return &CommandHandler{
		manager:    manager,
		signaller:  signaller,
		adminUsers: admins,
		levelSet:   levelSet,
		logger:     logger,
		startTime:  time.Now(),
		guildID:    guildID,
	}
}

// IsAdmin checks if a user is allowed to run commands.
func (h *CommandHandler) IsAdmin(sender string) bool {
	return h.adminUsers[sender]
}

// Handle processes a command string and returns the reply text.
func (h *CommandHandler) Handle(ctx context.Context, roomID id.RoomID, sender, body string) string {
	parts := strings.Fields(body)
	if len(parts) == 0 {
		return ""
	}
	cmd := strings.ToLower(parts[0])
	args := parts[1:]

	h.logger.Info("admin command",
		slog.String("matrix_user", sender),
		slog.String("command", cmd),
	)

	switch cmd {
	case "!help":
		return h.cmdHelp()
	case "!status":
		return h.cmdStatus()
	case "!rooms":
		return h.cmdRooms()
	case "!bots":
		return h.cmdBots()
	case "!join":
		return h.cmdJoin(ctx, args)
	case "!leave":
		return h.cmdLeave(ctx, args)
	case "!log-level":
		return h.cmdLogLevel(args)
	case "!bot-add":
		return h.cmdBotAdd(ctx, args)
	case "!bot-remove":
		return h.cmdBotRemove(ctx, args)
	case "!sync-db":
		return h.cmdSyncDB(ctx)
	default:
		return fmt.Sprintf("unknown command: %s — try !help", cmd)
	}
}

func (h *CommandHandler) cmdHelp() string {
	return `Available commands:
!status — uptime, active bridges, slot usage
!rooms — list voice channel → Matrix room mappings
!bots — list sidecar slots and their status
!bot-add <token> — hot-add a Discord bot (persisted to DB)
!bot-remove <slot> — remove a bot slot
!sync-db — rebuild DB from Matrix state events
!join <channel-name> — manually start bridge for a channel
!leave [channel-name] — stop bridge (all if no name)
!log-level <info|debug|trace> — change log level at runtime
!help — this message`
}

func (h *CommandHandler) cmdStatus() string {
	bridges, busy, total, users := h.manager.Stats()
	uptime := time.Since(h.startTime).Round(time.Second)
	return fmt.Sprintf(`Bridge Status:
  Uptime: %s
  Guild: %s
  Active bridges: %d
  Slots: %d/%d busy
  Discord users in VCs: %d`, uptime, h.guildID, bridges, busy, total, users)
}

func (h *CommandHandler) cmdRooms() string {
	rooms := h.manager.ListRooms()
	if len(rooms) == 0 {
		return "No voice rooms discovered."
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Voice rooms (%d):\n", len(rooms)))
	for ch, info := range rooms {
		name := info.Name
		if name == "" {
			name = fmt.Sprintf("channel-%d", ch)
		}
		sb.WriteString(fmt.Sprintf("  %s → %s\n", name, info.RoomID))
	}
	return sb.String()
}

func (h *CommandHandler) cmdBots() string {
	slots := h.manager.ListSlots()
	if len(slots) == 0 {
		return "No sidecar slots."
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Bot slots (%d):\n", len(slots)))
	for _, s := range slots {
		line := fmt.Sprintf("  [%d] %s", s.Index, s.Status)
		if s.Status == "busy" {
			name := s.ChannelName
			if name == "" {
				name = fmt.Sprintf("%d", s.ChannelID)
			}
			line += fmt.Sprintf(" → %s", name)
		}
		if s.Token != "" {
			line += fmt.Sprintf(" (token: %s)", s.Token)
		}
		sb.WriteString(line + "\n")
	}
	return sb.String()
}

func (h *CommandHandler) cmdJoin(ctx context.Context, args []string) string {
	if len(args) == 0 {
		return "Usage: !join <channel-name>"
	}
	name := strings.Join(args, " ")
	if err := h.manager.ManualJoin(ctx, name); err != nil {
		return fmt.Sprintf("Failed to join: %s", err)
	}
	return fmt.Sprintf("Started bridge for %q", name)
}

func (h *CommandHandler) cmdLeave(ctx context.Context, args []string) string {
	name := strings.Join(args, " ")
	if err := h.manager.ManualLeave(ctx, name); err != nil {
		return fmt.Sprintf("Failed to leave: %s", err)
	}
	if name == "" {
		return "Stopped all bridges."
	}
	return fmt.Sprintf("Stopped bridge for %q", name)
}

func (h *CommandHandler) cmdLogLevel(args []string) string {
	if len(args) == 0 {
		return "Usage: !log-level <info|debug|trace>"
	}
	level := strings.ToLower(args[0])
	switch level {
	case "info":
		h.levelSet.Set(slog.LevelInfo)
	case "debug":
		h.levelSet.Set(slog.LevelDebug)
	case "trace":
		h.levelSet.Set(slog.Level(-8)) // LevelTrace
	case "warn":
		h.levelSet.Set(slog.LevelWarn)
	default:
		return fmt.Sprintf("Unknown level %q — use info, debug, or trace", level)
	}
	h.logger.Info("log level changed via command", slog.String("level", level))
	return fmt.Sprintf("Log level set to %s", level)
}

func (h *CommandHandler) cmdSyncDB(ctx context.Context) string {
	found, err := h.manager.RebuildDB(ctx)
	if err != nil {
		return fmt.Sprintf("Failed to rebuild DB: %s", err)
	}
	return fmt.Sprintf("Rebuilt database from Matrix state: %d rooms found.", found)
}

func (h *CommandHandler) cmdBotAdd(ctx context.Context, args []string) string {
	if len(args) == 0 {
		return "Usage: !bot-add <discord-bot-token>"
	}
	token := args[0]
	slotIdx, err := h.manager.AddBot(ctx, token)
	if err != nil {
		return fmt.Sprintf("Failed to add bot: %s", err)
	}
	return fmt.Sprintf("Bot added as slot %d (audio-only). Persisted to database.", slotIdx)
}

func (h *CommandHandler) cmdBotRemove(ctx context.Context, args []string) string {
	if len(args) == 0 {
		return "Usage: !bot-remove <slot-number>"
	}
	var slotIdx int
	if _, err := fmt.Sscanf(args[0], "%d", &slotIdx); err != nil {
		return fmt.Sprintf("Invalid slot number: %s", args[0])
	}
	if err := h.manager.RemoveBot(ctx, slotIdx); err != nil {
		return fmt.Sprintf("Failed to remove bot: %s", err)
	}
	return fmt.Sprintf("Slot %d removed and marked dead.", slotIdx)
}
