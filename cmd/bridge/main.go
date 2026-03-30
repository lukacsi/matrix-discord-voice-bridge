package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lukacsi/matrix-discord-voice-bridge/pkg/bridge"
	"github.com/lukacsi/matrix-discord-voice-bridge/pkg/config"
	"github.com/lukacsi/matrix-discord-voice-bridge/pkg/ipc"
	lk "github.com/lukacsi/matrix-discord-voice-bridge/pkg/livekit"
	"github.com/lukacsi/matrix-discord-voice-bridge/pkg/store"
	mx "github.com/lukacsi/matrix-discord-voice-bridge/pkg/matrix"
)

// LevelTrace is a custom slog level below Debug for per-frame verbosity.
const LevelTrace = slog.Level(-8)

func parseLogLevel(s string) slog.Level {
	switch s {
	case "trace":
		return LevelTrace
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	logLevel := flag.String("log-level", "", "log level: info, debug, trace")
	flag.Parse()

	var levelVar slog.LevelVar
	levelVar.Set(slog.LevelInfo)

	handler := &filterHandler{
		inner: slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: &levelVar,
			ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if a.Key == slog.LevelKey {
					if l, ok := a.Value.Any().(slog.Level); ok && l == LevelTrace {
						a.Value = slog.StringValue("TRACE")
					}
				}
				return a
			},
		}),
	}
	logger := slog.New(handler)
	slog.SetDefault(logger)

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("config error", slog.Any("err", err))
		os.Exit(1)
	}

	// Flag overrides config
	if *logLevel != "" {
		cfg.LogLevel = *logLevel
	}
	if cfg.LogLevel != "" {
		levelVar.Set(parseLogLevel(cfg.LogLevel))
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, logger, cfg, &levelVar); err != nil {
		logger.Error("bridge exited", slog.Any("err", err))
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger, cfg *config.Config, levelVar *slog.LevelVar) error {
	// Open database
	db, err := store.Open(cfg.Database)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()
	logger.Info("database opened", slog.String("path", cfg.Database))

	// Seed config tokens into DB (idempotent — skips existing)
	for _, token := range cfg.Discord.Tokens() {
		if _, err := db.AddBot(token, cfg.Discord.GuildID); err != nil {
			logger.Debug("bot token already in database or error", slog.Any("err", err))
		}
	}

	// Load active bots from DB (includes config + previously hot-added)
	bots, err := db.AllActiveBots()
	if err != nil {
		return fmt.Errorf("load bots: %w", err)
	}
	tokens := make([]string, len(bots))
	for i, b := range bots {
		tokens[i] = b.Token
	}

	logger.Info("starting bridge",
		slog.Int("bot_count", len(tokens)),
		slog.String("guild", cfg.Discord.GuildID),
	)

	// Matrix signaller
	signaller, err := mx.NewSignaller(mx.Config{
		HomeserverURL: cfg.Matrix.HomeserverURL,
		ASToken:       cfg.Matrix.ASToken,
		ServerName:    cfg.Matrix.ServerName,
		LKJWTService:  cfg.Matrix.LKJWTService,
	}, logger)
	if err != nil {
		return err
	}

	// Start N sidecars — one per bot token
	var slots []*bridge.SidecarSlot
	var servers []*ipc.Server
	var cmds []*os.Process

	for i, bot := range bots {
		token := bot.Token
		socketPath := fmt.Sprintf("%s.%d", cfg.Sidecar.SocketPath, i)
		primary := i == 0

		srv, err := ipc.NewServer(socketPath)
		if err != nil {
			return fmt.Errorf("sidecar %d: %w", i, err)
		}
		servers = append(servers, srv)

		cmd, err := ipc.StartSidecar(cfg.Sidecar.Dir, socketPath, token, cfg.Discord.GuildID, cfg.LogLevel, primary, i)
		if err != nil {
			srv.Close()
			return fmt.Errorf("sidecar %d: %w", i, err)
		}
		cmds = append(cmds, cmd.Process)

		conn, err := srv.Accept(30 * time.Second)
		if err != nil {
			return fmt.Errorf("sidecar %d accept: %w", i, err)
		}
		conn.SetWriteTimeout(5 * time.Second)

		// Mask token for display: show first 10 chars + "..."
		masked := token
		if len(masked) > 10 {
			masked = masked[:10] + "..."
		}

		slots = append(slots, &bridge.SidecarSlot{
			Conn:    conn,
			Index:   i,
			Primary: primary,
			Token:   masked,
			BotDBID: bot.ID,
		})

		role := "audio"
		if primary {
			role = "primary"
		}
		logger.Info("sidecar connected",
			slog.Int("slot", i),
			slog.String("role", role),
			slog.Int("pid", cmd.Process.Pid),
		)
	}

	// Cleanup all sidecars on exit — bounded shutdown
	defer func() {
		for _, p := range cmds {
			_ = p.Signal(syscall.SIGTERM)
		}
		done := make(chan struct{})
		go func() {
			for _, p := range cmds {
				_, _ = p.Wait()
			}
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			logger.Warn("sidecars did not exit in time, sending SIGKILL")
			for _, p := range cmds {
				_ = p.Signal(syscall.SIGKILL)
			}
		}
		for _, s := range servers {
			s.Close()
		}
		for _, slot := range slots {
			slot.Conn.Close()
		}
	}()

	// Voice channel manager
	lkConfig := lk.Config{
		URL:       cfg.LiveKit.URL,
		APIKey:    cfg.LiveKit.APIKey,
		APISecret: cfg.LiveKit.APISecret,
	}

	mgr := bridge.NewManager(slots, signaller, db, lkConfig,
		cfg.Discord.GuildID, cfg.Matrix.ServerName, cfg.Matrix.LKJWTService, logger)
	defer mgr.Close(context.Background())

	// Read loop launcher — used for initial slots and hot-added bots
	errCh := make(chan error, 16)
	var wg sync.WaitGroup
	startReadLoop := func(s *bridge.SidecarSlot) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				msg, err := s.Conn.ReadMessage()
				if err != nil {
					select {
					case <-ctx.Done():
						return
					default:
						mgr.HandleSlotDeath(ctx, s.Index)
						if s.Primary {
							errCh <- fmt.Errorf("primary sidecar (slot %d) died: %w", s.Index, err)
						}
						return
					}
				}
				switch msg.Type {
				case ipc.MsgReady:
					role := "audio"
					if s.Primary {
						role = "primary"
					}
					logger.Info("sidecar READY", slog.Int("slot", s.Index), slog.String("role", role))
				case ipc.MsgShutdown:
					return
				default:
					mgr.HandleMessage(ctx, s.Index, msg)
				}
			}
		}()
	}

	// Configure hot-add support
	mgr.SetSidecarConfig(bridge.SidecarConfig{
		Dir:        cfg.Sidecar.Dir,
		SocketBase: cfg.Sidecar.SocketPath,
		LogLevel:   cfg.LogLevel,
	}, startReadLoop)

	// Discover rooms from previous runs
	if err := mgr.DiscoverExistingRooms(ctx); err != nil {
		logger.Warn("room discovery failed", slog.Any("err", err))
	}

	// Start Matrix /sync loop + membership renewal
	// Command handler for Matrix DM admin interface
	var cmdHandler *mx.CommandHandler
	if len(cfg.Matrix.AdminUsers) > 0 {
		cmdHandler = mx.NewCommandHandler(mgr, signaller, cfg.Matrix.AdminUsers, levelVar, cfg.Discord.GuildID, logger)
		logger.Info("admin DM commands enabled", slog.Int("admins", len(cfg.Matrix.AdminUsers)))
	}

	if err := signaller.StartSync(ctx, mgr.OnMatrixCallMember, cmdHandler); err != nil {
		return fmt.Errorf("start sync: %w", err)
	}
	signaller.StartRenewal(ctx)

	// Signal all sidecars to shut down on context cancel
	go func() {
		<-ctx.Done()
		logger.Info("shutting down")
		for _, slot := range slots {
			_ = slot.Conn.WriteMessage(&ipc.Message{Type: ipc.MsgShutdown})
		}
	}()

	// Start read loops for initial slots
	for _, slot := range slots {
		startReadLoop(slot)
	}

	// Monitor sidecar processes — detect crashes before IPC EOF
	for i, proc := range cmds {
		go func(idx int, p *os.Process) {
			state, _ := p.Wait()
			if ctx.Err() == nil { // not a graceful shutdown
				logger.Error("sidecar process exited unexpectedly",
					slog.Int("slot", idx),
					slog.String("state", state.String()),
				)
			}
		}(i, proc)
	}

	// Stats
	go func() {
		startTime := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(60 * time.Second):
				bridges, busy, total, users := mgr.Stats()
				logger.Info("stats",
					slog.Float64("uptime_m", time.Since(startTime).Minutes()),
					slog.Int("active_bridges", bridges),
					slog.Int("slots_busy", busy),
					slog.Int("slots_total", total),
					slog.Int("discord_users", users),
				)
			}
		}
	}()

	// Wait for shutdown or error
	select {
	case <-ctx.Done():
	case err := <-errCh:
		// Close all connections to unblock read loops
		for _, slot := range slots {
			slot.Conn.Close()
		}
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			logger.Warn("timed out waiting for read loops to exit")
		}
		return err
	}

	// Graceful shutdown: close connections to unblock readers
	for _, slot := range slots {
		slot.Conn.Close()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		logger.Warn("timed out waiting for read loops to exit")
	}
	return nil
}

// filterHandler drops noisy pion/WebRTC and LiveKit internal logs.
type filterHandler struct {
	inner slog.Handler
}

func (h *filterHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *filterHandler) Handle(ctx context.Context, r slog.Record) error {
	msg := r.Message
	if strings.HasPrefix(msg, "pion.") ||
		strings.HasPrefix(msg, "\"level\"=") {
		return nil
	}
	return h.inner.Handle(ctx, r)
}

func (h *filterHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &filterHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *filterHandler) WithGroup(name string) slog.Handler {
	return &filterHandler{inner: h.inner.WithGroup(name)}
}
