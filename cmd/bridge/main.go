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

	"github.com/lukacsi/livekit-discord-bridge/pkg/bridge"
	"github.com/lukacsi/livekit-discord-bridge/pkg/config"
	"github.com/lukacsi/livekit-discord-bridge/pkg/ipc"
	lk "github.com/lukacsi/livekit-discord-bridge/pkg/livekit"
	mx "github.com/lukacsi/livekit-discord-bridge/pkg/matrix"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	handler := &filterHandler{
		inner: slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}),
	}
	logger := slog.New(handler)
	slog.SetDefault(logger)

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("config error", slog.Any("err", err))
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, logger, cfg); err != nil {
		logger.Error("bridge exited", slog.Any("err", err))
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger, cfg *config.Config) error {
	tokens := cfg.Discord.Tokens()
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

	for i, token := range tokens {
		socketPath := fmt.Sprintf("%s.%d", cfg.Sidecar.SocketPath, i)
		primary := i == 0

		srv, err := ipc.NewServer(socketPath)
		if err != nil {
			return fmt.Errorf("sidecar %d: %w", i, err)
		}
		servers = append(servers, srv)

		cmd, err := ipc.StartSidecar(cfg.Sidecar.Dir, socketPath, token, cfg.Discord.GuildID, primary)
		if err != nil {
			srv.Close()
			return fmt.Errorf("sidecar %d: %w", i, err)
		}
		cmds = append(cmds, cmd.Process)

		conn, err := srv.Accept()
		if err != nil {
			return fmt.Errorf("sidecar %d accept: %w", i, err)
		}

		slots = append(slots, &bridge.SidecarSlot{
			Conn:    conn,
			Index:   i,
			Primary: primary,
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

	// Cleanup all sidecars on exit
	defer func() {
		for _, p := range cmds {
			_ = p.Signal(syscall.SIGTERM)
			_, _ = p.Wait()
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

	mgr := bridge.NewManager(slots, signaller, lkConfig,
		cfg.Discord.GuildID, cfg.Matrix.ServerName, cfg.Matrix.LKJWTService, logger)
	defer mgr.Close(context.Background())

	// Discover rooms from previous runs
	if err := mgr.DiscoverExistingRooms(ctx); err != nil {
		logger.Warn("room discovery failed", slog.Any("err", err))
	}

	// Start Matrix /sync loop + membership renewal
	signaller.StartSync(ctx, mgr.OnMatrixCallMember)
	signaller.StartRenewal(ctx)

	// Signal all sidecars to shut down on context cancel
	go func() {
		<-ctx.Done()
		logger.Info("shutting down")
		for _, slot := range slots {
			_ = slot.Conn.WriteMessage(&ipc.Message{Type: ipc.MsgShutdown})
		}
	}()

	// Start a read loop per sidecar slot
	errCh := make(chan error, len(slots))
	var wg sync.WaitGroup

	for _, slot := range slots {
		wg.Add(1)
		go func(s *bridge.SidecarSlot) {
			defer wg.Done()
			for {
				msg, err := s.Conn.ReadMessage()
				if err != nil {
					select {
					case <-ctx.Done():
						return
					default:
						errCh <- fmt.Errorf("slot %d: %w", s.Index, err)
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
		}(slot)
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
		wg.Wait()
		return nil
	case err := <-errCh:
		wg.Wait()
		return err
	}
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
