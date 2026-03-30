package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lukacsi/livekit-discord-bridge/pkg/bridge"
	"github.com/lukacsi/livekit-discord-bridge/pkg/ipc"
	lk "github.com/lukacsi/livekit-discord-bridge/pkg/livekit"
	mx "github.com/lukacsi/livekit-discord-bridge/pkg/matrix"
)

const socketPath = "/tmp/discord-voice-bridge.sock"

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// Discord config
	token := os.Getenv("DISCORD_BOT_TOKEN")
	guildID := os.Getenv("DISCORD_GUILD_ID")

	// LiveKit config
	lkURL := os.Getenv("LIVEKIT_URL")
	lkKey := os.Getenv("LIVEKIT_API_KEY")
	lkSecret := os.Getenv("LIVEKIT_API_SECRET")

	// Matrix config
	matrixURL := os.Getenv("MATRIX_HOMESERVER_URL")
	matrixASToken := os.Getenv("MATRIX_AS_TOKEN")
	matrixServer := os.Getenv("MATRIX_SERVER_NAME")
	lkJWTService := os.Getenv("LK_JWT_SERVICE_URL")

	if token == "" || guildID == "" {
		logger.Error("set DISCORD_BOT_TOKEN, DISCORD_GUILD_ID")
		os.Exit(1)
	}
	if lkURL == "" || lkKey == "" || lkSecret == "" {
		logger.Error("set LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET")
		os.Exit(1)
	}
	if matrixURL == "" || matrixASToken == "" || matrixServer == "" || lkJWTService == "" {
		logger.Error("set MATRIX_HOMESERVER_URL, MATRIX_AS_TOKEN, MATRIX_SERVER_NAME, LK_JWT_SERVICE_URL")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, logger, token, guildID, lkURL, lkKey, lkSecret,
		matrixURL, matrixASToken, matrixServer, lkJWTService); err != nil {
		logger.Error("bridge exited", slog.Any("err", err))
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger,
	token, guildID,
	lkURL, lkKey, lkSecret,
	matrixURL, matrixASToken, matrixServer, lkJWTService string,
) error {
	// Matrix signaller
	signaller, err := mx.NewSignaller(mx.Config{
		HomeserverURL: matrixURL,
		ASToken:       matrixASToken,
		ServerName:    matrixServer,
		LKJWTService:  lkJWTService,
	}, logger)
	if err != nil {
		return err
	}

	// IPC + sidecar
	srv, err := ipc.NewServer(socketPath)
	if err != nil {
		return err
	}
	defer srv.Close()

	cmd, err := ipc.StartSidecar("sidecar", socketPath, token, guildID)
	if err != nil {
		return err
	}
	defer func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		_ = cmd.Wait()
	}()
	logger.Info("sidecar started", slog.Int("pid", cmd.Process.Pid))

	conn, err := srv.Accept()
	if err != nil {
		return err
	}
	defer conn.Close()
	logger.Info("sidecar connected")

	// Voice channel manager
	lkConfig := lk.Config{
		URL:       lkURL,
		APIKey:    lkKey,
		APISecret: lkSecret,
	}

	mgr := bridge.NewManager(conn, signaller, lkConfig, guildID, matrixServer, lkJWTService, logger)
	defer mgr.Close(context.Background())

	// Signal sidecar to shut down when context cancels
	go func() {
		<-ctx.Done()
		logger.Info("shutting down")
		_ = conn.WriteMessage(&ipc.Message{Type: ipc.MsgShutdown})
	}()

	// Main loop
	startTime := time.Now()
	lastStats := time.Now()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		msg, err := conn.ReadMessage()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				return err
			}
		}

		switch msg.Type {
		case ipc.MsgReady:
			logger.Info("sidecar READY — watching voice states", slog.String("guild", guildID))

		case ipc.MsgShutdown:
			return nil

		default:
			mgr.HandleMessage(ctx, msg)
		}

		if time.Since(lastStats) > 30*time.Second {
			logger.Info("stats", slog.Float64("elapsed_s", time.Since(startTime).Seconds()))
			lastStats = time.Now()
		}
	}
}
