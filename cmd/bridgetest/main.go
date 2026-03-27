package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ogerverse/livekit-discord-bridge/pkg/ipc"
)

const socketPath = "/tmp/discord-voice-bridge.sock"

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	token := os.Getenv("DISCORD_BOT_TOKEN")
	guildID := os.Getenv("DISCORD_GUILD_ID")
	channelID := os.Getenv("DISCORD_CHANNEL_ID")

	if token == "" || guildID == "" || channelID == "" {
		logger.Error("set DISCORD_BOT_TOKEN, DISCORD_GUILD_ID, DISCORD_CHANNEL_ID")
		os.Exit(1)
	}

	srv, err := ipc.NewServer(socketPath)
	if err != nil {
		logger.Error("failed to create IPC server", slog.Any("err", err))
		os.Exit(1)
	}
	defer srv.Close()
	logger.Info("IPC server listening", slog.String("socket", socketPath))

	sidecarDir := "sidecar"
	cmd, err := ipc.StartSidecar(sidecarDir, socketPath, token, guildID, channelID)
	if err != nil {
		logger.Error("failed to start sidecar", slog.Any("err", err))
		os.Exit(1)
	}
	logger.Info("sidecar started", slog.Int("pid", cmd.Process.Pid))

	conn, err := srv.Accept()
	if err != nil {
		logger.Error("failed to accept sidecar connection", slog.Any("err", err))
		_ = cmd.Process.Signal(syscall.SIGTERM)
		os.Exit(1)
	}
	logger.Info("sidecar connected")

	// Handle shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Info("shutting down")
		_ = conn.WriteMessage(&ipc.Message{Type: ipc.MsgShutdown})
		time.Sleep(500 * time.Millisecond)
		_ = cmd.Process.Signal(syscall.SIGTERM)
		conn.Close()
		os.Exit(0)
	}()

	var totalFrames int64
	users := make(map[uint64]bool)
	startTime := time.Now()
	lastStats := time.Now()

	for {
		msg, err := conn.ReadMessage()
		if err != nil {
			logger.Error("read error", slog.Any("err", err))
			break
		}

		switch msg.Type {
		case ipc.MsgReady:
			logger.Info("sidecar READY")

		case ipc.MsgUserJoin:
			if !users[msg.UserID] {
				users[msg.UserID] = true
				logger.Info("user joined", slog.Uint64("user", msg.UserID))
			}

		case ipc.MsgUserLeave:
			delete(users, msg.UserID)
			logger.Info("user left", slog.Uint64("user", msg.UserID))

		case ipc.MsgAudioFromDiscord:
			totalFrames++
			if totalFrames%250 == 1 {
				elapsed := time.Since(startTime).Seconds()
				logger.Info("recv opus",
					slog.Uint64("user", msg.UserID),
					slog.Int("bytes", len(msg.Payload)),
					slog.Int64("total", totalFrames),
					slog.Float64("elapsed_s", elapsed),
				)
			}
		}

		if time.Since(lastStats) > 5*time.Second {
			elapsed := time.Since(startTime).Seconds()
			logger.Info("stats",
				slog.Int64("frames", totalFrames),
				slog.Int("users", len(users)),
				slog.Float64("elapsed_s", elapsed),
			)
			lastStats = time.Now()
		}
	}
}
