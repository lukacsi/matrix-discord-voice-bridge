//go:build ignore

package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lukacsi/matrix-discord-voice-bridge/pkg/ipc"
)

const socketPath = "/tmp/discord-voice-bridge.sock"

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	token := os.Getenv("DISCORD_BOT_TOKEN")
	guildID := os.Getenv("DISCORD_GUILD_ID")

	if token == "" || guildID == "" {
		logger.Error("set DISCORD_BOT_TOKEN, DISCORD_GUILD_ID")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	srv, err := ipc.NewServer(socketPath)
	if err != nil {
		logger.Error("failed to create IPC server", slog.Any("err", err))
		os.Exit(1)
	}
	defer srv.Close()

	cmd, err := ipc.StartSidecar("sidecar", socketPath, token, guildID, "info", true, 0)
	if err != nil {
		logger.Error("failed to start sidecar", slog.Any("err", err))
		os.Exit(1)
	}
	defer func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		_ = cmd.Wait()
	}()
	logger.Info("sidecar started", slog.Int("pid", cmd.Process.Pid))

	conn, err := srv.Accept(30 * time.Second)
	if err != nil {
		logger.Error("failed to accept sidecar", slog.Any("err", err))
		os.Exit(1)
	}
	defer conn.Close()
	logger.Info("sidecar connected")

	go func() {
		<-ctx.Done()
		logger.Info("shutting down")
		_ = conn.WriteMessage(&ipc.Message{Type: ipc.MsgShutdown})
	}()

	var totalFrames int64
	users := make(map[uint64]bool)
	startTime := time.Now()
	lastStats := time.Now()

	for {
		msg, err := conn.ReadMessage()
		if err != nil {
			select {
			case <-ctx.Done():
			default:
				logger.Error("read error", slog.Any("err", err))
			}
			return
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
				logger.Info("recv opus",
					slog.Uint64("user", msg.UserID),
					slog.Int("bytes", len(msg.Payload)),
					slog.Int64("total", totalFrames),
					slog.Float64("elapsed_s", time.Since(startTime).Seconds()),
				)
			}
		}

		if time.Since(lastStats) > 5*time.Second {
			logger.Info("stats",
				slog.Int64("frames", totalFrames),
				slog.Int("users", len(users)),
				slog.Float64("elapsed_s", time.Since(startTime).Seconds()),
			)
			lastStats = time.Now()
		}
	}
}
