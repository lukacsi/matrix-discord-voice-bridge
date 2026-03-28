package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ogerverse/livekit-discord-bridge/pkg/ipc"
	lk "github.com/ogerverse/livekit-discord-bridge/pkg/livekit"
)

const socketPath = "/tmp/discord-voice-bridge.sock"

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	token := os.Getenv("DISCORD_BOT_TOKEN")
	guildID := os.Getenv("DISCORD_GUILD_ID")
	channelID := os.Getenv("DISCORD_CHANNEL_ID")
	lkURL := os.Getenv("LIVEKIT_URL")
	lkKey := os.Getenv("LIVEKIT_API_KEY")
	lkSecret := os.Getenv("LIVEKIT_API_SECRET")
	lkRoom := os.Getenv("LIVEKIT_ROOM")

	if token == "" || guildID == "" || channelID == "" {
		logger.Error("set DISCORD_BOT_TOKEN, DISCORD_GUILD_ID, DISCORD_CHANNEL_ID")
		os.Exit(1)
	}
	if lkURL == "" || lkKey == "" || lkSecret == "" || lkRoom == "" {
		logger.Error("set LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET, LIVEKIT_ROOM")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, logger, token, guildID, channelID, lkURL, lkKey, lkSecret, lkRoom); err != nil {
		logger.Error("bridge exited", slog.Any("err", err))
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger, token, guildID, channelID, lkURL, lkKey, lkSecret, lkRoom string) error {
	lkManager := lk.NewManager(lk.Config{
		URL:       lkURL,
		APIKey:    lkKey,
		APISecret: lkSecret,
		RoomName:  lkRoom,
	}, logger)
	defer lkManager.Close()

	srv, err := ipc.NewServer(socketPath)
	if err != nil {
		return err
	}
	defer srv.Close()

	cmd, err := ipc.StartSidecar("sidecar", socketPath, token, guildID, channelID)
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

	// Subscriber: LiveKit → Discord (mixed audio)
	var reverseFrames atomic.Int64
	lkConfig := lk.Config{URL: lkURL, APIKey: lkKey, APISecret: lkSecret, RoomName: lkRoom}
	sub, err := lk.NewSubscriber(lkConfig, func(opusFrame []byte) error {
		n := reverseFrames.Add(1)
		if n%250 == 1 {
			logger.Info("LK→Discord opus frame",
				slog.Int("bytes", len(opusFrame)),
				slog.Int64("total", n),
			)
		}
		return conn.WriteMessage(&ipc.Message{
			Type:    ipc.MsgAudioToDiscord,
			UserID:  0,
			Payload: opusFrame,
		})
	}, logger)
	if err != nil {
		logger.Warn("subscriber failed — reverse path disabled", slog.Any("err", err))
	} else {
		defer sub.Close()
	}

	// Shut down sidecar when context cancels
	go func() {
		<-ctx.Done()
		logger.Info("shutting down")
		_ = conn.WriteMessage(&ipc.Message{Type: ipc.MsgShutdown})
		time.Sleep(500 * time.Millisecond)
		conn.Close()
	}()

	return bridgeLoop(ctx, logger, conn, lkManager, lkRoom)
}

func bridgeLoop(ctx context.Context, logger *slog.Logger, conn *ipc.Conn, lkManager *lk.Manager, lkRoom string) error {
	var totalFrames int64
	var errCount int64
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
				return nil // expected during shutdown
			default:
				return err
			}
		}

		switch msg.Type {
		case ipc.MsgReady:
			logger.Info("sidecar READY — bridge active", slog.String("livekit_room", lkRoom))

		case ipc.MsgUserLeave:
			lkManager.RemoveParticipant(msg.UserID)

		case ipc.MsgAudioFromDiscord:
			totalFrames++
			if err := lkManager.WriteOpus(msg.UserID, msg.Payload); err != nil {
				errCount++
				if errCount%500 == 0 {
					logger.Error("opus write failed",
						slog.Uint64("user", msg.UserID),
						slog.Int64("err_count", errCount),
						slog.Any("err", err),
					)
				}
			}
		}

		if time.Since(lastStats) > 10*time.Second {
			logger.Info("stats",
				slog.Int64("frames", totalFrames),
				slog.Float64("elapsed_s", time.Since(startTime).Seconds()),
			)
			lastStats = time.Now()
		}
	}
}
