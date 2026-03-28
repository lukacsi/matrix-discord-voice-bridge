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
	mx "github.com/ogerverse/livekit-discord-bridge/pkg/matrix"

	"maunium.net/go/mautrix/id"
)

const socketPath = "/tmp/discord-voice-bridge.sock"

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// Discord config
	token := os.Getenv("DISCORD_BOT_TOKEN")
	guildID := os.Getenv("DISCORD_GUILD_ID")
	channelID := os.Getenv("DISCORD_CHANNEL_ID")

	// LiveKit config
	lkURL := os.Getenv("LIVEKIT_URL")
	lkKey := os.Getenv("LIVEKIT_API_KEY")
	lkSecret := os.Getenv("LIVEKIT_API_SECRET")

	// Matrix config
	matrixURL := os.Getenv("MATRIX_HOMESERVER_URL")
	matrixASToken := os.Getenv("MATRIX_AS_TOKEN")
	matrixServer := os.Getenv("MATRIX_SERVER_NAME")
	matrixRoomID := os.Getenv("MATRIX_ROOM_ID")
	lkJWTService := os.Getenv("LK_JWT_SERVICE_URL")

	if token == "" || guildID == "" || channelID == "" {
		logger.Error("set DISCORD_BOT_TOKEN, DISCORD_GUILD_ID, DISCORD_CHANNEL_ID")
		os.Exit(1)
	}
	if lkURL == "" || lkKey == "" || lkSecret == "" {
		logger.Error("set LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET")
		os.Exit(1)
	}
	if matrixURL == "" || matrixASToken == "" || matrixServer == "" || matrixRoomID == "" || lkJWTService == "" {
		logger.Error("set MATRIX_HOMESERVER_URL, MATRIX_AS_TOKEN, MATRIX_SERVER_NAME, MATRIX_ROOM_ID, LK_JWT_SERVICE_URL")
		os.Exit(1)
	}

	// LiveKit room name — lk-jwt-service legacy endpoint uses the raw Matrix room ID
	lkRoom := matrixRoomID
	logger.Info("LiveKit room", slog.String("matrix_room", matrixRoomID), slog.String("lk_room", lkRoom))

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, logger, token, guildID, channelID, lkURL, lkKey, lkSecret, lkRoom,
		matrixURL, matrixASToken, matrixServer, matrixRoomID, lkJWTService); err != nil {
		logger.Error("bridge exited", slog.Any("err", err))
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger,
	token, guildID, channelID,
	lkURL, lkKey, lkSecret, lkRoom,
	matrixURL, matrixASToken, matrixServer, matrixRoomID, lkJWTService string,
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
	defer signaller.LeaveAll(context.Background())

	// LiveKit manager with Matrix-compatible identities
	lkManager := lk.NewManager(lk.Config{
		URL:       lkURL,
		APIKey:    lkKey,
		APISecret: lkSecret,
		RoomName:  lkRoom,
	}, logger)
	lkManager.SetIdentityFunc(signaller.LiveKitIdentity)
	defer lkManager.Close()

	// IPC + sidecar
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

	// Signal sidecar to shut down when context cancels
	go func() {
		<-ctx.Done()
		logger.Info("shutting down")
		_ = conn.WriteMessage(&ipc.Message{Type: ipc.MsgShutdown})
	}()

	return bridgeLoop(ctx, logger, conn, lkManager, signaller, id.RoomID(matrixRoomID), lkRoom)
}

func bridgeLoop(ctx context.Context, logger *slog.Logger, conn *ipc.Conn,
	lkManager *lk.Manager, signaller *mx.Signaller, matrixRoomID id.RoomID, lkRoom string,
) error {
	var totalFrames int64
	var errCount int64
	joinedUsers := make(map[uint64]bool)
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
			logger.Info("sidecar READY — bridge active", slog.String("livekit_room", lkRoom))

		case ipc.MsgUserJoin:
			if !joinedUsers[msg.UserID] {
				joinedUsers[msg.UserID] = true
				if err := signaller.JoinCall(ctx, msg.UserID, matrixRoomID); err != nil {
					logger.Error("failed to signal Matrix call join",
						slog.Uint64("user", msg.UserID),
						slog.Any("err", err),
					)
					joinedUsers[msg.UserID] = false
				}
			}

		case ipc.MsgUserLeave:
			// Don't remove LiveKit participant or Matrix membership on speaking gaps.
			// Discord sends leave/join on every silence gap — keep the participant alive.
			// Cleanup happens on bridge shutdown via defer.

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
