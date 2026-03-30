//go:build ignore

package main

import "C"

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/disgoorg/disgo"
	"github.com/disgoorg/disgo/bot"
	"github.com/disgoorg/disgo/events"
	"github.com/disgoorg/disgo/gateway"
	"github.com/disgoorg/disgo/voice"
	"github.com/disgoorg/godave/golibdave"
	"github.com/disgoorg/snowflake/v2"
)

var (
	token     = os.Getenv("DISCORD_BOT_TOKEN")
	guildID   = snowflake.GetEnv("DISCORD_GUILD_ID")
	channelID = snowflake.GetEnv("DISCORD_CHANNEL_ID")
)

type loggingReceiver struct {
	logger *slog.Logger
	count  atomic.Int64
}

func (r *loggingReceiver) ReceiveOpusFrame(userID snowflake.ID, packet *voice.Packet) error {
	n := r.count.Add(1)
	if n%50 == 1 {
		r.logger.Info("recv opus frame",
			slog.String("user", userID.String()),
			slog.Int("opus_bytes", len(packet.Opus)),
			slog.Int("seq", int(packet.Sequence)),
			slog.Int64("total", n),
		)
	}
	return nil
}

func (r *loggingReceiver) CleanupUser(userID snowflake.ID) {
	r.logger.Info("user left voice", slog.String("user", userID.String()))
}

func (r *loggingReceiver) Close() {
	r.logger.Info("receiver closed", slog.Int64("total_frames", r.count.Load()))
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)

	if token == "" || guildID == 0 || channelID == 0 {
		logger.Error("set DISCORD_BOT_TOKEN, DISCORD_GUILD_ID, DISCORD_CHANNEL_ID")
		os.Exit(1)
	}

	client, err := disgo.New(token,
		bot.WithGatewayConfigOpts(
			gateway.WithIntents(gateway.IntentGuildVoiceStates),
		),
		bot.WithEventListenerFunc(func(e *events.Ready) {
			logger.Info("bot ready, joining voice channel")
			go joinAndReceive(e.Client(), logger)
		}),
		bot.WithVoiceManagerConfigOpts(
			voice.WithDaveSessionCreateFunc(golibdave.NewSession),
		),
	)
	if err != nil {
		logger.Error("error creating client", slog.Any("err", err))
		os.Exit(1)
	}

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		client.Close(ctx)
	}()

	if err = client.OpenGateway(context.TODO()); err != nil {
		logger.Error("error connecting to gateway", slog.Any("err", err))
		os.Exit(1)
	}

	logger.Info("bot running, press CTRL-C to exit")
	s := make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGINT, syscall.SIGTERM)
	<-s
}

func joinAndReceive(client *bot.Client, logger *slog.Logger) {
	conn := client.VoiceManager.CreateConn(guildID)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := conn.Open(ctx, channelID, false, false); err != nil {
		logger.Error("error joining voice channel", slog.Any("err", err))
		return
	}

	receiver := &loggingReceiver{logger: logger}
	conn.SetOpusFrameReceiver(receiver)

	logger.Info("joined voice channel, receiving audio",
		slog.String("guild", guildID.String()),
		slog.String("channel", channelID.String()),
	)
}
