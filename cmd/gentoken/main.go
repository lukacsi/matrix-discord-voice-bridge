package main

import (
	"fmt"
	"os"
	"time"

	"github.com/livekit/protocol/auth"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: gentoken <room>")
		os.Exit(1)
	}

	room := os.Args[1]
	key := os.Getenv("LIVEKIT_API_KEY")
	secret := os.Getenv("LIVEKIT_API_SECRET")

	if key == "" || secret == "" {
		fmt.Fprintln(os.Stderr, "set LIVEKIT_API_KEY and LIVEKIT_API_SECRET")
		os.Exit(1)
	}

	at := auth.NewAccessToken(key, secret)
	grant := &auth.VideoGrant{
		RoomJoin: true,
		Room:     room,
	}
	at.SetVideoGrant(grant).
		SetIdentity(fmt.Sprintf("listener-%d", time.Now().Unix())).
		SetValidFor(24 * 30 * time.Hour) // 30 days

	token, err := at.ToJWT()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(token)
}
