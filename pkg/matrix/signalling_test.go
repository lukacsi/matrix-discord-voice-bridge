package matrix

import (
	"testing"
)

func TestLiveKitRoomAlias(t *testing.T) {
	tests := []struct {
		name   string
		roomID string
		want   string
	}{
		{
			name:   "real room ID",
			roomID: "!QtfrksvqxhfdavPQjB:lukacsi.org",
			want:   "hG3c1V2/qQzQwNyPBwSZGSgRF2g/g2K0jJE4lQRMR4c",
		},
		{
			name:   "second room",
			roomID: "!xuEADtQwIpGGDVCIdS:lukacsi.org",
			want:   "oyXsb9+rC6tga9DZx/LgV4sMf531wpqIWoZ0/zPD79w",
		},
		{
			name:   "simple room ID",
			roomID: "!test:example.com",
			// Deterministic — same input always produces same output
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := LiveKitRoomAlias(tt.roomID)

			if tt.want != "" && got != tt.want {
				t.Errorf("LiveKitRoomAlias(%q) = %q; want %q", tt.roomID, got, tt.want)
			}

			// Must use standard base64 (+ and /), not URL-safe (- and _)
			for _, c := range got {
				if c == '-' || c == '_' {
					t.Errorf("LiveKitRoomAlias(%q) = %q; contains URL-safe chars, must use StdEncoding", tt.roomID, got)
					break
				}
			}

			// Must not have padding
			if len(got) > 0 && got[len(got)-1] == '=' {
				t.Errorf("LiveKitRoomAlias(%q) = %q; has padding, must use RawStdEncoding", tt.roomID, got)
			}

			// Deterministic
			got2 := LiveKitRoomAlias(tt.roomID)
			if got != got2 {
				t.Errorf("LiveKitRoomAlias is not deterministic: %q != %q", got, got2)
			}
		})
	}
}

func TestSignallerLiveKitIdentity(t *testing.T) {
	s := &Signaller{config: Config{ServerName: "lukacsi.org"}}

	tests := []struct {
		name     string
		userID   uint64
		wantID   string
	}{
		{
			name:   "real discord user",
			userID: 274276440642551818,
			wantID: "@discord_274276440642551818:lukacsi.org:VOICE_274276440642551818",
		},
		{
			name:   "zero user",
			userID: 0,
			wantID: "@discord_0:lukacsi.org:VOICE_0",
		},
		{
			name:   "max uint64",
			userID: ^uint64(0),
			wantID: "@discord_18446744073709551615:lukacsi.org:VOICE_18446744073709551615",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := s.LiveKitIdentity(tt.userID)
			if got != tt.wantID {
				t.Errorf("LiveKitIdentity(%d) = %q; want %q", tt.userID, got, tt.wantID)
			}

			// Must match sender:device_id format for Element Call legacy path
			mxid := s.discordMXID(tt.userID)
			deviceID := s.deviceID(tt.userID)
			expected := string(mxid) + ":" + deviceID
			if got != expected {
				t.Errorf("identity %q doesn't match sender:device_id format %q", got, expected)
			}
		})
	}
}

func TestDiscordMXID(t *testing.T) {
	s := &Signaller{config: Config{ServerName: "example.com"}}

	got := s.discordMXID(123456789)
	want := "@discord_123456789:example.com"
	if string(got) != want {
		t.Errorf("discordMXID(123456789) = %q; want %q", got, want)
	}
}

func TestDeviceID(t *testing.T) {
	s := &Signaller{}

	got := s.deviceID(123456789)
	want := "VOICE_123456789"
	if got != want {
		t.Errorf("deviceID(123456789) = %q; want %q", got, want)
	}
}

func TestStateKey(t *testing.T) {
	// State key must match Element Call's expected format:
	// _@user:server_DEVICEID_m.call
	s := &Signaller{config: Config{ServerName: "lukacsi.org"}}

	mxid := s.discordMXID(274276440642551818)
	deviceID := s.deviceID(274276440642551818)
	stateKey := "_" + string(mxid) + "_" + deviceID + "_" + callApplication

	want := "_@discord_274276440642551818:lukacsi.org_VOICE_274276440642551818_m.call"
	if stateKey != want {
		t.Errorf("state key = %q; want %q", stateKey, want)
	}
}
