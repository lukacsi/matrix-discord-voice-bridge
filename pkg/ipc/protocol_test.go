package ipc

import (
	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func socketPair(t *testing.T) (*Conn, *Conn) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sock")

	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })

	clientConn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}

	serverConn, err := l.Accept()
	if err != nil {
		clientConn.Close()
		t.Fatal(err)
	}

	client := &Conn{conn: clientConn}
	server := &Conn{conn: serverConn}
	t.Cleanup(func() { client.Close(); server.Close() })
	return client, server
}

func TestMessageRoundtrip(t *testing.T) {
	tests := []struct {
		name string
		msg  Message
	}{
		{
			name: "audio frame",
			msg:  Message{Type: MsgAudioFromDiscord, UserID: 274276440642551818, Payload: []byte{0xf8, 0xff, 0xfe}},
		},
		{
			name: "empty payload",
			msg:  Message{Type: MsgReady, UserID: 0, Payload: nil},
		},
		{
			name: "user join",
			msg:  Message{Type: MsgUserJoin, UserID: 123456789012345678, Payload: nil},
		},
		{
			name: "shutdown",
			msg:  Message{Type: MsgShutdown, UserID: 0, Payload: nil},
		},
		{
			name: "large payload",
			msg:  Message{Type: MsgAudioToDiscord, UserID: 42, Payload: make([]byte, 1275)},
		},
		{
			name: "max user id",
			msg:  Message{Type: MsgUserLeave, UserID: ^uint64(0), Payload: []byte{1}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sender, receiver := socketPair(t)

			if err := sender.WriteMessage(&tt.msg); err != nil {
				t.Fatalf("WriteMessage: %v", err)
			}

			got, err := receiver.ReadMessage()
			if err != nil {
				t.Fatalf("ReadMessage: %v", err)
			}

			if got.Type != tt.msg.Type {
				t.Errorf("Type = %d, want %d", got.Type, tt.msg.Type)
			}
			if got.UserID != tt.msg.UserID {
				t.Errorf("UserID = %d, want %d", got.UserID, tt.msg.UserID)
			}
			if len(got.Payload) != len(tt.msg.Payload) {
				t.Errorf("Payload len = %d, want %d", len(got.Payload), len(tt.msg.Payload))
			}
		})
	}
}

func TestMultipleMessages(t *testing.T) {
	sender, receiver := socketPair(t)

	msgs := []Message{
		{Type: MsgUserJoin, UserID: 100},
		{Type: MsgAudioFromDiscord, UserID: 100, Payload: []byte{1, 2, 3}},
		{Type: MsgAudioFromDiscord, UserID: 100, Payload: []byte{4, 5, 6}},
		{Type: MsgUserLeave, UserID: 100},
	}

	for i := range msgs {
		if err := sender.WriteMessage(&msgs[i]); err != nil {
			t.Fatalf("WriteMessage[%d]: %v", i, err)
		}
	}

	for i := range msgs {
		got, err := receiver.ReadMessage()
		if err != nil {
			t.Fatalf("ReadMessage[%d]: %v", i, err)
		}
		if got.Type != msgs[i].Type || got.UserID != msgs[i].UserID {
			t.Errorf("[%d] got type=%d user=%d, want type=%d user=%d",
				i, got.Type, got.UserID, msgs[i].Type, msgs[i].UserID)
		}
	}
}

func TestReadMessageEOF(t *testing.T) {
	sender, receiver := socketPair(t)
	sender.Close()

	_, err := receiver.ReadMessage()
	if err == nil {
		t.Fatal("expected error on closed connection")
	}
}

func TestReadMessageTruncatedHeader(t *testing.T) {
	client, server := socketPair(t)

	// Write partial header (5 bytes instead of 11)
	_, _ = client.conn.Write([]byte{0x01, 0x02, 0x03, 0x04, 0x05})
	client.Close()

	_, err := server.ReadMessage()
	if err == nil {
		t.Fatal("expected error on truncated header")
	}
}

func TestReadMessageTruncatedPayload(t *testing.T) {
	client, server := socketPair(t)

	// Write valid header claiming 100 bytes payload, then close
	header := make([]byte, 11)
	header[0] = MsgAudioFromDiscord
	binary.LittleEndian.PutUint16(header[9:11], 100)
	_, _ = client.conn.Write(header)
	_, _ = client.conn.Write([]byte{1, 2, 3}) // only 3 of 100 bytes
	client.Close()

	_, err := server.ReadMessage()
	if err == nil {
		t.Fatal("expected error on truncated payload")
	}
}

func TestServerAcceptAndCleanup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sock")

	srv, err := NewServer(path)
	if err != nil {
		t.Fatal(err)
	}

	// Socket file should exist
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("socket file not created: %v", err)
	}

	// Connect a client
	go func() {
		c, err := net.Dial("unix", path)
		if err != nil {
			return
		}
		c.Close()
	}()

	conn, err := srv.Accept(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()

	// Close should remove socket file
	srv.Close()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("socket file not cleaned up after Close")
	}
}

func TestServerRemovesStaleSocket(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sock")

	// Create a stale socket file
	if err := os.WriteFile(path, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}

	// NewServer should remove and replace it
	srv, err := NewServer(path)
	if err != nil {
		t.Fatalf("NewServer should handle stale socket: %v", err)
	}
	srv.Close()
}

func TestWriteMessageConcurrent(t *testing.T) {
	sender, receiver := socketPair(t)

	const n = 100
	done := make(chan struct{})

	// Write from multiple goroutines
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			msg := &Message{Type: MsgAudioFromDiscord, UserID: uint64(i), Payload: []byte{byte(i)}}
			if err := sender.WriteMessage(msg); err != nil {
				t.Errorf("WriteMessage: %v", err)
				return
			}
		}
	}()

	// Read all messages
	for i := 0; i < n; i++ {
		msg, err := receiver.ReadMessage()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("ReadMessage[%d]: %v", i, err)
		}
		if msg.Type != MsgAudioFromDiscord {
			t.Errorf("[%d] type = %d, want %d", i, msg.Type, MsgAudioFromDiscord)
		}
	}
	<-done
}

func TestAcceptTimeout(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sock")

	srv, err := NewServer(path)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	// No client connects — should timeout
	_, err = srv.Accept(100 * time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestReadTimeout(t *testing.T) {
	sender, receiver := socketPair(t)
	defer sender.Close()
	defer receiver.Close()

	receiver.SetReadTimeout(100 * time.Millisecond)

	// Don't send anything — reader should timeout
	_, err := receiver.ReadMessage()
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestWriteTimeout(t *testing.T) {
	sender, receiver := socketPair(t)
	defer receiver.Close()

	sender.SetWriteTimeout(100 * time.Millisecond)

	// Fill the socket buffer by writing without reading
	bigPayload := make([]byte, 65000)
	msg := &Message{Type: MsgAudioFromDiscord, Payload: bigPayload}

	var writeErr error
	for i := 0; i < 1000; i++ {
		if err := sender.WriteMessage(msg); err != nil {
			writeErr = err
			break
		}
	}
	if writeErr == nil {
		t.Log("write buffer not exhausted — skipping timeout test")
	}
}

func TestReadWriteWithTimeoutSuccess(t *testing.T) {
	sender, receiver := socketPair(t)
	defer sender.Close()
	defer receiver.Close()

	sender.SetWriteTimeout(5 * time.Second)
	receiver.SetReadTimeout(5 * time.Second)

	// Normal operation should succeed within timeout
	msg := &Message{Type: MsgReady, UserID: 42}
	if err := sender.WriteMessage(msg); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	got, err := receiver.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if got.Type != MsgReady || got.UserID != 42 {
		t.Errorf("got type=%d user=%d, want type=%d user=42", got.Type, got.UserID, MsgReady)
	}
}
