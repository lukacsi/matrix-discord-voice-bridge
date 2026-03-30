package ipc

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"sync"
)

// Message types for the sidecar IPC protocol.
// See docs/ipc-protocol.md for the full specification.
const (
	MsgAudioFromDiscord byte = 0x01
	MsgAudioToDiscord   byte = 0x02
	MsgUserJoin         byte = 0x03
	MsgUserLeave        byte = 0x04
	MsgReady            byte = 0x05
	MsgShutdown         byte = 0x06
	MsgJoinChannel      byte = 0x07
	MsgLeaveChannel     byte = 0x08
	MsgVoiceState       byte = 0x09
	MsgChannelList      byte = 0x0A
	MsgUserInfo         byte = 0x0B
	MsgMatrixUsers      byte = 0x0C
)

// Message represents an IPC message from/to the sidecar.
type Message struct {
	Type    byte
	UserID  uint64
	Payload []byte
}

// Conn wraps a Unix socket connection with the sidecar.
// ReadMessage must be called from a single goroutine.
// WriteMessage is safe for concurrent use.
type Conn struct {
	conn net.Conn
	mu   sync.Mutex // protects writes only
}

// ReadMessage reads one IPC message from the connection.
// Not safe for concurrent use — call from a single goroutine.
func (c *Conn) ReadMessage() (*Message, error) {
	header := make([]byte, 11)
	if _, err := io.ReadFull(c.conn, header); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	msg := &Message{
		Type:   header[0],
		UserID: binary.LittleEndian.Uint64(header[1:9]),
	}

	payloadLen := binary.LittleEndian.Uint16(header[9:11])
	if payloadLen > 0 {
		msg.Payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(c.conn, msg.Payload); err != nil {
			return nil, fmt.Errorf("read payload: %w", err)
		}
	}

	return msg, nil
}

// WriteMessage writes one IPC message to the connection.
func (c *Conn) WriteMessage(msg *Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	header := make([]byte, 11)
	header[0] = msg.Type
	binary.LittleEndian.PutUint64(header[1:9], msg.UserID)
	binary.LittleEndian.PutUint16(header[9:11], uint16(len(msg.Payload)))

	if _, err := c.conn.Write(header); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if len(msg.Payload) > 0 {
		if _, err := c.conn.Write(msg.Payload); err != nil {
			return fmt.Errorf("write payload: %w", err)
		}
	}
	return nil
}

// Close closes the connection.
func (c *Conn) Close() error {
	return c.conn.Close()
}

// Server listens on a Unix socket and accepts a sidecar connection.
type Server struct {
	listener net.Listener
	path     string
}

// NewServer creates a new IPC server on the given socket path.
func NewServer(socketPath string) (*Server, error) {
	_ = os.Remove(socketPath) // remove stale socket; ignore "not exist" error
	l, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}
	// Restrict socket to owner only — prevents local process injection
	if err := os.Chmod(socketPath, 0600); err != nil {
		l.Close()
		return nil, fmt.Errorf("chmod socket: %w", err)
	}
	return &Server{listener: l, path: socketPath}, nil
}

// Accept waits for the sidecar to connect.
func (s *Server) Accept() (*Conn, error) {
	conn, err := s.listener.Accept()
	if err != nil {
		return nil, err
	}
	return &Conn{conn: conn}, nil
}

// Close closes the listener and removes the socket file.
func (s *Server) Close() error {
	err := s.listener.Close()
	_ = os.Remove(s.path) // clean up socket file
	return err
}

// StartSidecar launches the Node.js sidecar process.
// If primary is true, the sidecar watches voice states and sends channel lists.
// Non-primary sidecars only handle audio bridging (JOIN/LEAVE/audio).
func StartSidecar(sidecarDir, socketPath, token, guildID string, primary bool) (*exec.Cmd, error) {
	cmd := exec.Command("node", "index.mjs")
	cmd.Dir = sidecarDir
	env := append(os.Environ(),
		"IPC_SOCKET_PATH="+socketPath,
		"DISCORD_BOT_TOKEN="+token,
		"DISCORD_GUILD_ID="+guildID,
	)
	if primary {
		env = append(env, "SIDECAR_PRIMARY=true")
	}
	cmd.Env = env
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start sidecar: %w", err)
	}
	return cmd, nil
}
