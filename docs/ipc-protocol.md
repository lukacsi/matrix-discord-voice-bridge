# Discord Voice Sidecar IPC Protocol

Binary protocol over Unix domain socket. Little-endian.

## Frame Format

```
[1 byte type][8 byte user_id][2 byte payload_len][payload]
```

Total header: 11 bytes. Max payload: 65535 bytes.

## Message Types

| Type | Value | Direction | Payload |
|------|-------|-----------|---------|
| AUDIO_FROM_DISCORD | 0x01 | Sidecar → Bridge | Opus frame (raw bytes) |
| AUDIO_TO_DISCORD | 0x02 | Bridge → Sidecar | Opus frame (raw bytes) |
| USER_JOIN | 0x03 | Sidecar → Bridge | empty |
| USER_LEAVE | 0x04 | Sidecar → Bridge | empty |
| READY | 0x05 | Sidecar → Bridge | empty |
| SHUTDOWN | 0x06 | Either direction | empty |

## Flow

1. Go bridge starts, creates Unix socket, listens
2. Go bridge spawns Node.js sidecar as child process
3. Sidecar connects to Unix socket
4. Sidecar joins Discord voice channel, sends READY
5. Sidecar sends USER_JOIN for each existing user
6. Audio flows: AUDIO_FROM_DISCORD (per-user Opus) and AUDIO_TO_DISCORD (mixed Opus)
7. On shutdown, either side sends SHUTDOWN

## Notes

- user_id is Discord snowflake as uint64 LE
- AUDIO_TO_DISCORD user_id is 0 (bot's own stream, goes to all)
- Opus frames are 20ms, 48kHz, typically 3-200 bytes
- At 50fps per user, bandwidth is ~100KB/s per user over IPC
