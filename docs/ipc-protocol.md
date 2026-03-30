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
| USER_JOIN | 0x03 | Sidecar → Bridge | empty (user started speaking in bridged VC) |
| USER_LEAVE | 0x04 | Sidecar → Bridge | empty (user stopped speaking in bridged VC) |
| READY | 0x05 | Sidecar → Bridge | empty |
| SHUTDOWN | 0x06 | Either direction | empty |
| JOIN_CHANNEL | 0x07 | Bridge → Sidecar | channel_id as uint64 LE (8 bytes) |
| LEAVE_CHANNEL | 0x08 | Bridge → Sidecar | empty |
| VOICE_STATE | 0x09 | Sidecar → Bridge | channel_id as uint64 LE (8 bytes, 0 = left all) |

## Flow

1. Go bridge starts, creates Unix socket, listens
2. Go bridge spawns Node.js sidecar with guild ID + bot token
3. Sidecar connects to IPC, logs in to Discord gateway, sends READY
4. Sidecar watches voice states guild-wide, sends VOICE_STATE for every join/leave/move
5. Bridge decides which channel to bridge, sends JOIN_CHANNEL
6. Sidecar joins the voice channel, starts receiving/sending audio
7. Audio flows: AUDIO_FROM_DISCORD (per-user Opus) and AUDIO_TO_DISCORD (mixed Opus)
8. When channel empties, bridge sends LEAVE_CHANNEL
9. Sidecar leaves the voice channel, stops audio, waits for next JOIN_CHANNEL
10. On shutdown, either side sends SHUTDOWN

## Notes

- user_id is Discord snowflake as uint64 LE
- AUDIO_TO_DISCORD user_id is 0 (bot's own stream, goes to all)
- VOICE_STATE user_id is the Discord user who changed state
- Opus frames are 20ms, 48kHz, typically 3-200 bytes
- Sidecar does NOT auto-join any channel — waits for JOIN_CHANNEL
- One bot can only be in one voice channel per guild at a time
