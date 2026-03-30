# Architecture & Technical Notes

Notes for contributors — things that took time to figure out.

## Component Overview

```
cmd/bridge/main.go        Entry point, sidecar lifecycle, read loops
pkg/bridge/manager.go     Slot pool, bridge lifecycle, state machine
pkg/matrix/signalling.go  m.call.member events, /sync loop, profile management
pkg/matrix/commands.go    Matrix DM admin interface
pkg/livekit/manager.go    Per-user LiveKit publisher (Discord→Matrix audio)
pkg/livekit/subscriber.go LiveKit subscriber + mixer (Matrix→Discord audio)
pkg/ipc/protocol.go       Binary IPC protocol over Unix socket
pkg/store/store.go        SQLite persistence (rooms, bots, guilds)
pkg/config/config.go      YAML + env var config
pkg/types/types.go        Shared types (breaks import cycle bridge↔matrix)
sidecar/index.mjs         Node.js Discord voice handler
```

## Hard-Won Lessons

### LiveKit Room Naming

lk-jwt-service computes the LiveKit room name as `SHA256(matrixRoomID + "|" + slotID)` base64-encoded. The bridge MUST use `LiveKitRoomAlias()` — NOT the raw Matrix room ID. If these don't match, the bridge and Element Call join different LiveKit rooms and audio doesn't flow.

### m.call.member Format

```json
{
  "application": "m.call",
  "scope": "m.room",
  "device_id": "VOICE_<discord_snowflake>",
  "membershipID": "@discord_<id>:server:VOICE_<id>",
  "expires_ts": 1711839600000,
  "foci_preferred": [{"type": "livekit", "livekit_alias": "<hashed>", "livekit_service_url": "..."}]
}
```

- `expires_ts` MUST be absolute epoch milliseconds (not relative duration)
- State key format: `_@user:server_DEVICE_m.call`
- Empty content `{}` = leave
- Element Call refreshes sessions by sending leave+join rapidly — the bridge debounces stops by 5 seconds

### Discord DAVE E2EE

Discord mandates DAVE for all voice. [godave](https://github.com/disgoorg/godave) provides Go CGO bindings to libdave but the build chain (libdave C++ + vcpkg + boringssl + mlspp) is complex. We use discord.js + [davey](https://github.com/nicholasgasior/davey) (Rust/NAPI) for the sidecar because it was the fastest path to working DAVE. Getting godave to build cleanly would eliminate the sidecar entirely.

### Discord Voice Constraints

- **1 voice channel per bot per guild** — gateway-level limit, not a library bug
- **Bots cannot send/receive video** — API doesn't expose video streams
- **Voice join needs time** — `entersState(Ready)` can take 3-10s; don't register disconnect handlers before Ready (they fire during negotiation)
- **Stale connections** — always `getVoiceConnection(guildId)?.destroy()` before creating a new one

### mautrix-discord Integration

- Guild Spaces are found via `m.bridge` state events with key `fi.mau.discord://discord/{guild_id}`
- Category sub-Spaces have key `fi.mau.discord://discord/{guild_id}/{category_id}`
- Alias-based discovery (`#discord_<id>:server`) does NOT work — mautrix-discord doesn't create aliases
- We share the appservice token to reuse Discord user puppets (`@discord_<id>:server`)
- The bridge bot is `@discord_voice_bridge:server` — separate from mautrix-discord's `@discordbot`

### Voice Room Type

Cinny shows voice UI for rooms with `m.room.create` type `org.matrix.msc3417.call`. NOT `org.matrix.msc3815.voice` (that's a different MSC). Set `state_default: 0` in power levels so ghost users can send m.call.member.

### Concurrency Patterns

- `Manager.mu` protects all shared state (slots, bridges, rooms, timers)
- Always capture `*SidecarSlot` pointer under the lock — `m.slots` can be reallocated by `AddBot`
- `stopBridgeForChannel` preserves dead slot sentinel (`^uint64(0)`) — don't reset to 0
- HTTP calls (GetDisplayName, EnsureJoined) must happen OUTSIDE the lock
- Debounce timers run in their own goroutines — cancel them in `Close()` and `HandleSlotDeath()`
- `AddBot` TOCTOU on slot index: concurrent adds get same index, socket conflict is the guard

### IPC Protocol

11-byte header: `[type:1][userID:8][payloadLen:2]` + payload. See `docs/ipc-protocol.md`.

Audio frames (AUDIO_FROM/AUDIO_TO) are the hot path — 50 frames/sec per speaking user. These are excluded from trace logging. Non-blocking `WriteOpus` drops frames during LiveKit connect to avoid blocking the IPC read loop.
