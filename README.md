# Matrix Discord Voice Bridge

Bidirectional voice bridge between Discord and Matrix, powered by LiveKit. Works alongside [mautrix-discord](https://github.com/mautrix/discord) to provide a complete Discord-to-Matrix migration path.

## Vision

Soft-migrate Discord servers to Matrix. Text channels are already bridged by mautrix-discord — this bridge adds voice. Discord users stay on Discord, Matrix users use Element Call, and both hear each other. The goal: offer your community the freedom of self-hosted Matrix while retaining Discord access for users who don't want to switch.

## Features

- **Bidirectional audio** — Discord voice channels bridged to Matrix voice rooms via LiveKit
- **mautrix-discord integration** — voice rooms appear alongside text channels in the guild Space hierarchy
- **Matrix-triggered** — audio bridge starts when a Matrix user joins the voice room, not when Discord users are in VC
- **Discord presence** — Discord users shown in Matrix voice rooms (m.call.member) even without active bridge
- **Puppeting** — Discord users get proper names + avatars in Matrix; Matrix users shown via bot nickname in Discord
- **Multi-bot** — N Discord bot tokens = N concurrent voice channels (Discord limits 1 VC per bot)
- **Hot bot management** — add/remove Discord bots at runtime via Matrix DM commands
- **SQLite persistence** — room mappings and bot tokens survive restarts without scanning Matrix state
- **Admin DM interface** — `!status`, `!rooms`, `!bots`, `!bot-add`, `!bot-remove`, `!join`, `!leave`, `!log-level`
- **Resilient** — IPC timeouts, bounded shutdown, sidecar crash recovery, Matrix API retry, LiveKit reconnect callbacks

## Architecture

```
Discord Voice ←→ Node.js Sidecar ←→ Unix IPC ←→ Go Bridge ←→ LiveKit ←→ Element Call
                 (discord.js)                    (mautrix-go)
```

- **Go bridge** — main process. Manages slots, Matrix signalling, LiveKit connections, SQLite state
- **Node.js sidecar** — one per Discord bot. Handles Discord voice via discord.js + @discordjs/voice
- **IPC** — binary protocol over Unix socket. Audio frames, voice states, channel control
- **LiveKit** — WebRTC SFU. Element Call connects to the same LiveKit room as the bridge

### Why Node.js sidecar?

Discord requires [DAVE E2EE](https://daveprotocol.com/) for all voice connections. I tried getting [godave](https://github.com/nicholasgasior/godave) (Go CGO bindings to libdave) working but couldn't — the repo is abandoned and the C++ build dependencies were a nightmare. The only working third-party DAVE implementation is [davey](https://github.com/nicholasgasior/davey) (Rust + NAPI-RS), used by discord.js. So the sidecar exists purely to handle Discord voice. If someone gets godave or equivalent CGO bindings working, the sidecar can be eliminated entirely — the Go bridge is already structured to replace it.

### Appservice registration

The bridge shares [mautrix-discord](https://github.com/mautrix/discord)'s appservice token (`as_token`) to reuse its Discord user puppets (`@discord_<id>:server`). This is permitted by the Matrix spec — multiple processes can use the same token. The bridge's bot user (`@discord_voice_bridge`) should be added to mautrix-discord's registration namespace, or you can create a separate appservice registration with the same token.

For **standalone mode** (without mautrix-discord), the bridge would need its own registration, guild Space creation, and user puppets under a separate namespace (e.g., `@discordvoice_*`).

## Quick Start

### You need

1. **A Matrix homeserver** (Synapse) with [mautrix-discord](https://github.com/mautrix/discord) bridging your Discord guild
2. **A LiveKit server** with [lk-jwt-service](https://github.com/nicholasgasior/lk-jwt-service) — Element Call uses this for voice
3. **A Discord bot** added to your guild with `Connect` + `Speak` voice permissions
4. **The appservice token** (`as_token`) from mautrix-discord's registration — the bridge reuses it to puppet Discord users

### Build & run

```bash
git clone https://github.com/lukacsi/matrix-discord-voice-bridge
cd matrix-discord-voice-bridge

# Install sidecar dependencies
cd sidecar && npm ci && cd ..

# Build the Go binary
go build -o bridge ./cmd/bridge

# Configure — fill in your tokens
cp config.example.yaml config.yaml
vim config.yaml

# Run (use -log-level debug for troubleshooting)
./bridge
./bridge -log-level debug
./bridge -log-level trace   # per-IPC-message verbosity
```

On first run the bridge:
1. Opens `bridge.db` (SQLite) and seeds your config tokens
2. Connects the Node.js sidecar to Discord
3. Discovers mautrix-discord's guild Space via `m.bridge` state events
4. Pre-creates Matrix voice rooms for all Discord voice channels and places them in the Space
5. Starts a Matrix `/sync` loop watching for Element Call joins

When a Matrix user joins a voice room in Element Call, the bridge tells the sidecar to join the matching Discord voice channel and audio starts flowing both ways.

### Docker

```bash
docker run -v ./config.yaml:/data/config.yaml -v bridge-data:/data \
  ghcr.io/lukacsi/matrix-discord-voice-bridge:latest
```

### Helm (Kubernetes)

```bash
helm install voice-bridge \
  oci://ghcr.io/lukacsi/charts/matrix-discord-voice-bridge \
  --set config.discord.guild_id=YOUR_GUILD_ID \
  --set config.matrix.homeserver_url=https://matrix.example.com \
  --set config.matrix.as_token=YOUR_AS_TOKEN \
  --set config.matrix.server_name=example.com \
  --set config.livekit.url=wss://livekit.example.com \
  --set config.livekit.api_key=YOUR_KEY \
  --set config.livekit.api_secret=YOUR_SECRET \
  --set config.matrix.lk_jwt_service_url=https://lk-jwt.example.com \
  --set botTokens[0]=YOUR_DISCORD_BOT_TOKEN
```

## Configuration

```yaml
discord:
  bot_token: "single-token"        # or use bot_tokens for multi-bot
  bot_tokens:                       # N tokens = N concurrent voice channels
    - "primary-bot-token"           # first = primary (voice states + audio)
    - "audio-bot-token"             # audio only
  guild_id: "123456789012345678"

livekit:
  url: "wss://livekit.example.com"
  api_key: "devkey"
  api_secret: "secret"

matrix:
  homeserver_url: "https://matrix.example.com"
  as_token: "your-appservice-token"  # same as mautrix-discord
  server_name: "example.com"
  lk_jwt_service_url: "https://lk-jwt.example.com"
  admin_users:                       # who can DM the bot with commands
    - "@admin:example.com"

log_level: "info"                    # info, debug, trace
database: "bridge.db"               # SQLite path
```

All fields can be overridden with environment variables: `DISCORD_BOT_TOKEN`, `LIVEKIT_URL`, `MATRIX_AS_TOKEN`, etc.

## Admin Commands

DM `@discord_voice_bridge:your-server` on Matrix:

| Command | Description |
|---------|-------------|
| `!status` | Uptime, active bridges, slot usage |
| `!rooms` | List voice channel to Matrix room mappings |
| `!bots` | List bot slots and their status |
| `!bot-add <token>` | Hot-add a Discord bot (persisted to DB) |
| `!bot-remove <slot>` | Remove a bot slot |
| `!join <channel>` | Manually bridge a voice channel |
| `!leave [channel]` | Stop bridging (all if no name) |
| `!log-level <level>` | Change log level at runtime |
| `!sync-db` | Rebuild DB from Matrix state events |

## How It Works

1. **Startup** — discovers mautrix-discord's guild Space, pre-creates Matrix voice rooms for all Discord voice channels, places them in the Space hierarchy
2. **Presence** — Discord voice state changes are reflected as `m.call.member` events in the corresponding Matrix voice room. Matrix clients show who's in the Discord VC.
3. **Bridge trigger** — when a Matrix user joins a voice room (via Element Call), the bridge tells the sidecar to join that Discord voice channel. Audio starts flowing.
4. **Audio** — Discord-to-Matrix: Opus frames passed through to LiveKit per-user. Matrix-to-Discord: LiveKit subscriber mixes and encodes to Opus for Discord playback.
5. **Leave** — when all Matrix users leave, the audio bridge stops (5s debounce). Discord presence stays visible.

## Known Limitations

- **No video/screen share** — Discord's bot API is voice-only; bots cannot send or receive video
- **Must rejoin after restart** — the bridge skips stale `m.call.member` events on startup (4h expiry window makes active vs stale indistinguishable). Future: query LiveKit for active participants to auto-reconnect.
- **1 VC per bot per guild** — Discord API constraint (gateway-level, not a library limit), mitigated by multi-bot pool
- **Single guild** — currently hardcoded to one guild. Multi-guild is architecturally supported (one bot token handles all guilds, SQLite stores per-guild state) but not yet implemented.
- **Node.js dependency** — required for Discord DAVE E2EE via davey. Goal: replace with pure Go CGO bindings to libdave.

## E2EE

Element Call has two E2EE layers:

1. **Matrix-level** — encryption keys distributed via `m.call.encryption_keys` to-device messages (Olm/Megolm transport)
2. **WebRTC-level** — AES-GCM frame encryption via Insertable Streams, before DTLS-SRTP

The bridge currently works with **unencrypted Matrix rooms**. In a self-hosted environment where the LiveKit SFU is trusted, this is the recommended approach — create voice rooms without Matrix encryption enabled.

Supporting E2EE in encrypted rooms would require the bridge to:
- Participate in Matrix to-device key exchange (`RTCEncryptionManager`)
- Encrypt/decrypt WebRTC frames with AES-GCM
- Handle key rotation on participant join/leave

This is planned but not yet implemented.

## Status

This project started as something I needed for my friends group — soft-migrating a Discord server to a self-hosted Matrix setup without splitting the group. It works for my use case but hasn't been battle-tested beyond that. There are rough edges.

Most of this code was written with AI assistance (Claude). I designed the architecture and tested everything, but I want to be upfront about that. The code has been through multiple review rounds and works for my setup.

If you find this useful, have ideas, or want to contribute — issues and PRs are very welcome. I'm open to any suggestions on architecture, features, or direction.

## License

GNU Affero General Public License v3.0 — see [LICENSE](LICENSE).
