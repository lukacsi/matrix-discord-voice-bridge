# Notes — livekit-discord-bridge

## 2026-03-31 — SQLite, hot bot management, Dockerfile, CI/CD

**Context:** Bridge needed persistent state, runtime bot management, and containerized deployment.

**Decisions:**
- `modernc.org/sqlite` (pure Go, no CGo) with WAL mode for concurrent reads
- DB is source of truth for rooms and bots; Matrix state scan as fallback for fresh DB
- Single container: Go binary + Node.js sidecar (shared /data volume for DB)
- Helm chart with Secret for config, PVC for data persistence
- CI runs tests on push; release builds Docker + pushes Helm OCI on git tag

**Done:**
- `pkg/store`: SQLite persistence for bots, rooms, guilds (84% test coverage)
- !bot-add / !bot-remove via Matrix DM — hot sidecar lifecycle + DB persistence
- !sync-db — rebuild DB from Matrix state events
- DiscoverExistingRooms: DB-first (instant startup), Matrix fallback
- Dockerfile: multi-stage (Go 1.26 + Node 22), /data volume
- Helm chart: deployment, secret, PVC, configurable values
- GitHub Actions: ci.yaml (test), release.yaml (Docker + Helm OCI publish)
- Two review rounds fixing concurrency (slot races, sentinel panic, lock ordering)

## 2026-03-30 — Resilience, logging, DM commands

**Context:** Bridge was functional but fragile — no timeouts, silent failures, inconsistent logs, no runtime control.

**Done:**
- Resilience: IPC read/write deadlines, bounded shutdown (SIGTERM→5s→SIGKILL), sidecar crash recovery (dead slot sentinel), LiveKit OnDisconnected callbacks, Matrix API retry (3 attempts + timeouts), non-blocking WriteOpus
- Uniform logging: consistent field names (discord_user, matrix_room, etc.), 3 levels (info/debug/trace via -log-level flag), pion log filter, sidecar slot prefix
- Matrix DM command interface: !status, !rooms, !bots, !join, !leave, !log-level, !help. Admin-only via config. Auto-joins DM rooms on invite. Bot display name set at startup.
- Bug fixes: voice join failure (disconnect handler timing), channel switch slot preemption, debounced stop, camera toggle filter, LiveKit room alias hash mismatch

## 2026-03-30 — Production-quality bridge rebuild

**Context:** Bridge had basic dynamic mirroring (voice state → auto-bridge) but rooms weren't in the Space hierarchy, rooms duplicated on restart, audio bridged whenever Discord users were in VC (not when Matrix users joined), and only supported 1 bot token.

**Decisions:**
- Standalone companion bridge (not merged into mautrix-discord) — shares AS token, discovers Spaces via m.bridge state events
- No database — room persistence via m.bridge state events on Matrix rooms
- Matrix-triggered bridging: audio only flows when a Matrix user joins the voice room via Element Call
- Discord users always shown as presence (m.call.member) regardless of bridge state
- Multi-bot pool for concurrent VC bridging (1 bot = 1 VC, Discord limit)
- Debounced bridge stop (5s) to handle Element Call session refreshes
- Initial sync skipped — stale m.call.member can't be distinguished from active (4h expiry window)

**Done:**
- Guild Space + category sub-Space discovery and placement
- Startup channel sync (34 voice channels pre-created as Matrix rooms)
- /sync loop watching m.call.member for bridge triggers
- Multi-bot sidecar pool (N tokens, primary + audio-only roles)
- Puppeting both directions (Discord→Matrix names+avatars, Matrix→Discord bot nickname)
- YAML config with env var overlay
- Parallel room discovery (10 workers), parallel join (Discord + LiveKit)
- Membership renewal (3h interval for 4h expiry)
- Security: socket permissions, avatar hash validation, HTTP timeouts
- Correctness: expires_ts absolute epoch, livekit_alias hash, debounced stop
- Pion log filtering, structured stats
