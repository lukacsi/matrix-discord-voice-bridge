# Notes — livekit-discord-bridge

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
