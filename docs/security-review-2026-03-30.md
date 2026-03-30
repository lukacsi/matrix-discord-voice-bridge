# Security Review — livekit-discord-bridge
Date: 2026-03-30
Reviewer: Claude Code (security role)

---

## Summary

7 findings across 3 severity levels. No CRITICAL issues. The codebase is generally
well-structured for a personal homelab project. The main concerns are: a real guild ID
in a committed example file, 6 high-severity CVEs in the sidecar's dependency tree,
an unbounded IPC payload size, an unrestricted HTTP client for avatar downloads, and
impersonation scope that is intentionally broad.

---

## Findings

### HIGH-1 — Real Guild ID Committed in config.example.yaml

**File:** `config.example.yaml:4`
**Severity:** HIGH

The example config contains `guild_id: "298535878702137344"` — this is the actual production
guild ID, not a placeholder. Discord guild IDs are not secret by themselves, but committing
operational identifiers means anyone who clones the repo knows the exact guild this bridge
targets. Combined with a leaked token or appservice credential, the guild ID completes the
attack picture.

**Remediation:** Replace with a clearly fictional value:

```yaml
guild_id: "123456789012345678"
```

---

### HIGH-2 — 6 High-Severity CVEs in sidecar/node_modules (undici via discord.js)

**File:** `sidecar/package.json`, `sidecar/package-lock.json`
**Severity:** HIGH

`npm audit` reports 7 vulnerabilities (1 moderate, 6 high) all rooted in `undici`, pulled
in by `discord.js`. The high findings include:

| CVE advisory | Issue |
|---|---|
| GHSA-g9mf-h72j-4rw9 | Unbounded decompression chain → resource exhaustion |
| GHSA-f269-vfmq-vjvj | 64-bit WebSocket length overflow → crash |
| GHSA-2mjp-6q6p-2qxm | HTTP Request/Response Smuggling |
| GHSA-vrm6-8vpv-qv8q | Unbounded memory in WebSocket permessage-deflate |
| GHSA-v9p9-hfj2-hcw8 | Unhandled exception on invalid server_max_window_bits |
| GHSA-4992-7rv2-5pvq | CRLF Injection via `upgrade` option |

The sidecar connects outbound to Discord (not receiving untrusted HTTP), which limits
exploitability from the outside. However, the resource exhaustion and memory unbounding
issues could be triggered by Discord's servers in a degraded or adversarial scenario.

**Remediation:** `npm audit fix --force` will install `discord.js@13.17.1` (breaking change).
Evaluate whether the API surface you use is compatible with 13.x, then pin to a patched
release. Alternatively, check whether a non-breaking `discord.js` patch has landed.

---

### HIGH-3 — Unbounded IPC Payload Size (Go reader)

**File:** `pkg/ipc/protocol.go:58`
**Severity:** HIGH

```go
payloadLen := binary.LittleEndian.Uint16(header[9:11])
if payloadLen > 0 {
    msg.Payload = make([]byte, payloadLen)
```

`payloadLen` is a `uint16`, so it caps at 65535 bytes. This is fine for audio frames
(Opus frames are <4000 bytes) but a malicious or buggy sidecar could send
`payloadLen=65535` for every message type, causing the Go process to allocate 64 KB
per message and hold it until GC. Under sustained conditions (e.g. compromised sidecar
process) this causes unbounded memory growth.

The IPC socket is restricted to owner-only (`chmod 0600` at `protocol.go:109`), so
the threat is a compromised or replaced sidecar binary, not an external attacker.

**Remediation:** Add a per-type maximum and reject oversized payloads:

```go
const maxPayloadByType = map[byte]uint16{
    MsgAudioFromDiscord: 4000,
    MsgAudioToDiscord:   4000,
    MsgVoiceState:       512,
    MsgChannelList:      512,
    MsgUserInfo:         512,
    MsgMatrixUsers:      2048,
    // ...
}

payloadLen := binary.LittleEndian.Uint16(header[9:11])
if max, ok := maxPayloadByType[msg.Type]; ok && payloadLen > max {
    return nil, fmt.Errorf("oversized payload for type 0x%02x: %d > %d", msg.Type, payloadLen, max)
}
```

---

### HIGH-4 — Unrestricted HTTP Client for Avatar Downloads (SSRF risk)

**File:** `pkg/matrix/signalling.go:253`
**Severity:** HIGH

```go
func (s *Signaller) uploadAndSetAvatar(ctx context.Context, ..., url string) {
    resp, err := http.Get(url)
```

The `url` is constructed from the `avatarHash` field sent over IPC from the Node.js
sidecar (`signalling.go:246`):

```go
avatarURL := fmt.Sprintf("https://cdn.discordapp.com/avatars/%d/%s.png?size=256",
    discordUserID, avatarHash)
```

The format string anchors the host to `cdn.discordapp.com`, which prevents the most
obvious SSRF. However, `avatarHash` is a free-form string from the sidecar with no
validation. An attacker who controls the sidecar process (or injects a crafted
`MSG_USER_INFO` IPC message) could send a value like
`../../../../../../etc/passwd%3f` or use URL fragment tricks depending on how the
Go `http` package interprets the constructed URL.

More practically: the `http.Get` call uses the default Go `http.Client` with no
timeout, no redirect limit override, and no TLS certificate pinning for the CDN.
A slow Discord CDN response hangs a goroutine indefinitely.

**Remediation:**

1. Validate `avatarHash` to only allow characters in the expected Discord hash format
   (`[0-9a-f]{32}` or similar) before building the URL.
2. Add a timeout to the HTTP client:

```go
var avatarClient = &http.Client{Timeout: 10 * time.Second}
// replace http.Get(url) with avatarClient.Get(url)
```

3. Enforce a redirect policy that stays on `cdn.discordapp.com`:

```go
avatarClient = &http.Client{
    Timeout: 10 * time.Second,
    CheckRedirect: func(req *http.Request, via []*http.Request) error {
        if req.URL.Host != "cdn.discordapp.com" {
            return fmt.Errorf("avatar redirect to unexpected host: %s", req.URL.Host)
        }
        return nil
    },
}
```

---

### MEDIUM-1 — IPC Socket in World-Writable /tmp (Default Path)

**File:** `pkg/config/config.go:59`, `pkg/ipc/protocol.go:103-112`
**Severity:** MEDIUM

The default socket path is `/tmp/discord-voice-bridge.sock`. While the socket itself
is `chmod 0600` after creation, `/tmp` has the sticky bit set but is world-readable.
Any local process can observe the socket filename and attempt a TOCTOU attack:
delete the socket file before the bridge creates it (between `os.Remove` and
`net.Listen`), then create a socket at the same path with world-readable permissions,
causing the sidecar to connect to the attacker's socket instead.

This is low-probability on a single-user homelab but relevant if the bridge runs as a
system service on a shared host.

**Remediation:** Use a runtime-specific directory for the socket:

```go
// In config.go default:
SocketPath: fmt.Sprintf("/run/user/%d/discord-voice-bridge.sock", os.Getuid()),
// or create a dedicated dir: /run/discord-bridge/<pid>/
```

Alternatively, document that `IPC_SOCKET_PATH` should always be set to a path in a
non-world-writable directory when deploying.

---

### MEDIUM-2 — Appservice Impersonates @discordbot Without Scope Guard

**File:** `pkg/bridge/manager.go:605-607`, `pkg/bridge/manager.go:665-667`
**Severity:** MEDIUM

The bridge uses `s.signaller.Intent(botMXID)` where `botMXID` is
`@discordbot:<server_name>` — the mautrix-discord bridge bot. This impersonation is
intentional (to add rooms to Spaces it controls), but there is no guard ensuring the
constructed MXID matches what mautrix-discord actually registered.

If `cfg.Matrix.ServerName` is misconfigured, the bridge will attempt to act as an
unrelated or non-existent user. More importantly, the appservice token grants the
ability to act as *any* user in the namespace. If the namespace regex in the
registration is wide (e.g. `.*`), the bridge can impersonate any Matrix user on the
homeserver — including real human accounts.

**Remediation:**

1. Add a config field `matrix.discord_bot_mxid` that explicitly names the
   mautrix-discord bot user ID, rather than constructing it from server name.
2. Document the minimum required appservice namespace in `config.example.yaml` and
   validate at startup that the token only covers `@discord_*` and
   `@discord_voice_bridge:*` namespaces.
3. Consider using a *separate* appservice registration for the voice bridge, scoped
   only to the users it needs to create, rather than sharing mautrix-discord's token.

---

### LOW-1 — public_chat Preset Creates Globally Joinable Rooms

**File:** `pkg/bridge/manager.go:527`
**Severity:** LOW

```go
Preset: "public_chat",
```

The `public_chat` preset sets `join_rule: public` and `history_visibility: shared`.
This means anyone who discovers the room ID can join the voice room and observe
Matrix signalling events (m.call.member events including LiveKit JWT service URLs and
participant identities).

For a homelab with a closed Matrix server this is low risk, but it would allow
any user on a federated homeserver to join the voice rooms and observe presence data
for all Discord users.

**Remediation:** Use `Preset: "private_chat"` or `Preset: "trusted_private_chat"`, or
explicitly set `JoinRule: "invite"` after room creation. Access control for voice
rooms should match the Discord channel's visibility model (private channels should
map to invite-only rooms).

---

## Positive Security Observations

These were explicitly checked and found to be handled correctly:

- `config.yaml` is in `.gitignore` — real credentials are not committed.
- IPC socket is `chmod 0600` immediately after creation (`protocol.go:109`).
- Avatar download is capped at 512 KB via `io.LimitReader` (`signalling.go:264`).
- No shell commands with user-controlled input — `exec.Command("node", "index.mjs")` is fully static.
- Bot token passed to sidecar via environment variable, not command-line argument.
- IPC payload parsing uses bounds checks before slicing (`manager.go:108-116`, `163-175`).
- No hardcoded passwords or tokens in committed source files.
- The `/sync` loop correctly uses `SetAppServiceUserID = true` — AS token is not used as a user token.
- `OnMatrixCallMember` correctly filters out `@discord_*` ghost users to prevent bridge loops (`manager.go:397-399`).
- Discord bot token never appears in log output.

---

## Priority Remediation Order

| # | Finding | Effort |
|---|---------|--------|
| 1 | HIGH-2: Update discord.js / undici | Low (test compatibility) |
| 2 | HIGH-4: Validate avatarHash + add HTTP timeout | Low |
| 3 | HIGH-1: Replace real guild ID in example config | Trivial |
| 4 | HIGH-3: Add per-type IPC payload size limits | Low |
| 5 | MEDIUM-1: Move socket out of /tmp | Low |
| 6 | MEDIUM-2: Separate AS registration for voice bridge | Medium |
| 7 | LOW-1: Switch room preset to private_chat | Trivial |
