# pi-peers

Cross-session messaging between pi-compatible coding agent instances on one machine. Each running instance publishes a registry record (pid, name, status, working directory) in a shared directory, listens on a local IPC endpoint (Windows named pipe / POSIX unix socket), and exchanges newline-delimited JSON frames authenticated with a per-process token. Agents discover each other through the `send_peer_message` tool (or `/peers`), send messages that arrive in the target session as user-role `<cross-session-message>` blocks, and reply by copying the sender's `from` address. The plugin loads on both **pi** and **oh-my-pi** from the same single code path.

## Install

### pi

Copy or clone this repository into `~/.pi/agent/extensions/pi-peers`. pi discovers packages in subdirectories of the extensions dir by reading each subdirectory's `package.json` manifest; this package's `"pi"` manifest field points at `./src/index.ts`, so nothing else is needed:

```sh
git clone <this-repo> ~/.pi/agent/extensions/pi-peers
cd ~/.pi/agent/extensions/pi-peers && npm install
```

### oh-my-pi

Copy or clone into `~/.omp/agent/extensions/pi-peers`. oh-my-pi reads either the `"omp"` or `"pi"` manifest field (this package declares both, both pointing at `./src/index.ts`):

```sh
git clone <this-repo> ~/.omp/agent/extensions/pi-peers
cd ~/.omp/agent/extensions/pi-peers && npm install
```

Run `npm install` once after cloning; the hosts load TypeScript directly and need no build step.

## Configuration

| Variable | Meaning |
| --- | --- |
| `PI_PEERS_ACCEPT` | Accept policy for inbound peer messages: `ask` (prompt the user), `all` (accept silently), `off` (refuse). Default: `ask` when a dialog-capable UI is available, else `off`. |
| `PI_PEERS_DISABLED` | Set to `1` (or `true`/`yes`) to disable the extension entirely — no tool, no command, no listener. |
| `PI_PEERS_DIR` | Override the shared registry directory (default `~/.pi-peers`). All hosts read the same dir, so pi and oh-my-pi instances see each other. |
| `PI_PEERS_SOCKET` / `PI_PEERS_TOKEN` | Parent handoff: a peer that spawns child processes (e.g. subagents) exports its wire address and child token so children can message it back. Read-only for users. |

## Usage

- `/peers` — list live peer sessions on this machine (name, status, kind, cwd, disambiguation ref).
- `send_peer_message` tool (model-facing) — `action: "list"` to discover peers; `action: "send"` with `to` (a peer name, `"name [ref]"` to disambiguate, or a wire address from an incoming message) and `message`. Optional `priority: "now"` interrupts the peer immediately; `notifyWhenIdle: true` asks for a one-shot ping when the peer next goes idle.
- Incoming peer messages appear as user-role text wrapped in `<cross-session-message from="ADDRESS" from-name="NAME">…</cross-session-message>`. The reply convention is to call `send_peer_message` with `to` set to the message's `from` address or the sender's name.
- Success only means the message reached the peer's queue; a receipt (`delivered` / `held` / `denied` / `expired` / `refused` / `dropped`) follows asynchronously and is injected into the sender's context.

## Security model

- **Authentication**: every process publishes a 0600 key file (`<pid>.<sha256(socketPath)>.key`) containing its peer and child tokens; inbound connections must present the matching token on the first line or they are dropped.
- **Anti-abuse**: inbound user frames pass a token-bucket rate limit, a dedupe window, a hop-chain guard (loop and runaway detection), and a queue cap; the outbox paces sends locally so a peer that would drop us fails fast with a merge-and-retry hint instead.
- **No central server**: the filesystem is the only shared truth; the registry is self-healing (records of dead pids and their key files are swept on every scan).

## Known host differences

- **oh-my-pi has no `session_info_changed`**: renames are not broadcast to other peers. A renamed oh-my-pi session republishes its (derived) name on the next `session_start`; until then, peers see the old name.
- **Shutdown budget**: hosts give `session_shutdown` handlers roughly 2 seconds before continuing; registry unpublish and key deletion are best-effort synchronous, so this is normally ample, but a hung close could be cut off.

## Development

```sh
npm install        # once
npm test           # vitest run
npm run typecheck  # tsc --noEmit
```
