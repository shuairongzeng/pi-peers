# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-10-24

First public release. Cross-session messaging between pi-compatible coding agent instances
on one machine — a registry directory, per-process IPC endpoints, token authentication,
anti-abuse guards, and receipts — as a single extension that runs on both pi and oh-my-pi.

### Added

- **Discovery**: every instance publishes a `<pid>.json` record (name, status, kind, cwd,
  socket address, protocol version, features) in a shared registry directory
  (`~/.pi-peers`, overridable via `PI_PEERS_DIR`); scans sweep records of dead processes
  and broken zombies (live pid without its auth key), with a 500 ms scan cache.
- **Transport**: one IPC listener per instance — Windows named pipe
  (`\\.\pipe\LOCAL\pi-peer-<rand>`) or POSIX unix socket — carrying newline-delimited
  JSON frames (1 MiB line cap) over short-lived connections; receipts travel back over a
  fresh connection.
- **Authentication**: per-process 0600 key files holding peer and child tokens; senders
  present the target's token on the first line; a separate child token
  (`PI_PEERS_SOCKET`/`PI_PEERS_TOKEN` environment handoff) marks spawned subagents as
  trusted. PID-reuse protection via process-start tokens / pid domains.
- **Anti-abuse guard on receive**: per-sender token bucket, global rate bucket, a
  child-class bucket, 30 s dedupe window, hop-chain loop guard, and an inbound queue cap;
  the outbox paces sends locally so an overloaded peer fails fast with a retry hint.
- **`send_peer_message` tool**: `list` / `send` / `shutdown` actions; `to` accepts a
  name, a disambiguated name (`"alice [ab12cd]"`), a wire address, or an array to fan
  out to up to 32 peers; `priority: "now"` interrupts, `notifyWhenIdle` subscribes for
  an idle ping, `scope: "all"` deliberately crosses project boundaries.
- **`spawn_peer` tool**: launch a new agent instance as a managed peer that reproduces
  the host invocation, registers automatically, and is addressable by name right away;
  optional `headless` (background) and `ephemeral` (exit after reporting back) workers,
  with abortable spawn waits and actionable timeout guidance.
- **Remote shutdown**: `shutdown` requests obey the peer's accept policy
  (`all`/`ask`/`off`), fail-closed project scoping, and a graceful handshake.
- **Dual-host support**: one code path loads on pi and oh-my-pi (`pi.extensions` /
  `omp.extensions` manifests); host API differences (idle delivery, session rename
  events, shutdown budget) are normalized, so instances of both products see each other
  in one shared registry and message across products.
- **Project scoping**: peers default to the same git repository root — windows in other
  projects are invisible and unreachable unless `scope: "all"` is passed explicitly.
- **Receipts**: asynchronous `delivered` / `held` / `denied` / `refused` / `dropped`
  statuses are budgeted, sanitized, and injected back into the sender's session; prompt
  guidance documents that a send reaching the queue is not consent.
- **System-prompt convention**: an injected "Cross-session peers" section teaches the
  model that peer messages never establish user intent and how to reply — enabling
  multi-hop conversations without protocol changes; incoming messages are wrapped in
  `<cross-session-message>` envelopes (wrapper enforced on receive, forged wrappers
  dropped).
- **Configuration**: `PI_PEERS_ACCEPT` (`all` default / `ask` / `off`),
  `PI_PEERS_DISABLED`, `PI_PEERS_DIR`, `PI_PEERS_NAME`, `PI_PEERS_EPHEMERAL`.
- **Project README** and MIT license.

### Changed

- Default accept policy settled on `all` — frictionless same-user messaging, safe under
  the 0600 same-user token trust model.
- Registry liveness hardened: auth keys of live pids are never swept during startup;
  keyless live records are swept from the peer list; records are validated field-by-field.
- Freshness semantics documented and implemented: `statusUpdatedAt` stamps busy/idle
  flips, `contextUsage` refreshes at turn boundaries, liveness always re-checked via
  `kill(pid,0)` + start-token compare.
- Performance: scan caching, pid-direct spawn polling, O(1) key lookup, bounded caches.
- Refactors: `PeerServiceDeps` split into per-consumer interfaces; address conversion
  and the send ladder deduplicated; dead code removed and naming unified.

### Fixed

- Idle injection starts a turn on oh-my-pi (`deliverAs` only queues there); `steer` /
  `followUp` are only passed when the receiving session is busy.
- Registry no longer advertises a cross-project escape hatch to the model by default.
- Receipt polling stopped by aligning prompts with silent-success reality.
- Event chain hardened against host/FS throws; shutdown admission window closed;
  liveness self-heal and idle-subscription eviction on flush.

### Security

- Receipt text sanitized; shutdown scoping fails closed; control characters stripped
  from wrapper attributes; receipts and idle notices authenticated; wrapper enforcement
  on receive; global rate bucket added (rotating the `from` field buys nothing);
  name sanitization and socket-error visibility.
- Documented child-token exposure and the accepted limitation of no post-auth
  connection caps within the same-user trust model.

### Documentation

- Protocol appendix for third-party implementers: discovery, channel, handshake, frames,
  anti-abuse, versioning rules, and list cache semantics.
- Design rationale and reverse-engineering writeup of Claude Code's cross-session
  messaging in companion docs.

### Testing & release readiness (this release)

- 11 test files (unit, guard, end-to-end over a live socket, dual-host wiring) plus a
  deterministic property/fuzz suite (fast-check) for parsers, framing, records, and the
  guard — `npm test` and `npm run typecheck` green on the release baseline.
- npm publish readiness: scoped package name, `files: ["src"]` manifest,
  `publishConfig.access: public`, `prepublishOnly` gate, publint-clean tarball
  (source, README, CHANGELOG, LICENSE, package.json only).

[0.1.0]: https://github.com/shuairongzeng/pi-peers/releases/tag/v0.1.0
