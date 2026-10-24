# pi-peers performance baseline

**This is the baseline for future performance work on this package.** Date: 2026-10-24.
Machine: Windows 10 x64 (release 10.0.19045), node v24.12.0, Intel Core i7-8700 @ 3.20 GHz (12 threads).

Reproduce with:

```
npm run bench
```

The script (`bench/peers-bench.mjs`) prints the three tables below and exits with code 0.
Numbers below are from one run of that command on the machine above; repeated runs on the same
machine stay in the same band (cold scan p50 5.9–7.2 ms, send p50 0.8–1.6 ms, codec 270k–450k
ops/sec) — treat ~±40 % as run-to-run noise on this box.

## Results

### 1. Registry scan — N = 50 records + key files, all pids live

| metric                        | p50    | p95    | min    | max    |
| ----------------------------- | ------ | ------ | ------ | ------ |
| cold scan `list({fresh:true})`| 5.95 ms | 8.54 ms | 5.27 ms | 9.76 ms |
| warm cache hit `list()`       | 99 ns  | 244 ns | 93 ns  | 797 ns |

200 measured scans (+25 warmup, discarded). Fixture: this process + 49 sleeper children, each
with one `<pid>.json` record and one key file in a temp registry dir.

### 2. send() full path — real transport server + token auth + user frame + delivered receipt

| metric                     | p50    | p95    | min    | max     |
| -------------------------- | ------ | ------ | ------ | ------- |
| send() → delivered receipt | 0.90 ms | 1.86 ms | 0.57 ms | 3.11 ms |

200 iterations (+25 warmup, discarded), ~256 B message body. The timed region covers
`outbox.send()` start until the `delivered` receipt is observed on the sender: resolve → key
lookup → connect → auth line → user frame → receiver guard/inject → receipt frame back over
the wire.

### 3. Protocol codec — 1 user frame, 1481 B on the wire

| op              | ops/sec   | per-op  |
| --------------- | --------- | ------- |
| serializeFrame  | 420,631   | 2.38 µs |
| parseFrameLine  | 444,030   | 2.25 µs |

20 batches × 10,000 ops (+3 warmup batches, discarded); the reported rate is the median batch rate.

## Method

Common methodology, implemented in `bench/peers-bench.mjs`:

- **Warmup first**: every metric runs ≥10 % of its measured iteration count as warmup, discarded
  (JIT tiering, module init, filesystem cache).
- **Medians, not means**: p50 is the reported headline; p95/min/max bound the tail. Interquartile
  noise (Windows timer granularity, background load) would dominate a mean.
- **Per-call ops too cheap for the clock are batched**: warm `list()` latency and codec throughput
  are measured as batches and divided out; the median of batch results is reported.
- **Exit code 0** on success; every section also asserts correctness (entry count, receipt state,
  codec round-trip) so a broken measurement cannot pass silently.

### 1. Registry scan

- A temp dir gets 50 valid records **and** 50 key files, all pointing at **live** pids. This is
  required because `list()` self-heals: it sweeps records whose pid is dead, which would shrink
  the fixture mid-run. Fixture pids are `process.pid` plus 49 spawned sleeper children
  (`ping -n 100000 127.0.0.1` on Windows — one tick per second, no `sleep.exe` needed; `sleep
  100000` on POSIX).
- **Cold** = `list({ fresh: true })` (bypasses the 500 ms result cache) **after**
  `registry.invalidateAlive(pid)` for every pid: without that, the registry's 1.5 s liveness
  cache would skip the per-pid `kill(pid,0)` probes and the "cold" number would be a warm one.
  Each measured scan therefore re-runs the full readdir + 50 reads/parses + 50 liveness probes
  + key sweep.
- **Warm** = `list()` served from the ≤500 ms result cache: 25 batches of 1000 calls, reported
  in ns per call.

### 2. send() full path

- Two complete stacks ("alpha" = this process, "bravo" = a sleeper pid) are wired exactly like
  `test/peers-end-to-end.test.ts`: `createPeerRegistry` + `createPeerAuth` +
  `createPeerTransport` + `createPeerOutbox` + `createPeerInbox`, two real listening transport
  servers (Windows named pipes here), both records and key files published.
- One iteration = `alphaOutbox.send("bravo", body, {})` until the `delivered` receipt lands back
  on the sender. Receipt arrival is detected via the sender's `host.notify` callback (fired by
  `outbox.handleReceipt` on `delivered`) — event-driven, no polling granularity in the timing.
- Anti-abuse knobs are relaxed so the benchmark measures the path, not the limiter defaults
  (30 sends per sender per 30 s would throttle a 200-send run): the sender pacer and receiver
  guard token buckets are enlarged through their documented constructor options
  (`rateCapacity`, `globalRateCapacity`) — the same injection point the test suite uses.
- The **receiver's inbox is recreated per iteration** because the receipt-reflection budget in
  `src/inbox.ts` (burst 4, 1 token / 2 s per sender) is not injectable; without the reset it
  would silently drop receipts after the 4th back-to-back message and the run would fail its
  own `delivered` assertion rather than report a bogus number.
- Registry resolution runs against the real 2-record registry dir (the send path resolves by
  name through `registry.resolve`).

### 3. Protocol codec

- One `PeerUserFrame` whose wrapped content body is 1024 chars (1481 B serialized, a realistic
  chat-sized message), serialized once and reused; both ops run on the identical input per
  iteration so only the codec is measured. A round-trip equality check guards the fixture.
