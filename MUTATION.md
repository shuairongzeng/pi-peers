# Mutation Testing Report — pi-peers

**Date:** 2026-10-24
**Command:** `npm run mutation` (→ `stryker run`, config in `stryker.config.mjs`)
**Toolchain:** StrykerJS 10.1.1 + @stryker-mutator/vitest-runner 10.1.1, vitest 4.1.11, node 24.12.0, Windows 10 (12 logical cores)
**Scope:** the three highest-risk modules — `src/protocol.ts` (wire frame parsing/encoding), `src/registry.ts` (peer registry, file discovery, self-healing), `src/guard.ts` (inbound anti-abuse + outbound pacer).
**Test suite under evaluation:** 15 test files, 581 passing tests (+2 skipped), 94.32% line coverage of `src/`.

Configuration choices (rationale):

- `coverageAnalysis: "perTest"` — each mutant runs only the tests that cover it (avg 11.35 tests/mutant) instead of all 581; the full run took **9 min 35 s** with 6 concurrent workers (`concurrency = cores/2 = 6`).
- `timeoutMS: 60000`, `timeoutFactor: 3` — real named-pipe/socket fault-injection tests can slow down under a mutant; the relaxed budget produced 6 timeout-kills and zero false timeout errors.
- Reporters: `html`, `clear-text`, `json` → `reports/mutation/index.html`, `reports/mutation/mutation.json` (JSON kept as in-repo evidence; HTML regenerable and gitignored).
- `.stryker-tmp/` is cleaned automatically after the run (`cleanTempDir: true`) and gitignored.

## 1. Results

Three full runs were executed on 2026-10-24 with identical config (the timing-sensitive real-socket tests flip a handful of mutants between killed/timeout/survived across runs):

| Run | Wall-clock | Killed | Timeout | Survived | No coverage | Score |
| --- | ---: | -----: | ------: | -------: | -----------: | ----: |
| Baseline (threshold calibration) | 9 min 35 s | 1021 | 6 | 176 | 36 | **82.89 %** |
| Acceptance rerun | 7 min 4 s | 1014 | 7 | 182 | 36 | **82.41 %** |
| Final exit-code verification (`npm run mutation`) | 6 min 27 s | 1015 | 6 | 182 | 36 | **82.41 %** |

The third run exited with code 0 (score ≥ break threshold 80) — that is the committed `reports/mutation/mutation.json`. Run-to-run delta vs baseline: 13 mutants moved (protocol +3, registry +3 net new non-killed — `PEER_TMP_FILE_RE` regex relaxation, `ALIVE_CACHE_PRUNE_MS` `*10`→`/10`, `sort` call removal, `parseAttrs` tab variants); every delta falls into a triage bucket already listed in §3/§4. guard.ts was bit-identical across all runs.

**Module breakdown (baseline run):**

| Module      | Mutants | Killed | Timeout | Survived | No coverage | Score    | Covered score |
| ----------- | ------: | -----: | ------: | -------: | -----------: | -------: | ------------: |
| protocol.ts |     422 |    386 |       2 |       34 |            0 |  91.94 % |           —   |
| registry.ts |     624 |    460 |       0 |      128 |           36 |  73.72 % |        78.23 % |
| guard.ts    |     193 |    175 |       4 |       14 |            0 |  92.75 % |           —   |
| **Total**   | **1239**| **1021**|  **6** | **176** |       **36** | **82.89 %** | **85.37 %**  |

(A single-module smoke run on `guard.ts` first confirmed the config in 2 min 27 s at 92.75 %, before the full run.)

## 2. Thresholds

```js
thresholds: { break: 80, low: 82, high: 85 }
```

- `break: 80` = `floor(82.89) − 2` — two points of headroom under the measured baseline, so `npm run mutation` exits 0 today but fails on any real regression that loses more than ~2 points.
- `low: 82` — one point under the measured score: a mild regression (below 82) shows as warning, not failure.
- `high: 85` — the improvement target: achievable by killing the "test blind spot" survivors triaged below (§4) without touching the equivalent ones (§3).

## 3. Survivor triage — equivalent mutants (killing them is not expected)

**212 mutants were not killed in the baseline run** (176 survived + 36 no-coverage; the acceptance rerun's 225 non-killed differ only by the 13 timing-variance mutants noted in §1). Triage verdict: **113 equivalent or host-invariant**, **99 genuine test blind spots** (§4). Breakdown per module: protocol 26 EQ / 8 blind; guard 9 EQ / 5 blind; registry 78 EQ / 86 blind (incl. all 36 NoCoverage: 27 host-invariant Linux branches + 9 blind).

### protocol.ts — 26 equivalent

| Location | Mutant | Replacement | Why equivalent |
| --- | --- | --- | --- |
| 199:6 (×2), 199:31, 222:6 (×2), 222:31 | ConditionalExpression / LogicalOperator | `false` / `&&` (skip `msg_id` guard in `validateUserFrame`/`validateControlFrame`) | `parseFrameLine` already checks `msg_id` (315) *before* dispatching to the validators; the inner guard is defense-in-depth unreachable through the public API. |
| 312:6 | ConditionalExpression | `false` (skip `typeof parsed !== "object"` in `parseFrameLine`) | `parsed === null \|\| Array.isArray` still rejects null/arrays; non-object JSON primitives fall through to the `msgV`/`msg_id` checks (314–315) and are rejected there. |
| 337:6 | ConditionalExpression | `false` (same check in `parseAuthLine`) | Same structure: the `type !== "auth"` / token checks reject everything the typeof check would. |
| 315:6, 315:31 | ConditionalExpression | `false` | Same-path redundancy as 199/222 (envelope check chain). |
| 111:7, 111:14, 113:7, 113:8 (×2), 114:7, 116:7, 116:19, 119:53 | Conditional / Unary / Regex | cursor arithmetic and attr-name regex relaxations in `parseAttrs` | Any junk attribute block that still matches `WRAP_RE` gets parsed differently, but the byte-exact re-wrap in `parsePeerContent` then mismatches and returns undefined — the round-trip check makes every relaxation unobservable. (Exception: the `'\t'` variant at 119:35 livelocks — see §5.) |
| 126:14 | ConditionalExpression | `true` (`name === "hop-chain"` always matches) | Non-canonical attr names then parse as hop-chain; the re-wrap emits canonical attrs only → mismatch → undefined. Identical observable result. |
| 130:10, 141:6 | BlockStatement / Conditional | `{}` / `false` (unknown-attr `return undefined` removed) | Same byte-exact round-trip argument: an unknown attr can never re-wrap to itself. |
| 148:10 | BlockStatement | `{}` (re-wrap catch body) | The catch is only reachable when `wrapPeerContent` throws mid-re-wrap; parsed attrs never contain the closing tag (WRAP_RE prevents it). |
| 187:45 | StringLiteral | `""` (encoding of `Buffer.byteLength`) | Node treats `""` encoding as UTF-8 — verified: `Buffer.byteLength("héllo","") === 6 === Buffer.byteLength("héllo","utf8")`. |
| 309:10, 334:10 | BlockStatement | `{}` (JSON.parse catch body) | Fall-through hits the next guard (typeof object / type check) which rejects the same input. |

### guard.ts — 9 equivalent

| Location | Mutant | Replacement | Why equivalent |
| --- | --- | --- | --- |
| 152:8 | ConditionalExpression | `true` (`seenAtMs !== undefined`) | When the fingerprint is missing, `atMs - undefined = NaN`, and `NaN < dedupWindowMs` is false — the condition behaves identically. |
| 203:8 | ConditionalExpression | `false` (skip `if (evicted === undefined) break`) | The `while (senders.size > max)` guard implies the map is non-empty, so `next().value` is never `undefined`; the break-guard is defensive dead code. |
| 252:3 | CallExpression | `;` (skip `targets.delete(target)` before re-`set`) | Re-`set` on an existing key does not change insertion order in a JS `Map`; for a *new* key the delete is a no-op. |
| 256:8 | ConditionalExpression | `false` | Same defensive break-guard in the pacer eviction loop (see 203:8). |
| 210:8 (×4) | Conditional / Equality | `true`/`false`/`<`/`>` on the stale-fingerprint purge | Purging one iteration earlier/later is invisible: `check()`'s own window comparison (`atMs - seenAtMs < dedupWindowMs`) governs rejections from the stored timestamps regardless of the purge. |
| 210:28 | CallExpression | `;` (skip `state.seen.delete`) | Same: the purge is a memory optimization; rejection semantics live in `check()`. |

### registry.ts — 78 equivalent (incl. 27 host-invariant NoCoverage)

| Group | Mutants | Why equivalent / unreachable |
| --- | --- | --- |
| NoCoverage 192–197, 230–240 (21) | `readProcStart`/`pidDomain` Linux branches + machine-id ladder | Windows host: `process.platform !== "linux"` short-circuits; unreachable on this host (host-invariant, not a test gap). |
| NoCoverage 214:40 (×2), 214:64, 215 (×3) (6) | `isAlive` procStart-mismatch branch | Requires Linux `/proc` observability; on Windows `readProcStart` is always undefined. |
| 40:6 (1 of 3) | Conditional `false` (skip `typeof value !== "object"` in `parsePeerRecord`) | Non-object values (42, "str", arrays) are then rejected by the `r.version !== 1` check on line 44. |
| 47:6 (×5), 47:63 (×2), 47:75 (8) | pid checks weakened | Structurally unreachable: `PEER_RECORD_FILE_RE` only matches `<digits>.json`, so a non-integer/string pid never passes the file-name↔record-pid match (torn-write sweep); pid 0/negative file names never match the regex. |
| 63:6 (2 of 4) | over-length check variants | `r.cwd.length > A && r.sessionId.length > B` style relaxations: the remaining fixture inputs are also rejected by the length variants that *were* killed (the `>=` boundary and whole-check-`false` relaxations are blind spots, §4 #1). |
| 176:6, 176:16, 176:20 (3) | `close === -1` guard removed in `parseProcStatStarttime` | Content without a closing paren then slices from 0 and fails the `fields[19]`/`/^\d+$/` check → same undefined. |
| 182:10, 184:6, 184:34 (×2) (4) | regex relaxations in `parseProcStatStarttime` | `/\s+/`→`/\s/` still splits on single spaces; anchor relaxations only differ on whitespace-adjacent starttime, which real `/proc` stat never produces. |
| 191:57, 192:6 (×3), 192:27, 192:36 (6) | platform guard + catch in `readProcStart` | Windows: guard always true / try always throws → undefined either way. |
| 209:7, 209:16 (2) | `code !== "EPERM"` → `true` / `code === ""` in `isAlive` | On Windows a failed `kill(pid,0)` gives ESRCH (≠ EPERM, ≠ "") — both mutants keep returning false for dead pids; EPERM never occurs on this host. |
| 227:6 (1) | `true` (win32 check in `pidDomain`) | On Windows the win32 branch is taken either way. |
| 255:16, 255:25 (2) | `rmSync(path, {})` / `{force: false}` in `silentUnlink` | `force` defaults to true; `force:false` on a missing file throws ENOENT → caught → same best-effort semantics. |
| 266:10 (1) | `readdirSync` → junk array | Junk names fail `PEER_RECORD_FILE_RE` / key / tmp regexes and are skipped — same as an empty listing. |
| 134:29, 134:41 (2) | temp-file name junk in `atomicWriteFileSync` | The temp name is internal; write+rename still lands the record at the canonical path. |
| 363:7 (×3 of 5), 363:38, 363:82 (5) | `update()` current-shape guard weakened | A non-object/corrupt `current` spreads into `merged` — but then `parsePeerRecord` in the *next* `list()` sweep rejects the written record and self-heals it; the merged write is transient. (The *body* mutants at 353/354/360 are blind spots, §4 #2.) |
| 453:7 (×2) | `live \|\| !fresh` / `true` (cache-write condition in `liveVerdictFor`) | Even a dead verdict wrongly cached is immediately deleted by `scanRegistry`'s defensive `aliveCache.delete(record.pid)` on the dead branch — the sweep masks it. |
| 466:44, 467:50, 468:5 (3) | alive-cache prune loop body disabled | Prune only bounds map growth; verdict TTL already forces re-probing, so probe counts are unchanged within any test horizon. |
| 482:44, 483:12 (2) | `readFileSync(path, "")` / catch body `{}` in `readRecord` | `""` encoding ≡ utf8; emptied catch falls through to the shape check → undefined. |
| 497:8, 497:15, 499:5 (3) | dead-record branch weakened in `scanRegistry` | A dead-pid record that skips the dead branch falls into the zombie-key branch (`!existsSync(keyPathFor)`) — the `makeRecord` fixture never plants keys for `DEAD_PID`, so both paths push `live: false` and sweep the file. Defense-in-depth overlap. |
| StringLiteral `"Stryker was here!"` at 50:57, 53:76, 87:57, 90:59, 563:17, 598:22 (6) | comparing a string field to a junk literal | Only changes behavior for inputs equal to the junk string, which no test or real writer produces. |
| 570:20 (×4), 572:13 (5) | `[ref]` regex relaxations / untrimmed `refMatch[1]` | Multi-space or mid-string `[hex]` inputs are the only distinguishable cases; `normalizeName` + downstream filters make them unobservable for every real resolution input. |
| 467:8 (1 of 2), 549:8 (1 of 2) (2) | boundary `>` vs `>=` | Exact-ms boundary (15 000 ms prune / 60 000 ms grace) — one-clock-tick difference; fixtures test 2× past the boundary. |


## 4. Survivor triage — test blind spots (worth future tests)

**99 survivors are genuine test gaps.** The dominant root cause in `registry.ts`: **mistyped-field fixtures use `DEAD_PID`, so a record that `parsePeerRecord` wrongly *accepts* is still deleted by the liveness sweep — acceptance relaxations are masked**. Killing them needs fixtures with a live pid asserting `live: true` (or asserting the record survives a scan).

### High value

1. **registry.ts parsePeerRecord field-validation relaxations (42 mutants, L40–L119)** — the biggest single cluster: `sessionId`/`cwd`/`name`/`statusUpdatedAt`/`startedAt`/`procStart`/`pidDomain`/`socketPath` type-or-emptiness checks, name-normalization match, max-length boundaries (`>=` boundary variants), control-char checks, `formerNames` per-item checks (L103:5 ×9, L104, L106, L107), `features.every` string check (L112:61), `contextUsage` object check (L119:42). Weakening any of them admits a malformed record that the DEAD_PID liveness sweep then deletes anyway — the test passes for the wrong reason. *Fix:* add fixtures asserting `list()` returns the entry with `live: true` for a **live** pid carrying each mistyped field (or export/observe `parsePeerRecord` directly), plus boundary-exact lengths (64-char name, 512-char cwd, 128-char sessionId) and a `null`/non-object record file (L40 null-crash class).

2. **registry.ts L353–363, L423–430 (12)** — `update()`/`readRecord()` error-path bodies: "silently ignores update when no record exists / corrupt JSON" asserts no-throw and file inventory, but never asserts the file **content was not patched** — a mutant that skips the early `return` and writes `merged` from corrupt `current` survives. *Fix:* snapshot `readFileSync` content across `update()` on a corrupt record.
3. **registry.ts L529 (2)** — orphan-key sweep `EPERM` handling (`code === "EPERM"` → `""`/`false`): on Windows dead pids yield ESRCH and no test injects EPERM on this path. *Fix:* spy `process.kill` to throw `{code: "EPERM"}` for an orphan-key pid; assert the key survives.
4. **registry.ts L553 (2)** — `entries.sort` comparator can be removed: no test asserts multi-record ordering. *Fix:* publish three records out of pid order; assert `list()` order.
5. **registry.ts L539:8/38 (2)** — dot-tmp sweep condition `!PEER_TMP_FILE_RE.test(name)` → `false` sends *every* file through the mtime check; only distinguishable via a backdated non-record file that must survive. *Fix:* backdate a `readme.txt`, assert survival.
6. **registry.ts L562–572, L598 (9)** — `resolveIn` edges: `name.trim()` removed, `input === ""` guard removed, normalized-empty early return, `[ref]` target untrimmed. *Fix:* `resolve(" alice ")`, `resolve("")` after a warm cache, `"twin [ abc ]"`-style spacing.
7. **registry.ts L214:6 (5)** — `isAlive`'s `procStart !== undefined && observed !== undefined` relaxations: `&&`→`||` compares `procStart` against undefined on Windows → a live peer with `procStart` set is declared dead. No fixture sets `procStart` on a live pid on Windows. *Fix:* publish a live-pid record with `procStart: "123"`; assert `live: true`.
8. **protocol.ts L226, 242, 243, 259, 272, 286, 287 (7)** — per-action optional-field guards (`fromName`, `from`) skippable per control action: the `badLines` table covers `reason as number`, `nonce as number`, `reply_to as number` but not per-action `fromName`/`from` confusions. *Fix:* extend the table.
9. **protocol.ts L119:35 (1)** — the tab-handling conditional whose sibling regex mutant livelocked (§5): the surviving `false` variant shows tab-separated attribute blocks reach a code path no test pins. *Fix:* unit-test `parsePeerContent` with tab separators between attributes.

### Medium value

10. **guard.ts L208/210/209 cluster (7)** — dedup stale-fingerprint purge: cutoff sign flip, `<=`→`<` boundary, purge-body removal — all invisible because `check()` re-derives rejection from stored timestamps. Only the LRU eviction bounds the `seen` map otherwise. *Fix:* age a sender's fingerprints past the window without LRU pressure, re-send, assert `ok: true` and that the map does not grow unboundedly (memory assertion).
11. **guard.ts L254:10** — pacer LRU eviction boundary `>=` vs `>`: evicting one target early is observable only with exact-count targets. *Fix:* assert target-count-exact bucket behavior.
12. **guard.ts L223:25 / L270:19** — `refund()`'s cap on the *per-sender* and *pacer* buckets (the global-bucket variant is killed). *Fix:* refund a full bucket; assert the next two consumes match capacity exactly.
13. **registry.ts NoCoverage L136–138 (4), L546:12** — `atomicWriteFileSync` catch body / `statSync` raced-away catch: no FS-fault injection exists. *Fix:* mock `writeFileSync` to fail once; assert the temp file is cleaned and the error rethrown.
14. **registry.ts L467:8 / L549:8 boundary pins (2)** — exact 15 000 ms prune / 60 000 ms grace boundaries. *Fix:* `advanceTimersByTime(15_000)` exactly, etc.
15. **registry.ts NoCoverage L598:26, L599 (3), L40:75 (1)** — early-return bodies for `resolveIn` empty-normalized and `parsePeerRecord` value-not-object: conditions covered, bodies not. *Fix:* same fixtures as #1/#6.
16. **registry.ts L40:6 (×2), L40:35 — null/non-object record file (3)** — a hand-written `null.json` record crashes `scanRegistry` when the null check is weakened (TypeError on `r.version`). *Fix:* write `null` into a record file; assert `list()` self-heals instead of throwing.

### Summary of remaining blind spots (the actionable list)

- **`parsePeerRecord` acceptance relaxations masked by DEAD_PID fixtures** (42 mutants — the single highest-value fix: live-pid mistyped-field fixtures).
- `update()`/`readRecord()` error paths assert "no throw" but not "no write" (12).
- Per-action control-frame optional-field type confusion rows (7, protocol).
- `EPERM` handling in the orphan-key sweep (2).
- `list()` ordering (2).
- `resolveIn` whitespace/empty edges (9).
- procStart-present-but-unobservable on Windows liveness (5).
- Dedup-fingerprint purge / refund caps / pacer eviction boundary (10, guard).
- FS-fault injection for atomic-write cleanup (5).
- Null-record-file crash class (3).
- Exact-boundary semantics (prune/grace/length limits) (4).
- `parseAttrs` tab path (1, protocol).

## 5. Timeout kills (counted as killed)

| Location | Mutant | Effect |
| --- | --- | --- |
| guard.ts 201:44 / 204:4 | `{}` / `;` (eviction loop body disabled) | `while (senders.size > max)` never shrinks → infinite loop |
| guard.ts 254:44 / 257:4 | same in pacer | infinite loop |
| protocol.ts 109:32 | `{}` (cursor-advance loop body) | `while (cursor < block.length)` never advances → infinite loop |
| protocol.ts 119:35 | `block[cursor] !== "\t"` | whitespace-skip loop skips a non-tab char infinitely (only escaped via timeout) — related to blind spot #9 |

These six are legitimate kills: the suite detected livelock via timeout.

## 6. Reproducing

```bash
npm run mutation          # full three-module run, ~10 min, exit 0 at score ≥ 80
npx stryker run --mutate src/guard.ts   # single-module quick check, ~2.5 min
```

Artifacts: `reports/mutation/mutation.json` (per-mutant status, killed-by test ids, durations — kept as in-repo machine-readable evidence), `reports/mutation/index.html` (regenerable, gitignored), `.stryker-tmp/` (auto-removed after every run).
