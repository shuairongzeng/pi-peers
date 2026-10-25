# Mutation Testing Report — pi-peers

**Date:** 2026-10-25 (post-fixture-fix re-run; the 2026-10-24 baseline report is summarized in §1)
**Command:** `npm run mutation` (→ `stryker run`, config in `stryker.config.mjs`)
**Toolchain:** StrykerJS 10.1.1 + @stryker-mutator/vitest-runner 10.1.1, vitest 4.1.11, node 24.12.0, Windows 10 (12 logical cores)
**Scope:** the three highest-risk modules — `src/protocol.ts` (wire frame parsing/encoding), `src/registry.ts` (peer registry, file discovery, self-healing), `src/guard.ts` (inbound anti-abuse + outbound pacer).
**Test suite under evaluation:** 15 test files, 583 passing tests (+2 skipped), 94.25% line coverage of `src/`.

**What changed since the 2026-10-24 baseline:** `test/peers-registry.test.ts` replaced the `DEAD_PID` mistyped-field fixtures with `LIVE_PID` fixtures plus liveness controls (the §4 item-1 fix), and the transport/outbox/fault tests were reworked around the new `connection_lost` send-error semantics. This re-run re-measures the baseline after those changes.

Configuration choices (rationale):

- `coverageAnalysis: "perTest"` — each mutant runs only the tests that cover it (avg 11.7 tests/mutant) instead of all 583; the full run takes ~7 min 20 s with 6 concurrent workers (`concurrency = cores/2 = 6`).
- `timeoutMS: 60000`, `timeoutFactor: 3` — real named-pipe/socket fault-injection tests can slow down under a mutant; the relaxed budget produced 7 timeout-kills and zero false timeout errors.
- Reporters: `html`, `clear-text`, `json` → `reports/mutation/index.html`, `reports/mutation/mutation.json` (JSON kept as in-repo evidence; HTML regenerable and gitignored).
- `.stryker-tmp/` is cleaned automatically after the run (`cleanTempDir: true`) and gitignored.

## 1. Results

Two full runs were executed on 2026-10-25 with identical config (the timing-sensitive real-socket e2e tests flip a handful of mutants between killed/timeout/survived across runs):

| Run | Wall-clock | Killed | Timeout | Survived | No coverage | Score |
| --- | ---: | -----: | ------: | -------: | -----------: | ----: |
| Re-run 1 | 7 min 14 s | 1065 | 6 | 133 | 35 | **86.44 %** |
| Re-run 2 (committed `mutation.json`) | 7 min 21 s | 1060 | 7 | 137 | 35 | **86.12 %** |

Both runs exited with code 0 (score ≥ break threshold 80). Run-to-run delta: **5 mutants moved.** Four registry `readProcStart` platform-guard mutants (191:57, 192:6, 192:27, 192:36 — triaged equivalent in §3) were *killed* in run 1 by end-to-end `no live peer matches "bravo"` failures — a timing flake of the real-child e2e tests, not a semantic kill: on Windows both the original (`platform !== "linux"` early return) and the mutants (`readFileSync("/proc/…")` → ENOENT → catch) yield `undefined`. Run 2 correctly reports them as survived. The fifth delta is protocol.ts 119:10 (`block[cursor] !== " "` → livelock), killed in run 1, timeout-killed in run 2 — counted as killed either way. guard.ts was bit-identical across both runs.

Historical comparison: the 2026-10-24 baseline (pre-fixture-fix) measured **82.89 %** (1021 killed + 6 timeout of 1239 mutants). The live-pid fixture fix is worth **+3.23 points** (registry.ts alone: 72.92 % → 80.29 % on the committed runs).

**Module breakdown (re-run 2, the committed `reports/mutation/mutation.json`):**

| Module      | Mutants | Killed | Timeout | Survived | No coverage | Score    | Covered score |
| ----------- | ------: | -----: | ------: | -------: | -----------: | -------: | ------------: |
| protocol.ts |     422 |    384 |       3 |       35 |            0 |  91.71 % |           —   |
| registry.ts |     624 |    501 |       0 |       88 |           35 |  80.29 % |        85.06 % |
| guard.ts    |     193 |    175 |       4 |       14 |            0 |  92.75 % |           —   |
| **Total**   | **1239**| **1060**|  **7** | **137** |       **35** | **86.12 %** | **88.62 %**  |

## 2. Thresholds

```js
thresholds: { break: 80, low: 82, high: 85 }
```

- `break: 80` = `floor(82.89) − 2`, calibrated from the 2026-10-24 baseline — two points of headroom, so `npm run mutation` exits 0 today (86.12 % ≥ 80) but fails on any real regression that loses more than ~6 points.
- `low: 82` — the 2026-10-24 baseline signal line; the post-fix score now sits 4 points above it, so a regression below 82 shows as warning, not failure.
- `high: 85` — the improvement target, now **achieved** (86.12 %) by killing the §4 blind-spot survivors without touching the equivalent ones (§3); it can be raised on the next calibration pass.

## 3. Survivor triage — equivalent mutants (killing them is not expected)

**172 mutants were not killed in the committed run** (137 survived + 35 no-coverage). Triage verdict: **122 equivalent or host-invariant**, **50 genuine test blind spots** (§4). Breakdown per module: protocol 26 EQ / 9 blind; guard 9 EQ / 5 blind; registry 87 EQ / 36 blind (incl. all 35 NoCoverage: 27 host-invariant Linux branches + 8 blind).

### protocol.ts — 26 equivalent

| Location | Mutant | Replacement | Why equivalent |
| --- | --- | --- | --- |
| 199:6 (×2), 199:31, 222:6 (×2), 222:31 | ConditionalExpression / LogicalOperator | `false` / `&&` (skip `msg_id` guard in `validateUserFrame`/`validateControlFrame`) | `parseFrameLine` already checks `msg_id` (315) *before* dispatching to the validators; the inner guard is defense-in-depth unreachable through the public API. |
| 312:6 | ConditionalExpression | `false` (skip `typeof parsed !== "object"` in `parseFrameLine`) | `parsed === null \|\| Array.isArray` still rejects null/arrays; non-object JSON primitives fall through to the `msgV`/`msg_id` checks (314–315) and are rejected there. |
| 337:6 | ConditionalExpression | `false` (same check in `parseAuthLine`) | Same structure: the `type !== "auth"` / token checks reject everything the typeof check would. |
| 315:6, 315:31 | ConditionalExpression | `false` | Same-path redundancy as 199/222 (envelope check chain). |
| 111:7, 111:14, 113:7, 113:8 (×2), 114:7, 116:7, 116:19, 119:53 | Conditional / Unary / Regex | cursor arithmetic and attr-name regex relaxations in `parseAttrs` | Any junk attribute block that still matches `WRAP_RE` gets parsed differently, but the byte-exact re-wrap in `parsePeerContent` then mismatches and returns undefined — the round-trip check makes every relaxation unobservable. (Exception: the `'\t'`/`' '` variants at 119:10/119:35 livelock — see §5.) |
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

### registry.ts — 87 equivalent (incl. 27 host-invariant NoCoverage)

| Group | Mutants | Why equivalent / unreachable |
| --- | --- | --- |
| NoCoverage 192–197, 230–240 (21) | `readProcStart`/`pidDomain` Linux branches + machine-id ladder | Windows host: `process.platform !== "linux"` short-circuits; unreachable on this host (host-invariant, not a test gap). |
| NoCoverage 214:40 (×2), 214:64, 215 (×3) (6) | `isAlive` procStart-mismatch branch | Requires Linux `/proc` observability; on Windows `readProcStart` is always undefined. |
| 40:6 (1 of 3) | Conditional `false` (skip `typeof value !== "object"` in `parsePeerRecord`) | Non-object values (42, "str", arrays) are then rejected by the `r.version !== 1` check on line 44. |
| 47:6 (×5), 47:63 (×2), 47:75 (8) | pid checks weakened | Structurally unreachable: `PEER_RECORD_FILE_RE` only matches `<digits>.json`, so a non-integer/string pid never passes the file-name↔record-pid match (torn-write sweep); pid 0/negative file names never match the regex. |
| 78:6, 81:6 (2) | Conditional `false` (skip the `typeof !== "number"` conjunct of the `statusUpdatedAt`/`startedAt` checks) | The sibling `Number.isFinite(...)` conjunct rejects every non-number (including `"soon"` and `Infinity`), so skipping the typeof test changes nothing. |
| 103:5 (1) | Conditional `true` (skip `typeof f === "object"` in the `formerNames.every` predicate) | The remaining conjuncts reject every distinguishable item: `f !== null` (104) kills null, `!Array.isArray(f)` (105) kills arrays, and `typeof f.name === "string"` (106) is undefined-false for primitives like 42/"str". |
| 119:42 (1) | Conditional `false` (skip the `contextUsage === null` disjunct) | A `null` contextUsage is observably identical to an absent one: the only downstream reader is `record.contextUsage?.percent` (tool.ts), and optional chaining yields `undefined` for both. |
| 176:6, 176:16, 176:20 (3) | `close === -1` guard removed in `parseProcStatStarttime` | Content without a closing paren then slices from 0 and fails the `fields[19]`/`/^\d+$/` check → same undefined. |
| 182:10, 184:6, 184:34 (×2) (4) | regex relaxations in `parseProcStatStarttime` | `/\s+/`→`/\s/` still splits on single spaces; anchor relaxations only differ on whitespace-adjacent starttime, which real `/proc` stat never produces. |
| 191:57, 192:6 (×3), 192:27, 192:36 (6) | platform guard + catch in `readProcStart` | Windows: guard always true / try always throws (ENOENT on `/proc/…`, verified) → undefined either way. (Run 1 flakily *killed* four of these via e2e timing — see §1.) |
| 209:7, 209:16 (2) | `code !== "EPERM"` → `true` / `code === ""` in `isAlive` | On Windows a failed `kill(pid,0)` gives ESRCH (≠ EPERM, ≠ "") — both mutants keep returning false for dead pids; EPERM never occurs on this host. |
| 227:6 (1) | `true` (win32 check in `pidDomain`) | On Windows the win32 branch is taken either way. |
| 255:16, 255:25 (2) | `rmSync(path, {})` / `{force: false}` in `silentUnlink` | `force` defaults to true; `force:false` on a missing file throws ENOENT → caught → same best-effort semantics. |
| 266:10 (1) | `readdirSync` → junk array | Junk names fail `PEER_RECORD_FILE_RE` / key / tmp regexes and are skipped — same as an empty listing. |
| 134:29, 134:41 (2) | temp-file name junk in `atomicWriteFileSync` | The temp name is internal; write+rename still lands the record at the canonical path. |
| 363:7 (×5), 363:38 (6) | `update()` current-shape guard weakened | A non-object/corrupt `current` spreads into `merged` — but then `parsePeerRecord` in the *next* `list()` sweep rejects the written record and self-heals it; the merged write is transient. (The 363:82 *body* mutant, with 353/354/360, 423/424/430, is a blind spot, §4 #1.) |
| 453:7 (×2) | `live \|\| !fresh` / `true` (cache-write condition in `liveVerdictFor`) | Even a dead verdict wrongly cached is immediately deleted by `scanRegistry`'s defensive `aliveCache.delete(record.pid)` on the dead branch — the sweep masks it. |
| 466:44, 467:50, 468:5 (3) | alive-cache prune loop body disabled | Prune only bounds map growth; verdict TTL already forces re-probing, so probe counts are unchanged within any test horizon. |
| 482:44, 483:12 (2) | `readFileSync(path, "")` / catch body `{}` in `readRecord` | `""` encoding ≡ utf8; emptied catch falls through to the shape check → undefined. |
| 497:8, 497:15, 499:5 (3) | dead-record branch weakened in `scanRegistry` | A dead-pid record that skips the dead branch falls into the zombie-key branch (`!existsSync(keyPathFor)`) — the `makeRecord` fixture never plants keys for `DEAD_PID`, so both paths push `live: false` and sweep the file. Defense-in-depth overlap. |
| StringLiteral `"Stryker was here!"` at 563:17, 598:22 (2) | comparing a string field to a junk literal | Only changes behavior for inputs equal to the junk string, which no test or real writer produces. (The 50:57/53:76/87:57/90:59 quartet of this family *died* to the new live-pid `sessionId: ""`/`pidDomain: ""`/`socketPath: ""` fixtures — see §4 resolution note.) |
| 570:20 (×4), 572:13 (5) | `[ref]` regex relaxations / untrimmed `refMatch[1]` | Multi-space or mid-string `[hex]` inputs are the only distinguishable cases; `normalizeName` + downstream filters make them unobservable for every real resolution input. |
| 467:8 (1 of 2), 549:8 (1 of 2) (2) | boundary `>` vs `>=` | Exact-ms boundary (15 000 ms prune / 60 000 ms grace) — one-clock-tick difference; fixtures test 2× past the boundary. |


## 4. Survivor triage — test blind spots (worth future tests)

**50 survivors are genuine test gaps.** The former dominant root cause in `registry.ts` — mistyped-field fixtures pinned to `DEAD_PID`, so records that `parsePeerRecord` wrongly *accepted* were still deleted by the liveness sweep — is **resolved**: the 2026-10-25 live-pid fixtures (with `assertValidRecordSurvivesScan` liveness controls, boundary-exact valid values, and planted auth keys for each malformed record) killed every killable mutant in that cluster. The four L40–L119 survivors that remain are re-triaged as equivalent (§3): 78:6/81:6, 103:5, and 119:42.

### High value

1. **registry.ts L353–363, L423–430 (7)** — `update()`/`readRecord()` error-path bodies: "silently ignores update when no record exists / corrupt JSON" asserts no-throw and file inventory, but never asserts the file **content was not patched** — a mutant that skips the early `return` and writes `merged` from corrupt `current` survives. *Fix:* snapshot `readFileSync` content across `update()` on a corrupt record.
2. **registry.ts L214:6 (5)** — `isAlive`'s `procStart !== undefined && observed !== undefined` relaxations: `&&`→`||` compares `procStart` against undefined on Windows → a live peer with `procStart` set is declared dead. No fixture sets `procStart` on a live pid on Windows. *Fix:* publish a live-pid record with `procStart: "123"`; assert `live: true`.
3. **registry.ts L562–563, L598, L607 (5)** — `resolveIn` edges: `name.trim()` removed, `input === ""` guard removed, normalized-empty early return, former-name predicate hard-coded `true`. *Fix:* `resolve(" alice ")`, `resolve("")` after a warm cache, and a target that must *not* match a peer's former name.
4. **registry.ts L539:8/38 + L34 (5)** — dot-tmp sweep condition `!PEER_TMP_FILE_RE.test(name)` → `false` sends *every* file through the mtime check, and the `PEER_TMP_FILE_RE` regex definition itself has three surviving relaxations (dropped `$`/`^` anchors, dropped `?`). Only distinguishable via a backdated non-record file that must survive. *Fix:* backdate a `readme.txt`, assert survival; pin the tmp-name shape.
5. **registry.ts L553 (3)** — `entries.sort` comparator can be removed or broken: no test asserts multi-record ordering. *Fix:* publish three records out of pid order; assert `list()` order.
6. **registry.ts L529 (2)** — orphan-key sweep `EPERM` handling (`code === "EPERM"` → `""`/`false`): on Windows dead pids yield ESRCH and no test injects EPERM on this path. *Fix:* spy `process.kill` to throw `{code: "EPERM"}` for an orphan-key pid; assert the key survives.
7. **protocol.ts L226, 242, 243, 259, 272, 286, 287 (7)** — per-action optional-field guards (`fromName`, `from`) skippable per control action: the `badLines` table covers `reason as number`, `nonce as number`, `reply_to as number` but not per-action `fromName`/`from` confusions. *Fix:* extend the table.
8. **protocol.ts L119:35 (1) + L95:28 (1)** — the tab-handling conditional whose sibling regex mutant livelocked (§5): the surviving `false` variant shows tab-separated attribute blocks reach a code path no test pins. L95:28 skips the empty-attr check in `wrapPeerContent`, emitting `attr=""` instead of omitting it. *Fix:* unit-test `parsePeerContent` with tab separators between attributes; wrap attrs with `""` values.
9. **registry.ts L299 (1)** — `ALIVE_CACHE_PRUNE_MS = ALIVE_CACHE_TTL_MS * 10` → `/10` shrinks the prune horizon 100×: within any single test the map is too small for the difference to show. *Fix:* seed many stale verdicts (LRU-free) and assert map size after a scan.

### Medium value

10. **guard.ts L208:18, L209:57 (2)** — dedup stale-fingerprint purge: cutoff sign flip and purge-body removal are invisible because `check()` re-derives rejection from stored timestamps. Only the LRU eviction bounds the `seen` map otherwise. *Fix:* age a sender's fingerprints past the window without LRU pressure, re-send, assert `ok: true` and that the map does not grow unboundedly (memory assertion).
11. **guard.ts L254:10** — pacer LRU eviction boundary `>=` vs `>`: evicting one target early is observable only with exact-count targets. *Fix:* assert target-count-exact bucket behavior.
12. **guard.ts L223:25 / L270:19** — `refund()`'s cap on the *per-sender* and *pacer* buckets (the global-bucket variant is killed). *Fix:* refund a full bucket; assert the next two consumes match capacity exactly.
13. **registry.ts NoCoverage L136–138 (4), L546:12** — `atomicWriteFileSync` catch body / `statSync` raced-away catch: no FS-fault injection exists. *Fix:* mock `writeFileSync` to fail once; assert the temp file is cleaned and the error rethrown.
14. **registry.ts L467:8 / L549:8 boundary pins (2)** — exact 15 000 ms prune / 60 000 ms grace boundaries. Already triaged equivalent in §3 (one-clock-tick difference); listed because exact-boundary fixtures would pin the semantics for free. *Fix:* `advanceTimersByTime(15_000)` exactly, etc. (Not counted in the 50.)
15. **registry.ts NoCoverage L598:26, L599 (3)** — early-return bodies for `resolveIn` empty-normalized: conditions covered, bodies not. *Fix:* same fixtures as #3.

### Resolution note (2026-10-25): the DEAD_PID fixture cluster

Former §4 item 1 — "**registry.ts parsePeerRecord field-validation relaxations (42 mutants, L40–L119)**" — is **resolved** via the live-pid fixtures in `test/peers-registry.test.ts` (commit `0d65fe3`). Registry.ts score: **72.92 % → 80.29 %** (committed-run comparison; baseline-run comparison 73.72 % → 80.29 %).

Every killable mutant in the band died in both re-runs. Of the 59 non-killed mutants the band held on 2026-10-24 (58 survived + 1 no-coverage), **46 died** — including the whole 42-mutant blind-spot cluster, the null-record-file crash class (L40:6 ×2, L40:35, L40:75), the `>=` max-length boundary variants (L60:42, L63:6 ×2, L63:43), the control-char and name-normalization relaxations, the `formerNames` per-item checks (L103:5 ×9, L104, L106, L107), the `features.every` string check (L112:61), the `contextUsage` array check (L119:4 group), and the `"Stryker was here!"` quartet at 50:57/53:76/87:57/90:59.

**13 band mutants survived, and all 13 are equivalent, not blind spots** (they now live in §3):

- `631` (40:6 `false`), `642`–`646`, `650`, `651`, `653` (47:6 ×5, 47:63 ×2, 47:75) — the pid-check group: a mistyped pid never passes the file-name↔record-pid match (torn-write sweep) — unreachable by fixtures, as before.
- `750` (78:6), `758` (81:6) — skip of the `typeof !== "number"` conjunct: `Number.isFinite` already rejects every non-number, so the conjunct is redundant.
- `816` (103:5 `true`) — skip of `typeof f === "object"` in the `formerNames.every` predicate: the null/array/name-type conjuncts reject every distinguishable item.
- `853` (119:42 `false`) — skip of the `contextUsage === null` disjunct: `null` is observably identical to absent downstream (`record.contextUsage?.percent`).

### Summary of remaining blind spots (the actionable list)

- `update()`/`readRecord()` error paths assert "no throw" but not "no write" (7).
- procStart-present-but-unobservable on Windows liveness (5).
- `resolveIn` whitespace/empty/former-name edges (5, +3 no-coverage bodies).
- Dot-tmp sweep + `PEER_TMP_FILE_RE` shape (5).
- `list()` ordering (3).
- `EPERM` handling in the orphan-key sweep (2).
- Per-action control-frame optional-field type confusion rows (7, protocol).
- `parseAttrs` tab path + wrap empty-attr skip (2, protocol).
- Alive-cache prune horizon constant (1).
- Dedup-fingerprint purge / refund caps / pacer eviction boundary (5, guard).
- FS-fault injection for atomic-write cleanup (5).
- Exact-boundary semantics (prune/grace) — §3-equivalent one-tick cases, not counted above; free to pin with exact-timer fixtures.

## 5. Timeout kills (counted as killed)

| Location | Mutant | Effect |
| --- | --- | --- |
| guard.ts 201:44 / 204:4 | `{}` / `;` (eviction loop body disabled) | `while (senders.size > max)` never shrinks → infinite loop |
| guard.ts 254:44 / 257:4 | same in pacer | infinite loop |
| protocol.ts 109:32 | `{}` (cursor-advance loop body) | `while (cursor < block.length)` never advances → infinite loop |
| protocol.ts 119:35 | `block[cursor] !== "\t"` | whitespace-skip loop skips a non-tab char infinitely (only escaped via timeout) — related to blind spot #8 |
| protocol.ts 119:10 | `block[cursor] !== " "` | same livelock for the space variant; killed in re-run 1, timeout-killed in re-run 2 (counted as killed either way) |

These seven (re-run 2) are legitimate kills: the suite detected livelock via timeout.

## 6. Reproducing

```bash
npm test                 # full vitest suite first — must be green
npm run mutation         # full three-module run, ~7.5 min, exit 0 at score ≥ 80
npx stryker run --mutate src/guard.ts   # single-module quick check, ~2.5 min
```

Artifacts: `reports/mutation/mutation.json` (per-mutant status, killed-by test ids, durations — kept as in-repo machine-readable evidence), `reports/mutation/index.html` (regenerable, gitignored), `.stryker-tmp/` (auto-removed after every run).
