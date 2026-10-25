#!/usr/bin/env node
/**
 * peers-bench.mjs — performance baseline for @zengcool/pi-peers.
 *
 * Generates the numbers recorded in BENCH.md (the baseline for future
 * performance work on this package). Three sections, each printed as a table:
 *
 *   1. registry list()  — cold directory scan (all liveness verdicts evicted)
 *                         vs. warm hit served from the <=500 ms result cache,
 *                         N = 50 records + key files, every pid live.
 *   2. send() full path — real transport server + token auth + one user frame
 *                         + delivered receipt over the wire, 200 iterations.
 *   3. protocol codec   — serializeFrame / parseFrameLine ops/sec on a ~1 KB
 *                         user frame.
 *
 * Methodology: every metric runs a warmup phase first (>= 10% of the measured
 * iteration count, discarded), then reports the median (p50) rather than the
 * mean, alongside p95 / min / max.
 *
 * Fixture relaxations (all via documented constructor options or fixture
 * shape; src/ is never modified):
 *   - the sender pacer and receiver guard token buckets are enlarged so 200
 *     back-to-back sends are not dominated by the anti-abuse refill rates
 *     (same injection point the test suite uses);
 *   - the receiver's inbox is recreated per iteration because the receipt
 *     reflection budget inside inbox.ts (burst 4, 1 token / 2 s per sender)
 *     is not injectable and would silently drop receipts after the 4th
 *     back-to-back message.
 *
 * Run: npm run bench     (exit code 0 on success, 1 on any failure)
 */

import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";

import { createPeerAuth } from "../src/auth.ts";
import { createPeerGuard, createPeerSenderPacer } from "../src/guard.ts";
import { createPeerInbox } from "../src/inbox.ts";
import { createPeerOutbox } from "../src/outbox.ts";
import { createPeerProtocol } from "../src/protocol.ts";
import { createPeerRegistry } from "../src/registry.ts";
import { createPeerTransport } from "../src/transport.ts";
import { PEER_FEATURES, PEER_PROTOCOL_VERSION } from "../src/types.ts";

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

/** Registry fixture size: N records + N key files, all pids live. */
const REGISTRY_N = 50;
const COLD_WARMUP = 25;
const COLD_ITERS = 200;
const WARM_BATCHES = 25;
const WARM_BATCH_OPS = 1000;

const SEND_WARMUP = 25;
const SEND_ITERS = 200;
/** Body size of one sent message (short chat-style message). */
const SEND_BODY_BYTES = 256;

const PROTO_WARMUP_BATCHES = 3;
const PROTO_BATCHES = 20;
const PROTO_BATCH_OPS = 10_000;
/** Body size of the codec-throughput frame (~1 KB class). */
const PROTO_BODY_BYTES = 1024;

const RECEIPT_TIMEOUT_MS = 15_000;

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** Linear-interpolated percentile of a pre-sorted array. */
function pct(sorted, q) {
	const pos = (sorted.length - 1) * q;
	const lo = Math.floor(pos);
	const hi = Math.ceil(pos);
	return lo === hi ? sorted[lo] : sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo);
}

function stats(values) {
	const sorted = [...values].sort((a, b) => a - b);
	return { p50: pct(sorted, 0.5), p95: pct(sorted, 0.95), min: sorted[0], max: sorted[sorted.length - 1] };
}

/**
 * A real live process for registry liveness probes (kill(pid,0) must succeed;
 * dead-pid records would be swept by list() and shrink the fixture).
 * Windows has no `sleep`, but `ping -n <count> 127.0.0.1` ticks once per
 * second and stays alive for ~count seconds.
 */
function spawnSleeper() {
	const cmd = process.platform === "win32" ? "ping" : "sleep";
	const args = process.platform === "win32" ? ["-n", "100000", "127.0.0.1"] : ["100000"];
	const child = spawn(cmd, args, { stdio: "ignore" });
	const ready = Promise.withResolvers();
	child.once("spawn", () => ready.resolve());
	child.once("error", (err) => ready.reject(err));
	return { child, ready: ready.promise };
}

/** Assert a pid is alive (retrying briefly for just-spawned processes). */
async function waitAlive(pid, label) {
	for (let i = 0; i < 40; i++) {
		try {
			process.kill(pid, 0);
			return;
		} catch {
			await sleep(25);
		}
	}
	throw new Error(`fixture process "${label}" (pid ${pid}) never became alive`);
}

/** Random transport address, same shape src/transport.ts derives. */
function makeAddress(label) {
	if (process.platform === "win32") {
		const name = `pi-peer-${randomBytes(16).toString("hex")}`;
		return { path: `\\\\.\\pipe\\LOCAL\\${name}`, address: `pipe:LOCAL\\${name}` };
	}
	const path = join(os.tmpdir(), `pi-peer-bench-${label}-${process.pid}-${randomBytes(4).toString("hex")}.sock`);
	return { path, address: `sock:${path}` };
}

/** A well-formed registry record (mirrors the e2e test fixture). */
function makeRecord(registry, pid, name, socketPath, sessionId) {
	return {
		version: 1,
		pid,
		sessionId,
		cwd: process.cwd(),
		name,
		nameSource: "user",
		formerNames: [],
		kind: "interactive",
		status: "idle",
		statusUpdatedAt: Date.now(),
		startedAt: Date.now(),
		procStart: registry.readProcStart(pid),
		pidDomain: registry.pidDomain(),
		socketPath,
		peerProtocol: PEER_PROTOCOL_VERSION,
		features: [...PEER_FEATURES],
	};
}

// ---------------------------------------------------------------------------
// Output helpers
// ---------------------------------------------------------------------------

function printStatsTable(title, note, rows) {
	console.log(`\n[${title}]`);
	for (const line of note) console.log(`    ${line}`);
	const labelW = 32;
	const colW = 12;
	console.log("    " + "metric".padEnd(labelW) + ["p50", "p95", "min", "max"].map((h) => h.padStart(colW)).join(""));
	for (const row of rows) {
		const s = row.st;
		const cells = [s.p50, s.p95, s.min, s.max].map((v) => `${v.toFixed(row.dec)} ${row.unit}`.padStart(colW));
		console.log("    " + row.label.padEnd(labelW) + cells.join(""));
	}
}

function printBanner() {
	const cpus = os.cpus();
	console.log("pi-peers performance baseline");
	console.log("=============================");
	console.log(`date     : ${new Date().toISOString().slice(0, 10)}`);
	console.log(`platform : ${process.platform} ${os.arch()} (release ${os.release()})`);
	console.log(`node     : ${process.version}`);
	console.log(`cpu      : ${cpus[0]?.model ?? "unknown"} x ${cpus.length}`);
}

// ---------------------------------------------------------------------------
// Section 1: registry scan (cold vs. warm)
// ---------------------------------------------------------------------------

async function runRegistry(ctx) {
	const peersDir = mkdtempSync(join(os.tmpdir(), "pi-peers-bench-registry-"));
	ctx.dirs.push(peersDir);
	const registry = createPeerRegistry(peersDir);
	const auth = createPeerAuth();

	// Fixture: this process + (N-1) sleeper children; a record + key file each.
	const sleepers = [];
	for (let i = 0; i < REGISTRY_N - 1; i++) sleepers.push(spawnSleeper());
	ctx.sleepers.push(...sleepers.map((s) => s.child));
	await Promise.all(sleepers.map((s) => s.ready));
	const pids = [process.pid, ...sleepers.map((s) => s.child.pid)];
	for (const [i, pid] of pids.entries()) await waitAlive(pid, `record-pid-${i}`);
	if (new Set(pids).size !== REGISTRY_N) throw new Error("fixture pid collision");

	for (const [i, pid] of pids.entries()) {
		const addr = makeAddress(`reg${i}`);
		const record = makeRecord(
			registry,
			pid,
			`bench-${String(i + 1).padStart(2, "0")}`,
			addr.path,
			randomBytes(16).toString("hex"),
		);
		registry.publish(record);
		auth.publishKey(peersDir, pid, addr.path, auth.generateTokens(), record.procStart, record.pidDomain);
	}

	// Cold: force a real scan AND evict every cached liveness verdict first, so
	// all 50 pids are genuinely probed (kill(pid,0)) on every measured scan.
	// Without the eviction the 1.5 s alive-cache would skip the probes.
	const cold = [];
	for (let i = 0; i < COLD_WARMUP + COLD_ITERS; i++) {
		for (const pid of pids) registry.invalidateAlive(pid);
		const t0 = performance.now();
		const entries = registry.list({ fresh: true });
		const dt = performance.now() - t0;
		if (entries.length !== REGISTRY_N) {
			throw new Error(`scan saw ${entries.length} entries, expected ${REGISTRY_N} (fixture was swept)`);
		}
		if (!entries.every((e) => e.live)) throw new Error("scan reported a dead record — fixture broken");
		if (i >= COLD_WARMUP) cold.push(dt);
	}

	// Warm: list() served from the <=500 ms result cache. Per-call latency is
	// below clock resolution, so each batch of WARM_BATCH_OPS calls is timed
	// as a whole and the median of the batch means is reported.
	const warm = [];
	for (let b = 0; b < WARM_BATCHES; b++) {
		registry.list(); // (re)anchor the cache; the timed calls below all hit it
		const t0 = performance.now();
		for (let j = 0; j < WARM_BATCH_OPS; j++) registry.list();
		const dt = performance.now() - t0;
		warm.push((dt * 1e6) / WARM_BATCH_OPS); // ns per call
	}

	printStatsTable(
		`1. registry scan — N=${REGISTRY_N} records + key files, all pids live`,
		[
			`fixture: this process + ${REGISTRY_N - 1} sleeper children, one <pid>.json record and one key file each`,
			`cold: list({ fresh: true }) after registry.invalidateAlive(pid) for every pid (all probes re-run)`,
			`warm: list() served from the 500 ms result cache (${WARM_BATCHES} batches x ${WARM_BATCH_OPS} calls)`,
			`${COLD_ITERS} measured scans (+${COLD_WARMUP} warmup, discarded)`,
		],
		[
			{ label: "cold scan list({fresh:true})", st: stats(cold), unit: "ms", dec: 2 },
			{ label: "warm cache hit list()", st: stats(warm), unit: "ns", dec: 0 },
		],
	);
}

// ---------------------------------------------------------------------------
// Section 2: send() full path with delivered receipt
// ---------------------------------------------------------------------------

async function runSend(ctx) {
	const peersDir = mkdtempSync(join(os.tmpdir(), "pi-peers-bench-send-"));
	ctx.dirs.push(peersDir);
	const protocol = createPeerProtocol();
	const transport = createPeerTransport();
	const auth = createPeerAuth();
	const registry = createPeerRegistry(peersDir);

	// The receiver's registry record needs a live pid: one dedicated sleeper.
	const sleeper = spawnSleeper();
	ctx.sleepers.push(sleeper.child);
	await sleeper.ready;
	await waitAlive(sleeper.child.pid, "receiver pid");

	// ---- sender stack ("alpha", this process) ----
	const alphaAddr = makeAddress("alpha");
	const alphaTokens = auth.generateTokens();
	const alphaIdentity = {
		pid: process.pid,
		sessionId: randomBytes(16).toString("hex"),
		socketAddress: alphaAddr.address,
		name: "alpha",
		hopToken: randomBytes(4).toString("hex"),
		tokens: alphaTokens,
	};
	// The sender's host: host.notify("Peer message", ...) fires exactly once
	// per delivered receipt (inside outbox.handleReceipt), which is the signal
	// the timed loop waits on — no polling granularity in the measurement.
	let receiptArrived = Promise.withResolvers();
	const alphaHost = {
		notify: (title) => {
			if (title === "Peer message") receiptArrived.resolve();
		},
		confirm: async () => "yes",
		injectAgentNotice: async () => {},
		injectUserMessage: async () => {},
		hasUI: () => true,
		isIdleNow: () => true,
		quit: () => {},
		acceptPolicy: () => "all",
	};
	// Pacer bucket enlarged (sanctioned option): default 30 tokens @ 0.5/s
	// would pace the 200 back-to-back sends.
	const alphaPacer = createPeerSenderPacer({ rateCapacity: 1000 });
	const alphaOutbox = createPeerOutbox({
		identity: alphaIdentity,
		protocol,
		transport,
		auth,
		registry,
		pacer: alphaPacer,
		host: alphaHost,
		peersDir,
	});
	const alphaInbox = createPeerInbox({
		identity: alphaIdentity,
		protocol,
		transport,
		registry,
		guard: createPeerGuard(),
		host: alphaHost,
		outbox: alphaOutbox,
	});
	const alphaRecord = makeRecord(registry, process.pid, "alpha", alphaAddr.path, alphaIdentity.sessionId);
	registry.publish(alphaRecord);
	auth.publishKey(peersDir, process.pid, alphaAddr.path, alphaTokens, alphaRecord.procStart, alphaRecord.pidDomain);

	// ---- receiver stack ("bravo", sleeper pid) ----
	const bravoAddr = makeAddress("bravo");
	const bravoTokens = auth.generateTokens();
	const bravoIdentity = {
		pid: sleeper.child.pid,
		sessionId: randomBytes(16).toString("hex"),
		socketAddress: bravoAddr.address,
		name: "bravo",
		hopToken: randomBytes(4).toString("hex"),
		tokens: bravoTokens,
	};
	const bravoHost = {
		injected: [],
		notify: () => {},
		confirm: async () => "yes",
		injectAgentNotice: async () => {},
		injectUserMessage: async (content) => {
			bravoHost.injected.push(content);
		},
		hasUI: () => true,
		isIdleNow: () => true,
		quit: () => {},
		acceptPolicy: () => "all",
	};
	// Guard buckets enlarged (sanctioned options): defaults (30 per-sender /
	// 120 global) would rate-limit the 200-message stream.
	const bravoGuard = createPeerGuard({ rateCapacity: 1000, globalRateCapacity: 5000 });
	const bravoPacer = createPeerSenderPacer({ rateCapacity: 1000 });
	const bravoOutbox = createPeerOutbox({
		identity: bravoIdentity,
		protocol,
		transport,
		auth,
		registry,
		pacer: bravoPacer,
		host: bravoHost,
		peersDir,
	});
	const bravoInboxDeps = {
		identity: bravoIdentity,
		protocol,
		transport,
		registry,
		guard: bravoGuard,
		host: bravoHost,
		outbox: bravoOutbox,
	};
	// Recreated per iteration: the receipt-reflection budget (burst 4, 1 token
	// / 2 s) lives in the inbox closure and is not injectable any other way.
	let bravoInbox = createPeerInbox(bravoInboxDeps);
	const bravoRecord = makeRecord(
		registry,
		sleeper.child.pid,
		"bravo",
		bravoAddr.path,
		bravoIdentity.sessionId,
	);
	registry.publish(bravoRecord);
	auth.publishKey(
		peersDir,
		sleeper.child.pid,
		bravoAddr.path,
		bravoTokens,
		bravoRecord.procStart,
		bravoRecord.pidDomain,
	);

	// ---- servers (accept loop mirrors the wiring in the e2e test) ----
	const startServer = (stack, onFrame) =>
		transport
			.listen(stack.address, (conn, firstLine, lines) => {
				const token = firstLine !== undefined ? protocol.parseAuthLine(firstLine) : undefined;
				const cls =
					token === stack.tokens.childToken ? "child" : token === stack.tokens.peerToken ? "peer" : undefined;
				if (cls === undefined) {
					conn.destroy();
					return;
				}
				void (async () => {
					try {
						for await (const line of lines) {
							const frame = protocol.parseFrameLine(line);
							if (frame === undefined) {
								conn.destroy();
								return;
							}
							await onFrame(frame, cls);
						}
					} catch {
						conn.destroy();
					}
				})();
			})
			.then((server) => {
				ctx.servers.push(server);
				return server;
			});

	await startServer(
		{ address: alphaAddr, tokens: alphaTokens },
		(frame, cls) => alphaInbox.handleFrame(frame, cls),
	);
	await startServer(
		{ address: bravoAddr, tokens: bravoTokens },
		// Reads the current inbox at frame-arrival time: the fresh one per send.
		(frame, cls) => bravoInbox.handleFrame(frame, cls),
	);

	// ---- timed loop ----
	async function oneSend(seq) {
		bravoInbox = createPeerInbox(bravoInboxDeps);
		receiptArrived = Promise.withResolvers();
		const body = `bench ${String(seq).padStart(6, "0")} ` + "x".repeat(Math.max(0, SEND_BODY_BYTES - 13));
		let timeoutHandle;
		const timeout = new Promise((_, reject) => {
			timeoutHandle = setTimeout(
				() => reject(new Error(`receipt for send #${seq} did not arrive within ${RECEIPT_TIMEOUT_MS} ms`)),
				RECEIPT_TIMEOUT_MS,
			);
		});
		try {
			const t0 = performance.now();
			const outcome = await alphaOutbox.send("bravo", body, {});
			await Promise.race([receiptArrived.promise, timeout]);
			const dt = performance.now() - t0;
			const state = alphaOutbox.receiptOf(outcome.msgId);
			if (state !== "delivered") throw new Error(`send #${seq}: receipt state is ${state}`);
			return { dt, bodyBytes: Buffer.byteLength(body, "utf8") };
		} finally {
			clearTimeout(timeoutHandle);
		}
	}

	let bodyBytes = 0;
	for (let i = 0; i < SEND_WARMUP; i++) {
		const r = await oneSend(i);
		bodyBytes = r.bodyBytes;
	}
	if (bravoHost.injected.length !== SEND_WARMUP) {
		throw new Error(`warmup injected ${bravoHost.injected.length} messages, expected ${SEND_WARMUP}`);
	}
	if (!bravoHost.injected[SEND_WARMUP - 1].includes("cross-session-message")) {
		throw new Error("injected content is not a wrapped peer message");
	}

	const times = [];
	for (let i = 0; i < SEND_ITERS; i++) {
		const r = await oneSend(SEND_WARMUP + i);
		bodyBytes = r.bodyBytes;
		times.push(r.dt);
	}
	if (bravoHost.injected.length !== SEND_WARMUP + SEND_ITERS) {
		throw new Error(`measured loop injected ${bravoHost.injected.length} messages, expected ${SEND_WARMUP + SEND_ITERS}`);
	}

	printStatsTable(
		"2. send full path — real pipe server + token auth + user frame + delivered receipt",
		[
			'sender "alpha" (this process) -> receiver "bravo" (sleeper pid), two full stacks with live servers',
			"timed region: outbox.send() start ... delivered receipt observed on the sender (host notify signal)",
			`path: resolve -> key lookup -> connect -> auth line -> frame -> receiver guard/inject -> receipt frame back`,
			`${SEND_ITERS} iterations (+${SEND_WARMUP} warmup, discarded), ~${bodyBytes} B body per message`,
			"anti-abuse relaxed: pacer/guard buckets enlarged via options; receiver inbox recreated per iteration",
			"(resets the non-injectable receipt budget) — see BENCH.md",
		],
		[{ label: "send() -> delivered receipt", st: stats(times), unit: "ms", dec: 2 }],
	);
}

// ---------------------------------------------------------------------------
// Section 3: protocol codec throughput
// ---------------------------------------------------------------------------

function runProtocol() {
	const protocol = createPeerProtocol();
	const addr = makeAddress("proto");
	const fromSession = randomBytes(16).toString("hex");
	const frame = {
		...protocol.newEnvelope(),
		type: "user",
		from: addr.address,
		fromPid: process.pid,
		fromSession,
		fromName: "bench",
		hopChain: [randomBytes(4).toString("hex")],
		priority: "next",
		content: protocol.wrapPeerContent(
			{ from: addr.address, fromSession, fromName: "bench" },
			"y".repeat(PROTO_BODY_BYTES),
		),
	};
	const line = protocol.serializeFrame(frame);
	const lineNoNl = line.slice(0, -1);
	const frameBytes = Buffer.byteLength(line, "utf8");
	const check = protocol.parseFrameLine(lineNoNl);
	if (check?.type !== "user" || check.content !== frame.content) {
		throw new Error("protocol round-trip check failed");
	}

	function bench(op) {
		for (let b = 0; b < PROTO_WARMUP_BATCHES; b++) {
			for (let i = 0; i < PROTO_BATCH_OPS; i++) op();
		}
		const rates = [];
		for (let b = 0; b < PROTO_BATCHES; b++) {
			const t0 = performance.now();
			for (let i = 0; i < PROTO_BATCH_OPS; i++) op();
			const dt = performance.now() - t0;
			if (dt <= 0) throw new Error("clock resolution too low for one batch");
			rates.push(PROTO_BATCH_OPS / (dt / 1000)); // ops/sec of this batch
		}
		return stats(rates);
	}

	const ser = bench(() => protocol.serializeFrame(frame));
	const par = bench(() => protocol.parseFrameLine(lineNoNl));

	console.log(`\n[3. protocol codec — 1 user frame, ${frameBytes} B on the wire]`);
	console.log(
		`    ${PROTO_BATCHES} batches x ${PROTO_BATCH_OPS} ops (+${PROTO_WARMUP_BATCHES} warmup batches, discarded), median batch rate`,
	);
	console.log("    " + "op".padEnd(18) + "ops/sec".padStart(14) + "per-op".padStart(12));
	for (const [label, s] of [
		["serializeFrame", ser],
		["parseFrameLine", par],
	]) {
		console.log(
			"    " +
				label.padEnd(18) +
				Math.round(s.p50).toLocaleString("en-US").padStart(14) +
				`${(1e6 / s.p50).toFixed(2)} us`.padStart(12),
		);
	}
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
	printBanner();
	const ctx = { sleepers: [], dirs: [], servers: [] };
	try {
		await runRegistry(ctx);
		await runSend(ctx);
		runProtocol();
	} finally {
		for (const server of ctx.servers) {
			try {
				await server.close();
			} catch {
				// best-effort teardown
			}
		}
		for (const child of ctx.sleepers) {
			try {
				child.kill();
			} catch {
				// already gone
			}
		}
		for (const dir of ctx.dirs) {
			try {
				rmSync(dir, { recursive: true, force: true });
			} catch {
				// best-effort teardown
			}
		}
	}
}

main().catch((err) => {
	console.error(`\nbench failed: ${err?.stack ?? String(err)}`);
	process.exitCode = 1;
});
