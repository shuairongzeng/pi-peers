/**
 * Fault-injection suite — the "zero silent loss" contract:
 * every failure mode must surface as a typed error, an explicit drop reason,
 * or observable cleanup behavior. Never a hang, never an unhandledRejection,
 * never a crashed host process.
 *
 * Two harnesses:
 * - Registry faults (garbage bytes, torn JSON, wrong-typed fields, zombie
 *   record: live pid without its key file) run against a real tmp registry
 *   directory — harness patterns from peers-registry.test.ts.
 * - Wire faults (peer vanishing mid-send, half line + EOF, over-cap line,
 *   replayed frame, silent first line) run against a REAL transport pair
 *   (named pipe on Windows / UDS elsewhere) with full outbox+inbox stacks —
 *   harness patterns from peers-end-to-end.test.ts.
 *
 * Real wall-clock timers appear only where the fault under test IS a
 * socket/timer lifetime: the watchdogs (a hang regression must fail this
 * file, not the CI run) and scenario 6 (first-line timeout, driven through
 * the listener's firstLineTimeoutMs injection point so the same code path
 * runs without a real 30s wait). Everything else awaits real events (close,
 * receipts, injections) through bounded polls, never guessed sleeps.
 */

import { spawn, type ChildProcess } from "node:child_process";
import { randomBytes } from "node:crypto";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { connect as netConnect, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createPeerAuth, keyFileNameFor } from "../src/auth.ts";
import { createPeerGuard, createPeerSenderPacer } from "../src/guard.ts";
import { createPeerInbox } from "../src/inbox.ts";
import { createPeerOutbox } from "../src/outbox.ts";
import { createPeerProtocol } from "../src/protocol.ts";
import { createPeerRegistry } from "../src/registry.ts";
import { createPeerTransport } from "../src/transport.ts";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import {
	PEER_FEATURES,
	PEER_FIRST_LINE_TIMEOUT_MS,
	PEER_MAX_LINE_BYTES,
	PEER_PROTOCOL_VERSION,
	PeerSendError,
	type PeerConnection,
	type PeerFrame,
	type PeerHost,
	type PeerIdentity,
	type PeerOutbox,
	type PeerRecord,
	type PeerRegistry,
	type PeerSendOutcome,
	type PeerSocketAddress,
	type PeerServer,
} from "../src/types.ts";

// ---------------------------------------------------------------------------
// Watchdogs: a fault that regresses into a hang must fail HERE
// ---------------------------------------------------------------------------

/**
 * Race a promise against a watchdog timer. Real timers are the point: the
 * scenarios exercise real socket lifetimes, and a lost settlement must
 * surface as this file's failure, not a CI timeout.
 */
async function bounded<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
	const { promise: timeout, reject } = Promise.withResolvers<never>();
	const timer = setTimeout(() => {
		reject(new Error(`fault watchdog: ${label} did not settle within ${ms}ms (hang regression)`));
	}, ms);
	timer.unref();
	try {
		return await Promise.race([promise, timeout]);
	} finally {
		clearTimeout(timer);
	}
}

/**
 * Poll `probe()` until true, deadline-bounded. The 10ms interval is a poll
 * floor, not a fixed wait — the awaited condition (receipt, injection, close)
 * drives completion as soon as its event lands.
 */
async function untilTrue(probe: () => boolean, ms: number, label: string): Promise<void> {
	const expiresAt = Date.now() + ms;
	await bounded(
		(async () => {
			while (!probe()) {
				if (Date.now() >= expiresAt) {
					throw new Error(`fault watchdog: ${label} never became true within ${ms}ms`);
				}
				const { promise, resolve } = Promise.withResolvers<void>();
				const timer = setTimeout(resolve, 10);
				timer.unref();
				await promise;
			}
		})(),
		ms + 5_000,
		`outer watchdog: ${label}`,
	);
}

// ---------------------------------------------------------------------------
// unhandledRejection surveillance (per test)
// ---------------------------------------------------------------------------

/** Rejections recorded during the current test; reset by beforeEach. */
let unhandledRejections: unknown[] = [];
const rejectionListener = (reason: unknown) => {
	unhandledRejections.push(reason);
};

beforeEach(() => {
	unhandledRejections = [];
	process.on("unhandledRejection", rejectionListener);
});

afterEach(() => {
	process.off("unhandledRejection", rejectionListener);
	// Zero-silent-loss contract: a fault must surface as a typed error, an
	// explicit drop reason, or cleanup — never as a process-level unhandled
	// rejection that could crash the host.
	const observed = unhandledRejections;
	unhandledRejections = [];
	expect(
		observed.map((reason) => (reason instanceof Error ? `${reason.name}: ${reason.message}` : String(reason))),
		"unhandledRejection leaked during a fault scenario",
	).toEqual([]);
});

// ---------------------------------------------------------------------------
// Shared fixtures
// ---------------------------------------------------------------------------

const tempDirs: string[] = [];
afterEach(() => {
	for (const dir of tempDirs.splice(0)) {
		rmSync(dir, { recursive: true, force: true });
	}
});

function tempDir(prefix: string): string {
	const dir = mkdtempSync(join(tmpdir(), prefix));
	tempDirs.push(dir);
	return dir;
}

/** A structurally valid registry record for fixture purposes. */
function fixtureRecord(pid: number, overrides: Partial<PeerRecord> = {}): PeerRecord {
	return {
		version: 1,
		pid,
		sessionId: `session-${pid}`,
		cwd: "/tmp",
		name: `peer-${pid}`,
		nameSource: "user",
		formerNames: [],
		kind: "interactive",
		status: "idle",
		statusUpdatedAt: 1_000,
		startedAt: 900,
		pidDomain: "win32:faulthost",
		socketPath: `\\\\.\\pipe\\LOCAL\\pi-peer-${String(pid).padStart(32, "0")}`,
		peerProtocol: PEER_PROTOCOL_VERSION,
		features: [...PEER_FEATURES],
		...overrides,
	};
}

// ---------------------------------------------------------------------------
// Real-socket harness (patterns from peers-end-to-end / peers-transport)
// ---------------------------------------------------------------------------

const protocol = createPeerProtocol();
const transport = createPeerTransport();

/** A recording host: policy "all" keeps the accept pipeline synchronous. */
function makeRecordingHost() {
	const injected: Array<{ content: string; priority: string }> = [];
	const notices: Array<{ title: string; message: string }> = [];
	const agentNotices: string[] = [];
	const host: PeerHost = {
		injectUserMessage: async (content, priority) => {
			injected.push({ content, priority });
		},
		notify: (title, message) => {
			notices.push({ title, message });
		},
		confirm: async () => "yes",
		injectAgentNotice: async (text) => {
			agentNotices.push(text);
		},
		hasUI: () => true,
		isIdleNow: () => true,
		quit: () => {},
		acceptPolicy: () => "all",
	};
	return { host, injected, notices, agentNotices };
}

/** A pipe/UDS test address unique per call, exactly like the e2e harness. */
function testAddress(label: string): PeerSocketAddress {
	const name = `pi-peer-${randomBytes(16).toString("hex")}`;
	if (process.platform === "win32") {
		return { path: `\\\\.\\pipe\\LOCAL\\${name}`, address: `pipe:LOCAL\\${name}` };
	}
	const path = join(tmpdir(), `pi-peer-fault-${label}-${randomBytes(4).toString("hex")}.sock`);
	return { path, address: `sock:${path}` };
}

/** Listener cleanup queue (also removes POSIX socket files). */
const listenerClosers: Array<() => Promise<void>> = [];
afterEach(async () => {
	const closers = listenerClosers.splice(0);
	await bounded(Promise.allSettled(closers.map((close) => close())), 10_000, "listener cleanup");
});

/** One fully wired peer stack on the real transport. */
interface WireStack {
	name: string;
	pid: number;
	address: PeerSocketAddress;
	identity: PeerIdentity;
	outbox: PeerOutbox;
	registry: PeerRegistry;
	server: PeerServer | undefined;
	serverReady: Promise<void>;
	record: PeerRecord;
	injected: Array<{ content: string; priority: string }>;
	notices: Array<{ title: string; message: string }>;
	agentNotices: string[];
	/** Every frame the accept loop parsed (pre-guard), for drop-path visibility. */
	handledFrames: PeerFrame[];
	/** Connection lifecycle: onConnection invocations vs exited accept loops. */
	connStats: { opened: number; closed: number };
}

/**
 * Build one stack: outbox + inbox + e2e-style accept loop (auth → guard →
 * inject). `onFault`, when set, replaces the healthy accept loop — the
 * fault injection point for the mid-send vanish scenario.
 */
function buildStack(
	name: string,
	pid: number,
	peersDir: string,
	onFault?: (conn: PeerConnection, firstLine: string | undefined) => void,
): { stack: WireStack; token: string } {
	const registry = createPeerRegistry(peersDir);
	const guard = createPeerGuard();
	const pacer = createPeerSenderPacer();
	const { host, injected, notices, agentNotices } = makeRecordingHost();
	const address = testAddress(name);
	const token = randomBytes(16).toString("hex");
	const identity: PeerIdentity = {
		pid,
		sessionId: randomBytes(16).toString("hex"),
		socketAddress: address.address,
		name,
		hopToken: randomBytes(4).toString("hex"),
		tokens: { peerToken: token, childToken: randomBytes(16).toString("hex") },
	};
	const outbox = createPeerOutbox({
		identity,
		protocol,
		transport,
		auth: auth,
		registry,
		pacer,
		host,
		peersDir,
	});
	const inbox = createPeerInbox({ identity, protocol, transport, registry, guard, host, outbox });
	const record = fixtureRecord(pid, {
		sessionId: identity.sessionId,
		cwd: process.cwd(),
		name,
		socketPath: address.path,
	});

	const stack: WireStack = {
		name,
		pid,
		address,
		identity,
		outbox,
		registry,
		server: undefined,
		serverReady: Promise.resolve(),
		record,
		injected,
		notices,
		agentNotices,
		handledFrames: [],
		connStats: { opened: 0, closed: 0 },
	};

	const listening = transport.listen(address, (conn, firstLine, lines) => {
		stack.connStats.opened++;
		if (onFault !== undefined) {
			// Fault injection: the peer "dies" right after the auth line, before
			// any frame is parsed.
			onFault(conn, firstLine);
			return;
		}
		const parsed = firstLine !== undefined ? protocol.parseAuthLine(firstLine) : undefined;
		if (parsed !== token) {
			conn.destroy();
			return;
		}
		void (async () => {
			try {
				for await (const line of lines) {
					const frame: PeerFrame | undefined = protocol.parseFrameLine(line);
					if (frame === undefined) {
						conn.destroy();
						return;
					}
					stack.handledFrames.push(frame);
					await inbox.handleFrame(frame, "peer");
				}
			} finally {
				stack.connStats.closed++;
			}
		})();
	});
	listenerClosers.push(() => listening.then((server) => server.close()));
	stack.serverReady = listening.then((server) => {
		stack.server = server;
	});
	return { stack, token };
}

/** Real auth module: key files are published and looked up on disk. */
const auth = createPeerAuth();

/** Raw (framing-free) client socket to a listening address. */
function rawConnect(path: string): Promise<Socket> {
	const { promise, resolve, reject } = Promise.withResolvers<Socket>();
	const sock = netConnect({ path });
	sock.once("connect", () => resolve(sock));
	sock.once("error", (error) => reject(error));
	return promise;
}

/** Resolve on the socket's close event, deadline-bounded. */
function closed(sock: Socket, ms: number, label: string): Promise<void> {
	const { promise, resolve } = Promise.withResolvers<void>();
	if (sock.destroyed) {
		resolve();
	} else {
		sock.once("close", () => resolve());
	}
	return bounded(promise, ms, label);
}

/**
 * One send under the fault watchdog. Never hangs; a non-typed rejection
 * fails the test directly (that would itself violate the contract).
 */
async function auditedSend(
	send: () => Promise<PeerSendOutcome>,
	ms: number,
	label: string,
): Promise<{ typedError?: PeerSendError; outcome?: PeerSendOutcome }> {
	const result: { typedError?: PeerSendError; outcome?: PeerSendOutcome } = {};
	await bounded(
		(async () => {
			try {
				result.outcome = await send();
			} catch (error) {
				if (error instanceof PeerSendError) {
					result.typedError = error;
				} else {
					throw error;
				}
			}
		})(),
		ms,
		label,
	);
	return result;
}

// ---------------------------------------------------------------------------
// Scenario 1: corrupted registry directory
// ---------------------------------------------------------------------------

describe("fault: corrupted registry directory", () => {
	let peersDir: string;
	let registry: PeerRegistry;
	let liveChild: ChildProcess | undefined;

	/** Fresh isolated registry directory per test — list() self-healing is stateful. */
	function freshRegistry(): PeerRegistry {
		peersDir = tempDir("pi-peers-fault-registry-");
		registry = createPeerRegistry(peersDir);
		return registry;
	}

	beforeAll(async () => {
		liveChild = spawn(process.execPath, ["-e", "process.stdin.resume()"], { stdio: "ignore" });
		if (typeof liveChild.pid !== "number") {
			throw new Error("failed to spawn registry fixture child process");
		}
		const { promise, resolve } = Promise.withResolvers<void>();
		liveChild.once("spawn", () => resolve());
		await promise;
	});

	afterAll(async () => {
		if (liveChild !== undefined) {
			liveChild.kill();
			await bounded(new Promise<void>((resolve) => liveChild!.once("exit", () => resolve())), 10_000, "registry fixture child exit");
		}
	});

	it("scenario 1: garbage bytes, torn JSON, wrong-typed fields, and a zombie record never throw; the valid record survives and the corruption is swept", () => {
		freshRegistry();

		// A genuinely-live peer (spawned child) with record + key: the signal
		// that corruption cannot break discovery of healthy peers.
		const good = fixtureRecord(liveChild!.pid!, { name: "good-peer" });
		registry.publish(good);
		writeFileSync(join(peersDir, keyFileNameFor(good.pid, good.socketPath)), "{}");

		// Garbage bytes (invalid UTF-8, control characters).
		writeFileSync(join(peersDir, "5555.json"), Buffer.from([0x00, 0xff, 0xfe, 0x00, 0x12]));
		// Torn JSON: a crash between an atomic write's temp create and rename.
		writeFileSync(join(peersDir, "6666.json"), '{"version":1,"pid":');
		// Valid JSON, every field the wrong type.
		writeFileSync(
			join(peersDir, "7777.json"),
			JSON.stringify({ version: "one", pid: "string", sessionId: [], cwd: {}, name: 42, features: "no" }),
		);
		// Zombie: structurally valid record, pid alive (this process), key file missing.
		writeFileSync(
			join(peersDir, `${process.pid}.json`),
			JSON.stringify(fixtureRecord(process.pid, { name: "zombie-peer", sessionId: "zombie-session" })),
		);

		// The core contract: list() never throws, corrupt files or not.
		const entries = registry.list();

		// Zero silent loss for healthy peers: the valid record is still returned.
		const goodEntry = entries.find((entry) => entry.record.pid === good.pid);
		expect(goodEntry, "the valid record must still be discoverable beside corruption").toBeDefined();
		expect(goodEntry!.live).toBe(true);

		// The zombie is reported once (live: false — unreachable for sends) and swept.
		const zombieEntry = entries.find((entry) => entry.record.sessionId === "zombie-session");
		expect(zombieEntry, "the zombie record must be reported, not silently dropped").toBeDefined();
		expect(zombieEntry!.live).toBe(false);
		expect(existsSync(join(peersDir, `${process.pid}.json`))).toBe(false);

		// Self-healing: every corrupt file is gone after the scan.
		for (const file of ["5555.json", "6666.json", "7777.json"]) {
			expect(existsSync(join(peersDir, file))).toBe(false);
		}

		// The cached result path (second list() within the scan-cache TTL) is
		// also throw-free and still serves the valid record.
		const cached = registry.list();
		expect(cached.some((entry) => entry.record.pid === good.pid && entry.live)).toBe(true);
	});

	it("scenario 1b: a directory of ONLY corruption lists as empty and sweeps everything, without throwing", () => {
		freshRegistry();
		writeFileSync(join(peersDir, "5555.json"), Buffer.from([0x00, 0xff, 0xfe, 0x00, 0x12]));
		writeFileSync(join(peersDir, "6666.json"), '{"version":1,"pid":');
		writeFileSync(join(peersDir, "7777.json"), "not json at all");

		expect(registry.list()).toEqual([]);
		for (const file of ["5555.json", "6666.json", "7777.json"]) {
			expect(existsSync(join(peersDir, file))).toBe(false);
		}
	});
});

// ---------------------------------------------------------------------------
// Scenarios 2–6: real-transport fault injection
// ---------------------------------------------------------------------------

describe("fault: real-transport peer failures", () => {
	let peersDir: string;
	let sender: WireStack;
	let target: WireStack;
	let vanisher: WireStack;
	let targetToken: string;
	let liveChildA: ChildProcess | undefined;
	let liveChildB: ChildProcess | undefined;

	/** Spawn one live pid and resolve once its spawn event lands. */
	async function spawnLiveChild(): Promise<ChildProcess> {
		// pipe stdin like peers-registry.test.ts: a piped stdin keeps the
		// child alive deterministically and its exit event observable.
		const child = spawn(process.execPath, ["-e", "process.stdin.resume()"], { stdio: ["pipe", "ignore", "ignore"] });
		if (typeof child.pid !== "number") {
			throw new Error("failed to spawn wire fixture child process");
		}
		const { promise, resolve } = Promise.withResolvers<void>();
		child.once("spawn", () => resolve());
		await promise;
		return child;
	}

	beforeEach(async () => {
		peersDir = tempDir("pi-peers-fault-wire-");
		liveChildA = await spawnLiveChild();
		liveChildB = await spawnLiveChild();

		const senderBuilt = buildStack("sender", process.pid, peersDir);
		const targetBuilt = buildStack("target", liveChildA.pid!, peersDir);
		// The vanisher: a real, registered, key-published peer whose listener
		// destroys every connection right after the auth line — "dies mid-send".
		const vanisherBuilt = buildStack("vanisher", liveChildB.pid!, peersDir, (conn) => {
			conn.destroy();
		});
		sender = senderBuilt.stack;
		target = targetBuilt.stack;
		vanisher = vanisherBuilt.stack;
		targetToken = targetBuilt.token;
		await bounded(Promise.all([sender.serverReady, target.serverReady, vanisher.serverReady]), 10_000, "stack listeners up");

		// Publish records + real key files so outbox.send() resolves and
		// authenticates (exactly the e2e harness's publish step).
		for (const stack of [sender, target, vanisher]) {
			stack.registry.publish(stack.record);
			auth.publishKey(peersDir, stack.pid, stack.address.path, stack.identity.tokens, undefined, stack.record.pidDomain);
		}
	});

	afterEach(async () => {
		const children = [liveChildA, liveChildB].filter((child) => child !== undefined);
		for (const child of children) {
			child.kill();
		}
		const exits = children.map((child) => {
			const { promise, resolve } = Promise.withResolvers<void>();
			child.once("exit", () => resolve());
			return promise;
		});
		await bounded(Promise.all(exits), 10_000, "wire fixture children exit");
	});

	it("scenario 2: a peer vanishing mid-send settles the sender's promise in time — typed error once the endpoint is gone, receipt-absence otherwise — with zero unhandledRejections", async () => {
		// Phase A: the vanisher destroys the connection right after the auth
		// line, before the frame is parsed ("mid-send" from the wire's view).
		const consoleErrors: string[] = [];
		const spy = vi.spyOn(console, "error").mockImplementation((...args: unknown[]) => {
			consoleErrors.push(args.map(String).join(" "));
		});
		let phaseA: { typedError?: PeerSendError; outcome?: PeerSendOutcome };
		try {
			// ~600KB body: comfortably under the 1MB wire cap, large enough that
			// the flush into the destroyed pipe fails observably.
			phaseA = await auditedSend(
				() => sender.outbox.send("vanisher", "v".repeat(600 * 1024), {}),
				10_000,
				"send to a peer that vanishes mid-send",
			);
		} finally {
			spy.mockRestore();
		}

		if (phaseA.typedError !== undefined) {
			// Connect-phase fault: the typed-error family the contract promises.
			expect(["connect_failed", "timeout", "offline", "connection_lost"]).toContain(phaseA.typedError.code);
			// A mid-flush vanish is connection_lost by contract: the connect
			// succeeded, the flush failed — the caller must be told delivery
			// is uncertain (this is the silent-loss hole this suite pins).
			if (phaseA.typedError.code === "connection_lost") {
				expect(phaseA.typedError.message).toContain("disappeared mid-send");
				expect(phaseA.typedError.message).toContain(vanisher.address.path);
			}
			expect(phaseA.outcome).toBeUndefined();
		} else {
			// Post-auth vanish where the flush happened to complete: the send
			// resolved. The designed failure signal is the receipt that never
			// arrives (receipts are the delivery contract) plus the module-
			// owned log.
			expect(phaseA.outcome).toBeDefined();
			// Absence can only be sampled over time: local-IPC receipts arrive
			// within milliseconds of delivery, so a 300ms window is a generous
			// bound — bounded() turns a regression into a test failure.
			await new Promise<void>((resolve) => {
				const timer = setTimeout(resolve, 300);
				timer.unref();
			});
			expect(sender.outbox.receiptOf(phaseA.outcome!.msgId)).toBeUndefined();
			// Whatever failure was logged must be module-owned (prefixed),
			// never a raw crash dump — and nothing may have thrown past send().
			for (const line of consoleErrors) {
				expect(line.startsWith("[pi-peers]") || line.startsWith("[peers]")).toBe(true);
			}
		}

		// Phase B: the endpoint is now fully gone (listener closed, process
		// exited) — the next send MUST surface the typed error family.
		await bounded(vanisher.server!.close(), 5_000, "closing the vanishing server");
		const phaseB = await auditedSend(
			() => sender.outbox.send("vanisher", "after the peer fully vanished", {}),
			10_000,
			"send after the peer fully vanished",
		);
		expect(phaseB.typedError, "a fully-vanished endpoint must reject with a typed PeerSendError").toBeInstanceOf(
			PeerSendError,
		);
		expect(["connect_failed", "timeout", "offline", "connection_lost"]).toContain(phaseB.typedError!.code);
		// Zero unhandledRejections during the whole scenario: asserted by the
		// file-level afterEach, exactly the "host never crashes" contract.
	});

	it("scenario 3: half a frame + EOF cleans the connection up and leaves the server serving the next one", async () => {
		const sock = await bounded(rawConnect(target.address.path), 5_000, "raw client connect (half-line fault)");
		try {
			sock.write(protocol.buildAuthLine(targetToken));
			// A frame body with no trailing newline, then destroy: the
			// receiver's line queue must see the connection end, not wait forever.
			sock.write('{"msgV":1,"msg_id":"half-frame-never-completes');
			sock.destroy();
			await closed(sock, 5_000, "raw client close after half line + EOF");

			// Cleanup is observable: the accept loop's connection count drains.
			await untilTrue(
				() => target.connStats.closed >= 1,
				5_000,
				"accept loop exited after the half-line connection ended",
			);
			// The partial line never parsed into a frame, nothing was injected.
			expect(target.handledFrames).toHaveLength(0);
			expect(target.injected).toHaveLength(0);
		} finally {
			sock.destroy();
		}

		// The server survived: a normal send through the full stack works.
		const followUp = await bounded(
			sender.outbox.send("target", "still alive after the half-line fault", {}),
			10_000,
			"follow-up send after the half-line fault",
		);
		await untilTrue(() => target.injected.length === 1, 5_000, "follow-up message injected");
		await untilTrue(
			() => sender.outbox.receiptOf(followUp.msgId) === "delivered",
			5_000,
			"follow-up receipt observed",
		);
	});

	it("scenario 4: an over-cap line (PEER_MAX_LINE_BYTES) drops only that connection; the server keeps serving", async () => {
		const sock = await bounded(rawConnect(target.address.path), 5_000, "raw client connect (oversize fault)");
		try {
			sock.write(protocol.buildAuthLine(targetToken));
			// A completed line strictly over the per-line cap: framing must
			// destroy the connection instead of delivering it.
			sock.write(`x${"a".repeat(PEER_MAX_LINE_BYTES)}\n`);
			await closed(sock, 15_000, "connection closed after an over-cap line");

			await untilTrue(
				() => target.connStats.closed >= 1,
				5_000,
				"accept loop exited after the oversize-line drop",
			);
			expect(target.handledFrames).toHaveLength(0);
			expect(target.injected).toHaveLength(0);
		} finally {
			sock.destroy();
		}

		// Not poisoned: the next connection through the full stack works.
		const followUp = await bounded(
			sender.outbox.send("target", "still serving after the oversize-line fault", {}),
			10_000,
			"follow-up send after the oversize-line fault",
		);
		await untilTrue(() => target.injected.length === 1, 5_000, "follow-up message injected");
		await untilTrue(
			() => sender.outbox.receiptOf(followUp.msgId) === "delivered",
			5_000,
			"follow-up receipt observed",
		);
	});

	it("scenario 5: a replayed frame is dropped by dedupe and the 'duplicate' reason is observable on the public receipt surface", async () => {
		const body = "replay me exactly once";

		// First delivery: accepted, injected, receipted as delivered.
		const first = await bounded(sender.outbox.send("target", body, {}), 10_000, "first send");
		await untilTrue(() => target.injected.length === 1, 5_000, "first message injected");
		await untilTrue(() => sender.outbox.receiptOf(first.msgId) === "delivered", 5_000, "first receipt delivered");

		// Replay: identical content from the same sender session — the real
		// guard's dedup fingerprint matches, so the frame is refused.
		const second = await bounded(sender.outbox.send("target", body, {}), 10_000, "replayed send");

		// The drop surfaces on the public face: a "dropped" receipt carrying
		// the duplicate reason, delivered back over the real wire.
		await untilTrue(
			() => sender.outbox.receiptOf(second.msgId) === "dropped",
			5_000,
			"replayed frame receipted as dropped",
		);
		// ...and into the agent-visible notice (the receipt handler's public path).
		await untilTrue(
			() => sender.agentNotices.some((text) => text.includes("duplicate")),
			5_000,
			"agent notice carrying the duplicate drop reason",
		);
		// Dedupe did its job: exactly one injection.
		expect(target.injected).toHaveLength(1);
	});

	it("scenario 6: a connection that never sends the auth line is closed by the first-line deadline (injected), never a hang", async () => {
		// The shipped default is PEER_FIRST_LINE_TIMEOUT_MS (30s) — asserting
		// the constant pins the documented bound; the injected override drives
		// the same timer/destroy code path without a real 30s wait.
		expect(PEER_FIRST_LINE_TIMEOUT_MS).toBe(30_000);
		const firstLineTimeoutMs = 150;
		const address = testAddress("slow-first-line");
		let onConnectionRan = false;
		const server = await bounded(
			transport.listen(
				address,
				() => {
					onConnectionRan = true;
				},
				{ firstLineTimeoutMs },
			),
			5_000,
			"slow-first-line listener start",
		);
		listenerClosers.push(() => server.close());

		const sock = await bounded(rawConnect(address.path), 5_000, "silent client connect");
		try {
			// Say nothing. The deadline must destroy the connection.
			await closed(
				sock,
				Math.max(firstLineTimeoutMs * 8, 2_000),
				"silent client destroyed by the first-line deadline",
			);
			// No auth line ever arrived, so onConnection must never have run.
			expect(onConnectionRan).toBe(false);
		} finally {
			sock.destroy();
		}
	});
});
