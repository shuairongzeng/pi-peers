import { describe, expect, it, vi } from "vitest";
import { createPeerInbox } from "../src/inbox.ts";
import { createPeerOutbox } from "../src/outbox.ts";
import { createPeerProtocol } from "../src/protocol.ts";
import {
	PEER_APPROVAL_TIMEOUT_MS,
	PEER_MAX_QUEUED_MESSAGES,
	PEER_RECEIPT_TTL_MS,
	type PeerAuth,
	type PeerConnection,
	type PeerControlFrame,
	type PeerGuard,
	type PeerGuardCheck,
	type PeerHost,
	type PeerIdentity,
	type PeerIdleNoticeFrame,
	type PeerInbox,
	type PeerMessageStatusFrame,
	type PeerOutbox,
	type PeerPriority,
	type PeerProtocol,
	type PeerRecord,
	type PeerRegistry,
	type PeerShutdownResponseFrame,
	type PeerRegistryEntry,
	type PeerResolution,
	PeerSendError,
	type PeerSenderPacer,
	type PeerTransport,
	type PeerUserFrame,
} from "../src/types.ts";

// ---------------------------------------------------------------------------
// Test doubles: in-memory fakes for the whole PeerServiceDeps surface
// ---------------------------------------------------------------------------

interface ScriptedHost extends PeerHost {
	injected: Array<{ content: string; priority: PeerPriority }>;
	notices: string[];
	userNotifies: Array<{ title: string; message: string }>;
	confirmCalls: Array<{ title: string; message: string; timeoutMs: number }>;
	nextAnswer: "yes" | "no" | "timeout";
	injectError: Error | undefined;
	hasUIValue: boolean;
	policy: "ask" | "all" | "off";
	idleNowValue: boolean;
	quitCalls: number;
}

function makeHost(overrides: Partial<ScriptedHost> = {}): ScriptedHost {
	return {
		injected: [],
		notices: [],
		userNotifies: [],
		confirmCalls: [],
		nextAnswer: "yes",
		injectError: undefined,
		hasUIValue: true,
		policy: "ask",
		idleNowValue: false,
		quitCalls: 0,
		...overrides,
		async injectUserMessage(content, priority) {
			if (this.injectError) throw this.injectError;
			this.injected.push({ content, priority });
		},
		notify(title, message) {
			this.userNotifies.push({ title, message });
		},
		async confirm(title, message, timeoutMs) {
			this.confirmCalls.push({ title, message, timeoutMs });
			return this.nextAnswer;
		},
		async injectAgentNotice(text) {
			this.notices.push(text);
		},
		hasUI() {
			return this.hasUIValue;
		},
		acceptPolicy() {
			return this.policy;
		},
		isIdleNow() {
			return this.idleNowValue;
		},
		quit() {
			this.quitCalls++;
		},
	};
}

interface ScriptedPacer extends PeerSenderPacer {
	consumed: string[];
	refunded: string[];
	nextAllow: boolean;
}

function makePacer(overrides: Partial<ScriptedPacer> = {}): ScriptedPacer {
	return {
		consumed: [],
		refunded: [],
		nextAllow: true,
		...overrides,
		tryConsume(target) {
			this.consumed.push(target);
			return this.nextAllow;
		},
		refund(target) {
			this.refunded.push(target);
		},
	};
}

interface ScriptedGuard extends PeerGuard {
	checks: Array<{ fromKey: string; queued: number }>;
	accepted: string[];
	refunded: string[];
	nextCheck: PeerGuardCheck;
	/** checkChild() calls in order, for flood assertions. */
	childChecks: string[];
	nextChildCheck: PeerGuardCheck;
}

function makeGuard(overrides: Partial<ScriptedGuard> = {}): ScriptedGuard {
	return {
		checks: [],
		accepted: [],
		refunded: [],
		nextCheck: { ok: true },
		childChecks: [],
		nextChildCheck: { ok: true },
		...overrides,
		check(fromKey, _frame, _ownHopToken, queuedCount) {
			this.checks.push({ fromKey, queued: queuedCount });
			return this.nextCheck;
		},
		checkChild(fromKey) {
			this.childChecks.push(fromKey);
			return this.nextChildCheck;
		},
		recordAccepted(fromKey) {
			this.accepted.push(fromKey);
		},
		refund(fromKey) {
			this.refunded.push(fromKey);
		},
	};
}

function makeAuth(keys: Record<string, { pid: number; peerToken: string }> = {}): PeerAuth {
	return {
		generateTokens: () => ({ peerToken: "new-peer", childToken: "new-child" }),
		publishKey: () => {},
		lookupKey: (_peersDir: string, canonicalSocketPath: string) => keys[canonicalSocketPath],
		deleteKeys: () => {},
	};
}

interface FakeTransport extends PeerTransport {
	validAddresses: Set<string>;
	connects: string[];
	failConnect: boolean;
	failWrite: boolean;
	sessions: FakeConn[];
}

interface FakeConn extends PeerConnection {
	lines: string[];
	ended: boolean;
	destroyed: boolean;
}

function makeTransport(overrides: Partial<FakeTransport> = {}): FakeTransport {
	const t: FakeTransport = {
		validAddresses: new Set<string>(),
		connects: [],
		failConnect: false,
		failWrite: false,
		sessions: [],
		deriveSocketAddress: () => ({ path: "p", address: "a" }),
		isValidAddress: (address: string) => t.validAddresses.has(address),
		resolveAddress: (address: string) =>
			t.validAddresses.has(address) && address.startsWith("pipe:") ? `\\\\.\\pipe\\${address.slice(5)}` : undefined,
		addressFromPath: (path: string) =>
			path.startsWith("\\\\.\\pipe\\") ? `pipe:${path.slice("\\\\.\\pipe\\".length)}` : undefined,
		listen: async () => {
			throw new Error("listen not needed in this test");
		},
		async connect(address) {
			t.connects.push(address);
			if (t.failConnect) throw new PeerSendError("connect_failed", `cannot reach ${address}`);
			const conn: FakeConn = {
				lines: [],
				ended: false,
				destroyed: false,
				writeLine(line: string) {
					if (t.failWrite) throw new PeerSendError("timeout", "write timed out");
					this.lines.push(line);
				},
				async end() {
					if (t.failWrite) throw new PeerSendError("timeout", "end timed out");
					this.ended = true;
				},
				destroy() {
					this.destroyed = true;
				},
			};
			t.sessions.push(conn);
			return conn;
		},
		readLines: () => {
			throw new Error("readLines not needed in this test");
		},
		...overrides,
	};
	return t;
}

function makeRegistry(resolveImpl: (name: string) => PeerResolution, entries: PeerRegistryEntry[] = []): PeerRegistry {
	return {
		publish: () => {},
		update: () => {},
		unpublish: () => {},
		list: () => entries,
		readRecord: (pid: number) => entries.find((e) => e.record.pid === pid),
		resolve: resolveImpl,
		resolveIn: (name: string, scoped: PeerRegistryEntry[]) => resolveImpl(name),
		normalizeName: (name: string) => name.toLowerCase().replace(/\s+/g, "-"),
		refOf: (rec: PeerRecord) => `${(rec.pid % 256).toString(16).padStart(2, "0")}a1b2c`,
		isAlive: () => true,
		readProcStart: () => undefined,
		pidDomain: () => "win32:box",
	};
}

function entry(rec: Partial<PeerRecord> = {}, live = true): PeerRegistryEntry {
	return {
		record: {
			version: 1,
			pid: 4242,
			sessionId: "abcdef1234567890",
			cwd: "D:/work/alpha",
			name: "alpha",
			nameSource: "user",
			formerNames: [],
			kind: "interactive",
			status: "busy",
			statusUpdatedAt: 0,
			startedAt: 0,
			pidDomain: "win32:box",
			socketPath: `\\\\.\\pipe\\LOCAL\\pi-peer-${"a".repeat(32)}`,
			peerProtocol: 1,
			features: ["notify_idle"],
			...rec,
		},
		live,
	};
}

function makeIdentity(overrides: Partial<PeerIdentity> = {}): PeerIdentity {
	return {
		pid: 100,
		sessionId: "self-session",
		socketAddress: `pipe:LOCAL\\pi-peer-${"f".repeat(32)}`,
		name: "me",
		hopToken: "hop-self",
		tokens: { peerToken: "t1", childToken: "t2" },
		...overrides,
	};
}

/** Real protocol: wrapping/parsing/sanitizing must behave exactly as shipped. */
const proto: PeerProtocol = createPeerProtocol();

/**
 * Inbound user frames carry wire-wrapped content by contract. `body` is the
 * message text to wrap; `rawContent` bypasses wrapping for tests that inject
 * deliberately malformed content. The wrapper always mirrors the frame's wire
 * identity, exactly as outbox.send does.
 */
function userFrame(
	overrides: Partial<Omit<PeerUserFrame, "content">> & {
		body?: string;
		rawContent?: string;
	} = {},
): PeerUserFrame {
	const { body = "hello from alpha", rawContent, ...rest } = overrides;
	const frame: PeerUserFrame = {
		msgV: 1,
		msg_id: "incoming-1",
		type: "user",
		from: `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`,
		fromPid: 4242,
		fromSession: "peer-session",
		fromName: "alpha",
		hopChain: ["hop-peer"],
		priority: "next",
		content: "",
		...rest,
	};
	frame.content = rawContent ?? proto.wrapPeerContent(
		{
			from: frame.from,
			fromSession: frame.fromSession,
			fromName: frame.fromName,
		},
		body,
	);
	return frame;
}

function statusFrame(overrides: Partial<PeerMessageStatusFrame> = {}): PeerMessageStatusFrame {
	return {
		msgV: 1,
		msg_id: "m-42",
		type: "control",
		action: "peer_message_status",
		from: `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`,
		fromName: "alpha",
		state: "delivered",
		...overrides,
	};
}

/** Wire the inbox with a scripted outbox that records sendControl traffic. */
interface ScriptedOutbox extends PeerOutbox {
	controlCalls: Array<{ address: string; frame: PeerControlFrame }>;
	receiptsHandled: PeerMessageStatusFrame[];
	failControl: boolean;
	/** Idle nonces registered by a test; consumption is one-shot like the real outbox. */
	idleNonces: Set<string>;
}

function makeScriptedOutbox(): ScriptedOutbox {
	return {
		controlCalls: [],
		receiptsHandled: [],
		failControl: false,
		idleNonces: new Set<string>(),
		async send(_target, _body, _opts) {
			return { msgId: "m-1", to: "x", toName: "n" };
		},
		async sendControl(address, frame) {
			if (this.failControl) throw new Error("control unavailable");
			this.controlCalls.push({ address, frame });
		},
		receiptOf: () => undefined,
		handleReceipt(frame) {
			this.receiptsHandled.push(frame);
		},
		registerOutbound() {},
		expectsReceipt: () => false,
		registerIdleNonce(nonce) {
			this.idleNonces.add(nonce);
		},
		consumeIdleNonce(nonce) {
			return nonce !== undefined && this.idleNonces.delete(nonce);
		},
	};
}

// ---------------------------------------------------------------------------
// Outbox tests
// ---------------------------------------------------------------------------

describe("createPeerOutbox", () => {
	const alphaEntry = entry();
	const keyForAlpha = { pid: 4242, peerToken: "key-alpha" };
	/** Placeholder for the self-referential outbox slot (the outbox never reads it). */
	const outboxSlot = undefined as unknown as PeerOutbox;

	it("sends a well-formed frame: auth line then user frame with all fields", async () => {
		const transport = makeTransport();
		const pacer = makePacer();
		const deps = {
			identity: makeIdentity(),
			protocol: proto,
			transport,
			auth: makeAuth({ [alphaEntry.record.socketPath]: keyForAlpha }),
			registry: makeRegistry(() => ({ kind: "unique", entry: alphaEntry })),
			guard: makeGuard(),
			pacer,
			host: makeHost(),
			peersDir: "/fake/peers",
			outbox: outboxSlot,
		};
		const outbox = createPeerOutbox(deps);

	const outcome = await outbox.send("alpha", "ping body", { priority: "now" });

	expect(outcome).toEqual({
		msgId: expect.any(String),
		to: alphaEntry.record.socketPath,
		toName: "alpha",
	});
	expect(pacer.consumed).toEqual(["alpha"]);
	expect(pacer.refunded).toEqual([]);
	expect(transport.connects).toEqual([`pipe:LOCAL\\pi-peer-${"a".repeat(32)}`]);
	expect(transport.sessions.length).toBe(1);
	const conn = transport.sessions[0]!;
	expect(conn.ended).toBe(true);
	expect(conn.lines.length).toBe(2);
	// Line 1: auth carrying the target's key token.
	expect(JSON.parse(conn.lines[0]!)).toEqual({ type: "auth", token: "key-alpha" });
	// Line 2: the user frame, field by field.
	const frame = JSON.parse(conn.lines[1]!) as PeerUserFrame;
	expect(frame.msgV).toBe(1);
	expect(frame.msg_id).toBe(outcome.msgId);
	expect(frame.type).toBe("user");
	expect(frame.from).toBe(deps.identity.socketAddress);
	expect(frame.fromPid).toBe(deps.identity.pid);
	expect(frame.fromSession).toBe(deps.identity.sessionId);
	expect(frame.fromName).toBe("me");
	expect(frame.hopChain).toEqual([deps.identity.hopToken]);
	expect(frame.priority).toBe("now");
	expect(frame.content).toBe(
		proto.wrapPeerContent(
			{
				from: deps.identity.socketAddress,
				fromSession: deps.identity.sessionId,
				fromName: "me",
			},
			"ping body",
		),
	);
	// The sent msg_id is registered: receipts for it will be honored.
	expect(outbox.expectsReceipt(outcome.msgId)).toBe(true);
	});

	it("defaults priority to next", async () => {
		const transport = makeTransport();
		const deps = {
			identity: makeIdentity(),
			protocol: proto,
			transport,
			auth: makeAuth({ [alphaEntry.record.socketPath]: keyForAlpha }),
			registry: makeRegistry(() => ({ kind: "unique", entry: alphaEntry })),
			guard: makeGuard(),
			pacer: makePacer(),
			host: makeHost(),
			peersDir: "/fake/peers",
			outbox: outboxSlot,
		};
		const outbox = createPeerOutbox(deps);
		await outbox.send("alpha", "body", {});
		const frame = JSON.parse(transport.sessions[0]!.lines[1]!) as PeerUserFrame;
		expect(frame.priority).toBe("next");
	});

	it("appends a notify_when_idle frame when notifyWhenIdle is set", async () => {
		const transport = makeTransport();
		const deps = {
			identity: makeIdentity(),
			protocol: proto,
			transport,
			auth: makeAuth({ [alphaEntry.record.socketPath]: keyForAlpha }),
			registry: makeRegistry(() => ({ kind: "unique", entry: alphaEntry })),
			guard: makeGuard(),
			pacer: makePacer(),
			host: makeHost(),
			peersDir: "/fake/peers",
			outbox: outboxSlot,
		};
		const outbox = createPeerOutbox(deps);
		await outbox.send("alpha", "body", { notifyWhenIdle: true });

		const conn = transport.sessions[0]!;
		expect(conn.lines.length).toBe(3);
		// The subscription precedes the user frame so it covers the work the
		// message triggers (the peer processes lines in order).
		const notify = JSON.parse(conn.lines[1]!) as PeerControlFrame & { reply_to: string; nonce?: string };
		expect(notify.type).toBe("control");
		expect(notify.action).toBe("notify_when_idle");
		expect(notify.msgV).toBe(1);
		expect(typeof notify.msg_id).toBe("string");
		expect(notify.msg_id.length).toBeGreaterThan(0);
		expect(notify.reply_to).toBe(deps.identity.socketAddress);
		expect(notify.from).toBe(deps.identity.socketAddress);
		expect(notify.fromName).toBe("me");
		expect(typeof notify.nonce).toBe("string");
		expect(JSON.parse(conn.lines[2]!).type).toBe("user");
	});

	it("throws not_found without a peer list and hints at stale addresses", async () => {
		const beta = entry({ pid: 55, name: "beta", socketPath: `\\\\.\\pipe\\LOCAL\\pi-peer-${"c".repeat(32)}` });
		const dead = entry(
			{ pid: 66, name: "dead-peer", socketPath: `\\\\.\\pipe\\LOCAL\\pi-peer-${"d".repeat(32)}` },
			false,
		);
		const deps = {
			identity: makeIdentity(),
			protocol: proto,
			transport: makeTransport(),
			auth: makeAuth(),
			registry: makeRegistry(() => ({ kind: "none" }), [beta, dead, alphaEntry]),
			guard: makeGuard(),
			pacer: makePacer(),
			host: makeHost(),
			peersDir: "/fake/peers",
			outbox: outboxSlot,
		};
		const outbox = createPeerOutbox(deps);

		await expect(outbox.send("ghost", "hi", {})).rejects.toMatchObject({
			name: "PeerSendError",
			code: "not_found",
		});
		let message = "";
		try {
			await outbox.send("ghost", "hi", {});
		} catch (error) {
			message = error instanceof Error ? error.message : String(error);
		}
		// The outbox has no project-scope notion, so the error carries no
		// machine-wide peer list (tool.ts renders the scoped candidates).
		expect(message).toContain("no live peer matches");
		expect(message).not.toContain("beta");
		expect(message).not.toContain("dead-peer");
		expect(message).toContain("never reuse a stale address");
	});
	it("throws ambiguous with candidate lines name [ref] status cwd", async () => {
		const a1 = entry({ pid: 11, cwd: "D:/one" });
		const a2 = entry({
			pid: 22,
			name: "alpha",
			status: "idle",
			cwd: "D:/two",
			socketPath: `\\\\.\\pipe\\LOCAL\\pi-peer-${"e".repeat(32)}`,
		});
		const deps = {
			identity: makeIdentity(),
			protocol: proto,
			transport: makeTransport(),
			auth: makeAuth(),
			registry: makeRegistry(() => ({ kind: "ambiguous", candidates: [a1, a2] }), [a1, a2]),
			guard: makeGuard(),
			pacer: makePacer(),
			host: makeHost(),
			peersDir: "/fake/peers",
			outbox: outboxSlot,
		};
		const outbox = createPeerOutbox(deps);

		const error = await outbox.send("alpha", "hi", {}).then(
			() => undefined,
			(e) => e,
		);
		expect(error).toBeInstanceOf(PeerSendError);
		expect((error as PeerSendError).code).toBe("ambiguous");
		expect((error as PeerSendError).candidates).toEqual([
			`alpha [0ba1b2c] busy D:/one`,
			`alpha [16a1b2c] idle D:/two`,
		]);
	});

	it("throws offline when the unique record is not live", async () => {
		const dead = entry({}, false);
		const deps = {
			identity: makeIdentity(),
			protocol: proto,
			transport: makeTransport(),
			auth: makeAuth(),
			registry: makeRegistry(() => ({ kind: "unique", entry: dead })),
			guard: makeGuard(),
			pacer: makePacer(),
			host: makeHost(),
			peersDir: "/fake/peers",
			outbox: outboxSlot,
		};
		const outbox = createPeerOutbox(deps);
		await expect(outbox.send("alpha", "hi", {})).rejects.toMatchObject({ code: "offline" });
	});

	it("throws no_key when the target has no published key", async () => {
		const deps = {
			identity: makeIdentity(),
			protocol: proto,
			transport: makeTransport(),
			auth: makeAuth(),
			registry: makeRegistry(() => ({ kind: "unique", entry: alphaEntry })),
			guard: makeGuard(),
			pacer: makePacer(),
			host: makeHost(),
			peersDir: "/fake/peers",
			outbox: outboxSlot,
		};
		const outbox = createPeerOutbox(deps);
		const error = await outbox.send("alpha", "hi", {}).then(
			() => undefined,
			(e) => e,
		);
		expect(error).toBeInstanceOf(PeerSendError);
		expect((error as PeerSendError).code).toBe("no_key");
		expect((error as PeerSendError).message).toContain("has not published a key");
	});

	it("throws sender_paced before doing any work when tryConsume fails", async () => {
		const transport = makeTransport();
		const pacer = makePacer({ nextAllow: false });
		const deps = {
			identity: makeIdentity(),
			protocol: proto,
			transport,
			auth: makeAuth(),
			registry: makeRegistry(() => ({ kind: "unique", entry: alphaEntry })),
			guard: makeGuard(),
			pacer,
			host: makeHost(),
			peersDir: "/fake/peers",
			outbox: outboxSlot,
		};
		const outbox = createPeerOutbox(deps);

		const error = await outbox.send("alpha", "hi", {}).then(
			() => undefined,
			(e) => e,
		);
		expect(error).toBeInstanceOf(PeerSendError);
		expect((error as PeerSendError).code).toBe("sender_paced");
		expect((error as PeerSendError).message).toContain("merge");
		expect(transport.connects).toEqual([]);
		expect(pacer.refunded).toEqual([]);
	});

	it("refunds the pacer when connect fails and wraps non-PeerSendError as connect_failed", async () => {
		const transport = makeTransport({ failConnect: true });
		const pacer = makePacer();
		const deps = {
			identity: makeIdentity(),
			protocol: proto,
			transport,
			auth: makeAuth({ [alphaEntry.record.socketPath]: keyForAlpha }),
			registry: makeRegistry(() => ({ kind: "unique", entry: alphaEntry })),
			guard: makeGuard(),
			pacer,
			host: makeHost(),
			peersDir: "/fake/peers",
			outbox: outboxSlot,
		};
		const outbox = createPeerOutbox(deps);

		await expect(outbox.send("alpha", "hi", {})).rejects.toMatchObject({ code: "connect_failed" });
		expect(pacer.refunded).toEqual(["alpha"]);
	});

	it("refunds the pacer when a write fails and rethrows PeerSendError as-is", async () => {
		const transport = makeTransport({ failWrite: true });
		const pacer = makePacer();
		const deps = {
			identity: makeIdentity(),
			protocol: proto,
			transport,
			auth: makeAuth({ [alphaEntry.record.socketPath]: keyForAlpha }),
			registry: makeRegistry(() => ({ kind: "unique", entry: alphaEntry })),
			guard: makeGuard(),
			pacer,
			host: makeHost(),
			peersDir: "/fake/peers",
			outbox: outboxSlot,
		};
		const outbox = createPeerOutbox(deps);

		await expect(outbox.send("alpha", "hi", {})).rejects.toMatchObject({ code: "timeout" });
		expect(pacer.refunded).toEqual(["alpha"]);
	});

	it("sendControl rejects invalid addresses with invalid_target", async () => {
		const transport = makeTransport();
		const deps = {
			identity: makeIdentity(),
			protocol: proto,
			transport,
			auth: makeAuth(),
			registry: makeRegistry(() => ({ kind: "none" })),
			guard: makeGuard(),
			pacer: makePacer(),
			host: makeHost(),
			peersDir: "/fake/peers",
			outbox: outboxSlot,
		};
		const outbox = createPeerOutbox(deps);

		const frame = statusFrame();
		await expect(outbox.sendControl("pipe:LOCAL\\not-a-peer-name", frame)).rejects.toMatchObject({
			code: "invalid_target",
		});
		expect(transport.connects).toEqual([]);
	});

	it("sendControl delivers auth line and frame, then swallows transport failures", async () => {
		const transport = makeTransport();
		const deps = {
			identity: makeIdentity(),
			protocol: proto,
			transport,
			auth: makeAuth(),
			registry: makeRegistry(() => ({ kind: "none" })),
			guard: makeGuard(),
			pacer: makePacer(),
			host: makeHost(),
			peersDir: "/fake/peers",
			outbox: outboxSlot,
		};
		const address = `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`;
		transport.validAddresses.add(address);
		// Auth uses the target socket's published key, never our own token.
		const targetPath = `\\\\.\\pipe\\LOCAL\\pi-peer-${"b".repeat(32)}`;
		deps.auth = makeAuth({ [targetPath]: { pid: 99, peerToken: "peer-b-token" } });
		const outbox = createPeerOutbox(deps);
		const frame = statusFrame();
		await outbox.sendControl(address, frame);

		expect(transport.connects).toEqual([address]);
		const conn = transport.sessions[0]!;
		expect(JSON.parse(conn.lines[0]!)).toEqual({ type: "auth", token: "peer-b-token" });
		expect(JSON.parse(conn.lines[1]!)).toEqual(frame);

		// Delivery failure is logged, never thrown.
		transport.failConnect = true;
		await expect(outbox.sendControl(address, frame)).resolves.toBeUndefined();
	});

	it("handleReceipt stores state and notifies for held/delivered", () => {
		const host = makeHost();
		const deps = {
			identity: makeIdentity(),
			protocol: proto,
			transport: makeTransport(),
			auth: makeAuth(),
			registry: makeRegistry(() => ({ kind: "none" })),
			guard: makeGuard(),
			pacer: makePacer(),
			host,
			peersDir: "/fake/peers",
			outbox: outboxSlot,
		};
		const outbox = createPeerOutbox(deps);

	// Only receipts for msg_ids we actually sent are honored.
	outbox.registerOutbound("m-1");
	outbox.handleReceipt(statusFrame({ msg_id: "m-1", state: "held" }));
	expect(outbox.receiptOf("m-1")).toBe("held");
	outbox.handleReceipt(statusFrame({ msg_id: "m-1", state: "delivered" }));
	expect(outbox.receiptOf("m-1")).toBe("delivered");
	expect(host.notices).toEqual([]);
	expect(host.userNotifies.length).toBe(2);
	expect(host.userNotifies[0]!.title).toBe("Peer message");

	// Unregistered (spoofed) receipts produce no state at all.
	outbox.handleReceipt(statusFrame({ msg_id: "m-spoof", state: "held" }));
	expect(outbox.receiptOf("m-spoof")).toBeUndefined();
	expect(outbox.receiptOf("unknown")).toBeUndefined();
	});

	it("handleReceipt injects an agent notice for failed states with reason", () => {
		const host = makeHost();
		const deps = {
			identity: makeIdentity(),
			protocol: proto,
			transport: makeTransport(),
			auth: makeAuth(),
			registry: makeRegistry(() => ({ kind: "none" })),
			guard: makeGuard(),
			pacer: makePacer(),
			host,
			peersDir: "/fake/peers",
			outbox: outboxSlot,
		};
		const outbox = createPeerOutbox(deps);

		outbox.registerOutbound("m-9");
		outbox.registerOutbound("m-10");
		outbox.registerOutbound("m-11");
		outbox.registerOutbound("m-12");
		outbox.handleReceipt(statusFrame({ msg_id: "m-9", state: "denied", reason: "rejected by user" }));
		expect(outbox.receiptOf("m-9")).toBe("denied");
		outbox.handleReceipt(statusFrame({ msg_id: "m-10", state: "dropped", reason: "rate-limited" }));
		outbox.handleReceipt(statusFrame({ msg_id: "m-11", state: "expired" }));
		outbox.handleReceipt(statusFrame({ msg_id: "m-12", state: "refused" }));

		// Negative receipts reach BOTH the agent (notices) and the human (userNotifies).
		expect(host.userNotifies).toEqual([
			{ title: "Peer message failed", message: "Peer message to alpha (m-9) was denied: rejected by user." },
			{ title: "Peer message failed", message: "Peer message to alpha (m-10) was dropped: rate-limited." },
			{ title: "Peer message failed", message: "Peer message to alpha (m-11) was expired." },
			{ title: "Peer message failed", message: "Peer message to alpha (m-12) was refused." },
		]);
		expect(host.notices).toEqual([
			"Peer message to alpha (m-9) was denied: rejected by user.",
			"Peer message to alpha (m-10) was dropped: rate-limited.",
			"Peer message to alpha (m-11) was expired.",
			"Peer message to alpha (m-12) was refused.",
		]);
	});

	it("handleReceipt falls back to the raw address when fromName is missing", () => {
		const host = makeHost();
		const deps = {
			identity: makeIdentity(),
			protocol: proto,
			transport: makeTransport(),
			auth: makeAuth(),
			registry: makeRegistry(() => ({ kind: "none" })),
			guard: makeGuard(),
			pacer: makePacer(),
			host,
			peersDir: "/fake/peers",
			outbox: outboxSlot,
		};
		const outbox = createPeerOutbox(deps);
		outbox.registerOutbound("m-13");
		const from = `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`;
		outbox.handleReceipt(statusFrame({ msg_id: "m-13", state: "denied", fromName: undefined, from }));
		expect(host.notices).toEqual([`Peer message to ${from} (m-13) was denied.`]);
	});

	it("expires receipt state after PEER_RECEIPT_TTL_MS", async () => {
		const host = makeHost();
		const deps = {
			identity: makeIdentity(),
			protocol: proto,
			transport: makeTransport(),
			auth: makeAuth(),
			registry: makeRegistry(() => ({ kind: "none" })),
			guard: makeGuard(),
			pacer: makePacer(),
			host,
			peersDir: "/fake/peers",
			outbox: outboxSlot,
		};
		const outbox = createPeerOutbox(deps);
		outbox.registerOutbound("m-old");
		outbox.handleReceipt(statusFrame({ msg_id: "m-old", state: "held" }));
		expect(outbox.receiptOf("m-old")).toBe("held");

		const now = Date.now;
		const advance = async (ms: number) => {
			(Date.now as unknown as () => number) = () => now() + ms;
		};
		await advance(PEER_RECEIPT_TTL_MS + 1);
		expect(outbox.receiptOf("m-old")).toBeUndefined();
		(Date.now as unknown as () => number) = now;
	});
});

// ---------------------------------------------------------------------------
// Inbox tests
// ---------------------------------------------------------------------------

describe("createPeerInbox", () => {
	function makeInbox(opts: {
		host?: Partial<ScriptedHost>;
		guard?: Partial<ScriptedGuard>;
		outbox?: ScriptedOutbox;
		transport?: FakeTransport;
		/** Registry entries visible to registry.list() (receiver-side scoping). */
		entries?: PeerRegistryEntry[];
	}): { inbox: PeerInbox; host: ScriptedHost; guard: ScriptedGuard; outbox: ScriptedOutbox } {
		const host = makeHost(opts.host);
		const guard = makeGuard(opts.guard);
		const outbox = opts.outbox ?? makeScriptedOutbox();
		const deps = {
			identity: makeIdentity(),
			protocol: proto,
			transport: opts.transport ?? makeTransport(),
			auth: makeAuth(),
			registry: makeRegistry(() => ({ kind: "none" }), opts.entries ?? []),
			guard,
			pacer: makePacer(),
			host,
			outbox,
		};
		return { inbox: createPeerInbox(deps), host, guard, outbox };
	}

	it("child frames bypass guard and policy and inject directly with delivered receipt", async () => {
		const { inbox, host, guard, outbox } = makeInbox({ host: { policy: "off" } });
		const frame = userFrame();

		await inbox.handleFrame(frame, "child");

		expect(host.injected).toEqual([{ content: frame.content, priority: frame.priority }]);
		expect(guard.checks).toEqual([]);
		expect(guard.accepted).toEqual([]);
		expect(host.confirmCalls).toEqual([]);
		expect(outbox.controlCalls.length).toBe(1);
		const { address } = outbox.controlCalls[0]!;
		const receiptFrame = outbox.controlCalls[0]!.frame as PeerMessageStatusFrame;
		expect(address).toBe(frame.from);
		expect(receiptFrame.action).toBe("peer_message_status");
		expect(receiptFrame.msg_id).toBe(frame.msg_id);
		expect(receiptFrame.state).toBe("delivered");
		expect(receiptFrame.reason).toBeUndefined();
		expect(receiptFrame.from).toBe(`pipe:LOCAL\\pi-peer-${"f".repeat(32)}`);
		expect(receiptFrame.fromName).toBe("me");
		expect(inbox.pendingCount()).toBe(1);
	});

	it("guard rejection drops the message with the guard's reason", async () => {
		const { inbox, host, guard, outbox } = makeInbox({
			guard: { nextCheck: { ok: false, reason: "rate-limited" } },
		});
		const frame = userFrame();

		await inbox.handleFrame(frame, "peer");

		expect(host.injected).toEqual([]);
		expect(guard.checks).toEqual([{ fromKey: frame.from, queued: 0 }]);
		const status = outbox.controlCalls[0]!.frame as PeerMessageStatusFrame;
		expect(status.state).toBe("dropped");
		expect(status.reason).toBe("rate-limited");
		expect(inbox.pendingCount()).toBe(0);
	});

	it("accept policy off refuses the message", async () => {
		const { inbox, host, outbox } = makeInbox({ host: { policy: "off" } });
		await inbox.handleFrame(userFrame(), "peer");
		expect(host.injected).toEqual([]);
		const status = outbox.controlCalls[0]!.frame as PeerMessageStatusFrame;
		expect(status.state).toBe("refused");
	});

	it("accept policy all injects without asking", async () => {
		const { inbox, host, guard, outbox } = makeInbox({ host: { policy: "all" } });
		const frame = userFrame();
		await inbox.handleFrame(frame, "peer");
		expect(host.confirmCalls).toEqual([]);
		expect(host.injected).toEqual([{ content: frame.content, priority: frame.priority }]);
		expect(guard.accepted).toEqual([frame.from]);
		const status = outbox.controlCalls[0]!.frame as PeerMessageStatusFrame;
		expect(status.state).toBe("delivered");
	});

	it("unwrapped content is dropped with a malformed-wrapper receipt", async () => {
		const { inbox, host, outbox } = makeInbox({ host: { policy: "all" } });
		const frame = userFrame({ rawContent: "hello from alpha" });

		await inbox.handleFrame(frame, "peer");

		expect(host.injected).toEqual([]);
		const status = outbox.controlCalls[0]!.frame as PeerMessageStatusFrame;
		expect(status.state).toBe("dropped");
		expect(status.reason).toBe("malformed-wrapper");
		expect(inbox.pendingCount()).toBe(0);
	});

	it("a tampered wrapper is dropped with a malformed-wrapper receipt", async () => {
		const { inbox, host, outbox } = makeInbox({ host: { policy: "all" } });
		// Round-trip fidelity: reordered attributes never parse.
		const frame = userFrame({
			rawContent: `<cross-session-message from-session="peer-session" from="pipe:LOCAL\\pi-peer-${"b".repeat(32)}">\nhello from alpha\n</cross-session-message>`,
		});

		await inbox.handleFrame(frame, "peer");

		expect(host.injected).toEqual([]);
		const status = outbox.controlCalls[0]!.frame as PeerMessageStatusFrame;
		expect(status.state).toBe("dropped");
		expect(status.reason).toBe("malformed-wrapper");
	});

	it("injected content is re-wrapped with the frame's wire identity", async () => {
		const { inbox, host } = makeInbox({ host: { policy: "all" } });
		const frame = userFrame({ body: "the payload" });

		await inbox.handleFrame(frame, "peer");

		// The agent sees the parsed body re-wrapped under OUR protocol, with
		// the sender's wire identity — not the sender's wrapper bytes.
		expect(host.injected).toEqual([
			{
				content: proto.wrapPeerContent(
					{ from: frame.from, fromSession: frame.fromSession, fromName: "alpha" },
					"the payload",
				),
				priority: frame.priority,
			},
		]);
	});

	it("injected wrapper sanitizes a hostile fromName", async () => {
		const { inbox, host } = makeInbox({ host: { policy: "all" } });
		// The raw fromName carries tag/quote characters; the injected wrapper's
		// attribute value must be stripped of them.
		const frame = userFrame({
			fromName: 'alpha"><script>alert(1)</script>',
			body: "the payload",
		});

		await inbox.handleFrame(frame, "peer");

		const injected = host.injected[0]!.content;
		// sanitizePeerText strips <>" outright, so no tag forging or quote
		// escape remains in the attribute value.
		const expected = "alphascriptalert(1)/script";
		expect(injected).toBe(
			proto.wrapPeerContent(
				{ from: frame.from, fromSession: frame.fromSession, fromName: expected },
				"the payload",
			),
		);
		expect(injected).toContain(`from-name="${expected}"`);
		expect(injected).not.toContain("<script");
	});

	it("guard queue-full drops with a truthful reason naming the cap", async () => {
		const { inbox, host, guard, outbox } = makeInbox({
			guard: { nextCheck: { ok: false, reason: "queue-full" } },
		});
		const frame = userFrame();

		await inbox.handleFrame(frame, "peer");

		expect(host.injected).toEqual([]);
		const status = outbox.controlCalls[0]!.frame as PeerMessageStatusFrame;
		expect(status.state).toBe("dropped");
		expect(status.reason).toBe(
			`queue-full: peer has accepted ${PEER_MAX_QUEUED_MESSAGES} messages since its last idle; retry after it goes idle`,
		);
	});

	it("ask: held receipt first, then delivered after user says yes", async () => {
		const { inbox, host, outbox } = makeInbox({ host: { policy: "ask", nextAnswer: "yes" } });
		const frame = userFrame({ fromName: "alpha", body: "please review" });

		await inbox.handleFrame(frame, "peer");

		expect(host.confirmCalls).toEqual([
			{ title: "Peer message from alpha", message: "please review", timeoutMs: PEER_APPROVAL_TIMEOUT_MS },
		]);
		const states = outbox.controlCalls.map((c) => (c.frame as PeerMessageStatusFrame).state);
		expect(states).toEqual(["held", "delivered"]);
		expect(host.injected.length).toBe(1);
		expect(inbox.pendingCount()).toBe(1);
	});

	it("ask: truncates the preview to 500 chars", async () => {
		const { inbox, host } = makeInbox({ host: { policy: "ask", nextAnswer: "yes" } });
		const long = "x".repeat(600);
		await inbox.handleFrame(userFrame({ body: long }), "peer");
		const message = host.confirmCalls[0]!.message;
		expect(message.length).toBe(501);
		expect(message.startsWith("x".repeat(500))).toBe(true);
	});

	it("ask: user says no → denied", async () => {
		const { inbox, host, outbox } = makeInbox({ host: { policy: "ask", nextAnswer: "no" } });
		await inbox.handleFrame(userFrame(), "peer");
		expect(host.injected).toEqual([]);
		const status = outbox.controlCalls[outbox.controlCalls.length - 1]!.frame as PeerMessageStatusFrame;
		expect(status.state).toBe("denied");
	});

	it("ask: timeout → expired", async () => {
		const { inbox, host, outbox } = makeInbox({ host: { policy: "ask", nextAnswer: "timeout" } });
		await inbox.handleFrame(userFrame(), "peer");
		expect(host.injected).toEqual([]);
		const status = outbox.controlCalls[outbox.controlCalls.length - 1]!.frame as PeerMessageStatusFrame;
		expect(status.state).toBe("expired");
	});

	it("ask without UI → refused, no confirm prompt", async () => {
		const { inbox, host, outbox } = makeInbox({
			host: { policy: "ask", hasUIValue: false, nextAnswer: "yes" },
		});
		await inbox.handleFrame(userFrame(), "peer");
		expect(host.confirmCalls).toEqual([]);
		expect(host.injected).toEqual([]);
		const status = outbox.controlCalls[0]!.frame as PeerMessageStatusFrame;
		expect(status.state).toBe("refused");
	});

	it("inject failure decrements the queue, refunds the guard, and sends dropped", async () => {
		const { inbox, host, guard, outbox } = makeInbox({
			host: { policy: "all", injectError: new Error("boom") },
		});
		const frame = userFrame();
		await inbox.handleFrame(frame, "peer");

		expect(host.injected).toEqual([]);
		expect(guard.refunded).toEqual([frame.from]);
		expect(guard.accepted).toEqual([frame.from]);
		expect(inbox.pendingCount()).toBe(0);
		const status = outbox.controlCalls[0]!.frame as PeerMessageStatusFrame;
		expect(status.state).toBe("dropped");
		expect(status.reason).toContain("inject-failed");
	});

	it("acceptedSinceIdle grows and is passed to the guard", async () => {
		const { inbox, guard } = makeInbox({ host: { policy: "all" } });
		await inbox.handleFrame(userFrame({ msg_id: "i1" }), "peer");
		await inbox.handleFrame(userFrame({ msg_id: "i2", body: "second" }), "peer");
		expect(inbox.pendingCount()).toBe(2);
		expect(guard.checks.map((c) => c.queued)).toEqual([0, 1]);
	});

	it("peer_message_status control frames reach the outbox", async () => {
		const { inbox, outbox } = makeInbox({});
		const frame = statusFrame({ msg_id: "m-7", state: "dropped", reason: "queue-full" });
		await inbox.handleFrame(frame, "peer");
		expect(outbox.receiptsHandled).toEqual([frame]);
		expect(outbox.controlCalls).toEqual([]);
	});

	it("notify_when_idle stores a valid subscription; onIdle fires it once", async () => {
		const replyTo = `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`;
		const transport = makeTransport();
		transport.validAddresses.add(replyTo);
		const { inbox, outbox } = makeInbox({ transport });
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "sub-1",
			type: "control",
			action: "notify_when_idle",
			reply_to: replyTo,
			from: replyTo,
			fromName: "alpha",
			nonce: "n-1",
		};
		await inbox.handleFrame(frame, "peer");
		expect(outbox.controlCalls).toEqual([]);

		inbox.onIdle();
		expect(outbox.controlCalls.length).toBe(1);
		const { address, frame: notice } = outbox.controlCalls[0]!;
		const idleNotice = notice as PeerIdleNoticeFrame;
		expect(address).toBe(replyTo);
		expect(idleNotice.action).toBe("peer_idle_notice");
		expect(idleNotice.from).toBe(`pipe:LOCAL\\pi-peer-${"f".repeat(32)}`);
		expect(idleNotice.fromName).toBe("me");
		expect(idleNotice.nonce).toBe("n-1");

		// One-shot: a second idle transition sends nothing.
		inbox.onIdle();
		expect(outbox.controlCalls.length).toBe(1);
	});

	it("notify_when_idle while already idle replies immediately and parks no subscription", async () => {
		const replyTo = `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`;
		const transport = makeTransport();
		transport.validAddresses.add(replyTo);
		const { inbox, outbox } = makeInbox({ transport, host: { idleNowValue: true } });
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "sub-immediate",
			type: "control",
			action: "notify_when_idle",
			reply_to: replyTo,
			from: replyTo,
			fromName: "alpha",
			nonce: "n-imm",
		};

		await inbox.handleFrame(frame, "peer");

		expect(outbox.controlCalls.length).toBe(1);
		const { address, frame: notice } = outbox.controlCalls[0]!;
		const idleNotice = notice as PeerIdleNoticeFrame;
		expect(address).toBe(replyTo);
		expect(idleNotice.action).toBe("peer_idle_notice");
		expect(idleNotice.from).toBe(`pipe:LOCAL\\pi-peer-${"f".repeat(32)}`);
		expect(idleNotice.fromName).toBe("me");
		expect(idleNotice.nonce).toBe("n-imm");
		// No residue: a later busy→idle transition must not send a second notice.
		inbox.onIdle();
		expect(outbox.controlCalls.length).toBe(1);
	});

	it("notify_when_idle while busy defers the notice to the next onIdle", async () => {
		const replyTo = `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`;
		const transport = makeTransport();
		transport.validAddresses.add(replyTo);
		const { inbox, outbox } = makeInbox({ transport, host: { idleNowValue: false } });
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "sub-busy",
			type: "control",
			action: "notify_when_idle",
			reply_to: replyTo,
			from: replyTo,
			fromName: "beta",
			nonce: "n-busy",
		};

		await inbox.handleFrame(frame, "peer");
		expect(outbox.controlCalls).toEqual([]);

		inbox.onIdle();
		expect(outbox.controlCalls.length).toBe(1);
		const idleNotice = outbox.controlCalls[0]!.frame as PeerIdleNoticeFrame;
		expect(idleNotice.action).toBe("peer_idle_notice");
		expect(idleNotice.nonce).toBe("n-busy");
	});

	it("notify_when_idle with an invalid address is rejected", async () => {
		const { inbox, outbox } = makeInbox({});
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "sub-2",
			type: "control",
			action: "notify_when_idle",
			reply_to: "pipe:LOCAL\\not-a-peer-name",
			from: `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`,
		};
		await inbox.handleFrame(frame, "peer");
		inbox.onIdle();
		expect(outbox.controlCalls).toEqual([]);
	});

	it("notify_when_idle aimed at ourselves is rejected (self-subscription)", async () => {
		const { inbox, outbox } = makeInbox({});
		const self = `pipe:LOCAL\\pi-peer-${"f".repeat(32)}`;
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "sub-3",
			type: "control",
			action: "notify_when_idle",
			reply_to: self,
			from: `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`,
		};
		await inbox.handleFrame(frame, "peer");
		inbox.onIdle();
		expect(outbox.controlCalls).toEqual([]);
	});

	it("a peer_idle_notice with a valid nonce surfaces as an agent notice", async () => {
		const { inbox, host, outbox } = makeInbox({});
		outbox.registerIdleNonce("n-valid");
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "notice-1",
			type: "control",
			action: "peer_idle_notice",
			from: `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`,
			fromName: "alpha",
			nonce: "n-valid",
		};
		await inbox.handleFrame(frame, "peer");
		expect(host.notices).toEqual(['Peer "alpha" is now idle.']);
	});

	it("peer_idle_notice without a nonce is dropped silently", async () => {
		const { inbox, host } = makeInbox({});
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "notice-2",
			type: "control",
			action: "peer_idle_notice",
			from: `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`,
			fromName: "alpha",
		};
		await inbox.handleFrame(frame, "peer");
		expect(host.notices).toEqual([]);
	});

	it("peer_idle_notice with an unknown nonce is dropped silently", async () => {
		const { inbox, host, outbox } = makeInbox({});
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "notice-3",
			type: "control",
			action: "peer_idle_notice",
			from: `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`,
			fromName: "alpha",
			nonce: "never-registered",
		};
		await inbox.handleFrame(frame, "peer");
		expect(host.notices).toEqual([]);
	});

	it("a peer_idle_notice nonce is one-shot: a replay is dropped", async () => {
		const { inbox, host, outbox } = makeInbox({});
		outbox.registerIdleNonce("n-once");
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "notice-4",
			type: "control",
			action: "peer_idle_notice",
			from: `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`,
			fromName: "alpha",
			nonce: "n-once",
		};
		await inbox.handleFrame(frame, "peer");
		await inbox.handleFrame({ ...frame, msg_id: "notice-5" }, "peer");
		expect(host.notices).toEqual(['Peer "alpha" is now idle.']);
	});

	it("peer_idle_notice sanitizes a hostile name before the agent sees it", async () => {
		const { inbox, host, outbox } = makeInbox({});
		outbox.registerIdleNonce("n-hostile");
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "notice-hostile",
			type: "control",
			action: "peer_idle_notice",
			from: `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`,
			fromName: 'alpha"><script>alert(1)</script>',
			nonce: "n-hostile",
		};
		await inbox.handleFrame(frame, "peer");
		// Control characters and <>" never reach the notice text.
		expect(host.notices).toEqual(['Peer "alphascriptalert(1)/script" is now idle.']);
	});

	it("peer_idle_notice without a name quotes the address", async () => {
		const { inbox, host, outbox } = makeInbox({});
		outbox.registerIdleNonce("n-addr");
		const from = `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`;
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "notice-6",
			type: "control",
			action: "peer_idle_notice",
			from,
			nonce: "n-addr",
		};
		await inbox.handleFrame(frame, "peer");
		expect(host.notices).toEqual([`Peer "${from}" is now idle.`]);
	});

	it("onIdle resets the queued count", async () => {
		const { inbox } = makeInbox({ host: { policy: "all" } });
		await inbox.handleFrame(userFrame(), "peer");
		expect(inbox.pendingCount()).toBe(1);
		inbox.onIdle();
		expect(inbox.pendingCount()).toBe(0);
	});

	it("receipt delivery failures never break message processing", async () => {
		const outbox = makeScriptedOutbox();
		outbox.failControl = true;
		const { inbox, host } = makeInbox({ outbox, host: { policy: "all" } });
		const frame = userFrame();
		await expect(inbox.handleFrame(frame, "peer")).resolves.toBeUndefined();
		expect(host.injected.length).toBe(1);
	});

	// Shutdown receiving: every branch answers with a shutdown_response frame
	// before any local effect (quit only after the response is sent).

	it("accept policy all: same-project shutdown is accepted, responded, then quit", async () => {
		const from = `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`;
		const { inbox, host, outbox } = makeInbox({
			host: { policy: "all" },
			entries: [
				entry({ cwd: "D:/work/alpha", projectRoot: "D:/work/alpha", socketPath: `\\\\.\\pipe\\LOCAL\\pi-peer-${"b".repeat(32)}` }),
				entry({ pid: 100, cwd: "D:/work/alpha", projectRoot: "D:/work/alpha", socketPath: `\\\\.\\pipe\\LOCAL\\pi-peer-${"f".repeat(32)}` }),
			],
		});
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "shutdown-1",
			type: "control",
			action: "shutdown",
			from,
			fromName: "alpha",
		};

		await inbox.handleFrame(frame, "peer");

		// Response frame first: exactly one control frame, sent to the sender.
		expect(outbox.controlCalls.length).toBe(1);
		const { address, frame: sent } = outbox.controlCalls[0]!;
		expect(address).toBe(from);
		const response = sent as PeerShutdownResponseFrame;
		expect(response.action).toBe("shutdown_response");
		expect(response.inReplyTo).toBe("shutdown-1");
		expect(response.from).toBe(`pipe:LOCAL\\pi-peer-${"f".repeat(32)}`);
		expect(response.fromName).toBe("me");
		expect(response.outcome).toBe("accepted");
		expect(response.reason).toBeUndefined();
		expect(response.msgV).toBe(1);
		expect(typeof response.msg_id).toBe("string");
		expect(response.msg_id).not.toBe(frame.msg_id);
		// Then the notify + quit.
		expect(host.quitCalls).toBe(1);
		expect(host.userNotifies).toEqual([{ title: "peers", message: "Shutting down at request of peer alpha" }]);
		expect(host.confirmCalls).toEqual([]);
	});

	it("accept policy all: unknown sender (registry race) is accepted under all", async () => {
		// No registry entries at all: benefit of the doubt.
		const { inbox, host, outbox } = makeInbox({ host: { policy: "all" } });
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "shutdown-unknown",
			type: "control",
			action: "shutdown",
			from: `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`,
			fromName: "alpha",
		};

		await inbox.handleFrame(frame, "peer");

		expect(host.quitCalls).toBe(1);
		const response = outbox.controlCalls[0]!.frame as PeerShutdownResponseFrame;
		expect(response.outcome).toBe("accepted");
	});

	it("accept policy all: cross-project sender with UI → confirm shown; no → denied, no quit", async () => {
		const from = `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`;
		const { inbox, host, outbox } = makeInbox({
			host: { policy: "all", nextAnswer: "no" },
			entries: [
				entry({ cwd: "D:/work/beta", projectRoot: "D:/work/beta", socketPath: `\\\\.\\pipe\\LOCAL\\pi-peer-${"b".repeat(32)}` }),
				entry({ pid: 100, cwd: "D:/work/alpha", projectRoot: "D:/work/alpha", socketPath: `\\\\.\\pipe\\LOCAL\\pi-peer-${"f".repeat(32)}` }),
			],
		});
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "shutdown-2",
			type: "control",
			action: "shutdown",
			from,
			fromName: "alpha",
		};

		await inbox.handleFrame(frame, "peer");

		expect(host.confirmCalls.length).toBe(1);
		expect(host.confirmCalls[0]!.title).toBe("Peer shutdown request");
		expect(host.quitCalls).toBe(0);
		expect(host.userNotifies).toEqual([]);
		const response = outbox.controlCalls[0]!.frame as PeerShutdownResponseFrame;
		expect(response.outcome).toBe("denied");
		expect(response.reason).toBe("cross-project refused");
		expect(response.inReplyTo).toBe("shutdown-2");
	});

	it("accept policy all: cross-project headless → denied without confirm or quit", async () => {
		const from = `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`;
		const { inbox, host, outbox } = makeInbox({
			host: { policy: "all", hasUIValue: false, nextAnswer: "yes" },
			entries: [
				entry({ cwd: "D:/work/beta", projectRoot: "D:/work/beta", socketPath: `\\\\.\\pipe\\LOCAL\\pi-peer-${"b".repeat(32)}` }),
				entry({ pid: 100, cwd: "D:/work/alpha", projectRoot: "D:/work/alpha", socketPath: `\\\\.\\pipe\\LOCAL\\pi-peer-${"f".repeat(32)}` }),
			],
		});
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "shutdown-3",
			type: "control",
			action: "shutdown",
			from,
			fromName: "alpha",
		};

		await inbox.handleFrame(frame, "peer");

		expect(host.confirmCalls).toEqual([]);
		expect(host.quitCalls).toBe(0);
		const response = outbox.controlCalls[0]!.frame as PeerShutdownResponseFrame;
		expect(response.outcome).toBe("denied");
		expect(response.reason).toBe("cross-project refused");
	});

	it("accept policy off: shutdown is denied with a reason and no quit", async () => {
		const from = `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`;
		const { inbox, host, outbox } = makeInbox({ host: { policy: "off" } });
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "shutdown-4",
			type: "control",
			action: "shutdown",
			from,
			fromName: "alpha",
		};

		await inbox.handleFrame(frame, "peer");

		expect(host.quitCalls).toBe(0);
		expect(host.userNotifies).toEqual([]);
		expect(host.confirmCalls).toEqual([]);
		expect(outbox.controlCalls.length).toBe(1);
		expect(outbox.controlCalls[0]!.address).toBe(from);
		const response = outbox.controlCalls[0]!.frame as PeerShutdownResponseFrame;
		expect(response.action).toBe("shutdown_response");
		expect(response.inReplyTo).toBe("shutdown-4");
		expect(response.outcome).toBe("denied");
		expect(response.reason).toBe("accept policy off");
	});

	it("accept policy ask: shutdown proceeds after user says yes", async () => {
		const from = `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`;
		const { inbox, host, outbox } = makeInbox({ host: { policy: "ask", nextAnswer: "yes" } });
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "shutdown-5",
			type: "control",
			action: "shutdown",
			from,
			fromName: "alpha",
		};

		await inbox.handleFrame(frame, "peer");

		expect(host.confirmCalls.length).toBe(1);
		expect(host.quitCalls).toBe(1);
		expect(host.userNotifies).toEqual([{ title: "peers", message: "Shutting down at request of peer alpha" }]);
		const response = outbox.controlCalls[0]!.frame as PeerShutdownResponseFrame;
		expect(response.outcome).toBe("accepted");
		expect(response.reason).toBeUndefined();
		expect(response.inReplyTo).toBe("shutdown-5");
	});

	it("accept policy ask: shutdown denied with 'user declined' when user says no", async () => {
		const { inbox, host, outbox } = makeInbox({ host: { policy: "ask", nextAnswer: "no" } });
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "shutdown-6",
			type: "control",
			action: "shutdown",
			from: `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`,
			fromName: "alpha",
		};

		await inbox.handleFrame(frame, "peer");

		expect(host.quitCalls).toBe(0);
		expect(host.userNotifies).toEqual([]);
		expect(host.confirmCalls.length).toBe(1);
		const response = outbox.controlCalls[0]!.frame as PeerShutdownResponseFrame;
		expect(response.outcome).toBe("denied");
		expect(response.reason).toBe("user declined");
	});

	it("accept policy ask: shutdown denied with 'confirmation timed out' on timeout", async () => {
		const { inbox, host, outbox } = makeInbox({ host: { policy: "ask", nextAnswer: "timeout" } });
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "shutdown-7",
			type: "control",
			action: "shutdown",
			from: `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`,
			fromName: "alpha",
		};

		await inbox.handleFrame(frame, "peer");

		expect(host.quitCalls).toBe(0);
		const response = outbox.controlCalls[0]!.frame as PeerShutdownResponseFrame;
		expect(response.outcome).toBe("denied");
		expect(response.reason).toBe("confirmation timed out");
	});

	it("accept policy ask: shutdown confirm message includes the reason when provided", async () => {
		const { inbox, host } = makeInbox({ host: { policy: "ask", nextAnswer: "yes" } });
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "shutdown-8",
			type: "control",
			action: "shutdown",
			from: `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`,
			fromName: "alpha",
			reason: "work finished, please exit",
		};

		await inbox.handleFrame(frame, "peer");

		expect(host.confirmCalls.length).toBe(1);
		expect(host.confirmCalls[0]!.message).toContain("work finished, please exit");
		expect(host.quitCalls).toBe(1);
	});

	it("accepted shutdown waits for the response frame before quitting", async () => {
		// The response must be on the wire (await sendControl resolved) before
		// quit() runs: order recorded via a quit() that observes "sent".
		const order: string[] = [];
		const outbox = makeScriptedOutbox();
		const host = makeHost({ policy: "all" });
		host.quit = () => {
			order.push(outbox.controlCalls.length > 0 ? "sent" : "nothing-sent");
		};
		const deps = {
			identity: makeIdentity(),
			protocol: proto,
			transport: makeTransport(),
			auth: makeAuth(),
			registry: makeRegistry(() => ({ kind: "none" })),
			guard: makeGuard(),
			pacer: makePacer(),
			host,
			outbox,
		};
		const inbox = createPeerInbox(deps);

		await inbox.handleFrame(
			{
				msgV: 1,
				msg_id: "shutdown-9",
				type: "control",
				action: "shutdown",
				from: `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`,
				fromName: "alpha",
			},
			"peer",
		);

		expect(order).toEqual(["sent"]);
		expect(outbox.controlCalls.length).toBe(1);
	});

	it("inbound shutdown_response notifies the user and the requesting agent", async () => {
		const { inbox, host } = makeInbox({});
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "resp-1",
			type: "control",
			action: "shutdown_response",
			inReplyTo: "m-orig",
			from: `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`,
			fromName: "alpha",
			outcome: "accepted",
		};

		await inbox.handleFrame(frame, "peer");

		expect(host.userNotifies).toEqual([
			{ title: "peers", message: 'Peer "alpha" accepted the shutdown request.' },
		]);
		expect(host.notices).toEqual(['Peer "alpha" accepted the shutdown request.']);
	});

	it("inbound shutdown_response with a reason and hostile text is sanitized", async () => {
		const { inbox, host } = makeInbox({});
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "resp-2",
			type: "control",
			action: "shutdown_response",
			inReplyTo: "m-orig",
			from: `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`,
			fromName: "alpha",
			outcome: "denied",
			reason: 'busy <script>alert(1)</script>"quoted"',
		};

		await inbox.handleFrame(frame, "peer");

		const expected = 'Peer "alpha" denied the shutdown request (busy scriptalert(1)/scriptquoted).';
		expect(host.userNotifies).toEqual([{ title: "peers", message: expected }]);
		expect(host.notices).toEqual([expected]);
	});

	it("inbound shutdown_response without a name quotes the address", async () => {
		const { inbox, host } = makeInbox({});
		const from = `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`;
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "resp-3",
			type: "control",
			action: "shutdown_response",
			inReplyTo: "m-orig",
			from,
			outcome: "denied",
			reason: "user declined",
		};

		await inbox.handleFrame(frame, "peer");

		const expected = `Peer "${from}" denied the shutdown request (user declined).`;
		expect(host.notices).toEqual([expected]);
	});

	it("child frame flood: exhausted child bucket drops frames without injection", async () => {
		const { inbox, host, guard, outbox } = makeInbox({
			guard: { nextChildCheck: { ok: false, reason: "rate-limited" } },
			host: { policy: "all" },
		});
		const frame = userFrame({ msg_id: "child-1" });

		await inbox.handleFrame(frame, "child");
		await inbox.handleFrame({ ...frame, msg_id: "child-2" }, "child");

		// Dropped silently: no injection, no receipt, guard consulted each time.
		expect(host.injected).toEqual([]);
		expect(outbox.controlCalls).toEqual([]);
		expect(guard.childChecks).toEqual([frame.from, frame.from]);
		expect(inbox.pendingCount()).toBe(0);
	});

	it("idleSubs: the 65th subscription evicts the oldest", async () => {
		const transport = makeTransport();
		const { inbox, outbox } = makeInbox({ transport });
		for (let i = 0; i < 65; i++) {
			const from = `pipe:LOCAL\\pi-peer-${String(i).padStart(32, "0")}`;
			transport.validAddresses.add(from);
			await inbox.handleFrame(
				{
					msgV: 1,
					msg_id: `sub-${i}`,
					type: "control",
					action: "notify_when_idle",
					reply_to: from,
					from,
					nonce: `n-${i}`,
				},
				"peer",
			);
		}

		inbox.onIdle();

		// Exactly 64 notices: sub-0 (the oldest) was evicted by sub-64.
		expect(outbox.controlCalls.length).toBe(64);
		const nonces = outbox.controlCalls.map((c) => (c.frame as PeerIdleNoticeFrame).nonce);
		expect(nonces).not.toContain("n-0");
		expect(nonces).toContain("n-1");
		expect(nonces).toContain("n-64");
	});

	it("an expired idleSub (31 min old) never fires on idle", async () => {
		const from = `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`;
		const transport = makeTransport();
		transport.validAddresses.add(from);
		const { inbox, outbox } = makeInbox({ transport });
		await inbox.handleFrame(
			{
				msgV: 1,
				msg_id: "sub-old",
				type: "control",
				action: "notify_when_idle",
				reply_to: from,
				from,
				nonce: "n-old",
			},
			"peer",
		);
		expect(outbox.controlCalls).toEqual([]);

		// 31 minutes later a NEW subscription arrives — evicting the stale one.
		vi.useFakeTimers();
		vi.setSystemTime(Date.now() + 31 * 60_000);
		const fresh = `pipe:LOCAL\\pi-peer-${"c".repeat(32)}`;
		transport.validAddresses.add(fresh);
		await inbox.handleFrame(
			{
				msgV: 1,
				msg_id: "sub-fresh",
				type: "control",
				action: "notify_when_idle",
				reply_to: fresh,
				from: fresh,
				nonce: "n-fresh",
			},
			"peer",
		);
		vi.useRealTimers();

		inbox.onIdle();
		// Only the fresh subscription fires; the 31-minute-old one was evicted.
		const nonces = outbox.controlCalls.map((c) => (c.frame as PeerIdleNoticeFrame).nonce);
		expect(nonces).toEqual(["n-fresh"]);
	});

});
