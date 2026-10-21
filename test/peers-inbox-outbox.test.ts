import { describe, expect, it } from "vitest";
import { createPeerInbox } from "../src/inbox.ts";
import { createPeerOutbox } from "../src/outbox.ts";
import {
	PEER_APPROVAL_TIMEOUT_MS,
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
}

function makeGuard(overrides: Partial<ScriptedGuard> = {}): ScriptedGuard {
	return {
		checks: [],
		accepted: [],
		refunded: [],
		nextCheck: { ok: true },
		...overrides,
		check(fromKey, _frame, _ownHopToken, queuedCount) {
			this.checks.push({ fromKey, queued: queuedCount });
			return this.nextCheck;
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
			features: [],
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

/** Minimal protocol fake: real serialization/wrapping, stubbed parsers. */
const proto: PeerProtocol = {
	newEnvelope: () => ({ msgV: 1, msg_id: `msg-${Math.random().toString(36).slice(2, 10)}` }),
	serializeFrame: (frame) => `${JSON.stringify(frame)}\n`,
	wrapPeerContent: (attrs, body) =>
		`<cross-session-message${attrs.fromName ? ` from-name="${attrs.fromName}"` : ""}>\n${body}\n</cross-session-message>`,
	parseFrameLine: () => undefined,
	parsePeerContent: () => undefined,
	buildAuthLine: (token) => `${JSON.stringify({ type: "auth", token })}\n`,
	parseAuthLine: () => undefined,
};

function userFrame(overrides: Partial<PeerUserFrame> = {}): PeerUserFrame {
	return {
		msgV: 1,
		msg_id: "incoming-1",
		type: "user",
		from: `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`,
		fromPid: 4242,
		fromSession: "peer-session",
		fromName: "alpha",
		hopChain: ["hop-peer"],
		priority: "next",
		content: "hello from alpha",
		...overrides,
	};
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
}

function makeScriptedOutbox(): ScriptedOutbox {
	return {
		controlCalls: [],
		receiptsHandled: [],
		failControl: false,
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
			msgId: expect.stringMatching(/^msg-/),
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
		expect(frame.content).toBe(`<cross-session-message from-name="me">\nping body\n</cross-session-message>`);
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

	it("throws not_found with live peer list and no-send hint", async () => {
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
		expect(message).toContain("beta [37a1b2c]");
		expect(message).toContain("alpha [");
		expect(message).not.toContain("dead-peer");
		expect(message).toContain("may not be running");
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

		outbox.handleReceipt(statusFrame({ msg_id: "m-1", state: "held" }));
		expect(outbox.receiptOf("m-1")).toBe("held");
		outbox.handleReceipt(statusFrame({ msg_id: "m-1", state: "delivered" }));
		expect(outbox.receiptOf("m-1")).toBe("delivered");
		expect(host.notices).toEqual([]);
		expect(host.userNotifies.length).toBe(2);
		expect(host.userNotifies[0]!.title).toBe("Peer message");

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

		outbox.handleReceipt(statusFrame({ msg_id: "m-9", state: "denied", reason: "rejected by user" }));
		expect(outbox.receiptOf("m-9")).toBe("denied");
		outbox.handleReceipt(statusFrame({ msg_id: "m-10", state: "dropped", reason: "rate-limited" }));
		outbox.handleReceipt(statusFrame({ msg_id: "m-11", state: "expired" }));
		outbox.handleReceipt(statusFrame({ msg_id: "m-12", state: "refused" }));

		expect(host.userNotifies).toEqual([]);
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
	}): { inbox: PeerInbox; host: ScriptedHost; guard: ScriptedGuard; outbox: ScriptedOutbox } {
		const host = makeHost(opts.host);
		const guard = makeGuard(opts.guard);
		const outbox = opts.outbox ?? makeScriptedOutbox();
		const deps = {
			identity: makeIdentity(),
			protocol: proto,
			transport: opts.transport ?? makeTransport(),
			auth: makeAuth(),
			registry: makeRegistry(() => ({ kind: "none" })),
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

	it("ask: held receipt first, then delivered after user says yes", async () => {
		const { inbox, host, outbox } = makeInbox({ host: { policy: "ask", nextAnswer: "yes" } });
		const frame = userFrame({ fromName: "alpha", content: "please review" });

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
		await inbox.handleFrame(userFrame({ content: long }), "peer");
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

	it("queued count grows and is passed to the guard", async () => {
		const { inbox, guard } = makeInbox({ host: { policy: "all" } });
		await inbox.handleFrame(userFrame({ msg_id: "i1" }), "peer");
		await inbox.handleFrame(userFrame({ msg_id: "i2", content: "second" }), "peer");
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

	it("a peer_idle_notice control frame surfaces as an agent notice", async () => {
		const { inbox, host } = makeInbox({});
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "notice-1",
			type: "control",
			action: "peer_idle_notice",
			from: `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`,
			fromName: "alpha",
		};
		await inbox.handleFrame(frame, "peer");
		expect(host.notices).toEqual(['Peer "alpha" is now idle.']);
	});

	it("peer_idle_notice without a name quotes the address", async () => {
		const { inbox, host } = makeInbox({});
		const from = `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`;
		const frame: PeerControlFrame = {
			msgV: 1,
			msg_id: "notice-2",
			type: "control",
			action: "peer_idle_notice",
			from,
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
});
