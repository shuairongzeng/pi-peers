import { describe, expect, it } from "vitest";
import { createPeerOutbox } from "../src/outbox.ts";
import {
	type PeerAuth,
	type PeerConnection,
	type PeerGuard,
	type PeerGuardCheck,
	type PeerHost,
	type PeerIdentity,
	type PeerMessageStatusFrame,
	type PeerOutbox,
	type PeerProtocol,
	type PeerRegistry,
	type PeerRegistryEntry,
	type PeerResolution,
	PeerSendError,
	type PeerSenderPacer,
	type PeerTransport,
} from "../src/types.ts";

// ---------------------------------------------------------------------------
// Fakes (in-memory doubles for the PeerServiceDeps surface)
// ---------------------------------------------------------------------------

interface ScriptedHost extends PeerHost {
	notices: string[];
	userNotifies: Array<{ title: string; message: string }>;
}

function makeHost(): ScriptedHost {
	return {
		notices: [],
		userNotifies: [],
		async injectUserMessage() {},
		notify(title, message) {
			this.userNotifies.push({ title, message });
		},
		async confirm() {
			return "yes";
		},
		async injectAgentNotice(text) {
			this.notices.push(text);
		},
		hasUI() {
			return true;
		},
		acceptPolicy() {
			return "ask";
		},
		isIdleNow() {
			return false;
		},
		quit() {},
	};
}

interface ScriptedPacer extends PeerSenderPacer {
	consumed: string[];
	refunded: string[];
}

function makePacer(): ScriptedPacer {
	return {
		consumed: [],
		refunded: [],
		tryConsume(target) {
			this.consumed.push(target);
			return true;
		},
		refund(target) {
			this.refunded.push(target);
		},
	};
}

function makeGuard(): PeerGuard {
	return {
		check: (_fromKey, _frame, _ownHopToken, _queuedCount): PeerGuardCheck => ({ ok: true }),
		checkChild: (): PeerGuardCheck => ({ ok: true }),
		recordAccepted() {},
		refund() {},
	};
}

interface ScriptedAuth extends PeerAuth {
	/** Number of lookupKey calls (one per send in the restructured outbox). */
	lookupCalls: string[];
}

function makeAuth(keys: Record<string, { pid: number; peerToken: string }> = {}): ScriptedAuth {
	return {
		lookupCalls: [],
		generateTokens: () => ({ peerToken: "new-peer", childToken: "new-child" }),
		publishKey: () => {},
		lookupKey(peersDir: string, canonicalSocketPath: string) {
			this.lookupCalls.push(canonicalSocketPath);
			return keys[canonicalSocketPath];
		},
		deleteKeys: () => {},
	};
}

interface FakeTransport extends PeerTransport {
	connects: string[];
	failConnect: boolean;
	sessions: FakeConn[];
}

interface FakeConn extends PeerConnection {
	lines: string[];
	ended: boolean;
	destroyed: boolean;
}

function makeTransport(overrides: Partial<FakeTransport> = {}): FakeTransport {
	const t: FakeTransport = {
		connects: [],
		failConnect: false,
		sessions: [],
		deriveSocketAddress: () => ({ path: "p", address: "a" }),
		isValidAddress: () => true,
		resolveAddress: (address: string) =>
			address.startsWith("pipe:") ? `\\\\.\\pipe\\${address.slice(5)}` : undefined,
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
					this.lines.push(line);
				},
				async end() {
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

function entry(rec: Partial<PeerRegistryEntry["record"]> = {}, live = true): PeerRegistryEntry {
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

function makeRegistry(resolveImpl: (name: string) => PeerResolution, entries: PeerRegistryEntry[] = []): PeerRegistry {
	return {
		publish: () => {},
		update: () => {},
		unpublish: () => {},
		list: () => entries,
		readRecord: (pid: number) => entries.find((e) => e.record.pid === pid),
		resolve: resolveImpl,
		resolveIn: (name: string) => resolveImpl(name),
		normalizeName: (name: string) => name.toLowerCase().replace(/\s+/g, "-"),
		refOf: (rec) => `${(rec.pid % 256).toString(16).padStart(2, "0")}a1b2c`,
		isAlive: () => true,
		readProcStart: () => undefined,
		pidDomain: () => "win32:box",
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
/** Minimal protocol fake: real serialization, stubbed parsing. */
const proto: PeerProtocol = {
	newEnvelope: () => ({ msgV: 1, msg_id: `msg-${Math.random().toString(36).slice(2, 10)}` }),
	serializeFrame: (frame) => `${JSON.stringify(frame)}\n`,
	wrapPeerContent: (_attrs, body) => body,
	parseFrameLine: () => undefined,
	parsePeerContent: () => undefined,
	sanitizePeerText: (text) => text,
	buildAuthLine: (token) => `${JSON.stringify({ type: "auth", token })}\n`,
	parseAuthLine: () => undefined,
};

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

/** Deps bundle for one outbox under test. */
function makeDeps(
	overrides: {
		transport?: FakeTransport;
		pacer?: ScriptedPacer;
		host?: ScriptedHost;
		auth?: ScriptedAuth;
		registry?: PeerRegistry;
		identity?: PeerIdentity;
	} = {},
) {
	const alphaEntry = entry();
	const deps = {
		identity: overrides.identity ?? makeIdentity(),
		protocol: proto,
		transport: overrides.transport ?? makeTransport(),
		auth: overrides.auth ?? makeAuth({ [alphaEntry.record.socketPath]: { pid: 4242, peerToken: "key-alpha" } }),
		registry: overrides.registry ?? makeRegistry(() => ({ kind: "unique", entry: alphaEntry })),
		guard: makeGuard(),
		pacer: overrides.pacer ?? makePacer(),
		host: overrides.host ?? makeHost(),
		peersDir: "/fake/peers",
		outbox: undefined as unknown as PeerOutbox,
	};
	return deps;
}

// ---------------------------------------------------------------------------
// Receipt authenticity
// ---------------------------------------------------------------------------

describe("createPeerOutbox receipt authenticity", () => {
	it("ignores a receipt for an unknown msg_id: no store, no injectAgentNotice", () => {
		const host = makeHost();
		const deps = makeDeps({ host });
		const outbox = createPeerOutbox(deps);

		outbox.handleReceipt(statusFrame({ msg_id: "spoofed-1", state: "denied", reason: "nope" }));
		outbox.handleReceipt(statusFrame({ msg_id: "spoofed-2", state: "delivered" }));

		expect(outbox.receiptOf("spoofed-1")).toBeUndefined();
		expect(outbox.receiptOf("spoofed-2")).toBeUndefined();
		expect(host.notices).toEqual([]);
		expect(host.userNotifies).toEqual([]);
	});

	it("processes a receipt for a msg_id the outbox actually sent", async () => {
		const host = makeHost();
		const deps = makeDeps({ host });
		const outbox = createPeerOutbox(deps);

		const outcome = await outbox.send("alpha", "hello", {});
		expect(outcome.msgId).toBeTruthy();

		outbox.handleReceipt(statusFrame({ msg_id: outcome.msgId, state: "delivered" }));
		expect(outbox.receiptOf(outcome.msgId)).toBe("delivered");
		outbox.handleReceipt(statusFrame({ msg_id: outcome.msgId, state: "denied", reason: "rejected by user" }));
		expect(outbox.receiptOf(outcome.msgId)).toBe("denied");
		expect(host.notices).toEqual([
			`Peer message to alpha (${outcome.msgId}) was denied: rejected by user.`,
		]);
	});

	it("expectsReceipt only answers true after registerOutbound", () => {
		const deps = makeDeps();
		const outbox = createPeerOutbox(deps);
		expect(outbox.expectsReceipt("m-unknown")).toBe(false);
		outbox.registerOutbound("m-mine");
		expect(outbox.expectsReceipt("m-mine")).toBe(true);
	});

	it("does not register the msg_id when the send fails", async () => {
		const transport = makeTransport({ failConnect: true });
		const pacer = makePacer();
		const deps = makeDeps({ transport, pacer });
		const outbox = createPeerOutbox(deps);

		await expect(outbox.send("alpha", "hi", {})).rejects.toMatchObject({ code: "connect_failed" });
		// A failed send produced no msg_id the caller could learn, but receipts
		// for any id from this failed attempt must not be accepted.
		expect(outbox.expectsReceipt("any-id")).toBe(false);
	});
});

// ---------------------------------------------------------------------------
// Pacer refund scope
// ---------------------------------------------------------------------------

describe("createPeerOutbox pacer refund scope", () => {
	it("sendControl failure does not refund the pacer (it never consumed a token)", async () => {
		const transport = makeTransport();
		const pacer = makePacer();
		const deps = makeDeps({ transport, pacer });
		const outbox = createPeerOutbox(deps);
		const address = `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`;

		await outbox.send("alpha", "first", {});
		expect(pacer.consumed).toEqual(["alpha"]);
		expect(pacer.refunded).toEqual([]);

		// Control delivery fails: sendControl swallows the error, and no
		// refund may appear (sendControl consumed nothing).
		transport.failConnect = true;
		await expect(outbox.sendControl(address, statusFrame())).resolves.toBeUndefined();
		expect(pacer.refunded).toEqual([]);

		// A failed send() (which did consume) still refunds.
		await expect(outbox.send("alpha", "second", {})).rejects.toMatchObject({ code: "connect_failed" });
		expect(pacer.refunded).toEqual(["alpha"]);
	});
});

// ---------------------------------------------------------------------------
// Idle nonces
// ---------------------------------------------------------------------------

describe("createPeerOutbox idle nonces", () => {
	it("consumeIdleNonce is one-shot: true once, false on reuse, unknown, and undefined", () => {
		const deps = makeDeps();
		const outbox = createPeerOutbox(deps);

		expect(outbox.consumeIdleNonce(undefined)).toBe(false);
		expect(outbox.consumeIdleNonce("never-registered")).toBe(false);

		outbox.registerIdleNonce("n-1");
		expect(outbox.consumeIdleNonce("n-1")).toBe(true);
		expect(outbox.consumeIdleNonce("n-1")).toBe(false);
	});

	it("send with notifyWhenIdle registers the nonce it put on the wire", async () => {
		const deps = makeDeps();
		const outbox = createPeerOutbox(deps);
		const transport = deps.transport as FakeTransport;

		await outbox.send("alpha", "ping", { notifyWhenIdle: true });

		// Line 0 is the auth line; line 1 the notify_when_idle frame.
		const notify = JSON.parse(transport.sessions[0]!.lines[1]!) as { action: string; nonce: string };
		expect(notify.action).toBe("notify_when_idle");
		expect(outbox.consumeIdleNonce(notify.nonce)).toBe(true);
	});

	it("send without notifyWhenIdle registers no nonce", async () => {
		const deps = makeDeps();
		const outbox = createPeerOutbox(deps);
		const transport = deps.transport as FakeTransport;

		await outbox.send("alpha", "ping", {});

		const frame = JSON.parse(transport.sessions[0]!.lines[1]!) as { action?: string; nonce?: string };
		expect(frame.action).toBeUndefined();
		// The only line pair is auth + user frame; no nonce to consume.
		expect(outbox.consumeIdleNonce(frame.nonce ?? "none")).toBe(false);
	});

	it("skips the idle subscription and flags the outcome when the peer lacks notify_idle", async () => {
		// Simulate an older plugin: record without the notify_idle feature.
		const legacyEntry = entry({ features: [] });
		const deps = makeDeps({ registry: makeRegistry(() => ({ kind: "unique", entry: legacyEntry })) });
		const outbox = createPeerOutbox(deps);
		const transport = deps.transport as FakeTransport;

		const outcome = await outbox.send("alpha", "ping", { notifyWhenIdle: true });

		// auth + user frame only — no subscription line on the wire.
		expect(transport.sessions[0]!.lines).toHaveLength(2);
		expect(outcome.notifyIdleUnsupported).toBe(true);
	});
});

// ---------------------------------------------------------------------------
// Single token computation per send
// ---------------------------------------------------------------------------

describe("createPeerOutbox token resolution", () => {
	it("send performs exactly one key lookup for the target", async () => {
		const alphaEntry = entry();
		const auth = makeAuth({ [alphaEntry.record.socketPath]: { pid: 4242, peerToken: "key-alpha" } });
		const deps = makeDeps({
			auth,
			registry: makeRegistry(() => ({ kind: "unique", entry: alphaEntry })),
		});
		const outbox = createPeerOutbox(deps);

		await outbox.send("alpha", "hi", {});

		// One send → one lookupKey call for the target socket path (the token
		// is passed down to deliver(); nothing re-reads the key file).
		expect(auth.lookupCalls).toEqual([alphaEntry.record.socketPath]);
		// The wire carries that key's token.
		const transport = deps.transport as FakeTransport;
		const authLine = JSON.parse(transport.sessions[0]!.lines[0]!) as { token: string };
		expect(authLine.token).toBe("key-alpha");
	});

	it("send to our own parent authenticates with the parent's child token and never reads a key", async () => {
		// The parent's socket, in WIRE form; the outbox resolves it to the
		// canonical path, which the target record also stores.
		const parentWire = `pipe:LOCAL\\pi-peer-${"e".repeat(32)}`;
		const parentEntry = entry({
			pid: 77,
			name: "parent-peer",
			socketPath: `\\\\.\\pipe\\LOCAL\\pi-peer-${"e".repeat(32)}`,
		});
		const auth = makeAuth(); // empty key table: any lookup returns undefined
		const deps = makeDeps({
			auth,
			identity: makeIdentity({ parent: { socketAddress: parentWire, token: "child-token-42" } }),
			registry: makeRegistry(() => ({ kind: "unique", entry: parentEntry })),
		});
		const outbox = createPeerOutbox(deps);

		await outbox.send("parent-peer", "hi parent", {});

		// The CHILD token (identity.parent.token) is on the auth line, and
		// lookupKey was never called for the parent's socket path.
		const transport = deps.transport as FakeTransport;
		const authLine = JSON.parse(transport.sessions[0]!.lines[0]!) as { token: string };
		expect(authLine.token).toBe("child-token-42");
		expect(auth.lookupCalls).toEqual([]);
	});
});
