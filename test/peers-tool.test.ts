import { describe, expect, it } from "vitest";
import type {
	PeerIdentity,
	PeerOutbox,
	PeerRecord,
	PeerRegistry,
	PeerRegistryEntry,
	PeerToolDeps,
	PeersCommandContext,
	PeersToolDefinition,
} from "../src/types.ts";
import { PeerSendError } from "../src/types.ts";
import { createPeerMessageTool, createPeersCommand } from "../src/tool.ts";

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

function record(overrides: Partial<PeerRecord> = {}): PeerRecord {
	return {
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
		...overrides,
	};
}
function resolveInImpl(
	name: string,
	entries: Array<{ record: PeerRecord; live: boolean }>,
): { kind: "unique" | "ambiguous" | "none"; entry?: unknown; candidates?: unknown[] } {
	const matches = entries.filter(({ record }) => record.name === name);
	if (matches.length === 0) return { kind: "none" };
	if (matches.length > 1) return { kind: "ambiguous", candidates: matches };
	return { kind: "unique", entry: matches[0] };
}

function makeRegistry(entries: PeerRecord[]): PeerRegistry {
	const list = entries.map((rec) => ({ record: rec, live: true }));
	return {
		publish: () => {},
		update: () => {},
		unpublish: () => {},
		list: () => list,
		resolve: (name: string) => resolveInImpl(name, list),
		resolveIn: (name: string, scoped: PeerRegistryEntry[]) => resolveInImpl(name, scoped),
		refOf: (rec: PeerRecord) => `${(rec.pid % 256).toString(16).padStart(2, "0")}a1b2c`,
		isAlive: () => true,
		readProcStart: () => undefined,
		pidDomain: () => "win32:box",
	} as unknown as PeerRegistry;
}

function makeOutbox(
	sendImpl: (
		target: string,
		body: string,
		opts: { priority?: "next" | "now"; notifyWhenIdle?: boolean },
	) => Promise<never | { msgId: string; to: string; toName?: string }>,
): PeerOutbox {
	return {
		send: sendImpl,
		sendControl: async () => {},
		receiptOf: () => undefined,
		handleReceipt: () => {},
	} as unknown as PeerOutbox;
}

function fakeIdentity(pid: number): PeerIdentity {
	return {
		pid,
		sessionId: "self-session",
		socketAddress: `pipe:LOCAL\\pi-peer-${"f".repeat(32)}`,
		name: "me",
		hopToken: "hop-self",
		tokens: { peerToken: "t1", childToken: "t2" },
	};
}

function fakeDeps(opts: { outbox: PeerOutbox; entries?: PeerRecord[]; ownPid?: number }): PeerToolDeps {
	const ownPid = opts.ownPid ?? 100;
	const registry = makeRegistry(opts.entries ?? []);
	return { outbox: opts.outbox, registry, identity: fakeIdentity(ownPid) };
}

async function runTool(tool: PeersToolDefinition, params: Record<string, unknown>, cwd = "D:/work/alpha") {
	const ctx = { cwd } as Parameters<PeersToolDefinition["execute"]>[4];
	return tool.execute("call-1", params as never, undefined, undefined, ctx);
}

function textOf(result: { content: Array<{ type: string; text?: string }> }): string {
	return result.content
		.filter((block): block is { type: "text"; text: string } => block.type === "text")
		.map((block) => block.text)
		.join("\n");
}

// ---------------------------------------------------------------------------
// Tool: success
// ---------------------------------------------------------------------------

describe("createPeerMessageTool", () => {
	it("returns a send confirmation with msg_id and target name", async () => {
		const outbox = makeOutbox(async () => ({ msgId: "m-42", to: "pipe:LOCAL\\pi-peer-aaa", toName: "alpha" }));
		const tool = createPeerMessageTool(fakeDeps({ outbox }));
		const result = await runTool(tool, { to: "alpha", message: "hi" });
		expect(result.isError).toBeUndefined();
		const text = textOf(result);
		expect(text).toContain("msg_id m-42");
		expect(text).toContain('"alpha"');
		expect(text).toContain("pipe:LOCAL\\pi-peer-aaa");
		expect(text).toContain("Receipt will follow asynchronously");
	});

	it("passes priority and notifyWhenIdle to the outbox", async () => {
		const calls: Array<[string, string, { priority?: string; notifyWhenIdle?: boolean }]> = [];
		const outbox = makeOutbox(async (target, body, opts) => {
			calls.push([target, body, opts]);
			return { msgId: "m-1", to: "sock:/tmp/x.sock" };
		});
		const tool = createPeerMessageTool(fakeDeps({ outbox }));
		await runTool(tool, { to: "beta", message: "hello", priority: "now", notifyWhenIdle: true });
		expect(calls).toEqual([["beta", "hello", { priority: "now", notifyWhenIdle: true }]]);
	});

	it("omits priority and notifyWhenIdle when not given", async () => {
		const calls: Array<{ priority?: string; notifyWhenIdle?: boolean }> = [];
		const outbox = makeOutbox(async (_t, _b, opts) => {
			calls.push(opts);
			return { msgId: "m-1", to: "sock:/tmp/x.sock" };
		});
		const tool = createPeerMessageTool(fakeDeps({ outbox }));
		await runTool(tool, { to: "beta", message: "hello" });
		expect(calls).toEqual([{ priority: undefined, notifyWhenIdle: undefined }]);
	});

	it("falls back to the address when the peer name is unknown", async () => {
		const outbox = makeOutbox(async () => ({ msgId: "m-7", to: "sock:/tmp/y.sock" }));
		const tool = createPeerMessageTool(fakeDeps({ outbox }));
		const text = textOf(await runTool(tool, { to: "beta", message: "hi" }));
		expect(text).toContain("sock:/tmp/y.sock");
		expect(text).toContain("msg_id m-7");
	});

	it("action=list returns live peers without calling the outbox", async () => {
		let sends = 0;
		const outbox = makeOutbox(async () => {
			sends++;
			return { msgId: "m-1", to: "x" };
		});
		const tool = createPeerMessageTool(
			fakeDeps({
				outbox,
				entries: [record({ pid: 100, name: "self" }), record({ pid: 200, name: "alpha" })],
				ownPid: 100,
			}),
		);
		const text = textOf(await runTool(tool, { action: "list" }));
		expect(sends).toBe(0);
		expect(text).toContain("alpha");
		expect(text).toContain("self");
		expect(text).toContain("(this session)");
	});

	it("send without to/message is an error pointing at action=list", async () => {
		let sends = 0;
		const outbox = makeOutbox(async () => {
			sends++;
			return { msgId: "m-1", to: "x" };
		});
		const tool = createPeerMessageTool(fakeDeps({ outbox }));
		const result = await runTool(tool, { message: "hi" });
		expect(sends).toBe(0);
		expect(result.isError).toBe(true);
		expect(textOf(result)).toContain('action: "list"');
	});
});

// ---------------------------------------------------------------------------
// Tool: project scoping
// ---------------------------------------------------------------------------

describe("createPeerMessageTool project scoping", () => {
	const selfRecord = record({
		pid: 100,
		name: "me",
		cwd: "D:/work/alpha",
		projectRoot: "D:/work/alpha",
		sessionId: "1000000000000000",
	});
	const sameProjectPeer = record({
		pid: 200,
		name: "alpha",
		cwd: "D:/work/alpha/sub",
		projectRoot: "D:/work/alpha",
		sessionId: "2000000000000000",
	});
	const otherProjectPeer = record({
		pid: 300,
		name: "beta",
		cwd: "D:/work/beta",
		projectRoot: "D:/work/beta",
		sessionId: "3000000000000000",
	});
	const entries = [selfRecord, sameProjectPeer, otherProjectPeer];

	it("list defaults to the same project and hides other projects with a summary line", async () => {
		const outbox = makeOutbox(async () => ({ msgId: "m-1", to: "x" }));
		const tool = createPeerMessageTool(fakeDeps({ outbox, entries, ownPid: 100 }));
		const text = textOf(await runTool(tool, { action: "list" }, "D:/work/alpha"));
		expect(text).toContain("me");
		expect(text).toContain("alpha");
		expect(text).not.toContain("beta");
		expect(text).toContain('(+1 sessions in other projects hidden; pass scope: "all" to include)');
	});

	it("list with scope:all lists every live session", async () => {
		const outbox = makeOutbox(async () => ({ msgId: "m-1", to: "x" }));
		const tool = createPeerMessageTool(fakeDeps({ outbox, entries, ownPid: 100 }));
		const text = textOf(await runTool(tool, { action: "list", scope: "all" }, "D:/work/alpha"));
		expect(text).toContain("me");
		expect(text).toContain("alpha");
		expect(text).toContain("beta");
		expect(text).not.toContain("other projects hidden");
	});

	it("send to another project is refused by default and names the target project root", async () => {
		let sends = 0;
		const outbox = makeOutbox(async () => {
			sends++;
			return { msgId: "m-1", to: "x" };
		});
		const tool = createPeerMessageTool(fakeDeps({ outbox, entries, ownPid: 100 }));
		const result = await runTool(tool, { to: "beta", message: "hi" }, "D:/work/alpha");
		expect(sends).toBe(0);
		expect(result.isError).toBe(true);
		const text = textOf(result);
		expect(text).toContain("another project");
		expect(text).toContain("D:/work/beta");
		expect(text).toContain('scope: "all"');
	});

	it("send with scope:all reaches the other-project peer", async () => {
		const calls: Array<[string, string, { priority?: string; notifyWhenIdle?: boolean }]> = [];
		const outbox = makeOutbox(async (target, body, opts) => {
			calls.push([target, body, opts]);
			return { msgId: "m-9", to: "pipe:LOCAL\\pi-peer-eee", toName: "beta" };
		});
		const tool = createPeerMessageTool(fakeDeps({ outbox, entries, ownPid: 100 }));
		const result = await runTool(tool, { to: "beta", message: "hi", scope: "all" }, "D:/work/alpha");
		expect(result.isError).toBeUndefined();
		expect(calls).toEqual([["beta", "hi", { priority: undefined, notifyWhenIdle: undefined }]]);
		expect(textOf(result)).toContain('"beta"');
	});

	it("send within the same project is untouched by the scope gate", async () => {
		const outbox = makeOutbox(async () => ({ msgId: "m-3", to: "pipe:LOCAL\\pi-peer-ccc", toName: "alpha" }));
		const tool = createPeerMessageTool(fakeDeps({ outbox, entries, ownPid: 100 }));
		const result = await runTool(tool, { to: "alpha", message: "hi" }, "D:/work/alpha");
		expect(result.isError).toBeUndefined();
		expect(textOf(result)).toContain('"alpha"');
	});

	it("scopes by ctx.cwd when this session is not registered", async () => {
		// No self record in the registry: the fallback path compares the
		// context cwd (normalized) against each candidate's project root.
		const unregistered = [sameProjectPeer, otherProjectPeer];
		const outbox = makeOutbox(async () => ({ msgId: "m-1", to: "x" }));
		const tool = createPeerMessageTool(fakeDeps({ outbox, entries: unregistered, ownPid: 999 }));
		const text = textOf(await runTool(tool, { action: "list" }, "D:/work/alpha/"));
		expect(text).toContain("alpha");
		expect(text).not.toContain("beta");
		expect(text).toContain('(+1 sessions in other projects hidden; pass scope: "all" to include)');

		const sendResult = await runTool(tool, { to: "beta", message: "hi" }, "D:/work/alpha/");
		expect(sendResult.isError).toBe(true);
		expect(textOf(sendResult)).toContain("D:/work/beta");
	});

	it("send to a name that exists nowhere still falls through to the outbox", async () => {
		let sends = 0;
		const outbox = makeOutbox(async () => {
			sends++;
			throw new PeerSendError("not_found", "ghost");
		});
		const tool = createPeerMessageTool(fakeDeps({ outbox, entries, ownPid: 100 }));
		const result = await runTool(tool, { to: "ghost", message: "hi" }, "D:/work/alpha");
		expect(sends).toBe(1);
		expect(result.isError).toBe(true);
		expect(textOf(result)).toContain("No live peer matches");
	});
});

// ---------------------------------------------------------------------------
// Tool: PeerSendError codes
// ---------------------------------------------------------------------------

describe("createPeerMessageTool error mapping", () => {
	const liveEntries = [
		record({ pid: 200, name: "alpha", cwd: "D:/work/alpha", sessionId: "1111111111111111" }),
		record({
			pid: 300,
			name: "beta",
			cwd: "D:/work/beta",
			sessionId: "2222222222222222",
			status: "idle",
			kind: "rpc",
		}),
	];

	function toolForError(error: Error) {
		const outbox = makeOutbox(() => Promise.reject(error));
		return createPeerMessageTool(fakeDeps({ outbox, entries: liveEntries }));
	}

	async function errorText(error: Error): Promise<string> {
		const result = await runTool(toolForError(error), { to: "alpha", message: "hi" });
		expect(result.isError).toBe(true);
		return textOf(result);
	}

	it("not_found lists the live peers", async () => {
		const text = await errorText(new PeerSendError("not_found", "alpha"));
		expect(text).toContain("No live peer matches");
		expect(text).toContain("alpha");
		expect(text).toContain("beta");
	});

	it("ambiguous lists candidate lines", async () => {
		const error = new PeerSendError("ambiguous", "alpha", [
			"alpha [0aa1b2c]  busy  interactive  D:/work/alpha  (11111111)",
			"alpha [0aa1b2c]  idle  rpc  D:/work/two  (33333333)",
		]);
		const text = await errorText(error);
		expect(text).toContain("disambiguate");
		expect(text).toContain("alpha [0aa1b2c]  busy");
		expect(text).toContain("alpha [0aa1b2c]  idle");
	});

	it("maps simple codes to actionable text", async () => {
		const cases: Array<[string, string]> = [
			["offline", "offline"],
			["no_key", "key"],
			["timeout", "Timed out"],
			["connect_failed", "connect"],
			["sender_paced", "pacing"],
			["message_too_large", "1 MiB"],
			["invalid_target", "Invalid target"],
		];
		for (const [code, needle] of cases) {
			const text = await errorText(new PeerSendError(code as never, "the reason"));
			expect(text.length, `code ${code} produced empty text`).toBeGreaterThan(0);
			expect(text).toContain(needle);
		}
	});

	it("rethrows non-PeerSendError exceptions", async () => {
		const outbox = makeOutbox(() => Promise.reject(new TypeError("boom")));
		const tool = createPeerMessageTool(fakeDeps({ outbox }));
		await expect(runTool(tool, { to: "alpha", message: "hi" })).rejects.toThrow(TypeError);
		await expect(runTool(tool, { to: "alpha", message: "hi" })).rejects.toThrow("boom");
	});
});

// ---------------------------------------------------------------------------
// /peers command
// ---------------------------------------------------------------------------

describe("createPeersCommand", () => {
	it("lists live peers with name, ref, status, kind, cwd, and session prefix", async () => {
		const command = createPeersCommand({
			outbox: makeOutbox(async () => ({ msgId: "unused", to: "unused" })),
			registry: makeRegistry([
				record({
					pid: 200,
					name: "alpha",
					status: "busy",
					kind: "interactive",
					cwd: "D:/work/alpha",
					sessionId: "11111111aaaa",
				}),
				record({
					pid: 300,
					name: "beta",
					status: "idle",
					kind: "rpc",
					cwd: "D:/work/beta",
					sessionId: "22222222bbbb",
				}),
			]),
			identity: fakeIdentity(100),
		});
		const messages: Array<{ message: string; type?: string }> = [];
		await command.handler("", {
			ui: { notify: (message: string, type?: "info" | "warning" | "error") => messages.push({ message, type }) },
		} as unknown as PeersCommandContext);
		expect(messages).toHaveLength(1);
		expect(messages[0].type).toBe("info");
		const text = messages[0].message;
		expect(text).toContain("alpha [");
		expect(text).toContain("busy");
		expect(text).toContain("interactive");
		expect(text).toContain("D:/work/alpha");
		expect(text).toContain("(11111111");
		expect(text).toContain("beta [");
		expect(text).toContain("idle");
		expect(text).toContain("rpc");
		expect(text).toContain("(22222222");
	});

	it("marks this session's row", async () => {
		const command = createPeersCommand({
			outbox: makeOutbox(async () => ({ msgId: "unused", to: "unused" })),
			registry: makeRegistry([
				record({ pid: 100, name: "me", sessionId: "12345678", cwd: "D:/work/me" }),
				record({ pid: 200, name: "alpha", sessionId: "11111111", cwd: "D:/work/alpha" }),
			]),
			identity: fakeIdentity(100),
		});
		const messages: Array<{ message: string; type?: string }> = [];
		await command.handler("", {
			ui: { notify: (message: string, type?: "info" | "warning" | "error") => messages.push({ message, type }) },
		} as unknown as PeersCommandContext);
		const text = messages[0].message;
		expect(text).toContain("(this session)");
		expect(text).toContain("me [");
	});

	it("says there are no other live sessions when alone", async () => {
		const command = createPeersCommand({
			outbox: makeOutbox(async () => ({ msgId: "unused", to: "unused" })),
			registry: makeRegistry([record({ pid: 100, name: "me" })]),
			identity: fakeIdentity(100),
		});
		const messages: string[] = [];
		await command.handler("", {
			ui: { notify: (message: string) => messages.push(message) },
		} as unknown as PeersCommandContext);
		expect(messages).toHaveLength(1);
		expect(messages[0]).toContain("No other live pi sessions");
	});

	it("says there are no other live sessions when the registry is empty", async () => {
		const command = createPeersCommand({
			outbox: makeOutbox(async () => ({ msgId: "unused", to: "unused" })),
			registry: makeRegistry([]),
			identity: fakeIdentity(100),
		});
		const messages: string[] = [];
		await command.handler("", {
			ui: { notify: (message: string) => messages.push(message) },
		} as unknown as PeersCommandContext);
		expect(messages).toHaveLength(1);
		expect(messages[0]).toContain("No other live pi sessions");
	});
});
