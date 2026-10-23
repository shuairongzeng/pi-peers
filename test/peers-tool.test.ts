import { describe, expect, it } from "vitest";
import type {
	PeerControlFrame,
	PeerIdentity,
	PeerOutbox,
	PeerRecord,
	PeerRegistry,
	PeerRegistryEntry,
	PeerSpawner,
	PeerSpawnOptions,
	PeerSpawnOutcome,
	PeerToolDeps,
	PeerTransport,
	PeersCommandContext,
	PeersToolDefinition,
} from "../src/types.ts";
import { createResolveLiveTarget } from "../src/outbox.ts";
import { PeerSendError, PeerSpawnError } from "../src/types.ts";
import type { PeerLiveTargetResolution } from "../src/types.ts";
import { createPeerMessageTool, createPeerSpawnTool, createPeersCommand, peerLine } from "../src/tool.ts";

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
	// Wire address (pipe:…) selects by socket path, like the real registry.
	const asPath = name.startsWith("pipe:") ? `\\\\.\\pipe\\${name.slice(5)}` : name;
	const byPath = entries.filter(({ record }) => record.socketPath === asPath);
	if (byPath.length > 0) {
		return byPath.length > 1 ? { kind: "ambiguous", candidates: byPath } : { kind: "unique", entry: byPath[0] };
	}
	const matches = entries.filter(({ record }) => record.name === name);
	if (matches.length === 0) return { kind: "none" };
	if (matches.length > 1) return { kind: "ambiguous", candidates: matches };
	return { kind: "unique", entry: matches[0] };
}

function makeRegistry(entries: PeerRecord[], pidDomain = "win32:box"): PeerRegistry {
	const list = entries.map((rec) => ({ record: rec, live: true }));
	return {
		publish: () => {},
		update: () => {},
		unpublish: () => {},
		list: () => list,
		readRecord: (pid: number) => list.find((e) => e.record.pid === pid),
		resolve: (name: string) => resolveInImpl(name, list),
		resolveIn: (name: string, scoped: PeerRegistryEntry[]) => resolveInImpl(name, scoped),
		refOf: (rec: PeerRecord) => `${(rec.pid % 256).toString(16).padStart(2, "0")}a1b2c`,
		isAlive: () => true,
		readProcStart: () => undefined,
		pidDomain: () => pidDomain,
	} as unknown as PeerRegistry;
}

/** Outbox fake: wraps a plain send impl and logs sendControl calls. The
 *  optional resolver mimics what createPeerOutbox attaches at runtime. */
function makeOutbox(
	sendImpl: (
		target: string,
		body: string,
		opts: { priority?: "next" | "now"; notifyWhenIdle?: boolean },
	) => Promise<never | { msgId: string; to: string; toName?: string }>,
	resolveLiveTarget?: (target: string) => PeerLiveTargetResolution,
): PeerOutbox & { controlCalls: Array<{ address: string; frame: PeerControlFrame }> } {
	const controlCalls: Array<{ address: string; frame: PeerControlFrame }> = [];
	return {
		send: sendImpl,
		async sendControl(address, frame) {
			controlCalls.push({ address, frame });
		},
		receiptOf: () => undefined,
		handleReceipt: () => {},
		registerOutbound: () => {},
		expectsReceipt: () => false,
		registerIdleNonce: () => {},
		consumeIdleNonce: () => false,
		...(resolveLiveTarget !== undefined ? { resolveLiveTarget } : {}),
		controlCalls,
	} as PeerOutbox & { controlCalls: Array<{ address: string; frame: PeerControlFrame }> };
}

/** Same ref format as makeRegistry's refOf (candidate lines use it). */
function refOf(rec: PeerRecord): string {
	return `${(rec.pid % 256).toString(16).padStart(2, "0")}a1b2c`;
}
/** Minimal fake transport whose addressFromPath logs every call (Windows pipe shape). */
function makeLadderTransport(): Pick<PeerTransport, "addressFromPath"> & { addressFromPathCalls: string[] } {
	const addressFromPathCalls: string[] = [];
	return {
		addressFromPath(path: string) {
			addressFromPathCalls.push(path);
			return path.startsWith("\\\\.\\pipe\\") ? `pipe:${path.slice("\\\\.\\pipe\\".length)}` : undefined;
		},
		addressFromPathCalls,
	};
}

/** Build a resolveLiveTarget over the given registry + transport, like the real outbox does. */
function makeResolver(
	registry: PeerRegistry,
	transport: Pick<PeerTransport, "addressFromPath">,
): (target: string) => PeerLiveTargetResolution {
	const stub = { registry, transport } as unknown as Parameters<typeof createResolveLiveTarget>[0];
	return createResolveLiveTarget(stub);
}

/** Spawner fake: records every call and resolves a fixed outcome. */
function makeSpawner(
	outcome: PeerSpawnOutcome = { pid: 12345, name: "researcher", ref: "ab12cd", taskSent: false },
): PeerSpawner & { calls: PeerSpawnOptions[] } {
	const calls: PeerSpawnOptions[] = [];
	const spawner = (options: PeerSpawnOptions): Promise<PeerSpawnOutcome> => {
		calls.push(options);
		return Promise.resolve(outcome);
	};
	return Object.assign(spawner, { calls });
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

function fakeDeps(
	opts: {
		outbox: PeerOutbox;
		entries?: PeerRecord[];
		ownPid?: number;
		spawner?: PeerSpawner;
		pidDomain?: string;
	},
): PeerToolDeps {
	const ownPid = opts.ownPid ?? 100;
	const registry = makeRegistry(opts.entries ?? [], opts.pidDomain);
	return {
		outbox: opts.outbox,
		registry,
		identity: fakeIdentity(ownPid),
		spawner: opts.spawner ?? makeSpawner(),
	};
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
		expect(text).toContain("Success is silent");
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

	it("action=list includes ctx N% when the record reports a numeric percentage", async () => {
		const outbox = makeOutbox(async () => ({ msgId: "m-1", to: "x" }));
		const tool = createPeerMessageTool(
			fakeDeps({
				outbox,
				entries: [
					record({ pid: 100, name: "self" }),
					record({ pid: 200, name: "alpha", contextUsage: { percent: 47.6 } }),
				],
				ownPid: 100,
			}),
		);
		const text = textOf(await runTool(tool, { action: "list" }));
		expect(text).toContain("alpha");
		expect(text).toContain("ctx 48%");
	});

	it("action=list omits ctx when no percentage is reported", async () => {
		const outbox = makeOutbox(async () => ({ msgId: "m-1", to: "x" }));
		const tool = createPeerMessageTool(
			fakeDeps({
				outbox,
				entries: [
					record({ pid: 100, name: "self" }),
					record({ pid: 200, name: "alpha", contextUsage: { percent: null } }),
				],
				ownPid: 100,
			}),
		);
		const text = textOf(await runTool(tool, { action: "list" }));
		expect(text).toContain("alpha");
		expect(text).not.toContain("ctx ");
		expect(text).not.toContain("%");
	});

	it("peerLine includes ctx% when present and omits it when absent", () => {
		const refOf = (rec: PeerRecord): string => `ref-${rec.name}`;
		const withUsage = record({ pid: 200, name: "alpha", contextUsage: { percent: 42.4 } });
		expect(peerLine(withUsage, false, refOf)).toBe(
			"alpha [ref-alpha]  busy  ctx 42%  interactive  D:/work/alpha  (abcdef12)",
		);
		const noUsage = record({ pid: 300, name: "beta" });
		expect(peerLine(noUsage, true, refOf)).toBe(
			"beta [ref-beta]  busy  interactive  D:/work/alpha  (abcdef12)  (this session)",
		);
		const nullUsage = record({ pid: 400, name: "gamma", contextUsage: { percent: null } });
		expect(peerLine(nullUsage, false, refOf)).not.toContain("ctx ");
	});

	it("action=list with no live peers prints the unified empty wording", async () => {
		const outbox = makeOutbox(async () => ({ msgId: "m-1", to: "x" }));
		const tool = createPeerMessageTool(fakeDeps({ outbox, entries: [], ownPid: 100 }));
		const text = textOf(await runTool(tool, { action: "list" }, "D:/work/alpha"));
		expect(text).toBe("No live peers in this project.");
	});

	it("action=list with only other-project peers appends the other-project count", async () => {
		const outbox = makeOutbox(async () => ({ msgId: "m-1", to: "x" }));
		const other = record({
			pid: 300,
			name: "beta",
			cwd: "D:/work/beta",
			projectRoot: "D:/work/beta",
			sessionId: "3000000000000000",
		});
		const tool = createPeerMessageTool(fakeDeps({ outbox, entries: [other], ownPid: 999 }));
		const text = textOf(await runTool(tool, { action: "list" }, "D:/work/alpha"));
		expect(text).toBe("No live peers in this project. (+1 in other projects)");
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
		expect(text).toContain("(+1 in other projects)");
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
		expect(text).toContain("(+1 in other projects)");

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
		expect(textOf(result)).toContain("ghost");
	});
});

describe("createPeerMessageTool shared project rule (either-side win32 folding)", () => {
	it("case-folds a foreign-platform record's path when the local pidDomain is win32", async () => {
		// No self record (own pid 999 is not registered): selfCwd stands in as
		// our project root. The candidate is POSIX-domain with an UPPERCASE
		// path; the local pidDomain is win32, so the either-side rule folds
		// case and "D:/WORK/ALPHA" still matches D:/work/alpha.
		const caseVariants = [
			record({
				pid: 210,
				name: "gamma",
				cwd: "D:/WORK/ALPHA/sub",
				projectRoot: "D:/WORK/ALPHA",
				sessionId: "2100000000000000",
				pidDomain: "linux:host",
			}),
		];
		const outbox = makeOutbox(async () => ({ msgId: "m-1", to: "x" }));
		const tool = createPeerMessageTool(fakeDeps({ outbox, entries: caseVariants, ownPid: 999 }));
		const text = textOf(await runTool(tool, { action: "list" }, "D:/work/alpha"));
		expect(text).toContain("gamma");
		expect(text).not.toContain("(+1 in other projects)");
	});
});

// ---------------------------------------------------------------------------
// Tool: batch sends (to as an array)
// ---------------------------------------------------------------------------

describe("createPeerMessageTool batch sends", () => {
	const selfRecord = record({
		pid: 100,
		name: "me",
		cwd: "D:/work/alpha",
		projectRoot: "D:/work/alpha",
		sessionId: "1000000000000000",
	});
	const alpha = record({
		pid: 200,
		name: "alpha",
		cwd: "D:/work/alpha/sub",
		projectRoot: "D:/work/alpha",
		sessionId: "2000000000000000",
	});
	const beta = record({
		pid: 300,
		name: "beta",
		cwd: "D:/work/alpha/deep",
		projectRoot: "D:/work/alpha",
		sessionId: "3000000000000000",
	});
	const gamma = record({
		pid: 400,
		name: "gamma",
		cwd: "D:/work/gamma",
		projectRoot: "D:/work/gamma",
		sessionId: "4000000000000000",
	});

	it("batch-sends to each target and summarizes one line per target", async () => {
		const calls: Array<[string, string, { priority?: string; notifyWhenIdle?: boolean }]> = [];
		const outbox = makeOutbox(async (target, body, opts) => {
			calls.push([target, body, opts]);
			if (target === "gamma") {
				// gamma is offline: a dead process is the classic batch failure.
				throw new PeerSendError("offline", 'peer "gamma" (pid 400) has a registry record but its process is dead');
			}
			return { msgId: `msg-${target}-1234567890`, to: `pipe:LOCAL\\pi-peer-${target}`, toName: target };
		});
		const tool = createPeerMessageTool(fakeDeps({ outbox, entries: [selfRecord, alpha, beta], ownPid: 100 }));
		const result = await runTool(tool, { to: ["alpha", "beta", "gamma"], message: "hi" }, "D:/work/alpha");

		expect(calls).toHaveLength(3);
		expect(calls.map((c) => c[0])).toEqual(["alpha", "beta", "gamma"]);
		for (const [, body, opts] of calls) {
			expect(body).toBe("hi");
			expect(opts).toEqual({ priority: undefined, notifyWhenIdle: undefined });
		}
		// The offline peer (gamma) is the classic one-target failure; alpha and
		// beta succeed, so the batch is a partial success, not an error:
		// isError stays unset (the default falsy) so the agent reads the lines.
		expect(result.isError).toBeUndefined();
		const text = textOf(result);
		expect(text).toContain("✓ alpha — sent (msg_id msg-alph…)");
		expect(text).toContain("✓ beta — sent (msg_id msg-beta…)");
		expect(text).toContain("✗ gamma:");
		expect(text).toContain("2 of 3 targets; 1 failed (1 offline).");
		const details = result.details as Array<{ to: string; ok: boolean; msgId?: string; error?: string; code?: string }>;
		expect(details).toEqual([
			{ to: "alpha", ok: true, msgId: "msg-alpha-1234567890" },
			{ to: "beta", ok: true, msgId: "msg-beta-1234567890" },
			{
				to: "gamma",
				ok: false,
				error: 'peer "gamma" (pid 400) has a registry record but its process is dead',
				code: "offline",
			},
		]);
	});

	it("all targets failing sets isError", async () => {
		const outbox = makeOutbox(async (target) => {
			throw new PeerSendError("offline", `peer "${target}" is dead`);
		});
		const tool = createPeerMessageTool(fakeDeps({ outbox, entries: [selfRecord, alpha], ownPid: 100 }));
		const result = await runTool(tool, { to: ["alpha", "beta"], message: "hi" }, "D:/work/alpha");
		expect(result.isError).toBe(true);
		const text = textOf(result);
		expect(text).toContain("✗ alpha:");
		expect(text).toContain("✗ beta:");
	});

	it("rejects an array combined with notifyWhenIdle", async () => {
		let sends = 0;
		const outbox = makeOutbox(async () => {
			sends++;
			return { msgId: "m-1", to: "x" };
		});
		const tool = createPeerMessageTool(fakeDeps({ outbox, entries: [selfRecord, alpha], ownPid: 100 }));
		const result = await runTool(
			tool,
			{ to: ["alpha", "beta"], message: "hi", notifyWhenIdle: true },
			"D:/work/alpha",
		);
		expect(sends).toBe(0);
		expect(result.isError).toBe(true);
		const text = textOf(result);
		expect(text).toContain("Invalid target");
		expect(text).toContain("notifyWhenIdle");
	});

	it("a cross-project target in a batch fails alone while the rest succeed", async () => {
		const calls: string[] = [];
		const outbox = makeOutbox(async (target) => {
			calls.push(target);
			return { msgId: `msg-${target}-1234567890`, to: `pipe:LOCAL\\pi-peer-${target}`, toName: target };
		});
		const tool = createPeerMessageTool(fakeDeps({ outbox, entries: [selfRecord, alpha, gamma], ownPid: 100 }));
		const result = await runTool(tool, { to: ["alpha", "gamma"], message: "hi" }, "D:/work/alpha");

		// gamma lives in D:/work/gamma: the scope gate refuses it before the
		// outbox, while alpha goes through.
		expect(calls).toEqual(["alpha"]);
		expect(result.isError).toBeUndefined();
		const text = textOf(result);
		expect(text).toContain("✓ alpha — sent (msg_id msg-alph…)");
		expect(text).toContain("✗ gamma:");
		expect(text).toContain("D:/work/gamma");
		expect(text).toContain('scope: "all"');
		const details = result.details as Array<{ to: string; ok: boolean; code?: string }>;
		expect(details[0]).toEqual({ to: "alpha", ok: true, msgId: "msg-alpha-1234567890" });
		expect(details[1].to).toBe("gamma");
		expect(details[1].code).toBe("invalid_target");
	});

	it("batch header groups failures by error code", async () => {
		const outbox = makeOutbox(async (target) => {
			if (target === "alpha") {
				return { msgId: `msg-${target}-1234567890`, to: `pipe:LOCAL\\pi-peer-${target}`, toName: target };
			}
			if (target === "beta") {
				throw new PeerSendError("offline", 'peer "beta" is dead');
			}
			throw new PeerSendError("invalid_target", "bad target");
		});
		// Three sends with two failure codes: the header must count each code
		// once, in first-seen order.
		const tool = createPeerMessageTool(fakeDeps({ outbox, entries: [selfRecord, alpha, beta, gamma], ownPid: 100 }));
		const result = await runTool(tool, { to: ["alpha", "beta", "ghost"], message: "hi" }, "D:/work/alpha");
		expect(result.isError).toBeUndefined();
		const text = textOf(result);
		expect(text).toContain("1 of 3 targets; 2 failed (1 offline, 1 invalid_target).");
	});

	it("batch with scope:all reaches a cross-project target", async () => {
		const calls: string[] = [];
		const outbox = makeOutbox(async (target) => {
			calls.push(target);
			return { msgId: `msg-${target}-1234567890`, to: `pipe:LOCAL\\pi-peer-${target}`, toName: target };
		});
		const tool = createPeerMessageTool(fakeDeps({ outbox, entries: [selfRecord, alpha, gamma], ownPid: 100 }));
		const result = await runTool(tool, { to: ["alpha", "gamma"], message: "hi", scope: "all" }, "D:/work/alpha");
		expect(calls).toEqual(["alpha", "gamma"]);
		expect(result.isError).toBeUndefined();
		const text = textOf(result);
		expect(text).toContain("✓ alpha");
		expect(text).toContain("✓ gamma");
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

	it("not_found renders the outbox message once (no double wrap) and lists same-project peers only", async () => {
		const text = await errorText(
			new PeerSendError(
				"not_found",
				'no live peer matches "alpha": the peer may not be running or may have exited. Socket addresses change on every start, so never reuse a stale address',
			),
		);
		// Single wrap: the message appears exactly once, not re-prefixed inside
		// another 'No live peer matches "…"' envelope.
		expect(text.match(/no live peer matches/g)).toHaveLength(1);
		expect(text).toContain("Live peers:");
		expect(text).toContain("alpha");
		// beta lives in another project: a scoped error must not leak its name.
		expect(text).not.toContain("beta");
		expect(text).toContain("(+1 in other projects)");
	});

	it("not_found with scope:all lists every live peer", async () => {
		const outbox = makeOutbox(() =>
			Promise.reject(new PeerSendError("not_found", 'no live peer matches "alpha"')),
		);
		const tool = createPeerMessageTool(fakeDeps({ outbox, entries: liveEntries }));
		const text = textOf(await runTool(tool, { to: "alpha", message: "hi", scope: "all" }, "D:/work/alpha"));
		expect(text).toContain("alpha");
		expect(text).toContain("beta");
	});

	it("ambiguous lists candidate lines after the outbox message", async () => {
		const error = new PeerSendError("ambiguous", 'several live peers match "alpha"', [
			"alpha [0aa1b2c] busy D:/work/alpha",
			"alpha [0aa1b2c] idle D:/work/two",
		]);
		const text = await errorText(error);
		expect(text).toContain('several live peers match "alpha"');
		expect(text).toContain("alpha [0aa1b2c] busy");
		expect(text).toContain("alpha [0aa1b2c] idle");
	});

	it("maps simple codes to the outbox message verbatim (no re-prefixing)", async () => {
		// The outbox messages are complete sentences: cause + remedy. The tool
		// renders them as-is; the needles assert each code's message survives.
		const cases: Array<[string, string]> = [
			["offline", "process is dead"],
			["no_key", "has not published a key"],
			["timeout", "timed out"],
			["connect_failed", "could not connect"],
			["sender_paced", "pacing predicts"],
			["message_too_large", "over the wire cap"],
			["invalid_target", "unrecognized"],
		];
		for (const [code, needle] of cases) {
			const text = await errorText(new PeerSendError(code as never, `outbox says: ${needle}`));
			expect(text.length, `code ${code} produced empty text`).toBeGreaterThan(0);
			expect(text).toContain(needle);
			// Exactly one layer: the rendered text IS the message.
			expect(text).toBe(`outbox says: ${needle}`);
		}
	});

	it("invalid_target for a cross-project refusal tells the user to widen scope", async () => {
		const text = await errorText(
			new PeerSendError(
				"invalid_target",
				'target "beta" belongs to another project (D:/work/beta); pass scope: "all" to message across projects',
			),
		);
		expect(text).toContain('Resend with scope: "all" (only if the user asked to reach that session).');
		expect(text).toContain("belongs to another project");
	});

	it("invalid_target for other causes renders the message verbatim", async () => {
		const text = await errorText(new PeerSendError("invalid_target", "unrecognized peer socket path: bad"));
		expect(text).toBe("unrecognized peer socket path: bad");
	});

	it("no_key renders the outbox message (which carries the retry guidance) verbatim", async () => {
		const text = await errorText(
			new PeerSendError(
				"no_key",
				'no auth key for peer "alpha" at path: the peer has not published a key; wait for it to re-register, then retry',
			),
		);
		expect(text).toContain("wait for it to re-register, then retry");
		expect(text).not.toContain("~/.pi/agent/peers/");
	});

	it("offline renders the outbox message once (no 'run /peers' duplicate of its own guidance)", async () => {
		const message =
			'peer "gamma" (pid 400) has a registry record but its process is dead; it may have exited without cleaning up. Run the peers listing again after a moment';
		const text = await errorText(new PeerSendError("offline", message));
		expect(text).toBe(message);
	});

	it("rethrows non-PeerSendError exceptions", async () => {
		const outbox = makeOutbox(() => Promise.reject(new TypeError("boom")));
		const tool = createPeerMessageTool(fakeDeps({ outbox }));
		await expect(runTool(tool, { to: "alpha", message: "hi" })).rejects.toThrow(TypeError);
		await expect(runTool(tool, { to: "alpha", message: "hi" })).rejects.toThrow("boom");
	});
});

// ---------------------------------------------------------------------------
// Tool: spawn_peer
// ---------------------------------------------------------------------------

describe("createPeerSpawnTool", () => {
	it("returns name, ref, and pid and calls the spawner with the exact options", async () => {
		const spawner = makeSpawner({ pid: 12345, name: "researcher", ref: "ab12cd", taskSent: false });
		const tool = createPeerSpawnTool(fakeDeps({ outbox: makeOutbox(async () => ({ msgId: "m", to: "x" })), spawner }));
		const result = await runTool(tool, { name: "researcher", headless: true, ephemeral: true });
		expect(result.isError).toBeUndefined();
		expect(spawner.calls).toEqual([{ name: "researcher", task: undefined, headless: true, ephemeral: true }]);
		const text = textOf(result);
		expect(text).toContain('"researcher"');
		expect(text).toContain("[ab12cd]");
		expect(text).toContain("pid 12345");
		expect(text).toContain("No task given.");
	});

	it("says the task was delivered when the spawner sent it", async () => {
		const spawner = makeSpawner({ pid: 1, name: "worker", ref: "ff00aa", taskSent: true });
		const tool = createPeerSpawnTool(fakeDeps({ outbox: makeOutbox(async () => ({ msgId: "m", to: "x" })), spawner }));
		const result = await runTool(tool, { name: "worker", task: "do the thing" });
		expect(spawner.calls).toEqual([{ name: "worker", task: "do the thing", headless: undefined, ephemeral: undefined }]);
		expect(textOf(result)).toContain("Task delivered.");
	});

	it("task-not-delivered result tells the user to send it with send_peer_message", async () => {
		const spawner = makeSpawner({ pid: 77, name: "worker", ref: "ff00aa", taskSent: false });
		const tool = createPeerSpawnTool(fakeDeps({ outbox: makeOutbox(async () => ({ msgId: "m", to: "x" })), spawner }));
		const result = await runTool(tool, { name: "worker", task: "do the thing" });
		expect(result.isError).toBeUndefined();
		const text = textOf(result);
		expect(text).toContain('Spawned peer "worker" [ff00aa] (pid 77)');
		expect(text).toContain("The peer is online but the task was not delivered (startup race)");
		expect(text).toContain('send it now with send_peer_message to "worker"');
	});

	it("description contains the Task-NOT-delivered recovery bullet", () => {
		const tool = createPeerSpawnTool(fakeDeps({ outbox: makeOutbox(async () => ({ msgId: "m", to: "x" })) }));
		expect(tool.description).toContain(
			'If the result says "Task NOT delivered", the peer is online and addressable by name; send the task with send_peer_message.',
		);
	});

	it("says no task was given when none was passed", async () => {
		const spawner = makeSpawner();
		const tool = createPeerSpawnTool(fakeDeps({ outbox: makeOutbox(async () => ({ msgId: "m", to: "x" })), spawner }));
		const result = await runTool(tool, {});
		expect(spawner.calls).toEqual([{ name: undefined, task: undefined, headless: undefined, ephemeral: undefined }]);
		expect(textOf(result)).toContain("No task given.");
	});

	it("maps PeerSpawnError to an isError result with the message", async () => {
		const calls: PeerSpawnOptions[] = [];
		const spawner: PeerSpawner = async (options) => {
			calls.push(options);
			throw new PeerSpawnError("register_timeout", 'spawned "x" but it did not register within 30s');
		};
		const tool = createPeerSpawnTool(fakeDeps({ outbox: makeOutbox(async () => ({ msgId: "m", to: "x" })), spawner }));
		const result = await runTool(tool, { name: "x" });
		expect(calls).toEqual([{ name: "x", task: undefined, headless: undefined, ephemeral: undefined }]);
		expect(result.isError).toBe(true);
		const text = textOf(result);
		expect(text).toContain("Spawn failed:");
		expect(text).toContain("register");
	});
});

// ---------------------------------------------------------------------------
// Tool: action "shutdown"
// ---------------------------------------------------------------------------

describe("createPeerMessageTool action=shutdown", () => {
	const selfRecord = record({
		pid: 100,
		name: "me",
		cwd: "D:/work/alpha",
		projectRoot: "D:/work/alpha",
		sessionId: "1000000000000000",
	});
	const alpha = record({
		pid: 200,
		name: "alpha",
		cwd: "D:/work/alpha/sub",
		projectRoot: "D:/work/alpha",
		sessionId: "2000000000000000",
		socketPath: `\\\\.\\pipe\\LOCAL\\pi-peer-${"b".repeat(32)}`,
	});
	const alpha2 = record({
		pid: 250,
		name: "alpha",
		cwd: "D:/work/alpha/deep",
		projectRoot: "D:/work/alpha",
		sessionId: "2500000000000000",
		socketPath: `\\\\.\\pipe\\LOCAL\\pi-peer-${"c".repeat(32)}`,
	});
	const beta = record({
		pid: 300,
		name: "beta",
		cwd: "D:/work/beta",
		projectRoot: "D:/work/beta",
		sessionId: "3000000000000000",
	});

	/** Outbox fake with the shared ladder installed, like createPeerOutbox does. */
	function makeShutdownOutbox(entries: PeerRecord[]) {
		const transport = makeLadderTransport();
		const registry = makeRegistry(entries);
		const resolver = makeResolver(registry, transport);
		const outbox = makeOutbox(async () => ({ msgId: "m", to: "x" }), resolver);
		return { outbox, transport, registry };
	}

	it("sends one shutdown frame to the peer's wire address with from and reason", async () => {
		const { outbox } = makeShutdownOutbox([selfRecord, alpha]);
		const tool = createPeerMessageTool(fakeDeps({ outbox, entries: [selfRecord, alpha], ownPid: 100 }));
		const result = await runTool(tool, { action: "shutdown", to: "alpha", reason: "work done" }, "D:/work/alpha");
		expect(result.isError).toBeUndefined();
		expect(outbox.controlCalls).toHaveLength(1);
		const { address, frame } = outbox.controlCalls[0]!;
		// Wire address form, derived from the record's connect path by the
		// shared ladder inside the outbox (not by tool.ts directly).
		expect(address).toBe(`pipe:LOCAL\\pi-peer-${"b".repeat(32)}`);
		expect(frame.type).toBe("control");
		expect(frame.action).toBe("shutdown");
		if (frame.type === "control" && frame.action === "shutdown") {
			expect(frame.from).toBe(`pipe:LOCAL\\pi-peer-${"f".repeat(32)}`);
			expect(frame.fromName).toBe("me");
			expect(frame.reason).toBe("work done");
		}
		expect(typeof frame.msg_id).toBe("string");
		expect(textOf(result)).toContain('Shutdown requested for "alpha"');
		expect(textOf(result)).toContain("accept policy");
	});

	it("shutdown resolution goes through the outbox's transport.addressFromPath", async () => {
		const { outbox, transport } = makeShutdownOutbox([selfRecord, alpha]);
		const tool = createPeerMessageTool(fakeDeps({ outbox, entries: [selfRecord, alpha], ownPid: 100 }));
		await runTool(tool, { action: "shutdown", to: "alpha" }, "D:/work/alpha");
		// The shared ladder (inside the outbox) performed the connect-path →
		// wire-address conversion; tool.ts has no addressFromPath import left.
		expect(transport.addressFromPathCalls).toEqual([alpha.socketPath]);
	});

	it("shutdown to a wire address resolves through the shared ladder", async () => {
		const { outbox } = makeShutdownOutbox([selfRecord, alpha]);
		const tool = createPeerMessageTool(fakeDeps({ outbox, entries: [selfRecord, alpha], ownPid: 100 }));
		const result = await runTool(
			tool,
			{ action: "shutdown", to: `pipe:LOCAL\\pi-peer-${"b".repeat(32)}` },
			"D:/work/alpha",
		);
		expect(result.isError).toBeUndefined();
		expect(outbox.controlCalls).toHaveLength(1);
		expect(outbox.controlCalls[0]!.address).toBe(`pipe:LOCAL\\pi-peer-${"b".repeat(32)}`);
		expect(textOf(result)).toContain('Shutdown requested for "alpha"');
	});

	it("shutdown to an ambiguous name lists candidates in the shared format", async () => {
		const { outbox } = makeShutdownOutbox([selfRecord, alpha, alpha2]);
		const tool = createPeerMessageTool(fakeDeps({ outbox, entries: [selfRecord, alpha, alpha2], ownPid: 100 }));
		const result = await runTool(tool, { action: "shutdown", to: "alpha" }, "D:/work/alpha");
		expect(outbox.controlCalls).toHaveLength(0);
		expect(result.isError).toBe(true);
		const text = textOf(result);
		expect(text).toContain('several live peers match "alpha"');
		// candidateLine format (name [ref] status cwd), shared with the outbox.
		expect(text).toContain(`alpha [${refOf(alpha)}] busy D:/work/alpha/sub`);
		expect(text).toContain(`alpha [${refOf(alpha2)}] busy D:/work/alpha/deep`);
	});

	it("shutdown to an unknown name is a not_found error (single wrap)", async () => {
		const { outbox } = makeShutdownOutbox([selfRecord]);
		const tool = createPeerMessageTool(fakeDeps({ outbox, entries: [selfRecord], ownPid: 100 }));
		const result = await runTool(tool, { action: "shutdown", to: "ghost" }, "D:/work/alpha");
		expect(outbox.controlCalls).toHaveLength(0);
		expect(result.isError).toBe(true);
		const text = textOf(result);
		expect(text).toContain('no live peer matches "ghost"');
		expect(text.match(/no live peer matches/g)).toHaveLength(1);
	});

	it("shutdown to a cross-project peer is refused under the default scope", async () => {
		const { outbox } = makeShutdownOutbox([selfRecord, beta]);
		const tool = createPeerMessageTool(fakeDeps({ outbox, entries: [selfRecord, beta], ownPid: 100 }));
		const result = await runTool(tool, { action: "shutdown", to: "beta" }, "D:/work/alpha");
		expect(outbox.controlCalls).toHaveLength(0);
		expect(result.isError).toBe(true);
		const text = textOf(result);
		expect(text).toContain("another project");
		expect(text).toContain('scope: "all"');
	});

	it("shutdown refuses a peer speaking a newer wire protocol, naming both versions", async () => {
		const alphaV2 = { ...alpha, peerProtocol: 2 };
		const { outbox } = makeShutdownOutbox([selfRecord, alphaV2]);
		const tool = createPeerMessageTool(fakeDeps({ outbox, entries: [selfRecord, alphaV2], ownPid: 100 }));
		const result = await runTool(tool, { action: "shutdown", to: "alpha" }, "D:/work/alpha");
		expect(outbox.controlCalls).toHaveLength(0);
		expect(result.isError).toBe(true);
		const text = textOf(result);
		// The message names BOTH versions: the peer's and ours, and the fix.
		expect(text).toBe(
			'peer "alpha" speaks wire protocol v2, this plugin speaks v1 — upgrade the pi-peers plugin',
		);
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
			spawner: makeSpawner(),
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
			spawner: makeSpawner(),
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
			spawner: makeSpawner(),
		});
		const messages: string[] = [];
		await command.handler("", {
			ui: { notify: (message: string) => messages.push(message) },
		} as unknown as PeersCommandContext);
		expect(messages).toHaveLength(1);
		expect(messages[0]).toContain("No live peers in this project.");
	});

	it("says there are no other live sessions when the registry is empty", async () => {
		const command = createPeersCommand({
			outbox: makeOutbox(async () => ({ msgId: "unused", to: "unused" })),
			registry: makeRegistry([]),
			identity: fakeIdentity(100),
			spawner: makeSpawner(),
		});
		const messages: string[] = [];
		await command.handler("", {
			ui: { notify: (message: string) => messages.push(message) },
		} as unknown as PeersCommandContext);
		expect(messages).toHaveLength(1);
		expect(messages[0]).toContain("No live peers in this project.");
	});
});
