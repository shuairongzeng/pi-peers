/**
 * peers-coverage-index.test.ts — coverage closure for src/index.ts host
 * wiring, driven by the fake-PeersHostAPI pattern of wiring.test.ts:
 *
 * - publishRecord name precedence (env / session name / derived), throwing
 *   getContextUsage hook, and ctx.mode → kind mapping
 * - session_shutdown non-quit reasons republish via session_start
 * - before_agent_start malformed event shapes
 * - session_info_changed rename ladder (history, cap, no-record, nameless)
 * - turn_start usage drift thresholds and a throwing usage hook
 * - acceptConnection guard over the real listener: malformed auth line,
 *   unknown token, malformed frame, wrong msgV, unknown control action,
 *   and the closing gate after quit
 * - acceptPolicy env ladder observed through injections + the best-effort
 *   receipt's module-owned console.error signal
 * - host.confirm "no" without UI on an inbound shutdown frame (ask policy)
 * - registry-dir unusable: warn() falls back to console.warn
 */

import { createConnection } from "node:net";
import { mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { randomUUID } from "node:crypto";
import peersExtension from "../src/index.ts";
import { createPeerProtocol } from "../src/protocol.ts";
import {
	PEER_KEY_FILE_RE,
	PEER_RECORD_FILE_RE,
	type PeerContentAttrs,
	type PeersExtensionContext,
	type PeersHostAPI,
} from "../src/types.ts";

interface RegisteredTool {
	name: string;
}

type Handler = (event: unknown, ctx: PeersExtensionContext) => unknown;

interface FakePi {
	pi: PeersHostAPI;
	handlers: Map<string, Handler>;
	tools: RegisteredTool[];
	commands: Map<string, unknown>;
	sent: Array<{ text: string; options?: { deliverAs?: string } }>;
}

function createFakePi(): FakePi {
	const handlers = new Map<string, Handler>();
	const tools: RegisteredTool[] = [];
	const commands = new Map<string, unknown>();
	const sent: Array<{ text: string; options?: { deliverAs?: string } }> = [];
	const pi = {
		registerTool(tool: RegisteredTool) {
			tools.push(tool);
		},
		registerCommand(name: string, handlerOrOptions: unknown) {
			commands.set(name, handlerOrOptions);
		},
		on(event: string, handler: Handler) {
			handlers.set(event, handler);
			return () => handlers.delete(event);
		},
		sendUserMessage(text: string, options?: { deliverAs?: string }) {
			sent.push({ text, options });
		},
	};
	return { pi: pi as unknown as PeersHostAPI, handlers, tools, commands, sent };
}

function createFakeCtx(cwd: string, overrides: Partial<PeersExtensionContext> = {}): PeersExtensionContext {
	return {
		ui: {
			notify: () => {},
			confirm: async () => true,
		},
		hasUI: false,
		mode: "tui",
		cwd,
		sessionManager: {
			getSessionId: () => "session-1",
			getSessionName: () => undefined,
			getCwd: () => cwd,
		},
		isIdle: () => true,
		shutdown: () => {},
		...overrides,
	};
}

describe("peers plugin host-wiring coverage", () => {
	let peersDir: string;
	const savedEnv: Record<string, string | undefined> = {};
	const envKeys = [
		"PI_PEERS_DISABLED",
		"PI_PEERS_ACCEPT",
		"PI_PEERS_SOCKET",
		"PI_PEERS_TOKEN",
		"PI_PEERS_DIR",
		"PI_PEERS_NAME",
		"PI_PEERS_EPHEMERAL",
	] as const;

	beforeEach(() => {
		peersDir = mkdtempSync(join(tmpdir(), "pi-peers-cov-index-"));
		for (const key of envKeys) savedEnv[key] = process.env[key];
		process.env.PI_PEERS_DIR = peersDir;
		delete process.env.PI_PEERS_DISABLED;
		delete process.env.PI_PEERS_ACCEPT;
		delete process.env.PI_PEERS_SOCKET;
		delete process.env.PI_PEERS_TOKEN;
		delete process.env.PI_PEERS_NAME;
		delete process.env.PI_PEERS_EPHEMERAL;
	});

	afterEach(() => {
		for (const key of envKeys) {
			if (savedEnv[key] === undefined) delete process.env[key];
			else process.env[key] = savedEnv[key];
		}
		rmSync(peersDir, { recursive: true, force: true });
	});

	/**
	 * Fresh registry dir + child-handoff env scrub for a second plugin
	 * instance within one test: the previous instance's session_start
	 * published PI_PEERS_SOCKET/PI_PEERS_TOKEN and left key files in the
	 * dir — a new instance reads the env as parent credentials at
	 * construction and the stale key would shadow the new one.
	 */
	function resetRuntimeState(): void {
		rmSync(peersDir, { recursive: true, force: true });
		peersDir = mkdtempSync(join(tmpdir(), "pi-peers-cov-index-"));
		process.env.PI_PEERS_DIR = peersDir;
		delete process.env.PI_PEERS_SOCKET;
		delete process.env.PI_PEERS_TOKEN;
	}

	function readOwnRecord(): Record<string, unknown> {
		return JSON.parse(readFileSync(join(peersDir, `${process.pid}.json`), "utf8")) as Record<string, unknown>;
	}

	/** Wire the plugin (env already set) and drive session_start once. */
	async function started(overrides: Partial<PeersExtensionContext> = {}) {
		const fake = createFakePi();
		peersExtension(fake.pi);
		const ctx = createFakeCtx(peersDir, overrides);
		await fake.handlers.get("session_start")?.({ type: "session_start", reason: "startup" }, ctx);
		return { fake, ctx };
	}

	it("publishes kind per ctx.mode: rpc, json, print, unknown falls back to print", async () => {
		for (const [mode, kind] of [
			["rpc", "rpc"],
			["json", "json"],
			["print", "print"],
			["weird-future-mode", "print"],
		] as const) {
			await started({ mode });
			expect(readOwnRecord().kind).toBe(kind);
		}
	});

	it("notify() reaches ui.notify only when hasUI is true (ephemeral quit toast)", async () => {
		process.env.PI_PEERS_EPHEMERAL = "1";
		const toasts: string[] = [];
		const a = await started({
			hasUI: true,
			ui: { notify: (m: string) => toasts.push(m), confirm: async () => true },
			shutdown: () => {},
		});
		await a.fake.handlers.get("agent_start")?.({ type: "agent_start" }, a.ctx);
		await a.fake.handlers.get("agent_settled")?.({ type: "agent_settled" }, a.ctx);
		expect(toasts.some((t) => t.includes("Ephemeral peer finished"))).toBe(true);

		const silent: string[] = [];
		const b = await started({
			hasUI: false,
			ui: { notify: (m: string) => silent.push(m), confirm: async () => true },
			shutdown: () => {},
		});
		await b.fake.handlers.get("agent_start")?.({ type: "agent_start" }, b.ctx);
		await b.fake.handlers.get("agent_settled")?.({ type: "agent_settled" }, b.ctx);
		expect(silent).toHaveLength(0);
	});

	it("publishes the sessionManager name, else the derived name", async () => {
		await started({
			sessionManager: {
				getSessionId: () => "s1",
				getSessionName: () => "Researcher Bot",
				getCwd: () => peersDir,
			},
		});
		expect(readOwnRecord().name).toBe("researcher-bot");
		expect(readOwnRecord().nameSource).toBe("user");

		await started();
		const name = readOwnRecord().name;
		if (typeof name !== "string") throw new Error("name missing");
		expect(name).toContain(process.pid.toString());
		expect(readOwnRecord().nameSource).toBe("derived");
	});

	it("PI_PEERS_NAME is normalized (whitespace only) before publish", async () => {
		process.env.PI_PEERS_NAME = "My  Worker ";
		await started();
		const r = readOwnRecord();
		// normalizeName: NFKC → trim → lowercase → collapse whitespace to
		// dashes; punctuation survives by design.
		expect(r.name).toBe("my-worker");
		expect(r.nameSource).toBe("env");
	});

	it("session_shutdown for non-quit reasons keeps the record for the republish", async () => {
		const { fake, ctx } = await started();
		await fake.handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "new" }, ctx);
		expect(readFileSync(join(peersDir, `${process.pid}.json`), "utf8")).toBeTruthy();
		// A non-object event is treated like quit (reason absent).
		await fake.handlers.get("session_start")?.({ type: "session_start", reason: "resume" }, ctx);
		expect(readOwnRecord().pid).toBe(process.pid);
	});

	it("malformed before_agent_start events return undefined without mutation", async () => {
		const { fake, ctx } = await started();
		const bad: unknown[] = [
			"string",
			null,
			[],
			42,
			{},
			{ systemPromptOptions: "not-an-object" },
			{ systemPromptOptions: { appendSystemPrompt: 42 } },
			{ systemPromptOptions: null },
			{ systemPrompt: "not-an-array" },
			{ systemPrompt: [1, 2] },
		];
		for (const event of bad) {
			expect(fake.handlers.get("before_agent_start")?.(event, ctx)).toBeUndefined();
		}
	});

	it("publishRecord tolerates a throwing getContextUsage hook", async () => {
		await started({
			ui: { notify: () => {}, confirm: async () => true },
			getContextUsage: () => {
				throw new Error("usage hook broken");
			},
		});
		expect(readOwnRecord().pid).toBe(process.pid);
		expect(readOwnRecord().contextUsage).toBeUndefined();
	});

	it("turn_start: sub-1pp drift keeps the file, material change rewrites, no hook is tolerated", async () => {
		let pct = 30;
		const { fake, ctx } = await started({
			getContextUsage: () => ({ tokens: pct * 100, contextWindow: 10_000, percent: pct }),
		});
		const usageOf = (): number | undefined => {
			const usage = readOwnRecord().contextUsage as { percent?: number } | undefined;
			return usage?.percent;
		};
		expect(usageOf()).toBe(30);

		pct = 30.4;
		const before = readFileSync(join(peersDir, `${process.pid}.json`), "utf8");
		await fake.handlers.get("turn_start")?.({ type: "turn_start" }, ctx);
		expect(readFileSync(join(peersDir, `${process.pid}.json`), "utf8")).toBe(before);

		pct = 77;
		await fake.handlers.get("turn_start")?.({ type: "turn_start" }, ctx);
		expect(usageOf()).toBe(77);

		// No usage hook at all → early return.
		const b = await started();
		await b.fake.handlers.get("turn_start")?.({ type: "turn_start" }, b.ctx);
		expect(readOwnRecord().pid).toBe(process.pid);

		// A throwing hook → swallowed early return.
		const c = await started({
			getContextUsage: () => {
				throw new Error("boom");
			},
		});
		await expect(c.fake.handlers.get("turn_start")?.({ type: "turn_start" }, c.ctx)).toBeUndefined();
		expect(readOwnRecord().pid).toBe(process.pid);
	});

	it("session_info_changed: no record no-op, rename history, same-name no-op, cap at 32, nameless no-op", async () => {
		const { fake, ctx } = await started();
		// The no-record branch: a fresh plugin instance before session_start.
		const fresh = createFakePi();
		peersExtension(fresh.pi);
		await fresh.handlers.get("session_info_changed")?.({ name: "x" }, ctx);

		await fake.handlers.get("session_info_changed")?.({ type: "session_info_changed", name: "New Bot Name" }, ctx);
		let record = readOwnRecord();
		expect(record.name).toBe("new-bot-name");
		expect(record.nameSource).toBe("user");
		expect((record.formerNames as unknown[]).length).toBe(1);

		await fake.handlers.get("session_info_changed")?.({ name: "New Bot Name" }, ctx);
		record = readOwnRecord();
		expect((record.formerNames as unknown[]).length).toBe(1);

		await fake.handlers.get("session_info_changed")?.({ type: "session_info_changed" }, ctx);
		expect(readOwnRecord().name).toBe("new-bot-name");

		for (let i = 0; i < 40; i++) {
			await fake.handlers.get("session_info_changed")?.({ name: `bot-${i}` }, ctx);
		}
		record = readOwnRecord();
		expect((record.formerNames as unknown[]).length).toBeLessThanOrEqual(32);
	});

	it("registry-dir unusable: warn() falls back to console.warn when the toast throws", async () => {
		// peersDir is captured at plugin construction: set the broken dir
		// BEFORE peersExtension() runs.
		const messages: string[] = [];
		const notADir = join(peersDir, "blocked");
		writeFileSync(notADir, "x");
		process.env.PI_PEERS_DIR = notADir;
		const fake = createFakePi();
		peersExtension(fake.pi);
		const ctx = createFakeCtx(notADir, {
			ui: {
				notify: (m: string) => {
					messages.push(m);
					throw new Error("toast broken");
				},
				confirm: async () => true,
			},
		});
		const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
		try {
			await fake.handlers.get("session_start")?.({ type: "session_start", reason: "startup" }, ctx);
			expect(messages.length).toBeGreaterThan(0);
			expect(warnSpy).toHaveBeenCalled();
		} finally {
			warnSpy.mockRestore();
			process.env.PI_PEERS_DIR = peersDir;
		}
	});

	it("acceptConnection: malformed first line, unknown token, malformed frame line, wrong msgV, unknown action", async () => {
		const { fake } = await started();
		const record = readOwnRecord();
		const socketPath = record.socketPath as string;
		const keyName = readdirSync(peersDir).find((f) => PEER_KEY_FILE_RE.test(f));
		if (keyName === undefined) throw new Error("no key file published");
		const token = (JSON.parse(readFileSync(join(peersDir, keyName), "utf8")) as { peerToken: string }).peerToken;

		/** Lines received back from the peer for a raw write. */
		const writeLines = (lines: string[]): Promise<string[]> => {
			const { promise, resolve, reject } = Promise.withResolvers<string[]>();
			const back: string[] = [];
			const sock = createConnection(socketPath, () => {
				for (const line of lines) sock.write(`${line}\n`);
				sock.end();
			});
			sock.on("data", (d: Buffer) => {
				for (const line of d.toString().split("\n")) if (line !== "") back.push(line);
			});
			sock.on("close", () => resolve(back));
			sock.on("error", reject);
			return promise;
		};

		// Malformed first line (not JSON) → connection destroyed, no reply.
		expect(await writeLines(["not-json-at-all"])).toEqual([]);
		// Valid JSON, but an unknown token → destroyed before any frame.
		expect(await writeLines([JSON.stringify({ type: "auth", token: "0".repeat(32) })])).toEqual([]);
		// Not an auth line at all → same refusal.
		expect(await writeLines([JSON.stringify({ type: "user" })])).toEqual([]);
		// Empty token string → refused.
		expect(await writeLines([JSON.stringify({ type: "auth", token: "" })])).toEqual([]);
		// Valid token, but a garbage frame line → destroyed, no reply.
		expect(await writeLines([JSON.stringify({ type: "auth", token }), "garbage"])).toEqual([]);
		// Valid token, frame with wrong msgV → refused.
		expect(
			await writeLines([
				JSON.stringify({ type: "auth", token }),
				JSON.stringify({
					msgV: 9,
					msg_id: "x",
					type: "user",
					from: "f",
					fromPid: 1,
					fromSession: "s",
					hopChain: [],
					priority: "next",
					content: "c",
				}),
			]),
		).toEqual([]);
		// Control frame with unknown action → parse refuses, connection dies.
		expect(
			await writeLines([
				JSON.stringify({ type: "auth", token }),
				JSON.stringify({ msgV: 1, msg_id: "x", type: "control", action: "nope", from: "f" }),
			]),
		).toEqual([]);
		void fake;
	});

	it("acceptConnection: after session_shutdown (quit) new connections are refused and cleanup ran", async () => {
		const { fake, ctx } = await started();
		const record = readOwnRecord();
		const socketPath = record.socketPath as string;
		const keyName = readdirSync(peersDir).find((f) => PEER_KEY_FILE_RE.test(f));
		if (keyName === undefined) throw new Error("no key file published");
		const token = (JSON.parse(readFileSync(join(peersDir, keyName), "utf8")) as { peerToken: string }).peerToken;

		await fake.handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "quit" }, ctx);

		// The closing gate refuses any new connection: destroy() fires before
		// any frame handling, so nothing comes back (and the connect itself
		// may error once the listener is gone — either event settles the wait).
		const { promise, resolve } = Promise.withResolvers<string[]>();
		const back: string[] = [];
		const sock = createConnection(socketPath, () => {
			sock.write(`${JSON.stringify({ type: "auth", token })}\n`);
			sock.end();
		});
		sock.on("data", (d: Buffer) => {
			for (const line of d.toString().split("\n")) if (line !== "") back.push(line);
		});
		sock.on("close", () => resolve(back));
		sock.on("error", () => resolve(back));
		const lines = await promise;
		expect(lines).toEqual([]);
		// Record and key files are cleaned up by the quit path.
		expect(readdirSync(peersDir).filter((f) => PEER_RECORD_FILE_RE.test(f))).toHaveLength(0);
	});

	/** One wired plugin + one inbound user frame from a fabricated sender. */
	async function deliverWithPolicy(
		policy: string,
		body: string,
		overrides: Partial<PeersExtensionContext> = {},
	): Promise<{ fake: FakePi; ctx: PeersExtensionContext }> {
		process.env.PI_PEERS_ACCEPT = policy;
		const { fake, ctx } = await started({ isIdle: () => true, ...overrides });
		const record = readOwnRecord();
		const socketPath = record.socketPath as string;
		const keyName = readdirSync(peersDir).find((f) => PEER_KEY_FILE_RE.test(f));
		if (keyName === undefined) throw new Error("no key file published");
		const token = (JSON.parse(readFileSync(join(peersDir, keyName), "utf8")) as { peerToken: string }).peerToken;
		const wireFrom = `pipe:LOCAL\\pi-peer-${"c".repeat(32)}`;
		const wireAttrs: PeerContentAttrs = { from: wireFrom, fromSession: "sess-x", fromName: "x" };
		const { promise, resolve, reject } = Promise.withResolvers<void>();
		const sock = createConnection(socketPath, () => {
			sock.write(`${JSON.stringify({ type: "auth", token })}\n`);
			sock.write(
				`${JSON.stringify({
					msgV: 1,
					msg_id: randomUUID(),
					type: "user",
					from: wireFrom,
					fromPid: 4242,
					fromSession: "sess-x",
					fromName: "x",
					hopChain: [wireFrom],
					content: createPeerProtocol().wrapPeerContent(wireAttrs, body),
					priority: "next",
				})}\n`,
			);
			sock.end(() => resolve());
		});
		sock.on("error", reject);
		await promise;
		return { fake, ctx };
	}

	it("acceptPolicy env ladder: off/ask(no UI) refuse, all and unknown values accept", async () => {
		// "all" (and unrecognized values falling back to the open default)
		// inject the message into the agent. Each policy gets a fresh registry
		// dir: a prior plugin instance's key file must not shadow this one's.
		for (const policy of ["all", "bogus"]) {
			resetRuntimeState();
			const { fake } = await deliverWithPolicy(policy, `probe-${policy}`);
			await vi.waitFor(() => {
				expect(fake.sent.length).toBe(1);
			});
			expect(fake.sent[0].options).toBeUndefined(); // idle: bare prompt()
		}

		// "off" and "ask"-without-UI refuse: no injection, and the refusal
		// receipt's best-effort delivery surfaces as the module-owned
		// console.error (the fabricated sender has no listener/key).
		for (const policy of ["off", "ask"]) {
			resetRuntimeState();
			const errors: string[] = [];
			const spy = vi.spyOn(console, "error").mockImplementation((...args: unknown[]) => {
				errors.push(args.map(String).join(" "));
			});
			try {
				const { fake } = await deliverWithPolicy(policy, `probe-${policy}`);
				await vi.waitFor(() => {
					expect(errors.length).toBeGreaterThan(0);
				});
				expect(errors[0].startsWith("[peers]")).toBe(true);
				expect(fake.sent).toHaveLength(0);
			} finally {
				spy.mockRestore();
			}
		}
	});

	it("host.confirm answers 'no' without a UI: an inbound shutdown frame is denied, session survives", async () => {
		const shutdownCalls: number[] = [];
		const errors: string[] = [];
		const spy = vi.spyOn(console, "error").mockImplementation((...args: unknown[]) => {
			errors.push(args.map(String).join(" "));
		});
		try {
			process.env.PI_PEERS_ACCEPT = "ask";
			const fake = createFakePi();
			peersExtension(fake.pi);
			const ctx = createFakeCtx(peersDir, {
				hasUI: false,
				isIdle: () => true,
				shutdown: () => {
					shutdownCalls.push(1);
				},
			});
			await fake.handlers.get("session_start")?.({ type: "session_start", reason: "startup" }, ctx);
			const record = readOwnRecord();
			const socketPath = record.socketPath as string;
			const keyName = readdirSync(peersDir).find((f) => PEER_KEY_FILE_RE.test(f));
			if (keyName === undefined) throw new Error("no key file published");
			const token = (JSON.parse(readFileSync(join(peersDir, keyName), "utf8")) as { peerToken: string }).peerToken;
			const wireFrom = `pipe:LOCAL\\pi-peer-${"c".repeat(32)}`;
			const { promise, resolve } = Promise.withResolvers<void>();
			const sock = createConnection(socketPath, () => {
				sock.write(`${JSON.stringify({ type: "auth", token })}\n`);
				sock.write(
					`${JSON.stringify({
						msgV: 1,
						msg_id: randomUUID(),
						type: "control",
						action: "shutdown",
						from: wireFrom,
						fromName: "asker",
						reason: "please",
					})}\n`,
				);
				sock.end(() => resolve());
			});
			sock.on("error", () => resolve());
			await promise;
			sock.destroy();
			// The deny path ran (its best-effort response surfaces as the
			// module-owned console.error against the unreachable sender).
			await vi.waitFor(() => {
				expect(errors.length).toBeGreaterThan(0);
			});
			expect(errors[0].startsWith("[peers]")).toBe(true);
			// The session survived: quit never fired.
			expect(shutdownCalls).toHaveLength(0);
		} finally {
			spy.mockRestore();
			delete process.env.PI_PEERS_ACCEPT;
		}
	});
});
