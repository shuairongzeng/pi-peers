/**
 * Wiring test for the dual-host plugin entry point: drives PeersHostAPI with a
 * fake host and verifies lifecycle behavior — tool/command registration,
 * registry publish on session_start (and stale-record sweep), system-prompt
 * section for both pi and omp before_agent_start shapes, busy/idle status
 * flips across agent_settled / agent_end (including omp continuation flags),
 * and record+key cleanup on session_shutdown — against the real
 * registry/auth modules and a real bound socket.
 *
 * No wall-clock timers: events are driven by invoking captured handlers, and
 * the peers record is read back from disk after each transition.
 */

import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createConnection } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import peersExtension from "../src/index.ts";
import { PEER_KEY_FILE_RE, PEER_RECORD_FILE_RE, type PeersExtensionContext, type PeersHostAPI } from "../src/types.ts";

interface RegisteredTool {
	name: string;
}

/** Captured event handler keyed by event name. */
type Handler = (event: unknown, ctx: PeersExtensionContext) => unknown;

function createFakePi() {
	const handlers = new Map<string, Handler>();
	const tools: RegisteredTool[] = [];
	const commands = new Map<string, unknown>();
	const sent: { text: string; options?: { deliverAs?: string } }[] = [];
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
		...overrides,
	};
}

describe("peers plugin wiring (dual-host)", () => {
	let peersDir: string;
	const savedEnv: Record<string, string | undefined> = {};

	beforeEach(() => {
		peersDir = mkdtempSync(join(tmpdir(), "pi-peers-wiring-"));
		for (const key of ["PI_PEERS_DISABLED", "PI_PEERS_ACCEPT", "PI_PEERS_SOCKET", "PI_PEERS_TOKEN", "PI_PEERS_DIR"] as const) {
			savedEnv[key] = process.env[key];
		}
		process.env.PI_PEERS_DIR = peersDir;
		delete process.env.PI_PEERS_DISABLED;
		delete process.env.PI_PEERS_ACCEPT;
		delete process.env.PI_PEERS_SOCKET;
		delete process.env.PI_PEERS_TOKEN;
	});

	afterEach(() => {
		for (const key of ["PI_PEERS_DISABLED", "PI_PEERS_ACCEPT", "PI_PEERS_SOCKET", "PI_PEERS_TOKEN", "PI_PEERS_DIR"] as const) {
			if (savedEnv[key] === undefined) delete process.env[key];
			else process.env[key] = savedEnv[key];
		}
		rmSync(peersDir, { recursive: true, force: true });
	});

	/** Read the record this process published from the registry dir. */
	function readOwnRecord(): Record<string, unknown> {
		const path = join(peersDir, `${process.pid}.json`);
		return JSON.parse(readFileSync(path, "utf8")) as Record<string, unknown>;
	}

	it("registers the tool and command, publishes a record on session_start, sweeps stale records, cleans up on quit", async () => {
		const { pi, handlers, tools, commands } = createFakePi();
		peersExtension(pi);

		expect(tools.map((t) => t.name)).toContain("send_peer_message");
		expect(commands.has("peers")).toBe(true);

		// Plant a stale record + key from a dead pid; startup must sweep them.
		mkdirSync(peersDir, { recursive: true });
		writeFileSync(
			join(peersDir, "99999999.json"),
			JSON.stringify({
				version: 1,
				pid: 99999999,
				sessionId: "dead",
				cwd: peersDir,
				name: "dead-peer",
				nameSource: "derived",
				formerNames: [],
				kind: "interactive",
				status: "idle",
				statusUpdatedAt: 0,
				startedAt: 0,
				pidDomain: "win32:x",
				socketPath: `\\\\.\\pipe\\LOCAL\\pi-peer-${"0".repeat(32)}`,
				peerProtocol: 1,
				features: ["notify_idle"],
			}),
		);
		writeFileSync(join(peersDir, `99999999.${"0".repeat(64)}.key`), "{}");

		const ctx = createFakeCtx(peersDir);
		await handlers.get("session_start")?.({ type: "session_start", reason: "startup" }, ctx);

		expect(existsSync(join(peersDir, "99999999.json"))).toBe(false);
		const records = readdirSync(peersDir).filter((f) => PEER_RECORD_FILE_RE.test(f));
		expect(records).toEqual([`${process.pid}.json`]);
		const record = readOwnRecord();
		expect(record.sessionId).toBe("session-1");
		expect(record.kind).toBe("interactive");
		expect(record.pid).toBe(process.pid);
		expect(record.socketPath).toBeTruthy();
		expect(readdirSync(peersDir).some((f) => PEER_KEY_FILE_RE.test(f))).toBe(true);
		// Child handoff env published: wire address + 32-hex child token.
		expect(process.env.PI_PEERS_SOCKET).toMatch(/^(pipe:|sock:)/);
		expect(process.env.PI_PEERS_TOKEN).toMatch(/^[0-9a-f]{32}$/);

		// Quit → record and keys removed
		await handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "quit" }, ctx);
		expect(readdirSync(peersDir).filter((f) => PEER_RECORD_FILE_RE.test(f))).toHaveLength(0);
		expect(readdirSync(peersDir).filter((f) => PEER_KEY_FILE_RE.test(f))).toHaveLength(0);
	});

	it("appends the peers section to pi-shaped before_agent_start (systemPromptOptions)", async () => {
		const { pi, handlers, tools } = createFakePi();
		peersExtension(pi);
		expect(tools).toHaveLength(1); // sanity: extension loaded

		const ctx = createFakeCtx(peersDir);
		await handlers.get("session_start")?.({ type: "session_start", reason: "startup" }, ctx);

		const promptOptions = { appendSystemPrompt: "base" };
		const result = handlers.get("before_agent_start")?.(
			{ type: "before_agent_start", systemPromptOptions: promptOptions },
			ctx,
		);
		expect(result).toBeUndefined();
		expect(promptOptions.appendSystemPrompt.startsWith("base")).toBe(true);
		expect(promptOptions.appendSystemPrompt).toContain("## Cross-session peers");
		expect(promptOptions.appendSystemPrompt).toContain("<cross-session-message");
	});

	it("returns prompt array with previous sections plus the peers section for omp-shaped before_agent_start", async () => {
		const { pi, handlers } = createFakePi();
		peersExtension(pi);

		const ctx = createFakeCtx(peersDir);
		await handlers.get("session_start")?.({ type: "session_start", reason: "startup" }, ctx);

		const input = ["one", "two"];
		const result = handlers.get("before_agent_start")?.(
			{ type: "before_agent_start", systemPrompt: input },
			ctx,
		);
		const returned = result as { systemPrompt: string[] };
		expect(Array.isArray(returned.systemPrompt)).toBe(true);
		expect(returned.systemPrompt.slice(0, 2)).toEqual(["one", "two"]);
		expect(returned.systemPrompt).toHaveLength(3);
		expect(returned.systemPrompt[2]).toContain("## Cross-session peers");
		// The input array is not mutated; a new array is returned.
		expect(input).toEqual(["one", "two"]);
		expect(returned.systemPrompt).not.toBe(input);
	});

	it("flips status busy on agent_start, idle on agent_settled and plain agent_end, stays busy on omp continuation agent_end", async () => {
		const { pi, handlers } = createFakePi();
		peersExtension(pi);

		const ctx = createFakeCtx(peersDir);
		await handlers.get("session_start")?.({ type: "session_start", reason: "startup" }, ctx);

		await handlers.get("agent_start")?.({ type: "agent_start" }, ctx);
		expect(readOwnRecord().status).toBe("busy");

		// pi's agent_settled has no continuation flags → idle.
		await handlers.get("agent_settled")?.({ type: "agent_settled", aborted: false }, ctx);
		expect(readOwnRecord().status).toBe("idle");

		// omp continuation flags must NOT flip to idle.
		await handlers.get("agent_start")?.({ type: "agent_start" }, ctx);
		expect(readOwnRecord().status).toBe("busy");
		await handlers.get("agent_end")?.({ type: "agent_end", willContinue: true }, ctx);
		expect(readOwnRecord().status).toBe("busy");
		await handlers.get("agent_start")?.({ type: "agent_start" }, ctx);
		await handlers.get("agent_end")?.({ type: "agent_end", awaitingAsyncWork: true }, ctx);
		expect(readOwnRecord().status).toBe("busy");

		// omp plain agent_end (no flags) → idle.
		await handlers.get("agent_end")?.({ type: "agent_end", messages: [] }, ctx);
		expect(readOwnRecord().status).toBe("idle");
	});

	it("delivers inbound messages bare when idle, with deliverAs when busy", async () => {
		process.env.PI_PEERS_ACCEPT = "all";
		const { pi, handlers, sent } = createFakePi();
		peersExtension(pi);

		let idle = true;
		const ctx = createFakeCtx(peersDir, { isIdle: () => idle });
		await handlers.get("session_start")?.({ type: "session_start", reason: "startup" }, ctx);

		// Deliver a real frame over the wire, as a peer would.
		const record = readOwnRecord();
		const socketPath = record.socketPath as string;
		const keyName = readdirSync(peersDir).find((f) => PEER_KEY_FILE_RE.test(f));
		const key = JSON.parse(readFileSync(join(peersDir, keyName!), "utf8")) as { peerToken: string };

		const deliver = (text: string): Promise<void> => {
			const { promise, resolve, reject } = Promise.withResolvers<void>();
			const sock = createConnection(socketPath, () => {
				sock.write(`${JSON.stringify({ type: "auth", token: key.peerToken })}\n`);
				sock.write(
					`${JSON.stringify({
						msgV: 1,
						msg_id: randomUUID(),
						type: "user",
						from: "pipe:LOCAL\\pi-peer-cccccccccccccccccccccccccccccccc",
						fromPid: 4242,
						fromSession: "sess-test",
						fromName: "test-peer",
						hopChain: ["pipe:LOCAL\\pi-peer-cccccccccccccccccccccccccccccccc"],
						content: `<cross-session-message from="pipe:LOCAL\\pi-peer-cccccccccccccccccccccccccccccccc">${text}</cross-session-message>`,
						priority: "next",
					})}\n`,
				);
				sock.end(() => resolve());
			});
			sock.on("error", reject);
			return promise;
		};

		await deliver("first message");
		await vi.waitFor(() => expect(sent).toHaveLength(1));
		// Idle: no deliverAs — pi reaches prompt(), omp reaches prompt(); both start a turn.
		expect(sent[0]!.options).toBeUndefined();

		idle = false;
		await deliver("second message");
		await vi.waitFor(() => expect(sent).toHaveLength(2));
		// Busy: followUp queues behind the running turn on both hosts.
		expect(sent[1]!.options).toEqual({ deliverAs: "followUp" });

		await handlers.get("session_shutdown")?.({ type: "session_shutdown", reason: "quit" }, ctx);
	});

	it("does nothing when PI_PEERS_DISABLED is set", () => {
		process.env.PI_PEERS_DISABLED = "1";
		const { pi, handlers, tools, commands } = createFakePi();
		peersExtension(pi);
		expect(tools).toHaveLength(0);
		expect(commands.size).toBe(0);
		expect(handlers.size).toBe(0);
	});

	it("registers a command usable in both pi and omp forms", async () => {
		const { pi, commands } = createFakePi();
		peersExtension(pi);

		const registered = commands.get("peers") as unknown as {
			(args: string, ctx: PeersExtensionContext): Promise<void>;
			handler?: (args: string, ctx: PeersExtensionContext) => Promise<void>;
			description?: string;
		};
		expect(typeof registered).toBe("function");
		expect(typeof registered.handler).toBe("function");
		expect(typeof registered.description).toBe("string");
		expect(registered.handler).toBe(registered);

		// pi reads the options shape; omp calls the function. Both must work.
		const ctx = createFakeCtx(peersDir);
		await registered.handler?.("", ctx);
		await registered("", ctx);
	});
});
