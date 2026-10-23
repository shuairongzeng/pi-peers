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

import { execFile, type ChildProcess, spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createConnection } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import peersExtension, { computeProjectRoot } from "../src/index.ts";
import { createPeerProtocol } from "../src/protocol.ts";
import { PEER_KEY_FILE_RE, PEER_RECORD_FILE_RE, type PeerContentAttrs, type PeersExtensionContext, type PeersHostAPI, type PeerRecord } from "../src/types.ts";

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
		shutdown: () => {},
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

	/** git's own answer for the work-tree root of `cwd`; undefined when git is missing/fails. */
	async function gitToplevel(cwd: string): Promise<string | undefined> {
		try {
			const { stdout } = await promisify(execFile)("git", ["-C", cwd, "rev-parse", "--show-toplevel"]);
			const out = stdout.trim();
			return out !== "" ? out : undefined;
		} catch {
			return undefined;
		}
	}

	/** True when `git init` works here (skips git-dependent sub-assertions). */
	async function gitWorks(): Promise<boolean> {
		const probe = mkdtempSync(join(tmpdir(), "pi-peers-gitprobe-"));
		try {
			await promisify(execFile)("git", ["init", "--quiet", probe]);
			return true;
		} catch {
			return false;
		} finally {
			rmSync(probe, { recursive: true, force: true });
		}
	}

	/** Minimal record the real registry's parser accepts. */
	function peerRecord(pid: number, overrides: Partial<PeerRecord> = {}): PeerRecord {
		return {
			version: 1,
			pid,
			sessionId: `sess-${pid}`,
			cwd: peersDir,
			projectRoot: peersDir,
			name: `p-${pid}`,
			nameSource: "derived",
			formerNames: [],
			kind: "interactive",
			status: "idle",
			statusUpdatedAt: Date.now(),
			startedAt: Date.now(),
			pidDomain: "win32:box",
			socketPath: `\\\\.\\pipe\\LOCAL\\pi-peer-${pid.toString(16).padStart(32, "0")}`,
			peerProtocol: 1,
			features: ["notify_idle"],
			...overrides,
		};
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
		expect(tools).toHaveLength(2); // sanity: both tools registered

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

	it("degrades to local-only when the registry dir is unusable", async () => {
		// PI_PEERS_DIR points at a regular file: listen-side setup (key publish on
		// POSIX dir creation / publishKey mkdir) and record publish both fail; the
		// session must still come up, with a warning instead of an exception.
		const notADir = join(peersDir, "not-a-dir");
		writeFileSync(notADir, "x");
		process.env.PI_PEERS_DIR = notADir;
		const { pi, handlers } = createFakePi();
		peersExtension(pi);
		const messages: string[] = [];
		const ctx = createFakeCtx(notADir, {
			ui: { notify: (m: string) => messages.push(m), confirm: async () => true },
		});
		await handlers.get("session_start")?.({ type: "session_start", reason: "startup" }, ctx);
		expect(messages.some((m) => m.includes("peers:"))).toBe(true);
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
		// No PI_PEERS_ACCEPT override: the default policy is "all" (open by design),
		// even though this fake ctx has no UI.
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

		const wireFrom = "pipe:LOCAL\\pi-peer-cccccccccccccccccccccccccccccccc";
		const wireAttrs: PeerContentAttrs = { from: wireFrom, fromSession: "sess-test", fromName: "test-peer" };
		const deliver = (text: string): Promise<void> => {
			const { promise, resolve, reject } = Promise.withResolvers<void>();
			const sock = createConnection(socketPath, () => {
				sock.write(`${JSON.stringify({ type: "auth", token: key.peerToken })}\n`);
				sock.write(
					`${JSON.stringify({
						msgV: 1,
						msg_id: randomUUID(),
						type: "user",
						from: wireFrom,
						fromPid: 4242,
						fromSession: "sess-test",
						fromName: "test-peer",
						hopChain: [wireFrom],
						content: createPeerProtocol().wrapPeerContent(wireAttrs, text),
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

	it("publishes a record with projectRoot: git root inside a work tree, ctx.cwd outside", async () => {
		const { pi, handlers } = createFakePi();
		peersExtension(pi);

		// Outside any git work tree (fresh mkdtemp under the OS tmpdir):
		// projectRoot === ctx.cwd.
		const outside = mkdtempSync(join(tmpdir(), "pi-peers-outside-"));
		try {
			await handlers.get("session_start")?.({ type: "session_start", reason: "startup" }, createFakeCtx(outside));
			const r = readOwnRecord();
			expect(r.cwd).toBe(outside);
			expect(r.projectRoot).toBe(outside);
		} finally {
			rmSync(outside, { recursive: true, force: true });
		}

		// Inside a git work tree (skipped when git is unavailable here):
		// projectRoot === git's own toplevel answer.
		if (!(await gitWorks())) return;
		const repo = mkdtempSync(join(tmpdir(), "pi-peers-repo-"));
		try {
			await promisify(execFile)("git", ["init", "--quiet", repo]);
			// A second session_start republishes with the new project's root.
			await handlers.get("session_start")?.({ type: "session_start", reason: "new" }, createFakeCtx(repo));
			const r2 = readOwnRecord();
			expect(r2.cwd).toBe(repo);
			expect(r2.projectRoot).toBe(await gitToplevel(repo));
		} finally {
			rmSync(repo, { recursive: true, force: true });
		}
	});

	it("computeProjectRoot: cwd fallback outside a repo, git toplevel inside one", async () => {
		const outside = mkdtempSync(join(tmpdir(), "pi-peers-outside-"));
		try {
			// Not a repo (and even a failing/missing git must resolve, not reject).
			expect(await computeProjectRoot(outside)).toBe(outside);
		} finally {
			rmSync(outside, { recursive: true, force: true });
		}

		if (!(await gitWorks())) return;
		const repo = mkdtempSync(join(tmpdir(), "pi-peers-repo-"));
		try {
			await promisify(execFile)("git", ["init", "--quiet", repo]);
			// A nested cwd still resolves to the work-tree root.
			const nested = join(repo, "sub", "dir");
			mkdirSync(nested, { recursive: true });
			expect(await computeProjectRoot(nested)).toBe(await gitToplevel(nested));
		} finally {
			rmSync(repo, { recursive: true, force: true });
		}
	});

	it("computeProjectRoot falls back to the cwd when git is unavailable (ENOENT)", async () => {
		// Empty PATH (plus a bogus COMSPEC so Windows' execvp-style lookup
		// cannot fall back to the system dir) makes git unspawnable.
		const savedPath = process.env.PATH;
		const savedComSpec = process.env.COMSPEC;
		const cwd = mkdtempSync(join(tmpdir(), "pi-peers-nogit-"));
		process.env.PATH = "";
		process.env.COMSPEC = "definitely-not-a-real-shell";
		try {
			await expect(computeProjectRoot(cwd)).resolves.toBe(cwd);
		} finally {
			process.env.PATH = savedPath;
			if (savedComSpec === undefined) delete process.env.COMSPEC;
			else process.env.COMSPEC = savedComSpec;
			rmSync(cwd, { recursive: true, force: true });
		}
	});

	it("/peers lists same-project sessions line by line and only summarizes other projects", async () => {
		const { pi, handlers, commands } = createFakePi();
		peersExtension(pi);

		// Two real live child processes: one in another project, one in ours.
		// (Liveness is a real kill(pid, 0) probe — records must point at
		// processes that actually exist, so plant records under their pids.)
		// Planted BEFORE session_start: raw file writes bypass publish() and its
		// cache invalidation, so they must predate the startup scan to be seen.
		const other = spawn(process.execPath, ["-e", "setInterval(() => {}, 60000);"], { stdio: "ignore", windowsHide: true });
		const same = spawn(process.execPath, ["-e", "setInterval(() => {}, 60000);"], { stdio: "ignore", windowsHide: true });
		const otherPid = other.pid as number;
		const samePid = same.pid as number;
		const ctx = createFakeCtx(peersDir);
		try {
			const exitPromises = [other, same].map((child) => {
				const { promise, resolve } = Promise.withResolvers<void>();
				child.once("exit", () => resolve());
				return promise;
			});
			writeFileSync(
				join(peersDir, `${otherPid}.json`),
				JSON.stringify(
					peerRecord(otherPid, { name: "elsewhere", cwd: "D:/nowhere/other-project", projectRoot: "D:/nowhere/other-project" }),
				),
			);
			writeFileSync(
				join(peersDir, `${samePid}.json`),
				JSON.stringify(peerRecord(samePid, { name: "here", cwd: peersDir, projectRoot: peersDir })),
			);
			// list() sweeps live-pid records without an auth key: plant both.
			for (const pid of [otherPid, samePid]) {
				const rec = peerRecord(pid);
				const hash = createHash("sha256").update(rec.socketPath).digest("hex");
				writeFileSync(join(peersDir, `${pid}.${hash}.key`), "{}");
			}

			await handlers.get("session_start")?.({ type: "session_start", reason: "startup" }, ctx);

			const messages: string[] = [];
			const notifyCtx = createFakeCtx(peersDir, {
				ui: { notify: (m: string) => messages.push(m), confirm: async () => true },
			});
			const registered = commands.get("peers") as unknown as {
				(args: string, ctx: PeersExtensionContext): Promise<void>;
			};
			await registered("", notifyCtx);

			const text = messages.join("\n");
			// Same-project sessions are listed line by line, self included.
			expect(text).toContain("here");
			expect(text).toContain("(this session)");
			// Other-project sessions collapse into one summary line.
			expect(text).toContain('(+1 sessions in other projects — use the send_peer_message tool with scope: "all" to reach them)');
			expect(text).not.toContain("elsewhere");
			expect(text).not.toContain("other-project");

			other.kill();
			same.kill();
			await Promise.all(exitPromises);
		} finally {
			other.kill();
			same.kill();
		}
	});

	it("agent_start during the session_start async gap still publishes busy", async () => {
		const { pi, handlers } = createFakePi();
		peersExtension(pi);
		const ctx = createFakeCtx(peersDir, { isIdle: () => true });
		// Fire agent_start BEFORE session_start completes (the handler awaits
		// listen + git probe): the busy status must survive into the record.
		await handlers.get("agent_start")?.({ type: "agent_start" }, ctx);
		await handlers.get("session_start")?.({ type: "session_start", reason: "startup" }, ctx);
		expect(readOwnRecord().status).toBe("busy");
	});

	it("PI_PEERS_NAME wins over derived naming and stamps nameSource=env", async () => {
		process.env.PI_PEERS_NAME = "Researcher Bot";
		try {
			const { pi, handlers } = createFakePi();
			peersExtension(pi);
			await handlers.get("session_start")?.({ type: "session_start", reason: "startup" }, createFakeCtx(peersDir));
			const record = readOwnRecord();
			expect(record.name).toBe("researcher-bot");
			expect(record.nameSource).toBe("env");
		} finally {
			delete process.env.PI_PEERS_NAME;
		}
	});

	it("whitespace-only PI_PEERS_NAME falls back to the derived name", async () => {
		process.env.PI_PEERS_NAME = "   ";
		try {
			const { pi, handlers } = createFakePi();
			peersExtension(pi);
			await handlers.get("session_start")?.({ type: "session_start", reason: "startup" }, createFakeCtx(peersDir));
			const record = readOwnRecord();
			expect(typeof record.name).toBe("string");
			expect(String(record.name).length).toBeGreaterThan(0);
			expect(record.nameSource).toBe("derived");
		} finally {
			delete process.env.PI_PEERS_NAME;
		}
	});

	it("ephemeral instances quit on the first idle after work, never before", async () => {
		process.env.PI_PEERS_EPHEMERAL = "1";
		let quitCalls = 0;
		try {
			const { pi, handlers } = createFakePi();
			peersExtension(pi);
			const ctx = createFakeCtx(peersDir, {
				shutdown: () => {
					quitCalls++;
				},
			});
			await handlers.get("session_start")?.({ type: "session_start", reason: "startup" }, ctx);

			// Idle without having worked: stay alive (e.g. still waiting for the task).
			await handlers.get("agent_settled")?.({ type: "agent_settled", aborted: false }, ctx);
			expect(quitCalls).toBe(0);

			// Busy → idle: the work is done, time to exit.
			await handlers.get("agent_start")?.({ type: "agent_start" }, ctx);
			await handlers.get("agent_settled")?.({ type: "agent_settled", aborted: false }, ctx);
			expect(quitCalls).toBe(1);
		} finally {
			delete process.env.PI_PEERS_EPHEMERAL;
		}
	});
});
