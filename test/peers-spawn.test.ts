/**
 * peers-spawn.test.ts — spawn engine: command reproduction, env contract,
 * unique naming, registration wait, timeout, and the real-process path.
 */

import { type ChildProcess, spawn as nodeSpawn } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { hostname, tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createPeerRegistry } from "../src/registry.ts";
import { assertValidModelArg, createPeerSpawner, hostCommand, type PeerExec, type SpawnedProcess } from "../src/spawn.ts";
import { ENV_PEERS_DIR, ENV_PEERS_EPHEMERAL, ENV_PEERS_NAME, PeerSendError, PeerSpawnError, type PeerRecord } from "../src/types.ts";

let peersDir: string;

beforeEach(() => {
	peersDir = mkdtempSync(join(tmpdir(), "pi-peers-spawn-"));
	process.env[ENV_PEERS_DIR] = peersDir;
});

afterEach(() => {
	delete process.env[ENV_PEERS_DIR];
	// Windows: a real `cmd /c start` child (visible-spawn tests) holds the dir
	// briefly after exit; retry instead of flaking on EPERM.
	rmSync(peersDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});

function fakeRecord(name: string, pid: number, overrides?: Partial<PeerRecord>): PeerRecord {
	return {
		version: 1,
		pid,
		sessionId: "sess-1",
		cwd: peersDir,
		projectRoot: peersDir,
		name,
		nameSource: "env",
		formerNames: [],
		kind: "rpc",
		status: "idle",
		statusUpdatedAt: Date.now(),
		startedAt: Date.now(),
		pidDomain: `win32:${hostname().toLowerCase()}`,
		socketPath: "\\\\.\\pipe\\LOCAL\\nonexistent",
		peerProtocol: 1,
		features: [],
		...overrides,
	};
}

/** list() sweeps live-pid records without an auth key: fixtures plant one. */
function plantRecordWithKey(record: PeerRecord): void {
	const hash = createHash("sha256").update(record.socketPath).digest("hex");
	writeFileSync(join(peersDir, `${record.pid}.${hash}.key`), "{}");
	writeFileSync(join(peersDir, `${record.pid}.json`), JSON.stringify(record));
}

interface FakeProc extends SpawnedProcess {
	env?: NodeJS.ProcessEnv;
	headless?: boolean;
}

/** Exec that pretends the child registered instantly (record written under the test's own pid). */
function fakeExec(captured: { env?: NodeJS.ProcessEnv; headless?: boolean }): PeerExec {
	return (_command, _args, options) => {
		captured.env = options.env;
		captured.headless = options.headless;
		plantRecordWithKey(fakeRecord(options.env[ENV_PEERS_NAME] ?? "unset", process.pid));
		return { pid: process.pid, kill: () => {}, onError: () => {} };
	};
}
/**
 * Await a condition: retries a predicate until it holds or the 5s budget
 * runs out (each miss defers to the event loop's own turn via a short
 * timer, so the wait ends the moment the condition flips — no fixed sleep
 * masking the awaited event; the budget bounds a broken condition instead).
 */
async function waitFor(predicate: () => boolean, budgetMs = 5_000): Promise<void> {
	const deadline = Date.now() + budgetMs;
	while (!predicate()) {
		if (Date.now() >= deadline) throw new Error("timed out waiting for condition");
		await new Promise<void>((resolve) => setTimeout(resolve, 10));
	}
}

/** Await a file's appearance and return its contents (see waitFor). */
async function waitForFile(path: string, budgetMs = 5_000): Promise<string> {
	await waitFor(() => existsSync(path), budgetMs);
	return readFileSync(path, "utf8");
}

/** Exec that records its args and registers instantly (same fake as fakeExec). */
function fakeExecWithArgs(captured: { env?: NodeJS.ProcessEnv; headless?: boolean; args?: string[] }): PeerExec {
	return (_command, args, options) => {
		captured.args = args;
		return fakeExec(captured)(_command, args, options);
	};
}

const fakeOutbox = (sent: Array<{ target: string; content: string }>) => ({
	send: async (target: string, content: string) => {
		sent.push({ target, content });
		return { msgId: "m1", to: "addr", toName: target };
	},
	sendControl: async () => {},
	resolveLiveTarget: () => ({ ok: false as const, error: new PeerSendError("not_found", "fake") }),
	receiptOf: () => undefined,
	handleReceipt: () => {},
	registerOutbound: () => {},
	expectsReceipt: () => false,
	registerIdleNonce: () => {},
	consumeIdleNonce: () => false,
});

describe("hostCommand", () => {
	it("compiled host: executable only", () => {
		expect(hostCommand(["C:\\omp\\omp.exe"], "C:\\omp\\omp.exe", [])).toEqual(["C:\\omp\\omp.exe"]);
	});
	it("source host: exe + loader flags + script", () => {
		expect(hostCommand(["node", "src/cli.ts"], "node", ["--import", "file:///r.ts"])).toEqual([
			"node",
			"--import",
			"file:///r.ts",
			"src/cli.ts",
		]);
	});
});

describe("createPeerSpawner", () => {
	it("assigns the requested name via env and waits for registration", async () => {
		const captured: { env?: NodeJS.ProcessEnv } = {};
		const registry = createPeerRegistry(peersDir);
		const spawner = createPeerSpawner({ registry, outbox: fakeOutbox([]), cwd: () => peersDir, exec: fakeExec(captured) });
		const outcome = await spawner({ name: "researcher" });
		expect(outcome.name).toBe("researcher");
		expect(outcome.pid).toBe(process.pid);
		expect(outcome.ref).toMatch(/^[0-9a-f]{6}$/);
		expect(outcome.taskSent).toBe(false);
		expect(captured.env?.[ENV_PEERS_NAME]).toBe("researcher");
	});

	it("auto-generates a unique worker name when omitted", async () => {
		// A "taken" name must be backed by a LIVE pid: dead-pid records are swept by
		// list(), which would make the name look free again. Spawn a keeper process.
		const keeper = nodeSpawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], {
			cwd: tmpdir(),
			stdio: "ignore",
		});
		try {
			const registry = createPeerRegistry(peersDir);
			plantRecordWithKey(fakeRecord("researcher", keeper.pid!));
			const captured: { env?: NodeJS.ProcessEnv } = {};
			const spawner = createPeerSpawner({ registry, outbox: fakeOutbox([]), cwd: () => peersDir, exec: fakeExec(captured) });
			const outcome = await spawner({});
			expect(outcome.name).toMatch(/^worker-[0-9a-z]{4}$/);

			// Taken requested name gets a numeric suffix.
			const outcome2 = await spawner({ name: "researcher" });
			expect(outcome2.name).toBe("researcher-2");
		} finally {
			keeper.kill();
		}
	});

	it("does not leak the parent's ephemeral flag to non-ephemeral children", async () => {
		process.env[ENV_PEERS_EPHEMERAL] = "1";
		try {
			const captured: { env?: NodeJS.ProcessEnv } = {};
			const spawner = createPeerSpawner({ registry: createPeerRegistry(peersDir), outbox: fakeOutbox([]), cwd: () => peersDir, exec: fakeExec(captured) });
			await spawner({ name: "persistent-helper" });
			expect(captured.env?.[ENV_PEERS_EPHEMERAL]).toBeUndefined();
			// …and sets it only when explicitly requested.
			await spawner({ name: "short-lived", ephemeral: true });
			expect(captured.env?.[ENV_PEERS_EPHEMERAL]).toBe("1");
		} finally {
			delete process.env[ENV_PEERS_EPHEMERAL];
		}
	});

	it("sets the ephemeral env flag only when requested", async () => {
		const captured: { env?: NodeJS.ProcessEnv } = {};
		const registry = createPeerRegistry(peersDir);
		const spawner = createPeerSpawner({ registry, outbox: fakeOutbox([]), cwd: () => peersDir, exec: fakeExec(captured) });
		await spawner({ name: "a", ephemeral: true });
		expect(captured.env?.[ENV_PEERS_EPHEMERAL]).toBe("1");
		await spawner({ name: "b" });
		expect(captured.env?.[ENV_PEERS_EPHEMERAL]).toBeUndefined();
	});

	it("never hands the parent's socket credentials to the child", async () => {
		process.env.PI_PEERS_SOCKET = "pipe:LOCAL\\parent";
		process.env.PI_PEERS_TOKEN = "secret";
		try {
			const captured: { env?: NodeJS.ProcessEnv } = {};
			const registry = createPeerRegistry(peersDir);
			const spawner = createPeerSpawner({ registry, outbox: fakeOutbox([]), cwd: () => peersDir, exec: fakeExec(captured) });
			await spawner({ name: "c" });
			expect(captured.env?.PI_PEERS_SOCKET).toBeUndefined();
			expect(captured.env?.PI_PEERS_TOKEN).toBeUndefined();
		} finally {
			delete process.env.PI_PEERS_SOCKET;
			delete process.env.PI_PEERS_TOKEN;
		}
	});

	it("sends the task after registration and reports taskSent", async () => {
		const sent: Array<{ target: string; content: string }> = [];
		const registry = createPeerRegistry(peersDir);
		const spawner = createPeerSpawner({
			registry,
			outbox: fakeOutbox(sent),
			cwd: () => peersDir,
			exec: fakeExec({}),
		});
		const outcome = await spawner({ name: "researcher", task: "summarize the repo" });
		expect(outcome.taskSent).toBe(true);
		expect(sent).toEqual([{ target: "researcher", content: "summarize the repo" }]);
	});

	it("times out when the child never registers", async () => {
		const registry = createPeerRegistry(peersDir);
		const spawner = createPeerSpawner({
			registry,
			outbox: fakeOutbox([]),
			cwd: () => peersDir,
			registerTimeoutMs: 300,
			exec: () => ({ pid: 1, kill: () => {}, onError: () => {} }),
		});
		await expect(spawner({ name: "ghost" })).rejects.toMatchObject({ kind: "register_timeout" });
	});

	it("successful registration refreshes the scan cache for subsequent listings", async () => {
		// The headless wait finds the record via readRecord (no scan); without
		// a forced post-success rescan, the cached listing from the pre-spawn
		// name scan would hide the new peer for the cache TTL window.
		const registry = createPeerRegistry(peersDir);
		const list = registry.list.bind(registry);
		const freshSawPeer: boolean[] = [];
		registry.list = (opts?: { fresh?: boolean }) => {
			const entries = list(opts);
			if (opts?.fresh === true) {
				freshSawPeer.push(entries.some((entry) => entry.record.name === "cache-refresh"));
			}
			return entries;
		};
		const spawner = createPeerSpawner({
			registry,
			outbox: fakeOutbox([]),
			cwd: () => peersDir,
			exec: fakeExec({}),
		});
		const outcome = await spawner({ name: "cache-refresh", headless: true });
		expect(outcome.name).toBe("cache-refresh");
		// Pre-spawn name scan sees no record; the forced post-success rescan is
		// the one fresh call that sees the new peer.
		expect(freshSawPeer.filter((saw) => saw)).toHaveLength(1);
		// …so a cached (non-fresh) listing includes the new peer immediately.
		expect(registry.list().some((entry) => entry.record.name === "cache-refresh")).toBe(true);
	});

	it("abort before spawn: rejects with the shutdown message and starts no child", async () => {
		const controller = new AbortController();
		controller.abort();
		let execCalls = 0;
		const exec: PeerExec = () => {
			execCalls += 1;
			return { pid: 1, kill: () => {}, onError: () => {} };
		};
		const registry = createPeerRegistry(peersDir);
		const spawner = createPeerSpawner({
			registry,
			outbox: fakeOutbox([]),
			cwd: () => peersDir,
			exec,
			signal: controller.signal,
		});
		await expect(spawner({ name: "never-started", headless: true })).rejects.toMatchObject({
			name: "PeerSpawnError",
			kind: "spawn_failed",
			message: "spawn aborted: session is shutting down",
		});
		expect(execCalls).toBe(0);
	});

	it("abort mid-wait: rejects promptly and the poll loop stops", async () => {
		// The child stub never registers; only the abort (not the long register
		// timeout) may end this wait.
		const controller = new AbortController();
		const registry = createPeerRegistry(peersDir);
		const readRecord = registry.readRecord.bind(registry);
		let polls = 0;
		registry.readRecord = (pid) => {
			polls += 1;
			// Shut the session down mid-wait, after two poll iterations.
			if (polls === 2) controller.abort();
			return readRecord(pid);
		};
		const spawner = createPeerSpawner({
			registry,
			outbox: fakeOutbox([]),
			cwd: () => peersDir,
			registerTimeoutMs: 30_000,
			exec: () => ({ pid: 12345, kill: () => {}, onError: () => {} }),
			signal: controller.signal,
		});
		const startedAt = Date.now();
		await expect(spawner({ name: "held-worker", headless: true })).rejects.toMatchObject({
			name: "PeerSpawnError",
			kind: "spawn_failed",
			message: "spawn aborted: session is shutting down",
		});
		expect(Date.now() - startedAt).toBeLessThan(2_000);
		expect(polls).toBe(2);
		// The loop is really dead: 400ms exceeds the 300ms poll interval, so a
		// zombie loop would land another readRecord here.
		const { promise: settleWindow, resolve: settlePassed } = Promise.withResolvers<void>();
		setTimeout(settlePassed, 400);
		await settleWindow;
		expect(polls).toBe(2);
	});

	it("headless: ignores a stale same-name record for the child pid", async () => {
		// A recycled pid's older record: same assigned name, startedAt 10s before
		// the spawn call — outside the 5s grace window, must not be matched. Planted
		// inside exec (AFTER name assignment) so the requested name survives
		// pickUniqueName and the only difference from a fresh record is startedAt.
		const staleExec: PeerExec = (_command, _args, options) => {
			plantRecordWithKey(
				fakeRecord(options.env[ENV_PEERS_NAME] ?? "unset", process.pid, { startedAt: Date.now() - 10_000 }),
			);
			return { pid: process.pid, kill: () => {}, onError: () => {} };
		};
		const registry = createPeerRegistry(peersDir);
		const spawner = createPeerSpawner({
			registry,
			outbox: fakeOutbox([]),
			cwd: () => peersDir,
			registerTimeoutMs: 300,
			exec: staleExec,
		});
		await expect(spawner({ name: "stale-worker", headless: true })).rejects.toMatchObject({
			kind: "register_timeout",
		});
	});

	it("headless: ignores a pid-reused record publishing a foreign name", async () => {
		// The child pid already has a LIVE record — but under a name we did not
		// assign (foreign process grabbed the recycled pid): keep waiting, do
		// not adopt it, and time out.
		plantRecordWithKey(fakeRecord("foreign-owner", process.pid));
		const registry = createPeerRegistry(peersDir);
		const spawner = createPeerSpawner({
			registry,
			outbox: fakeOutbox([]),
			cwd: () => peersDir,
			registerTimeoutMs: 300,
			exec: () => ({ pid: process.pid, kill: () => {}, onError: () => {} }),
		});
		await expect(spawner({ name: "ghost", headless: true })).rejects.toMatchObject({
			kind: "register_timeout",
		});
	});

	it("real process: a node stub registers and is discovered", async () => {
		// The stub writes its own registry record (its real pid) then stays alive.
		const stub = `
			const { writeFileSync } = require("node:fs");
			const { createHash } = require("node:crypto");
			const { hostname } = require("node:os");
			const { join } = require("node:path");
			const rec = {
				version: 1, pid: process.pid, sessionId: "stub", cwd: process.cwd(),
				name: process.env.${ENV_PEERS_NAME}, nameSource: "env", formerNames: [],
				kind: "rpc", status: "idle", statusUpdatedAt: Date.now(), startedAt: Date.now(),
				pidDomain: "win32:" + hostname().toLowerCase(),
				socketPath: "\\\\\\\\.\\\\pipe\\\\LOCAL\\\\stub", peerProtocol: 1, features: [],
			};
			const keyName = process.pid + "." + createHash("sha256").update(rec.socketPath).digest("hex") + ".key";
			writeFileSync(join(process.env.${ENV_PEERS_DIR}, keyName), "{}");
			writeFileSync(join(process.env.${ENV_PEERS_DIR}, process.pid + ".json"), JSON.stringify(rec));
			setInterval(() => {}, 1000);
		`;
		let child: ChildProcess | undefined;
		const realExec: PeerExec = (_command, _args, options) => {
			// cwd must NOT be the registry dir: on Windows a live child's cwd pins
			// the directory and afterEach rmSync fails with EPERM.
			child = nodeSpawn(process.execPath, ["-e", stub], {
				cwd: tmpdir(),
				env: options.env,
				stdio: "ignore",
			});
			return { pid: child.pid, kill: () => child?.kill(), onError: (cb) => child?.once("error", cb) };
		};
		const registry = createPeerRegistry(peersDir);
		const spawner = createPeerSpawner({ registry, outbox: fakeOutbox([]), cwd: () => peersDir, exec: realExec });
		try {
			const outcome = await spawner({ name: "stub-worker", headless: true });
			expect(outcome.name).toBe("stub-worker");
			expect(outcome.pid).toBe(child?.pid);
		} finally {
			child?.kill();
		}
	}, 20_000);

	it("visible spawn on POSIX: actionable spawn_failed instead of a cryptic ENOENT", async () => {
		// The guard is platform-conditional and Windows CI cannot hit the real
		// POSIX path, so simulate one: patch process.platform (defaultExec reads
		// it per call, never captured at module load). Without the guard,
		// nodeSpawn("cmd.exe", …) fails ENOENT and the caller would get a
		// generic "failed to launch: spawn cmd.exe ENOENT".
		const realPlatform = process.platform;
		const descriptor = Object.getOwnPropertyDescriptor(process, "platform") ?? {
			enumerable: true,
			configurable: true,
			value: realPlatform,
		};
		Object.defineProperty(process, "platform", { ...descriptor, value: "linux" });
		try {
			const registry = createPeerRegistry(peersDir);
			// defaultExec (no injected exec) + headless omitted → visible branch.
			const spawner = createPeerSpawner({ registry, outbox: fakeOutbox([]), cwd: () => peersDir });
			// The guard throws directly: the exact message reaches the caller
			// unwrapped (not the generic "failed to launch: …" onError wrap).
			await expect(spawner({ name: "visible-worker" })).rejects.toMatchObject({
				name: "PeerSpawnError",
				kind: "spawn_failed",
				message: "visible (windowed) spawn is Windows-only for now; pass headless: true to spawn a background worker on this platform",
			});
		} finally {
			Object.defineProperty(process, "platform", { ...descriptor, value: realPlatform });
		}
	});

	it("register timeout: a real headless child that never registers is killed", async () => {
		// REAL exec (no fake): the child is a live node keeper (setInterval, no
		// registry record ever). The spawner must time out AND its cleanup path
		// must actually kill the child. Liveness is proven by awaiting the
		// child's own "exit" event (the platform's authoritative dead-signal)
		// rather than a wall-clock sleep; vitest's 20s test timeout bounds a
		// failed kill. kill(pid, 0) afterwards must throw ESRCH.
		let child: ChildProcess | undefined;
		// Gate that resolves on the child's real "exit" — attached at spawn
		// time (before the spawner can kill) so no exit can slip past us.
		const { promise: exited, resolve: markExited } = Promise.withResolvers<void>();
		const keeperExec: PeerExec = (_command, _args, options) => {
			// Swap the host command for the keeper stub while honoring the
			// headless exec contract: detached, cwd outside the registry dir
			// (a live child's cwd would pin peersDir against rmSync), stdin
			// pipe held open.
			const proc = nodeSpawn(process.execPath, ["-e", "setInterval(() => {}, 1000);"], {
				cwd: tmpdir(),
				env: options.env,
				detached: true,
				stdio: ["pipe", "ignore", "ignore"],
			});
			proc.stdin?.on("error", () => {}); // Held open, never written.
			// The keeper cannot die on its own (a bare interval): its exit can
			// only be the spawner's cleanup kill.
			proc.once("exit", () => markExited());
			child = proc;
			return { pid: proc.pid, kill: () => proc.kill(), onError: (cb) => proc.once("error", cb) };
		};
		const registry = createPeerRegistry(peersDir);
		const spawner = createPeerSpawner({
			registry,
			outbox: fakeOutbox([]),
			cwd: () => tmpdir(),
			registerTimeoutMs: 300,
			exec: keeperExec,
		});
		try {
			await expect(spawner({ name: "never-registers", headless: true })).rejects.toMatchObject({
				name: "PeerSpawnError",
				kind: "register_timeout",
			});
			const pid = child?.pid;
			if (child === undefined || pid === undefined) throw new Error("keeper never spawned");
			// A leaked (never-killed) child hangs here and the 20s test timeout
			// names the leak — no wall-clock sleep involved.
			await exited;
			expect(() => process.kill(pid, 0)).toThrow();
		} finally {
			// Belt and braces: never leak the keeper even when an assertion fails.
			child?.kill();
		}
	}, 20_000);

	it("register timeout message reports a dead headless child with stderr guidance", async () => {
		// A deterministically dead pid: a short-lived node process that exits
		// immediately (its pid may not be recycled this fast, but the exit event
		// guarantees the process is gone before the 300ms register window ends).
		const { promise: dead, resolve: markDead } = Promise.withResolvers<void>();
		let deadPid: number | undefined;
		const dyingExec: PeerExec = (_command, _args, options) => {
			const proc = nodeSpawn(process.execPath, ["-e", "process.exit(0);"], {
				cwd: tmpdir(),
				env: options.env,
				stdio: "ignore",
			});
			deadPid = proc.pid;
			proc.once("exit", () => markDead());
			return { pid: proc.pid, kill: () => proc.kill(), onError: (cb) => proc.once("error", cb) };
		};
		const registry = createPeerRegistry(peersDir);
		const spawner = createPeerSpawner({
			registry,
			outbox: fakeOutbox([]),
			cwd: () => peersDir,
			registerTimeoutMs: 300,
			exec: dyingExec,
		});
		// Race-free ordering: the spawn call starts the child, then we await its
		// real exit event; the error's promise settles only after the register
		// deadline (>= 300ms), which the exited child comfortably precedes on
		// the event loop. The probe then sees ESRCH deterministically.
		const errorPromise = spawner({ name: "dead-worker", headless: true }).then(
			() => {
				throw new Error("expected register_timeout");
			},
			(e: unknown) => e,
		);
		await dead;
		const error = await errorPromise;
		expect(error).toBeInstanceOf(PeerSpawnError);
		expect(error).toMatchObject({ kind: "register_timeout" });
		const message = (error as PeerSpawnError).message;
		expect(message).toContain('spawned "dead-worker" but it did not register within 0s');
		expect(message).toContain("has exited");
		expect(message).toContain("run with a visible window or check the child's stderr via the host");
	});

	it("register timeout message reports a live headless child and differs from the dead variant", async () => {
		// Live keeper as the child: probe says "is still running".
		const keeper = nodeSpawn(process.execPath, ["-e", "setInterval(() => {}, 1000);"], {
			cwd: tmpdir(),
			stdio: "ignore",
		});
		try {
			const registry = createPeerRegistry(peersDir);
			const spawner = createPeerSpawner({
				registry,
				outbox: fakeOutbox([]),
				cwd: () => peersDir,
				registerTimeoutMs: 300,
				exec: () => ({ pid: keeper.pid, kill: () => {}, onError: () => {} }),
			});
			const liveError = await spawner({ name: "live-worker", headless: true }).then(
				() => {
					throw new Error("expected register_timeout");
				},
				(e: unknown) => e,
			);
			expect(liveError).toBeInstanceOf(PeerSpawnError);
			const liveMessage = (liveError as PeerSpawnError).message;
			expect(liveMessage).toContain("is still running");
			expect(liveMessage).toContain("run with a visible window or check the child's stderr via the host");

			// Same scenario, dead pid (deterministically ESRCH — a pid that has
			// exited and is not recycled by the keeper above).
			const deadKeeper = nodeSpawn(process.execPath, ["-e", "process.exit(0);"], {
				cwd: tmpdir(),
				stdio: "ignore",
			});
			await new Promise<void>((resolve) => deadKeeper.once("exit", () => resolve()));
			const deadPid = deadKeeper.pid;
			if (deadPid === undefined) throw new Error("dead keeper never spawned");
			expect(() => process.kill(deadPid, 0)).toThrow();
			const spawner2 = createPeerSpawner({
				registry: createPeerRegistry(peersDir),
				outbox: fakeOutbox([]),
				cwd: () => peersDir,
				registerTimeoutMs: 300,
				exec: () => ({ pid: deadPid, kill: () => {}, onError: () => {} }),
			});
			const deadError = await spawner2({ name: "live-worker", headless: true }).then(
				() => {
					throw new Error("expected register_timeout");
				},
				(e: unknown) => e,
			);
			expect(deadError).toBeInstanceOf(PeerSpawnError);
			const deadMessage = (deadError as PeerSpawnError).message;
			expect(deadMessage).toContain("has exited");
			// The two variants differ exactly on the aliveness phrase.
			expect(liveMessage).not.toBe(deadMessage);
			expect(liveMessage.replace("is still running", "X")).not.toContain("has exited");
			expect(deadMessage.replace("has exited", "X")).not.toContain("is still running");
		} finally {
			keeper.kill();
		}
	});

	it("register timeout message for a visible spawn points at the spawned window", async () => {
		// Visible path (headless omitted): the deadline runs out scanning for
		// the assigned name that never appears. The stub pid is this process
		// (alive) — visible spawns can't poll the pid, the probe still reports it.
		const registry = createPeerRegistry(peersDir);
		const spawner = createPeerSpawner({
			registry,
			outbox: fakeOutbox([]),
			cwd: () => peersDir,
			registerTimeoutMs: 300,
			exec: () => ({ pid: process.pid, kill: () => {}, onError: () => {} }),
		});
		const error = await spawner({ name: "window-worker" }).then(
			() => {
				throw new Error("expected register_timeout");
			},
			(e: unknown) => e,
		);
		expect(error).toBeInstanceOf(PeerSpawnError);
		expect(error).toMatchObject({ kind: "register_timeout" });
		const message = (error as PeerSpawnError).message;
		expect(message).toContain("is still running");
		expect(message).toContain("check the spawned window for a trust dialog or startup error");
		// The headless-only guidance must not leak into the visible variant.
		expect(message).not.toContain("stderr");
	});

	// -- model arg -----------------------------------------------------------

	it("model given: exec args carry --model followed by the value", async () => {
		const captured: { args?: string[] } = {};
		const registry = createPeerRegistry(peersDir);
		const spawner = createPeerSpawner({
			registry,
			outbox: fakeOutbox([]),
			cwd: () => peersDir,
			exec: fakeExecWithArgs(captured),
		});
		const outcome = await spawner({ name: "model-worker", model: "aliyun/kimi-k3" });
		expect(outcome.name).toBe("model-worker");
		// Adjacency, not just membership: --model must be immediately followed
		// by its value (a standalone "--model" assertion would pass even if the
		// value landed elsewhere or never).
		const idx = captured.args?.indexOf("--model");
		expect(idx).toBeDefined();
		expect(captured.args?.[idx! + 1]).toBe("aliyun/kimi-k3");
	});

	it("no model given: exec args contain no --model", async () => {
		const captured: { args?: string[] } = {};
		const registry = createPeerRegistry(peersDir);
		const spawner = createPeerSpawner({
			registry,
			outbox: fakeOutbox([]),
			cwd: () => peersDir,
			exec: fakeExecWithArgs(captured),
		});
		await spawner({ name: "default-model-worker" });
		expect(captured.args?.includes("--model")).toBe(false);
	});

	it("trims surrounding whitespace off the model value", async () => {
		const captured: { args?: string[] } = {};
		const registry = createPeerRegistry(peersDir);
		const spawner = createPeerSpawner({
			registry,
			outbox: fakeOutbox([]),
			cwd: () => peersDir,
			exec: fakeExecWithArgs(captured),
		});
		await spawner({ name: "trim-worker", model: "  k3  " });
		const idx = captured.args?.indexOf("--model");
		expect(captured.args?.[idx! + 1]).toBe("k3");
	});

	it("invalid models are rejected before any exec call", async () => {
		// Each of these must fail the pre-spawn validation: a leading '-' would
		// turn the value into an arbitrary CLI flag on the child host, empty /
		// whitespace-only and spaced values are meaningless ids, and >200 chars
		// is abuse of a fuzzy-match pattern.
		for (const model of ["-x", "", "  ", "a b", "--dangerously-skip-permissions", "a".repeat(201)]) {
			let execCalls = 0;
			const exec: PeerExec = () => {
				execCalls += 1;
				return { pid: 1, kill: () => {}, onError: () => {} };
			};
			const registry = createPeerRegistry(peersDir);
			const spawner = createPeerSpawner({ registry, outbox: fakeOutbox([]), cwd: () => peersDir, exec });
			await expect(spawner({ name: "bad-model", model })).rejects.toMatchObject({
				name: "PeerSpawnError",
				kind: "spawn_failed",
			});
			// Validation happens BEFORE spawn: no child may ever be launched with
			// a crafted value.
			expect(execCalls).toBe(0);
			// The rejection names the rejected value so the caller can fix it.
			await expect(spawner({ name: "bad-model", model })).rejects.toThrow(/invalid model/);
		}
	});

	it("valid model edge cases pass validation", () => {
		// Bare fuzzy names, provider-qualified ids, and thinking-suffixed ids.
		expect(assertValidModelArg("k3")).toBe("k3");
		expect(assertValidModelArg("aliyun/kimi-k3")).toBe("aliyun/kimi-k3");
		expect(assertValidModelArg("zai-org/GLM-5.1-FP8:high")).toBe("zai-org/GLM-5.1-FP8:high");
		expect(assertValidModelArg("gpt-5.2")).toBe("gpt-5.2");
		// 200 chars is still inside the limit.
		expect(assertValidModelArg("a".repeat(200))).toBe("a".repeat(200));
	});

	it("child that exits before registering rejects fast with its stderr tail", async () => {
		// An invalid --model makes the real host print a diagnostic to stderr
		// and exit(1) before rpc mode starts. The spawner must surface that
		// immediately — not after the 30s register timeout.
		const stderr = 'Model "bogus" not found. Use --list-models to see available models.';
		const exec: PeerExec = () => ({
			pid: 4321,
			kill: () => {},
			onError: () => {},
			// Fire the exit synchronously at spawn time: the loop's first
			// iteration must observe it.
			onExit: (cb) => cb(1),
			stderrTail: () => stderr,
		});
		const registry = createPeerRegistry(peersDir);
		const spawner = createPeerSpawner({
			registry,
			outbox: fakeOutbox([]),
			cwd: () => peersDir,
			registerTimeoutMs: 30_000,
			exec,
		});
		const startedAt = Date.now();
		const error = await spawner({ name: "model-reject", headless: true, model: "bogus" }).then(
			() => {
				throw new Error("expected spawn_failed");
			},
			(e: unknown) => e,
		);
		expect(error).toBeInstanceOf(PeerSpawnError);
		expect(error).toMatchObject({ kind: "spawn_failed" });
		const message = (error as PeerSpawnError).message;
		expect(message).toContain('spawned "model-reject" but it exited (code 1) before registering');
		expect(message).toContain(stderr);
		expect(message).toContain("the child host rejected its startup arguments");
		// FAST: far under the 30s register timeout a dead child would otherwise
		// burn. Generous margin keeps the assertion robust on slow CI.
		expect(Date.now() - startedAt).toBeLessThan(5_000);
	});

	it("child that stays alive but never registers still times out", async () => {
		// onExit never fires: the historic register_timeout path must stay
		// intact for alive-but-stalled children.
		const exec: PeerExec = () => ({
			pid: 5555,
			kill: () => {},
			onError: () => {},
			onExit: () => {},
			stderrTail: () => "",
		});
		const registry = createPeerRegistry(peersDir);
		const spawner = createPeerSpawner({
			registry,
			outbox: fakeOutbox([]),
			cwd: () => peersDir,
			registerTimeoutMs: 300,
			exec,
		});
		await expect(spawner({ name: "stalled", headless: true })).rejects.toMatchObject({
			kind: "register_timeout",
		});
	});

	// Windows-only: the visible branch runs `cmd.exe /c start`, which does not
	// exist on POSIX (the POSIX guard has its own test above). Patching
	// process.platform on Linux would spawn a missing binary and hang waiting
	// for stub output that never appears.
	it.skipIf(process.platform !== "win32")("visible spawn with model: cmd start args include the --model value", async () => {
		// defaultExec's visible branch runs `cmd /c start "" <command> --model k3`
		// where <command> comes from hostCommand(process.argv, …). No real host
		// session is launched: patch process.argv so the "host" is a node stub
		// that records its own argv and exits — the spawner then times out
		// waiting for a record that never appears. (The brief `start` window is
		// the same one every visible spawn opens; on headless CI none appears.)
		const realPlatform = process.platform;
		const platformDescriptor = Object.getOwnPropertyDescriptor(process, "platform") ?? {
			enumerable: true,
			configurable: true,
			value: realPlatform,
		};
		const realArgv = process.argv;
		const argvDescriptor = Object.getOwnPropertyDescriptor(process, "argv") ?? {
			enumerable: true,
			configurable: true,
			value: realArgv,
		};
		// Stub files and the child's cwd live OUTSIDE peersDir: a real
		// `cmd /c start` child pins its cwd, and Windows refuses to delete a
		// directory that is a live (or just-exited) process's cwd — mixing the
		// two flaked the afterEach rm with EPERM.
		const stubDir = mkdtempSync(join(tmpdir(), "pi-peers-stub-"));
		const argvFile = join(stubDir, "cmdstub-argv.txt");
		const stub = join(stubDir, "cmdstub.cjs");
		writeFileSync(
			stub,
			// chdir FIRST: a spawned process pins its cwd, and Windows refuses to
			// delete a pinned directory. Leaving stubDir immediately makes the
			// finally-cleanup independent of when the start-chain fully exits.
			`process.chdir(${JSON.stringify(tmpdir())}); ` +
				"require('node:fs').writeFileSync(__dirname + '/cmdstub-argv.txt', process.argv.slice(2).join('\\n')); setImmediate(() => process.exit(0));",
		);
		// hostCommand: argv[1] (the stub, a .cjs) becomes the child script, so
		// `start` launches: node <stub.cjs> --model k3 [--mode rpc is headless-only].
		Object.defineProperty(process, "argv", { ...argvDescriptor, value: [process.execPath, stub] });
		Object.defineProperty(process, "platform", { ...platformDescriptor, value: "win32" });
		try {
			const registry = createPeerRegistry(peersDir);
			const spawner = createPeerSpawner({
				registry,
				outbox: fakeOutbox([]),
				cwd: () => stubDir,
				registerTimeoutMs: 300,
			});
			await expect(spawner({ name: "visible-model", model: "k3" })).rejects.toMatchObject({
				kind: "register_timeout",
			});
			// The started host's own argv must carry the model as an adjacent
			// --model <value> pair. The stub may still be racing the 300ms
			// deadline: await its output deterministically instead of assuming.
			const startArgs = (await waitForFile(argvFile)).split("\n");
			const idx = startArgs.indexOf("--model");
			expect(idx).toBeGreaterThanOrEqual(0);
			expect(startArgs[idx + 1]).toBe("k3");
		} finally {
			Object.defineProperty(process, "argv", { ...argvDescriptor, value: realArgv });
			Object.defineProperty(process, "platform", { ...platformDescriptor, value: realPlatform });
			rmSync(stubDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
		}
	});
});
