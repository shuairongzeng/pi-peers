/**
 * peers-spawn.test.ts — spawn engine: command reproduction, env contract,
 * unique naming, registration wait, timeout, and the real-process path.
 */

import { type ChildProcess, spawn as nodeSpawn } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { hostname, tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createPeerRegistry } from "../src/registry.ts";
import { createPeerSpawner, hostCommand, type PeerExec, type SpawnedProcess } from "../src/spawn.ts";
import { ENV_PEERS_DIR, ENV_PEERS_EPHEMERAL, ENV_PEERS_NAME, PeerSendError, PeerSpawnError, type PeerRecord } from "../src/types.ts";

let peersDir: string;

beforeEach(() => {
	peersDir = mkdtempSync(join(tmpdir(), "pi-peers-spawn-"));
	process.env[ENV_PEERS_DIR] = peersDir;
});

afterEach(() => {
	delete process.env[ENV_PEERS_DIR];
	rmSync(peersDir, { recursive: true, force: true });
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
});
