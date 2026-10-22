/**
 * spawn.ts — launching a new host instance as a managed peer.
 *
 * The spawner reproduces the current process's own invocation (executable +
 * loader flags + script entry) so the child runs the same host build the
 * user is running, then waits for the child's plugin to publish its registry
 * record. Every spawned child gets a registry-unique name via PI_PEERS_NAME
 * (auto-generated `worker-xxxx` when the caller omits one) — the name is the
 * rendezvous key, since a visible-window launch (`cmd /c start`) hides the
 * real child pid behind the intermediate shell.
 *
 * Headless children run `--mode rpc` with stdin held open (closing it would
 * end the session) and stdio otherwise ignored. Visible children get their
 * own terminal window so the user can watch or intervene.
 */

import { spawn as nodeSpawn } from "node:child_process";
import {
	ENV_PEERS_EPHEMERAL,
	ENV_PEERS_NAME,
	ENV_PEERS_SOCKET,
	ENV_PEERS_TOKEN,
	type PeerOutbox,
	type PeerRegistry,
	type PeerRegistryEntry,
	PeerSpawnError,
	type PeerSpawnOptions,
	type PeerSpawnOutcome,
	type PeerSpawner,
} from "./types.ts";

/** Handle for a launched child process; the real implementation wraps child_process. */
export interface SpawnedProcess {
	pid: number | undefined;
	kill(): void;
	onError(cb: (err: Error) => void): void;
}

/** Injectable process launcher — tests substitute a fake or a node stub. */
export type PeerExec = (
	command: string,
	args: string[],
	options: { cwd: string; env: NodeJS.ProcessEnv; headless: boolean },
) => SpawnedProcess;

export interface PeerSpawnerDeps {
	registry: PeerRegistry;
	outbox: PeerOutbox;
	/** Working directory for the child (the current session's cwd). */
	cwd: () => string;
	/** How long to wait for the child's registry record before giving up. */
	registerTimeoutMs?: number;
	exec?: PeerExec;
}

/**
 * Reproduce the current process's invocation. Compiled hosts (omp.exe) have
 * no script argument; source-run hosts (node cli.ts) carry loader flags
 * (e.g. `--import source-resolver.ts`) in process.execArgv.
 */
export function hostCommand(argv: string[], execPath: string, execArgv: string[]): string[] {
	const script = argv[1];
	return script !== undefined && /\.(cjs|mjs|mts|ts|js)$/i.test(script)
		? [execPath, ...execArgv, script]
		: [execPath];
}

const defaultExec: PeerExec = (command, args, options) => {
	if (options.headless) {
		const child = nodeSpawn(command, [...args, "--mode", "rpc"], {
			cwd: options.cwd,
			env: options.env,
			detached: true,
			stdio: ["pipe", "ignore", "ignore"],
		});
		// The child's rpc session lives as long as stdin stays open; hold it.
		child.stdin?.on("error", () => {});
		return { pid: child.pid, kill: () => child.kill(), onError: (cb) => child.once("error", cb) };
	}
	// Visible terminal window. `start ""` owns the console; the returned pid is
	// the intermediate shell's and unused — the registry name is the handle.
	const child = nodeSpawn("cmd.exe", ["/c", "start", "", command, ...args], {
		cwd: options.cwd,
		env: options.env,
		detached: true,
		stdio: "ignore",
	});
	child.unref();
	return { pid: child.pid, kill: () => {}, onError: (cb) => child.once("error", cb) };
};

function buildChildEnv(opts: PeerSpawnOptions, name: string): NodeJS.ProcessEnv {
	const env = { ...process.env };
	// A spawned peer is a fresh first-class instance, not our subagent: do not
	// hand it our socket credentials (those mark it as our child).
	delete env[ENV_PEERS_SOCKET];
	delete env[ENV_PEERS_TOKEN];
	// Same reasoning: an ephemeral parent spawning a persistent helper must not
	// pass its own self-destruct flag down.
	delete env[ENV_PEERS_EPHEMERAL];
	env[ENV_PEERS_NAME] = name;
	if (opts.ephemeral === true) env[ENV_PEERS_EPHEMERAL] = "1";
	return env;
}

function pickUniqueName(registry: PeerRegistry, requested: string | undefined): string {
	const base = requested !== undefined && requested.trim() !== "" ? requested.trim() : `worker-${randomSuffix()}`;
	const taken = new Set(registry.list().map((entry) => entry.record.name));
	if (!taken.has(base)) return base;
	for (let i = 2; ; i++) {
		const candidate = `${base}-${i}`;
		if (!taken.has(candidate)) return candidate;
	}
}

function randomSuffix(): string {
	return Math.random().toString(36).slice(2, 6);
}

function sleep(ms: number): Promise<void> {
	const { promise, resolve } = Promise.withResolvers<void>();
	setTimeout(resolve, ms);
	return promise;
}

export function createPeerSpawner(deps: PeerSpawnerDeps): PeerSpawner {
	const exec = deps.exec ?? defaultExec;
	const timeoutMs = deps.registerTimeoutMs ?? 30_000;

	return async (opts: PeerSpawnOptions): Promise<PeerSpawnOutcome> => {
		const name = pickUniqueName(deps.registry, opts.name);
		const [command, ...args] = hostCommand(process.argv, process.execPath, process.execArgv);
		const env = buildChildEnv(opts, name);
		const cwd = deps.cwd();
		const spawnedAt = Date.now();

		let spawnError: Error | undefined;
		const child = exec(command, args, { cwd, env, headless: opts.headless === true });
		child.onError((err) => {
			spawnError = err;
		});

		// Wait for the child's plugin to publish its registry record. Match on
		// the name we assigned plus startedAt ≥ spawn time (stale same-name
		// records from earlier runs must not count).
		const normalizedName = deps.registry.normalizeName(name);
		const deadline = spawnedAt + timeoutMs;
		let entry: PeerRegistryEntry | undefined;
		while (Date.now() < deadline) {
			if (spawnError !== undefined) {
				throw new PeerSpawnError("spawn_failed", `failed to launch: ${spawnError.message}`);
			}
			entry = deps.registry
				.list()
				.find(
					(candidate) =>
						candidate.record.name === normalizedName && candidate.record.startedAt >= spawnedAt - 5_000,
				);
			if (entry !== undefined) break;
			await sleep(300);
		}
		if (entry === undefined) {
			if (opts.headless === true) child.kill();
			throw new PeerSpawnError(
				"register_timeout",
				`spawned "${name}" but it did not register within ${Math.round(timeoutMs / 1000)}s`,
			);
		}

		let taskSent = false;
		if (opts.task !== undefined && opts.task.trim() !== "") {
			try {
				await deps.outbox.send(name, opts.task, { priority: "next" });
				taskSent = true;
			} catch {
				// Registered but the send raced the listener startup; the caller sees
				// taskSent=false and can retry by name.
			}
		}

		return { pid: entry.record.pid, name: entry.record.name, ref: deps.registry.refOf(entry.record), taskSent };
	};
}
