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
	/**
	 * Aborts an in-flight waitForRegistration: the while+sleep poll loop would
	 * otherwise run to its full timeout on a session that is already dying.
	 * Optional — callers without a session lifetime pass nothing.
	 */
	signal?: AbortSignal;
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
	// POSIX: `cmd.exe` does not exist, so this spawn would fail with a cryptic
	// ENOENT "spawn_failed". Guard on platform BEFORE spawning and throw
	// directly — createPeerSpawner is an async function, so the error
	// propagates to its caller unchanged (the "failed to launch:" wrap is for
	// errors the wait loop observes via onError; a pre-flight check has no
	// child to clean up and must not wait for the poll loop). The check reads
	// the global `process` per call (never captured at module load) so tests
	// can simulate a POSIX host by patching process.platform.
	if (process.platform !== "win32") {
		throw new PeerSpawnError(
			"spawn_failed",
			"visible (windowed) spawn is Windows-only for now; pass headless: true to spawn a background worker on this platform",
		);
	}
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
	// fresh: two rapid spawns must not both miss the other's brand-new record
	// through the short-TTL scan cache.
	const taken = new Set(registry.list({ fresh: true }).map((entry) => entry.record.name));
	if (!taken.has(base)) return base;
	for (let i = 2; ; i++) {
		const candidate = `${base}-${i}`;
		if (!taken.has(candidate)) return candidate;
	}
}

function randomSuffix(): string {
	return Math.random().toString(36).slice(2, 6);
}

/**
 * Abortable sleep: settles immediately when the signal fires (the caller's
 * next loop-iteration abort check then throws) — otherwise after `ms`.
 */
function sleep(ms: number, signal?: AbortSignal): Promise<void> {
	const { promise, resolve } = Promise.withResolvers<void>();
	const timer = setTimeout(resolve, ms);
	const onAbort = () => {
		clearTimeout(timer);
		resolve();
	};
	// One listener per 300ms step would stack ~100 of them over a 30s wait and
	// trip Node's MaxListeners warning: always detach on settle. `once` does not
	// cover the timer expiry path, so detach covers both settle routes.
	if (signal?.aborted === true) onAbort();
	else signal?.addEventListener("abort", onAbort, { once: true });
	promise.then(
		() => signal?.removeEventListener("abort", onAbort),
		() => signal?.removeEventListener("abort", onAbort),
	);
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
		// A session already shutting down must not start a child at all — the
		// loop's in-flight abort only covers the wait phase.
		if (deps.signal?.aborted === true) {
			throw new PeerSpawnError("spawn_failed", "spawn aborted: session is shutting down");
		}

		let spawnError: Error | undefined;
		const child = exec(command, args, { cwd, env, headless: opts.headless === true });
		child.onError((err) => {
			spawnError = err;
		});

		// Wait for the child's plugin to publish its registry record. Match on
		// the name we assigned plus startedAt ≥ spawn time minus grace (stale
		// same-name records from earlier runs — or a recycled pid's older
		// record — must not count).
		const normalizedName = deps.registry.normalizeName(name);
		const deadline = spawnedAt + timeoutMs;
		const graceFloor = spawnedAt - 5_000;
		const headless = opts.headless === true;
		let entry: PeerRegistryEntry | undefined;
		while (Date.now() < deadline) {
			// A dying session must not keep polling (nor sleep out the loop's
			// 300ms steps) up to the full register timeout: abort as soon as
			// shutdown is signalled. The timeout path below still applies on
			// its own. Plain member reads (not ?.aborted === true) — TS keeps
			// `aborted` boolean, so the flag can flip mid-loop without the
			// pre-spawn narrow-check making this branch look unreachable.
			if (deps.signal !== undefined && deps.signal.aborted) {
				if (headless) child.kill();
				throw new PeerSpawnError("spawn_failed", "spawn aborted: session is shutting down");
			}
			if (spawnError !== undefined) {
				throw new PeerSpawnError("spawn_failed", `failed to launch: ${spawnError.message}`);
			}
			if (headless && child.pid !== undefined) {
				// Direct child: poll just its record by pid. O(1) file read — no
				// directory scan, no self-healing side effects, no scan cache.
				const candidate = deps.registry.readRecord(child.pid);
				// A record under a DIFFERENT name means the pid was reused by a
				// foreign process between spawn and registration: keep waiting
				// (for the child's own record to replace it) until the deadline.
				if (
					candidate !== undefined &&
					candidate.record.name === normalizedName &&
					candidate.record.startedAt >= graceFloor
				) {
					entry = candidate;
				}
			} else {
				// Visible spawn: `cmd /c start` hides the real host pid behind the
				// intermediate shell (see defaultExec), so pid polling cannot
				// work — fall back to name matching over a real (uncached) scan.
				entry = deps.registry
					.list({ fresh: true })
					.find((c) => c.record.name === normalizedName && c.record.startedAt >= graceFloor);
			}
			if (entry !== undefined) break;
			await sleep(300, deps.signal);
		}
		if (entry === undefined) {
			if (headless) child.kill();
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

		// Finding the record (readRecord / fresh list above) does not refresh the
		// parent's cached /peers listing: without this forced rescan, callers could
		// miss the new peer for the scan cache's TTL window.
		deps.registry.list({ fresh: true });
		return { pid: entry.record.pid, name: entry.record.name, ref: deps.registry.refOf(entry.record), taskSent };
	};
}
