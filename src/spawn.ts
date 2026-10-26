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
 * end the session), stdout ignored and stderr captured to a rolling tail —
 * a child that dies before registering (e.g. an invalid `--model`) fails
 * fast with that tail instead of a registration timeout. Visible children
 * get their own terminal window so the user can watch or intervene.
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
	/**
	 * Subscribe to the child's exit (code null when killed by a signal).
	 * Optional: fake launchers in tests may omit it, in which case a child
	 * that dies before registering is diagnosed at the register timeout
	 * (the historic path) instead of fail-fast.
	 */
	onExit?(cb: (code: number | null) => void): void;
	/**
	 * Last few KB of the child's captured stderr ("" when nothing was
	 * captured — visible-window spawns and stderr-less launchers).
	 */
	stderrTail?(): string;
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
			// stdout stays ignored: the child's rpc protocol stream must never
			// sit in an unread pipe. stderr is captured (below) so a startup
			// failure (e.g. an invalid --model) can be surfaced in the spawn
			// error instead of dying with a 30s registration timeout.
			stdio: ["pipe", "ignore", "pipe"],
		});
		// The child's rpc session lives as long as stdin stays open; hold it.
		child.stdin?.on("error", () => {});
		// Rolling tail of the child's stderr, capped so a chatty child cannot
		// grow memory without bound — only the last few KB ever matter for
		// diagnosing a startup failure.
		const STDERR_TAIL_LIMIT = 8 * 1024;
		let stderrTailText = "";
		child.stderr?.on("data", (chunk: Buffer) => {
			stderrTailText = (stderrTailText + chunk.toString("utf8")).slice(-STDERR_TAIL_LIMIT);
		});
		child.stderr?.on("error", () => {});
		return {
			pid: child.pid,
			kill: () => child.kill(),
			onError: (cb) => child.once("error", cb),
			onExit: (cb) => child.once("exit", (code) => cb(code)),
			stderrTail: () => stderrTailText,
		};
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

/**
 * Validate a caller-supplied --model value before it reaches the child host's
 * CLI. The value is model-facing (it comes from a tool schema), so anything
 * that could re-shape the child's command line must be rejected up front: a
 * leading '-' would let a crafted value become an arbitrary CLI flag, and
 * whitespace/shell metacharacters have no business in a model id. Accepted:
 * provider-qualified ids ("aliyun/kimi-k3"), thinking-suffixed ids
 * ("zai-org/GLM-5.1-FP8:high") and bare short names ("k3").
 */
const MODEL_ARG_RE = /^[A-Za-z0-9][A-Za-z0-9._\/@:+~-]*$/;

export function assertValidModelArg(model: string): string {
	const trimmed = model.trim();
	let reason: string | undefined;
	if (trimmed === "") reason = "empty or whitespace-only";
	else if (trimmed.length > 200) reason = "longer than 200 characters";
	else if (!MODEL_ARG_RE.test(trimmed)) reason = "must be alphanumeric first, then [A-Za-z0-9._/:@:+~-] only (no leading '-', whitespace, or shell metacharacters)";
	if (reason !== undefined) {
		throw new PeerSpawnError("spawn_failed", `invalid model "${model}": ${reason}`);
	}
	return trimmed;
}

/**
 * Registration-timeout diagnostics: whether the child is still alive (a bare
 * kill(pid, 0) probe, checked here rather than in the registry so spawn keeps
 * only its types.ts deps) plus one actionable next step. A dead child usually
 * died at the host's "Trust project folder?" dialog or a model-data/TUI
 * startup failure the parent agent cannot see; a live one stalled before
 * publishing its record.
 */
function registerTimeoutMessage(
	name: string,
	timeoutMs: number,
	child: SpawnedProcess,
	headless: boolean,
): string {
	let alive = false;
	if (child.pid !== undefined) {
		try {
			process.kill(child.pid, 0);
			alive = true;
		} catch (err) {
			// EPERM: exists but not signalable by us — still alive (registry idiom).
			alive = (err as NodeJS.ErrnoException).code === "EPERM";
		}
	}
	const base = `spawned "${name}" but it did not register within ${Math.round(timeoutMs / 1000)}s`;
	if (headless) {
		return `${base}; the child process ${alive ? "is still running" : "has exited"} — run with a visible window or check the child's stderr via the host to see why.`;
	}
	return `${base}; the child process ${alive ? "is still running" : "has exited"} — check the spawned window for a trust dialog or startup error.`;
}

export function createPeerSpawner(deps: PeerSpawnerDeps): PeerSpawner {
	const exec = deps.exec ?? defaultExec;
	const timeoutMs = deps.registerTimeoutMs ?? 30_000;

	return async (opts: PeerSpawnOptions): Promise<PeerSpawnOutcome> => {
		// The model arg is model-facing CLI input: validate it BEFORE anything
		// else (name assignment, child spawn) so a crafted value can never
		// reach the child host's command line.
		const model = opts.model !== undefined ? assertValidModelArg(opts.model) : undefined;
		const name = pickUniqueName(deps.registry, opts.name);
		const [command, ...baseArgs] = hostCommand(process.argv, process.execPath, process.execArgv);
		const args = model !== undefined ? [...baseArgs, "--model", model] : baseArgs;
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
		// Track the child's exit (optional member: launchers without it keep
		// the historic timeout-only diagnosis). undefined = not exited yet or
		// not observable.
		let exitCode: number | null | undefined;
		child.onExit?.((code) => {
			exitCode = code;
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
			// Fail fast on a dead headless child: a child that exits before
			// publishing its record will never publish it (an invalid --model
			// makes the host print its diagnostic and exit(1) before rpc mode
			// starts). Waiting out the full register timeout would only hide
			// the real cause, so surface the exit plus the captured stderr
			// tail right away; alive-but-stalled children keep the timeout path.
			// Checked AFTER the record poll: a registration raced against an
			// exit must still win.
			if (headless && exitCode !== undefined && entry === undefined) {
				const tail = child.stderrTail?.() ?? "";
				const tailNote =
					tail.trim() !== ""
						? `: ${tail.trim().slice(-300)} — the child host rejected its startup arguments`
						: " — the child host rejected its startup arguments";
				throw new PeerSpawnError(
					"spawn_failed",
					`spawned "${name}" but it exited (code ${exitCode}) before registering${tailNote}`,
				);
			}
			if (entry !== undefined) break;
			await sleep(300, deps.signal);
		}
		if (entry === undefined) {
			if (headless) child.kill();
			throw new PeerSpawnError("register_timeout", registerTimeoutMessage(name, timeoutMs, child, headless));
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
