/**
 * Peer registry: file-based discovery of live peer pi instances.
 *
 * Every peer publishes `<peersDir>/<pid>.json` plus zero or more `<pid>.<64hex>.key` auth-key
 * files. Scanning (`list()`) is self-healing: malformed records, records whose embedded pid
 * disagrees with their file name (torn writes), and records whose process is gone are deleted
 * on sight, so a crashed peer never shadows a name.
 */

import { createHash, randomBytes } from "node:crypto";
import { mkdirSync, readdirSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { hostname } from "node:os";
import { basename, dirname, join } from "node:path";
import type { PeerRecord, PeerRegistry, PeerRegistryEntry, PeerResolution } from "./types.ts";
import { PEER_KEY_FILE_RE, PEER_RECORD_FILE_RE } from "./types.ts";

// ---------------------------------------------------------------------------
// Boundary parsing: registry files are written by other processes, possibly
// other versions, so every field is checked before the value is trusted.
// ---------------------------------------------------------------------------

/** Parse a JSON value as a PeerRecord; undefined when malformed. */
function parsePeerRecord(value: unknown): PeerRecord | undefined {
	if (typeof value !== "object" || value === null || Array.isArray(value)) {
		return undefined;
	}
	const r = value as Record<string, unknown>;
	if (r.version !== 1) {
		return undefined;
	}
	if (typeof r.pid !== "number" || !Number.isInteger(r.pid) || r.pid <= 0) {
		return undefined;
	}
	if (typeof r.sessionId !== "string" || r.sessionId === "") {
		return undefined;
	}
	if (typeof r.cwd !== "string" || typeof r.name !== "string" || r.name === "") {
		return undefined;
	}
	if (r.nameSource !== "user" && r.nameSource !== "derived" && r.nameSource !== "env") {
		return undefined;
	}
	if (r.kind !== "interactive" && r.kind !== "rpc" && r.kind !== "print" && r.kind !== "json") {
		return undefined;
	}
	if (r.status !== "busy" && r.status !== "idle") {
		return undefined;
	}
	if (typeof r.statusUpdatedAt !== "number" || !Number.isFinite(r.statusUpdatedAt)) {
		return undefined;
	}
	if (typeof r.startedAt !== "number" || !Number.isFinite(r.startedAt)) {
		return undefined;
	}
	if (r.procStart !== undefined && typeof r.procStart !== "string") {
		return undefined;
	}
	if (typeof r.pidDomain !== "string" || r.pidDomain === "") {
		return undefined;
	}
	if (typeof r.socketPath !== "string" || r.socketPath === "") {
		return undefined;
	}
	if (typeof r.peerProtocol !== "number") {
		return undefined;
	}
	if (
		!Array.isArray(r.formerNames) ||
		!r.formerNames.every(
			(f) =>
				typeof f === "object" &&
				f !== null &&
				!Array.isArray(f) &&
				typeof (f as Record<string, unknown>).name === "string" &&
				typeof (f as Record<string, unknown>).until === "string",
		)
	) {
		return undefined;
	}
	if (!Array.isArray(r.features) || !r.features.every((f) => typeof f === "string")) {
		return undefined;
	}
	// contextUsage is passed through untouched (shallow check only: fields are not
	// individually validated) so records written with it survive a list() round trip.
	if (
		r.contextUsage !== undefined &&
		(typeof r.contextUsage !== "object" || r.contextUsage === null || Array.isArray(r.contextUsage))
	) {
		return undefined;
	}
	return value as PeerRecord;
}

// ---------------------------------------------------------------------------
// Atomic writes
// ---------------------------------------------------------------------------

/** Write `path` atomically: temp file beside the target, then rename over it. */
function atomicWriteFileSync(path: string, data: string): void {
	const temp = join(dirname(path), `.${basename(path)}.tmp-${randomBytes(6).toString("hex")}`);
	try {
		writeFileSync(temp, data, { encoding: "utf8", mode: 0o600 });
		renameSync(temp, path);
	} finally {
		rmSync(temp, { force: true });
	}
}

// ---------------------------------------------------------------------------
// Name handling
// ---------------------------------------------------------------------------

/** NFKC → trim → lowercase → collapse whitespace runs into single dashes. */
function normalizeName(name: string): string {
	return name.normalize("NFKC").trim().toLowerCase().replace(/\s+/g, "-");
}

// ---------------------------------------------------------------------------
// Liveness
// ---------------------------------------------------------------------------

/**
 * Extract the starttime field (field 22) from `/proc/<pid>/stat` content.
 *
 * The comm field (field 2) is parenthesized and may itself contain spaces, parentheses, or
 * ") " sequences, so field splitting must start after the LAST closing paren; starttime is
 * then the 20th whitespace-delimited field following it.
 */
export function parseProcStatStarttime(statContent: string): string | undefined {
	const close = statContent.lastIndexOf(")");
	if (close === -1) {
		return undefined;
	}
	const fields = statContent
		.slice(close + 1)
		.trim()
		.split(/\s+/);
	const starttime = fields[19];
	if (starttime === undefined || !/^\d+$/.test(starttime)) {
		return undefined;
	}
	return starttime;
}

/** Best-effort /proc/<pid>/stat starttime; undefined off Linux or when unreadable. */
function readProcStart(pid: number): string | undefined {
	if (process.platform !== "linux") {
		return undefined;
	}
	try {
		return parseProcStatStarttime(readFileSync(`/proc/${pid}/stat`, "utf8"));
	} catch {
		return undefined;
	}
}

/** Liveness: kill(pid, 0) probe plus procStart match when both sides expose one. */
function isAlive(record: PeerRecord): boolean {
	try {
		process.kill(record.pid, 0);
	} catch (err) {
		const code = (err as NodeJS.ErrnoException).code;
		// EPERM: the process exists but we may not signal it (another user's): still alive.
		if (code !== "EPERM") {
			return false; // ESRCH (gone) or unexpected: dead.
		}
	}
	const observed = readProcStart(record.pid);
	if (record.procStart !== undefined && observed !== undefined) {
		return record.procStart === observed; // Mismatch → the pid was reused: dead.
	}
	return true;
}

// ---------------------------------------------------------------------------
// Identity helpers
// ---------------------------------------------------------------------------

/** This machine's pidDomain: "win32:<host>" / "posix:<host>[:<machine-id prefix 8>]" on Linux. */
function pidDomain(): string {
	const host = hostname();
	if (process.platform === "win32") {
		return `win32:${host.toLowerCase()}`;
	}
	if (process.platform === "linux") {
		try {
			const machineId = readFileSync("/etc/machine-id", "utf8").trim();
			if (machineId.length >= 8) {
				return `posix:${host}:${machineId.slice(0, 8)}`;
			}
		} catch {
			// Best-effort: unreadable machine-id (containers, permissions) → hostname only.
		}
	}
	return `posix:${host}`;
}

/** Short disambiguation ref: first 6 hex of sha256(canonical socketPath). */
function refOf(record: PeerRecord): string {
	return createHash("sha256").update(record.socketPath).digest("hex").slice(0, 6);
}

// ---------------------------------------------------------------------------
// Filesystem helpers (self-healing must never throw on races with other peers)
// ---------------------------------------------------------------------------

/** Best-effort delete; never throws. */
function silentUnlink(path: string): void {
	try {
		rmSync(path, { force: true });
	} catch {
		// Racing another peer's unpublish or an AV lock on Windows: already gone / retry later.
	}
}

/** readdir that tolerates a missing directory. */
function readDirNames(dir: string): string[] {
	try {
		return readdirSync(dir);
	} catch {
		return [];
	}
}

// ---------------------------------------------------------------------------
// Resolution
// ---------------------------------------------------------------------------

/** 0 matches → undefined (fall through to the next rule); 1 → unique; more → ambiguous. */
function toResolution(matches: PeerRegistryEntry[]): PeerResolution | undefined {
	if (matches.length === 0) {
		return undefined;
	}
	if (matches.length === 1) {
		return { kind: "unique", entry: matches[0] };
	}
	return { kind: "ambiguous", candidates: matches };
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/**
 * TTL for cached positive liveness verdicts. Balance point between probe
 * syscall overhead (list runs on tool calls, /peers, and send resolution, so
 * the same pids are probed repeatedly within one user interaction) and the
 * worst-case staleness a caller can perceive: 1.5s stays below anything a
 * human would notice in peer liveness. Negative verdicts are never cached —
 * self-healing depends on accurate dead detection, so they re-probe always.
 */
const ALIVE_CACHE_TTL_MS = 1500;

/** Positive-verdict cache entry; `live` is always true (false is never cached). */
interface AliveCacheEntry {
	live: true;
	/** ms epoch of the probe that produced the verdict. */
	at: number;
}

/** Liveness short-TTL cache (positive verdicts only; instance-scoped). */
type AliveCache = Map<number, AliveCacheEntry>;

export function createPeerRegistry(peersDir: string): PeerRegistry {
	const aliveCache: AliveCache = new Map();

	function recordPath(pid: number): string {
		return join(peersDir, `${pid}.json`);
	}

	function publish(record: PeerRecord): void {
		mkdirSync(peersDir, { recursive: true, mode: 0o700 });
		atomicWriteFileSync(recordPath(record.pid), JSON.stringify(record));
	}

	function update(pid: number, patch: Partial<PeerRecord>): void {
		const path = recordPath(pid);
		let raw: string;
		try {
			raw = readFileSync(path, "utf8");
		} catch {
			return; // Not published (or already removed): nothing to patch.
		}
		let current: unknown;
		try {
			current = JSON.parse(raw);
		} catch {
			return; // Corrupt: leave the file for list() to clean up.
		}
		if (typeof current !== "object" || current === null || Array.isArray(current)) {
			return;
		}
		const merged = { ...(current as object), ...patch, pid } as PeerRecord;
		atomicWriteFileSync(path, JSON.stringify(merged));
	}

	function unpublish(pid: number): void {
		aliveCache.delete(pid); // Our own exit must be reflected on the very next list().
		silentUnlink(recordPath(pid));
		for (const name of readDirNames(peersDir)) {
			const key = PEER_KEY_FILE_RE.exec(name);
			if (key && Number(key[1]) === pid) {
				silentUnlink(join(peersDir, name));
			}
		}
	}

	function list(): PeerRegistryEntry[] {
		const entries: PeerRegistryEntry[] = [];
		const now = Date.now();
		// Full-directory reads (and the self-healing they drive) still run on every list();
		// only the per-pid probe syscalls are cached.
		for (const name of readDirNames(peersDir)) {
			const match = PEER_RECORD_FILE_RE.exec(name);
			if (!match) {
				continue;
			}
			const path = join(peersDir, name);
			const filePid = Number(match[1]);
			let parsed: unknown;
			try {
				parsed = JSON.parse(readFileSync(path, "utf8"));
			} catch {
				silentUnlink(path); // Malformed JSON: self-heal.
				continue;
			}
			const record = parsePeerRecord(parsed);
			if (record === undefined) {
				silentUnlink(path); // Malformed record: self-heal.
				continue;
			}
			if (record.pid !== filePid) {
				silentUnlink(path); // Torn write (name/content pid mismatch): self-heal.
				continue;
			}
			const cached = aliveCache.get(record.pid);
			// Fresh positive verdict: trust it and skip the per-pid probe syscalls.
			const fresh = cached !== undefined && now - cached.at < ALIVE_CACHE_TTL_MS;
			const live = fresh ? cached.live : isAlive(record);
			if (live && !fresh) {
				aliveCache.set(record.pid, { live: true, at: now });
			}
			if (!live) {
				// Stale record (process gone / pid reused): report once with live=false, then self-heal.
				aliveCache.delete(record.pid); // Defensive: bury any verdict along with the record.
				silentUnlink(path);
				entries.push({ record, live: false });
				continue;
			}
			entries.push({ record, live: true });
		}
		// Orphan key files (pid without a live record) are useless credentials: self-heal.
		const livePids = new Set(entries.filter((e) => e.live).map((e) => e.record.pid));
		for (const name of readDirNames(peersDir)) {
			const keyMatch = PEER_KEY_FILE_RE.exec(name);
			if (keyMatch && !livePids.has(Number(keyMatch[1]))) {
				silentUnlink(join(peersDir, name));
			}
		}
		entries.sort((a, b) => a.record.pid - b.record.pid);
		return entries;
	}

	function resolve(name: string): PeerResolution {
		return resolveIn(name, list());
	}

	function resolveIn(name: string, entries: PeerRegistryEntry[]): PeerResolution {
		const input = name.trim();
		if (input === "") {
			return { kind: "none" };
		}

		let target = input;
		let ref: string | undefined;
		// Optional "[ref]" suffix narrows candidates by socketPath hash up front.
		const refMatch = /^(.*?)\s*\[([0-9a-f]{6})\]$/.exec(input);
		if (refMatch) {
			target = refMatch[1].trim();
			ref = refMatch[2];
		}

		// Only live peers are resolvable targets; dead entries are reported by list() for cleanup.
		const candidates = entries
			.filter((entry) => entry.live)
			.filter((entry) => ref === undefined || refOf(entry.record) === ref);

		// A wire address (`pipe:…` / `sock:…`) or raw socket path selects a peer
		// directly — this is how replies copy the incoming message's `from`.
		const asPath = input.startsWith("pipe:")
			? `\\\\.\\pipe\\${input.slice(5)}`
			: input.startsWith("sock:")
				? input.slice(5)
				: input;
		const bySocketPath = toResolution(candidates.filter((entry) => entry.record.socketPath === asPath));
		if (bySocketPath) {
			return bySocketPath;
		}

		// Match in priority order: exact sessionId, current name, former names.
		const bySessionId = toResolution(candidates.filter((entry) => entry.record.sessionId === target));
		if (bySessionId) {
			return bySessionId;
		}

		const normalized = normalizeName(target);
		if (normalized === "") {
			return { kind: "none" };
		}
		const byName = toResolution(candidates.filter((entry) => normalizeName(entry.record.name) === normalized));
		if (byName) {
			return byName;
		}
		const byFormerName = toResolution(
			candidates.filter((entry) =>
				entry.record.formerNames.some((former) => normalizeName(former.name) === normalized),
			),
		);
		if (byFormerName) {
			return byFormerName;
		}

		return { kind: "none" };
	}

	return {
		publish,
		update,
		unpublish,
		list,
		resolve,
		resolveIn,
		normalizeName,
		refOf,
		isAlive,
		readProcStart,
		pidDomain,
	};
}
