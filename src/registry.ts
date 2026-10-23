/**
 * Peer registry: file-based discovery of live peer pi instances.
 *
 * Every peer publishes `<peersDir>/<pid>.json` plus zero or more `<pid>.<64hex>.key` auth-key
 * files. Scanning (`list()`) is self-healing: malformed records, records whose embedded pid
 * disagrees with their file name (torn writes), and records whose process is gone are deleted
 * on sight, so a crashed peer never shadows a name. A record whose process is alive but whose
 * auth-key file is missing is a broken instance (unreachable for sends) and is swept the same
 * way — production publish order is key-then-record, so a healthy startup never hits this.
 */

import { createHash, randomBytes } from "node:crypto";
import { existsSync, mkdirSync, readdirSync, readFileSync, renameSync, rmSync, statSync, writeFileSync } from "node:fs";
import { hostname } from "node:os";
import { basename, dirname, join } from "node:path";
import type { PeerRecord, PeerRegistry, PeerRegistryEntry, PeerResolution } from "./types.ts";
import { PEER_KEY_FILE_RE, PEER_RECORD_FILE_RE } from "./types.ts";
import { keyFileNameFor } from "./auth.ts";
import { socketPathFromAddress } from "./transport.ts";

// ---------------------------------------------------------------------------
// Boundary parsing: registry files are written by other processes, possibly
// other versions, so every field is checked before the value is trusted.
// ---------------------------------------------------------------------------

/** Record-field hygiene bounds (parsePeerRecord): lengths and control chars.
 * Legit publishers stay far inside these; anything past them is a foreign or
 * corrupt writer. C0/C1/DEL must never reach terminal-rendered output. */
const PEER_NAME_MAX_CHARS = 64;
const PEER_CWD_MAX_CHARS = 512;
const PEER_SESSION_ID_MAX_CHARS = 128;
const PEER_CONTROL_CHARS_RE = /[\x00-\x1f\x7f-\x9f]/;
/** Dot-tmp leftovers from atomicWriteFileSync: `.<name>.tmp-<hex>` (auth.ts writes `.<name>.<hex>.tmp`). */
const PEER_TMP_FILE_RE = /^\..*\.tmp(-[0-9a-f]+)?$/;
/** A concurrent writer's tmp must not be swept mid-flight; older ones are orphans. */
const PEER_TMP_GRACE_MS = 60_000;

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
	// Field hygiene: legit publishers normalize the name at publish time and
	// never write control chars into path-like fields — anything else is a
	// foreign/corrupt writer. These fields render raw into /peers and tool
	// output, so C0/C1/DEL control characters must never pass through.
	if (r.name !== normalizeName(r.name) || r.name.length > PEER_NAME_MAX_CHARS) {
		return undefined;
	}
	if (r.cwd.length > PEER_CWD_MAX_CHARS || r.sessionId.length > PEER_SESSION_ID_MAX_CHARS) {
		return undefined;
	}
	if (PEER_CONTROL_CHARS_RE.test(r.cwd) || PEER_CONTROL_CHARS_RE.test(r.sessionId)) {
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
	if (PEER_CONTROL_CHARS_RE.test(r.socketPath)) {
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
	} catch (error) {
		try {
			rmSync(temp, { force: true });
		} catch {
			// Best-effort cleanup; the write/rename error is the real failure.
			// (Windows AV can hold the temp file: EBUSY/EPERM must not mask it.)
		}
		throw error;
	}
}

// ---------------------------------------------------------------------------
// Name handling
// ---------------------------------------------------------------------------

/**
 * NFKC → trim → lowercase → collapse whitespace runs into single dashes →
 * strip C0/C1/DEL control chars. Names derive from basename(cwd) and peer
 * renames, so control chars must never reach terminal-rendered output.
 * Stripping that empties a non-empty name falls back to "unnamed".
 */
function normalizeName(name: string): string {
	const normalized = name.normalize("NFKC").trim().toLowerCase().replace(/\s+/g, "-");
	const stripped = normalized.replace(/[\x00-\x1f\x7f-\x9f]/g, "");
	return stripped === "" && normalized !== "" ? "unnamed" : stripped;
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
/** Verdicts this old are pruned at the start of a real list() scan (pid churn). */
const ALIVE_CACHE_PRUNE_MS = ALIVE_CACHE_TTL_MS * 10;

/**
 * TTL for the list() result cache. One user action (send, /peers, resolve,
 * name disambiguation) triggers several list() calls within a few ms, and
 * every real scan is a full directory sweep — readdir + per-record
 * read/parse/probe + key sweep — which a dirty registry turns into hundreds
 * of blocked event-loop ms. Half a second of staleness is invisible to every
 * caller: write-side ops invalidate immediately, and the next real scan
 * re-heals anything the cached window hid.
 */
const REGISTRY_LIST_CACHE_MS = 500;

/** Positive-verdict cache entry; `live` is always true (false is never cached). */
interface AliveCacheEntry {
	live: true;
	/** ms epoch of the probe that produced the verdict. */
	at: number;
}

/** Liveness short-TTL cache (positive verdicts only; instance-scoped). */
type AliveCache = Map<number, AliveCacheEntry>;
/** Cached list() result; the array is handed out read-only, so one reference serves all callers. */
interface ListCacheEntry {
	entries: PeerRegistryEntry[];
	/** ms epoch of the scan that produced the entries. */
	at: number;
}


export function createPeerRegistry(peersDir: string): PeerRegistry {
	const aliveCache: AliveCache = new Map();
	let listCache: ListCacheEntry | undefined;

	function recordPath(pid: number): string {
		return join(peersDir, `${pid}.json`);
	}

	/** Expected auth-key file path for a record (naming owned by auth.keyFileNameFor). */
	function keyPathFor(record: PeerRecord): string {
		return join(peersDir, keyFileNameFor(record.pid, record.socketPath));
	}

	function publish(record: PeerRecord): void {
		mkdirSync(peersDir, { recursive: true, mode: 0o700 });
		atomicWriteFileSync(recordPath(record.pid), JSON.stringify(record));
		// Our own writes must be visible to the very next list(): drop the scan cache.
		listCache = undefined;
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
		// The patched record must be visible to the very next list(): drop the scan cache.
		listCache = undefined;
	}

	function unpublish(pid: number): void {
		aliveCache.delete(pid); // Our own exit must be reflected on the very next list().
		listCache = undefined; // …and the scan cache with it.
		silentUnlink(recordPath(pid));
		for (const name of readDirNames(peersDir)) {
			const key = PEER_KEY_FILE_RE.exec(name);
			if (key && Number(key[1]) === pid) {
				silentUnlink(join(peersDir, name));
			}
		}
	}
	/**
	 * Drop a cached "live" verdict for a pid (self-heal after a failed
	 * connect): a process that just died can sit in the liveness cache for
	 * up to its TTL, so the verdict must go now — the next list() probes
	 * for real instead of reporting the dead peer as live. The scan cache
	 * goes with it (same as publish/update): otherwise the stale listing
	 * would be served until its own TTL expires.
	 */
	function invalidateAlive(pid: number): void {
		aliveCache.delete(pid);
		listCache = undefined;
	}

	function list(opts?: { fresh?: boolean }): PeerRegistryEntry[] {
		const now = Date.now();
		// Short-TTL result cache: one user action (send, /peers, resolve, name
		// disambiguation) triggers several list() calls in a few ms; a full scan
		// is readdir + one read/parse per record + probe + key sweep, which a
		// dirty registry turns into hundreds of blocked-ms on the event loop.
		// Callers treat the array as read-only, so the same reference is safe.
		// Stale reads self-correct: write-side ops invalidate, the TTL expires,
		// and any record the cache hides appears on the next real scan.
		if (opts?.fresh !== true && listCache !== undefined && now - listCache.at < REGISTRY_LIST_CACHE_MS) {
			return listCache.entries;
		}
		const entries = scanRegistry(now);
		listCache = { entries, at: now };
		return entries;
	}

	/**
	 * Read exactly one record by pid: direct file read, no directory scan, no
	 * sweeping, no scan-cache. Returns the entry with a real liveness verdict
	 * (aliveCache semantics identical to list()), or undefined when the record
	 * is missing or malformed. For hot polling loops that watch a known pid.
	 */
	function readRecord(pid: number): PeerRegistryEntry | undefined {
		const path = recordPath(pid);
		let raw: string;
		try {
			raw = readFileSync(path, "utf8");
		} catch {
			return undefined; // Not published (or unreadable): no record for this pid.
		}
		let parsed: unknown;
		try {
			parsed = JSON.parse(raw);
		} catch {
			return undefined; // Malformed JSON: leave the file for list() to clean up.
		}
		const record = parsePeerRecord(parsed);
		if (record === undefined || record.pid !== pid) {
			// Malformed, or pid/name mismatch (torn write): not a readable record.
			return undefined;
		}
		return { record, live: liveVerdictFor(record, Date.now()) };
	}

	/**
	 * Liveness verdict with list()'s aliveCache semantics: a fresh positive
	 * verdict is trusted (probe syscalls skipped), a new positive verdict is
	 * cached. Negative verdicts are never cached — self-healing needs them
	 * accurate on every scan. `now` comes from the caller so one scan sees one
	 * coherent time (readRecord has no scan clock, so it reads the clock itself).
	 */
	function liveVerdictFor(record: PeerRecord, now: number): boolean {
		const cached = aliveCache.get(record.pid);
		// Fresh positive verdict: trust it and skip the per-pid probe syscalls.
		const fresh = cached !== undefined && now - cached.at < ALIVE_CACHE_TTL_MS;
		const live = fresh ? cached.live : isAlive(record);
		if (live && !fresh) {
			aliveCache.set(record.pid, { live: true, at: now });
		}
		return live;
	}

	/** Full directory scan: parse every record, self-heal, sweep orphan keys. */
	function scanRegistry(now: number): PeerRegistryEntry[] {
		const entries: PeerRegistryEntry[] = [];
		// Prune ancient verdicts before scanning: entries are otherwise only
		// deleted when a dead record is seen again, so pid churn (spawned
		// helpers, recycled OS pids) would grow the map without bound.
		// 10× the verdict TTL: anything older was re-probed long ago anyway.
		for (const [pid, verdict] of aliveCache) {
			if (now - verdict.at >= ALIVE_CACHE_PRUNE_MS) {
				aliveCache.delete(pid);
			}
		}
		// Full-directory reads (and the self-healing they drive) still run on
		// every real scan; only the per-pid probe syscalls are cached.
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
			const live = liveVerdictFor(record, now);
			if (!live) {
				// Stale record (process gone / pid reused): report once with live=false, then self-heal.
				aliveCache.delete(record.pid); // Defensive: bury any verdict along with the record.
				silentUnlink(path);
				entries.push({ record, live: false });
				continue;
			}
			// A live process without its auth key is a zombie: it can never
			// authenticate inbound connections, so it is unreachable for sends.
			// (Seen in the wild: an early orphan-sweep bug deleted a starting
			// instance's key mid-boot.) Report once as dead and sweep.
			if (!existsSync(keyPathFor(record))) {
				silentUnlink(path);
				entries.push({ record, live: false });
				continue;
			}
			entries.push({ record, live: true });
		}
		// Orphan key files (pid without a live record) are swept ONLY when the
		// pid is dead. A live pid with a key but no record is an instance still
		// starting up (session_start publishes the key BEFORE the record — the
		// async project-root probe sits between them); deleting its key would
		// permanently break its inbound auth.
		const livePids = new Set(entries.filter((e) => e.live).map((e) => e.record.pid));
		for (const name of readDirNames(peersDir)) {
			const keyMatch = PEER_KEY_FILE_RE.exec(name);
			if (keyMatch && !livePids.has(Number(keyMatch[1]))) {
				const keyPid = Number(keyMatch[1]);
				let alive = true;
				try {
					process.kill(keyPid, 0);
				} catch (err) {
					alive = (err as NodeJS.ErrnoException).code === "EPERM";
				}
				if (!alive) silentUnlink(join(peersDir, name));
			}
		}
		// Orphan dot-tmp files (a crash between an atomic write's temp-file
		// creation and its rename) are invisible to the record/key sweeps above,
		// so without this they leak forever. A 60s grace period protects a
		// concurrent writer's in-flight temp file from being deleted mid-write.
		for (const name of readDirNames(peersDir)) {
			if (!PEER_TMP_FILE_RE.test(name)) {
				continue;
			}
			const path = join(peersDir, name);
			let mtimeMs: number;
			try {
				mtimeMs = statSync(path).mtimeMs;
			} catch {
				continue; // Raced away (or locked): the next scan re-tries.
			}
			if (now - mtimeMs >= PEER_TMP_GRACE_MS) {
				silentUnlink(path);
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
		// socketPathFromAddress owns the wire-form conversion (and rejects
		// malformed addresses); raw canonical paths pass through unchanged.
		const asPath = socketPathFromAddress(input) ?? input;
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
		readRecord,
		invalidateAlive,
		resolve,
		resolveIn,
		normalizeName,
		refOf,
		isAlive,
		readProcStart,
		pidDomain,
	};
}
