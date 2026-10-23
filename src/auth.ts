/**
 * auth.ts — key file management for the peers extension.
 *
 * A peer that wants to receive messages publishes a key file next to its
 * registry record: <peersDir>/<pid>.<sha256(canonicalSocketPath)>.key. The
 * hash ties the key to the exact socket path, so a stale key from a reused
 * pid never authenticates against a live endpoint.
 */

import { createHash, randomBytes } from "node:crypto";
import { mkdirSync, readdirSync, readFileSync, renameSync, unlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { PEER_KEY_FILE_RE, type PeerAuth, type PeerTokens } from "./types.ts";

/** Key file payload as stored on disk. */
interface PeerKeyFile {
	pid: number;
	peerToken: string;
	procStart?: string;
	pidDomain?: string;
}

/** 32-hex token matcher (randomBytes(16) hex form). */
const TOKEN_RE = /^[0-9a-f]{32}$/;

/** sha256 of the canonical socket path, hex-encoded. */
function hashSocketPath(canonicalSocketPath: string): string {
	return createHash("sha256").update(canonicalSocketPath).digest("hex");
}

/** Generate a fresh token pair (32 hex chars each). */
export function generateTokens(): PeerTokens {
	return {
		peerToken: randomBytes(16).toString("hex"),
		childToken: randomBytes(16).toString("hex"),
	};
}

/**
 * Publish the key file for one (pid, socket path) pair.
 *
 * Written to a temp file first and renamed into place so peers racing a
 * lookup never observe a half-written key. Mode 0600: the file carries the
 * inbound auth token. childToken never leaves the process (handed to
 * children via env), so it is not part of the file.
 */
export function publishKey(
	peersDir: string,
	pid: number,
	canonicalSocketPath: string,
	tokens: PeerTokens,
	procStart: string | undefined,
	pidDomain: string,
): void {
	mkdirSync(peersDir, { recursive: true, mode: 0o700 });
	const hash = hashSocketPath(canonicalSocketPath);
	const fileName = `${pid}.${hash}.key`;
	const payload: PeerKeyFile = { pid, peerToken: tokens.peerToken };
	if (procStart !== undefined) {
		payload.procStart = procStart;
	}
	if (pidDomain !== undefined) {
		payload.pidDomain = pidDomain;
	}

	const finalPath = join(peersDir, fileName);
	const tmpPath = join(peersDir, `.${fileName}.${randomBytes(8).toString("hex")}.tmp`);
	writeFileSync(tmpPath, `${JSON.stringify(payload)}\n`, { mode: 0o600 });
	try {
		renameSync(tmpPath, finalPath);
	} catch (error) {
		try {
			unlinkSync(tmpPath);
		} catch {
			// Best-effort cleanup; the rename error is the real failure.
		}
		throw error;
	}
}

/**
 * Find and read the key file for a socket path.
 *
 * The pid segment of the filename is ignored during the scan: a reused pid
 * may have re-published under a different number while the path's key still
 * belongs to the live listener. Malformed payloads return undefined rather
 * than throwing — callers treat that as "cannot authenticate".
 *
 * When pidHint is given, the deterministic filename is probed first — one
 * readFileSync instead of a directory scan on the hot send path. Any miss
 * (ENOENT, malformed payload, pid mismatch) falls back to the full scan, so
 * a stale hint can never break a lookup the scan would have found.
 */
export function lookupKey(
	peersDir: string,
	canonicalSocketPath: string,
	pidHint?: number,
): { pid: number; peerToken: string; procStart?: string; pidDomain?: string } | undefined {
	const hash = hashSocketPath(canonicalSocketPath);

	if (pidHint !== undefined) {
		try {
			const direct = parseKeyFile(readFileSync(join(peersDir, `${pidHint}.${hash}.key`), "utf8"), pidHint);
			if (direct !== undefined) {
				return direct;
			}
		} catch {
			// Absent/unreadable (typical for a stale pid hint): fall back to the scan.
		}
	}

	let entries: string[];
	try {
		entries = readdirSync(peersDir);
	} catch {
		return undefined;
	}

	for (const entry of entries) {
		const match = PEER_KEY_FILE_RE.exec(entry);
		if (match === null || match[2] !== hash) {
			continue;
		}
		let raw: string;
		try {
			raw = readFileSync(join(peersDir, entry), "utf8");
		} catch {
			// Raced-away file: try the next candidate.
			continue;
		}
		const candidate = parseKeyFile(raw, Number(match[1]));
		if (candidate !== undefined) {
			return candidate;
		}
	}
	return undefined;
}

/**
 * Parse and validate one key file payload against the pid encoded in its
 * filename. Malformed or tampered content yields undefined, which callers
 * treat as "no key" (fast path: fall back to the scan; scan: keep looking).
 */
function parseKeyFile(
	raw: string,
	expectedPid: number,
): { pid: number; peerToken: string; procStart?: string; pidDomain?: string } | undefined {
	let parsed: unknown;
	try {
		parsed = JSON.parse(raw);
	} catch {
		// Unparseable payload is corrupt/tampered: report "no key", never throw.
		return undefined;
	}
	if (typeof parsed !== "object" || parsed === null) {
		return undefined;
	}
	const key = parsed as Record<string, unknown>;
	if (
		typeof key.pid !== "number" ||
		!Number.isInteger(key.pid) ||
		key.pid !== expectedPid ||
		typeof key.peerToken !== "string" ||
		!TOKEN_RE.test(key.peerToken)
	) {
		return undefined;
	}
	const result: PeerKeyFile = { pid: key.pid, peerToken: key.peerToken };
	if (typeof key.procStart === "string") {
		result.procStart = key.procStart;
	}
	if (typeof key.pidDomain === "string") {
		result.pidDomain = key.pidDomain;
	}
	return result;
}

/** Remove every key file belonging to this pid (shutdown path). */
export function deleteKeys(peersDir: string, pid: number): void {
	let entries: string[];
	try {
		entries = readdirSync(peersDir);
	} catch {
		return;
	}
	for (const entry of entries) {
		const match = PEER_KEY_FILE_RE.exec(entry);
		if (match === null || Number(match[1]) !== pid) {
			continue;
		}
		try {
			unlinkSync(join(peersDir, entry));
		} catch {
			// Best-effort: a concurrently removed file is fine.
		}
	}
}

/** PeerAuth facade over the module functions. */
export function createPeerAuth(): PeerAuth {
	return {
		generateTokens,
		publishKey,
		lookupKey,
		deleteKeys,
	};
}

/**
 * pid-hinted fast path over lookupKey. The PeerAuth interface (frozen in
 * types.ts) types lookupKey without the hint, and the interface type erases
 * the implementation's extra parameter at every interface-typed call site —
 * so the hint-accepting form is exposed under its own name for senders that
 * hold a live pid. Semantics are lookupKey's: hint miss (stale pid, malformed
 * direct file) falls back to the full scan.
 */
export function lookupKeyForPid(
	peersDir: string,
	socketPath: string,
	pid: number,
): { pid: number; peerToken: string; procStart?: string; pidDomain?: string } | undefined {
	return lookupKey(peersDir, socketPath, pid);
}
