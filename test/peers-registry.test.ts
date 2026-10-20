import type { ChildProcess } from "node:child_process";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { hostname, tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createPeerRegistry, parseProcStatStarttime } from "../src/registry.ts";
import type { PeerRecord, PeerRegistry } from "../src/types.ts";

const LIVE_PID = process.pid;
const DEAD_PID = 999999;

let peersDir: string;
let registry: PeerRegistry;
let secondLivePid = 0;
let liveChild: ChildProcess | undefined;
const tempDirs: string[] = [];

/** Fresh isolated registry directory per test — list() self-healing is stateful. */
function freshRegistry(): PeerRegistry {
	peersDir = mkdtempSync(join(tmpdir(), "pi-peers-registry-"));
	tempDirs.push(peersDir);
	registry = createPeerRegistry(peersDir);
	return registry;
}

function makeRecord(pid: number, overrides: Partial<PeerRecord> = {}): PeerRecord {
	return {
		version: 1,
		pid,
		sessionId: `session-${pid}`,
		cwd: "/tmp",
		name: `peer-${pid}`,
		nameSource: "user",
		formerNames: [],
		kind: "interactive",
		status: "idle",
		statusUpdatedAt: 1_000,
		startedAt: 900,
		pidDomain: "win32:testhost",
		socketPath: `\\\\.\\pipe\\LOCAL\\pi-peer-${String(pid).padStart(32, "0")}`,
		peerProtocol: 1,
		features: ["notify_idle"],
		...overrides,
	};
}

beforeAll(() => {
	// A second genuinely-live pid for multi-peer resolution cases.
	liveChild = spawn(process.execPath, ["-e", "process.stdin.resume()"], { stdio: ["pipe", "ignore", "ignore"] });
	if (typeof liveChild.pid !== "number") {
		throw new Error("failed to spawn helper child process");
	}
	secondLivePid = liveChild.pid;
});

afterAll(async () => {
	for (const dir of tempDirs) {
		rmSync(dir, { recursive: true, force: true });
	}
	if (liveChild) {
		liveChild.kill();
		// Await the real exit event (not a guessed delay) so the child pid is reaped.
		await new Promise<void>((done) => liveChild!.once("exit", () => done()));
	}
});

function recordFile(pid: number): string {
	return join(peersDir, `${pid}.json`);
}

function visibleFileNames(): string[] {
	return readdirSync(peersDir).sort();
}

describe("publish/list round trip", () => {
	it("publishes a record and lists it as live", () => {
		freshRegistry();
		const record = makeRecord(LIVE_PID, { name: "alice" });
		registry.publish(record);
		const entries = registry.list();
		expect(entries).toHaveLength(1);
		expect(entries[0].live).toBe(true);
		expect(entries[0].record).toEqual(record);
	});

	it("publishes atomically, leaving no temp files behind", () => {
		freshRegistry();
		registry.publish(makeRecord(LIVE_PID, { name: "alice" }));
		expect(visibleFileNames()).toEqual([`${LIVE_PID}.json`]);
	});

	it("creates the peers dir recursively when missing", () => {
		const base = mkdtempSync(join(tmpdir(), "pi-peers-registry-fresh-"));
		tempDirs.push(base);
		const nested = createPeerRegistry(join(base, "nested", "peers"));
		nested.publish(makeRecord(LIVE_PID));
		expect(existsSync(join(base, "nested", "peers", `${LIVE_PID}.json`))).toBe(true);
	});

	it("lists an empty array for a nonexistent directory", () => {
		const missing = createPeerRegistry(join(tmpdir(), "pi-peers-does-not-exist-xyz"));
		expect(missing.list()).toEqual([]);
	});
});

describe("update", () => {
	it("merges a patch into the published record", () => {
		freshRegistry();
		registry.publish(makeRecord(LIVE_PID, { name: "alice", status: "idle" }));
		registry.update(LIVE_PID, { status: "busy", statusUpdatedAt: 2_000 });
		const entries = registry.list();
		expect(entries).toHaveLength(1);
		expect(entries[0].record.status).toBe("busy");
		expect(entries[0].record.statusUpdatedAt).toBe(2_000);
		expect(entries[0].record.name).toBe("alice");
		expect(entries[0].record.pid).toBe(LIVE_PID);
	});

	it("silently ignores update when no record exists", () => {
		freshRegistry();
		registry.publish(makeRecord(LIVE_PID, { name: "alice" }));
		registry.update(DEAD_PID, { status: "busy" });
		expect(visibleFileNames()).toEqual([`${LIVE_PID}.json`]);
	});

	it("silently ignores update when the record is corrupt JSON", () => {
		freshRegistry();
		writeFileSync(recordFile(LIVE_PID), "{ not json");
		expect(() => registry.update(LIVE_PID, { status: "busy" })).not.toThrow();
		expect(visibleFileNames()).toEqual([`${LIVE_PID}.json`]);
	});
});

describe("unpublish", () => {
	it("removes the record and all key files for the pid", () => {
		freshRegistry();
		registry.publish(makeRecord(LIVE_PID, { name: "alice" }));
		writeFileSync(join(peersDir, `${LIVE_PID}.${"a".repeat(64)}.key`), "key");
		writeFileSync(join(peersDir, `${LIVE_PID}.${"b".repeat(64)}.key`), "key");
		registry.unpublish(LIVE_PID);
		expect(visibleFileNames()).toEqual([]);
	});

	it("leaves other pids' files alone", () => {
		freshRegistry();
		registry.publish(makeRecord(LIVE_PID, { name: "alice" }));
		writeFileSync(join(peersDir, `${DEAD_PID}.${"c".repeat(64)}.key`), "key");
		registry.unpublish(LIVE_PID);
		expect(visibleFileNames()).toEqual([`${DEAD_PID}.${"c".repeat(64)}.key`]);
	});

	it("does not throw when nothing is published", () => {
		freshRegistry();
		expect(() => registry.unpublish(DEAD_PID)).not.toThrow();
	});
});

describe("list self-healing", () => {
	it("deletes malformed JSON records", () => {
		freshRegistry();
		writeFileSync(recordFile(DEAD_PID), "{ not json");
		expect(registry.list()).toEqual([]);
		expect(existsSync(recordFile(DEAD_PID))).toBe(false);
	});

	it("deletes records with wrong version", () => {
		freshRegistry();
		writeFileSync(recordFile(DEAD_PID), JSON.stringify({ ...makeRecord(DEAD_PID), version: 2 }));
		registry.list();
		expect(existsSync(recordFile(DEAD_PID))).toBe(false);
	});

	it("deletes records whose pid mismatches the file name (torn write)", () => {
		freshRegistry();
		writeFileSync(recordFile(12345), JSON.stringify(makeRecord(LIVE_PID)));
		registry.list();
		expect(existsSync(recordFile(12345))).toBe(false);
	});

	it("deletes records with missing or mistyped required fields", () => {
		freshRegistry();
		const malformed: unknown[] = [
			{ ...makeRecord(DEAD_PID), sessionId: undefined },
			{ ...makeRecord(DEAD_PID), pid: "123" },
			{ ...makeRecord(DEAD_PID), statusUpdatedAt: "soon" },
			{ ...makeRecord(DEAD_PID), formerNames: "none" },
			{ ...makeRecord(DEAD_PID), features: [1, 2] },
			{ ...makeRecord(DEAD_PID), procStart: 42 },
		];
		for (const bad of malformed) {
			writeFileSync(recordFile(DEAD_PID), JSON.stringify(bad));
			registry.list();
			expect(existsSync(recordFile(DEAD_PID)), `should delete ${JSON.stringify(bad)}`).toBe(false);
		}
	});

	it("deletes records with invalid kind/status/nameSource", () => {
		freshRegistry();
		for (const patch of [{ kind: "daemon" }, { status: "sleeping" }, { nameSource: "auto" }]) {
			writeFileSync(recordFile(DEAD_PID), JSON.stringify({ ...makeRecord(DEAD_PID), ...patch }));
			registry.list();
			expect(existsSync(recordFile(DEAD_PID))).toBe(false);
		}
	});

	it("ignores non-record files in the directory", () => {
		freshRegistry();
		writeFileSync(join(peersDir, "readme.txt"), "hello");
		writeFileSync(join(peersDir, `${LIVE_PID}.${"d".repeat(64)}.key`), "key");
		registry.publish(makeRecord(LIVE_PID, { name: "alice" }));
		expect(registry.list()).toHaveLength(1);
		expect(existsSync(join(peersDir, "readme.txt"))).toBe(true);
	});

	it("reports dead-pid records with live=false and removes the file", () => {
		freshRegistry();
		registry.publish(makeRecord(DEAD_PID, { name: "ghost" }));
		const entries = registry.list();
		expect(entries).toHaveLength(1);
		expect(entries[0].live).toBe(false);
		expect(entries[0].record.name).toBe("ghost");
		expect(existsSync(recordFile(DEAD_PID))).toBe(false);
		// The self-healed file is gone: a second scan sees nothing.
		expect(registry.list()).toEqual([]);
	});

	it("keeps a leftover temp file out of the listing", () => {
		freshRegistry();
		writeFileSync(join(peersDir, `.${DEAD_PID}.json.tmp-abc123`), "{ partial");
		registry.publish(makeRecord(LIVE_PID, { name: "alice" }));
		expect(registry.list()).toHaveLength(1);
	});
});

describe("normalizeName", () => {
	it("trims, lowercases, and collapses whitespace to dashes", () => {
		freshRegistry();
		expect(registry.normalizeName("  My   Cool Peer ")).toBe("my-cool-peer");
	});

	it("applies NFKC compatibility normalization", () => {
		freshRegistry();
		// Fullwidth "AB" (U+FF21/U+FF22) → "ab"; ligature "ﬁ" (U+FB01) → "fi".
		expect(registry.normalizeName("\uFF21\uFF22")).toBe("ab");
		expect(registry.normalizeName("a\uFB01b")).toBe("afib");
		// NFKC also folds NBSP into a plain space before whitespace collapsing.
		expect(registry.normalizeName("a\u00A0\u00A0b")).toBe("a-b");
	});

	it("returns empty string for whitespace-only input", () => {
		freshRegistry();
		expect(registry.normalizeName("   ")).toBe("");
	});
});

describe("refOf", () => {
	it("is the first 6 hex chars of sha256(socketPath)", () => {
		freshRegistry();
		const record = makeRecord(LIVE_PID, { socketPath: "\\\\.\\pipe\\LOCAL\\pi-peer-abc" });
		const expected = createHash("sha256").update(record.socketPath).digest("hex").slice(0, 6);
		expect(registry.refOf(record)).toBe(expected);
	});

	it("differs between distinct socket paths", () => {
		freshRegistry();
		const a = registry.refOf(makeRecord(LIVE_PID, { socketPath: "sock-a" }));
		const b = registry.refOf(makeRecord(LIVE_PID, { socketPath: "sock-b" }));
		expect(a).not.toBe(b);
		expect(a).toMatch(/^[0-9a-f]{6}$/);
	});
});

describe("resolve", () => {
	it("resolves a unique current name", () => {
		freshRegistry();
		registry.publish(makeRecord(LIVE_PID, { name: "alice", sessionId: "sess-alice" }));
		expect(registry.resolve("alice")).toMatchObject({
			kind: "unique",
			entry: { live: true, record: { name: "alice", sessionId: "sess-alice" } },
		});
	});

	it("is case- and whitespace-insensitive on names", () => {
		freshRegistry();
		registry.publish(makeRecord(LIVE_PID, { name: "Alice Cooper" }));
		expect(registry.resolve("  alice cooper ")).toMatchObject({ kind: "unique" });
		expect(registry.resolve("ALICE   COOPER")).toMatchObject({ kind: "unique" });
	});

	it("returns ambiguous for two same-named live peers", () => {
		freshRegistry();
		registry.publish(makeRecord(LIVE_PID, { name: "twin", sessionId: "sess-twin-1" }));
		registry.publish(makeRecord(secondLivePid, { name: "twin", sessionId: "sess-twin-2" }));
		const result = registry.resolve("twin");
		expect(result.kind).toBe("ambiguous");
		if (result.kind === "ambiguous") {
			expect(result.candidates).toHaveLength(2);
		}
	});

	it("disambiguates twins with a [ref] suffix", () => {
		freshRegistry();
		registry.publish(makeRecord(LIVE_PID, { name: "twin", sessionId: "sess-twin-1", socketPath: "sock-twin-1" }));
		const b = makeRecord(secondLivePid, { name: "twin", sessionId: "sess-twin-2", socketPath: "sock-twin-2" });
		registry.publish(b);
		expect(registry.resolve(`twin [${registry.refOf(b)}]`)).toMatchObject({
			kind: "unique",
			entry: { record: { sessionId: "sess-twin-2" } },
		});
	});

	it("returns none when the [ref] suffix matches nothing", () => {
		freshRegistry();
		registry.publish(makeRecord(LIVE_PID, { name: "twin" }));
		expect(registry.resolve("twin [000000]")).toMatchObject({ kind: "none" });
	});

	it("resolves by exact sessionId", () => {
		freshRegistry();
		registry.publish(makeRecord(LIVE_PID, { name: "alice", sessionId: "sess-alice" }));
		expect(registry.resolve("sess-alice")).toMatchObject({
			kind: "unique",
			entry: { record: { name: "alice" } },
		});
	});

	it("resolves by raw socket path, as copied from an incoming message's from", () => {
		freshRegistry();
		registry.publish(makeRecord(LIVE_PID, { name: "alice", socketPath: "\\\\.\\pipe\\LOCAL\\pi-peer-aaaa1111" }));
		expect(registry.resolve("\\\\.\\pipe\\LOCAL\\pi-peer-aaaa1111")).toMatchObject({
			kind: "unique",
			entry: { record: { name: "alice" } },
		});
	});

	it("resolves by wire address form (pipe:/sock: prefixes)", () => {
		freshRegistry();
		registry.publish(makeRecord(LIVE_PID, { name: "alice", socketPath: "\\\\.\\pipe\\LOCAL\\pi-peer-aaaa1111" }));
		registry.publish(makeRecord(secondLivePid, { name: "bob", socketPath: "/tmp/pi-peers/socks/1.sock" }));
		expect(registry.resolve("pipe:LOCAL\\pi-peer-aaaa1111")).toMatchObject({
			kind: "unique",
			entry: { record: { name: "alice" } },
		});
		expect(registry.resolve("sock:/tmp/pi-peers/socks/1.sock")).toMatchObject({
			kind: "unique",
			entry: { record: { name: "bob" } },
		});
	});

	it("prefers an exact sessionId over a colliding peer name", () => {
		freshRegistry();
		registry.publish(makeRecord(LIVE_PID, { name: "alice", sessionId: "sess-alice" }));
		registry.publish(makeRecord(secondLivePid, { name: "sess-alice", sessionId: "sess-other" }));
		expect(registry.resolve("sess-alice")).toMatchObject({
			kind: "unique",
			entry: { record: { name: "alice" } },
		});
	});

	it("resolves former names", () => {
		freshRegistry();
		registry.publish(
			makeRecord(LIVE_PID, {
				name: "alice",
				sessionId: "sess-alice",
				formerNames: [{ name: "Alicia  Keys", until: "2026-01-01T00:00:00Z" }],
			}),
		);
		expect(registry.resolve("Alicia Keys")).toMatchObject({
			kind: "unique",
			entry: { record: { sessionId: "sess-alice" } },
		});
	});

	it("does not fall back to former names when the current name uniquely matches elsewhere", () => {
		freshRegistry();
		registry.publish(makeRecord(LIVE_PID, { name: "bob", sessionId: "sess-bob" }));
		registry.publish(
			makeRecord(secondLivePid, {
				name: "carol",
				sessionId: "sess-carol",
				formerNames: [{ name: "bob", until: "2026-01-01T00:00:00Z" }],
			}),
		);
		expect(registry.resolve("bob")).toMatchObject({
			kind: "unique",
			entry: { record: { sessionId: "sess-bob" } },
		});
	});

	it("returns none when only dead peers match", () => {
		freshRegistry();
		registry.publish(makeRecord(DEAD_PID, { name: "ghost", sessionId: "sess-ghost" }));
		expect(registry.resolve("ghost")).toMatchObject({ kind: "none" });
		expect(registry.resolve("sess-ghost")).toMatchObject({ kind: "none" });
	});

	it("returns none for empty input", () => {
		freshRegistry();
		registry.publish(makeRecord(LIVE_PID, { name: "alice" }));
		expect(registry.resolve("")).toMatchObject({ kind: "none" });
		expect(registry.resolve("   ")).toMatchObject({ kind: "none" });
	});
});

describe("isAlive", () => {
	it("reports the current process as alive", () => {
		freshRegistry();
		expect(registry.isAlive(makeRecord(LIVE_PID))).toBe(true);
	});

	it("reports a spawned child as alive", () => {
		freshRegistry();
		expect(registry.isAlive(makeRecord(secondLivePid))).toBe(true);
	});

	it("reports a dead pid as not alive", () => {
		freshRegistry();
		expect(registry.isAlive(makeRecord(DEAD_PID))).toBe(false);
	});

	it("accepts a procStart that equals the observed one", () => {
		freshRegistry();
		const observed = registry.readProcStart(LIVE_PID);
		expect(registry.isAlive(makeRecord(LIVE_PID, { procStart: observed }))).toBe(true);
	});

	it("rejects a procStart mismatch (pid reuse) when both sides are observable", () => {
		freshRegistry();
		const observed = registry.readProcStart(LIVE_PID);
		if (observed === undefined) {
			return; // Off Linux the mismatch branch is unreachable: /proc is absent.
		}
		expect(registry.isAlive(makeRecord(LIVE_PID, { procStart: `${observed}0` }))).toBe(false);
	});
});

describe("readProcStart", () => {
	it("returns a numeric starttime for a live pid on Linux, undefined elsewhere", () => {
		freshRegistry();
		const start = registry.readProcStart(LIVE_PID);
		if (process.platform !== "linux") {
			expect(start).toBeUndefined();
		} else {
			expect(start).toMatch(/^\d+$/);
		}
	});

	it("returns undefined for a dead pid", () => {
		freshRegistry();
		expect(registry.readProcStart(DEAD_PID)).toBeUndefined();
	});
});

describe("parseProcStatStarttime", () => {
	it("extracts starttime despite spaces and parens inside comm", () => {
		// comm = "bad ) name ( here" — contains parens, spaces, and a ") " sequence.
		// state(3)…pgrp(6) then filler up to starttime(22), plus trailing junk fields.
		const head = `1234 (bad ) name ( here) R 1 1234 1234 0 -1`;
		const filler = Array.from({ length: 13 }, () => "0").join(" ");
		const stat = `${head} ${filler} 12345678 0 0 0 0 0 0 0 0 0 0 0 0 0`;
		expect(parseProcStatStarttime(stat)).toBe("12345678");
	});

	it("extracts starttime for a simple comm", () => {
		const stat = `1 (systemd) S 0 1 1 0 -1 4194560 0 0 0 0 2 1 0 0 20 0 1 0 99 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0`;
		expect(parseProcStatStarttime(stat)).toBe("99");
	});

	it("returns undefined when there is no closing paren", () => {
		expect(parseProcStatStarttime("1234")).toBeUndefined();
	});

	it("returns undefined when fields after comm are too few", () => {
		expect(parseProcStatStarttime("1 (systemd) S 0 1 1 0 -1")).toBeUndefined();
	});

	it("returns undefined when the starttime field is not numeric", () => {
		// 19 filler fields then "abc" exactly at the starttime (field 22) position.
		const filler = Array.from({ length: 19 }, () => "7").join(" ");
		expect(parseProcStatStarttime(`1 (systemd) ${filler} abc 0 0`)).toBeUndefined();
	});
});

describe("pidDomain", () => {
	it("is win32:<lowercase-hostname> on Windows", () => {
		freshRegistry();
		if (process.platform !== "win32") {
			return;
		}
		expect(registry.pidDomain()).toBe(`win32:${hostname().toLowerCase()}`);
	});

	it("starts with posix: on POSIX platforms", () => {
		freshRegistry();
		if (process.platform === "win32") {
			return;
		}
		expect(registry.pidDomain()).toMatch(/^posix:/);
	});

	it("is stable across calls", () => {
		freshRegistry();
		expect(registry.pidDomain()).toBe(registry.pidDomain());
	});
});
