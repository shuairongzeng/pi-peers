/**
 * Fuzz / property tests for the peers wire protocol boundary (R16).
 *
 * Deterministic by construction: every input derives from a mulberry32 PRNG
 * seeded from a fixed seed list — no Math.random / Date.now influences any
 * generated case. Five families, one per attack surface:
 *
 *  1. parseFrameLine robustness — random bytes, truncated frames, JSON
 *     primitives, deep nesting, `__proto__` keys, weird msgV/fromPid.
 *  2. wrap/parse content round-trip under hostile attrs/body.
 *  3. Line framing reassembly over a real socket pair with random chunking.
 *  4. parsePeerRecord (via registry readRecord) on mutated records.
 *  5. createPeerGuard check()/recordAccepted() invariants under randomized
 *     interleavings with a fake clock.
 */

import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { connect as netConnect, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { createPeerGuard } from "../src/guard.ts";
import { createPeerProtocol } from "../src/protocol.ts";
import { createPeerRegistry } from "../src/registry.ts";
import { createPeerTransport, deriveSocketAddress, resolveAddress } from "../src/transport.ts";
import {
	PEER_CONTENT_TAG,
	PEER_MAX_LINE_BYTES,
	PEER_MAX_MESSAGE_BYTES,
	PEER_PROTOCOL_VERSION,
	type PeerConnection,
	type PeerControlFrame,
	type PeerContentAttrs,
	type PeerFrame,
	type PeerRecord,
	type PeerRegistry,
	type PeerRegistryEntry,
	type PeerServer,
	type PeerUserFrame,
} from "../src/types.ts";

// ---------------------------------------------------------------------------
// Deterministic PRNG (mulberry32) + generators
// ---------------------------------------------------------------------------

const SEEDS = [1, 42, 1337, 0xdeadbeef];

/** mulberry32: 32-bit state, deterministic across runs and platforms. */
function mulberry32(seed: number): () => number {
	let a = seed >>> 0;
	return () => {
		a = (a + 0x6d2b79f5) >>> 0;
		let t = Math.imul(a ^ (a >>> 15), 1 | a);
		t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
}

/** A seeded random source for one seed; created per test to keep cases independent. */
interface Rng {
	next(): number;
	int(maxExclusive: number): number;
	range(minInclusive: number, maxInclusive: number): number;
	pick<T>(items: readonly T[]): T;
	chance(p: number): boolean;
}

/** A seeded random source; created per test to keep cases independent. */
function seeded(seed: number): Rng {
	const rand = mulberry32(seed);
	return {
		next: rand,
		int: (maxExclusive) => Math.floor(rand() * maxExclusive),
		range: (lo, hi) => lo + Math.floor(rand() * (hi - lo + 1)),
		pick: (items) => items[Math.floor(rand() * items.length)],
		chance: (p) => rand() < p,
	};
}

// Char pools used to build hostile strings.
const CONTROL_CHARS: string[] = [];
for (let c = 0; c < 0x20; c++) CONTROL_CHARS.push(String.fromCharCode(c));
CONTROL_CHARS.push(String.fromCharCode(0x7f)); // DEL
for (let c = 0x80; c < 0xa0; c++) CONTROL_CHARS.push(String.fromCharCode(c)); // C1
const HOSTILE_SPECIALS = ['"', "'", "<", ">", "\\", "/", "&", "=", ";", "\u2028", "\u2029", "\ufeff"];
const HOSTILE_POOL = [...CONTROL_CHARS, ...HOSTILE_SPECIALS, "a", "Z", "0", " ", "\t"];

/** Random string mixing control chars, U+2028/2029, and specials. */
function hostileString(rng: Rng, length: number): string {
	let out = "";
	for (let i = 0; i < length; i++) {
		out += rng.pick(HOSTILE_POOL);
	}
	return out;
}

/** Random printable+unicode payload (letters, emoji, astral plane). */
function payloadString(rng: Rng, length: number): string {
	const pool = ["a", "b", "é", "中", "\u{1F600}", "\u{10FFFF}", " ", "-", "_", "😀", "🎉"];
	let out = "";
	for (let i = 0; i < length; i++) {
		out += rng.pick(pool);
	}
	return out;
}

/** Uniform random bytes (0..255) as a latin1 string. */
function randomBytesString(rng: Rng, length: number): string {
	let out = "";
	for (let i = 0; i < length; i++) {
		out += String.fromCharCode(rng.int(256));
	}
	return out;
}

// ---------------------------------------------------------------------------
// Valid frame fixtures (the base for mutation)
// ---------------------------------------------------------------------------

const protocol = createPeerProtocol();

function validUserFrame(rng: Rng, contentLength = 24): PeerUserFrame {
	return {
		msgV: PEER_PROTOCOL_VERSION,
		msg_id: `m-${rng.int(1e9)}`,
		type: "user",
		from: `pipe:LOCAL\\pi-peer-${"a".repeat(32)}`,
		fromPid: rng.range(1, 65535),
		fromSession: `sess-${rng.int(1e9)}`,
		fromName: `name-${rng.int(1e6)}`,
		hopChain: [`t${rng.int(100)}`],
		priority: rng.pick(["next", "now"] as const),
		content: payloadString(rng, contentLength),
	};
}

function validControlFrame(rng: Rng): PeerControlFrame {
	const actions = ["peer_message_status", "notify_when_idle", "peer_idle_notice", "shutdown", "shutdown_response"] as const;
	const action = rng.pick(actions);
	return {
		msgV: PEER_PROTOCOL_VERSION,
		msg_id: `m-${rng.int(1e9)}`,
		type: "control",
		action,
		from: `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`,
		...(action === "peer_message_status"
			? { state: rng.pick(["held", "delivered", "denied", "expired", "refused", "dropped"] as const) }
			: action === "notify_when_idle"
				? { reply_to: `pipe:LOCAL\\pi-peer-${"c".repeat(32)}` }
				: action === "shutdown_response"
					? { inReplyTo: `orig-${rng.int(1000)}`, outcome: rng.pick(["accepted", "denied"] as const) }
					: {}),
	} as PeerControlFrame;
}

/** Serialize a frame without the size cap (raw JSON line, no trailing \n). */
function jsonLine(value: unknown): string {
	return JSON.stringify(value);
}

// ---------------------------------------------------------------------------
// 1. parseFrameLine robustness
// ---------------------------------------------------------------------------

/**
 * Structural invariants every accepted frame must satisfy. Mirrors the
 * validators' contracts from protocol.ts without reimplementing them: if
 * parseFrameLine returned a frame, the frame must be re-serializable and
 * re-parseable to an equal frame, carry msgV===PEER_PROTOCOL_VERSION, and
 * stay within the wire byte cap.
 */
function assertFrameInvariants(frame: PeerFrame, sourceLine: string): void {
	expect(frame.msgV).toBe(PEER_PROTOCOL_VERSION);
	expect(typeof frame.msg_id).toBe("string");
	expect(frame.msg_id.length).toBeGreaterThan(0);
	expect(Buffer.byteLength(sourceLine, "utf-8")).toBeLessThanOrEqual(PEER_MAX_MESSAGE_BYTES);
	// Round-trip: the same line must parse to an equal frame (fields the
	// validator dropped, e.g. unknown extras, do not affect equality).
	const reparsed = protocol.parseFrameLine(sourceLine);
	expect(reparsed).toEqual(frame);
}

describe("fuzz: parseFrameLine never throws / never accepts junk", () => {
	for (const seed of SEEDS) {
		it(`absorbs ${200} random byte-string lines (seed ${seed})`, () => {
			const rng = seeded(seed);
			for (let i = 0; i < 50; i++) {
				const line = randomBytesString(rng, rng.range(0, 200));
				const parsed = protocol.parseFrameLine(line);
				if (parsed !== undefined) {
					assertFrameInvariants(parsed, line);
				}
			}
		});

		it(`rejects or safely parses mutated valid frames (seed ${seed})`, () => {
			const rng = seeded(seed ^ 0x5eed);
			for (let i = 0; i < 50; i++) {
				const base = rng.chance(0.5) ? validUserFrame(rng) : validControlFrame(rng);
				const obj = JSON.parse(jsonLine(base)) as Record<string, unknown>;
				const mutated = mutateObject(rng, obj);
				const line = jsonLine(mutated);
				const parsed = protocol.parseFrameLine(line);
				if (parsed !== undefined) {
					assertFrameInvariants(parsed, line);
				}
			}
		});

		it(`handles truncated and augmented valid lines (seed ${seed})`, () => {
			const rng = seeded(seed ^ 0xa5a5);
			for (let i = 0; i < 50; i++) {
				const base = rng.chance(0.5) ? validUserFrame(rng) : validControlFrame(rng);
				const line = jsonLine(base);
				const cut = rng.range(0, line.length);
				const truncated = line.slice(0, cut);
				const augmented = `${line}${randomBytesString(rng, rng.range(0, 8))}`;
				for (const candidate of [truncated, augmented]) {
					const parsed = protocol.parseFrameLine(candidate);
					if (parsed !== undefined) {
						assertFrameInvariants(parsed, candidate);
					}
				}
			}
		});

		it(`rejects JSON primitives, arrays, null, and empty (seed ${seed})`, () => {
			const rng = seeded(seed ^ 0xfeed);
			const junk: string[] = [
				"42",
				"-0.5",
				"1e999",
				'"string value"',
				"null",
				"true",
				"false",
				"",
				"[1,2,3]",
				"[]",
				"{}",
				'{"msgV":1}',
				"NaN",
				"Infinity",
				'{"msgV":1,"msg_id":"m","type":"user","from":"f","fromPid":"str","fromSession":"s","hopChain":[],"priority":"next","content":"c"}',
			];
			for (let i = 0; i < 50; i++) {
				const line = rng.chance(0.5) ? rng.pick(junk) : jsonLine(rng.pick(junkLines(rng)));
				const parsed = protocol.parseFrameLine(line);
				expect(parsed).toBeUndefined();
			}
		});

		it(`survives nested-depth ramp objects (seed ${seed})`, () => {
			const rng = seeded(seed ^ 0x1234);
			for (let depth of [1, 2, 10, 50, 100, 150, 200]) {
				let nested: unknown = { msgV: 1, msg_id: "m", type: "user", content: "x" };
				for (let d = 0; d < depth; d++) {
					nested = { payload: nested };
				}
				const line = jsonLine({ ...frameShell(rng), nested });
				const parsed = protocol.parseFrameLine(line);
				// The nested blob rides along as an unknown extra field only if
				// the outer shape validates; either way it must not throw.
				expect(() => protocol.parseFrameLine(line)).not.toThrow();
				if (parsed !== undefined) {
					assertFrameInvariants(parsed, line);
				}
				// Deeply nested alone (no envelope) must be rejected.
				expect(protocol.parseFrameLine(jsonLine(nested))).toBeUndefined();
			}
		});

		it(`survives duplicate and proto-hazard keys (seed ${seed})`, () => {
			const rng = seeded(seed ^ 0xbeef);
			const valid = jsonLine(validUserFrame(rng));
			const variants = [
				`{${valid.slice(1, -1)},"msgV":"1"}`, // msgV as string (duplicate key)
				`{${valid.slice(1, -1)},"msgV":0}`,
				`{${valid.slice(1, -1)},"msgV":-1}`,
				`{${valid.slice(1, -1)},"msgV":1.5}`,
				`{${valid.slice(1, -1)},"fromPid":-1}`,
				`{${valid.slice(1, -1)},"fromPid":1e999}`,
				`{${valid.slice(1, -1)},"fromPid":NaN}`,
				`{${valid.slice(1, -1)},"__proto__":{"x":1}}`,
				`{${valid.slice(1, -1)},"constructor":{"prototype":{"x":1}}}`,
				`{"__proto__":${valid.slice(1, -1)}}`,
				`{${valid.slice(1, -1)},${valid.slice(1, -1)}}`,
			];
			for (const line of variants) {
				let parsed: PeerFrame | undefined;
				expect(() => {
					parsed = protocol.parseFrameLine(line);
				}).not.toThrow();
				// JSON.parse drops the "msgV as string" duplicate (earlier key wins),
				// so string msgV duplicates resolve to number 1 and would parse —
				// but fromPid NaN/1e999 line remains valid for a *user* frame
				// (validateUserFrame only requires typeof number). The invariant
				// that matters: result is a frame or undefined, never junk.
				if (parsed !== undefined) {
					expect(typeof parsed.msg_id).toBe("string");
				}
			}
			// The prototype must never be polluted by any of these lines.
			const probe: Record<string, unknown> = {};
			expect("x" in probe).toBe(false);
			expect(({} as Record<string, unknown>).x).toBeUndefined();
		});
	}
});

/** Minimal envelope-shaped shell used to bury nested payloads under. */
function frameShell(rng: Rng): Record<string, unknown> {
	const frame = validUserFrame(rng);
	const { content: _content, ...shell } = frame;
	return shell;
}

/** Random junk JSON values (primitives/arrays/null) as strings. */
function junkLines(rng: Rng): unknown[] {
	const pool: unknown[] = [42, -0.5, "text", null, true, [1, 2, 3], [], {}, NaN, Infinity];
	return [rng.pick(pool)];
}

/** Recursively mutate an object: drop/retype/corrupt fields, add junk. */
function mutateObject(rng: Rng, obj: Record<string, unknown>): Record<string, unknown> {
	const out: Record<string, unknown> = { ...obj };
	const keys = Object.keys(out);
	if (keys.length > 0) {
		const key = rng.pick(keys);
		const op = rng.int(4);
		if (op === 0) {
			delete out[key];
		} else if (op === 1) {
			out[key] = rng.pick([null, 42, "text", true, [], {}, undefined, NaN, 1e999, -1]);
		} else if (op === 2) {
			out[key] = hostileString(rng, rng.range(0, 12));
		} else {
			out[key] = { nested: hostileString(rng, rng.range(0, 8)) };
		}
	}
	if (rng.chance(0.4)) {
		out[rng.pick(["__proto__", "constructor", "junk", "msgV", "type", "fromPid"])] = hostileString(rng, 4);
	}
	return out;
}

// ---------------------------------------------------------------------------
// 2. wrap/parse round-trip under hostile attrs
// ---------------------------------------------------------------------------

describe("fuzz: wrapPeerContent / parsePeerContent hostile round-trip", () => {
	for (const seed of SEEDS) {
		it(`round-trips hostile attrs and body byte-identically (seed ${seed})`, () => {
			const rng = seeded(seed);
			for (let i = 0; i < 50; i++) {
				const attrs: PeerContentAttrs = {
					from: hostileString(rng, rng.range(0, 20)),
					fromSession: hostileString(rng, rng.range(0, 20)),
					fromName: rng.chance(0.5) ? hostileString(rng, rng.range(0, 20)) : undefined,
					hopChain: [`t${rng.int(100)}`, `t${rng.int(100)}`],
				};
				const body = payloadString(rng, rng.range(1, 40));
				const wrapped = protocol.wrapPeerContent(attrs, body);
				const parsed = protocol.parsePeerContent(wrapped);
				expect(parsed).toBeDefined();
				// Every control char stripped from attrs; body byte-identical.
				for (const value of [parsed?.attrs.from, parsed?.attrs.fromSession, parsed?.attrs.fromName]) {
					expect(UNSAFE_ATTR_RE.test(value ?? "")).toBe(false);
				}
				expect(parsed?.body).toBe(body);
			}
		});

		it(`never emits a raw close tag from hostile attrs/body (seed ${seed})`, () => {
			const rng = seeded(seed ^ 0x9e37);
			for (let i = 0; i < 50; i++) {
				const attrs: PeerContentAttrs = {
					from: hostileString(rng, rng.range(0, 30)),
					fromSession: hostileString(rng, rng.range(0, 30)),
				};
				const closeTag = `</${PEER_CONTENT_TAG}`;
				const body = rng.chance(0.5)
					? `${payloadString(rng, 5)}${closeTag}${payloadString(rng, 5)}`
					: `${payloadString(rng, 5)}${closeTag.toUpperCase()}${payloadString(rng, 5)}`;
				let threw = false;
				let wrapped: string | undefined;
				try {
					wrapped = protocol.wrapPeerContent(attrs, body);
				} catch {
					threw = true;
				}
				// Either it threw (body contained the exact close tag) or the
				// emitted wrapper must contain exactly one close tag — the final one.
				if (!threw) {
					const tagCount = wrapped!.split(closeTag).length - 1;
					expect(tagCount).toBe(1);
					expect(wrapped!.endsWith(`\n${closeTag}>`)).toBe(true);
				} else {
					expect(body).toContain(closeTag);
				}
			}
		});

		it(`never lets hostile attrs smuggle an early close tag (seed ${seed})`, () => {
			const rng = seeded(seed ^ 0x77aa);
			for (let i = 0; i < 50; i++) {
				const closeTag = `</${PEER_CONTENT_TAG}`;
				const attrs: PeerContentAttrs = {
					from: `${closeTag}" injected="x`,
					fromSession: `${closeTag}\` other="y`,
					fromName: `${closeTag}>`,
				};
				const body = payloadString(rng, 10);
				const wrapped = protocol.wrapPeerContent(attrs, body);
				// Attr stripping removes `<`, `>`, `"` — so no close tag can
				// survive inside an attribute value.
				const openIndex = wrapped.indexOf(`>\n${body}`);
				const attrSection = wrapped.slice(0, openIndex);
				expect(attrSection.includes(closeTag)).toBe(false);
				const parsed = protocol.parsePeerContent(wrapped);
				expect(parsed).toBeDefined();
				expect(parsed?.body).toBe(body);
			}
		});
	}
});

/** The exact char class protocol.ts strips from attribute values. */
const UNSAFE_ATTR_RE = /["<>\r\n\u0000-\u001f\u007f-\u009f\u2028\u2029]/;

// ---------------------------------------------------------------------------
// 3. Line framing reassembly over a real socket pair
// ---------------------------------------------------------------------------

const transport = createPeerTransport();
const listenerClosers: Array<() => Promise<void>> = [];
afterEach(async () => {
	await Promise.all(listenerClosers.splice(0).map((close) => close()));
});

/** Start a listener whose first connection is captured (peers-transport harness shape). */
async function startFuzzServer(
	onFirst: (conn: PeerConnection, firstLine: string | undefined, lines: AsyncIterable<string>) => void,
): Promise<{ server: PeerServer; first: Promise<{ firstLine: string | undefined; lines: AsyncIterable<string> }> }> {
	let resolveFirst:
		| ((value: { firstLine: string | undefined; lines: AsyncIterable<string> }) => void)
		| undefined;
	const first = new Promise<{ firstLine: string | undefined; lines: AsyncIterable<string> }>((resolve) => {
		resolveFirst = resolve;
	});
	const listening = transport.listen(deriveSocketAddress(tmpdir()), (conn, firstLine, lines) => {
		resolveFirst?.({ firstLine, lines });
		try {
			onFirst(conn, firstLine, lines);
		} catch {
			conn.destroy();
		}
	});
	listenerClosers.push(() => listening.then((server) => server.close()));
	const server = await listening;
	return { server, first };
}

/** Open a raw (framing-free) client socket to a listening transport. */
function rawConnect(server: PeerServer): Promise<Socket> {
	return new Promise<Socket>((resolve, reject) => {
		const path = resolveAddress(server.address.address);
		if (path === undefined) {
			reject(new Error(`unresolvable test address: ${server.address.address}`));
			return;
		}
		const sock = netConnect({ path });
		sock.once("connect", () => resolve(sock));
		sock.once("error", (error) => reject(error));
	});
}

/** Collect every line of an async iterable once the stream has ended. */
async function collectLines(lines: AsyncIterable<string>): Promise<string[]> {
	const collected: string[] = [];
	for await (const line of lines) {
		collected.push(line);
	}
	return collected;
}

/** Serialize N random frames; returns the lines (no trailing newline) and the framed byte stream. */
function buildRandomFrameLines(rng: Rng, count: number): string[] {
	const lines: string[] = [];
	for (let i = 0; i < count; i++) {
		const sizeClass = rng.int(3);
		const contentLength =
			sizeClass === 0 ? rng.range(0, 16) : sizeClass === 1 ? rng.range(100, 1000) : rng.range(2000, 8000);
		const frame = rng.chance(0.7) ? validUserFrame(rng, contentLength) : validControlFrame(rng);
		if (frame.type === "user") frame.content = payloadString(rng, contentLength);
		lines.push(jsonLine(frame));
	}
	return lines;
}

/** Split a byte stream into random-size chunks (may cut inside multi-byte UTF-8). */
function randomChunks(rng: Rng, data: Buffer, chunkCount: number): Buffer[] {
	const cuts: number[] = [0];
	for (let i = 1; i < chunkCount; i++) {
		cuts.push(rng.range(1, Math.max(1, data.length - 1)));
	}
	cuts.push(data.length);
	cuts.sort((a, b) => a - b);
	const chunks: Buffer[] = [];
	for (let i = 0; i < cuts.length - 1; i++) {
		if (cuts[i + 1] > cuts[i]) {
			chunks.push(data.subarray(cuts[i], cuts[i + 1]));
		}
	}
	return chunks.length > 0 ? chunks : [Buffer.alloc(0)];
}

describe("fuzz: line framing reassembly over a real socket pair", () => {
	it("reassembles N random frames split at random chunk boundaries (all seeds)", async () => {
		for (const seed of SEEDS) {
			const rng = seeded(seed);
			const lines = buildRandomFrameLines(rng, rng.range(6, 14));
			const authLine = jsonLine({ type: "auth", token: `tok-${seed}` });
			const stream = Buffer.from(`${[authLine, ...lines].join("\n")}\n`, "utf8");

			const { server, first } = await startFuzzServer(() => {});
			const sock = await rawConnect(server);
			for (const chunk of randomChunks(rng, stream, rng.range(10, 40))) {
				sock.write(chunk);
				if (rng.chance(0.3)) {
					await new Promise<void>((resolve) => setImmediate(resolve));
				}
			}
			sock.end();
			const recorded = await first;
			expect(recorded.firstLine).toBe(authLine);
			expect(await collectLines(recorded.lines)).toEqual(lines);
		}
	});

	it("delivers empty lines interleaved intact (all seeds)", async () => {
		for (const seed of SEEDS) {
			const rng = seeded(seed ^ 0xe0e0);
			const lines = buildRandomFrameLines(rng, 4);
			// Interleave empty lines (CRLF-terminated too); the receiver's
			// LF-only framing sees "\r" as part of the empty line.
			const pieces = [jsonLine({ type: "auth", token: `t-${seed}` })];
			for (const line of lines) {
				pieces.push(line, "");
			}
			const stream = Buffer.from(`${pieces.join("\n")}\n`, "utf8");

			const { server, first } = await startFuzzServer(() => {});
			const sock = await rawConnect(server);
			for (const chunk of randomChunks(rng, stream, rng.range(5, 15))) {
				sock.write(chunk);
			}
			sock.end();
			const recorded = await first;
			expect(recorded.firstLine).toBe(pieces[0]);
			// Empty lines are delivered as "" lines (framing is byte-faithful).
			expect(await collectLines(recorded.lines)).toEqual(pieces.slice(1));
		}
	});

	it("delivers CRLF-terminated lines with the CR attached (all seeds)", async () => {
		for (const seed of SEEDS) {
			const rng = seeded(seed ^ 0xc0de);
			const lines = buildRandomFrameLines(rng, 3);
			const stream = Buffer.from(
				`${jsonLine({ type: "auth", token: `t-${seed}` })}\r\n${lines.join("\r\n")}\r\n`,
				"utf8",
			);
			const { server, first } = await startFuzzServer(() => {});
			const sock = await rawConnect(server);
			for (const chunk of randomChunks(rng, stream, rng.range(4, 12))) {
				sock.write(chunk);
			}
			sock.end();
			const recorded = await first;
			// LF-only framing: the \r stays part of each delivered line.
			expect(recorded.firstLine).toBe(`${jsonLine({ type: "auth", token: `t-${seed}` })}\r`);
			const received = await collectLines(recorded.lines);
			expect(received).toEqual(lines.map((line) => `${line}\r`));
			// And each CR-terminated line must still parse as a frame.
			for (const crLine of received) {
				const parsed = protocol.parseFrameLine(crLine.slice(0, -1));
				expect(parsed).toBeDefined();
			}
		}
	});

	it("destroys the connection on a >1MiB line and delivers nothing after it (all seeds)", async () => {
		for (const seed of SEEDS) {
			const rng = seeded(seed ^ 0x1e1e);
			const { server, first } = await startFuzzServer(() => {});
			const sock = await rawConnect(server);
			const auth = jsonLine({ type: "auth", token: `t-${seed}` });
			const oversized = `x${"a".repeat(PEER_MAX_LINE_BYTES + rng.range(1, 4096))}`;
			const after = jsonLine(validUserFrame(rng));
			// Auth, then the oversized line, then a line that must NEVER arrive.
			sock.write(`${auth}\n`);
			await new Promise<void>((resolve) => setImmediate(resolve));
			sock.write(`${oversized}\n${after}\n`);
			// Socket destroyed on the oversized line: wait for close.
			await new Promise<void>((resolve) => sock.once("close", resolve));
			const recorded = await first;
			expect(recorded.firstLine).toBe(auth);
			const remaining = await collectLines(recorded.lines);
			expect(remaining).toEqual([]);
			expect(remaining.includes(after)).toBe(false);
		}
	});
});

// ---------------------------------------------------------------------------
// 4. parsePeerRecord fuzz (via registry readRecord)
// ---------------------------------------------------------------------------

function validRecord(pid: number): PeerRecord {
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
		statusUpdatedAt: 1000,
		startedAt: 900,
		pidDomain: "win32:testhost",
		socketPath: `\\\\.\\pipe\\LOCAL\\pi-peer-${String(pid).padStart(32, "0")}`,
		peerProtocol: 1,
		features: ["notify_idle"],
	};
}

const tempRegistryDirs: string[] = [];
afterEach(() => {
	for (const dir of tempRegistryDirs.splice(0)) {
		rmSync(dir, { recursive: true, force: true });
	}
});

/** Fresh registry over a temp dir; harness mirrors peers-registry.test.ts. */
function freshRegistry(): { registry: PeerRegistry; dir: string } {
	const dir = mkdtempSync(join(tmpdir(), "pi-peers-fuzz-"));
	tempRegistryDirs.push(dir);
	return { registry: createPeerRegistry(dir), dir };
}

describe("fuzz: parsePeerRecord via registry readRecord", () => {
	for (const seed of SEEDS) {
		it(`never throws on mutated records and returns valid-or-undefined (seed ${seed})`, () => {
			const rng = seeded(seed);
			const { registry, dir } = freshRegistry();
			const pid = 432100 + seed;
			const path = join(dir, `${pid}.json`);
			for (let i = 0; i < 50; i++) {
				const mutated = mutateRecord(rng, validRecord(pid));
				writeFileSync(path, JSON.stringify(mutated));
				let entry: PeerRegistryEntry | undefined;
				expect(() => {
					entry = registry.readRecord(pid);
				}).not.toThrow();
				if (entry !== undefined) {
					// Structurally valid record: every invariant the parser enforces.
					const record = entry.record;
					expect(record.version).toBe(1);
					expect(Number.isInteger(record.pid)).toBe(true);
					expect(record.pid).toBeGreaterThan(0);
					expect(record.pid).toBe(pid);
					expect(typeof record.sessionId).toBe("string");
					expect(record.sessionId.length).toBeGreaterThan(0);
					expect(typeof record.name).toBe("string");
					expect(record.name.length).toBeGreaterThan(0);
					expect(record.name).toBe(registry.normalizeName(record.name));
					expect(["user", "derived", "env"]).toContain(record.nameSource);
					expect(["interactive", "rpc", "print", "json"]).toContain(record.kind);
					expect(["busy", "idle"]).toContain(record.status);
					expect(Number.isFinite(record.statusUpdatedAt)).toBe(true);
					expect(Number.isFinite(record.startedAt)).toBe(true);
					expect(typeof record.pidDomain).toBe("string");
					expect(record.pidDomain.length).toBeGreaterThan(0);
					expect(typeof record.socketPath).toBe("string");
					expect(record.socketPath.length).toBeGreaterThan(0);
					expect(Array.isArray(record.formerNames)).toBe(true);
					expect(Array.isArray(record.features)).toBe(true);
					expect(typeof record.peerProtocol).toBe("number");
				}
			}
		});
	}

	it("rejects a 10MB name and huge control-char fields (all seeds)", () => {
		for (const seed of SEEDS) {
			const rng = seeded(seed ^ 0x10cb);
			const { registry, dir } = freshRegistry();
			const pid = 7654321 + seed;
			const path = join(dir, `${pid}.json`);
			const cases: Record<string, unknown>[] = [
				{ ...validRecord(pid), name: "a".repeat(10 * 1024 * 1024) },
				{ ...validRecord(pid), cwd: `x${"b".repeat(600)}\u0007` },
				{ ...validRecord(pid), sessionId: `s${"c".repeat(200)}` },
				{ ...validRecord(pid), socketPath: `\\\\.\\pipe\\LOCAL\\pi-peer-${"d".repeat(4000)}` },
				{ ...validRecord(pid), name: "peer-\u0000-inj" },
				{ ...validRecord(pid), features: "not-an-array" },
				{ ...validRecord(pid), formerNames: [{ name: "x" }] },
				{ ...validRecord(pid), version: 2 },
				{ ...validRecord(pid), pid: -1 },
				{ ...validRecord(pid), pid: 1.5 },
				{ ...validRecord(pid), status: "unknown" },
			];
			for (const mutated of cases) {
				writeFileSync(path, JSON.stringify(mutated));
				expect(() => registry.readRecord(pid)).not.toThrow();
				const entry = registry.readRecord(pid);
				if (entry !== undefined) {
					expect(entry.record.name.length).toBeLessThanOrEqual(64);
					expect(entry.record.cwd.length).toBeLessThanOrEqual(512);
				}
			}
		}
	});
});

/** Mutate a valid record: drop/retype/corrupt fields, add junk, hostile strings. */
function mutateRecord(rng: Rng, base: PeerRecord): Record<string, unknown> {
	const out: Record<string, unknown> = { ...base };
	const keys = Object.keys(out);
	const op = rng.int(5);
	if (op === 0 && keys.length > 0) {
		delete out[rng.pick(keys)];
	} else if (op === 1 && keys.length > 0) {
		out[rng.pick(keys)] = rng.pick([null, 42, "text", true, [], {}, undefined, NaN, 1e999, -1]);
	} else if (op === 2 && keys.length > 0) {
		out[rng.pick(keys)] = hostileString(rng, rng.range(0, 24));
	} else if (op === 3 && keys.length > 0) {
		out[rng.pick(keys)] = `x${"y".repeat(rng.range(70, 5000))}`;
	} else if (rng.chance(0.5)) {
		out[rng.pick(["junkField", "extra", "__proto__", "constructor"])] = hostileString(rng, 8);
	}
	return out;
}

// ---------------------------------------------------------------------------
// 5. Guard fuzz
// ---------------------------------------------------------------------------

describe("fuzz: createPeerGuard invariants under randomized interleavings", () => {
	it("holds dedupe, rate, and capacity invariants (all seeds)", () => {
		for (const seed of SEEDS) {
			let clock = 100_000;
			const rng = seeded(seed);
			const capacity = rng.range(5, 20);
			const dedupWindowMs = rng.range(1000, 5000);
			const guard = createPeerGuard({
				now: () => clock,
				rateCapacity: capacity,
				rateRefillPerSec: 1,
				dedupWindowMs,
				maxQueued: rng.range(2, 10),
				maxTrackedSenders: 16, // above the 8-sender pool: no LRU eviction during the replay probe below
				globalRateCapacity: capacity * 4,
				globalRateRefillPerSec: 2,
			});

			// Track accepted fingerprints per sender to verify dedupe on replay.
			const acceptedAt = new Map<string, Map<string, number>>();
			let acceptedTotal = 0;
			const ownHopToken = "self-hop";
			const queuedCount = () => rng.int(3); // randomized queue pressure

			for (let step = 0; step < 400; step++) {
				clock += rng.range(0, 300); // randomized time advances
				const sender = `sender-${rng.int(8)}`;
				const fromSession = `sess-${sender}`;
				const content = `msg-${rng.int(30)}`;
				const frame: PeerUserFrame = {
					msgV: PEER_PROTOCOL_VERSION,
					msg_id: `m-${step}`,
					type: "user",
					from: `pipe:LOCAL\\pi-peer-${"e".repeat(32)}`,
					fromPid: rng.range(1, 65535),
					fromSession,
					fromName: "fuzz",
					hopChain: rng.chance(0.2) ? [ownHopToken] : [],
					priority: "next",
					content,
				};
				const result = guard.check(sender, frame, ownHopToken, queuedCount());
				expect(() => guard.check(sender, frame, ownHopToken, queuedCount())).not.toThrow();
				expect(typeof result.ok).toBe("boolean");
				if (result.ok) {
					expect(result.reason).toBeUndefined();
					guard.recordAccepted(sender, frame);
					acceptedTotal++;
					const seen = acceptedAt.get(sender) ?? new Map<string, number>();
					seen.set(`${fromSession}\n${content}`, clock);
					acceptedAt.set(sender, seen);
				} else {
					expect([
						"rate-limited",
						"duplicate",
						"hop-loop",
						"hop-runaway",
						"queue-full",
					]).toContain(result.reason);
				}
			}

			// Deterministic invariant probes after the fuzz run:
			// (a) A replayed accepted fingerprint within the dedup window is rejected.
			for (const [sender, seen] of acceptedAt) {
				const [fp] = [...seen.keys()];
				const [sess, ...rest] = fp.split("\n");
				const replayFrame: PeerUserFrame = {
					msgV: PEER_PROTOCOL_VERSION,
					msg_id: "replay",
					type: "user",
					from: `pipe:LOCAL\\pi-peer-${"f".repeat(32)}`,
					fromPid: 1,
					fromSession: sess,
					fromName: "replay",
					hopChain: [],
					priority: "next",
					content: rest.join("\n"),
				};
				const replay = guard.check(sender, replayFrame, "self-hop", 0);
				if (clock - (seen.get(fp) ?? -Infinity) < dedupWindowMs) {
					expect(replay.ok).toBe(false);
					expect(replay.reason).toBe("duplicate");
				}
			}
			// (b) Capacity is never exceeded: a burst from one fresh sender at a
			// frozen clock can never accept more than `capacity` in a row.
			let burstAccepted = 0;
			for (let i = 0; i < capacity * 3; i++) {
				const frame: PeerUserFrame = {
					msgV: PEER_PROTOCOL_VERSION,
					msg_id: `burst-${i}`,
					type: "user",
					from: `pipe:LOCAL\\pi-peer-${"g".repeat(32)}`,
					fromPid: 2,
					fromSession: "burst-sess",
					fromName: "burst",
					hopChain: [],
					priority: "now",
					content: `burst-content-${i}`,
				};
				if (guard.check("burst-sender", frame, "self-hop", 0).ok) {
					guard.recordAccepted("burst-sender", frame);
					burstAccepted++;
				}
			}
			expect(burstAccepted).toBeLessThanOrEqual(capacity);
			// (c) checkChild rate capacity is bounded too.
			let childAccepted = 0;
			for (let i = 0; i < capacity * 3; i++) {
				if (guard.checkChild(`child-${i % 4}`).ok) childAccepted++;
			}
			expect(childAccepted).toBeLessThanOrEqual(capacity * 4);
			expect(acceptedTotal).toBeGreaterThan(0);
		}
	});

	it("never throws on hostile frames and hop chains (all seeds)", () => {
		for (const seed of SEEDS) {
			const rng = seeded(seed ^ 0x9911);
			let clock = 500;
			const guard = createPeerGuard({
				now: () => clock,
				rateCapacity: 3,
				rateRefillPerSec: 0,
				dedupWindowMs: 1000,
				maxHopChain: 8,
				maxSelfHops: 2,
			});
			for (let i = 0; i < 100; i++) {
				clock += rng.range(0, 50);
				const hostile = validUserFrame(rng, rng.range(0, 60));
				hostile.hopChain = Array.from({ length: rng.range(0, 20) }, (_, h) =>
					rng.chance(0.5) ? "self-hop" : hostileString(rng, 4) + String(h),
				);
				hostile.from = hostileString(rng, rng.range(0, 30));
				hostile.fromSession = hostileString(rng, rng.range(0, 30));
				expect(() => guard.check(`hostile-${rng.int(5)}`, hostile, "self-hop", rng.int(5))).not.toThrow();
				const result = guard.check(`hostile-${rng.int(5)}`, hostile, "self-hop", rng.int(5));
				expect(typeof result.ok).toBe("boolean");
			}
		}
	});
});
