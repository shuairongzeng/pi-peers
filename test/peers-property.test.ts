/**
 * Property-based tests (fast-check) for the peers pure functions and protocol surface.
 *
 * Complements peers-fuzz.test.ts (seeded mulberry32 fuzz) with shrinkable
 * fast-check properties: arbitrary inputs + invariant assertions, all pinned
 * to fixed seeds so runs are byte-identical across executions.
 *
 * Properties (one family per surface, asserted against the *documented*
 * semantics of src/protocol.ts, src/types.ts, src/guard.ts):
 *
 *  1. Frame round-trip: parseFrameLine(serializeFrame(f)) deep-equals f for
 *     any valid user/control frame across all five control actions.
 *  2. Robustness: parseFrameLine never throws on arbitrary strings/buffers
 *     and always rejects field-type-confused envelopes (msgV retype,
 *     invalid priority, non-array hopChain, missing required fields).
 *  3. Auth line: parseAuthLine(buildAuthLine(t)) === t; never throws.
 *  4. Content wrapping: wrapPeerContent/parsePeerContent equivalence for
 *     hostile attrs (quotes, angle brackets, controls, U+2028/2029) and
 *     multi-line bodies, per the strip-then-round-trip semantics.
 *  5. sanitizePeerText: output never contains banned code points, respects
 *     maxLen (>= 1), and is idempotent.
 *  6. types.ts helpers: normalizeFsPath idempotence + case folding,
 *     sameProject reflexivity/symmetry, resolvePeersDir override honoring,
 *     PEER_SOCKET_NAME_RE accept/reject on generated and mutated names.
 *  7. Guard token buckets: an exact-model oracle — under arbitrary
 *     interleavings of check/recordAccepted/refund/checkChild with a fake
 *     clock, the observable outcomes match the documented bucket math, so
 *     remaining credit can never exceed capacity nor go negative.
 */

import { describe, expect, it } from "vitest";
import fc from "fast-check";
import { createPeerGuard } from "../src/guard.ts";
import { createPeerProtocol } from "../src/protocol.ts";
import {
	ENV_PEERS_DIR,
	PEER_PROTOCOL_VERSION,
	PEER_SOCKET_NAME_RE,
	type PeerContentAttrs,
	type PeerGuardCheck,
	type PeerRecord,
	type PeerUserFrame,
	normalizeFsPath,
	resolvePeersDir,
	sameProject,
} from "../src/types.ts";

const protocol = createPeerProtocol();

/** Fixed seeds: every fc.assert pins one so the suite is deterministic. */
const SEED = 0x9e3779b9;

/** numRuns floor demanded by the property-test contract. */
const NUM_RUNS = 200;

// ---------------------------------------------------------------------------
// Shared hostile-text units
// ---------------------------------------------------------------------------

/**
 * Char pool covering the code points protocol.ts actually discriminates:
 * quotes, angle brackets, backslash, C0 controls (incl. ESC/NUL/TAB/CR/LF),
 * DEL, C1, U+2028/2029, both halves of an astral surrogate pair, plain text.
 */
const hostileUnit = fc.constantFrom(
	"a",
	"Z",
	"0",
	"-",
	" ",
	'"',
	"<",
	">",
	"\\",
	"\t",
	"\n",
	"\r",
	"\u0000",
	"\u001b",
	"\u007f",
	"\u0085",
	"\u0092",
	"\u2028",
	"\u2029",
	"\ud83c",
	"\udf89",
	"🎉",
	"会",
);

/** Arbitrary hostile string of at most `max` units. */
const hostileString = (max: number) => fc.string({ minLength: 0, maxLength: max, unit: hostileUnit });

/** Non-empty hostile string (msg_id must be non-empty). */
const nonEmptyHostileString = (max: number) => fc.string({ minLength: 1, maxLength: max, unit: hostileUnit });

/** The exact char class protocol.ts strips from attribute values (mirrors UNSAFE_ATTR_CHARS_RE). */
const UNSAFE_ATTR_CHARS_RE = /["<>\r\n\u0000-\u001f\u007f-\u009f\u2028\u2029]/g;

/** The exact char class sanitizePeerText strips (mirrors UNSAFE_TEXT_CHARS_RE). */
const UNSAFE_TEXT_CHARS_RE = /[\u0000-\u0008\u000e-\u001f\u007f-\u009f<>"]/g;

// ---------------------------------------------------------------------------
// Frame generators (all six frame variants)
// ---------------------------------------------------------------------------

const msgIdArb = nonEmptyHostileString(16);
const pidArb = fc.integer({ min: 1, max: 65_535 });

const userFrameArb: fc.Arbitrary<PeerUserFrame> = fc.record({
	msgV: fc.constant(PEER_PROTOCOL_VERSION),
	msg_id: msgIdArb,
	type: fc.constant("user"),
	from: hostileString(16),
	fromPid: pidArb,
	fromSession: hostileString(16),
	fromName: fc.option(hostileString(12), { nil: undefined }),
	hopChain: fc.array(hostileString(6), { minLength: 0, maxLength: 5 }),
	priority: fc.constantFrom("next", "now"),
	content: hostileString(60),
});

type ControlShape = Omit<Record<string, unknown>, "msgV" | "msg_id">;

const controlFrameArb: fc.Arbitrary<ControlShape> = fc.oneof(
	fc.record({
		type: fc.constant("control"),
		action: fc.constant("peer_message_status"),
		from: hostileString(16),
		fromName: fc.option(hostileString(12), { nil: undefined }),
		state: fc.constantFrom("held", "delivered", "denied", "expired", "refused", "dropped"),
		reason: fc.option(hostileString(20), { nil: undefined }),
	}),
	fc.record({
		type: fc.constant("control"),
		action: fc.constant("notify_when_idle"),
		reply_to: hostileString(16),
		from: hostileString(16),
		fromName: fc.option(hostileString(12), { nil: undefined }),
		nonce: fc.option(hostileString(8), { nil: undefined }),
	}),
	fc.record({
		type: fc.constant("control"),
		action: fc.constant("peer_idle_notice"),
		from: hostileString(16),
		fromName: fc.option(hostileString(12), { nil: undefined }),
		nonce: fc.option(hostileString(8), { nil: undefined }),
	}),
	fc.record({
		type: fc.constant("control"),
		action: fc.constant("shutdown"),
		from: hostileString(16),
		fromName: fc.option(hostileString(12), { nil: undefined }),
		reason: fc.option(hostileString(20), { nil: undefined }),
	}),
	fc.record({
		type: fc.constant("control"),
		action: fc.constant("shutdown_response"),
		inReplyTo: hostileString(12),
		from: hostileString(16),
		fromName: fc.option(hostileString(12), { nil: undefined }),
		outcome: fc.constantFrom("accepted", "denied"),
		reason: fc.option(hostileString(20), { nil: undefined }),
	}),
);

const frameArb = fc.oneof(
	userFrameArb,
	controlFrameArb.map((control) => ({ msgV: PEER_PROTOCOL_VERSION, msg_id: nonEmptyHostileStringSafe(control), ...control })),
);

/**
 * Deterministic msg_id derived from the control record's own fields — keeps
 * generation pure (no Math.random) so fast-check replay/shrink reproduces.
 */
function nonEmptyHostileStringSafe(seedObj: Record<string, unknown>): string {
	// Deterministic non-empty msg_id that exercises hostile code points too.
	const raw = `${seedObj.action}-${seedObj.from}`;
	const folded = [...raw].map((ch, i) => (i % 3 === 0 ? hostileUnits[i % hostileUnits.length] : ch)).join("");
	return folded.length === 0 ? "m" : folded;
}

const hostileUnits = ["a", '"', "\u2028", "\u001b", "\ud83c", "Z"];

/** Body generator: anything without the close-tag prefix (wrapPeerContent throws on it). */
const bodyArb = fc
	.string({ minLength: 0, maxLength: 60, unit: hostileUnit })
	.filter((body) => !body.includes("</cross-session-message"));

// ---------------------------------------------------------------------------
// 1. Frame round-trip
// ---------------------------------------------------------------------------

describe("property: serializeFrame / parseFrameLine round-trip", () => {
	it("parse(serialize(f)) deep-equals f for any user frame (msgV/msg_id preserved)", () => {
		fc.assert(
			fc.property(userFrameArb, (frame) => {
				const line = protocol.serializeFrame(frame);
				expect(line.endsWith("\n")).toBe(true);
				const parsed = protocol.parseFrameLine(line.slice(0, -1));
				expect(parsed).toEqual(frame);
			}),
			{ seed: SEED, numRuns: NUM_RUNS },
		);
	});

	it("parse(serialize(f)) deep-equals f for every control action variant", () => {
		fc.assert(
			fc.property(controlFrameArb, (control) => {
				const frame = { msgV: PEER_PROTOCOL_VERSION, msg_id: "m-1", ...control } as never;
				const line = protocol.serializeFrame(frame);
				const parsed = protocol.parseFrameLine(line.slice(0, -1));
				expect(parsed).toEqual(frame);
			}),
			{ seed: SEED ^ 0x51, numRuns: NUM_RUNS },
		);
	});

	it("round-trips frames carrying astral-plane and lone-surrogate payloads", () => {
		// JSON.stringify is well-formed in modern V8: lone surrogates round-trip as \uXXXX escapes.
		const astralArb = fc.record({
			content: fc.string({ minLength: 0, maxLength: 30, unit: "binary" }),
			hopChain: fc.array(fc.string({ minLength: 0, maxLength: 8, unit: "binary" }), { minLength: 0, maxLength: 3 }),
		});
		fc.assert(
			fc.property(astralArb, ({ content, hopChain }) => {
				const frame = userFrameWith({ content, hopChain });
				const line = protocol.serializeFrame(frame);
				const parsed = protocol.parseFrameLine(line.slice(0, -1));
				expect(parsed).toEqual(frame);
			}),
			{ seed: SEED ^ 0xa57, numRuns: NUM_RUNS },
		);
	});
});

/** Build a user frame overriding the free-text fields. */
function userFrameWith(overrides: { content?: string; hopChain?: string[] }): PeerUserFrame {
	return {
		msgV: PEER_PROTOCOL_VERSION,
		msg_id: "m",
		type: "user",
		from: "pipe:LOCAL\\pi-peer-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		fromPid: 4242,
		fromSession: "sess",
		hopChain: overrides.hopChain ?? [],
		priority: "next",
		content: overrides.content ?? "",
	};
}

// ---------------------------------------------------------------------------
// 2. parseFrameLine robustness: never throws, always rejects confusion
// ---------------------------------------------------------------------------

describe("property: parseFrameLine robustness", () => {
	it("never throws and returns frame-or-undefined for arbitrary strings", () => {
		fc.assert(
			fc.property(fc.string({ minLength: 0, maxLength: 200, unit: "binary-ascii" }), (line) => {
				let out: ReturnType<typeof protocol.parseFrameLine>;
				expect(() => {
					out = protocol.parseFrameLine(line);
				}).not.toThrow();
				// When it accepts, the structural invariants must hold.
				if (out !== undefined) {
					expect(out.msgV).toBe(PEER_PROTOCOL_VERSION);
					expect(typeof out.msg_id).toBe("string");
					expect(out.msg_id.length).toBeGreaterThan(0);
				}
			}),
			{ seed: SEED ^ 0xbeef, numRuns: NUM_RUNS },
		);
	});

	it("never throws for arbitrary buffers (coerced through toString)", () => {
		fc.assert(
			fc.property(fc.uint8Array({ minLength: 0, maxLength: 200, max: 255 }), (bytes) => {
				const line = Buffer.from(bytes).toString("latin1");
				expect(() => protocol.parseFrameLine(line)).not.toThrow();
			}),
			{ seed: SEED ^ 0xc0de, numRuns: NUM_RUNS },
		);
	});

	it("rejects msgV type confusion (string / 2 / 1.5 / null / undefined)", () => {
		const badMsgV = fc.constantFrom("1", 2, 1.5, 0, -1, null, undefined, true);
		fc.assert(
			fc.property(frameArb, badMsgV, (frame, msgV) => {
				const line = JSON.stringify({ ...frame, msgV });
				expect(protocol.parseFrameLine(line)).toBeUndefined();
			}),
			{ seed: SEED ^ 0xdead, numRuns: NUM_RUNS },
		);
	});

	it("rejects invalid priority values on user frames", () => {
		const badPriority = fc.constantFrom("urgent", "", "NEXT", "Now", null, undefined, 0, true, []);
		fc.assert(
			fc.property(userFrameArb, badPriority, (frame, priority) => {
				const line = JSON.stringify({ ...frame, priority });
				expect(protocol.parseFrameLine(line)).toBeUndefined();
			}),
			{ seed: SEED ^ 0xf00d, numRuns: NUM_RUNS },
		);
	});

	it("rejects non-array and non-string-array hopChain", () => {
		const badHopChain = fc.constantFrom("t1", 42, null, {}, [1, 2], ["a", null], ["a", 0], [true], "");
		fc.assert(
			fc.property(userFrameArb, badHopChain, (frame, hopChain) => {
				const line = JSON.stringify({ ...frame, hopChain });
				expect(protocol.parseFrameLine(line)).toBeUndefined();
			}),
			{ seed: SEED ^ 0x5eed, numRuns: NUM_RUNS },
		);
	});

	it("rejects frames with a required field removed (per variant)", () => {
		// Each frame has required fields beyond the envelope; removing any one must reject.
		fc.assert(
			fc.property(frameArb, fc.nat({ max: 60 }), (frame, dropIndex) => {
				const keys = Object.keys(frame as Record<string, unknown>);
				const key = keys[dropIndex % keys.length];
				const mutated: Record<string, unknown> = { ...(frame as Record<string, unknown>) };
				delete mutated[key];
				const line = JSON.stringify(mutated);
				const parsed = protocol.parseFrameLine(line);
				if (parsed !== undefined) {
					// Only genuinely-optional keys may vanish: the parse must then
					// deep-equal the frame with that optional key undefined.
					const expected = { ...mutated };
					optionalKeys.forEach((ok) => {
						if (!(ok in expected)) expected[ok] = undefined;
					});
					expect(parsed).toEqual(expected);
				}
			}),
			{ seed: SEED ^ 0x1234, numRuns: NUM_RUNS },
		);
	});
});

/** Keys whose absence is legal: only the documented optional fields. */
const optionalKeys = ["fromName", "reason", "nonce"] as const;

// ---------------------------------------------------------------------------
// 3. Auth line
// ---------------------------------------------------------------------------

describe("property: auth line round-trip", () => {
	it("parse(build(token)) === token for arbitrary non-empty tokens", () => {
		const tokenArb = nonEmptyHostileString(40);
		fc.assert(
			fc.property(tokenArb, (token) => {
				const line = protocol.buildAuthLine(token);
				expect(line.endsWith("\n")).toBe(true);
				expect(protocol.parseAuthLine(line.slice(0, -1))).toBe(token);
				// The trailing newline from buildAuthLine is tolerated by JSON.parse too.
				expect(protocol.parseAuthLine(line)).toBe(token);
			}),
			{ seed: SEED ^ 0xabc, numRuns: NUM_RUNS },
		);
	});

	it("never throws on arbitrary input lines", () => {
		fc.assert(
			fc.property(fc.string({ minLength: 0, maxLength: 200, unit: "binary-ascii" }), (line) => {
				expect(() => protocol.parseAuthLine(line)).not.toThrow();
			}),
			{ seed: SEED ^ 0xdef, numRuns: NUM_RUNS },
		);
	});

	it("rejects non-auth or malformed auth objects", () => {
		const badAuth = fc.constantFrom(
			'{"type":"user","token":"t"}',
			'{"type":"auth"}',
			'{"type":"auth","token":""}',
			'{"type":"auth","token":42}',
			'{"type":"auth","token":null}',
			"42",
			"null",
			"[1,2]",
			'"str"',
			"",
			"not json",
		);
		fc.assert(
			fc.property(badAuth, (line) => {
				expect(protocol.parseAuthLine(line)).toBeUndefined();
			}),
			{ seed: SEED ^ 0x999, numRuns: NUM_RUNS },
		);
	});
});

// ---------------------------------------------------------------------------
// 4. wrapPeerContent / parsePeerContent equivalence
// ---------------------------------------------------------------------------

describe("property: wrapPeerContent / parsePeerContent", () => {
	it("round-trips hostile attrs byte-identically when values stay clean enough", () => {
		// Attrs whose stripped values are non-empty and comma-free hops: exact identity.
		const cleanAttrsArb = fc
			.record({
				from: hostileString(20),
				fromSession: hostileString(20),
				fromName: fc.option(hostileString(20), { nil: undefined }),
				hopChain: fc.array(hostileString(6), { minLength: 0, maxLength: 4 }),
			})
			.map((attrs) => ({
				from: attrs.from.replace(UNSAFE_ATTR_CHARS_RE, ""),
				fromSession: attrs.fromSession.replace(UNSAFE_ATTR_CHARS_RE, ""),
				fromName: attrs.fromName?.replace(UNSAFE_ATTR_CHARS_RE, ""),
				hopChain: attrs.hopChain
					.map((hop) => hop.replace(UNSAFE_ATTR_CHARS_RE, "").replace(/,/g, ""))
					.filter((hop) => hop.length > 0),
			}))
			.map((attrs) => {
				const clean: PeerContentAttrs = {};
				if (attrs.from.length > 0) clean.from = attrs.from;
				if (attrs.fromSession.length > 0) clean.fromSession = attrs.fromSession;
				if (attrs.fromName !== undefined && attrs.fromName.length > 0) clean.fromName = attrs.fromName;
				if (attrs.hopChain.length > 0) clean.hopChain = attrs.hopChain;
				return clean;
			});
		fc.assert(
			fc.property(cleanAttrsArb, bodyArb, (attrs, body) => {
				const wrapped = protocol.wrapPeerContent(attrs, body);
				const parsed = protocol.parsePeerContent(wrapped);
				expect(parsed).toEqual({ attrs, body });
			}),
			{ seed: SEED ^ 0x77aa, numRuns: NUM_RUNS },
		);
	});

	it("equivalence relation for arbitrary hostile attrs (strip + comma-split semantics)", () => {
		// The documented semantics: attribute values are stripped of the unsafe class
		// (attr dropped when stripped empty), hop chains are comma-joined on wrap and
		// comma-split on parse (a hop containing a comma is NOT preserved), and the
		// body round-trips byte-identically.
		const attrsArb = fc.record({
			from: hostileString(20),
			fromSession: hostileString(20),
			fromName: fc.option(hostileString(20), { nil: undefined }),
			hopChain: fc.array(hostileString(8), { minLength: 0, maxLength: 5 }),
		});
		fc.assert(
			fc.property(attrsArb, bodyArb, (attrs, body) => {
				const wrapped = protocol.wrapPeerContent(attrs, body);
				const parsed = protocol.parsePeerContent(wrapped);

				// Oracle for the expected attrs, mirroring the documented wrap semantics.
				const expected: PeerContentAttrs = {};
				const scalar: [keyof PeerContentAttrs, string | undefined][] = [
					["from", attrs.from],
					["fromSession", attrs.fromSession],
					["fromName", attrs.fromName],
				];
				for (const [field, raw] of scalar) {
					if (raw === undefined) continue;
					const value = raw.replace(UNSAFE_ATTR_CHARS_RE, "");
					if (value.length === 0) continue;
					(expected as Record<string, unknown>)[field] = value;
				}
				if (attrs.hopChain.length > 0) {
					const joined = attrs.hopChain.join(",").replace(UNSAFE_ATTR_CHARS_RE, "");
					if (joined.length === 0) {
						// stripped to nothing -> attr absent
					} else if (joined.split(",").some((hop) => hop.length === 0)) {
						// an empty hop after comma-split -> parse must reject the wrapper
						expect(parsed).toBeUndefined();
						return;
					} else {
						expected.hopChain = joined.split(",");
					}
				}

				expect(parsed).toEqual({ attrs: expected, body });
			}),
			{ seed: SEED ^ 0x9e37, numRuns: NUM_RUNS },
		);
	});

	it("never lets attrs smuggle a close tag, quote breakout, or control char", () => {
		fc.assert(
			fc.property(hostileString(24), hostileString(24), hostileString(24), bodyArb, (from, session, name, body) => {
				const wrapped = protocol.wrapPeerContent({ from, fromSession: session, fromName: name }, body);
				const openIndex = wrapped.indexOf(">\n");
				expect(openIndex).toBeGreaterThan(0);
				const attrBlock = wrapped.slice(0, openIndex);
				// Attribute block: exactly the canonical tag + stripped, quoted values.
				expect(attrBlock).toMatch(/^<cross-session-message(?: [a-z-]+="[^"<>\r\n\u0000-\u001f\u007f-\u009f\u2028\u2029]*")*$/);
				expect(wrapped.endsWith(`\n</cross-session-message>`)).toBe(true);
				// The body region cannot be terminated early: the only close tag is the final one.
				const closeTag = "</cross-session-message>";
				expect(wrapped.split(closeTag).length - 1).toBe(1);
			}),
			{ seed: SEED ^ 0x33cc, numRuns: NUM_RUNS },
		);
	});

	it("bodies containing the close tag are refused at wrap (throws), never silently mangled", () => {
		// The documented contract: wrapPeerContent throws when body contains "</cross-session-message".
		const hostileBody = fc
			.string({ minLength: 0, maxLength: 30, unit: hostileUnit })
			.map((body) => `${body}</cross-session-message${body}`);
		fc.assert(
			fc.property(hostileBody, (body) => {
				expect(() => protocol.wrapPeerContent({ from: "f" }, body)).toThrow();
			}),
			{ seed: SEED ^ 0xf1ee, numRuns: NUM_RUNS },
		);
	});

	it("parsePeerContent never throws and rejects hand-forged wrappers with an early close tag", () => {
		const forgedArb = fc
			.tuple(bodyArb, bodyArb)
			.map(([head, tail]) => `<cross-session-message from="f">\n${head}</cross-session-message>${tail}\n</cross-session-message>`);
		fc.assert(
			fc.property(forgedArb, (text) => {
				expect(() => protocol.parsePeerContent(text)).not.toThrow();
				expect(protocol.parsePeerContent(text)).toBeUndefined();
			}),
			{ seed: SEED ^ 0x2bad, numRuns: NUM_RUNS },
		);
	});
});

// ---------------------------------------------------------------------------
// 5. sanitizePeerText
// ---------------------------------------------------------------------------

describe("property: sanitizePeerText", () => {
	const anyText = fc.string({ minLength: 0, maxLength: 300, unit: "binary" });

	it("output never contains a banned code point (UNSAFE_TEXT_CHARS_RE class)", () => {
		fc.assert(
			fc.property(anyText, (text) => {
				const out = protocol.sanitizePeerText(text);
				expect(UNSAFE_TEXT_CHARS_RE.test(out)).toBe(false);
			}),
			{ seed: SEED ^ 0x111, numRuns: NUM_RUNS },
		);
	});

	it("output never contains whitespace runs (two consecutive) or edge padding (collapse + trim)", () => {
		fc.assert(
			fc.property(anyText, (text) => {
				const out = protocol.sanitizePeerText(text);
				// Collapse leaves at most one whitespace char between non-whitespace chars,
				// and trim removes leading/trailing padding entirely.
				expect(/[\t\n\v\f\r ]{2}/.test(out)).toBe(false);
				expect(out.startsWith(" ")).toBe(false);
				expect(out.endsWith(" ")).toBe(false);
			}),
			{ seed: SEED ^ 0x222, numRuns: NUM_RUNS },
		);
	});

	it("length is capped at maxLen for maxLen >= 1 (default and custom)", () => {
		const maxLenArb = fc.integer({ min: 1, max: 500 });
		fc.assert(
			fc.property(anyText, maxLenArb, (text, maxLen) => {
				expect(protocol.sanitizePeerText(text, maxLen).length).toBeLessThanOrEqual(maxLen);
				expect(protocol.sanitizePeerText(text).length).toBeLessThanOrEqual(120);
			}),
			{ seed: SEED ^ 0x333, numRuns: NUM_RUNS },
		);
	});

	it("is idempotent under the same arguments", () => {
		const maxLenArb = fc.integer({ min: 1, max: 500 });
		fc.assert(
			fc.property(anyText, maxLenArb, (text, maxLen) => {
				const once = protocol.sanitizePeerText(text, maxLen);
				expect(protocol.sanitizePeerText(once, maxLen)).toBe(once);
			}),
			{ seed: SEED ^ 0x444, numRuns: NUM_RUNS },
		);
	});

	it("preserves already-clean text verbatim (fits, no banned chars, no runs, untrimmed edges equal)", () => {
		// Clean = printable graphemes minus the sanitize classes: no <>" (stripped),
		// no controls, and at most single isolated spaces (runs collapse).
		const cleanArb = fc
			.string({ minLength: 0, maxLength: 100, unit: fc.constantFrom("a", "Z", "0", "-", "_", ".", "?", "!", "会", "🎉") })
			.map((s) => s.replace(/[\t\n\v\f\r ]{2,}/g, " "));
		fc.assert(
			fc.property(cleanArb, (text) => {
				const padded = ` ${text} `;
				// Leading/trailing padding is trimmed; the interior is verbatim.
				expect(protocol.sanitizePeerText(padded, 120)).toBe(text.trim());
				expect(protocol.sanitizePeerText(text, 120)).toBe(text.trim());
			}),
			{ seed: SEED ^ 0x555, numRuns: NUM_RUNS },
		);
	});
});

// ---------------------------------------------------------------------------
// 6. types.ts helpers
// ---------------------------------------------------------------------------

describe("property: types.ts path helpers", () => {
	it("normalizeFsPath is idempotent", () => {
		const pathArb = fc
			.string({ minLength: 0, maxLength: 60, unit: fc.constantFrom("a", "B", "/", "\\", ":", ".", "0", " ") })
			.map((p) => `${p === "" ? "" : "C:"}${p}`);
		fc.assert(
			fc.property(pathArb, fc.boolean(), (p, windows) => {
				const once = normalizeFsPath(p, windows);
				expect(normalizeFsPath(once, windows)).toBe(once);
			}),
			{ seed: SEED ^ 0x666, numRuns: NUM_RUNS },
		);
	});

	it("windows folding: output is case-folded iff windows=true; posix preserves case", () => {
		const mixedPathArb = fc
			.string({ minLength: 0, maxLength: 60, unit: fc.constantFrom("a", "A", "z", "Z", "/", "\\", "-", "1") })
			.map((p) => `X:${p}`);
		fc.assert(
			fc.property(mixedPathArb, (p) => {
				const win = normalizeFsPath(p, true);
				expect(win).toBe(win.toLowerCase());
				expect(win).toBe(win.toUpperCase() === win ? win : win); // no-op sanity
				const posix = normalizeFsPath(p, false);
				// posix must differ from input only by backslash->slash and trailing-sep strip
				let expected = p.replace(/\\/g, "/");
				while (expected.length > 3 && expected.endsWith("/")) expected = expected.slice(0, -1);
				expect(posix).toBe(expected);
			}),
			{ seed: SEED ^ 0x777, numRuns: NUM_RUNS },
		);
	});

	it("normalizeFsPath folds case only when windows is set (same input, both flags)", () => {
		// 'C:/' is a bare drive root: the trailing separator is kept (length-3 floor).
		const caseyArb = fc
			.string({ minLength: 0, maxLength: 40, unit: fc.constantFrom("a", "A", "b", "B", "/") })
			.map((p) => `C:/${p}`);
		fc.assert(
			fc.property(caseyArb, (p) => {
				const win = normalizeFsPath(p, true);
				const posix = normalizeFsPath(p, false);
				// windows output is the posix output case-folded.
				expect(win).toBe(posix.toLowerCase());
				// posix output: backslashes become slashes, trailing separators stripped
				// while keeping a length-3 bare drive root like 'C:/'.
				let expected = p.replace(/\\/g, "/");
				while (expected.length > 3 && expected.endsWith("/")) expected = expected.slice(0, -1);
				expect(posix).toBe(expected);
			}),
			{ seed: SEED ^ 0x778, numRuns: NUM_RUNS },
		);
	});

	const recordArb: fc.Arbitrary<PeerRecord> = fc.record({
		version: fc.constant(1),
		pid: fc.integer({ min: 1, max: 65_535 }),
		sessionId: fc.string({ minLength: 1, maxLength: 12 }),
		cwd: fc.string({ minLength: 1, maxLength: 60, unit: fc.constantFrom("a", "B", "/", "\\", ":", "0") }).map((p) => `C:${p}`),
		projectRoot: fc.option(
			fc.string({ minLength: 1, maxLength: 60, unit: fc.constantFrom("a", "B", "/", "\\", ":", "0") }).map((p) => `C:${p}`),
			{ nil: undefined },
		),
		name: fc.string({ minLength: 1, maxLength: 12 }),
		nameSource: fc.constantFrom("user", "derived", "env"),
		formerNames: fc.constant([]),
		kind: fc.constantFrom("interactive", "rpc", "print", "json"),
		status: fc.constantFrom("busy", "idle"),
		statusUpdatedAt: fc.integer({ min: 0, max: 2 ** 40 }),
		startedAt: fc.integer({ min: 0, max: 2 ** 40 }),
		pidDomain: fc.constantFrom("win32:host-a", "posix:host-b"),
		socketPath: fc.constant("\\\\.\\pipe\\LOCAL\\pi-peer-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		peerProtocol: fc.constant(PEER_PROTOCOL_VERSION),
		features: fc.constant([]),
	});

	it("sameProject is reflexive", () => {
		fc.assert(
			fc.property(recordArb, (record) => {
				expect(sameProject(record, record)).toBe(true);
			}),
			{ seed: SEED ^ 0x888, numRuns: NUM_RUNS },
		);
	});

	it("sameProject is symmetric", () => {
		fc.assert(
			fc.property(recordArb, recordArb, (a, b) => {
				expect(sameProject(a, b)).toBe(sameProject(b, a));
			}),
			{ seed: SEED ^ 0x889, numRuns: NUM_RUNS },
		);
	});

	it("sameProject compares the normalized projectRoot (or cwd when absent)", () => {
		fc.assert(
			fc.property(recordArb, recordArb, (a, b) => {
				const windows = a.pidDomain.startsWith("win32:") || b.pidDomain.startsWith("win32:");
				const ra = a.projectRoot ?? a.cwd;
				const rb = b.projectRoot ?? b.cwd;
				expect(sameProject(a, b)).toBe(normalizeFsPath(ra, windows) === normalizeFsPath(rb, windows));
			}),
			{ seed: SEED ^ 0x88a, numRuns: NUM_RUNS },
		);
	});

	it("resolvePeersDir honors a non-blank PI_PEERS_DIR override and falls back otherwise", () => {
		const overrideArb = fc
			.string({ minLength: 1, maxLength: 60, unit: fc.constantFrom("X", ":", "/", "\\", "d", "i", "r", " ") })
			.map((s) => s.replace(/\\u0000/g, "")); // NUL impossible with this unit
		fc.assert(
			fc.property(overrideArb, (override) => {
				if (override.trim() === "") {
					// Blank override falls back to the default directory.
					expect(resolvePeersDir({ [ENV_PEERS_DIR]: override })).toBe(resolvePeersDir({}));
				} else {
					expect(resolvePeersDir({ [ENV_PEERS_DIR]: override })).toBe(override);
				}
				expect(resolvePeersDir({})).toBe(resolvePeersDir({ [ENV_PEERS_DIR]: "   " }));
			}),
			{ seed: SEED ^ 0x999, numRuns: NUM_RUNS },
		);
	});

	it("PEER_SOCKET_NAME_RE accepts every generated canonical name (with and without LOCAL\\)", () => {
		const hex32 = fc.string({ minLength: 32, maxLength: 32, unit: fc.constantFrom("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f") });
		fc.assert(
			fc.property(hex32, fc.boolean(), (hex, withLocal) => {
				const name = withLocal ? `LOCAL\\pi-peer-${hex}` : `pi-peer-${hex}`;
				expect(PEER_SOCKET_NAME_RE.test(name)).toBe(true);
			}),
			{ seed: SEED ^ 0xaaa, numRuns: NUM_RUNS },
		);
	});

	it("PEER_SOCKET_NAME_RE rejects mutated names (length, charset, case)", () => {
		const hex32 = fc.string({ minLength: 32, maxLength: 32, unit: fc.constantFrom("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f") });
		const mutationArb = fc.constantFrom(
			"length-1",
			"length+1",
			"uppercase-hex",
			"non-hex-letter",
			"missing-prefix",
			"wrong-prefix",
			"extra-suffix",
			"bad-namespace",
		);
		fc.assert(
			fc.property(hex32, mutationArb, (hex, mutation) => {
				let name: string;
				switch (mutation) {
					case "length-1":
						name = `pi-peer-${hex.slice(0, 31)}`;
						break;
					case "length+1":
						name = `pi-peer-${hex}0`;
						break;
					case "uppercase-hex":
						name = `pi-peer-${hex.toUpperCase()}`;
						break;
					case "non-hex-letter":
						name = `pi-peer-${"g".repeat(32)}`;
						break;
					case "missing-prefix":
						name = `peer-${hex}`;
						break;
					case "wrong-prefix":
						name = `pi-peers-${hex}`;
						break;
					case "extra-suffix":
						name = `pi-peer-${hex}x`;
						break;
					case "bad-namespace":
						name = `OTHER\\pi-peer-${hex}`;
						break;
				}
				expect(PEER_SOCKET_NAME_RE.test(name)).toBe(false);
			}),
			{ seed: SEED ^ 0xbbb, numRuns: NUM_RUNS },
		);
	});
});

// ---------------------------------------------------------------------------
// 7. Guard token buckets (exact-model oracle)
// ---------------------------------------------------------------------------

describe("property: createPeerGuard token buckets", () => {
	interface ModelBucket {
		tokens: number;
		last: number;
	}
	interface ModelState {
		bucket: ModelBucket;
		seen: Map<string, number>;
	}

	/**
	 * Oracle mirroring guard.ts's documented math exactly:
	 *  - per-sender bucket (capacity C1, refill R1), global bucket (Cg, Rg),
	 *    child bucket (Cc, Rc) — all refilled prorated per elapsed ms, capped at capacity;
	 *  - check() reads without persisting; recordAccepted()/checkChild()/refund() persist;
	 *  - refunds cap at capacity and only apply to already-tracked senders;
	 *  - dedup keyed by sha256(`${fromSession}\n${content}`) within dedupWindowMs.
	 */
	it("observable outcomes match the model exactly under arbitrary operation sequences", () => {
		const operations = fc.array(
			fc.tuple(
				fc.constantFrom("tick", "check", "record", "refund", "child"),
				fc.integer({ min: 0, max: 7 }),
			),
			{ minLength: 20, maxLength: 120 },
		);

		fc.assert(
			fc.property(operations, (ops) => {
				// Small fixed knobs keep the buckets tight so limits are actually hit.
				const rateCapacity = 5;
				const rateRefill = 0.7;
				const globalCapacity = 11;
				const globalRefill = 0.9;
				const childCapacity = 6;
				const childRefill = 1.3;
				const dedupWindowMs = 600;
				const maxQueued = 3;
				const maxTrackedSenders = 3;

				let clock = 1_000_000;
				const guard = createPeerGuard({
					now: () => clock,
					rateCapacity,
					rateRefillPerSec: rateRefill,
					globalRateCapacity: globalCapacity,
					globalRateRefillPerSec: globalRefill,
					childRateCapacity: childCapacity,
					childRateRefillPerSec: childRefill,
					dedupWindowMs,
					maxQueued,
					maxTrackedSenders,
				});

				const senders = new Map<string, ModelState>();
				const globalBucket: ModelBucket = { tokens: globalCapacity, last: clock };
				const childBucket: ModelBucket = { tokens: childCapacity, last: clock };

				const tokensAt = (b: ModelBucket, at: number, cap: number, refill: number): number =>
					Math.min(cap, b.tokens + (Math.max(0, at - b.last) * refill) / 1000);

				let contentCounter = 0;
				const mkFrame = (): PeerUserFrame => ({
					msgV: PEER_PROTOCOL_VERSION,
					msg_id: "m",
					type: "user",
					from: "pipe:LOCAL\\pi-peer-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
					fromPid: 1,
					fromSession: "sess",
					hopChain: [],
					priority: "next",
					content: `c${contentCounter}`,
				});
				const fingerprint = (frame: PeerUserFrame): string => `sess\n${frame.content}`;

				const oracleCheck = (sender: string, frame: PeerUserFrame, queued: number): PeerGuardCheck => {
					if (queued >= maxQueued) return { ok: false, reason: "queue-full" };
					const at = clock;
					const state = senders.get(sender);
					if (state !== undefined) {
						const seenAt = state.seen.get(fingerprint(frame));
						if (seenAt !== undefined && at - seenAt < dedupWindowMs) return { ok: false, reason: "duplicate" };
						if (tokensAt(state.bucket, at, rateCapacity, rateRefill) < 1) return { ok: false, reason: "rate-limited" };
					} else if (rateCapacity < 1) {
						return { ok: false, reason: "rate-limited" };
					}
					if (tokensAt(globalBucket, at, globalCapacity, globalRefill) < 1) return { ok: false, reason: "rate-limited" };
					return { ok: true };
				};

				const oracleRecord = (sender: string, frame: PeerUserFrame): void => {
					const at = clock;
					let state = senders.get(sender);
					if (state === undefined) {
						state = { bucket: { tokens: rateCapacity, last: at }, seen: new Map() };
					}
					senders.delete(sender);
					senders.set(sender, state);
					while (senders.size > maxTrackedSenders) {
						const evicted = senders.keys().next().value;
						if (evicted === undefined) break;
						senders.delete(evicted);
					}
					const cutoff = at - dedupWindowMs;
					for (const [fp, seenAt] of state.seen) {
						if (seenAt <= cutoff) state.seen.delete(fp);
					}
					state.bucket.tokens = Math.max(0, tokensAt(state.bucket, at, rateCapacity, rateRefill) - 1);
					state.bucket.last = at;
					globalBucket.tokens = Math.max(0, tokensAt(globalBucket, at, globalCapacity, globalRefill) - 1);
					globalBucket.last = at;
					state.seen.set(fingerprint(frame), at);
				};

				for (const [op, aux] of ops) {
					const sender = `s${aux % 4}`;
					switch (op) {
						case "tick": {
							clock += aux * 120;
							break;
						}
						case "check": {
							const frame = mkFrame();
							const queued = aux % (maxQueued + 2);
							const impl = guard.check(sender, frame, "self", queued);
							const oracle = oracleCheck(sender, frame, queued);
							expect(impl).toEqual(oracle);
							// check() must be side-effect free: the same call twice agrees.
							const again = guard.check(sender, frame, "self", queued);
							expect(again).toEqual(impl);
							contentCounter++;
							break;
						}
						case "record": {
							const frame = mkFrame();
							guard.recordAccepted(sender, frame);
							oracleRecord(sender, frame);
							contentCounter++;
							break;
						}
						case "refund": {
							guard.refund(sender);
							const state = senders.get(sender);
							if (state !== undefined) {
								state.bucket.tokens = Math.min(rateCapacity, tokensAt(state.bucket, clock, rateCapacity, rateRefill) + 1);
								state.bucket.last = clock;
								globalBucket.tokens = Math.min(globalCapacity, tokensAt(globalBucket, clock, globalCapacity, globalRefill) + 1);
								globalBucket.last = clock;
							}
							break;
						}
						case "child": {
							const impl = guard.checkChild("child-1");
							const childTokens = tokensAt(childBucket, clock, childCapacity, childRefill);
							const globalTokens = tokensAt(globalBucket, clock, globalCapacity, globalRefill);
							const oracle: PeerGuardCheck =
								childTokens < 1 || globalTokens < 1 ? { ok: false, reason: "rate-limited" } : { ok: true };
							expect(impl).toEqual(oracle);
							if (childTokens >= 1 && globalTokens >= 1) {
								childBucket.tokens = Math.max(0, childTokens - 1);
								childBucket.last = clock;
								globalBucket.tokens = Math.max(0, globalTokens - 1);
								globalBucket.last = clock;
							}
							break;
						}
					}
				}

				// Invariants the exact model implies directly:
				// remaining credit never exceeds capacity and never goes negative.
				for (const state of senders.values()) {
					expect(state.bucket.tokens).toBeGreaterThanOrEqual(0);
					expect(state.bucket.tokens).toBeLessThanOrEqual(rateCapacity);
				}
				expect(globalBucket.tokens).toBeGreaterThanOrEqual(0);
				expect(globalBucket.tokens).toBeLessThanOrEqual(globalCapacity);
				expect(childBucket.tokens).toBeGreaterThanOrEqual(0);
				expect(childBucket.tokens).toBeLessThanOrEqual(childCapacity);
			}),
			{ seed: SEED ^ 0x1e9f, numRuns: NUM_RUNS },
		);
	});

	it("a fresh sender can never burst past capacity at a frozen clock (with refill 0)", () => {
		fc.assert(
			fc.property(fc.integer({ min: 1, max: 12 }), (capacity) => {
				let clock = 5_000;
				const guard = createPeerGuard({
					now: () => clock,
					rateCapacity: capacity,
					rateRefillPerSec: 0,
					globalRateCapacity: capacity * 4,
					globalRateRefillPerSec: 0,
				});
				let accepted = 0;
				for (let i = 0; i < capacity * 3; i++) {
					const frame: PeerUserFrame = {
						msgV: PEER_PROTOCOL_VERSION,
						msg_id: `m-${i}`,
						type: "user",
						from: "f",
						fromPid: 1,
						fromSession: "s",
						hopChain: [],
						priority: "next",
						content: `c-${i}`,
					};
					if (guard.check("sender", frame, "self", 0).ok) {
						guard.recordAccepted("sender", frame);
						accepted++;
					}
				}
				// Bucket starts full; with zero refill, accepted burst is exactly capacity.
				expect(accepted).toBe(capacity);
			}),
			{ seed: SEED ^ 0x2f08, numRuns: NUM_RUNS },
		);
	});
});
