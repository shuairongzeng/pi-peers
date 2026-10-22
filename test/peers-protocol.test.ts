import { describe, expect, it } from "vitest";
import { createPeerProtocol } from "../src/protocol.ts";
import {
	PEER_CONTENT_TAG,
	PEER_MAX_MESSAGE_BYTES,
	PEER_PROTOCOL_VERSION,
	type PeerContentAttrs,
	type PeerFrame,
	PeerSendError,
} from "../src/types.ts";

const protocol = createPeerProtocol();

function userFrame(overrides: Record<string, unknown> = {}): PeerFrame {
	return {
		msgV: PEER_PROTOCOL_VERSION,
		msg_id: "11111111-2222-3333-4444-555555555555",
		type: "user",
		from: `pipe:LOCAL\\pi-peer-${"a".repeat(32)}`,
		fromPid: 4242,
		fromSession: "abcdef1234567890",
		hopChain: ["t1", "t2"],
		priority: "next",
		content: "how far along is the test refactor?",
		...overrides,
	} as PeerFrame;
}

function statusFrame(overrides: Record<string, unknown> = {}): PeerFrame {
	return {
		msgV: PEER_PROTOCOL_VERSION,
		msg_id: "11111111-2222-3333-4444-555555555555",
		type: "control",
		action: "peer_message_status",
		from: `pipe:LOCAL\\pi-peer-${"b".repeat(32)}`,
		state: "delivered",
		...overrides,
	} as PeerFrame;
}

function notifyWhenIdleFrame(overrides: Record<string, unknown> = {}): PeerFrame {
	return {
		msgV: PEER_PROTOCOL_VERSION,
		msg_id: "11111111-2222-3333-4444-555555555555",
		type: "control",
		action: "notify_when_idle",
		reply_to: `pipe:LOCAL\\pi-peer-${"c".repeat(32)}`,
		from: `pipe:LOCAL\\pi-peer-${"d".repeat(32)}`,
		...overrides,
	} as PeerFrame;
}

function idleNoticeFrame(overrides: Record<string, unknown> = {}): PeerFrame {
	return {
		msgV: PEER_PROTOCOL_VERSION,
		msg_id: "11111111-2222-3333-4444-555555555555",
		type: "control",
		action: "peer_idle_notice",
		from: `pipe:LOCAL\\pi-peer-${"e".repeat(32)}`,
		...overrides,
	} as PeerFrame;
}

function objLine(obj: object): string {
	return JSON.stringify(obj);
}

describe("createPeerProtocol", () => {
	describe("newEnvelope", () => {
		it("creates a v1 envelope with a unique UUID msg_id", () => {
			const a = protocol.newEnvelope();
			const b = protocol.newEnvelope();
			expect(a.msgV).toBe(1);
			expect(b.msgV).toBe(1);
			expect(a.msg_id).toMatch(/^[0-9a-f-]{36}$/);
			expect(b.msg_id).not.toBe(a.msg_id);
		});
	});

	describe("serializeFrame / parseFrameLine round-trip", () => {
		const frames: PeerFrame[] = [
			userFrame(),
			userFrame({ fromName: "alpha", priority: "now", hopChain: [], content: "" }),
			statusFrame(),
			statusFrame({ state: "held", reason: "queued", fromName: "beta" }),
			notifyWhenIdleFrame({ fromName: "gamma", nonce: "n-1" }),
			idleNoticeFrame({ fromName: "delta", nonce: "n-2" }),
		];

		for (const frame of frames) {
			it(`round-trips ${JSON.stringify(frame).slice(0, 60)}…`, () => {
				const line = protocol.serializeFrame(frame);
				expect(line.endsWith("\n")).toBe(true);
				const parsed = protocol.parseFrameLine(line.slice(0, -1));
				expect(parsed).toEqual(frame);
			});
		}

		it("round-trips multi-byte UTF-8 content", () => {
			const frame = userFrame({ content: "状态 → ✔ 世界 🎉" });
			const parsed = protocol.parseFrameLine(protocol.serializeFrame(frame).slice(0, -1));
			expect(parsed).toEqual(frame);
		});

		it("round-trips a frame just under the byte cap", () => {
			// The serialized line is the empty-content line with "" replaced by N single-byte chars.
			const emptyLine = protocol.serializeFrame(userFrame({ content: "" }));
			const fill = PEER_MAX_MESSAGE_BYTES - Buffer.byteLength(emptyLine, "utf-8");
			const frame = userFrame({ content: "x".repeat(fill) });
			const line = protocol.serializeFrame(frame);
			expect(Buffer.byteLength(line, "utf-8")).toBe(PEER_MAX_MESSAGE_BYTES);
			const parsed = protocol.parseFrameLine(line.slice(0, -1));
			expect(parsed).toEqual(frame);
		});

		it("accepts frames with unknown extra fields (forward compatibility)", () => {
			const line = objLine({ ...userFrame(), extra: "ignored" });
			const parsed = protocol.parseFrameLine(line);
			expect(parsed).toBeDefined();
		});
	});

	describe("serializeFrame size cap", () => {
		it("throws PeerSendError(message_too_large) past the cap with an actionable message", () => {
			const frame = userFrame({ content: "x".repeat(PEER_MAX_MESSAGE_BYTES + 2048) });
			let caught: unknown;
			try {
				protocol.serializeFrame(frame);
			} catch (error) {
				caught = error;
			}
			expect(caught).toBeInstanceOf(PeerSendError);
			const err = caught as PeerSendError;
			expect(err.code).toBe("message_too_large");
			expect(err.message).toContain("file");
			expect(err.message).toContain("path");
			expect(err.message).toContain(String(PEER_MAX_MESSAGE_BYTES)); // the cap itself
		});

		it("counts UTF-8 bytes, not UTF-16 code units", () => {
			// Each emoji is 4 UTF-8 bytes but 2 UTF-16 code units.
			const emoji = "🎉";
			const content = emoji.repeat(Math.ceil((PEER_MAX_MESSAGE_BYTES + 1024) / 4));
			const frame = userFrame({ content });
			expect(() => protocol.serializeFrame(frame)).toThrow(PeerSendError);
		});
	});

	describe("parseFrameLine malformed input", () => {
		const badLines: [string, string][] = [
			["not json", "invalid JSON"],
			["", "empty line"],
			["[1,2,3]", "array, not object"],
			["42", "number, not object"],
			['"string"', "string, not object"],
			["null", "null"],
			[objLine({ type: "user" }), "user frame missing envelope"],
			[objLine({ ...userFrame(), msgV: 2 }), "wrong msgV"],
			[objLine({ ...userFrame(), msgV: "1" }), "msgV as string"],
			[objLine({ ...userFrame(), msg_id: "" }), "empty msg_id"],
			[objLine({ ...userFrame(), msg_id: 123 }), "msg_id as number"],
			[objLine({ ...userFrame(), type: "other" }), "unknown type"],
			[objLine({ msgV: 1, msg_id: "m1" }), "missing type"],
			[objLine({ ...userFrame(), from: 123 }), "from as number"],
			[objLine({ ...userFrame(), fromPid: "4242" }), "fromPid as string"],
			[objLine({ ...userFrame(), fromSession: 1 }), "fromSession as number"],
			[objLine({ ...userFrame(), fromName: 5 }), "fromName as number"],
			[objLine({ ...userFrame(), hopChain: "t1" }), "hopChain as string"],
			[objLine({ ...userFrame(), hopChain: [1, 2] }), "hopChain with non-strings"],
			[objLine({ ...userFrame(), priority: "urgent" }), "invalid priority"],
			[objLine({ ...userFrame(), content: 3 }), "content as number"],
			[objLine(statusFrame({ action: "unknown_action" })), "unknown control action"],
			[objLine(statusFrame({ action: 42 })), "action as number"],
			[objLine({ msgV: 1, msg_id: "m1", type: "control" }), "control without action"],
			[objLine({ ...statusFrame(), state: "accepted" }), "invalid receipt state"],
			[objLine({ ...statusFrame(), state: 1 }), "state as number"],
			[objLine({ ...statusFrame(), from: null }), "status frame from: null"],
			[objLine({ ...statusFrame(), reason: 7 }), "reason as number"],
			[objLine({ ...notifyWhenIdleFrame(), reply_to: 3 }), "reply_to as number"],
			[objLine({ ...notifyWhenIdleFrame(), nonce: 3 }), "nonce as number"],
			[objLine({ ...idleNoticeFrame(), from: true }), "idle notice from as boolean"],
			[objLine({ ...idleNoticeFrame(), fromName: {} }), "idle notice fromName as object"],
		];

		for (const [line, label] of badLines) {
			it(`returns undefined for ${label}`, () => {
				expect(protocol.parseFrameLine(line)).toBeUndefined();
			});
		}

		it("never throws for any malformed input", () => {
			for (const [line] of badLines) {
				expect(() => protocol.parseFrameLine(line)).not.toThrow();
			}
		});
	});

	describe("auth line", () => {
		it("builds and parses an auth line", () => {
			const line = protocol.buildAuthLine("token-abc-123");
			expect(line).toBe('{"type":"auth","token":"token-abc-123"}\n');
			expect(protocol.parseAuthLine(line.slice(0, -1))).toBe("token-abc-123");
		});

		it("accepts tokens with JSON-escapable characters", () => {
			const token = 'quote" back\\slash\nnewline';
			const line = protocol.buildAuthLine(token);
			expect(line.endsWith("\n")).toBe(true);
			expect(protocol.parseAuthLine(line.slice(0, -1))).toBe(token);
		});

		const badAuthLines: [string, string][] = [
			["not json", "invalid JSON"],
			["", "empty line"],
			['{"type":"user","token":"t"}', "wrong type"],
			['{"type":"auth"}', "missing token"],
			['{"type":"auth","token":""}', "empty token"],
			['{"type":"auth","token":42}', "token as number"],
			['{"type":"auth","token":null}', "token as null"],
			["42", "number, not object"],
			["null", "null"],
		];

		for (const [line, label] of badAuthLines) {
			it(`returns undefined for ${label}`, () => {
				expect(protocol.parseAuthLine(line)).toBeUndefined();
			});
		}

		it("never throws for any malformed auth line", () => {
			for (const [line] of badAuthLines) {
				expect(() => protocol.parseAuthLine(line)).not.toThrow();
			}
		});
	});

	describe("wrapPeerContent / parsePeerContent", () => {
		it("wraps with all attributes present", () => {
			const attrs: PeerContentAttrs = {
				from: "pipe:LOCAL\\pi-peer-abc",
				fromSession: "sess-123",
				fromName: "alpha",
				hopChain: ["t1", "t2"],
			};
			const text = protocol.wrapPeerContent(attrs, "hello");
			expect(text).toBe(
				`<${PEER_CONTENT_TAG} from="pipe:LOCAL\\pi-peer-abc" from-session="sess-123" ` +
					`hop-chain="t1,t2" from-name="alpha">\nhello\n</${PEER_CONTENT_TAG}>`,
			);
		});

		it("omits attributes that are undefined or empty", () => {
			const text = protocol.wrapPeerContent({ from: "f", hopChain: [] }, "hi");
			expect(text).toBe(`<${PEER_CONTENT_TAG} from="f">\nhi\n</${PEER_CONTENT_TAG}>`);
			const bare = protocol.wrapPeerContent({}, "hi");
			expect(bare).toBe(`<${PEER_CONTENT_TAG}>\nhi\n</${PEER_CONTENT_TAG}>`);
		});

		it("strips injection characters from attribute values", () => {
			const attrs: PeerContentAttrs = {
				from: 'a"b<c>d\re\nf',
				fromSession: 'x"<>y\r\nz',
				fromName: '<script>"hi"\n</script>',
				hopChain: ['a"b', "c<d>", "e\r\nf"],
			};
			const text = protocol.wrapPeerContent(attrs, "body");
			expect(text).toBe(
				`<${PEER_CONTENT_TAG} from="abcdef" from-session="xyz" hop-chain="ab,cd,ef" ` +
					`from-name="scripthi/script">\nbody\n</${PEER_CONTENT_TAG}>`,
			);
		});

		it("drops an attribute entirely when stripping empties it", () => {
			const text = protocol.wrapPeerContent({ from: '""<>\r\n', fromSession: "ok" }, "b");
			expect(text).toBe(`<${PEER_CONTENT_TAG} from-session="ok">\nb\n</${PEER_CONTENT_TAG}>`);
		});

		it("throws when the body contains the closing tag", () => {
			const body = `prefix </${PEER_CONTENT_TAG} other`;
			expect(() => protocol.wrapPeerContent({ from: "f" }, body)).toThrow(Error);
			try {
				protocol.wrapPeerContent({ from: "f" }, body);
			} catch (error) {
				const err = error as Error;
				expect(err.message).toContain("file");
				expect(err.message).toContain(PEER_CONTENT_TAG);
			}
		});

		it("throws when the body is exactly the closing tag", () => {
			expect(() => protocol.wrapPeerContent({}, `</${PEER_CONTENT_TAG}>`)).toThrow();
		});

		it("round-trips attrs and body", () => {
			const attrs: PeerContentAttrs = {
				from: `pipe:LOCAL\\pi-peer-${"a".repeat(32)}`,
				fromSession: "abcdef1234567890",
				fromName: "alpha",
				hopChain: ["t1", "t2", "t3"],
			};
			const body = "line one\nline two: 参数 → done";
			const text = protocol.wrapPeerContent(attrs, body);
			const parsed = protocol.parsePeerContent(text);
			expect(parsed).toEqual({ attrs, body });
		});

		it("round-trips a body with a lone opening tag", () => {
			const body = `starts <${PEER_CONTENT_TAG} and continues`;
			const text = protocol.wrapPeerContent({ from: "f" }, body);
			const parsed = protocol.parsePeerContent(text);
			expect(parsed).toEqual({ attrs: { from: "f" }, body });
		});
	});

	describe("sanitizePeerText", () => {
		it("strips ANSI escapes, C0/C1/DEL controls, quotes, and angle brackets", () => {
			expect(protocol.sanitizePeerText("\u001b[31mred\u001b[0m")).toBe("[31mred[0m");
			expect(protocol.sanitizePeerText("a\u0000b\u0001c")).toBe("abc");
			expect(protocol.sanitizePeerText("a\u007fb")).toBe("ab");
			expect(protocol.sanitizePeerText("a\u0092b")).toBe("ab");
			expect(protocol.sanitizePeerText('say "hi" now')).toBe("say hi now");
			expect(protocol.sanitizePeerText("<script>alert</script>")).toBe("scriptalert/script");
		});

		it("collapses newlines and whitespace runs to a single space and trims", () => {
			expect(protocol.sanitizePeerText("line1\nline2")).toBe("line1 line2");
			expect(protocol.sanitizePeerText("line1\r\n\r\nline2")).toBe("line1 line2");
			expect(protocol.sanitizePeerText("a  \u0003  b")).toBe("a b");
			expect(protocol.sanitizePeerText("   padded   ")).toBe("padded");
		});

		it("leaves normal CJK/unicode text untouched", () => {
			expect(protocol.sanitizePeerText("会議は15時に開始")).toBe("会議は15時に開始");
			expect(protocol.sanitizePeerText("進捗どうですか？ ok")).toBe("進捗どうですか？ ok");
			expect(protocol.sanitizePeerText("定期同期 🚀 完了")).toBe("定期同期 🚀 完了");
		});

		it("caps at the default 120 chars, ellipsis counted within the cap", () => {
			const result = protocol.sanitizePeerText("a".repeat(200));
			expect(result).toBe(`${"a".repeat(119)}…`);
			expect(result.length).toBe(120);
		});

		it("caps at a custom maxLen and leaves exact fits untouched", () => {
			expect(protocol.sanitizePeerText("0123456789", 5)).toBe("0123…");
			expect(protocol.sanitizePeerText("abc", 3)).toBe("abc");
			expect(protocol.sanitizePeerText("abcd", 3)).toBe("ab…");
		});

		it("is idempotent, including truncated results", () => {
			const samples = [
				"\u001b[31mred\u001b[0m",
				"line1\n\nline2 <b> \"q\"",
				"a\u007f\u0092\u0000b",
				"a".repeat(200),
				"会議は15時に開始",
				"",
				"  \u000b spaced \t out \n",
			];
			for (const sample of samples) {
				const once = protocol.sanitizePeerText(sample);
				expect(protocol.sanitizePeerText(once)).toBe(once);
			}
		});
	});

	describe("parsePeerContent malformed input", () => {
		const goodText = protocol.wrapPeerContent(
			{ from: "f1", fromSession: "s1", fromName: "n1", hopChain: ["t1", "t2"] },
			"body",
		);

		const badTexts: [string, string][] = [
			["", "empty text"],
			["plain text", "not a wrapper"],
			[
				`<${PEER_CONTENT_TAG} from="f">\nbody\n</${PEER_CONTENT_TAG}>\ntrailing`,
				"trailing content after the close tag",
			],
			[`<${PEER_CONTENT_TAG} from="f">body</${PEER_CONTENT_TAG}>`, "no newlines around body"],
			[`<${PEER_CONTENT_TAG} from="f" from="f">\nbody\n</${PEER_CONTENT_TAG}>`, "duplicated attribute"],
			[`<${PEER_CONTENT_TAG} unknown="v">\nbody\n</${PEER_CONTENT_TAG}>`, "unknown attribute"],
			[`<${PEER_CONTENT_TAG} from-session="s" from="f">\nbody\n</${PEER_CONTENT_TAG}>`, "reversed attribute order"],
			[
				`<${PEER_CONTENT_TAG} hop-chain="t1,t2" from="f" from-session="s" from-name="n">\nbody\n</${PEER_CONTENT_TAG}>`,
				"rotated attribute order",
			],
			[`<${PEER_CONTENT_TAG} from="f" hop-chain="t1,,t2">\nbody\n</${PEER_CONTENT_TAG}>`, "empty hop in hop-chain"],
			[`<${PEER_CONTENT_TAG} from='f'>\nbody\n</${PEER_CONTENT_TAG}>`, "single-quoted attribute"],
			[`<${PEER_CONTENT_TAG} from="f\nbody\n</${PEER_CONTENT_TAG}>`, "unterminated attribute"],
			[`<${PEER_CONTENT_TAG}>\nbody\n</${PEER_CONTENT_TAG}`, "unterminated close tag"],
			[`<${PEER_CONTENT_TAG}>\nbody`, "missing close tag"],
		];

		for (const [text, label] of badTexts) {
			it(`returns undefined for ${label}`, () => {
				expect(protocol.parsePeerContent(text)).toBeUndefined();
			});
		}

		it("returns undefined for a value-reordered attribute block (round-trip mismatch)", () => {
			// The parsed values are valid, but re-wrapping emits the canonical order and no
			// longer matches the tampered text byte-for-byte.
			const tampered = goodText.replace('from="f1" from-session="s1"', 'from-session="s1" from="f1"');
			expect(protocol.parsePeerContent(tampered)).toBeUndefined();
		});

		it("returns undefined for a whitespace variant (extra spaces between attributes)", () => {
			const tampered = goodText.replace('from="f1" from-session="s1"', 'from="f1"  from-session="s1"');
			expect(protocol.parsePeerContent(tampered)).toBeUndefined();
		});

		it("accepts a structurally-faithful edit: a re-wrapped different body round-trips", () => {
			// Round-trip fidelity constrains the wrapper, not the content: a body swapped via a
			// fresh wrapPeerContent call is a legitimate message.
			const fresh = protocol.wrapPeerContent(
				{ from: "f1", fromSession: "s1", fromName: "n1", hopChain: ["t1", "t2"] },
				"b0dy",
			);
			expect(protocol.parsePeerContent(fresh)).toEqual({
				attrs: { from: "f1", fromSession: "s1", fromName: "n1", hopChain: ["t1", "t2"] },
				body: "b0dy",
			});
		});

		it("returns undefined for stripped injection characters smuggled back into the text", () => {
			// wrapPeerContent strips them, so a hand-crafted text containing them cannot round-trip.
			const smuggled = `<${PEER_CONTENT_TAG} from="a"b">\nbody\n</${PEER_CONTENT_TAG}>`;
			expect(protocol.parsePeerContent(smuggled)).toBeUndefined();
		});

		it("returns undefined when a hop-chain value cannot be a wrapped hop chain", () => {
			// A comma-separated hop-chain round-trips only through join(","); a single hop containing
			// a comma parses as two hops and fails the byte-exact re-wrap.
			const tricky = `<${PEER_CONTENT_TAG} from="f" hop-chain="a,b">\nbody\n</${PEER_CONTENT_TAG}>`;
			expect(protocol.parsePeerContent(tricky)).toBeDefined(); // ["a","b"] re-joins to "a,b"
			const mismatch = `<${PEER_CONTENT_TAG} from="f" hop-chain="a,,b">\nbody\n</${PEER_CONTENT_TAG}>`;
			expect(protocol.parsePeerContent(mismatch)).toBeUndefined();
		});

		it("never throws for any malformed wrapper", () => {
			const inputs = [...badTexts.map(([text]) => text), goodText, `<${PEER_CONTENT_TAG}`];
			for (const text of inputs) {
				expect(() => protocol.parsePeerContent(text)).not.toThrow();
			}
		});
	});
});
