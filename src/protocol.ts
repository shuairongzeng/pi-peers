/**
 * Wire protocol for cross-session peer messaging: envelopes, frame serialization and strict
 * validation, auth lines, and the `cross-session-message` content wrapper.
 *
 * Inbound lines are untrusted: `parseFrameLine()` and `parseAuthLine()` never throw — any
 * malformed input returns undefined so the caller can drop the connection. `parsePeerContent()`
 * enforces byte-exact round-trip fidelity, so a forged or hand-edited wrapper never reaches the
 * receiving agent. Design rationale: README.md; contract: ./types.ts.
 */

import { randomUUID } from "node:crypto";
import {
	type ParsedPeerContent,
	PEER_CONTENT_TAG,
	PEER_MAX_MESSAGE_BYTES,
	PEER_PROTOCOL_VERSION,
	type PeerAuthFrame,
	type PeerContentAttrs,
	type PeerControlFrame,
	type PeerEnvelope,
	type PeerFrame,
	type PeerPriority,
	type PeerProtocol,
	type PeerReceiptState,
	PeerSendError,
	type PeerUserFrame,
} from "./types.ts";

// ---------------------------------------------------------------------------
// Input shape guards (each establishes one named contract on unknown input)
// ---------------------------------------------------------------------------

function isString(value: unknown): value is string {
	return typeof value === "string";
}

function isOptionalString(value: unknown): value is string | undefined {
	return value === undefined || typeof value === "string";
}

function isStringArray(value: unknown): value is string[] {
	return Array.isArray(value) && value.every((item) => typeof item === "string");
}

function isPriority(value: unknown): value is PeerPriority {
	return value === "next" || value === "now";
}

function isReceiptState(value: unknown): value is PeerReceiptState {
	return (
		value === "held" ||
		value === "delivered" ||
		value === "denied" ||
		value === "expired" ||
		value === "refused" ||
		value === "dropped"
	);
}

// ---------------------------------------------------------------------------
// Content wrapping (the text the receiving agent actually sees)
// ---------------------------------------------------------------------------

/** Attribute rendering order, kept canonical so re-wrapping is reproducible. */
const ATTR_SPECS: readonly { attr: string; field: keyof PeerContentAttrs }[] = [
	{ attr: "from", field: "from" },
	{ attr: "from-session", field: "fromSession" },
	{ attr: "hop-chain", field: "hopChain" },
	{ attr: "from-name", field: "fromName" },
];

/**
 * Stripped outright from attribute values: `"` breaks out of the quoted value, `<>` could form
 * tags, CR/LF would end the line, and C0 controls (ESC/NUL/BEL/...), DEL, C1 controls, and the
 * Unicode line/paragraph separators (U+2028/U+2029) would survive the byte-exact re-wrap on the
 * receive path into agent-visible text (terminal ANSI injection). Strip-only — no replacement
 * chars — so canonical re-wrap round-trip fidelity is preserved.
 */
const UNSAFE_ATTR_CHARS_RE = /["<>\r\n\u0000-\u001f\u007f-\u009f\u2028\u2029]/g;

const WRAP_RE = new RegExp(
	`^<${PEER_CONTENT_TAG}((?:\\s+[a-z-]+="[^"\\r\\n]*")*)\\s*>\\n([\\s\\S]*)\\n</${PEER_CONTENT_TAG}>$`,
);

function wrapPeerContent(attrs: PeerContentAttrs, body: string): string {
	if (body.includes(`</${PEER_CONTENT_TAG}`)) {
		throw new Error(
			`body contains "</${PEER_CONTENT_TAG}" which would terminate the wrapper early; ` +
				`write the body to a file and send the file path in the message instead`,
		);
	}
	const parts: string[] = [];
	for (const spec of ATTR_SPECS) {
		const raw = attrs[spec.field];
		if (raw === undefined || raw.length === 0) continue;
		const joined = Array.isArray(raw) ? raw.join(",") : raw;
		const value = joined.replace(UNSAFE_ATTR_CHARS_RE, "");
		if (value.length === 0) continue;
		parts.push(`${spec.attr}="${value}"`);
	}
	const attrText = parts.length > 0 ? ` ${parts.join(" ")}` : "";
	return `<${PEER_CONTENT_TAG}${attrText}>\n${body}\n</${PEER_CONTENT_TAG}>`;
}

function parseAttrs(text: string): PeerContentAttrs | undefined {
	const attrs: PeerContentAttrs = {};
	const block = text.trim();
	let cursor = 0;
	while (cursor < block.length) {
		const eq = block.indexOf("=", cursor);
		if (eq === -1) return undefined;
		const name = block.slice(cursor, eq);
		if (!/^[a-z-]+$/.test(name)) return undefined;
		if (block[eq + 1] !== '"') return undefined;
		const closing = block.indexOf('"', eq + 2);
		if (closing === -1) return undefined;
		const value = block.slice(eq + 2, closing);
		cursor = closing + 1;
		while (block[cursor] === " " || block[cursor] === "\t") cursor++;
		if (name === "from") {
			attrs.from = value;
		} else if (name === "from-session") {
			attrs.fromSession = value;
		} else if (name === "from-name") {
			attrs.fromName = value;
		} else if (name === "hop-chain") {
			const hops = value.split(",");
			if (hops.some((hop) => hop.length === 0)) return undefined;
			attrs.hopChain = hops;
		} else {
			return undefined;
		}
	}
	return attrs;
}

function parsePeerContent(text: string): ParsedPeerContent | undefined {
	const match = WRAP_RE.exec(text);
	if (!match) return undefined;
	const attrs = parseAttrs(match[1]);
	if (attrs === undefined) return undefined;
	const body = match[2];
	// Round-trip fidelity: re-wrapping the parsed attrs+body must reproduce the input byte-for-byte,
	// so tampered or hand-forged wrappers (reordered, duplicated, unknown or unsafe attributes) fail.
	let rebuilt: string;
	try {
		rebuilt = wrapPeerContent(attrs, body);
	} catch {
		return undefined;
	}
	return rebuilt === text ? { attrs, body } : undefined;
}

// ---------------------------------------------------------------------------
// Peer-supplied text sanitization (notices, receipts, idle messages)
// ---------------------------------------------------------------------------

/**
 * Stripped outright from peer text: C0 controls other than the ASCII whitespace collapsed
 * below, DEL, C1 controls, and `<>`/`"` which could forge wrapper tags or break out of a
 * quoted attribute value.
 */
const UNSAFE_TEXT_CHARS_RE = /[\u0000-\u0008\u000e-\u001f\u007f-\u009f<>"]/g;

/** Runs of ASCII whitespace (spaces, tabs, newlines — including runs the stripping reveals) collapse to a single space. */
const TEXT_WHITESPACE_RUN_RE = /[\t\n\v\f\r ]+/g;

function sanitizePeerText(text: string, maxLen = 120): string {
	const safe = text
		.replace(UNSAFE_TEXT_CHARS_RE, "")
		.replace(TEXT_WHITESPACE_RUN_RE, " ")
		.trim();
	if (safe.length <= maxLen) return safe;
	return safe.slice(0, Math.max(0, maxLen - 1)) + "…";
}

// ---------------------------------------------------------------------------
// Wire frames (newline-delimited JSON)
// ---------------------------------------------------------------------------

function newEnvelope(): PeerEnvelope {
	return { msgV: PEER_PROTOCOL_VERSION, msg_id: randomUUID() };
}

function serializeFrame(frame: PeerFrame): string {
	const line = `${JSON.stringify(frame)}\n`;
	const frameBytes = Buffer.byteLength(line, "utf-8");
	if (frameBytes > PEER_MAX_MESSAGE_BYTES) {
		throw new PeerSendError(
			"message_too_large",
			`peer message ${frame.msg_id} is ${frameBytes} bytes, over the ${PEER_MAX_MESSAGE_BYTES}-byte wire cap; ` +
				`write the large content to a file and send the file path in the message instead`,
		);
	}
	return line;
}

function validateUserFrame(obj: Record<string, unknown>): PeerUserFrame | undefined {
	if (!isString(obj.msg_id) || obj.msg_id.length === 0) return undefined;
	if (!isString(obj.from)) return undefined;
	if (typeof obj.fromPid !== "number") return undefined;
	if (!isString(obj.fromSession)) return undefined;
	if (!isOptionalString(obj.fromName)) return undefined;
	if (!isStringArray(obj.hopChain)) return undefined;
	if (!isPriority(obj.priority)) return undefined;
	if (!isString(obj.content)) return undefined;
	return {
		msgV: PEER_PROTOCOL_VERSION,
		msg_id: obj.msg_id,
		type: "user",
		from: obj.from,
		fromPid: obj.fromPid,
		fromSession: obj.fromSession,
		fromName: obj.fromName,
		hopChain: obj.hopChain,
		priority: obj.priority,
		content: obj.content,
	};
}

function validateControlFrame(obj: Record<string, unknown>): PeerControlFrame | undefined {
	if (!isString(obj.msg_id) || obj.msg_id.length === 0) return undefined;
	const action = obj.action;
	if (action === "peer_message_status") {
		if (!isString(obj.from)) return undefined;
		if (!isOptionalString(obj.fromName)) return undefined;
		if (!isReceiptState(obj.state)) return undefined;
		if (!isOptionalString(obj.reason)) return undefined;
		return {
			msgV: PEER_PROTOCOL_VERSION,
			msg_id: obj.msg_id,
			type: "control",
			action: "peer_message_status",
			from: obj.from,
			fromName: obj.fromName,
			state: obj.state,
			reason: obj.reason,
		};
	}
	if (action === "notify_when_idle") {
		if (!isString(obj.reply_to)) return undefined;
		if (!isString(obj.from)) return undefined;
		if (!isOptionalString(obj.fromName)) return undefined;
		if (!isOptionalString(obj.nonce)) return undefined;
		return {
			msgV: PEER_PROTOCOL_VERSION,
			msg_id: obj.msg_id,
			type: "control",
			action: "notify_when_idle",
			reply_to: obj.reply_to,
			from: obj.from,
			fromName: obj.fromName,
			nonce: obj.nonce,
		};
	}
	if (action === "peer_idle_notice") {
		if (!isString(obj.from)) return undefined;
		if (!isOptionalString(obj.fromName)) return undefined;
		if (!isOptionalString(obj.nonce)) return undefined;
		return {
			msgV: PEER_PROTOCOL_VERSION,
			msg_id: obj.msg_id,
			type: "control",
			action: "peer_idle_notice",
			from: obj.from,
			fromName: obj.fromName,
			nonce: obj.nonce,
		};
	}
	if (action === "shutdown") {
		if (!isString(obj.from)) return undefined;
		if (!isOptionalString(obj.fromName)) return undefined;
		if (!isOptionalString(obj.reason)) return undefined;
		return {
			msgV: PEER_PROTOCOL_VERSION,
			msg_id: obj.msg_id,
			type: "control",
			action: "shutdown",
			from: obj.from,
			fromName: obj.fromName,
			reason: obj.reason,
		};
	}
	if (action === "shutdown_response") {
		if (!isString(obj.inReplyTo)) return undefined;
		if (!isString(obj.from)) return undefined;
		if (!isOptionalString(obj.fromName)) return undefined;
		if (obj.outcome !== "accepted" && obj.outcome !== "denied") return undefined;
		if (!isOptionalString(obj.reason)) return undefined;
		return {
			msgV: PEER_PROTOCOL_VERSION,
			msg_id: obj.msg_id,
			type: "control",
			action: "shutdown_response",
			inReplyTo: obj.inReplyTo,
			from: obj.from,
			fromName: obj.fromName,
			outcome: obj.outcome,
			reason: obj.reason,
		};
	}
	return undefined;
}

function parseFrameLine(line: string): PeerFrame | undefined {
	let parsed: unknown;
	try {
		parsed = JSON.parse(line);
	} catch {
		return undefined;
	}
	if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return undefined;
	const obj = parsed as Record<string, unknown>;
	if (obj.msgV !== PEER_PROTOCOL_VERSION) return undefined;
	if (!isString(obj.msg_id) || obj.msg_id.length === 0) return undefined;
	if (obj.type === "user") return validateUserFrame(obj);
	if (obj.type === "control") return validateControlFrame(obj);
	return undefined;
}

// ---------------------------------------------------------------------------
// Auth line (no envelope)
// ---------------------------------------------------------------------------

function buildAuthLine(token: string): string {
	const frame: PeerAuthFrame = { type: "auth", token };
	return `${JSON.stringify(frame)}\n`;
}

function parseAuthLine(line: string): string | undefined {
	let parsed: unknown;
	try {
		parsed = JSON.parse(line);
	} catch {
		return undefined;
	}
	if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return undefined;
	const obj = parsed as Record<string, unknown>;
	if (obj.type !== "auth") return undefined;
	if (!isString(obj.token) || obj.token.length === 0) return undefined;
	return obj.token;
}

// ---------------------------------------------------------------------------
// Module entry point
// ---------------------------------------------------------------------------

export function createPeerProtocol(): PeerProtocol {
	return {
		newEnvelope,
		serializeFrame,
		parseFrameLine,
		wrapPeerContent,
		parsePeerContent,
		sanitizePeerText,
		buildAuthLine,
		parseAuthLine,
	};
}
