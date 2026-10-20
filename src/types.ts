/**
 * Cross-session messaging ("peers") — shared contract.
 *
 * This file is the single source of truth for the wire protocol, on-disk formats,
 * limits, and module interfaces of the peers extension. Every other module in this
 * directory imports from here. Design rationale: docs/peers.md.
 *
 * Model: no central server. The filesystem is the shared truth (a registry directory
 * with one JSON record per live instance), and a per-instance IPC endpoint (Windows
 * named pipe / POSIX unix socket) is the only active channel. Frames are newline-
 * delimited JSON, one short-lived connection per message.
 */

import { homedir } from "node:os";
import { join } from "node:path";

// ---------------------------------------------------------------------------
// Host surface (structural, host-agnostic)

//
// This package is a standalone extension loaded by pi-compatible hosts (pi,
// oh-my-pi, …) through their public extension API. It must not import from any
// host-internal path: every host type we rely on is declared here as the
// minimal structural slice we actually use. Host contexts are supersets of
// these interfaces, so they satisfy them at runtime without adaptation.
// ---------------------------------------------------------------------------

/** Subset of the host UI context used by peers. */
export interface PeersUIContext {
	notify(message: string, type?: "info" | "warning" | "error"): void;
	confirm(title: string, message: string): Promise<boolean>;
}

/** Subset of the host session manager used by peers. */
export interface PeersSessionManager {
	getSessionId(): string;
	getSessionName(): string | undefined;
	getCwd(): string;
}

/** Subset of the host extension context used by peers. */
export interface PeersExtensionContext {
	ui: PeersUIContext;
	hasUI: boolean;
	mode: string;
	cwd: string;
	sessionManager: PeersSessionManager;
	/** Whether the agent is currently idle (both pi and oh-my-pi expose this). */
	isIdle(): boolean;
}

/** Tool-execution context (peers uses nothing beyond the base context). */
export type PeersToolContext = PeersExtensionContext;

/** Command context (peers uses nothing beyond the base context). */
export type PeersCommandContext = PeersExtensionContext;

/** Minimal structural slice of the host's extension API object. */
export interface PeersHostAPI {
	on(event: string, handler: (event: never, ctx: PeersExtensionContext) => unknown): void;
	registerTool(tool: unknown): void;
	registerCommand(name: string, handlerOrOptions: unknown): void;
	sendUserMessage(text: string, options?: { deliverAs?: "steer" | "followUp" }): void;
}

/** Extension factory shape every pi-compatible host loader expects. */
export type PeersExtensionFactory = (pi: PeersHostAPI) => void;

/** Text content block of a tool result. */
export interface PeersToolTextContent {
	type: "text";
	text: string;
}

/** Tool result shape accepted by both hosts. */
export interface PeersToolResult {
	content: PeersToolTextContent[];
	isError?: boolean;
	details?: unknown;
}

/**
 * Minimal structural tool definition accepted by both hosts' registerTool.
 * `parameters` is a typebox schema; typed as unknown to stay validator-agnostic
 * (each host rebinds the import to its own typebox implementation).
 */
export interface PeersToolDefinition<TParams = unknown> {
	name: string;
	label: string;
	description: string;
	parameters: unknown;
	execute(
		toolCallId: string,
		params: TParams,
		signal: AbortSignal | undefined,
		onUpdate: unknown,
		ctx: PeersToolContext,
	): Promise<PeersToolResult>;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Wire/envelope version. Bump on incompatible frame changes. */
export const PEER_PROTOCOL_VERSION = 1;

/** Feature flags advertised in registry records. */
export const PEER_FEATURE_NOTIFY_IDLE = "notify_idle";
export const PEER_FEATURES: string[] = [PEER_FEATURE_NOTIFY_IDLE];

/** Registry directory name inside a product agent dir (built-in layout only). */
export const PEERS_DIR_NAME = "peers";
/** Subdirectory of the registry dir holding POSIX socket files. */
export const PEERS_SOCKETS_DIR_NAME = "socks";

/** Windows pipe basename / namespace prefix: \\.\pipe\LOCAL\pi-peer-<32 hex>. */
export const PEER_SOCKET_PREFIX = "pi-peer-";
/** Canonical socket-name matcher (Windows pipe name, optionally without the LOCAL\ prefix). */
export const PEER_SOCKET_NAME_RE = /^(?:LOCAL\\)?pi-peer-[0-9a-f]{32}$/;

/** Registry record file: <pid>.json */
export const PEER_RECORD_FILE_RE = /^(\d+)\.json$/;
/** Key file: <pid>.<sha256-of-canonical-socket-path>.key */
export const PEER_KEY_FILE_RE = /^(\d+)\.([0-9a-f]{64})\.key$/;

/** Max bytes of a single message on the wire, including the auth line. */
export const PEER_MAX_MESSAGE_BYTES = 1024 * 1024;
/** Max bytes of a single line (auth or frame), before framing overhead. */
export const PEER_MAX_LINE_BYTES = PEER_MAX_MESSAGE_BYTES;
/** Time allowed for the first (auth) line after a connection opens. */
export const PEER_FIRST_LINE_TIMEOUT_MS = 30_000;
/** Outbound connect timeout. */
export const PEER_CONNECT_TIMEOUT_MS = 5_000;
/** Delay before closing an outbound socket on macOS (tail-loss workaround). */
export const PEER_MACOS_CLOSE_DELAY_MS = 150;

/** Peer-guard defaults (inbound anti-abuse). */
export const PEER_RATE_CAPACITY = 30;
export const PEER_RATE_REFILL_PER_SEC = 0.5;
export const PEER_DEDUP_WINDOW_MS = 30_000;
export const PEER_MAX_SELF_HOPS = 10;
export const PEER_MAX_HOP_CHAIN = 28;
export const PEER_MAX_QUEUED_MESSAGES = 50;
export const PEER_MAX_TRACKED_SENDERS = 256;

/** Approval timeout for the "ask" accept policy. */
export const PEER_APPROVAL_TIMEOUT_MS = 60_000;
/** How long the outbox keeps receipt state for a sent message. */
export const PEER_RECEIPT_TTL_MS = 10 * 60_000;

// ---------------------------------------------------------------------------
// Environment variables
// ---------------------------------------------------------------------------

/** Truthy disables the peers extension entirely. */
export const ENV_PEERS_DISABLED = "PI_PEERS_DISABLED";
/** Accept policy override: "ask" | "all" | "off". */
export const ENV_PEERS_ACCEPT = "PI_PEERS_ACCEPT";
/** Parent socket address handed to child processes (read-only for us). */
export const ENV_PEERS_SOCKET = "PI_PEERS_SOCKET";
/** Child token handed to child processes (authenticates the child to the parent). */
export const ENV_PEERS_TOKEN = "PI_PEERS_TOKEN";
/** Override for the shared registry directory. */
export const ENV_PEERS_DIR = "PI_PEERS_DIR";

/**
 * Product-independent registry directory so pi, oh-my-pi, and any other host
 * loading this extension all discover the same live instances. Default
 * `~/.pi-peers`; `PI_PEERS_DIR` overrides (absolute path).
 */
export function resolvePeersDir(env: NodeJS.ProcessEnv = process.env): string {
	const override = env[ENV_PEERS_DIR];
	if (override !== undefined && override.trim() !== "") return override;
	return join(homedir(), ".pi-peers");
}

// ---------------------------------------------------------------------------
// Registry record (<peersDir>/<pid>.json)
// ---------------------------------------------------------------------------

export type PeerKind = "interactive" | "rpc" | "print" | "json";
export type PeerStatus = "busy" | "idle";
export type PeerNameSource = "user" | "derived";

export interface PeerFormerName {
	name: string;
	/** ISO timestamp when the name stopped being primary. */
	until: string;
}

export interface PeerRecord {
	version: 1;
	pid: number;
	sessionId: string;
	cwd: string;
	name: string;
	nameSource: PeerNameSource;
	formerNames: PeerFormerName[];
	kind: PeerKind;
	status: PeerStatus;
	/** ms epoch of last status change. */
	statusUpdatedAt: number;
	/** ms epoch of process start (as recorded by the process itself). */
	startedAt: number;
	/** Best-effort process start token for PID-reuse detection (Linux /proc starttime). Undefined when unavailable. */
	procStart?: string;
	/** Machine/namespace qualifier, e.g. "win32:<host>" or "posix:<host>" (Linux appends machine-id/pidns). */
	pidDomain: string;
	/** Canonical socket path (Windows: \\.\pipe\LOCAL\pi-peer-<32hex>; POSIX: absolute .sock path). */
	socketPath: string;
	peerProtocol: number;
	features: string[];
}

// ---------------------------------------------------------------------------
// Wire frames (newline-delimited JSON)
// ---------------------------------------------------------------------------

/** Envelope present on every frame except the auth line. */
export interface PeerEnvelope {
	msgV: 1;
	msg_id: string;
}

/** First line on every connection. No envelope; must arrive within PEER_FIRST_LINE_TIMEOUT_MS. */
export interface PeerAuthFrame {
	type: "auth";
	token: string;
}

export type PeerPriority = "next" | "now";

export interface PeerUserFrame extends PeerEnvelope {
	type: "user";
	/** Sender reply address: "pipe:LOCAL\\pi-peer-<32hex>" or "sock:/absolute/path". */
	from: string;
	fromPid: number;
	fromSession: string;
	fromName?: string;
	/** Instance hop tokens; each relay appends its own. */
	hopChain: string[];
	priority: PeerPriority;
	/** Body text wrapped via wrapPeerContent(). */
	content: string;
}

export type PeerReceiptState = "held" | "delivered" | "denied" | "expired" | "refused" | "dropped";

export interface PeerMessageStatusFrame extends PeerEnvelope {
	type: "control";
	action: "peer_message_status";
	/** msg_id of the user frame this acknowledges. */
	msg_id: string;
	from: string;
	fromName?: string;
	state: PeerReceiptState;
	/** DropReason or short explanation for denied/expired/dropped/refused. */
	reason?: string;
}

export interface PeerNotifyWhenIdleFrame extends PeerEnvelope {
	type: "control";
	action: "notify_when_idle";
	/** Address the notice must be sent to. Must pass isPeerSocketAddress(); self-targets are refused. */
	reply_to: string;
	from: string;
	fromName?: string;
	nonce?: string;
}

export interface PeerIdleNoticeFrame extends PeerEnvelope {
	type: "control";
	action: "peer_idle_notice";
	from: string;
	fromName?: string;
	nonce?: string;
}

export type PeerControlFrame = PeerMessageStatusFrame | PeerNotifyWhenIdleFrame | PeerIdleNoticeFrame;
export type PeerFrame = PeerUserFrame | PeerControlFrame;

// ---------------------------------------------------------------------------
// Content wrapping (the text the receiving agent actually sees)
// ---------------------------------------------------------------------------

/** XML-ish tag wrapping peer message bodies in the transcript. */
export const PEER_CONTENT_TAG = "cross-session-message";

export interface PeerContentAttrs {
	from?: string;
	fromSession?: string;
	fromName?: string;
	hopChain?: string[];
}

export interface ParsedPeerContent {
	attrs: PeerContentAttrs;
	body: string;
}

// ---------------------------------------------------------------------------
// Errors and outcomes
// ---------------------------------------------------------------------------

export type PeerDropReason = "rate-limited" | "duplicate" | "hop-loop" | "hop-runaway" | "queue-full";

/** Machine-readable send failure codes. */
export type PeerSendErrorCode =
	| "not_found" // no live peer matches the target name
	| "ambiguous" // several live peers match; candidates are listed
	| "offline" // record exists but process is dead / unreachable
	| "no_key" // key file missing: cannot authenticate
	| "no_live_inbox" // registry/key missing AND endpoint unverifiable; refusing to send
	| "connect_failed"
	| "timeout"
	| "message_too_large"
	| "sender_paced" // local pacing predicts the peer would drop us; merge and retry
	| "invalid_target";

export class PeerSendError extends Error {
	readonly code: PeerSendErrorCode;
	/** For "ambiguous": human-readable candidate lines. */
	readonly candidates: string[];
	constructor(code: PeerSendErrorCode, message: string, candidates: string[] = []) {
		super(message);
		this.name = "PeerSendError";
		this.code = code;
		this.candidates = candidates;
	}
}

export interface PeerSendOutcome {
	/** msg_id of the accepted frame. */
	msgId: string;
	/** Canonical socket address the frame was written to. */
	to: string;
	/** Name of the peer at send time, when known. */
	toName?: string;
}

// ---------------------------------------------------------------------------
// Module interfaces (implemented by sibling files in this directory)
// ---------------------------------------------------------------------------

/** protocol.ts */
export interface PeerProtocol {
	/** Serialize a frame to a single line (with trailing \n). Throws PeerSendError("message_too_large") past the cap. */
	serializeFrame(frame: PeerFrame): string;
	/** Parse one line (without trailing \n). Returns undefined on malformed input (caller drops the connection). */
	parseFrameLine(line: string): PeerFrame | undefined;
	/** Wrap a body into <cross-session-message …> text. Attribute values are escaped. */
	wrapPeerContent(attrs: PeerContentAttrs, body: string): string;
	/** Parse wrapped content. Enforces round-trip fidelity (re-serializing attrs must reproduce the input). */
	parsePeerContent(text: string): ParsedPeerContent | undefined;
	/** Build the auth line (JSON + \n). */
	buildAuthLine(token: string): string;
	/** Extract the token from an auth line, or undefined when malformed. */
	parseAuthLine(line: string): string | undefined;
	/** Fresh envelope for a new frame. */
	newEnvelope(): PeerEnvelope;
}

/** auth.ts */
export interface PeerTokens {
	peerToken: string;
	childToken: string;
}

export interface PeerAuth {
	generateTokens(): PeerTokens;
	/** Publish <pid>.<sha256(canonicalSocketPath)>.key (mode 0600, atomic via tmp+rename). */
	publishKey(
		peersDir: string,
		pid: number,
		canonicalSocketPath: string,
		tokens: PeerTokens,
		procStart: string | undefined,
		pidDomain: string,
	): void;
	/** Find and read the key file for a target socket path. Returns undefined when absent/malformed. */
	lookupKey(
		peersDir: string,
		canonicalSocketPath: string,
	): { pid: number; peerToken: string; procStart?: string; pidDomain?: string } | undefined;
	/** Best-effort removal of this instance's key files. */
	deleteKeys(peersDir: string, pid: number): void;
}

/** transport.ts */
export interface PeerSocketAddress {
	/** Canonical filesystem/pipe path used with net.connect/net.listen. */
	path: string;
	/** Stable address string embedded in frames: "pipe:<name>" or "sock:<path>". */
	address: string;
}

export interface PeerConnection {
	/** Write one line (appends \n when the line lacks one; pre-framed input from serializeFrame/buildAuthLine is written verbatim). Rejects when the cumulative message would exceed the cap. */
	writeLine(line: string): void;
	/** End the outbound side after flushing (applies the macOS close delay). */
	end(): Promise<void>;
	/** Destroy immediately. */
	destroy(): void;
}

export interface PeerServer {
	address: PeerSocketAddress;
	close(): Promise<void>;
}

export interface PeerTransport {
	/** Derive this process's socket address (random pipe name on Windows; <peersDir>/socks/<pid>.sock elsewhere). */
	deriveSocketAddress(peersDir: string): PeerSocketAddress;
	/** Validate an address string from the wire (namespace allowlist; rejects UNC/remote/symlink tricks). */
	isValidAddress(address: string): boolean;
	/** Resolve a wire address back to a connect()able path, or undefined when invalid/non-local. */
	resolveAddress(address: string): string | undefined;
	/** Convert a canonical connect path (as stored in PeerRecord.socketPath) to its wire address form. */
	addressFromPath(path: string): string | undefined;
	/** Start listening. onConnection receives raw lines after the auth line is consumed by the caller's handler. */
	listen(
		address: PeerSocketAddress,
		onConnection: (conn: PeerConnection, firstLine: string | undefined, lines: AsyncIterable<string>) => void,
		opts?: { firstLineTimeoutMs?: number },
	): Promise<PeerServer>;
	/** Connect with PEER_CONNECT_TIMEOUT_MS. Throws PeerSendError. */
	connect(address: string, opts?: { connectTimeoutMs?: number; macosCloseDelayMs?: number }): Promise<PeerConnection>;
	/** All lines of one connection as an async iterable (strict \n framing, per-line size cap). */
	readLines(conn: PeerConnection): AsyncIterable<string>;
}

/** registry.ts */
export interface PeerRegistryEntry {
	record: PeerRecord;
	/** True when the process is alive (pid + procStart when available). */
	live: boolean;
}

export type PeerResolution =
	| { kind: "unique"; entry: PeerRegistryEntry }
	| { kind: "ambiguous"; candidates: PeerRegistryEntry[] }
	| { kind: "none" };

export interface PeerRegistry {
	/** Write (atomically) this instance's record. */
	publish(record: PeerRecord): void;
	/** Patch our own record (e.g. status transitions). */
	update(pid: number, patch: Partial<PeerRecord>): void;
	/** Remove our record and stale key files. */
	unpublish(pid: number): void;
	/** Scan the registry dir; deletes malformed/dead records (self-healing). */
	list(): PeerRegistryEntry[];
	/** Resolve a user/agent-supplied name ("name", "name [ref]", or exact sessionId) to one live peer. */
	resolve(name: string): PeerResolution;
	/** Normalize a display name for comparison (NFKC, lowercased, spaces→dashes). */
	normalizeName(name: string): string;
	/** Short disambiguation ref for a record (first 6 hex of sha256(canonical socketPath)). */
	refOf(record: PeerRecord): string;
	/** Liveness probe: kill(pid,0) plus procStart match when both sides expose one. */
	isAlive(record: PeerRecord): boolean;
	/** Best-effort process start token (Linux /proc/<pid>/stat starttime; undefined elsewhere). */
	readProcStart(pid: number): string | undefined;
	/** This machine's pidDomain string. */
	pidDomain(): string;
}

/** guard.ts */
export interface PeerGuardCheck {
	ok: boolean;
	reason?: PeerDropReason;
}

export interface PeerGuard {
	/** Inbound check for a user frame. fromKey uniquely identifies the sender (socket address). */
	check(fromKey: string, frame: PeerUserFrame, ownHopToken: string, queuedCount: number): PeerGuardCheck;
	/** Record that a message from fromKey was accepted (feeds dedup/rate accounting). */
	recordAccepted(fromKey: string, frame: PeerUserFrame): void;
	/** Forget a sender that failed to deliver (rate-token refund). */
	refund(fromKey: string): void;
}

export interface PeerSenderPacer {
	/** True when a send to target may proceed; false = PeerSendError("sender_paced"). */
	tryConsume(target: string): boolean;
	/** Refund after a failed send. */
	refund(target: string): void;
}

/** Identity of this instance on the peer network. */
export interface PeerIdentity {
	pid: number;
	sessionId: string;
	socketAddress: string;
	name: string;
	hopToken: string;
	tokens: PeerTokens;
	/** Parent credentials from the environment, when this process was spawned by a peer. */
	parent?: { socketAddress: string; token: string };
}

/** outbox.ts */
export interface PeerSendOptions {
	priority?: PeerPriority;
	/** Also ask the target to notify us when it next goes idle. */
	notifyWhenIdle?: boolean;
}

export interface PeerOutbox {
	/** Resolve target name → live record → connect → auth → write frame. Throws PeerSendError. */
	send(target: string, body: string, opts: PeerSendOptions): Promise<PeerSendOutcome>;
	/** Deliver a control frame to an explicit wire address (used for receipts and idle notices). */
	sendControl(address: string, frame: PeerControlFrame): Promise<void>;
	/** Latest receipt state observed for a msg_id we sent. */
	receiptOf(msgId: string): PeerReceiptState | undefined;
	/** Handle an inbound peer_message_status frame. */
	handleReceipt(frame: PeerMessageStatusFrame): void;
}

/** inbox.ts */
export type PeerAcceptPolicy = "ask" | "all" | "off";

export interface PeerInboundResult {
	/** What the sender should be told. */
	receipt: PeerReceiptState;
	reason?: string;
}

export interface PeerInbox {
	/** Handle one authenticated inbound frame. May send receipts/notices via outbox. */
	handleFrame(frame: PeerFrame, identity: "peer" | "child"): Promise<void>;
	/** Called when the local agent becomes idle: fires one-shot notify_when_idle subscriptions. */
	onIdle(): void;
	/** Called on status transitions so queued-count bookkeeping stays exact. */
	pendingCount(): number;
}

// ---------------------------------------------------------------------------
// Extension wiring dependencies (index.ts constructs these; tests fake them)
// ---------------------------------------------------------------------------

/** What inbox/outbox need from the extension host — kept narrow for testability. */
export interface PeerHost {
	/** Inject a peer message into the local agent ("now" → steer, "next" → followUp). */
	injectUserMessage(content: string, priority: PeerPriority): Promise<void>;
	/** Surface a non-turn notification to the user when UI exists; otherwise a no-op. */
	notify(title: string, message: string): void;
	/** Ask the user to approve an inbound peer message. "timeout" when the user does not answer within timeoutMs. */
	confirm(title: string, message: string, timeoutMs: number): Promise<"yes" | "no" | "timeout">;
	/** Inject a receipt/peer event the agent must see (triggers a turn). */
	injectAgentNotice(text: string): Promise<void>;
	/** Current run mode has a dialog-capable UI. */
	hasUI(): boolean;
	/** Effective accept policy after env/default resolution. */
	acceptPolicy(): PeerAcceptPolicy;
}

export interface PeerServiceDeps {
	identity: PeerIdentity;
	protocol: PeerProtocol;
	transport: PeerTransport;
	auth: PeerAuth;
	registry: PeerRegistry;
	guard: PeerGuard;
	pacer: PeerSenderPacer;
	host: PeerHost;
	/** Set after construction (inbox needs it for receipts; outbox is built before inbox). */
	outbox: PeerOutbox;
}

/** tool.ts */
export interface PeerToolDeps {
	outbox: PeerOutbox;
	registry: PeerRegistry;
	identity: PeerIdentity;
}
