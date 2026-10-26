/**
 * The `send_peer_message` and `spawn_peer` tools and the `/peers` command.
 *
 * The message tool is the model-facing entry into the outbox: it takes a peer name plus a
 * body, delegates to `PeerOutbox.send()`, and turns outcomes and `PeerSendError` codes into
 * actionable text; its `action: "shutdown"` asks a peer to exit gracefully via a control
 * frame. The spawn tool launches a new host session through the injected spawner so the
 * agent can farm work out to fresh peers. Listing and sending are scoped to this project
 * by default (same git repo root); `scope: "all"` widens them to the whole machine. The
 * command lists the live peers from the registry.
 *
 * See types.ts for the module contract and README.md for the user-facing design.
 */

import { randomUUID } from "node:crypto";
import { type Static, Type } from "typebox";

import type {
	PeersCommandContext,
	PeersToolDefinition,
	PeersToolResult,
	PeerRecord,
	PeerRegistry,
	PeerSendOptions,
	PeerSendOutcome,
	PeerShutdownFrame,
	PeerSpawnOptions,
	PeerToolDeps,
} from "./types.ts";
import {
	PEER_PROTOCOL_VERSION,
	PeerSendError,
	PeerSpawnError,
	projectRootOf,
	sameProjectEntries,
} from "./types.ts";
import type { PeerLiveTargetResolution, PeerOutbox, PeerSendErrorCode, PeersToolContext } from "./types.ts";

// ---------------------------------------------------------------------------
// Tool
// ---------------------------------------------------------------------------

export const PEER_MESSAGE_TOOL_NAME = "send_peer_message";

export const peerMessageSchema = Type.Object({
	action: Type.Optional(
		Type.Union([Type.Literal("send"), Type.Literal("list"), Type.Literal("shutdown")], {
			description:
				'"send" (default): deliver a message to a peer. ' +
				'"list": show the live peer sessions on this machine (use this first when you do not know the peer\'s name). ' +
				'"shutdown": ask a peer to close its session gracefully.',
		}),
	),
	to: Type.Optional(
		Type.Union(
			[
				Type.String({
					description:
						'Peer name to send to, as shown by action="list" (e.g. "alice"). ' +
						'When several sessions share a name, append the disambiguation ref from the candidate list: "alice [ab12cd]". ' +
						"Required for send; ignored for list.",
				}),
				Type.Array(Type.String(), {
					minItems: 1,
					maxItems: 32,
					description:
						"Array of 1–32 peer names for a batch send: the same message goes to every target. " +
						"A failing target does not stop the rest; the result lists one line per target. " +
						"Cannot be combined with notifyWhenIdle.",
				}),
			],
			{
				description:
					"Send target(s): a single peer name, or an array of peer names to batch-send the same message to all of them.",
			},
		),
	),
	message: Type.Optional(
		Type.String({
			description:
				"Message body. It is delivered as user input to the target session's agent. " +
				"For large content, write it to a file first and send the path instead. Required for send; ignored for list.",
		}),
	),
	priority: Type.Optional(
		Type.Union([Type.Literal("next"), Type.Literal("now")], {
			description:
				'"next" (default): queue the message until the peer finishes its current work. ' +
				'"now": interrupt the peer immediately and inject the message.',
		}),
	),
	notifyWhenIdle: Type.Optional(
		Type.Boolean({ description: "Also ask the peer to notify us once when it next becomes idle." }),
	),
	reason: Type.Optional(
		Type.String({
			description:
				'Shutdown explanation, shown to the peer (its human may see it in the quit confirmation). ' +
				'Optional; only used with action: "shutdown".',
		}),
	),
	scope: Type.Optional(
		Type.Union([Type.Literal("project"), Type.Literal("all")], {
			description:
				'"project" (default): only sessions in this same project (same git repo root) are listed and reachable. ' +
				'"all": widen to every live session on this machine, including other projects. ' +
				'Only use "all" when the user explicitly asks to reach a session in another project; cross-project sends require it.',
		}),
	),
});

export type PeerMessageInput = Static<typeof peerMessageSchema>;

const PEER_MESSAGE_DESCRIPTION = `# Send a message to another pi session on this machine

Sends a message to another running pi instance ("peer") on the same machine. The message is processed as user input by the peer's agent.

- Project scope by default: \`action: "list"\` and sends only see sessions in this same project (sessions whose git repo root matches this session's). Sessions in other projects are hidden from you; only widen with \`scope: "all"\` when the user explicitly asks to reach a session in another project.
- To discover peers, call with \`action: "list"\` — you get each live session's name, status, kind, working directory, and disambiguation ref. You do not need the user to tell you peer names.
- When you RECEIVE a \`<cross-session-message from="ADDRESS" from-name="X" …>\` in your context, that is a message from peer X. If it asks for a reply or reports results back, you MUST answer by calling this tool — \`to\` accepts the message's \`from\` address verbatim, or the sender's name. Writing the answer only in your local chat never reaches the sender — they cannot see your screen.
- A send failing with "belongs to another project" is a scoping block, not a missing peer: the target exists in another project; resend with \`scope: "all"\` only if the user asked to reach that session.
- If several sessions share one name, the send fails with a candidate list; resend with "name [ref]" to disambiguate, e.g. "alice [ab12cd]".
- \`action: "shutdown"\` asks the peer to close its session gracefully. The peer's own accept policy decides: with "ask" the human there may be asked to confirm, and it may refuse. Collect any results or state you need from the peer BEFORE shutting it down — after it exits there is nothing left to ask.
- \`notifyWhenIdle\`: also subscribes (once) to be notified when the peer next becomes idle.
- Success only means the message reached the peer's queue. Success is silent: you hear nothing more about delivery. Failures (denied / expired / refused / dropped) and the peer's reply arrive as messages in later turns. Never poll, sleep, or wait for either — after sending, continue with other work or end your turn.
- Silence is not consent: no reply within a timeout does not mean the peer agreed or acted.
- \`to\` accepts an array of peer names (up to 32) to batch-send the same message to all of them: one send per target, a failing target does not stop the rest, and the result reports one line per target. Batch sends cannot use \`notifyWhenIdle\`.
- For large content, write it to a file and send the file path in the message; each message is capped at 1 MiB on the wire.`;

/**
 * One listing line for a live peer, shared by the tool's `action: "list"`
 * rendering and the /peers command (index.ts reuses it for its scoped view).
 * The context fill (`ctx N%`) appears right after the status when the record
 * reports a numeric percentage; absent or null percentages print nothing.
 */
export function peerLine(record: PeerRecord, self: boolean, refOf: (record: PeerRecord) => string): string {
	const pct = record.contextUsage?.percent;
	const usage = typeof pct === "number" ? `  ctx ${Math.round(pct)}%` : "";
	const suffix = self ? "  (this session)" : "";
	return `${record.name} [${refOf(record)}]  ${record.status}${usage}  ${record.kind}  ${record.cwd}  (${record.sessionId.slice(0, 8)})${suffix}`;
}

function listLivePeers(
	registry: PeerRegistry,
	selfPid: number,
	scope: "project" | "all",
	selfCwd: string,
): string {
	const live = registry.list().filter((entry) => entry.live);
	const sameProject = sameProjectEntries(live, selfPid, selfCwd, registry.pidDomain());
	const entries = scope === "all" ? live : live.filter((entry) => sameProject(entry));
	const hidden = live.length - entries.length;
	if (entries.length === 0) {
		return hidden > 0 ? `No live peers in this project. (+${hidden} in other projects)` : "No live peers in this project.";
	}
	const lines = entries.map((entry) => peerLine(entry.record, entry.record.pid === selfPid, registry.refOf));
	if (hidden > 0) {
		lines.push(`(+${hidden} in other projects)`);
	}
	return lines.join("\n");
}

/**
 * When the default project scope would hide the send target: resolves `to`
 * within the same project first; if that yields nothing, resolves it against
 * the full live list. A unique hit there (but not in the project list) is a
 * cross-project target — reported as an `invalid_target` PeerSendError whose
 * message names the target's project root. Ambiguity and absence fall through
 * to the outbox, which owns those diagnostics.
 */
function findCrossProjectTarget(deps: PeerToolDeps, target: string, selfCwd: string): PeerSendError | undefined {
	const live = deps.registry.list().filter((entry) => entry.live);
	const sameProject = sameProjectEntries(live, deps.identity.pid, selfCwd, deps.registry.pidDomain());
	const inProject = deps.registry.resolveIn(target, live.filter((entry) => sameProject(entry)));
	if (inProject.kind !== "none") {
		return undefined;
	}
	const anywhere = deps.registry.resolveIn(target, live);
	if (anywhere.kind === "unique") {
		const record = anywhere.entry.record;
		return new PeerSendError(
			"invalid_target",
			`target "${target}" belongs to another project (${projectRootOf(record)}); pass scope: "all" to message across projects`,
		);
	}
	return undefined;
}

function describeSendError(
	error: PeerSendError,
	registry: PeerRegistry,
	selfPid: number,
	scope: "project" | "all",
	selfCwd: string,
): string {
	// The outbox's error messages are already complete user-facing sentences
	// (target, cause, remedy). Render them verbatim — never re-prefix with the
	// category name, which produced double-wrapped text like
	// 'No live peer matches "no live peer matches "alice"…"'.
	switch (error.code) {
		case "not_found":
			// The listing uses the send's own scope: same-project peers only
			// under the default, so an error in one project never leaks the
			// names/refs of sessions in other projects. The outbox error says
			// what happened; the appended listing adds information it does not
			// carry (the scoped live peers).
			return `${error.message}\nLive peers:\n${listLivePeers(registry, selfPid, scope, selfCwd)}`;
		case "ambiguous": {
			const candidates = error.candidates.length > 0 ? error.candidates : ["(no candidates)"];
			return `${error.message}\n${candidates.join("\n")}`;
		}
		case "offline":
		case "no_key":
		case "no_live_inbox":
		case "connect_failed":
		case "timeout":
		case "message_too_large":
		case "sender_paced":
		case "protocol_version":
			// Already actionable end to end (cause + remedy in the message).
			return error.message;
		case "connection_lost":
			// The outbox names the peer and the failure; add the retry
			// guidance it cannot know the user needs (wait for re-register,
			// then resend — the peer may have received nothing).
			return `${error.message} Retry once the peer re-registers (check with action: "list").`;
		case "invalid_target":
			// Cross-project refusals come from findCrossProjectTarget, whose
			// message names the other project. types.ts is frozen (no structured
			// hint field), so the marker is the message text itself.
			if (error.message.includes("belongs to another project")) {
				return `${error.message} Resend with scope: "all" (only if the user asked to reach that session).`;
			}
			return error.message;
	}
}

/** One structured per-target entry in a batch send's details. */
interface PeerBatchSendDetail {
	to: string;
	ok: boolean;
	msgId?: string;
	error?: string;
	code?: PeerSendErrorCode;
}

type SendAttempt = { ok: true; outcome: PeerSendOutcome } | { ok: false; error: unknown };

/**
 * One outbox send, including the project-scope gate: a cross-project target
 * under the default scope is refused before the outbox is called, matching the
 * single-target behavior.
 */
async function attemptSend(
	deps: PeerToolDeps,
	target: string,
	message: string,
	opts: PeerSendOptions,
	scope: "project" | "all",
	selfCwd: string,
): Promise<SendAttempt> {
	if (scope === "project") {
		const crossProject = findCrossProjectTarget(deps, target, selfCwd);
		if (crossProject !== undefined) {
			return { ok: false, error: crossProject };
		}
	}
	try {
		return { ok: true, outcome: await deps.outbox.send(target, message, opts) };
	} catch (error) {
		return { ok: false, error };
	}
}

/**
 * Resolve a shutdown target to its wire address with the same resolution and
 * project-scope rules as a send: "name", "name [ref]", or the raw address,
 * with cross-project targets refused under the default scope. The ladder is
 * the outbox's shared resolveLiveTarget (the outbox owns the injected
 * transport, which does the connect-path→wire-address conversion); this
 * function layers the project-scope gate and the outbox's forward-compat
 * protocol-version gate on top.
 */
function resolveShutdownTarget(
	deps: PeerToolDeps,
	target: string,
	scope: "project" | "all",
	selfCwd: string,
): PeerLiveTargetResolution {
	if (scope === "project") {
		const crossProject = findCrossProjectTarget(deps, target, selfCwd);
		if (crossProject !== undefined) {
			return { ok: false, error: crossProject };
		}
	}
	const resolution = deps.outbox.resolveLiveTarget(target);
	if (!resolution.ok) {
		return resolution;
	}
	// Same forward-compat gate as the outbox's send(): refuse to build frames
	// for a peer whose wire protocol is newer than ours.
	if (resolution.entry.record.peerProtocol > PEER_PROTOCOL_VERSION) {
		return {
			ok: false,
			error: new PeerSendError(
				"protocol_version",
				`peer "${resolution.entry.record.name}" speaks wire protocol v${resolution.entry.record.peerProtocol}, this plugin speaks v${PEER_PROTOCOL_VERSION} — upgrade the pi-peers plugin`,
			),
		};
	}
	return resolution;
}

/**
 * action: "shutdown" — resolve the target with the same resolution and
 * project-scope rules as a send, build a PeerShutdownFrame, and hand it to
 * outbox.sendControl (which takes the peer's wire address). Resolution
 * failures reuse the tool's PeerSendError text mapping.
 */
async function runShutdownAction(
	deps: PeerToolDeps,
	params: PeerMessageInput,
	scope: "project" | "all",
	ctx: PeersToolContext,
): Promise<PeersToolResult> {
	const target = params.to;
	if (typeof target !== "string" || target.trim() === "") {
		return {
			content: [
				{ type: "text", text: 'Missing "to". Call with action: "list" to discover live peers first.' },
			],
			details: {},
			isError: true,
		};
	}
	const resolved = resolveShutdownTarget(deps, target, scope, ctx.cwd);
	if (!resolved.ok) {
		return {
			content: [
				{ type: "text", text: describeSendError(resolved.error, deps.registry, deps.identity.pid, scope, ctx.cwd) },
			],
			details: {},
			isError: true,
		};
	}
	const reason =
		params.reason !== undefined && params.reason.trim() !== "" ? params.reason.trim() : undefined;
	const frame: PeerShutdownFrame = {
		msgV: PEER_PROTOCOL_VERSION,
		msg_id: randomUUID(),
		type: "control",
		action: "shutdown",
		from: deps.identity.socketAddress,
		...(deps.identity.name !== "" ? { fromName: deps.identity.name } : {}),
		...(reason !== undefined ? { reason } : {}),
	};
	try {
		await deps.outbox.sendControl(resolved.address, frame);
	} catch (error) {
		if (error instanceof PeerSendError) {
			return {
				content: [{ type: "text", text: describeSendError(error, deps.registry, deps.identity.pid, scope, ctx.cwd) }],
				details: {},
				isError: true,
			};
		}
		throw error;
	}
	return {
		content: [
			{
				type: "text",
				text: `Shutdown requested for "${resolved.entry.record.name}". (The peer obeys its own accept policy.)`,
			},
		],
		details: {},
	};
}

export function createPeerMessageTool(deps: PeerToolDeps): PeersToolDefinition<PeerMessageInput> {
	return {
		name: PEER_MESSAGE_TOOL_NAME,
		label: "Send Peer Message",
		description: PEER_MESSAGE_DESCRIPTION,
		parameters: peerMessageSchema,
		async execute(_toolCallId, params: PeerMessageInput, _signal, _onUpdate, ctx) {
			const scope = params.scope ?? "project";
			if (params.action === "list") {
				return {
					content: [{ type: "text", text: listLivePeers(deps.registry, deps.identity.pid, scope, ctx.cwd) }],
					details: {},
				};
			}
			if (params.action === "shutdown") {
				return runShutdownAction(deps, params, scope, ctx);
			}
			if (!params.to || (Array.isArray(params.to) && params.to.length === 0) || !params.message) {
				return {
					content: [
						{
							type: "text",
							text: 'Missing "to" and/or "message". Call with action: "list" to discover live peers first.',
						},
					],
					details: {},
					isError: true,
				};
			}
			// The idle subscription is per-target and one-shot; a batch would have
			// to invent semantics for it, so refuse the combination up front.
			if (Array.isArray(params.to) && params.notifyWhenIdle === true) {
				return {
					content: [
						{
							type: "text",
							text: 'Invalid target: "to" cannot be an array when notifyWhenIdle is true. Batch sends do not support idle subscriptions; send to a single peer to subscribe.',
						},
					],
					details: {},
					isError: true,
				};
			}
			const sendOpts: PeerSendOptions = {
				priority: params.priority,
				notifyWhenIdle: params.notifyWhenIdle,
			};
			if (!Array.isArray(params.to)) {
				const attempted = await attemptSend(deps, params.to, params.message, sendOpts, scope, ctx.cwd);
				if (attempted.ok) {
					const target = attempted.outcome.toName ? `"${attempted.outcome.toName}"` : "peer";
					const notifyNote = attempted.outcome.notifyIdleUnsupported === true
						? " NOTE: the peer's plugin version does not support idle notification — you will NOT be pinged; poll with list instead."
						: "";
					return {
						content: [
							{
								type: "text",
								text: `Sent to ${target} [${attempted.outcome.to}] (msg_id ${attempted.outcome.msgId}). Success is silent — failures and any reply arrive as messages in later turns; do not wait or poll for them.${notifyNote}`,
							},
						],
						details: {},
					};
				}
				if (attempted.error instanceof PeerSendError) {
					return {
						content: [
							{
								type: "text",
								text: describeSendError(attempted.error, deps.registry, deps.identity.pid, scope, ctx.cwd),
							},
						],
						details: {},
						isError: true,
					};
				}
				throw attempted.error;
			}
			// Batch: one send per target; a failing target never stops the rest.
			const details: PeerBatchSendDetail[] = [];
			const lines: string[] = [];
			let failures = 0;
			for (const target of params.to) {
				const attempted = await attemptSend(deps, target, params.message, sendOpts, scope, ctx.cwd);
				if (attempted.ok) {
					details.push({ to: target, ok: true, msgId: attempted.outcome.msgId });
					lines.push(`✓ ${target} — sent (msg_id ${attempted.outcome.msgId.slice(0, 8)}…)`);
					continue;
				}
				failures++;
				const failureText = attempted.error instanceof Error ? attempted.error.message : String(attempted.error);
				if (attempted.error instanceof PeerSendError) {
					details.push({ to: target, ok: false, error: failureText, code: attempted.error.code });
				} else {
					details.push({ to: target, ok: false, error: failureText });
				}
				lines.push(`✗ ${target}: ${failureText}`);
			}
			const okCount = params.to.length - failures;
			// Failures grouped by code in the header (e.g. "2 failed (1 offline,
			// 1 invalid_target)"); non-PeerSendError failures fall into "other".
			const failuresByCode = new Map<string, number>();
			for (const detail of details) {
				if (detail.ok) continue;
				const key = detail.code ?? "other";
				failuresByCode.set(key, (failuresByCode.get(key) ?? 0) + 1);
			}
			const failureGroups = [...failuresByCode.entries()].map(([code, count]) => `${count} ${code}`).join(", ");
			const header =
				failures === 0
					? `Sent to ${params.to.length} peers. Success is silent — failures and replies arrive as messages in later turns; do not wait or poll for them.`
					: okCount === 0
						? `Send failed for all ${params.to.length} targets.`
						: `Sent to ${okCount} of ${params.to.length} targets; ${failures} failed (${failureGroups}).`;
			const result: PeersToolResult = {
				content: [{ type: "text", text: `${header}\n${lines.join("\n")}` }],
				details,
			};
			if (failures === params.to.length) {
				result.isError = true;
			}
			// Partial failure is intentionally NOT isError: the agent must read
			// the per-target lines to see which sends failed.
			return result;
		},
	};
}

// ---------------------------------------------------------------------------
// spawn_peer tool
// ---------------------------------------------------------------------------

export const PEER_SPAWN_TOOL_NAME = "spawn_peer";

export const peerSpawnSchema = Type.Object({
	name: Type.Optional(
		Type.String({
			description:
				"Name for the new session (e.g. \"researcher\"). Auto-derived when omitted. " +
				"Address the peer by this name in later send_peer_message calls.",
		}),
	),
	task: Type.Optional(
		Type.String({
			description:
				"Task message delivered to the new peer as soon as it is online, as user input to its agent. " +
				"For large tasks, write them to a file and send the path instead.",
		}),
	),
	headless: Type.Optional(
		Type.Boolean({
			description: "Run in the background without a visible terminal window. Default false.",
		}),
	),
	ephemeral: Type.Optional(
		Type.Boolean({
			description:
				"The new session exits automatically once it has done its work and reported back. Default false. " +
				"Use with task for one-shot jobs; omit when follow-up questions are expected.",
		}),
	),
	model: Type.Optional(
		Type.String({
			description:
				'Model for the new session, passed as --model to the host CLI. Fuzzy match OK (e.g. "k3" or "aliyun/kimi-k3"). ' +
				"An invalid model fails fast with the host's own error instead of a registration timeout. " +
				"Default: the host's configured default model.",
		}),
	),
});

export type PeerSpawnInput = Static<typeof peerSpawnSchema>;

const PEER_SPAWN_DESCRIPTION = `# Spawn a new pi session on this machine

Launches a new pi instance that immediately registers as a peer. Use this to run work in parallel: the new session is a full agent with its own context window.

- The spawned session auto-registers in the peer list: address it by \`name\` in later \`send_peer_message\` calls (or its "name [ref]" when names collide).
- Its replies arrive as \`<cross-session-message from="ADDRESS" from-name="X" …>\` user messages in your context, like any peer's.
- Use \`task\` + \`ephemeral\` for one-shot jobs: the peer gets the task up front and exits by itself once it has done its work and reported back.
- Omit \`ephemeral\` when follow-up questions are expected — an ephemeral peer cannot be asked anything after it exits.
- \`model\` picks the new session's model via the host CLI's --model (fuzzy match OK, e.g. "k3" or "aliyun/kimi-k3"); default is the host's configured default model. An invalid model fails fast with the host's own error instead of a registration timeout.
- \`headless\` runs it in the background without a visible terminal window; leave it false when the user may want to watch or interact with the new session.
- The spawn waits for the new session to register (its record appears in the peer list). A failure here means the session did not start or did not register — nothing was lost, retry.
- If the result says "Task NOT delivered", the peer is online and addressable by name; send the task with send_peer_message.
- Do not use this to message an EXISTING peer — that is the \`send_peer_message\` tool. Spawn is only for creating new sessions.`;

export function createPeerSpawnTool(deps: PeerToolDeps): PeersToolDefinition<PeerSpawnInput> {
	return {
		name: PEER_SPAWN_TOOL_NAME,
		label: "Spawn Peer",
		description: PEER_SPAWN_DESCRIPTION,
		parameters: peerSpawnSchema,
		async execute(_toolCallId, params: PeerSpawnInput): Promise<PeersToolResult> {
			const options: PeerSpawnOptions = {
				name: params.name,
				task: params.task,
				headless: params.headless,
				ephemeral: params.ephemeral,
				model: params.model,
			};
			try {
				const outcome = await deps.spawner(options);
				if (!outcome.taskSent && params.task !== undefined) {
					// Startup race: the peer registered but the task could not be
					// delivered — the recovery is an ordinary send by name.
					return {
						content: [
							{
								type: "text",
								text: `Spawned peer "${outcome.name}" [${outcome.ref}] (pid ${outcome.pid}). The peer is online but the task was not delivered (startup race) — send it now with send_peer_message to "${outcome.name}".`,
							},
						],
						details: { ...outcome },
					};
				}
				const taskNote = outcome.taskSent
					? "Task delivered. Its reply arrives as a message in a later turn — do not wait or poll for it."
					: "No task given.";
				return {
					content: [
						{
							type: "text",
							text: `Spawned peer "${outcome.name}" [${outcome.ref}] (pid ${outcome.pid}). ${taskNote}`,
						},
					],
					details: { ...outcome },
				};
			} catch (error) {
				if (error instanceof PeerSpawnError) {
					return {
						content: [{ type: "text", text: `Spawn failed: ${error.message}` }],
						details: { kind: error.kind },
						isError: true,
					};
				}
				throw error;
			}
		},
	};
}

// ---------------------------------------------------------------------------
// Command
// ---------------------------------------------------------------------------

export interface PeersCommandDefinition {
	description: string;
	handler: (args: string, ctx: PeersCommandContext) => Promise<void>;
}

export function createPeersCommand(deps: PeerToolDeps): PeersCommandDefinition {
	return {
		description: "List live pi peer sessions on this machine",
		handler: async (_args, ctx) => {
			const live = deps.registry.list().filter((entry) => entry.live);
			const others = live.filter((entry) => entry.record.pid !== deps.identity.pid);
			if (others.length === 0) {
				// No other sessions anywhere (the command lists machine-wide), so
				// the unified empty wording applies without a cross-project count.
				ctx.ui.notify("No live peers in this project.", "info");
				return;
			}
			const lines = live.map((entry) =>
				peerLine(entry.record, entry.record.pid === deps.identity.pid, deps.registry.refOf),
			);
			ctx.ui.notify(`Live pi sessions:\n${lines.join("\n")}`, "info");
		},
	};
}
