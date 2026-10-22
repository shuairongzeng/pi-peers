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
 * See types.ts for the module contract and docs/peers.md for the user-facing design.
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
import { PEER_PROTOCOL_VERSION, PeerSendError, PeerSpawnError, normalizeFsPath, projectRootOf, sameProject } from "./types.ts";
import type { PeerRegistryEntry, PeerSendErrorCode, PeersToolContext } from "./types.ts";
import { addressFromPath } from "./transport.ts";

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
- Success only means the message reached the peer's queue. A receipt (delivered / held / denied / expired / refused / dropped) follows asynchronously and is injected into your context when it arrives.
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
	const sameProject = sameProjectEntries(registry, live, selfPid, selfCwd);
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
 * Membership test for live entries sharing this session's project. The
 * reference is our own registry record when we have one (pid match); without
 * one (unregistered caller) the tool context's cwd stands in for our project
 * root, normalized on both sides so path-shape differences do not split
 * projects.
 */
function sameProjectEntries(
	registry: PeerRegistry,
	live: PeerRegistryEntry[],
	selfPid: number,
	selfCwd: string,
): (entry: PeerRegistryEntry) => boolean {
	const selfEntry = live.find((entry) => entry.record.pid === selfPid);
	if (selfEntry === undefined) {
		// No record of our own: the local machine's pidDomain decides path
		// case-folding for both sides of the comparison.
		const windows = registry.pidDomain().startsWith("win32:");
		const own = normalizeFsPath(selfCwd, windows);
		return (entry) => normalizeFsPath(projectRootOf(entry.record), windows) === own;
	}
	const selfRecord = selfEntry.record;
	return (entry) => sameProject(selfRecord, entry.record);
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
	const sameProject = sameProjectEntries(deps.registry, live, deps.identity.pid, selfCwd);
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
	switch (error.code) {
		case "not_found":
			// The listing uses the send's own scope: same-project peers only
			// under the default, so an error in one project never leaks the
			// names/refs of sessions in other projects.
			return `No live peer matches "${error.message}". Run /peers to see live sessions.\nLive peers:\n${listLivePeers(registry, selfPid, scope, selfCwd)}`;
		case "ambiguous": {
			const candidates = error.candidates.length > 0 ? error.candidates : ["(no candidates)"];
			return `Several live peers match. Resend with "name [ref]" to disambiguate:\n${candidates.join("\n")}`;
		}
		case "offline":
			return `Peer is offline (process dead or unreachable): ${error.message}. The peer may have exited; run /peers to refresh the list.`;
		case "no_key":
			return `No auth key for the peer (key file missing): ${error.message}. The peer may have restarted since it registered; retry, or ask its user to restart the session.`;
		case "no_live_inbox":
			return `No live inbox for the target (registry/key missing and endpoint unverifiable): ${error.message}. Run /peers to refresh the list.`;
		case "connect_failed":
			return `Could not connect to the peer's socket: ${error.message}. The peer may have just exited; run /peers and retry.`;
		case "timeout":
			return `Timed out sending to the peer: ${error.message}. The peer may be busy or gone; retry or check with /peers.`;
		case "message_too_large":
			return `Message exceeds the 1 MiB wire cap: ${error.message}. Write the content to a file and send the file path instead.`;
		case "sender_paced":
			return `Local pacing predicts the peer would drop this send (too many recent messages): ${error.message}. Wait a moment, merge messages, and retry.`;
		case "invalid_target":
			// Cross-project refusals come from findCrossProjectTarget, whose
			// message names the other project. types.ts is frozen (no structured
			// hint field), so the marker is the message text itself.
			if (error.message.includes("belongs to another project")) {
				return `Invalid target: ${error.message}. Resend with scope: "all" (only if the user asked to reach that session).`;
			}
			return `Invalid target: ${error.message}. Use a peer name from /peers, or "name [ref]" to disambiguate.`;
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
 * with cross-project targets refused under the default scope. Registry
 * records store the connect path; sendControl() takes the wire address
 * form, so the conversion goes through transport.addressFromPath — the
 * same helper the outbox itself uses for sends.
 */
function resolveShutdownTarget(
	deps: PeerToolDeps,
	target: string,
	scope: "project" | "all",
	selfCwd: string,
): { address: string; entry: PeerRegistryEntry } | PeerSendError {
	if (scope === "project") {
		const crossProject = findCrossProjectTarget(deps, target, selfCwd);
		if (crossProject !== undefined) {
			return crossProject;
		}
	}
	const resolution = deps.registry.resolve(target);
	if (resolution.kind === "none") {
		return new PeerSendError(
			"invalid_target",
			`no live peer matches "${target}": the peer may not be running or may have exited. ` +
				'Call with action: "list" to see live peers first',
		);
	}
	if (resolution.kind === "ambiguous") {
		return new PeerSendError(
			"ambiguous",
			`several live peers match "${target}"; resend with "name [ref]" to disambiguate`,
			resolution.candidates.map((candidate) =>
				`${candidate.record.name} [${deps.registry.refOf(candidate.record)}] ${candidate.record.status} ${candidate.record.cwd}`,
			),
		);
	}
	const { entry } = resolution;
	if (!entry.live) {
		return new PeerSendError(
			"offline",
			`peer "${entry.record.name}" (pid ${entry.record.pid}) has a registry record but its process is dead`,
		);
	}
	const address = addressFromPath(entry.record.socketPath);
	if (address === undefined) {
		return new PeerSendError("invalid_target", `unrecognized peer socket path: ${entry.record.socketPath}`);
	}
	return { address, entry };
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
	if (resolved instanceof PeerSendError) {
		return {
			content: [{ type: "text", text: describeSendError(resolved, deps.registry, deps.identity.pid, scope, ctx.cwd) }],
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
					return {
						content: [
							{
								type: "text",
								text: `Sent to ${target} [${attempted.outcome.to}] (msg_id ${attempted.outcome.msgId}). Receipt will follow asynchronously.`,
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
					? `Sent to ${params.to.length} peers. Receipts will follow asynchronously.`
					: okCount === 0
						? `Send failed for all ${params.to.length} targets.`
						: `Sent to ${okCount} of ${params.to.length} targets; ${failures} failed (${failureGroups}).`;
			const result: PeersToolResult = {
				content: [{ type: "text", text: `${header}\n${lines.join("\n")}` }],
				details,
			};
			if (failures === params.to.length) {
				result.isError = true;
			} else if (failures > 0) {
				result.isError = false;
			}
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
});

export type PeerSpawnInput = Static<typeof peerSpawnSchema>;

const PEER_SPAWN_DESCRIPTION = `# Spawn a new pi session on this machine

Launches a new pi instance that immediately registers as a peer. Use this to run work in parallel: the new session is a full agent with its own context window.

- The spawned session auto-registers in the peer list: address it by \`name\` in later \`send_peer_message\` calls (or its "name [ref]" when names collide).
- Its replies arrive as \`<cross-session-message from="ADDRESS" from-name="X" …>\` user messages in your context, like any peer's.
- Use \`task\` + \`ephemeral\` for one-shot jobs: the peer gets the task up front and exits by itself once it has done its work and reported back.
- Omit \`ephemeral\` when follow-up questions are expected — an ephemeral peer cannot be asked anything after it exits.
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
				const taskNote = outcome.taskSent ? "Task delivered." : "No task given.";
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
