/**
 * The `send_peer_message` tool and the `/peers` command.
 *
 * The tool is the model-facing entry into the outbox: it takes a peer name plus a body,
 * delegates to `PeerOutbox.send()`, and turns outcomes and `PeerSendError` codes into
 * actionable text. Listing and sending are scoped to this project by default
 * (same git repo root); `scope: "all"` widens them to the whole machine. The
 * command lists the live peers from the registry.
 *
 * See types.ts for the module contract and docs/peers.md for the user-facing design.
 */

import { type Static, Type } from "typebox";
import type {
	PeersCommandContext,
	PeersToolDefinition,
	PeersToolResult,
	PeerRecord,
	PeerRegistry,
	PeerSendOptions,
	PeerSendOutcome,
	PeerToolDeps,
} from "./types.ts";
import { PeerSendError, normalizeFsPath, projectRootOf, sameProject } from "./types.ts";
import type { PeerRegistryEntry, PeerSendErrorCode } from "./types.ts";

// ---------------------------------------------------------------------------
// Tool
// ---------------------------------------------------------------------------

export const PEER_MESSAGE_TOOL_NAME = "send_peer_message";

export const peerMessageSchema = Type.Object({
	action: Type.Optional(
		Type.Union([Type.Literal("send"), Type.Literal("list")], {
			description:
				'"send" (default): deliver a message to a peer. ' +
				'"list": show the live peer sessions on this machine (use this first when you do not know the peer\'s name).',
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
	scope: Type.Optional(
		Type.Union([Type.Literal("project"), Type.Literal("all")], {
			description:
				'"project" (default): only sessions in this same project (same git repo root) are listed and reachable. ' +
				'"all": widen to every live session on this machine, including other projects. ' +
				'Cross-project sends require "all".',
		}),
	),
});

export type PeerMessageInput = Static<typeof peerMessageSchema>;

const PEER_MESSAGE_DESCRIPTION = `# Send a message to another pi session on this machine

Sends a message to another running pi instance ("peer") on the same machine. The message is processed as user input by the peer's agent.

- Project scope by default: \`action: "list"\` and sends only see sessions in this same project (sessions whose git repo root matches this session's). Sessions in other projects are hidden. To list or message sessions in any project on this machine, pass \`scope: "all"\`.
- To discover peers, call with \`action: "list"\` — you get each live session's name, status, kind, working directory, and disambiguation ref. You do not need the user to tell you peer names.
- When you RECEIVE a \`<cross-session-message from="ADDRESS" from-name="X" …>\` in your context, that is a message from peer X. If it asks for a reply or reports results back, you MUST answer by calling this tool — \`to\` accepts the message's \`from\` address verbatim, or the sender's name. Writing the answer only in your local chat never reaches the sender — they cannot see your screen.
- A send failing with "belongs to another project" is a scoping block, not a missing peer: resend with \`scope: "all"\` to message across projects.
- If several sessions share one name, the send fails with a candidate list; resend with "name [ref]" to disambiguate, e.g. "alice [ab12cd]".
- \`priority\`: "next" (default) queues the message until the peer's current work finishes; "now" interrupts the peer immediately. Use "now" sparingly.
- \`notifyWhenIdle\`: also subscribes (once) to be notified when the peer next becomes idle.
- Success only means the message reached the peer's queue. A receipt (delivered / held / denied / expired / refused / dropped) follows asynchronously and is injected into your context when it arrives.
- Silence is not consent: no reply within a timeout does not mean the peer agreed or acted.
- \`to\` accepts an array of peer names (up to 32) to batch-send the same message to all of them: one send per target, a failing target does not stop the rest, and the result reports one line per target. Batch sends cannot use \`notifyWhenIdle\`.
- For large content, write it to a file and send the file path in the message; each message is capped at 1 MiB on the wire.`;

function peerLine(record: PeerRecord, ref: string, self: boolean): string {
	const suffix = self ? "  (this session)" : "";
	return `${record.name} [${ref}]  ${record.status}  ${record.kind}  ${record.cwd}  (${record.sessionId.slice(0, 8)})${suffix}`;
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
		return hidden > 0
			? `No live peers in this project.\n(+${hidden} sessions in other projects hidden; pass scope: "all" to include)`
			: "No live peers.";
	}
	const lines = entries.map((entry) =>
		peerLine(entry.record, registry.refOf(entry.record), entry.record.pid === selfPid),
	);
	if (hidden > 0) {
		lines.push(`(+${hidden} sessions in other projects hidden; pass scope: "all" to include)`);
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

function describeSendError(error: PeerSendError, registry: PeerRegistry, selfPid: number): string {
	switch (error.code) {
		case "not_found":
			return `No live peer matches "${error.message}". Run /peers to see live sessions.\nLive peers:\n${listLivePeers(registry, selfPid, "all", "")}`;
		case "ambiguous": {
			const candidates = error.candidates.length > 0 ? error.candidates : ["(no candidates)"];
			return `Several live peers match. Resend with "name [ref]" to disambiguate:\n${candidates.join("\n")}`;
		}
		case "offline":
			return `Peer is offline (process dead or unreachable): ${error.message}. The peer may have exited; run /peers to refresh the list.`;
		case "no_key":
			return `No auth key for the peer (key file missing): ${error.message}. The peer may have restarted since it registered; ask its user to restart or check ~/.pi/agent/peers/.`;
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
								text: describeSendError(attempted.error, deps.registry, deps.identity.pid),
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
			const header =
				failures === 0
					? `Sent to ${params.to.length} peers. Receipts will follow asynchronously.`
					: okCount === 0
						? `Send failed for all ${params.to.length} targets.`
						: `Sent to ${okCount} of ${params.to.length} targets; ${failures} failed.`;
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
				ctx.ui.notify("No other live pi sessions on this machine.", "info");
				return;
			}
			const lines = live.map((entry) =>
				peerLine(entry.record, deps.registry.refOf(entry.record), entry.record.pid === deps.identity.pid),
			);
			ctx.ui.notify(`Live pi sessions:\n${lines.join("\n")}`, "info");
		},
	};
}
