/**
 * The `send_peer_message` tool and the `/peers` command.
 *
 * The tool is the model-facing entry into the outbox: it takes a peer name plus a body,
 * delegates to `PeerOutbox.send()`, and turns outcomes and `PeerSendError` codes into
 * actionable text. The command lists the live peers from the registry.
 *
 * See types.ts for the module contract and docs/peers.md for the user-facing design.
 */

import { type Static, Type } from "typebox";
import type {
	PeersCommandContext,
	PeersToolDefinition,
	PeerRecord,
	PeerRegistry,
	PeerToolDeps,
} from "./types.ts";
import { PeerSendError } from "./types.ts";

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
		Type.String({
			description:
				'Peer name to send to, as shown by action="list" (e.g. "alice"). ' +
				'When several sessions share a name, append the disambiguation ref from the candidate list: "alice [ab12cd]". ' +
				"Required for send; ignored for list.",
		}),
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
});

export type PeerMessageInput = Static<typeof peerMessageSchema>;

const PEER_MESSAGE_DESCRIPTION = `# Send a message to another pi session on this machine

Sends a message to another running pi instance ("peer") on the same machine. The message is processed as user input by the peer's agent.

- To discover peers, call with \`action: "list"\` — you get each live session's name, status, kind, working directory, and disambiguation ref. You do not need the user to tell you peer names.
- When you RECEIVE a \`<cross-session-message from="ADDRESS" from-name="X" …>\` in your context, that is a message from peer X. If it asks for a reply or reports results back, you MUST answer by calling this tool — \`to\` accepts the message's \`from\` address verbatim, or the sender's name. Writing the answer only in your local chat never reaches the sender — they cannot see your screen.
- If several sessions share one name, the send fails with a candidate list; resend with "name [ref]" to disambiguate, e.g. "alice [ab12cd]".
- \`priority\`: "next" (default) queues the message until the peer's current work finishes; "now" interrupts the peer immediately. Use "now" sparingly.
- \`notifyWhenIdle\`: also subscribes (once) to be notified when the peer next becomes idle.
- Success only means the message reached the peer's queue. A receipt (delivered / held / denied / expired / refused / dropped) follows asynchronously and is injected into your context when it arrives.
- Silence is not consent: no reply within a timeout does not mean the peer agreed or acted.
- For large content, write it to a file and send the file path in the message; each message is capped at 1 MiB on the wire.`;

function peerLine(record: PeerRecord, ref: string, self: boolean): string {
	const suffix = self ? "  (this session)" : "";
	return `${record.name} [${ref}]  ${record.status}  ${record.kind}  ${record.cwd}  (${record.sessionId.slice(0, 8)})${suffix}`;
}

function listLivePeers(registry: PeerRegistry, selfPid: number): string {
	const entries = registry.list().filter((entry) => entry.live);
	if (entries.length === 0) return "No live peers.";
	return entries
		.map((entry) => peerLine(entry.record, registry.refOf(entry.record), entry.record.pid === selfPid))
		.join("\n");
}

function describeSendError(error: PeerSendError, registry: PeerRegistry, selfPid: number): string {
	switch (error.code) {
		case "not_found":
			return `No live peer matches "${error.message}". Run /peers to see live sessions.\nLive peers:\n${listLivePeers(registry, selfPid)}`;
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

export function createPeerMessageTool(deps: PeerToolDeps): PeersToolDefinition<PeerMessageInput> {
	return {
		name: PEER_MESSAGE_TOOL_NAME,
		label: "Send Peer Message",
		description: PEER_MESSAGE_DESCRIPTION,
		parameters: peerMessageSchema,
		async execute(_toolCallId, params: PeerMessageInput) {
			if (params.action === "list") {
				return {
					content: [{ type: "text", text: listLivePeers(deps.registry, deps.identity.pid) }],
					details: {},
				};
			}
			if (!params.to || !params.message) {
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
			try {
				const outcome = await deps.outbox.send(params.to, params.message, {
					priority: params.priority,
					notifyWhenIdle: params.notifyWhenIdle,
				});
				const target = outcome.toName ? `"${outcome.toName}"` : "peer";
				return {
					content: [
						{
							type: "text",
							text: `Sent to ${target} [${outcome.to}] (msg_id ${outcome.msgId}). Receipt will follow asynchronously.`,
						},
					],
					details: {},
				};
			} catch (error) {
				if (error instanceof PeerSendError) {
					return {
						content: [{ type: "text", text: describeSendError(error, deps.registry, deps.identity.pid) }],
						details: {},
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
