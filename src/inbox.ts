/**
 * inbox.ts — receiving side of cross-session peer messaging.
 *
 * Classifies each authenticated inbound frame: user frames run through the
 * guard and accept policy (with a user approval step when configured) before
 * being injected into the local agent; control frames update receipt state,
 * register one-shot notify_when_idle subscriptions, or surface idle notices.
 *
 * See types.ts for the module contract and docs/peers.md for the user-facing design.
 */

import {
	PEER_APPROVAL_TIMEOUT_MS,
	type PeerAcceptPolicy,
	type PeerControlFrame,
	type PeerFrame,
	type PeerIdleNoticeFrame,
	type PeerInbox,
	type PeerMessageStatusFrame,
	type PeerOutbox,
	type PeerReceiptState,
	type PeerServiceDeps,
	type PeerUserFrame,
} from "./types.ts";

// ---------------------------------------------------------------------------
// Idle subscriptions
// ---------------------------------------------------------------------------

interface IdleSub {
	replyTo: string;
	from: string;
	fromName: string | undefined;
	nonce: string | undefined;
}

/** Max chars of the body preview shown in approval prompts. */
const PREVIEW_MAX_CHARS = 500;

// ---------------------------------------------------------------------------
// Inbox
// ---------------------------------------------------------------------------

export function createPeerInbox(deps: PeerServiceDeps & { outbox: PeerOutbox }): PeerInbox {
	const { identity, protocol, transport, guard, host } = deps;

	/** Number of accepted peer messages queued in the agent. */
	let queued = 0;
	/** One-shot notify_when_idle subscriptions, keyed by their envelope msg_id. */
	const idleSubs = new Map<string, IdleSub>();

	/**
	 * Report a receipt for a user frame. The status frame carries our own
	 * identity, a fresh envelope, and msg_id of the frame being acknowledged.
	 */
	async function sendReceipt(
		original: PeerUserFrame,
		state: PeerReceiptState,
		reason: string | undefined,
	): Promise<void> {
		const statusFrame: PeerMessageStatusFrame = {
			...protocol.newEnvelope(),
			type: "control",
			action: "peer_message_status",
			msg_id: original.msg_id,
			from: identity.socketAddress,
			fromName: identity.name,
			state,
			...(reason !== undefined ? { reason } : {}),
		};
		// Receipts are best-effort: a sendControl failure (peer gone, address
		// unreachable) must never fail the inbound message handling itself.
		try {
			await deps.outbox.sendControl(original.from, statusFrame);
		} catch {
			// The real outbox already logs transport failures; only invalid
			// addresses (PeerSendError) escape, which we also swallow here.
		}
	}

	/**
	 * Route one inbound user frame through guard → accept policy → injection.
	 * Receipts are sent via the outbox (best-effort by contract) and failures
	 * there never mask the outcome of the message itself.
	 */
	async function handleUserFrame(frame: PeerUserFrame, senderClass: "peer" | "child"): Promise<void> {
		if (senderClass === "child") {
			await inject(frame, senderClass);
			return;
		}

		const verdict = guard.check(frame.from, frame, identity.hopToken, queued);
		if (!verdict.ok) {
			await sendReceipt(frame, "dropped", verdict.reason);
			return;
		}

		const policy: PeerAcceptPolicy = host.acceptPolicy();
		if (policy === "off") {
			await sendReceipt(frame, "refused", "accept policy is off");
			return;
		}
		if (policy === "all") {
			await inject(frame, senderClass);
			return;
		}

		// policy === "ask"
		if (!host.hasUI()) {
			await sendReceipt(frame, "refused", "accept policy is ask but no dialog-capable UI is available");
			return;
		}

		await sendReceipt(frame, "held", undefined);
		const preview =
			frame.content.length > PREVIEW_MAX_CHARS ? `${frame.content.slice(0, PREVIEW_MAX_CHARS)}…` : frame.content;
		const answer = await host.confirm(
			`Peer message from ${frame.fromName ?? frame.from}`,
			preview,
			PEER_APPROVAL_TIMEOUT_MS,
		);
		if (answer === "no") {
			await sendReceipt(frame, "denied", "rejected by user");
			return;
		}
		if (answer === "timeout") {
			await sendReceipt(frame, "expired", "approval timed out");
			return;
		}
		await inject(frame, senderClass);
	}

	/** Step e: record acceptance, queue, and inject into the agent. */
	async function inject(frame: PeerUserFrame, senderClass: "peer" | "child"): Promise<void> {
		if (senderClass !== "child") {
			guard.recordAccepted(frame.from, frame);
		}
		queued++;
		try {
			await host.injectUserMessage(frame.content, frame.priority);
		} catch (error) {
			queued--;
			if (senderClass !== "child") {
				guard.refund(frame.from);
			}
			await sendReceipt(frame, "dropped", `inject-failed: ${String(error)}`);
			return;
		}
		await sendReceipt(frame, "delivered", undefined);
	}

	async function handleControlFrame(frame: PeerControlFrame): Promise<void> {
		switch (frame.action) {
			case "peer_message_status":
				await deps.outbox.handleReceipt(frame);
				return;
			case "notify_when_idle": {
				const valid = transport.isValidAddress(frame.reply_to) && frame.reply_to !== identity.socketAddress;
				if (!valid) {
					// Invalid or self-addressed subscriptions are silently ignored:
					// the sender either misbehaved or would loop notices back to itself.
					return;
				}
				idleSubs.set(frame.msg_id, {
					replyTo: frame.reply_to,
					from: frame.from,
					fromName: frame.fromName,
					nonce: frame.nonce,
				});
				return;
			}
			case "peer_idle_notice": {
				await host.injectAgentNotice(`Peer "${frame.fromName ?? frame.from}" is now idle.`);
				return;
			}
		}
	}

	async function handleFrame(frame: PeerFrame, senderClass: "peer" | "child"): Promise<void> {
		if (frame.type === "control") {
			await handleControlFrame(frame);
			return;
		}
		await handleUserFrame(frame, senderClass);
	}

	function onIdle(): void {
		const subs = Array.from(idleSubs.values());
		idleSubs.clear();
		queued = 0;
		for (const sub of subs) {
			const notice: PeerIdleNoticeFrame = {
				...protocol.newEnvelope(),
				type: "control",
				action: "peer_idle_notice",
				from: identity.socketAddress,
				fromName: identity.name,
				...(sub.nonce !== undefined ? { nonce: sub.nonce } : {}),
			};
			void deps.outbox.sendControl(sub.replyTo, notice);
		}
	}

	function pendingCount(): number {
		return queued;
	}

	return { handleFrame, onIdle, pendingCount };
}
