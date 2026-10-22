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
	PEER_MAX_QUEUED_MESSAGES,
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

	/**
	 * Number of peer messages accepted since the last idle transition. Not a
	 * real queue: injected messages are not waiting for the agent — this is a
	 * documented cap so a busy stretch cannot hide rate/dedup signals from the
	 * guard (guard sees an ever-growing count, and the reset-on-idle bounds it).
	 */
	let acceptedSinceIdle = 0;
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
	 * Route one inbound user frame through wrapper validation → guard → accept
	 * policy → injection. Receipts are sent via the outbox (best-effort by
	 * contract) and failures there never mask the outcome of the message itself.
	 */
	async function handleUserFrame(frame: PeerUserFrame, senderClass: "peer" | "child"): Promise<void> {
		// The wrapper is mandatory on the receive side: parse it strictly
		// (round-trip fidelity rejects tampered/forged wrappers), then re-wrap
		// with the wire identity from the frame so the agent never sees content
		// that was not reconstructed under our control.
		const parsed = protocol.parsePeerContent(frame.content);
		if (parsed === undefined) {
			await sendReceipt(frame, "dropped", "malformed-wrapper");
			return;
		}
		const content = protocol.wrapPeerContent(
			{
				from: frame.from,
				fromSession: frame.fromSession,
				...(frame.fromName !== undefined && frame.fromName !== ""
					? { fromName: protocol.sanitizePeerText(frame.fromName) }
					: {}),
			},
			parsed.body,
		);

		if (senderClass === "child") {
			await inject(frame, content, senderClass);
			return;
		}

		const verdict = guard.check(frame.from, frame, identity.hopToken, acceptedSinceIdle);
		if (!verdict.ok) {
			const reason =
				verdict.reason === "queue-full"
					? `queue-full: peer has accepted ${PEER_MAX_QUEUED_MESSAGES} messages since its last idle; retry after it goes idle`
					: verdict.reason;
			await sendReceipt(frame, "dropped", reason);
			return;
		}

		const policy: PeerAcceptPolicy = host.acceptPolicy();
		if (policy === "off") {
			await sendReceipt(frame, "refused", "accept policy is off");
			return;
		}
		if (policy === "all") {
			await inject(frame, content, senderClass);
			return;
		}

		// policy === "ask"
		if (!host.hasUI()) {
			await sendReceipt(frame, "refused", "accept policy is ask but no dialog-capable UI is available");
			return;
		}

		await sendReceipt(frame, "held", undefined);
		const preview =
			parsed.body.length > PREVIEW_MAX_CHARS ? `${parsed.body.slice(0, PREVIEW_MAX_CHARS)}…` : parsed.body;
		const answer = await host.confirm(
			`Peer message from ${protocol.sanitizePeerText(frame.fromName ?? frame.from)}`,
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
		await inject(frame, content, senderClass);
	}

	/** Step e: record acceptance, count, and inject into the agent. */
	async function inject(
		frame: PeerUserFrame,
		content: string,
		senderClass: "peer" | "child",
	): Promise<void> {
		if (senderClass !== "child") {
			guard.recordAccepted(frame.from, frame);
		}
		acceptedSinceIdle++;
		try {
			await host.injectUserMessage(content, frame.priority);
		} catch (error) {
			acceptedSinceIdle--;
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
				if (host.isIdleNow()) {
					// Already idle: fire immediately instead of parking a subscription
					// that would only flush on a future busy→idle transition.
					sendIdleNotice({
						replyTo: frame.reply_to,
						from: frame.from,
						fromName: frame.fromName,
						nonce: frame.nonce,
					});
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
			// One-shot nonce: unsolicited, reused, or missing nonces are dropped
			// silently — only notices answering our subscription get through.
			if (!deps.outbox.consumeIdleNonce(frame.nonce)) {
				return;
			}
			const peerName = protocol.sanitizePeerText(frame.fromName ?? frame.from);
			await host.injectAgentNotice(`Peer "${peerName}" is now idle.`);
			return;
		}
		case "shutdown": {
			// Peer-requested shutdown of this session. The host's quit path
			// triggers session_shutdown (unpublish + server teardown); the
			// inbox never tears down transport state itself.
			const policy = host.acceptPolicy();
			if (policy === "off") {
				return;
			}
			if (policy === "ask") {
				const fromName = protocol.sanitizePeerText(frame.fromName ?? frame.from);
				const sender =
					frame.fromName === undefined ? fromName : `"${fromName}" (${protocol.sanitizePeerText(frame.from)})`;
				const reason =
					frame.reason === undefined ? "" : `\nReason: ${protocol.sanitizePeerText(frame.reason)}`;
				const answer = await host.confirm(
					"Peer shutdown request",
					`Peer ${sender} asks to shut down this session.${reason}`,
					PEER_APPROVAL_TIMEOUT_MS,
				);
				if (answer !== "yes") {
					return;
				}
			}
			host.notify(
				"peers",
				`Shutting down at request of peer ${protocol.sanitizePeerText(frame.fromName ?? frame.from)}`,
			);
			host.quit();
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

	/**
	 * Build and fire one idle notice for a subscription. Shared by the
	 * busy→idle flush (onIdle) and the already-idle immediate path so both
	 * produce identical frames; sendControl is best-effort by contract.
	 */
	function sendIdleNotice(sub: IdleSub): void {
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

	/** Fire pending subscriptions when the local agent transitions to idle. */
	function onIdle(): void {
		const subs = Array.from(idleSubs.values());
		idleSubs.clear();
		acceptedSinceIdle = 0;
		for (const sub of subs) {
			sendIdleNotice(sub);
		}
	}

	function pendingCount(): number {
		return acceptedSinceIdle;
	}

	return { handleFrame, onIdle, pendingCount };
}
