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
	type PeerRecord,
	type PeerServiceDeps,
	type PeerShutdownFrame,
	type PeerShutdownResponseFrame,
	type PeerUserFrame,
	sameProject,
} from "./types.ts";

// ---------------------------------------------------------------------------
// Idle subscriptions
// ---------------------------------------------------------------------------

interface IdleSub {
	replyTo: string;
	from: string;
	fromName: string | undefined;
	nonce: string | undefined;
	/** ms epoch when the subscription was parked (TTL hygiene). */
	atMs: number;
}

/** How long a parked notify_when_idle subscription survives before eviction. */
const IDLE_SUB_TTL_MS = 30 * 60_000;
/** Max parked subscriptions; the oldest is evicted beyond this (leak cap). */
const IDLE_SUB_MAX = 64;

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
	 * Parked-subscription hygiene: drop entries older than IDLE_SUB_TTL_MS and
	 * cap the map at IDLE_SUB_MAX (evicting the oldest by atMs). A re-sent
	 * msg_id re-sets its atMs in place, so insertion order is not strictly age
	 * order — the map is bounded (~IDLE_SUB_MAX entries), a full scan is cheap.
	 */
	function evictIdleSubs(): void {
		const now = Date.now();
		for (const [key, sub] of idleSubs) {
			if (now - sub.atMs >= IDLE_SUB_TTL_MS) {
				idleSubs.delete(key);
			}
		}
		while (idleSubs.size > IDLE_SUB_MAX) {
			let oldestKey: string | undefined;
			let oldestAt = Number.POSITIVE_INFINITY;
			for (const [key, sub] of idleSubs) {
				if (sub.atMs < oldestAt) {
					oldestAt = sub.atMs;
					oldestKey = key;
				}
			}
			if (oldestKey === undefined) break;
			idleSubs.delete(oldestKey);
		}
	}

	/**
	 * Convert a wire address (`pipe:LOCAL\X` / `sock:<path>`) to the canonical
	 * connect-path form stored in PeerRecord.socketPath, so registry entries can
	 * be matched against frame senders and our own identity.
	 */
	function addressToSocketPath(address: string): string | undefined {
		if (address.startsWith("pipe:")) {
			return `\\\\.\\pipe\\${address.slice("pipe:".length)}`;
		}
		if (address.startsWith("sock:")) {
			return address.slice("sock:".length);
		}
		return undefined;
	}

	/**
	 * Peer-record lookup by wire address: entries whose record.socketPath
	 * converts back to the given address. Live-only — a dead sender's record is
	 * not a trustworthy project scope.
	 */
	function recordForAddress(address: string): PeerRecord | undefined {
		const socketPath = addressToSocketPath(address);
		if (socketPath === undefined) return undefined;
		const found = deps.registry.list().find((e) => e.live && e.record.socketPath === socketPath);
		return found?.record;
	}

	/**
	 * Receiver-side project scope for policy "all" shutdowns: a cross-project
	 * sender must not silently kill us. Same-project, unknown sender (registry
	 * race: benefit of the doubt under "all"), or a missing own record are all
	 * treated as same-project.
	 */
	function isCrossProjectShutdown(frame: PeerShutdownFrame): boolean {
		const sender = recordForAddress(frame.from);
		if (sender === undefined) return false;
		const own = recordForAddress(identity.socketAddress);
		if (own === undefined) return false;
		return !sameProject(sender, own);
	}

	/**
	 * Send the shutdown_response answer back to the sender. Best-effort like
	 * every receipt: a failure must not block (or crash) shutdown handling.
	 */
	async function sendShutdownResponse(
		frame: PeerShutdownFrame,
		outcome: "accepted" | "denied",
		reason: string | undefined,
	): Promise<void> {
		const responseFrame: PeerShutdownResponseFrame = {
			...protocol.newEnvelope(),
			type: "control",
			action: "shutdown_response",
			inReplyTo: frame.msg_id,
			from: identity.socketAddress,
			...(identity.name !== "" ? { fromName: identity.name } : {}),
			outcome,
			...(reason !== undefined ? { reason } : {}),
		};
		try {
			await deps.outbox.sendControl(frame.from, responseFrame);
		} catch {
			// The real outbox already logs transport failures; the outcome
			// below (notify + quit) must proceed regardless.
		}
	}

	/**
	 * Decide the outcome of a peer shutdown request per the accept policy.
	 * Returns "accepted" plus a reason for the response frame; the caller sends
	 * the response and performs notify + quit only on acceptance.
	 */
	async function decideShutdown(
		frame: PeerShutdownFrame,
	): Promise<{ outcome: "accepted" | "denied"; reason?: string }> {
		const policy = host.acceptPolicy();
		if (policy === "off") {
			return { outcome: "denied", reason: "accept policy off" };
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
			if (answer === "no") {
				return { outcome: "denied", reason: "user declined" };
			}
			if (answer === "timeout") {
				return { outcome: "denied", reason: "confirmation timed out" };
			}
			return { outcome: "accepted" };
		}
		// policy === "all": still refuse cross-project senders unless a human
		// confirms (headless sessions refuse outright).
		if (isCrossProjectShutdown(frame)) {
			if (!host.hasUI()) {
				return { outcome: "denied", reason: "cross-project refused" };
			}
			const answer = await host.confirm(
				"Peer shutdown request",
				`Peer ${protocol.sanitizePeerText(frame.fromName ?? frame.from)} from another project asks to shut down this session.`,
				PEER_APPROVAL_TIMEOUT_MS,
			);
			if (answer === "no") {
				return { outcome: "denied", reason: "cross-project refused" };
			}
			if (answer === "timeout") {
				return { outcome: "denied", reason: "confirmation timed out" };
			}
			return { outcome: "accepted" };
		}
		return { outcome: "accepted" };
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
			// Child frames skip the accept pipeline but not the child rate
			// limit: a flooding child is dropped silently (no receipt — a
			// child has no listener to receive one) rather than injected.
			const verdict = guard.checkChild(frame.from);
			if (!verdict.ok) {
				return;
			}
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
						nonce: frame.nonce,
					});
					return;
				}
			idleSubs.set(frame.msg_id, {
				replyTo: frame.reply_to,
				from: frame.from,
				fromName: frame.fromName,
				nonce: frame.nonce,
				atMs: Date.now(),
			});
			// Parked-subscription hygiene: a busy stretch must not accumulate
			// an unbounded Map of dead peers' subscriptions.
			evictIdleSubs();
			return;
		}
		case "peer_idle_notice": {
			// One-shot nonce: unsolicited, reused, or missing nonces are dropped
			// silently — only notices answering our subscription get through.
			if (!deps.outbox.consumeIdleNonce(frame.nonce)) {
				return;
			}
			const peerName = protocol.sanitizePeerText(frame.fromName ?? recordForAddress(frame.from)?.name ?? frame.from);
			await host.injectAgentNotice(`Peer "${peerName}" is now idle.`);
			return;
		}
		case "shutdown": {
			// Peer-requested shutdown of this session. Every branch answers the
			// sender with a shutdown_response frame first; the host's quit path
			// (only on "accepted") triggers session_shutdown (unpublish + server
			// teardown); the inbox never tears down transport state itself.
			const outcome = await decideShutdown(frame);
			await sendShutdownResponse(frame, outcome.outcome, outcome.reason);
			if (outcome.outcome === "accepted") {
				host.notify(
					"peers",
					`Shutting down at request of peer ${protocol.sanitizePeerText(frame.fromName ?? frame.from)}`,
				);
				host.quit();
			}
			return;
		}
		case "shutdown_response": {
			// Answer to a shutdown we requested: the requesting agent learns the
			// outcome (agent notice) and the user gets a toast.
			const peerName = protocol.sanitizePeerText(frame.fromName ?? frame.from);
			const text = `Peer "${peerName}" ${frame.outcome === "accepted" ? "accepted" : "denied"} the shutdown request${frame.reason === undefined ? "" : ` (${protocol.sanitizePeerText(frame.reason)})`}.`;
			host.notify("peers", text);
			await host.injectAgentNotice(text);
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
	function sendIdleNotice(sub: Pick<IdleSub, "replyTo" | "nonce">): void {
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
