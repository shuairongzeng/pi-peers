/**
 * outbox.ts — sending side of cross-session peer messaging.
 *
 * Resolves a peer name to a live registry record, looks up the peer's auth key,
 * opens one short-lived connection, and writes the auth line followed by the
 * frame (plus an optional notify_when_idle subscription). Also tracks
 * asynchronous receipts reported by peers via peer_message_status frames.
 *
 * See types.ts for the module contract and docs/peers.md for the user-facing design.
 */

import { randomUUID } from "node:crypto";
import {
	PEER_RECEIPT_TTL_MS,
	type PeerConnection,
	type PeerControlFrame,
	type PeerMessageStatusFrame,
	type PeerOutbox,
	type PeerReceiptState,
	type PeerRecord,
	type PeerRegistryEntry,
	PeerSendError,
	type PeerSendOptions,
	type PeerSendOutcome,
	type PeerServiceDeps,
	type PeerUserFrame,
} from "./types.ts";

// ---------------------------------------------------------------------------
// Receipt tracking
// ---------------------------------------------------------------------------

interface ReceiptEntry {
	state: PeerReceiptState;
	/** ms epoch when the state was last observed. */
	at: number;
}

/** One candidate line in an "ambiguous" error: `name [ref] status cwd`. */
function candidateLine(refOf: (record: PeerRecord) => string, entry: PeerRegistryEntry): string {
	const { record } = entry;
	return `${record.name} [${refOf(record)}] ${record.status} ${record.cwd}`;
}

// ---------------------------------------------------------------------------
// Outbox
// ---------------------------------------------------------------------------

export function createPeerOutbox(deps: PeerServiceDeps & { peersDir: string }): PeerOutbox {
	const { identity, protocol, transport, auth, registry, pacer, host } = deps;

	/** msg_id -> latest receipt state and observation time; pruned lazily by TTL. */
	const receipts = new Map<string, ReceiptEntry>();

	function pruneReceipts(): void {
		const cutoff = Date.now() - PEER_RECEIPT_TTL_MS;
		for (const [msgId, entry] of receipts) {
			if (entry.at < cutoff) {
				receipts.delete(msgId);
			}
		}
	}

	/**
	 * Open a connection, write the auth line plus each given serialized frame,
	 * then end the outbound side. Applies the sender-paced pacer around the
	 * attempt: any connect/write/end failure refunds the consumed token.
	 */
	async function deliver(target: string, socketPath: string, token: string, lines: string[]): Promise<void> {
		// Registry records store the connect path; transport.connect takes the
		// wire address form.
		const address = transport.addressFromPath(socketPath);
		if (address === undefined) {
			throw new PeerSendError("invalid_target", `unrecognized peer socket path: ${socketPath}`);
		}
		let conn: PeerConnection | undefined;
		try {
			conn = await transport.connect(address);
			conn.writeLine(protocol.buildAuthLine(token));
			for (const line of lines) {
				conn.writeLine(line);
			}
			await conn.end();
		} catch (error) {
			pacer.refund(target);
			conn?.destroy();
			if (error instanceof PeerSendError) {
				throw error;
			}
			throw new PeerSendError("connect_failed", `failed delivering peer message to ${socketPath}: ${String(error)}`);
		}
	}

	async function send(target: string, body: string, opts: PeerSendOptions): Promise<PeerSendOutcome> {
		if (!pacer.tryConsume(target)) {
			throw new PeerSendError(
				"sender_paced",
				`local pacing predicts peer "${target}" would drop this send; ` +
					`wait a few seconds or merge pending messages into one, then retry`,
			);
		}

		const resolution = registry.resolve(target);
		if (resolution.kind === "none") {
			const live = registry
				.list()
				.filter((entry) => entry.live)
				.map((entry) => `${entry.record.name} [${registry.refOf(entry.record)}]`);
			const peersList = live.length > 0 ? live.join("\n") : "(no live peers)";
			throw new PeerSendError(
				"not_found",
				`no live peer matches "${target}": the peer may not be running or may have exited. ` +
					`Socket addresses change on every start, so never reuse a stale address. Live peers:\n${peersList}`,
			);
		}
		if (resolution.kind === "ambiguous") {
			throw new PeerSendError(
				"ambiguous",
				`several live peers match "${target}"; resend with "name [ref]" to disambiguate`,
				resolution.candidates.map((candidate) => candidateLine(registry.refOf, candidate)),
			);
		}

		const entry = resolution.entry;
		const record = entry.record;
		if (!entry.live) {
			throw new PeerSendError(
				"offline",
				`peer "${record.name}" (pid ${record.pid}) has a registry record but its process is dead; ` +
					`it may have exited without cleaning up. Run the peers listing again after a moment`,
			);
		}

		const key = auth.lookupKey(deps.peersDir, record.socketPath);
		if (key === undefined) {
			throw new PeerSendError(
				"no_key",
				`no auth key for peer "${record.name}" at ${record.socketPath}: the peer has not published a key ` +
					`(it may be an old version, or it crashed right after starting); wait for it to re-register, then retry`,
			);
		}

		const envelope = protocol.newEnvelope();
		const frame: PeerUserFrame = {
			...envelope,
			type: "user",
			from: identity.socketAddress,
			fromPid: identity.pid,
			fromSession: identity.sessionId,
			...(identity.name !== undefined && identity.name !== "" ? { fromName: identity.name } : {}),
			hopChain: [identity.hopToken],
			priority: opts.priority ?? "next",
			content: protocol.wrapPeerContent(
				{
					from: identity.socketAddress,
					fromSession: identity.sessionId,
					...(identity.name !== undefined && identity.name !== "" ? { fromName: identity.name } : {}),
				},
				body,
			),
		};

		const lines: string[] = [];
		if (opts.notifyWhenIdle === true) {
			// The subscription precedes the message: it must cover the work this
			// message triggers, and the peer processes lines in order.
			const notifyFrame: PeerControlFrame = {
				...protocol.newEnvelope(),
				type: "control",
				action: "notify_when_idle",
				reply_to: identity.socketAddress,
				from: identity.socketAddress,
				...(identity.name !== undefined && identity.name !== "" ? { fromName: identity.name } : {}),
				nonce: randomUUID(),
			};
			lines.push(protocol.serializeFrame(notifyFrame));
		}
		lines.push(protocol.serializeFrame(frame));

		await deliver(target, record.socketPath, tokenForPath(record.socketPath) ?? key.peerToken, lines);

		return { msgId: frame.msg_id, to: record.socketPath, toName: record.name };
	}

	const parentPath = identity.parent ? transport.resolveAddress(identity.parent.socketAddress) : undefined;

	/** Token the peer at `socketPath` expects from us: the parent's child token when
	 *  talking to our own parent (so it classifies us as "child"), otherwise the
	 *  peerToken from that socket's published key file. */
	function tokenForPath(socketPath: string): string | undefined {
		if (parentPath !== undefined && socketPath === parentPath) {
			return identity.parent?.token;
		}
		return auth.lookupKey(deps.peersDir, socketPath)?.peerToken;
	}

	async function sendControl(address: string, frame: PeerControlFrame): Promise<void> {
		const socketPath = transport.resolveAddress(address);
		if (socketPath === undefined) {
			throw new PeerSendError("invalid_target", `invalid peer socket address: ${address}`);
		}
		try {
			const token = tokenForPath(socketPath);
			if (token === undefined) {
				// Best-effort: the peer's key file is gone (it exited); the receipt
				// simply never arrives. Never treat silence as agreement anyway.
				console.error(`[peers] sendControl to ${address}: no auth key, dropping`);
				return;
			}
			await deliver(address, socketPath, token, [protocol.serializeFrame(frame)]);
		} catch (error) {
			if (error instanceof PeerSendError && error.code === "invalid_target") {
				throw error;
			}
			// Receipts and idle notices are best-effort: the peer may be gone.
			console.error(`[peers] sendControl to ${address} failed: ${String(error)}`);
		}
	}

	function handleReceipt(frame: PeerMessageStatusFrame): void {
		pruneReceipts();
		receipts.set(frame.msg_id, { state: frame.state, at: Date.now() });
		const sender = frame.fromName ?? frame.from;
		const suffix = frame.reason !== undefined ? `: ${frame.reason}` : "";
		switch (frame.state) {
			case "denied":
			case "expired":
			case "dropped":
			case "refused": {
				const text = `Peer message to ${sender} (${frame.msg_id}) was ${frame.state}${suffix}.`;
				void host.injectAgentNotice(text).catch(() => {});
				break;
			}
			case "held":
			case "delivered": {
				host.notify("Peer message", `Message to ${sender} was ${frame.state}${suffix}.`);
				break;
			}
		}
	}

	function receiptOf(msgId: string): PeerReceiptState | undefined {
		pruneReceipts();
		return receipts.get(msgId)?.state;
	}

	return { send, sendControl, handleReceipt, receiptOf };
}
