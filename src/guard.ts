/**
 * Peer guard: inbound anti-abuse for peer messages, plus the outbound sender pacer.
 *
 * check() is a pure gate — it reads sender state but never mutates it: no token is consumed and
 * no dedup fingerprint is recorded until recordAccepted() runs. refund() hands one token back
 * when a message that consumed one ultimately fails to deliver.
 *
 * Per-sender state (token bucket + dedup fingerprints) is LRU-capped at
 * PEER_MAX_TRACKED_SENDERS: when a new sender arrives over the cap, the least recently used
 * sender is evicted with all its state, so a returning sender starts fresh.
 */

import { createHash } from "node:crypto";
import type { PeerGuard, PeerGuardCheck, PeerSenderPacer, PeerUserFrame } from "./types.ts";
import {
	PEER_DEDUP_WINDOW_MS,
	PEER_MAX_HOP_CHAIN,
	PEER_MAX_QUEUED_MESSAGES,
	PEER_MAX_SELF_HOPS,
	PEER_MAX_TRACKED_SENDERS,
	PEER_RATE_CAPACITY,
	PEER_RATE_REFILL_PER_SEC,
} from "./types.ts";

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

/** Injectable knobs; tests pass a fake clock and shrunken values. */
export interface PeerGuardOptions {
	/** Milliseconds since the epoch. Defaults to Date.now. */
	now?: () => number;
	/** Bucket capacity. Defaults to PEER_RATE_CAPACITY. */
	rateCapacity?: number;
	/** Tokens added per second, prorated per elapsed millisecond. Defaults to PEER_RATE_REFILL_PER_SEC. */
	rateRefillPerSec?: number;
	/** How long a content fingerprint suppresses re-delivery. Defaults to PEER_DEDUP_WINDOW_MS. */
	dedupWindowMs?: number;
	/** Max occurrences of our own hop token in a hopChain. Defaults to PEER_MAX_SELF_HOPS. */
	maxSelfHops?: number;
	/** Max hopChain length. Defaults to PEER_MAX_HOP_CHAIN. */
	maxHopChain?: number;
	/** queuedCount at which inbound frames are refused. Defaults to PEER_MAX_QUEUED_MESSAGES. */
	maxQueued?: number;
	/** Max tracked senders (guard) / targets (pacer) before LRU eviction. Defaults to PEER_MAX_TRACKED_SENDERS. */
	maxTrackedSenders?: number;
}

// ---------------------------------------------------------------------------
// Token buckets
// ---------------------------------------------------------------------------

interface TokenBucket {
	tokens: number;
	lastRefillMs: number;
}

/**
 * Tokens available at `atMs`: the stored amount plus `rateRefillPerSec/1000` per elapsed
 * millisecond, capped at capacity. Pure — callers decide whether to persist the refill.
 */
function tokensAt(bucket: TokenBucket, atMs: number, rateCapacity: number, rateRefillPerSec: number): number {
	const elapsed = Math.max(0, atMs - bucket.lastRefillMs);
	return Math.min(rateCapacity, bucket.tokens + (elapsed * rateRefillPerSec) / 1000);
}

// ---------------------------------------------------------------------------
// Inbound guard
// ---------------------------------------------------------------------------

interface SenderState {
	bucket: TokenBucket;
	/** content fingerprint -> accept timestamp */
	seen: Map<string, number>;
}

/** sha256 over `${fromSession}\n${content}` — the dedup key for a user frame. */
function peerContentFingerprint(frame: PeerUserFrame): string {
	return createHash("sha256").update(`${frame.fromSession}\n${frame.content}`).digest("hex");
}

export function createPeerGuard(opts: PeerGuardOptions = {}): PeerGuard {
	const now = opts.now ?? Date.now;
	const rateCapacity = opts.rateCapacity ?? PEER_RATE_CAPACITY;
	const rateRefillPerSec = opts.rateRefillPerSec ?? PEER_RATE_REFILL_PER_SEC;
	const dedupWindowMs = opts.dedupWindowMs ?? PEER_DEDUP_WINDOW_MS;
	const maxSelfHops = opts.maxSelfHops ?? PEER_MAX_SELF_HOPS;
	const maxHopChain = opts.maxHopChain ?? PEER_MAX_HOP_CHAIN;
	const maxQueued = opts.maxQueued ?? PEER_MAX_QUEUED_MESSAGES;
	const maxTrackedSenders = opts.maxTrackedSenders ?? PEER_MAX_TRACKED_SENDERS;

	/** Insertion-ordered; first key = least recently used, re-inserting a key refreshes recency. */
	const senders = new Map<string, SenderState>();

	function check(fromKey: string, frame: PeerUserFrame, ownHopToken: string, queuedCount: number): PeerGuardCheck {
		if (queuedCount >= maxQueued) {
			return { ok: false, reason: "queue-full" };
		}
		if (frame.hopChain.length > maxHopChain) {
			return { ok: false, reason: "hop-runaway" };
		}
		let selfHops = 0;
		for (const hop of frame.hopChain) {
			if (hop === ownHopToken) selfHops++;
		}
		if (selfHops >= maxSelfHops) {
			return { ok: false, reason: "hop-loop" };
		}
		// Pure from here on: state is read, never written.
		const atMs = now();
		const state = senders.get(fromKey);
		if (state !== undefined) {
			const seenAtMs = state.seen.get(peerContentFingerprint(frame));
			if (seenAtMs !== undefined && atMs - seenAtMs < dedupWindowMs) {
				return { ok: false, reason: "duplicate" };
			}
			if (tokensAt(state.bucket, atMs, rateCapacity, rateRefillPerSec) < 1) {
				return { ok: false, reason: "rate-limited" };
			}
		} else if (rateCapacity < 1) {
			// A fresh sender starts with a full bucket; capacity under one can never admit a message.
			return { ok: false, reason: "rate-limited" };
		}
		return { ok: true };
	}

	function recordAccepted(fromKey: string, frame: PeerUserFrame): void {
		const atMs = now();
		const existing = senders.get(fromKey);
		const state: SenderState =
			existing === undefined ? { bucket: { tokens: rateCapacity, lastRefillMs: atMs }, seen: new Map() } : existing;
		// Re-insertion refreshes recency; a new sender may evict the least recently used one.
		senders.delete(fromKey);
		senders.set(fromKey, state);
		while (senders.size > maxTrackedSenders) {
			const evicted = senders.keys().next().value;
			if (evicted === undefined) break;
			senders.delete(evicted);
		}
		const fingerprint = peerContentFingerprint(frame);
		// Same window the check uses: a fingerprint at or beyond the age of dedupWindowMs is stale.
		const cutoff = atMs - dedupWindowMs;
		for (const [seenFingerprint, seenAtMs] of state.seen) {
			if (seenAtMs <= cutoff) state.seen.delete(seenFingerprint);
		}
		state.bucket.tokens = Math.max(0, tokensAt(state.bucket, atMs, rateCapacity, rateRefillPerSec) - 1);
		state.bucket.lastRefillMs = atMs;
		state.seen.set(fingerprint, atMs);
	}

	function refund(fromKey: string): void {
		const state = senders.get(fromKey);
		if (state === undefined) return;
		const atMs = now();
		state.bucket.tokens = Math.min(rateCapacity, tokensAt(state.bucket, atMs, rateCapacity, rateRefillPerSec) + 1);
		state.bucket.lastRefillMs = atMs;
	}

	return { check, recordAccepted, refund };
}

// ---------------------------------------------------------------------------
// Outbound sender pacer
// ---------------------------------------------------------------------------

export function createPeerSenderPacer(opts: PeerGuardOptions = {}): PeerSenderPacer {
	const now = opts.now ?? Date.now;
	const rateCapacity = opts.rateCapacity ?? PEER_RATE_CAPACITY;
	const rateRefillPerSec = opts.rateRefillPerSec ?? PEER_RATE_REFILL_PER_SEC;
	const maxTrackedTargets = opts.maxTrackedSenders ?? PEER_MAX_TRACKED_SENDERS;

	/** Insertion-ordered; first key = least recently used, re-inserting a key refreshes recency. */
	const targets = new Map<string, TokenBucket>();

	function tryConsume(target: string): boolean {
		const atMs = now();
		const existing = targets.get(target);
		const bucket: TokenBucket = existing === undefined ? { tokens: rateCapacity, lastRefillMs: atMs } : existing;
		targets.delete(target);
		targets.set(target, bucket);
		while (targets.size > maxTrackedTargets) {
			const evicted = targets.keys().next().value;
			if (evicted === undefined) break;
			targets.delete(evicted);
		}
		bucket.tokens = tokensAt(bucket, atMs, rateCapacity, rateRefillPerSec);
		bucket.lastRefillMs = atMs;
		if (bucket.tokens < 1) return false;
		bucket.tokens -= 1;
		return true;
	}

	function refund(target: string): void {
		const bucket = targets.get(target);
		if (bucket === undefined) return;
		const atMs = now();
		bucket.tokens = Math.min(rateCapacity, tokensAt(bucket, atMs, rateCapacity, rateRefillPerSec) + 1);
		bucket.lastRefillMs = atMs;
	}

	return { tryConsume, refund };
}
