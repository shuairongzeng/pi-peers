/**
 * Peer guard: inbound anti-abuse for peer messages, plus the outbound sender pacer.
 *
 * check() is a pure gate — it reads sender state but never mutates it: no token is consumed and
 * no dedup fingerprint is recorded until recordAccepted() runs. refund() hands one token back
 * when a message that consumed one ultimately fails to deliver.
 *
 * A global token bucket caps the aggregate inbound flow: every accepted user frame consumes one
 * token from it regardless of sender, so rotating the attacker-controlled `from` string can't
 * hand each message a fresh per-sender bucket. Dedup stays per-sender.
 *
 * checkChild() gates CHILD-class frames (subagents authenticated with the child token): rate
 * limiting only — no dedupe, hop, or queue policing. Unlike check() it consumes its tokens on
 * success, because child frames bypass the accept pipeline and no recordAccepted() runs for
 * them. Child traffic draws from a generous dedicated bucket and from the global aggregate
 * bucket, so a misbehaving child can flood neither the parent nor the aggregate cap.
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

/** Aggregate inbound cap: tokens shared by ALL senders' accepted user frames. */
export const PEER_GLOBAL_RATE_CAPACITY = 120;
/** Aggregate inbound refill (tokens/sec), prorated per elapsed millisecond. */
export const PEER_GLOBAL_RATE_REFILL_PER_SEC = 2;

/** Child-class inbound cap: tokens shared by ALL child frames (checkChild). */
export const PEER_CHILD_RATE_CAPACITY = 120;
/** Child-class refill (tokens/sec), prorated per elapsed millisecond. */
export const PEER_CHILD_RATE_REFILL_PER_SEC = 10;

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

/** Injectable knobs; tests pass a fake clock and shrunken values. */
export interface PeerGuardOptions {
	/** Milliseconds since the epoch. Defaults to Date.now. */
	now?: () => number;
	/** Per-sender bucket capacity. Defaults to PEER_RATE_CAPACITY. */
	rateCapacity?: number;
	/** Tokens added per second, prorated per elapsed millisecond. Defaults to PEER_RATE_REFILL_PER_SEC. */
	rateRefillPerSec?: number;
	/** Aggregate bucket capacity across all senders. Defaults to PEER_GLOBAL_RATE_CAPACITY. */
	globalRateCapacity?: number;
	/** Aggregate refill (tokens/sec) shared by all senders. Defaults to PEER_GLOBAL_RATE_REFILL_PER_SEC. */
	globalRateRefillPerSec?: number;
	/** Child-class bucket capacity shared by all child frames (checkChild). Defaults to PEER_CHILD_RATE_CAPACITY. */
	childRateCapacity?: number;
	/** Child-class refill (tokens/sec), prorated per elapsed millisecond. Defaults to PEER_CHILD_RATE_REFILL_PER_SEC. */
	childRateRefillPerSec?: number;
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
	const globalRateCapacity = opts.globalRateCapacity ?? PEER_GLOBAL_RATE_CAPACITY;
	const globalRateRefillPerSec = opts.globalRateRefillPerSec ?? PEER_GLOBAL_RATE_REFILL_PER_SEC;
	const childRateCapacity = opts.childRateCapacity ?? PEER_CHILD_RATE_CAPACITY;
	const childRateRefillPerSec = opts.childRateRefillPerSec ?? PEER_CHILD_RATE_REFILL_PER_SEC;

	/** Insertion-ordered; first key = least recently used, re-inserting a key refreshes recency. */
	const senders = new Map<string, SenderState>();

	/** Aggregate bucket consumed by every accepted user frame, regardless of sender. */
	const globalBucket: TokenBucket = { tokens: globalRateCapacity, lastRefillMs: now() };

	/** Bucket shared by every child-class frame; children skip dedupe/hops/queue but not this. */
	const childBucket: TokenBucket = { tokens: childRateCapacity, lastRefillMs: now() };

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
		if (tokensAt(globalBucket, atMs, globalRateCapacity, globalRateRefillPerSec) < 1) {
			// Aggregate cap: rotating `from` cannot hand each message a fresh bucket.
			return { ok: false, reason: "rate-limited" };
		}
		return { ok: true };
	}

	/**
	 * Check-and-consume gate for child-class frames: they skip the accept pipeline, so no
	 * recordAccepted() will ever run for them and the token is charged here, on success only.
	 * The bucket is class-wide; `fromKey` exists for interface parity and future per-child
	 * scoping.
	 */
	function checkChild(fromKey: string): PeerGuardCheck {
		const atMs = now();
		const childTokens = tokensAt(childBucket, atMs, childRateCapacity, childRateRefillPerSec);
		if (childTokens < 1) {
			return { ok: false, reason: "rate-limited" };
		}
		// The aggregate cap binds children too: they cannot bypass the global bucket.
		const globalTokens = tokensAt(globalBucket, atMs, globalRateCapacity, globalRateRefillPerSec);
		if (globalTokens < 1) {
			return { ok: false, reason: "rate-limited" };
		}
		childBucket.tokens = Math.max(0, childTokens - 1);
		childBucket.lastRefillMs = atMs;
		globalBucket.tokens = Math.max(0, globalTokens - 1);
		globalBucket.lastRefillMs = atMs;
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
		globalBucket.tokens = Math.max(0, tokensAt(globalBucket, atMs, globalRateCapacity, globalRateRefillPerSec) - 1);
		globalBucket.lastRefillMs = atMs;
		state.seen.set(fingerprint, atMs);
	}

	function refund(fromKey: string): void {
		const state = senders.get(fromKey);
		if (state === undefined) return;
		const atMs = now();
		state.bucket.tokens = Math.min(rateCapacity, tokensAt(state.bucket, atMs, rateCapacity, rateRefillPerSec) + 1);
		state.bucket.lastRefillMs = atMs;
		globalBucket.tokens = Math.min(
			globalRateCapacity,
			tokensAt(globalBucket, atMs, globalRateCapacity, globalRateRefillPerSec) + 1,
		);
		globalBucket.lastRefillMs = atMs;
	}

	return { check, recordAccepted, refund, checkChild };
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
