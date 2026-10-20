import { beforeEach, describe, expect, it } from "vitest";
import { createPeerGuard, createPeerSenderPacer } from "../src/guard.ts";
import type { PeerUserFrame } from "../src/types.ts";

/** Mutable fake clock so tests advance time deterministically. */
let clock = 0;
const now = () => clock;

function makeFrame(overrides: Partial<PeerUserFrame> = {}): PeerUserFrame {
	return {
		msgV: 1,
		msg_id: `msg-${Math.random().toString(36).slice(2)}`,
		type: "user",
		from: "pipe:LOCAL\\pi-peer-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		fromPid: 1234,
		fromSession: "session-1",
		hopChain: [],
		priority: "next",
		content: "hello",
		...overrides,
	};
}

/** Advance the fake clock by enough to refill one token at the given rate. */
const MS_PER_TOKEN = (refillPerSec: number) => Math.ceil(1000 / refillPerSec);

beforeEach(() => {
	clock = 1000;
});

describe("createPeerGuard", () => {
	describe("token bucket", () => {
		it("allows the first 30 messages, rate-limits the 31st (default capacity)", () => {
			const guard = createPeerGuard();
			for (let i = 0; i < 30; i++) {
				expect(guard.check("s1", makeFrame({ content: `m${i}` }), "self", 0)).toEqual({ ok: true });
				guard.recordAccepted("s1", makeFrame({ content: `m${i}` }));
			}
			expect(guard.check("s1", makeFrame({ content: "m30" }), "self", 0)).toEqual({
				ok: false,
				reason: "rate-limited",
			});
		});

		it("recovers after enough time elapses for one refill", () => {
			const guard = createPeerGuard({ now });
			for (let i = 0; i < 30; i++) {
				guard.recordAccepted("s1", makeFrame({ content: `m${i}` }));
			}
			expect(guard.check("s1", makeFrame({ content: "next" }), "self", 0)).toEqual({
				ok: false,
				reason: "rate-limited",
			});
			// Default refill: 0.5 tokens/sec → 1 token needs 2000ms.
			clock += MS_PER_TOKEN(0.5);
			expect(guard.check("s1", makeFrame({ content: "next" }), "self", 0)).toEqual({ ok: true });
		});

		it("refills to capacity, never beyond", () => {
			const refillPerSec = 10;
			const guard = createPeerGuard({ now, rateRefillPerSec: refillPerSec });
			for (let i = 0; i < 30; i++) {
				guard.recordAccepted("s1", makeFrame({ content: `m${i}` }));
			}
			// Wait far longer than needed for a full refill.
			clock += 60_000;
			for (let i = 0; i < 30; i++) {
				expect(guard.check("s1", makeFrame({ content: `r${i}` }), "self", 0)).toEqual({ ok: true });
				guard.recordAccepted("s1", makeFrame({ content: `r${i}` }));
			}
			expect(guard.check("s1", makeFrame({ content: "overflow" }), "self", 0)).toEqual({
				ok: false,
				reason: "rate-limited",
			});
		});

		it("keeps per-sender buckets independent", () => {
			const guard = createPeerGuard({ now });
			for (let i = 0; i < 30; i++) {
				guard.recordAccepted("s1", makeFrame({ content: `m${i}` }));
			}
			expect(guard.check("s1", makeFrame({ content: "x" }), "self", 0).reason).toBe("rate-limited");
			expect(guard.check("s2", makeFrame({ content: "x" }), "self", 0)).toEqual({ ok: true });
		});
	});

	describe("dedup window", () => {
		it("rejects a same-content resend within the window", () => {
			const guard = createPeerGuard({ now });
			const frame = makeFrame({ content: "same" });
			expect(guard.check("s1", frame, "self", 0)).toEqual({ ok: true });
			guard.recordAccepted("s1", frame);
			expect(guard.check("s1", makeFrame({ content: "same" }), "self", 0)).toEqual({
				ok: false,
				reason: "duplicate",
			});
		});

		it("accepts the same content after the window expires", () => {
			const guard = createPeerGuard({ now, dedupWindowMs: 1000 });
			guard.recordAccepted("s1", makeFrame({ content: "same" }));
			expect(guard.check("s1", makeFrame({ content: "same" }), "self", 0).reason).toBe("duplicate");
			clock += 1000;
			expect(guard.check("s1", makeFrame({ content: "same" }), "self", 0)).toEqual({ ok: true });
		});

		it("scopes fingerprints to the sender session + content, not sender address alone", () => {
			const guard = createPeerGuard({ now });
			guard.recordAccepted("s1", makeFrame({ content: "same" }));
			// Different content from the same sender is not a duplicate.
			expect(guard.check("s1", makeFrame({ content: "other" }), "self", 0)).toEqual({ ok: true });
			// Same session/content from a different sender key is a separate dedup scope.
			expect(guard.check("s2", makeFrame({ content: "same" }), "self", 0)).toEqual({ ok: true });
		});
	});

	describe("hop rules", () => {
		it("drops frames whose hopChain exceeds maxHopChain", () => {
			const guard = createPeerGuard({ now, maxHopChain: 3 });
			expect(guard.check("s1", makeFrame({ hopChain: ["a", "b", "c", "d"] }), "self", 0)).toEqual({
				ok: false,
				reason: "hop-runaway",
			});
			expect(guard.check("s1", makeFrame({ hopChain: ["a", "b", "c"] }), "self", 0)).toEqual({ ok: true });
		});

		it("drops frames whose hopChain reaches the maxSelfHops threshold of our own token", () => {
			const guard = createPeerGuard({ now, maxSelfHops: 3 });
			expect(guard.check("s1", makeFrame({ hopChain: ["self", "self", "self"] }), "self", 0)).toEqual({
				ok: false,
				reason: "hop-loop",
			});
			expect(guard.check("s1", makeFrame({ hopChain: ["self", "self", "other"] }), "self", 0)).toEqual({
				ok: true,
			});
		});
	});

	describe("queue bound", () => {
		it("refuses frames once queuedCount reaches maxQueued", () => {
			const guard = createPeerGuard({ now, maxQueued: 5 });
			expect(guard.check("s1", makeFrame(), "self", 4)).toEqual({ ok: true });
			expect(guard.check("s1", makeFrame(), "self", 5)).toEqual({ ok: false, reason: "queue-full" });
			expect(guard.check("s1", makeFrame(), "self", 6)).toEqual({ ok: false, reason: "queue-full" });
		});
	});

	describe("check() purity", () => {
		it("check() alone never consumes a token or records a fingerprint", () => {
			const guard = createPeerGuard({ now, dedupWindowMs: 60_000, rateCapacity: 2 });
			const frame = makeFrame({ content: "ping" });
			for (let i = 0; i < 5; i++) {
				expect(guard.check("s1", frame, "self", 0)).toEqual({ ok: true });
			}
			// Still fresh: no dedup entry, full bucket.
			guard.recordAccepted("s1", frame);
			expect(guard.check("s1", frame, "self", 0).reason).toBe("duplicate");
		});
	});

	describe("recordAccepted / refund accounting", () => {
		it("consumes exactly one token per recordAccepted", () => {
			const guard = createPeerGuard({ now, rateCapacity: 3 });
			guard.recordAccepted("s1", makeFrame({ content: "a" }));
			guard.recordAccepted("s1", makeFrame({ content: "b" }));
			expect(guard.check("s1", makeFrame({ content: "c" }), "self", 0)).toEqual({ ok: true });
			guard.recordAccepted("s1", makeFrame({ content: "c" }));
			expect(guard.check("s1", makeFrame({ content: "d" }), "self", 0).reason).toBe("rate-limited");
		});

		it("refund() returns one token, capped at capacity", () => {
			const guard = createPeerGuard({ now, rateCapacity: 2 });
			guard.recordAccepted("s1", makeFrame({ content: "a" }));
			guard.recordAccepted("s1", makeFrame({ content: "b" }));
			expect(guard.check("s1", makeFrame({ content: "c" }), "self", 0).reason).toBe("rate-limited");
			guard.refund("s1");
			expect(guard.check("s1", makeFrame({ content: "c" }), "self", 0)).toEqual({ ok: true });
			// Refunding a full bucket must not exceed capacity.
			guard.refund("s1");
			guard.refund("s1");
			guard.recordAccepted("s1", makeFrame({ content: "c" }));
			guard.recordAccepted("s1", makeFrame({ content: "d" }));
			expect(guard.check("s1", makeFrame({ content: "e" }), "self", 0).reason).toBe("rate-limited");
		});

		it("refund() for an unknown sender is a no-op", () => {
			const guard = createPeerGuard({ now });
			expect(() => guard.refund("nobody")).not.toThrow();
			expect(guard.check("fresh", makeFrame(), "self", 0)).toEqual({ ok: true });
		});

		it("recordAccepted purges out-of-window fingerprints for that sender", () => {
			const guard = createPeerGuard({ now, dedupWindowMs: 1000 });
			guard.recordAccepted("s1", makeFrame({ content: "old" }));
			clock += 2000;
			// The purge happens inside recordAccepted; after it, "old" no longer blocks.
			guard.recordAccepted("s1", makeFrame({ content: "new" }));
			expect(guard.check("s1", makeFrame({ content: "old" }), "self", 0)).toEqual({ ok: true });
		});
	});

	describe("LRU eviction", () => {
		it("evicts the least recently used sender, letting it start fresh on return", () => {
			const guard = createPeerGuard({ now, maxTrackedSenders: 2, rateCapacity: 2 });
			guard.recordAccepted("a", makeFrame({ content: "1" }));
			guard.recordAccepted("a", makeFrame({ content: "2" }));
			guard.recordAccepted("b", makeFrame({ content: "1" }));
			// recordAccepted("b") refreshed b's recency, making "a" the LRU entry.
			guard.recordAccepted("c", makeFrame({ content: "1" }));
			// "a" was evicted → full bucket and empty dedup set on return; "b" survives with 1 token.
			expect(guard.check("a", makeFrame({ content: "x" }), "self", 0)).toEqual({ ok: true });
			expect(guard.check("b", makeFrame({ content: "1" }), "self", 0).reason).toBe("duplicate");
			expect(guard.check("b", makeFrame({ content: "y" }), "self", 0)).toEqual({ ok: true });
			guard.recordAccepted("b", makeFrame({ content: "y" }));
			expect(guard.check("b", makeFrame({ content: "z" }), "self", 0).reason).toBe("rate-limited");
		});
	});
});

describe("createPeerSenderPacer", () => {
	it("exhausts and returns false, then recovers via refund()", () => {
		const pacer = createPeerSenderPacer({ now, rateCapacity: 2 });
		expect(pacer.tryConsume("t1")).toBe(true);
		expect(pacer.tryConsume("t1")).toBe(true);
		expect(pacer.tryConsume("t1")).toBe(false);
		pacer.refund("t1");
		expect(pacer.tryConsume("t1")).toBe(true);
	});

	it("refunds up to capacity and no further", () => {
		const pacer = createPeerSenderPacer({ now, rateCapacity: 2 });
		expect(pacer.tryConsume("t1")).toBe(true);
		expect(pacer.tryConsume("t1")).toBe(true);
		pacer.refund("t1");
		pacer.refund("t1");
		pacer.refund("t1");
		expect(pacer.tryConsume("t1")).toBe(true);
		expect(pacer.tryConsume("t1")).toBe(true);
		expect(pacer.tryConsume("t1")).toBe(false);
	});

	it("tracks targets independently", () => {
		const pacer = createPeerSenderPacer({ now, rateCapacity: 1 });
		expect(pacer.tryConsume("t1")).toBe(true);
		expect(pacer.tryConsume("t2")).toBe(true);
		expect(pacer.tryConsume("t1")).toBe(false);
		expect(pacer.tryConsume("t2")).toBe(false);
	});

	it("refills over time", () => {
		const pacer = createPeerSenderPacer({ now, rateCapacity: 1, rateRefillPerSec: 4 });
		expect(pacer.tryConsume("t1")).toBe(true);
		expect(pacer.tryConsume("t1")).toBe(false);
		clock += 250;
		expect(pacer.tryConsume("t1")).toBe(true);
	});

	it("refund() for an unknown target is a no-op", () => {
		const pacer = createPeerSenderPacer({ now, rateCapacity: 1 });
		pacer.refund("unknown");
		expect(pacer.tryConsume("fresh")).toBe(true);
	});
});
