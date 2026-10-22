/**
 * End-to-end peers test: two fully wired stacks (protocol + transport + auth +
 * registry + guard + pacer + inbox + outbox) sharing one registry directory in
 * a single test process, exchanging real frames over real sockets/pipes.
 *
 * The accept loop mirrors src/extensions/peers/index.ts — this test is the
 * integration seam proving the modules compose the way the extension wires them.
 */

import { type ChildProcess, spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import net from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createPeerAuth } from "../src/auth.ts";
import { createPeerGuard, createPeerSenderPacer } from "../src/guard.ts";
import { createPeerInbox } from "../src/inbox.ts";
import { createPeerOutbox } from "../src/outbox.ts";
import { createPeerProtocol } from "../src/protocol.ts";
import { createPeerRegistry } from "../src/registry.ts";
import { createPeerTransport } from "../src/transport.ts";
import {
	PEER_FEATURES,
	PEER_PROTOCOL_VERSION,
	type PeerAcceptPolicy,
	type PeerConnection,
	type PeerFrame,
	type PeerHost,
	type PeerIdentity,
	type PeerInbox,
	type PeerPriority,
	type PeerRecord,
	type PeerServer,
	type PeerServiceDeps,
	type PeerSocketAddress,
} from "../src/types.ts";

interface InjectedCall {
	content: string;
	priority: PeerPriority;
}

interface FakeHost extends PeerHost {
	injected: InjectedCall[];
	notices: string[];
	policy: PeerAcceptPolicy;
}

function createFakeHost(): FakeHost {
	const host: FakeHost = {
		injected: [],
		notices: [],
		policy: "all",
		injectUserMessage: async (content, priority) => {
			host.injected.push({ content, priority });
		},
		notify: (_title, message) => {
			host.notices.push(message);
		},
		confirm: async () => "yes",
		injectAgentNotice: async (text) => {
			host.notices.push(text);
		},
		hasUI: () => true,
		isIdleNow: () => true,
		quit: () => {},
		acceptPolicy: () => host.policy,
	};
	return host;
}

interface Stack {
	name: string;
	pid: number;
	address: PeerSocketAddress;
	identity: PeerIdentity;
	deps: PeerServiceDeps;
	host: FakeHost;
	inbox: PeerInbox;
	server: PeerServer | undefined;
	serverReady: Promise<void>;
	record: PeerRecord;
}

function makeAddress(pid: number, label: string): PeerSocketAddress {
	if (process.platform === "win32") {
		const name = `pi-peer-${randomBytes(16).toString("hex")}`;
		return { path: `\\\\.\\pipe\\LOCAL\\${name}`, address: `pipe:LOCAL\\${name}` };
	}
	const path = join(tmpdir(), `pi-peer-test-${label}-${pid}-${randomBytes(4).toString("hex")}.sock`);
	return { path, address: `sock:${path}` };
}

describe("peers end-to-end", () => {
	let peersDir: string;
	let child: ChildProcess | undefined;
	let stacks: Stack[];

	const buildStack = (name: string, pid: number): Stack => {
		const protocol = createPeerProtocol();
		const transport = createPeerTransport();
		const auth = createPeerAuth();
		const registry = createPeerRegistry(peersDir);
		const guard = createPeerGuard();
		const pacer = createPeerSenderPacer();
		const host = createFakeHost();
		const address = makeAddress(pid, name);
		const tokens = auth.generateTokens();
		const identity: PeerIdentity = {
			pid,
			sessionId: randomBytes(16).toString("hex"),
			socketAddress: address.address,
			name,
			hopToken: randomBytes(4).toString("hex"),
			tokens,
		};
		const deps: PeerServiceDeps = {
			identity,
			protocol,
			transport,
			auth,
			registry,
			guard,
			pacer,
			host,
			outbox: undefined as unknown as PeerServiceDeps["outbox"],
		};
		const outbox = createPeerOutbox({ ...deps, peersDir });
		deps.outbox = outbox;
		const inbox: PeerInbox = createPeerInbox({ ...deps, outbox });

		const record: PeerRecord = {
			version: 1,
			pid,
			sessionId: identity.sessionId,
			cwd: process.cwd(),
			name,
			nameSource: "user",
			formerNames: [],
			kind: "interactive",
			status: "idle",
			statusUpdatedAt: Date.now(),
			startedAt: Date.now(),
			procStart: registry.readProcStart(pid),
			pidDomain: registry.pidDomain(),
			socketPath: address.path,
			peerProtocol: PEER_PROTOCOL_VERSION,
			features: [...PEER_FEATURES],
		};

		// Mirrors the accept loop in src/extensions/peers/index.ts.
		const stack: Stack = {
			name,
			pid,
			address,
			identity,
			deps,
			host,
			inbox,
			server: undefined,
			serverReady: Promise.resolve(),
			record,
		};
		stack.serverReady = transport
			.listen(address, (conn: PeerConnection, firstLine: string | undefined, lines: AsyncIterable<string>) => {
				const token = firstLine !== undefined ? protocol.parseAuthLine(firstLine) : undefined;
				const cls = token === tokens.childToken ? "child" : token === tokens.peerToken ? "peer" : undefined;
				if (cls === undefined) {
					conn.destroy();
					return;
				}
				void (async () => {
					try {
						for await (const line of lines) {
							const frame: PeerFrame | undefined = protocol.parseFrameLine(line);
							if (frame === undefined) {
								conn.destroy();
								return;
							}
							await inbox.handleFrame(frame, cls);
						}
					} catch {
						conn.destroy();
					}
				})();
			})
			.then((s) => {
				stack.server = s;
			});
		return stack;
	};

	beforeEach(async () => {
		peersDir = mkdtempSync(join(tmpdir(), "pi-peers-e2e-"));
		child = spawn(process.execPath, ["-e", "process.stdin.resume()"], { stdio: "ignore" });
		const spawned = Promise.withResolvers<void>();
		child.once("spawn", () => spawned.resolve());
		await spawned.promise;
		const a = buildStack("alpha", process.pid);
		const b = buildStack("bravo", child.pid!);
		stacks = [a, b];
		await Promise.all(stacks.map((s) => s.serverReady));
		for (const s of stacks) {
			s.deps.registry.publish(s.record);
			s.deps.auth.publishKey(peersDir, s.pid, s.address.path, s.identity.tokens, undefined, s.record.pidDomain);
		}
	});

	afterEach(async () => {
		for (const s of stacks) await s.server?.close();
		child?.kill();
		rmSync(peersDir, { recursive: true, force: true });
	});

	it("delivers a message end-to-end with a delivered receipt", async () => {
		const [a, b] = stacks;
		const outcome = await a.deps.outbox.send("bravo", "hello from alpha", {});
		expect(outcome.toName).toBe("bravo");

		await vi.waitFor(() => {
			expect(b.host.injected).toHaveLength(1);
		});
		expect(b.host.injected[0].priority).toBe("next");
		expect(b.host.injected[0].content).toContain("hello from alpha");
		expect(b.host.injected[0].content).toContain("cross-session-message");
		expect(b.host.injected[0].content).toContain('from-name="alpha"');

		await vi.waitFor(() => {
			expect(a.deps.outbox.receiptOf(outcome.msgId)).toBe("delivered");
		});
	});

	it("priority now is delivered with steer semantics", async () => {
		const [a, b] = stacks;
		await a.deps.outbox.send("bravo", "urgent", { priority: "now" });
		await vi.waitFor(() => {
			expect(b.host.injected).toHaveLength(1);
		});
		expect(b.host.injected[0].priority).toBe("now");
	});

	it("notify_when_idle subscription fires a peer_idle_notice back", async () => {
		const [a, b] = stacks;
		await a.deps.outbox.send("bravo", "ping me when idle", { notifyWhenIdle: true });
		await vi.waitFor(() => {
			expect(b.host.injected).toHaveLength(1);
		});
		// Bravo goes idle → alpha's agent gets an idle notice.
		b.inbox.onIdle();
		await vi.waitFor(() => {
			expect(a.host.notices.some((n) => n.includes("idle"))).toBe(true);
		});
	});

	it("rejects connections with a bad token", async () => {
		const [, b] = stacks;
		const protocol = createPeerProtocol();
		// Raw socket: the server must destroy the connection on a bad token;
		// awaiting the close event is deterministic, no settle sleep.
		const sock = net.createConnection({ path: b.address.path });
		const closed = Promise.withResolvers<void>();
		sock.once("close", () => closed.resolve());
		const connected = Promise.withResolvers<void>();
		sock.once("connect", () => connected.resolve());
		sock.once("error", () => connected.reject(new Error("connect failed")));
		await connected.promise;
		sock.write(protocol.buildAuthLine("0".repeat(32)));
		sock.write(
			protocol.serializeFrame({
				...protocol.newEnvelope(),
				type: "user",
				from: "pipe:LOCAL\\pi-peer-00000000000000000000000000000000",
				fromPid: 1234,
				fromSession: "x",
				hopChain: [],
				priority: "next",
				content: "forged",
			}),
		);
		sock.end();
		await closed.promise;
		expect(b.host.injected).toHaveLength(0);
	});

	it("reports not_found with the live peer list", async () => {
		const [a] = stacks;
		await expect(a.deps.outbox.send("nobody", "hi", {})).rejects.toMatchObject({ code: "not_found" });
	});
});
