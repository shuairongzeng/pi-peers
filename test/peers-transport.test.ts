import { randomUUID } from "node:crypto";
import { mkdtempSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { connect as netConnect, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createPeerAuth } from "../src/auth.ts";
import {
	addressFromPath,
	createPeerTransport,
	deriveSocketAddress,
	isValidAddress,
	type PeerConnectOptions,
	type PeerListenOptions,
	resolveAddress,
} from "../src/transport.ts";
import {
	PEER_KEY_FILE_RE,
	PEER_MAX_LINE_BYTES,
	PEER_MAX_MESSAGE_BYTES,
	PEER_SOCKET_NAME_RE,
	type PeerConnection,
	PeerSendError,
	type PeerServer,
	type PeerSocketAddress,
} from "../src/types.ts";

const transport = createPeerTransport();

/** Listener cleanup (also removes POSIX socket files). */
const listenerClosers: Array<() => Promise<void>> = [];
afterEach(async () => {
	const closers = listenerClosers.splice(0);
	await Promise.all(closers.map((close) => close()));
});

const tempDirs: string[] = [];
afterEach(() => {
	for (const dir of tempDirs.splice(0)) {
		rmSync(dir, { recursive: true, force: true });
	}
});

function tempDir(prefix: string): string {
	const dir = mkdtempSync(join(tmpdir(), prefix));
	tempDirs.push(dir);
	return dir;
}

/** A 32-hex string matching the pipe-name/token shape. */
function hex32(): string {
	return randomUUID().replaceAll("-", "");
}

function validPipeAddress(): string {
	return `pipe:pi-peer-${hex32()}`;
}

function validSockAddress(): string {
	return `sock:/tmp/pi-peer-test-${hex32()}.sock`;
}

/** A pi-peer pipe name exactly as deriveSocketAddress builds it. */
function pipeName(): string {
	return `pi-peer-${hex32()}`;
}

// ---------------------------------------------------------------------------
// Address derivation, validation, resolution
// ---------------------------------------------------------------------------

describe("address validation and resolution", () => {
	it("derives a LOCAL-namespaced pipe address on win32, a pid .sock path on POSIX", () => {
		const peersDir = tempDir("pi-peers-transport-");
		const derived = deriveSocketAddress(peersDir);

		if (process.platform === "win32") {
			const name = derived.address.slice("pipe:".length);
			// Wire address carries the LOCAL\ namespace; the path is the full pipe path.
			expect(name.startsWith("LOCAL\\")).toBe(true);
			expect(PEER_SOCKET_NAME_RE.test(name)).toBe(true);
			expect(derived.path).toBe(`\\\\.\\pipe\\${name}`);
			// No socks directory is created for a pipe endpoint.
			expect(readdirSync(peersDir).includes("socks")).toBe(false);
		} else {
			expect(derived.path).toBe(join(peersDir, "socks", `${process.pid}.sock`));
			expect(derived.address).toBe(`sock:${derived.path}`);
			expect(statSync(join(peersDir, "socks")).mode & 0o777).toBe(0o700);
		}
		expect(addressFromPath(derived.path)).toBe(derived.address);
	});

	it("accepts only well-formed pipe:/sock: addresses on the matching platform", () => {
		expect(isValidAddress(validPipeAddress())).toBe(process.platform === "win32");
		expect(isValidAddress(`pipe:LOCAL\\${pipeName()}`)).toBe(process.platform === "win32");
		expect(isValidAddress(validSockAddress())).toBe(process.platform !== "win32");

		const bad: string[] = [
			"pipe:",
			"pipe:LOCAL\\pi-peer-short",
			`pipe:pi-peer-${"g".repeat(32)}`, // 'g' is not hex
			`pipe:pi-peer-${hex32().toUpperCase()}`, // uppercase is not hex
			`pipe:REMOTE\\${pipeName()}`, // foreign pipe namespace
			`pipe:${pipeName()}\\..\\..\\evil`,
			"sock:",
			"sock:relative/path.sock",
			"sock:/has/../traversal.sock",
			"sock:/no-suffix",
			"sock:///double//slash.sock",
			"sock:/dot/./dir.sock",
			`sock:\\\\server\\share\\${hex32()}.sock`, // UNC
			"",
			"tcp:host:1234",
		];
		for (const address of bad) {
			expect(isValidAddress(address)).toBe(false);
			expect(resolveAddress(address)).toBeUndefined();
		}
	});

	it("rejects sock: on win32 and pipe: on POSIX", () => {
		if (process.platform === "win32") {
			expect(isValidAddress(validSockAddress())).toBe(false);
			expect(resolveAddress(validSockAddress())).toBeUndefined();
		} else {
			expect(isValidAddress(validPipeAddress())).toBe(false);
			expect(resolveAddress(validPipeAddress())).toBeUndefined();
		}
	});

	it("resolves pipe addresses to \\\\.\\pipe\\<name> and inverts addressFromPath", () => {
		const name = pipeName();
		// Without the LOCAL\ prefix.
		expect(resolveAddress(`pipe:${name}`)).toBe(`\\\\.\\pipe\\${name}`);
		// With the LOCAL\ prefix.
		expect(resolveAddress(`pipe:LOCAL\\${name}`)).toBe(`\\\\.\\pipe\\LOCAL\\${name}`);
		// addressFromPath maps the canonical LOCAL\ path back to the wire form.
		expect(addressFromPath(`\\\\.\\pipe\\LOCAL\\${name}`)).toBe(`pipe:LOCAL\\${name}`);
		// Derive -> address -> path round trip.
		const derived = deriveSocketAddress(tempDir("pi-peers-roundtrip-"));
		expect(resolveAddress(derived.address)).toBe(derived.path);
	});

	it("addressFromPath rejects non-canonical paths", () => {
		const name = pipeName();
		if (process.platform === "win32") {
			const badPaths: string[] = [
				`\\\\.\\pipe\\${name}`, // missing LOCAL\
				`\\\\.\\pipe\\LOCAL\\${name.slice(0, -1)}`, // short name
				`\\\\.\\pipe\\LOCAL\\other-${hex32()}`,
				`\\\\.\\pipe\\LOCAL\\${name}\\trailing`,
				"",
			];
			for (const p of badPaths) {
				expect(addressFromPath(p)).toBeUndefined();
			}
		} else {
			expect(addressFromPath("/tmp/abs/path.sock")).toBe("sock:/tmp/abs/path.sock");
			expect(addressFromPath("relative.sock")).toBeUndefined();
			expect(addressFromPath("/tmp/../evil.sock")).toBeUndefined();
			expect(addressFromPath("/tmp/no-suffix")).toBeUndefined();
			expect(addressFromPath("")).toBeUndefined();
		}
	});
});

// ---------------------------------------------------------------------------
// Live round trip (named pipes on win32, unix sockets elsewhere)
// ---------------------------------------------------------------------------

/**
 * A listening transport plus a promise resolving when its first connection
 * is accepted. The connection's remaining lines are the transport's own
 * async iterable; exactly one consumer (the test's onFirst) drains it.
 */
interface ServerHandle {
	server: PeerServer;
	first: Promise<{ conn: PeerConnection; firstLine: string | undefined; lines: AsyncIterable<string> }>;
}

/**
 * Start a listener whose first connection is recorded. `onFirst` runs inside
 * onConnection for per-test behavior like echoing or ending the connection.
 */
function startServer(
	onFirst: (conn: PeerConnection, firstLine: string | undefined, lines: AsyncIterable<string>) => void,
	options?: PeerListenOptions,
): Promise<ServerHandle> {
	let resolveFirst: ((value: Awaited<ServerHandle["first"]>) => void) | undefined;
	const first = new Promise<Awaited<ServerHandle["first"]>>((resolve) => {
		resolveFirst = resolve;
	});

	// The contract's PeerTransport.listen takes two arguments; the transport
	// implementation accepts an optional third (firstLineTimeoutMs). Narrowed
	// through a typed local so tests can shorten the deadline.
	const listenWithOptions = transport.listen as (
		address: PeerSocketAddress,
		onConnection: (conn: PeerConnection, firstLine: string | undefined, lines: AsyncIterable<string>) => void,
		options?: PeerListenOptions,
	) => Promise<PeerServer>;
	const listening = listenWithOptions(
		deriveSocketAddress(tmpdir()),
		(conn, firstLine, lines) => {
			resolveFirst?.({ conn, firstLine, lines });
			try {
				onFirst(conn, firstLine, lines);
			} catch {
				conn.destroy();
			}
		},
		options,
	);
	listenerClosers.push(() => listening.then((server) => server.close()));

	return listening.then((server) => ({ server, first }));
}

/**
 * connect() with the implementation's optional second argument. The contract
 * signature takes only the address; narrowing through a typed local keeps
 * tests type-clean while exercising the timeout overrides.
 */
const connectWithOptions = transport.connect as (
	address: string,
	options?: PeerConnectOptions,
) => Promise<PeerConnection>;

/** Collect every line of an async iterable once the stream has ended. */
async function collectLines(lines: AsyncIterable<string>): Promise<string[]> {
	const collected: string[] = [];
	for await (const line of lines) {
		collected.push(line);
	}
	return collected;
}

/** Open a raw (framing-free) client socket to a listening transport. */
function rawConnect(server: PeerServer): Promise<Socket> {
	return new Promise<Socket>((resolve, reject) => {
		const path = resolveAddress(server.address.address);
		if (path === undefined) {
			reject(new Error(`unresolvable test address: ${server.address.address}`));
			return;
		}
		const sock = netConnect({ path });
		sock.once("connect", () => resolve(sock));
		sock.once("error", (error) => reject(error));
	});
}

describe("transport round trip", () => {
	it("carries an auth line and two frames end-to-end", async () => {
		const token = hex32();
		const { server, first } = await startServer((conn) => {
			void conn.end();
		});

		const client = await transport.connect(server.address.address);
		client.writeLine(JSON.stringify({ type: "auth", token }));
		client.writeLine(JSON.stringify({ msgV: 1, msg_id: "m1", type: "user", content: "hello" }));
		client.writeLine(JSON.stringify({ msgV: 1, msg_id: "m2", type: "user", content: "world" }));
		await client.end();

		const recorded = await first;
		expect(JSON.parse(recorded.firstLine!)).toEqual({ type: "auth", token });
		const frames = await collectLines(recorded.lines);
		expect(frames).toHaveLength(2);
		expect(JSON.parse(frames[0]).content).toBe("hello");
		expect(JSON.parse(frames[1]).msg_id).toBe("m2");
	});

	it("frames lines split across writes and coalesced into one write", async () => {
		const { server, first } = await startServer(() => {});

		const sock = await rawConnect(server);
		// One line split across two writes, then two lines in a single write.
		sock.write(Buffer.from('{"part":1', "utf8"));
		await new Promise<void>((resolve) => setImmediate(resolve));
		sock.write(Buffer.from('}\n{"a":2}\n{"b":3', "utf8"));
		await new Promise<void>((resolve) => setImmediate(resolve));
		sock.write(Buffer.from("}\n", "utf8"));
		sock.end();

		const recorded = await first;
		expect(recorded.firstLine).toBe('{"part":1}');
		expect(await collectLines(recorded.lines)).toEqual(['{"a":2}', '{"b":3}']);
	});

	it("reassembles a multi-byte UTF-8 character split across chunks", async () => {
		const { server, first } = await startServer(() => {});

		const sock = await rawConnect(server);
		const payload = Buffer.from('{"emoji":"😀"}\n', "utf8");
		// Split inside the 4-byte emoji sequence.
		const cut = Buffer.byteLength('{"emoji":"', "utf8") + 2;
		sock.write(payload.subarray(0, cut));
		await new Promise<void>((resolve) => setImmediate(resolve));
		sock.write(payload.subarray(cut));
		sock.end();

		const recorded = await first;
		expect(recorded.firstLine).toBe('{"emoji":"😀"}');
		expect(await collectLines(recorded.lines)).toEqual([]);
	});

	it("destroys the connection when the first line is late", async () => {
		let sawConnection = false;
		const { server } = await startServer(
			() => {
				sawConnection = true;
			},
			{ firstLineTimeoutMs: 80 },
		);

		const sock = await rawConnect(server);
		// Wait for the deadline to fire and the socket to be destroyed.
		await new Promise<void>((resolve) => sock.once("close", resolve));
		// No auth line ever arrived, so onConnection must never have run.
		expect(sawConnection).toBe(false);
	});

	it("destroys the connection when a line exceeds PEER_MAX_LINE_BYTES", async () => {
		const { server, first } = await startServer(() => {});

		const sock = await rawConnect(server);
		// Valid auth line, then a line past the per-line cap: framing must
		// destroy the socket instead of delivering it.
		sock.write("auth-ok\n");
		sock.write(`x${"a".repeat(PEER_MAX_LINE_BYTES)}\n`);
		await new Promise<void>((resolve) => sock.once("close", resolve));

		const recorded = await first;
		expect(recorded.firstLine).toBe("auth-ok");
		expect(await collectLines(recorded.lines)).toEqual([]);
	});

	it("throws message_too_large when the cumulative outbound cap is exceeded", async () => {
		const { server, first } = await startServer(() => {});

		const client = await transport.connect(server.address.address);
		client.writeLine("auth");
		let caught: unknown;
		try {
			// Cumulative (auth + newline + this line) crosses PEER_MAX_MESSAGE_BYTES.
			client.writeLine("b".repeat(PEER_MAX_MESSAGE_BYTES));
		} catch (error) {
			caught = error;
		}
		expect(caught).toBeInstanceOf(PeerSendError);
		expect((caught as PeerSendError).code).toBe("message_too_large");

		const recorded = await first;
		// A failed write leaves the outbound side open; the sender destroys
		// the connection (the stream never gets a clean EOF otherwise).
		client.destroy();
		expect(recorded.firstLine).toBe("auth");
		expect(await collectLines(recorded.lines)).toEqual([]);
	});

	it("rejects invalid targets with PeerSendError(invalid_target)", async () => {
		const invalid: string[] = [
			"pipe:not-a-pipe-name",
			"sock:relative.sock",
			"sock:/has/../traversal.sock",
			"no-prefix-at-all",
		];
		for (const address of invalid) {
			let caught: unknown;
			try {
				await transport.connect(address);
			} catch (error) {
				caught = error;
			}
			expect(caught).toBeInstanceOf(PeerSendError);
			expect((caught as PeerSendError).code).toBe("invalid_target");
		}
	});

	it("reports connect_failed for an address nobody listens on", async () => {
		let caught: unknown;
		try {
			if (process.platform === "win32") {
				// A well-formed pipe name with no listener errors quickly.
				await connectWithOptions(validPipeAddress(), { connectTimeoutMs: 4_000 });
			} else {
				// A socket file that exists but has no listener.
				const dir = tempDir("pi-peers-dead-");
				const path = join(dir, "dead.sock");
				writeFileSync(path, "");
				await connectWithOptions(`sock:${path}`, { connectTimeoutMs: 4_000 });
			}
		} catch (error) {
			caught = error;
		}
		expect(caught).toBeInstanceOf(PeerSendError);
		expect((caught as PeerSendError).code).toBe("connect_failed");
		expect((caught as PeerSendError).message).toContain("re-resolve");
	});

	it("readLines exposes the lines written to a connect()ed connection", async () => {
		const { server } = await startServer((conn, _firstLine, lines) => {
			// Echo each inbound line back doubled, then end.
			void (async () => {
				for await (const line of lines) {
					conn.writeLine(`${line}${line}`);
				}
				await conn.end();
			})();
		});

		const client = await transport.connect(server.address.address);
		client.writeLine("auth");
		client.writeLine("a");
		client.writeLine("b");
		await client.end();

		const echoed: string[] = [];
		for await (const line of transport.readLines(client)) {
			echoed.push(line);
		}
		expect(echoed).toEqual(["aa", "bb"]);
	});

	it("logs a console.error and still resolves when end()'s flush fails", async () => {
		const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
		try {
			// Server-side connection that never drains: it accepts the auth
			// line, then destroys itself while the client's big line is still
			// flushing, so socket.end()'s callback receives the flush error.
			let serverConn: PeerConnection | undefined;
			const { server, first } = await startServer((conn) => {
				serverConn = conn;
			});

			const client = await transport.connect(server.address.address);
			client.writeLine("auth");
			// The auth line must have been consumed before we break the pipe.
			const recorded = await first;
			expect(recorded.firstLine).toBe("auth");

			client.writeLine(`x${"a".repeat(600 * 1024)}`);
			const ending = client.end();
			serverConn?.destroy();
			// Best-effort semantics: end() resolves even when the flush failed.
			await ending;

			const logged = consoleError.mock.calls
				.flat()
				.map((part) => String(part))
				.join("\n");
			expect(logged).toContain("[pi-peers]");
			expect(logged).toContain("end failed");
		} finally {
			consoleError.mockRestore();
		}
	});

	it("readLines rejects connections from another transport factory", () => {
		const fake: PeerConnection = {
			writeLine: () => {},
			end: () => Promise.resolve(),
			destroy: () => {},
		};
		expect(() => transport.readLines(fake)).toThrow();
	});
});

// ---------------------------------------------------------------------------
// auth.ts: key file round trip
// ---------------------------------------------------------------------------

describe("peer auth key files", () => {
	const auth = createPeerAuth();

	let peersDir: string;
	beforeEach(() => {
		peersDir = tempDir("pi-peers-auth-");
	});

	it("generates 32-hex token pairs", () => {
		const tokens = auth.generateTokens();
		expect(tokens.peerToken).toMatch(/^[0-9a-f]{32}$/);
		expect(tokens.childToken).toMatch(/^[0-9a-f]{32}$/);
		expect(tokens.peerToken).not.toBe(tokens.childToken);
	});

	it("publishes and looks up a key file, round trip", () => {
		const canonical =
			process.platform === "win32" ? `\\\\.\\pipe\\LOCAL\\${pipeName()}` : join(peersDir, "socks", "1234.sock");
		const tokens = auth.generateTokens();
		auth.publishKey(peersDir, 4321, canonical, tokens, "proc-start-7", "win32:host");

		const files = readdirSync(peersDir);
		expect(files).toHaveLength(1);
		const match = PEER_KEY_FILE_RE.exec(files[0]);
		expect(match).not.toBeNull();
		expect(match![1]).toBe("4321");

		expect(auth.lookupKey(peersDir, canonical)).toEqual({
			pid: 4321,
			peerToken: tokens.peerToken,
			procStart: "proc-start-7",
			pidDomain: "win32:host",
		});

		// A different socket path must not match any key.
		const other =
			process.platform === "win32" ? `\\\\.\\pipe\\LOCAL\\${pipeName()}` : join(peersDir, "socks", "4321.sock");
		expect(auth.lookupKey(peersDir, other)).toBeUndefined();
	});

	it("omits optional fields when not provided", () => {
		const canonical = `\\\\.\\pipe\\LOCAL\\${pipeName()}`;
		const tokens = auth.generateTokens();
		auth.publishKey(peersDir, 100, canonical, tokens, undefined, undefined as unknown as string);
		expect(auth.lookupKey(peersDir, canonical)).toEqual({ pid: 100, peerToken: tokens.peerToken });
	});

	it("returns undefined for malformed or tampered key files", () => {
		const canonical = `\\\\.\\pipe\\LOCAL\\${pipeName()}`;
		const tokens = auth.generateTokens();
		auth.publishKey(peersDir, 200, canonical, tokens, undefined, "d");
		const keyFile = readdirSync(peersDir)[0];

		// pid in content disagrees with the filename.
		writeFileSync(join(peersDir, keyFile), `${JSON.stringify({ pid: 999, peerToken: tokens.peerToken })}\n`);
		expect(auth.lookupKey(peersDir, canonical)).toBeUndefined();

		// Token is not 32-hex.
		writeFileSync(join(peersDir, keyFile), `${JSON.stringify({ pid: 200, peerToken: "short" })}\n`);
		expect(auth.lookupKey(peersDir, canonical)).toBeUndefined();

		// Not JSON at all.
		writeFileSync(join(peersDir, keyFile), "not-json");
		expect(auth.lookupKey(peersDir, canonical)).toBeUndefined();

		// No key for an unknown path.
		expect(auth.lookupKey(peersDir, `\\\\.\\pipe\\LOCAL\\${pipeName()}`)).toBeUndefined();
	});

	it("deleteKeys removes only the target pid's key files", () => {
		const canonicalA = `\\\\.\\pipe\\LOCAL\\${pipeName()}`;
		const canonicalB = `\\\\.\\pipe\\LOCAL\\${pipeName()}`;
		auth.publishKey(peersDir, 300, canonicalA, auth.generateTokens(), undefined, "d");
		auth.publishKey(peersDir, 400, canonicalB, auth.generateTokens(), undefined, "d");

		auth.deleteKeys(peersDir, 300);
		const remaining = readdirSync(peersDir);
		expect(remaining).toHaveLength(1);
		expect(PEER_KEY_FILE_RE.exec(remaining[0])![1]).toBe("400");
		expect(auth.lookupKey(peersDir, canonicalA)).toBeUndefined();
		expect(auth.lookupKey(peersDir, canonicalB)).toBeDefined();

		// Deleting again, or an unknown pid, changes nothing.
		auth.deleteKeys(peersDir, 300);
		auth.deleteKeys(peersDir, 999);
		expect(readdirSync(peersDir)).toHaveLength(1);
	});

	it("leaves no temporary files behind after publish", () => {
		const canonical = `\\\\.\\pipe\\LOCAL\\${pipeName()}`;
		auth.publishKey(peersDir, 500, canonical, auth.generateTokens(), undefined, "d");
		const files = readdirSync(peersDir);
		expect(files).toHaveLength(1);
		expect(files[0].endsWith(".key")).toBe(true);
	});
});

// ---------------------------------------------------------------------------
// POSIX-only branches
// ---------------------------------------------------------------------------

describe("POSIX socket paths", () => {
	it.skipIf(process.platform === "win32")("cleans up the socket file on close", async () => {
		const peersDir = tempDir("pi-peers-posix-");
		const derived = deriveSocketAddress(peersDir);
		expect(statSync(derived.path)).toBeDefined();

		const server = await transport.listen(derived, () => {});
		await server.close();

		let removed = false;
		try {
			statSync(derived.path);
		} catch {
			removed = true;
		}
		expect(removed).toBe(true);
	});

	it.skipIf(process.platform === "win32")("retries once after EADDRINUSE from a stale socket file", async () => {
		const peersDir = tempDir("pi-peers-stale-");
		const derived = deriveSocketAddress(peersDir);
		// Leftover socket file (no listener) from a crashed process.
		writeFileSync(derived.path, "");

		const server = await transport.listen(derived, () => {});
		listenerClosers.push(() => server.close());
		expect(statSync(derived.path)).toBeDefined();
		await server.close();
	});
});

// ---------------------------------------------------------------------------
// listen()/connect() option overrides
// ---------------------------------------------------------------------------

describe("listen/connect options", () => {
	it("accepts the documented optional overrides", async () => {
		const listenOptions: PeerListenOptions = { firstLineTimeoutMs: 1_000 };
		const { server, first } = await startServer((conn) => {
			void conn.end();
		}, listenOptions);

		const connectOptions: PeerConnectOptions = { connectTimeoutMs: 3_000, macosCloseDelayMs: 1 };
		const client = await connectWithOptions(server.address.address, connectOptions);
		client.writeLine("auth-options");
		await client.end();

		const recorded = await first;
		expect(recorded.firstLine).toBe("auth-options");
		expect(await collectLines(recorded.lines)).toEqual([]);
	});
});
