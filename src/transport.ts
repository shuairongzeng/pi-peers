/**
 * transport.ts — IPC endpoints of the peers extension.
 *
 * Windows listens on a named pipe under \\.\pipe\LOCAL\ with a random
 * pi-peer-<32hex> name; POSIX listens on <peersDir>/socks/<pid>.sock. Frames
 * are newline-delimited; framing here is strict LF-only (same rationale as
 * modes/rpc/jsonl.ts: readline also splits on Unicode separators that are
 * legal inside JSON payloads).
 */

import { randomBytes } from "node:crypto";
import { mkdirSync, unlinkSync } from "node:fs";
import { createServer, connect as netConnect, type Server, type Socket } from "node:net";
import { join } from "node:path";
import { StringDecoder } from "node:string_decoder";
import {
	PEER_CONNECT_TIMEOUT_MS,
	PEER_FIRST_LINE_TIMEOUT_MS,
	PEER_MACOS_CLOSE_DELAY_MS,
	PEER_MAX_LINE_BYTES,
	PEER_MAX_MESSAGE_BYTES,
	PEER_SOCKET_NAME_RE,
	PEER_SOCKET_PREFIX,
	PEERS_SOCKETS_DIR_NAME,
	type PeerConnection,
	PeerSendError,
	type PeerServer,
	type PeerSocketAddress,
	type PeerTransport,
} from "./types.ts";

/** Windows named pipe namespace root every peers pipe lives under. */
const WIN_PIPE_ROOT = "\\\\.\\pipe\\LOCAL\\";

/** Wire address prefix for named pipes; the value is the pipe name. */
const PIPE_ADDRESS_PREFIX = "pipe:";

/** Wire address prefix for unix sockets; the value is the absolute path. */
const SOCK_ADDRESS_PREFIX = "sock:";

/** Extra listen options; lets tests shorten the first-line deadline. */
export interface PeerListenOptions {
	firstLineTimeoutMs?: number;
}

/** Extra connect options; lets tests shorten the connect deadline. */
export interface PeerConnectOptions {
	connectTimeoutMs?: number;
	macosCloseDelayMs?: number;
}

/** All platform forks in this module read process.platform through here. */
function isWin32(): boolean {
	return process.platform === "win32";
}

// ---------------------------------------------------------------------------
// Socket address derivation, validation, resolution
// ---------------------------------------------------------------------------

/**
 * Derive the socket address this process should listen on.
 *
 * Windows: a named pipe with a random 32-hex name (globally unique per
 * machine, no cleanup needed). POSIX: <peersDir>/socks/<pid>.sock with a
 * 0700 directory so only this user can connect.
 */
export function deriveSocketAddress(peersDir: string): PeerSocketAddress {
	if (isWin32()) {
		const name = `${PEER_SOCKET_PREFIX}${randomBytes(16).toString("hex")}`;
		return {
			path: `${WIN_PIPE_ROOT}${name}`,
			// Address form keeps the LOCAL\ namespace so the wire address
			// round-trips through addressFromPath()/resolveAddress().
			address: `${PIPE_ADDRESS_PREFIX}LOCAL\\${name}`,
		};
	}
	const socksDir = join(peersDir, PEERS_SOCKETS_DIR_NAME);
	mkdirSync(socksDir, { recursive: true, mode: 0o700 });
	const path = join(socksDir, `${process.pid}.sock`);
	return {
		path,
		address: `${SOCK_ADDRESS_PREFIX}${path}`,
	};
}

/** A connectable sock: value must be an absolute, traversal-free .sock path. */
function validateSockPath(path: string): boolean {
	if (!path.startsWith("/") || !path.endsWith(".sock")) {
		return false;
	}
	const segments = path.slice(1).split(/\/+/);
	return !segments.includes("") && !segments.includes(".") && !segments.includes("..");
}

/**
 * Validate an address string from the wire. Pipe names must pass
 * PEER_SOCKET_NAME_RE and only exist on Windows; sock: paths must be local
 * absolute paths and only exist on POSIX. Everything else (UNC shares,
 * relative paths, foreign pipe namespaces, embedded "..") is rejected.
 */
export function isValidAddress(address: string): boolean {
	if (address.startsWith(PIPE_ADDRESS_PREFIX)) {
		return isWin32() && PEER_SOCKET_NAME_RE.test(address.slice(PIPE_ADDRESS_PREFIX.length));
	}
	if (address.startsWith(SOCK_ADDRESS_PREFIX)) {
		return !isWin32() && validateSockPath(address.slice(SOCK_ADDRESS_PREFIX.length));
	}
	return false;
}

/** Resolve a wire address back to the path net.connect()/listen() expects. */
export function resolveAddress(address: string): string | undefined {
	if (address.startsWith(PIPE_ADDRESS_PREFIX)) {
		const name = address.slice(PIPE_ADDRESS_PREFIX.length);
		if (!PEER_SOCKET_NAME_RE.test(name)) {
			return undefined;
		}
		// Literal passthrough: \\.\pipe\<name> on the path side, both
		// with and without the LOCAL\ namespace spelling.
		return `\\\\.\\pipe\\${name}`;
	}
	if (address.startsWith(SOCK_ADDRESS_PREFIX)) {
		const path = address.slice(SOCK_ADDRESS_PREFIX.length);
		if (isWin32() || !validateSockPath(path)) {
			return undefined;
		}
		return path;
	}
	return undefined;
}

/** Convert a canonical connect path (PeerRecord.socketPath) to its wire address form. */
export function addressFromPath(path: string): string | undefined {
	if (isWin32()) {
		const match = /^\\\\\.\\pipe\\(LOCAL\\pi-peer-[0-9a-f]{32})$/.exec(path);
		return match === null ? undefined : `${PIPE_ADDRESS_PREFIX}${match[1]}`;
	}
	return validateSockPath(path) ? `${SOCK_ADDRESS_PREFIX}${path}` : undefined;
}

// ---------------------------------------------------------------------------
// Framing: one shared LF-only line queue per socket
// ---------------------------------------------------------------------------

/** Completed lines waiting to be consumed plus consumers blocked on the next one. */
interface LineQueue {
	lines: string[];
	waiters: Array<(result: IteratorResult<string>) => void>;
	ended: boolean;
}

/** Hand one completed line to a consumer, waking a blocked one if present. */
function pushLine(queue: LineQueue, line: string): void {
	const waiter = queue.waiters.shift();
	if (waiter === undefined) {
		queue.lines.push(line);
		return;
	}
	waiter({ value: line, done: false });
}

/**
 * Mark the stream finished: wake blocked consumers with EOF. Buffered lines
 * stay drainable so a sender's final burst is never lost to a close race.
 */
function endQueue(queue: LineQueue): void {
	queue.ended = true;
	const waiters = queue.waiters.splice(0);
	for (const waiter of waiters) {
		waiter({ value: undefined, done: true });
	}
}

/**
 * Attach strict LF-only framing to a socket.
 *
 * Splitting on `\n` only (StringDecoder for cross-chunk UTF-8) mirrors the
 * JSONL reader. Any line whose wire size exceeds PEER_MAX_LINE_BYTES —
 * complete or still buffered — destroys the socket immediately.
 */
function attachLineFraming(socket: Socket, queue: LineQueue): void {
	const decoder = new StringDecoder("utf8");
	let buffer = "";
	let pendingBytes = 0;

	const drop = () => {
		endQueue(queue);
		socket.destroy();
	};

	// Socket errors (peer reset, broken pipe) surface as EOF to line
	// consumers; without this listener they would crash the process.
	socket.on("error", () => {});

	socket.on("data", (chunk: Buffer) => {
		if (queue.ended) {
			return;
		}
		buffer += decoder.write(chunk);
		pendingBytes += chunk.length;

		while (true) {
			const newlineIndex = buffer.indexOf("\n");
			if (newlineIndex === -1) {
				break;
			}
			const line = buffer.slice(0, newlineIndex);
			buffer = buffer.slice(newlineIndex + 1);
			const lineBytes = Buffer.byteLength(line, "utf8");
			pendingBytes -= lineBytes + 1;
			if (lineBytes > PEER_MAX_LINE_BYTES) {
				drop();
				return;
			}
			pushLine(queue, line);
		}

		if (pendingBytes > PEER_MAX_LINE_BYTES) {
			drop();
		}
	});

	socket.on("end", () => {
		if (queue.ended) {
			return;
		}
		buffer += decoder.end();
		if (Buffer.byteLength(buffer, "utf8") > PEER_MAX_LINE_BYTES) {
			drop();
			return;
		}
		if (buffer.length > 0) {
			pushLine(queue, buffer);
			buffer = "";
		}
		endQueue(queue);
	});

	socket.on("close", () => {
		endQueue(queue);
	});
}

/** Next line, or undefined once the stream ended (single queue pull). */
function nextLine(queue: LineQueue): Promise<string | undefined> {
	if (queue.lines.length > 0) {
		return Promise.resolve(queue.lines.shift());
	}
	if (queue.ended) {
		return Promise.resolve(undefined);
	}
	return new Promise<string | undefined>((resolve) => {
		queue.waiters.push((result) => {
			resolve(result.done ? undefined : result.value);
		});
	});
}

/** All lines of one queue as an async iterable, in arrival order. */
async function* iterateLines(queue: LineQueue): AsyncGenerator<string> {
	while (true) {
		if (queue.lines.length > 0) {
			yield queue.lines.shift()!;
			continue;
		}
		if (queue.ended) {
			return;
		}
		const line = await nextLine(queue);
		if (line === undefined) {
			return;
		}
		yield line;
	}
}

// ---------------------------------------------------------------------------
// PeerConnection
// ---------------------------------------------------------------------------

/** Per-connection state; readLines() reaches it via a module-level WeakMap. */
interface ConnState {
	socket: Socket;
	queue: LineQueue;
	writtenBytes: number;
	closing: boolean;
	macosCloseDelayMs: number;
}

const connStates = new WeakMap<PeerConnection, ConnState>();

/**
 * Wrap a socket as a PeerConnection.
 *
 * When `queue` is provided (inbound connections) the socket's framing is
 * already attached by the listener; otherwise framing is attached here so
 * readLines() never misses data that arrived early.
 */
function createConnection(socket: Socket, queue: LineQueue | undefined, macosCloseDelayMs: number): PeerConnection {
	const state: ConnState = {
		socket,
		queue: queue ?? { lines: [], waiters: [], ended: false },
		writtenBytes: 0,
		closing: false,
		macosCloseDelayMs,
	};
	if (queue === undefined) {
		attachLineFraming(socket, state.queue);
	}

	const connection: PeerConnection = {
		writeLine(line: string) {
			if (state.closing || state.socket.destroyed) {
				return;
			}
			// Callers pass either a bare line or a pre-framed one (protocol's
			// serializeFrame/buildAuthLine already end with "\n"); never emit a
			// double newline — the receiver treats empty lines as malformed.
			const encoded = Buffer.from(line.endsWith("\n") ? line : `${line}\n`, "utf8");
			if (state.writtenBytes + encoded.length > PEER_MAX_MESSAGE_BYTES) {
				throw new PeerSendError(
					"message_too_large",
					`peer message would exceed PEER_MAX_MESSAGE_BYTES (${PEER_MAX_MESSAGE_BYTES} bytes)`,
				);
			}
			state.writtenBytes += encoded.length;
			state.socket.write(encoded);
		},
		async end() {
			if (state.closing) {
				return;
			}
			state.closing = true;
			// darwin-only tail-loss workaround: give the peer a moment to
			// drain the pipe before we close our side.
			if (process.platform === "darwin" && state.macosCloseDelayMs > 0) {
				await new Promise<void>((resolve) => setTimeout(resolve, state.macosCloseDelayMs));
			}
			if (state.socket.destroyed) {
				return;
			}
			await new Promise<void>((resolve) => {
				state.socket.end((error?: Error | null) => {
					if (error) {
						console.error(`[pi-peers] connection end failed: ${String(error)}`);
					}
					resolve();
				});
				state.socket.once("close", () => resolve());
			});
		},
		destroy() {
			state.closing = true;
			state.socket.destroy();
		},
	};

	connStates.set(connection, state);
	return connection;
}

// ---------------------------------------------------------------------------
// Listen
// ---------------------------------------------------------------------------

/** Resolve once the server listens, or reject with the listen error. */
function startListen(server: Server, path: string): Promise<void> {
	return new Promise<void>((resolve, reject) => {
		const onListening = () => {
			server.off("error", onError);
			resolve();
		};
		const onError = (error: Error) => {
			server.off("listening", onListening);
			reject(error);
		};
		server.once("listening", onListening);
		server.once("error", onError);
		server.listen({ path });
	});
}

/**
 * Start listening on address.path.
 *
 * Every inbound connection is framed immediately. The first line must arrive
 * within firstLineTimeoutMs (options override) or the connection is
 * destroyed; once it arrives, onConnection runs with the connection, the
 * first line, and the remaining lines as an async iterable.
 */
async function listen(
	address: PeerSocketAddress,
	onConnection: (conn: PeerConnection, firstLine: string | undefined, lines: AsyncIterable<string>) => void,
	options?: PeerListenOptions,
): Promise<PeerServer> {
	const firstLineTimeoutMs = options?.firstLineTimeoutMs ?? PEER_FIRST_LINE_TIMEOUT_MS;

	const handleConnection = (socket: Socket) => {
		const queue: LineQueue = { lines: [], waiters: [], ended: false };
		attachLineFraming(socket, queue);
		openSockets.add(socket);
		socket.once("close", () => openSockets.delete(socket));

		const timeoutHandle = setTimeout(() => {
			socket.destroy();
		}, firstLineTimeoutMs);
		timeoutHandle.unref();

		void nextLine(queue).then((firstLine) => {
			clearTimeout(timeoutHandle);
			if (firstLine === undefined) {
				return;
			}
			const conn = createConnection(socket, queue, PEER_MACOS_CLOSE_DELAY_MS);
			try {
				onConnection(conn, firstLine, iterateLines(queue));
			} catch {
				socket.destroy();
			}
		});
	};

	// All live inbound sockets, destroyed on close(). net.Server offers no
	// closeAllConnections() on this Node build, so tracking is explicit.
	const openSockets = new Set<Socket>();

	const setupServer = () => {
		const server = createServer();
		// Post-listen server errors (e.g. sockets erroring during close)
		// must not crash the process; startListen installs its own
		// once-handler while binding.
		server.on("error", () => {});
		server.on("connection", handleConnection);
		return server;
	};

	let server = setupServer();
	try {
		await startListen(server, address.path);
	} catch (error) {
		const errno = error as NodeJS.ErrnoException;
		// POSIX EADDRINUSE on a registry-derived path means a stale socket
		// file left by a dead process: remove it and retry once. Any second
		// failure propagates to the caller.
		if (isWin32() || errno.code !== "EADDRINUSE") {
			throw error;
		}
		try {
			unlinkSync(address.path);
		} catch {
			throw error;
		}
		server = setupServer();
		await startListen(server, address.path);
	}

	let closePromise: Promise<void> | undefined;
	const peerServer: PeerServer = {
		address,
		close: () => {
			closePromise ??= (async () => {
				for (const socket of openSockets) {
					socket.destroy();
				}
				await new Promise<void>((resolve) => {
					server.close(() => resolve());
				});
				if (!isWin32()) {
					try {
						unlinkSync(address.path);
					} catch {
						// Socket file already gone: nothing to clean up.
					}
				}
			})();
			return closePromise;
		},
	};
	return peerServer;
}

// ---------------------------------------------------------------------------
// Connect
// ---------------------------------------------------------------------------

/**
 * Connect to a wire address with PEER_CONNECT_TIMEOUT_MS (options override).
 * Throws PeerSendError("invalid_target" | "timeout" | "connect_failed").
 */
async function connect(address: string, options?: PeerConnectOptions): Promise<PeerConnection> {
	const connectTimeoutMs = options?.connectTimeoutMs ?? PEER_CONNECT_TIMEOUT_MS;
	const path = resolveAddress(address);
	if (path === undefined) {
		throw new PeerSendError("invalid_target", `invalid peer socket address: ${address}`);
	}

	const socket = await new Promise<Socket>((resolve, reject) => {
		const sock = netConnect({ path });
		let settled = false;
		const timeoutHandle = setTimeout(() => {
			sock.destroy();
			if (settled) {
				return;
			}
			settled = true;
			reject(new PeerSendError("timeout", `peer connect timed out after ${connectTimeoutMs}ms (${address})`));
		}, connectTimeoutMs);

		const onError = (error: Error) => {
			if (settled) {
				return;
			}
			settled = true;
			clearTimeout(timeoutHandle);
			sock.destroy();
			reject(
				new PeerSendError(
					"connect_failed",
					`could not connect to peer socket ${address}: ${error.message}. ` +
						"The peer process may have exited; re-resolve its address from the peers registry.",
				),
			);
		};

		sock.once("error", onError);
		sock.once("connect", () => {
			if (settled) {
				return;
			}
			settled = true;
			clearTimeout(timeoutHandle);
			// The once-handler above only covers the connect phase. Once it is
			// detached, an error emitted before createConnection's framing
			// attaches its own handler (the await gap below) would crash the
			// process; keep a permanent logging handler on the socket instead.
			sock.off("error", onError);
			sock.on("error", (error: Error) => {
				console.error(`[pi-peers] socket error on ${address}: ${String(error)}`);
			});
			resolve(sock);
		});
	});

	return createConnection(socket, undefined, options?.macosCloseDelayMs ?? PEER_MACOS_CLOSE_DELAY_MS);
}

/** All lines of one connection (from listen or connect) as an async iterable. */
function readLinesInternal(conn: PeerConnection): AsyncIterable<string> {
	const state = connStates.get(conn);
	if (state === undefined) {
		throw new Error("readLines: connection was not created by createPeerTransport()");
	}
	return iterateLines(state.queue);
}

/** Construct the transport. All state lives per-connection; the factory itself is stateless. */
export function createPeerTransport(): PeerTransport {
	return {
		deriveSocketAddress,
		isValidAddress,
		resolveAddress,
		addressFromPath,
		listen,
		connect,
		readLines: readLinesInternal,
	};
}
