//===----------------------------------------------------------------------===//
// Connections to one server, shared by tasks on any worker.
//
// A semaphore holds the pool's capacity and a locked stack its idle
// connections. A task takes a permit -- at once if one is free and nobody is
// queued, else in FIFO order, up to the acquire timeout -- then an idle
// connection, or opens one. Releasing pushes the connection back before the
// permit, so the task the permit wakes finds it.
//
// Garuda's pools assumed one thread per process and kept their state in plain
// vars. Here any worker may acquire and release, so the state is locked, and
// the lock is held only to push and pop: checking a connection, opening and
// closing one all happen outside it.
//===----------------------------------------------------------------------===//

import Synchronization
import Vega

/// A connection a `ConnectionPool` can hold.
///
/// Sendable because the pool hands it from task to task. One task uses it at a
/// time: the one that acquired it, until it releases it.
public protocol PoolableConnection: AnyObject, Sendable {
    /// Whether the next caller can use it as it is: still open, and in the
    /// state a new connection starts in. Asked when it comes back, and again
    /// before an idle one is handed out, so it can look at the socket.
    var isReusable: Bool { get }

    /// Closes it. The pool calls this once, for a connection it drops.
    func close()
}

/// Why a pool did not hand out a connection.
public enum PoolError: Error, Sendable, Equatable {
    /// Every connection stayed in use for the pool's acquire timeout.
    case timedOut
    /// The pool was closed.
    case closed
    /// The waiting task was cancelled.
    case cancelled
}

/// Connections to one server, shared by tasks on any worker.
///
/// `connect` opens a connection, failing with the client's own error type;
/// `failure` turns the pool's own reasons into that type, so every method
/// throws one error.
public final class ConnectionPool<Connection: PoolableConnection, Failure: Error>: Sendable {
    public let maxConnections: Int
    /// How long `acquire` waits for a connection when every one is in use.
    public let acquireTimeout: Duration

    private let permits: Semaphore
    private let idle = Mutex<[Connection]>([])
    private let closed = Atomic<Bool>(false)
    private let connect: @Sendable () async throws(Failure) -> Connection
    private let failure: @Sendable (PoolError) -> Failure

    public init(maxConnections: Int, acquireTimeout: Duration,
                connect: @escaping @Sendable () async throws(Failure) -> Connection,
                failure: @escaping @Sendable (PoolError) -> Failure) {
        precondition(maxConnections > 0, "a pool needs room for at least one connection")
        self.maxConnections = maxConnections
        self.acquireTimeout = acquireTimeout
        self.permits = Semaphore(permits: maxConnections)
        self.connect = connect
        self.failure = failure
    }

    /// Runs `body` with a connection, then gives it back. A connection that
    /// `body` left unusable (`isReusable` false) is closed instead.
    public func withConnection<R>(_ body: (Connection) async throws(Failure) -> R) async throws(Failure) -> R {
        let connection = try await acquire()
        do {
            let result = try await body(connection)
            release(connection)
            return result
        } catch {
            release(connection)
            throw error
        }
    }

    /// The same, for a body that throws anything.
    public func withConnection<R>(_ body: (Connection) async throws -> R) async throws -> R {
        let connection = try await acquire()
        do {
            let result = try await body(connection)
            release(connection)
            return result
        } catch {
            release(connection)
            throw error
        }
    }

    /// Takes a connection: an idle one, or a new one if there is room, or the
    /// next one released. Give it back with `release`, exactly once.
    public func acquire() async throws(Failure) -> Connection {
        guard !closed.load(ordering: .acquiring) else { throw failure(.closed) }
        try await takePermit()
        while let connection = popIdle() {
            if connection.isReusable { return connection }
            connection.close()
        }
        do {
            return try await connect()
        } catch {
            permits.release()
            throw error
        }
    }

    /// Gives back a connection from `acquire`.
    public func release(_ connection: Connection) {
        if !closed.load(ordering: .acquiring) && connection.isReusable {
            idle.withLock { $0.append(connection) }
            // Closed in between: the close may have missed it.
            if closed.load(ordering: .acquiring) { drainIdle() }
        } else {
            connection.close()
        }
        permits.release()
    }

    /// Closes every idle connection and fails every wait, now and later.
    /// Connections in use are closed as they come back.
    public func close() {
        closed.store(true, ordering: .releasing)
        permits.close()
        drainIdle()
    }

    public var isClosed: Bool { closed.load(ordering: .acquiring) }

    /// For tests and metrics: idle connections, and connections in use or
    /// being opened.
    public var idleCount: Int { idle.withLock { $0.count } }
    public var inUseCount: Int { maxConnections - permits.availablePermits }

    // MARK: -

    private func takePermit() async throws(Failure) {
        if permits.tryAcquire() { return }
        let permits = self.permits
        do {
            try await Runtime.timeout(acquireTimeout) { try await permits.acquire() }
        } catch is TimeoutError {
            throw failure(.timedOut)
        } catch AcquireError.closed {
            throw failure(.closed)
        } catch {
            throw failure(.cancelled)
        }
    }

    private func popIdle() -> Connection? {
        idle.withLock { $0.popLast() }
    }

    private func drainIdle() {
        let dropped = idle.withLock { connections in
            defer { connections.removeAll() }
            return connections
        }
        for connection in dropped { connection.close() }
    }
}
