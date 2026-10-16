//===----------------------------------------------------------------------===//
// Connections to one server, shared by tasks on any worker.
//
// A semaphore holds the pool's capacity and locked stacks, one for each
// worker that gives connections back, its idle connections. A task takes a permit -- at once if one is free and nobody is
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
    /// Idle connections, a stack for each worker that releases them: the
    /// worker giving one back pushes to its own, and the task its permit
    /// wakes, which resumes on that worker, pops from it. With one stack for
    /// every worker, each release and acquire met the other workers at the
    /// lock, and a contended Mutex is a futex call each way.
    private let idle: [IdleStack]
    private final class IdleStack: Sendable {
        let connections = Mutex<[Connection]>([])
    }
    private static var stacks: Int { 16 }
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
        self.idle = (0..<Self.stacks).map { _ in IdleStack() }
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
            idle[home].connections.withLock { $0.append(connection) }
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
    public var idleCount: Int { idle.reduce(0) { n, stack in n + stack.connections.withLock { $0.count } } }
    public var inUseCount: Int { maxConnections - permits.availablePermits }

    // MARK: -

    private func takePermit() async throws(Failure) {
        if permits.tryAcquire() { return }
        // Not `Runtime.timeout`: that runs the wait as a child task in a
        // group of its own, on every acquire the pool can't serve at once.
        do throws(AcquireError) {
            try await permits.acquire(timeout: acquireTimeout)
        } catch .timedOut {
            throw failure(.timedOut)
        } catch .closed {
            throw failure(.closed)
        } catch {
            throw failure(.cancelled)
        }
    }

    /// The stack of the worker running the caller (the first, off a worker).
    private var home: Int { (Runtime.currentWorker ?? 0) % Self.stacks }

    /// The most recently released idle connection on this worker's stack, or
    /// else on the next stack that has one.
    private func popIdle() -> Connection? {
        let first = home
        for i in 0..<Self.stacks {
            if let connection = idle[(first + i) % Self.stacks].connections.withLock({ $0.popLast() }) {
                return connection
            }
        }
        return nil
    }

    private func drainIdle() {
        for stack in idle {
            let dropped = stack.connections.withLock { connections in
                defer { connections.removeAll() }
                return connections
            }
            for connection in dropped { connection.close() }
        }
    }
}
