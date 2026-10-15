//===----------------------------------------------------------------------===//
// Connections to one Redis server, shared by tasks on any worker.
//
//     let redis = RedisPool(RedisConfiguration(host: "cache"))
//     let visits = try await redis.incr("visits:\(page)")
//
// A connection is handed on only as the handshake left it -- a transaction
// left open, a watch, another database or a subscription closes it instead,
// so the next caller never runs inside somebody else's session.
//===----------------------------------------------------------------------===//

import Vega
import VegaPool
import VegaTLS
import VegaRedisProtocol

/// Connections to one Redis server.
public final class RedisPool: RedisCommandSender, Sendable {
    public let configuration: RedisConfiguration
    private let pool: ConnectionPool<RedisConnection, RedisClientError>
    private let tls: TLSSetup

    public var maxConnections: Int { pool.maxConnections }
    /// How long a command waits for a connection when every one is in use,
    /// before failing with `poolTimedOut`.
    public var acquireTimeout: Duration { pool.acquireTimeout }

    /// `acquireTimeout` defaults to the configuration's `timeout`.
    public init(_ configuration: RedisConfiguration, maxConnections: Int = 8, acquireTimeout: Duration? = nil) {
        let configuration = configuration.tracedByDefault
        let tls = TLSSetup(configuration)
        self.configuration = configuration
        self.tls = tls
        self.pool = ConnectionPool(
            maxConnections: maxConnections,
            acquireTimeout: acquireTimeout ?? configuration.timeout,
            connect: { () async throws(RedisClientError) -> RedisConnection in
                try await RedisConnection.connect(configuration, tls: try tls.context())
            },
            failure: { error in
                switch error {
                case .timedOut: return .poolTimedOut
                case .cancelled: return .cancelled
                case .closed: return .closed
                }
            })
    }

    public func send(_ command: RedisCommand) async throws(RedisClientError) -> RedisValue {
        try await send(command, timeout: nil)
    }

    /// Sends one command, waiting up to `timeout` for its reply: for BLPOP
    /// and the other commands that block, whose wait has to fit.
    public func send(_ command: RedisCommand, timeout: Duration?) async throws(RedisClientError) -> RedisValue {
        let reply = try await pool.withConnection { connection throws(RedisClientError) in
            try await connection.send([command], timeout: timeout)[0]
        }
        if case .error(let error) = reply { throw .server(error) }
        return reply
    }

    /// Sends every command in one write and reads every reply, in order. A
    /// command the server refused is an `.error` among them rather than a
    /// throw, so the others' replies are not lost with it.
    public func pipeline(_ commands: [RedisCommand]) async throws(RedisClientError) -> [RedisValue] {
        try await pipeline(commands, timeout: nil)
    }

    /// Sends every command in one write, waiting up to `timeout` for the
    /// replies.
    public func pipeline(_ commands: [RedisCommand], timeout: Duration?) async throws(RedisClientError) -> [RedisValue] {
        try await pool.withConnection { connection throws(RedisClientError) in
            try await connection.send(commands, timeout: timeout)
        }
    }

    /// Where this pool's server is, as `host:port` -- or the socket path --
    /// which is what a cluster keys its nodes by.
    public var address: String {
        configuration.unixSocketPath ?? "\(configuration.host):\(configuration.port)"
    }

    /// Runs the commands as one transaction -- MULTI, the commands, EXEC -- in
    /// one round trip, and returns their replies. A command refused while it
    /// was queued throws, and none of them ran; one that fails as it runs is
    /// an `.error` among the replies, and the others still ran, as Redis does.
    public func transaction(_ commands: [RedisCommand]) async throws(RedisClientError) -> [RedisValue] {
        let replies = try await pool.withConnection { connection throws(RedisClientError) in
            do throws(RedisClientError) {
                return try await connection.send([RedisCommand("MULTI")] + commands + [RedisCommand("EXEC")])
            } catch {
                throw error.withoutReplies
            }
        }
        // Nothing was watched, so EXEC always runs.
        guard let results = try transactionReplies(replies, commands: commands.count) else {
            throw .unexpectedReply(.null)
        }
        return results
    }

    /// Runs `body` with one connection to itself: for WATCH, then reading,
    /// then a transaction that only runs if nothing watched changed.
    ///
    /// ```
    /// let done = try await redis.session { s in
    ///     try await s.watch("stock:42")
    ///     let stock = Int(try await s.get("stock:42") ?? "0") ?? 0
    ///     guard stock > 0 else { return false }
    ///     return try await s.transaction([RedisCommand("DECR", "stock:42")]) != nil
    /// }
    /// ```
    public func session<Result>(_ body: (RedisSession) async throws -> Result) async throws -> Result {
        try await pool.withConnection { (connection: RedisConnection) async throws -> Result in
            try await body(RedisSession(connection: connection))
        }
    }

    /// Subscribes to channels and patterns on a connection of its own, which
    /// the pool does not count: a subscription holds its connection for as
    /// long as it lasts. The initial subscriptions are confirmed before this
    /// returns.
    public func subscribe(channels: [String] = [], patterns: [String] = []) async throws(RedisClientError) -> RedisSubscription {
        precondition(!channels.isEmpty || !patterns.isEmpty, "subscribe to at least one channel or pattern")
        return try await subscribing(channels: channels, patterns: patterns, sharded: [])
    }

    /// Subscribes to sharded channels, which belong to slots as keys do: in a
    /// cluster a sharded message reaches only the shard that owns it.
    public func subscribeSharded(channels: [String]) async throws(RedisClientError) -> RedisSubscription {
        precondition(!channels.isEmpty, "subscribe to at least one channel")
        return try await subscribing(channels: [], patterns: [], sharded: channels)
    }

    private func subscribing(channels: [String], patterns: [String],
                             sharded: [String]) async throws(RedisClientError) -> RedisSubscription {
        let connection = try await RedisConnection.connect(configuration, tls: try tls.context())
        connection.reusable = false
        let subscription = RedisSubscription(connection: connection)
        do throws(RedisClientError) {
            try await subscription.start(channels: channels, patterns: patterns, sharded: sharded)
        } catch {
            connection.close()
            throw error
        }
        return subscription
    }

    /// Closes every idle connection, and each one in use as it comes back.
    /// Commands sent after this fail with `closed`.
    public func close() {
        pool.close()
    }

    /// For tests: connections idle, and in use.
    var counts: (idle: Int, inUse: Int) { (pool.idleCount, pool.inUseCount) }
}

/// The TLS context a configuration asks for, made once.
private struct TLSSetup: Sendable {
    let result: Result<TLSContext, IOError>?

    init(_ configuration: RedisConfiguration) {
        guard configuration.tls == .require else {
            result = nil
            return
        }
        let roots: TLSConfiguration.TrustRoots = configuration.trustedCertificates.map { .certificates($0) } ?? .system
        do throws(IOError) {
            result = .success(try TLSContext(.client(trustRoots: roots)))
        } catch {
            result = .failure(error)
        }
    }

    func context() throws(RedisClientError) -> TLSContext? {
        switch result {
        case nil: return nil
        case .success(let context): return context
        case .failure(let error): throw .connect(error)
        }
    }
}

/// One connection, for commands that belong together. Valid only inside the
/// `session` call that made it.
public struct RedisSession: RedisCommandSender, @unchecked Sendable {
    let connection: RedisConnection

    /// The protocol the server speaks on this connection: 3, or 2.
    public var protocolVersion: Int { connection.protocolVersion }

    public func send(_ command: RedisCommand) async throws(RedisClientError) -> RedisValue {
        let reply = try await connection.send([command])[0]
        if case .error(let error) = reply { throw .server(error) }
        return reply
    }

    public func pipeline(_ commands: [RedisCommand]) async throws(RedisClientError) -> [RedisValue] {
        try await connection.send(commands)
    }

    /// Watches keys: a transaction on this session after this runs only if
    /// none of them has changed.
    public func watch(_ keys: String...) async throws(RedisClientError) {
        _ = try await send(RedisCommand("WATCH", arguments: keys))
    }

    /// MULTI, the commands and EXEC in one round trip. Nil when a watched key
    /// changed and none of them ran.
    public func transaction(_ commands: [RedisCommand]) async throws(RedisClientError) -> [RedisValue]? {
        let replies: [RedisValue]
        do throws(RedisClientError) {
            replies = try await connection.send([RedisCommand("MULTI")] + commands + [RedisCommand("EXEC")])
        } catch {
            throw error.withoutReplies
        }
        return try transactionReplies(replies, commands: commands.count)
    }
}

// MARK: - Pub/sub

/// A message published on a channel this subscription listens to.
public struct RedisMessage: Sendable, Equatable {
    public let channel: String
    /// The pattern it matched, for a pattern subscription.
    public let pattern: String?
    public let payload: [UInt8]

    public var text: String { String(decoding: payload, as: UTF8.self) }
}

/// Channels and patterns being listened to, on a connection of their own.
///
/// One task reads it at a time. `next` waits at most the time it is given, so
/// a task streaming messages to a client can check the client is still there
/// between waits.
public final class RedisSubscription: @unchecked Sendable {
    let connection: RedisConnection
    private var pending: [RedisMessage] = []
    private var pendingHead = 0

    init(connection: RedisConnection) {
        self.connection = connection
    }

    deinit {
        connection.close()
    }

    public var isOpen: Bool { connection.isOpen }

    func start(channels: [String], patterns: [String], sharded: [String] = []) async throws(RedisClientError) {
        var commands: [RedisCommand] = []
        if !channels.isEmpty { commands.append(RedisCommand("SUBSCRIBE", arguments: channels)) }
        if !patterns.isEmpty { commands.append(RedisCommand("PSUBSCRIBE", arguments: patterns)) }
        if !sharded.isEmpty { commands.append(RedisCommand("SSUBSCRIBE", arguments: sharded)) }
        try await connection.write(commands)
        var confirmed = 0
        let expected = channels.count + patterns.count + sharded.count
        let deadline = ContinuousClock.now + connection.configuration.timeout
        while confirmed < expected {
            let left = ContinuousClock.now.duration(to: deadline)
            guard left > .zero else {
                connection.close()
                throw .timedOut
            }
            let value = try await read(left)
            if case .error(let error) = value {
                connection.close()
                throw .server(error)
            }
            switch classify(value) {
            case .message(let message): pending.append(message)
            case .confirmation(let kind):
                if kind == "subscribe" || kind == "psubscribe" || kind == "ssubscribe" { confirmed += 1 }
            case .other: continue
            }
        }
    }

    /// The next message, waiting at most `timeout`; nil when none came in
    /// time. Throws once the connection has gone.
    public func next(timeout: Duration = .seconds(1)) async throws(RedisClientError) -> RedisMessage? {
        if pendingHead < pending.count {
            let message = pending[pendingHead]
            pendingHead += 1
            if pendingHead == pending.count {
                pending.removeAll(keepingCapacity: true)
                pendingHead = 0
            }
            return message
        }
        let deadline = ContinuousClock.now + timeout
        while true {
            let left = ContinuousClock.now.duration(to: deadline)
            guard left > .zero else { return nil }
            let value: RedisValue
            do {
                value = try await read(left)
            } catch .timedOut {
                return nil
            }
            if case .error(let error) = value { throw .server(error) }
            if case .message(let message) = classify(value) { return message }
        }
    }

    /// Listens to more channels. Confirmed as messages go by, not here.
    public func subscribe(_ channels: String...) async throws(RedisClientError) {
        try await connection.write([RedisCommand("SUBSCRIBE", arguments: channels)])
    }

    public func unsubscribe(_ channels: String...) async throws(RedisClientError) {
        try await connection.write([RedisCommand("UNSUBSCRIBE", arguments: channels)])
    }

    public func psubscribe(_ patterns: String...) async throws(RedisClientError) {
        try await connection.write([RedisCommand("PSUBSCRIBE", arguments: patterns)])
    }

    public func punsubscribe(_ patterns: String...) async throws(RedisClientError) {
        try await connection.write([RedisCommand("PUNSUBSCRIBE", arguments: patterns)])
    }

    /// Listens to more sharded channels. In a cluster they have to be in the
    /// slot this subscription's node owns.
    public func ssubscribe(_ channels: String...) async throws(RedisClientError) {
        try await connection.write([RedisCommand("SSUBSCRIBE", arguments: channels)])
    }

    public func sunsubscribe(_ channels: String...) async throws(RedisClientError) {
        try await connection.write([RedisCommand("SUNSUBSCRIBE", arguments: channels)])
    }

    /// Ends the subscription and closes its connection.
    public func close() {
        connection.close()
    }

    private func read(_ timeout: Duration) async throws(RedisClientError) -> RedisValue {
        do {
            return try await connection.next(timeout: timeout)
        } catch .timedOut {
            throw .timedOut
        } catch .unknownOutcome(.timedOut) {
            throw .timedOut
        } catch {
            // Anything but a quiet channel ends the subscription.
            connection.close()
            throw error
        }
    }

    private enum Kind {
        case message(RedisMessage)
        case confirmation(String)
        case other
    }

    /// A push over RESP3, an array over RESP2: `message channel payload`,
    /// `pmessage pattern channel payload`, or a confirmation.
    private func classify(_ value: RedisValue) -> Kind {
        guard let elements = value.array, let first = elements.first?.string else { return .other }
        let kind = first.lowercased()
        switch kind {
        case "message", "smessage":
            guard elements.count == 3, let channel = elements[1].string, let payload = elements[2].bytes else {
                return .other
            }
            return .message(RedisMessage(channel: channel, pattern: nil, payload: payload))
        case "pmessage":
            guard elements.count == 4, let pattern = elements[1].string, let channel = elements[2].string,
                  let payload = elements[3].bytes else { return .other }
            return .message(RedisMessage(channel: channel, pattern: pattern, payload: payload))
        case "subscribe", "psubscribe", "unsubscribe", "punsubscribe", "ssubscribe", "sunsubscribe":
            return .confirmation(kind)
        default:
            return .other
        }
    }
}
