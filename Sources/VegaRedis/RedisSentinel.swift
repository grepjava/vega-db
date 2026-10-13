//===----------------------------------------------------------------------===//
// Sentinel: a master, its replicas, and a quorum of sentinels that promote a
// replica when the master goes.
//
//     let sentinels = [RedisConfiguration(host: "s1", port: 26379),
//                      RedisConfiguration(host: "s2", port: 26379)]
//     let server = RedisConfiguration(host: "", password: secret)   // the master's own auth
//     let redis = RedisSentinelPool(RedisSentinelConfiguration(sentinels: sentinels,
//                                                              master: "cache", server: server))
//
// Where the master is is not configuration: it is asked of the sentinels, and
// it changes. So this is a pool whose address is discovered, kept while it
// works, and asked for again the moment it stops -- when the connection goes,
// or when the server answers READONLY, which is what a master that has been
// demoted says to a write. From Garuda's RedisSentinelPool.
//
// A sentinel can be behind the times and name a node that is no longer the
// master, so what it names is asked `ROLE` before anything is sent to it.
// Nothing is taken on trust that one round trip can settle.
//===----------------------------------------------------------------------===//

import Synchronization
import Vega

/// Which sentinels watch which master, and how to talk to the master itself.
public struct RedisSentinelConfiguration: Sendable {
    /// Where the sentinels are. A sentinel has its own password, if any, and
    /// is not the master: what it says here is only where to go.
    public var sentinels: [RedisConfiguration]
    /// The name the sentinels know this master by.
    public var master: String
    /// How to talk to the master once it is found: its password, its TLS, its
    /// timeouts. The host and port in it are ignored -- the sentinels say
    /// those.
    public var server: RedisConfiguration

    public init(sentinels: [RedisConfiguration], master: String, server: RedisConfiguration) {
        precondition(!sentinels.isEmpty, "a sentinel pool needs at least one sentinel to ask")
        precondition(!master.isEmpty, "a sentinel pool needs the name the master is monitored under")
        self.sentinels = sentinels
        self.master = master
        self.server = server
    }
}

/// A pool to whichever server the sentinels call the master.
public final class RedisSentinelPool: RedisCommandSender, Sendable {
    public let configuration: RedisSentinelConfiguration
    public let maxConnections: Int
    public let acquireTimeout: Duration
    /// How many times a command is tried against a freshly found master
    /// before it gives up: a failover takes a moment, and during it there is
    /// no master to be had.
    public let maxAttempts: Int
    /// What may be sent to the new master when the old one failed with the
    /// command already written. Reads, by default: a failover is exactly when
    /// a write's fate is least knowable.
    public let replay: RedisReplay

    private let state = Mutex(State())

    private struct State {
        var pool: RedisPool? = nil
        var found: String? = nil
        /// The sentinel that answered last, asked first next time.
        var preferred = 0
    }

    /// `acquireTimeout` defaults to the server configuration's `timeout`.
    public init(_ configuration: RedisSentinelConfiguration, maxConnections: Int = 8,
                acquireTimeout: Duration? = nil, maxAttempts: Int = 4, replay: RedisReplay = .reads) {
        precondition(maxAttempts > 0, "a command is tried at least once")
        self.configuration = configuration
        self.maxConnections = maxConnections
        self.acquireTimeout = acquireTimeout ?? configuration.server.timeout
        self.maxAttempts = maxAttempts
        self.replay = replay
    }

    /// Where the master was last found, as `host:port`, or nil before anyone
    /// has asked.
    public var masterAddress: String? { state.withLock { $0.found } }

    // MARK: Commands

    public func send(_ command: RedisCommand) async throws(RedisClientError) -> RedisValue {
        try await send(command, timeout: nil)
    }

    /// Sends one command, waiting up to `timeout` for its reply.
    public func send(_ command: RedisCommand, timeout: Duration?) async throws(RedisClientError) -> RedisValue {
        try await attempting([command]) { pool throws(RedisClientError) in
            try await pool.send(command, timeout: timeout)
        }
    }

    /// Sends every command in one write to the master, and reads every reply.
    ///
    /// A failover can land in the middle of a batch: the commands before it
    /// are answered and the writes after it are refused with READONLY. Only
    /// the refused ones go to the new master. A refusal is proof that the
    /// command did not run, and an answer is proof that it did -- sending the
    /// whole batch again would repeat everything that already happened.
    ///
    /// When a later attempt fails, what earlier ones answered is not lost:
    /// the error is `incomplete`, with those replies.
    public func pipeline(_ commands: [RedisCommand]) async throws(RedisClientError) -> [RedisValue] {
        guard !commands.isEmpty else { return [] }
        var replies = [RedisValue?](repeating: nil, count: commands.count)
        var outstanding = Array(commands.indices)
        var attempt = 0
        while true {
            attempt += 1
            // Going again, so not answered yet: a refusal kept here would
            // read as the answer if this attempt failed.
            for index in outstanding { replies[index] = nil }
            let batch = outstanding.map { commands[$0] }
            var failed: RedisPool? = nil
            do throws(RedisClientError) {
                let pool = try await master()
                failed = pool
                let answers = try await pool.pipeline(batch)
                guard answers.count == batch.count else { throw RedisClientError.unexpectedReply(.array(answers)) }
                var again: [Int] = []
                for (n, index) in outstanding.enumerated() {
                    replies[index] = answers[n]
                    if isFailover(answers[n]) { again.append(index) }
                }
                guard !again.isEmpty, attempt < maxAttempts else { return replies.map { $0 ?? .null } }
                outstanding = again
            } catch {
                // What this master answered before it failed stands; only
                // what it did not answer is in question, and what it refused
                // goes again whatever `replay` says.
                var failure = error
                var refused: [Int] = []
                var unanswered = outstanding
                if case .incomplete(let got, let inner) = error, got.count == outstanding.count {
                    unanswered = []
                    for (n, index) in outstanding.enumerated() {
                        guard let reply = got[n] else {
                            unanswered.append(index)
                            continue
                        }
                        replies[index] = reply
                        if isFailover(reply) { refused.append(index) }
                    }
                    failure = inner
                }
                guard attempt < maxAttempts, isFailover(failure),
                      replay.allows(failure, unanswered.map { commands[$0] }) else {
                    throw .settled(replies, failure)
                }
                outstanding = (refused + unanswered).sorted()
            }
            // Either the master has gone or it is not the master any more.
            // Both are answered by asking the sentinels.
            forget(failed)
            try? await Runtime.sleep(for: .milliseconds(100 * attempt))
        }
    }

    /// Runs the commands as one transaction on the master.
    public func transaction(_ commands: [RedisCommand]) async throws(RedisClientError) -> [RedisValue] {
        try await attempting([RedisCommand("MULTI")] + commands + [RedisCommand("EXEC")]) { pool throws(RedisClientError) in
            try await pool.transaction(commands)
        }
    }

    /// Runs `body` with one connection to the master: for WATCH, then reading,
    /// then a transaction that only runs if nothing watched changed.
    ///
    /// A failover in the middle of a session is the session's to deal with,
    /// not this pool's: what it has read may be from a server that is no
    /// longer the master, so it is reported rather than tried again.
    public func session<Result>(_ body: (RedisSession) async throws -> Result) async throws -> Result {
        try await master().session(body)
    }

    /// Subscribes on a connection of its own to the master.
    public func subscribe(channels: [String] = [],
                          patterns: [String] = []) async throws(RedisClientError) -> RedisSubscription {
        try await master().subscribe(channels: channels, patterns: patterns)
    }

    /// Asks the sentinels again, whatever the last answer was, and returns
    /// where the master is.
    @discardableResult
    public func refresh() async throws(RedisClientError) -> String {
        forget(nil)
        _ = try await master()
        return masterAddress ?? ""
    }

    /// Closes the master's idle connections, and each one in use as it comes
    /// back.
    public func close() {
        forget(nil)
    }

    // MARK: Finding the master

    private func attempting<R>(_ commands: [RedisCommand],
                               _ body: (RedisPool) async throws(RedisClientError) -> R) async throws(RedisClientError) -> R {
        var attempt = 0
        while true {
            attempt += 1
            let pool = try await master()
            do throws(RedisClientError) {
                return try await body(pool)
            } catch {
                guard attempt < maxAttempts, isFailover(error), replay.allows(error, commands) else { throw error }
                // Either the master has gone or it is not the master any
                // more. Both are answered by asking the sentinels.
                forget(pool)
                try? await Runtime.sleep(for: .milliseconds(100 * attempt))
            }
        }
    }

    /// The pool to the master, asking the sentinels when there is none. Two
    /// tasks that both find none both ask; the first to finish is kept, and
    /// the other's pool is closed.
    private func master() async throws(RedisClientError) -> RedisPool {
        let (current, preferred) = state.withLock { ($0.pool, $0.preferred) }
        if let current { return current }
        var last: RedisClientError? = nil
        let sentinels = configuration.sentinels
        for i in 0..<sentinels.count {
            let index = (preferred + i) % sentinels.count
            do throws(RedisClientError) {
                let address = try await ask(sentinels[index])
                let candidate = RedisPool(settings(for: address), maxConnections: maxConnections,
                                          acquireTimeout: acquireTimeout)
                do throws(RedisClientError) {
                    // A sentinel can be behind and name a node that has been
                    // demoted. The node itself is the authority on that.
                    let role = try await candidate.send(RedisCommand("ROLE"))
                    guard RedisSentinelPool.isMaster(role) else {
                        candidate.close()
                        last = .server(RedisServerError("ERR \(address) is not the master any more"))
                        continue
                    }
                } catch {
                    candidate.close()
                    last = error
                    continue
                }
                let kept = state.withLock { state -> RedisPool in
                    if let pool = state.pool { return pool }
                    state.preferred = index
                    state.pool = candidate
                    state.found = address
                    return candidate
                }
                if kept !== candidate { candidate.close() }
                return kept
            } catch {
                last = error
            }
        }
        throw last ?? .closed
    }

    /// `SENTINEL get-master-addr-by-name <name>`: a host and a port, or a null
    /// array for a master this sentinel does not watch.
    private func ask(_ sentinel: RedisConfiguration) async throws(RedisClientError) -> String {
        let asking = RedisPool(sentinel, maxConnections: 1)
        defer { asking.close() }
        let reply = try await asking.send(RedisCommand("SENTINEL", "get-master-addr-by-name", configuration.master))
        guard let address = RedisSentinelPool.parseAddress(reply) else { throw .unexpectedReply(reply) }
        return address
    }

    /// The two-element reply as `host:port`, or nil when it is not one.
    static func parseAddress(_ reply: RedisValue) -> String? {
        guard let parts = reply.array, parts.count == 2,
              let host = parts[0].string, !host.isEmpty,
              let port = parts[1].string, let number = UInt16(port), number > 0 else { return nil }
        return "\(host):\(number)"
    }

    /// `ROLE`: the first element is `master` on one, `slave` on a replica.
    static func isMaster(_ reply: RedisValue) -> Bool {
        guard let parts = reply.array, let role = parts.first?.string else { return false }
        return role == "master"
    }

    private func settings(for address: String) -> RedisConfiguration {
        var settled = configuration.server
        settled.unixSocketPath = nil
        guard let colon = address.lastIndex(of: ":") else {
            settled.host = address
            return settled
        }
        settled.host = String(address[address.startIndex..<colon])
        settled.port = UInt16(address[address.index(after: colon)...]) ?? settled.port
        return settled
    }

    /// Drops the master's pool: `failed`, if it is still the one in use
    /// (another task may have found the new master already), or whatever is
    /// in use when nil.
    private func forget(_ failed: RedisPool?) {
        let dropped = state.withLock { state -> RedisPool? in
            guard let pool = state.pool, failed == nil || pool === failed else { return nil }
            state.pool = nil
            state.found = nil
            return pool
        }
        dropped?.close()
    }

    /// Whether a failure means the master has moved rather than the command
    /// being wrong.
    private func isFailover(_ error: RedisClientError) -> Bool {
        switch error.cause {
        case .connect, .closed, .timedOut, .poolTimedOut:
            return true
        case .server(let error):
            // READONLY: this was the master and is now a replica. MASTERDOWN
            // and LOADING: it is not ready to be one.
            return error.code == "READONLY" || error.code == "MASTERDOWN" || error.code == "LOADING"
        default:
            return false
        }
    }

    /// The same, for a reply among a pipeline's rather than a throw.
    private func isFailover(_ reply: RedisValue) -> Bool {
        guard case .error(let error) = reply else { return false }
        return error.code == "READONLY" || error.code == "MASTERDOWN" || error.code == "LOADING"
    }
}
