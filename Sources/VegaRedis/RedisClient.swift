//===----------------------------------------------------------------------===//
// Redis on Vega: what a client is configured with, and how it fails.
//
// From Garuda's Redis driver. The protocol is VegaRedisProtocol's: replies
// parsed and commands written there, with no socket in sight. Works with
// Redis 6 and later over RESP3, older servers over RESP2, and Valkey.
//===----------------------------------------------------------------------===//

import Vega
// Its values and commands are part of this API, so `import VegaRedis` is enough.
@_exported import VegaRedisProtocol
// So is the tracing hook a configuration takes.
@_exported import VegaDBTracing

/// Where a Redis server is and how to reach it.
public struct RedisConfiguration: Sendable {
    public enum TLS: Sendable {
        /// Plaintext. For a server on this machine, or a network nobody else is on.
        case disable
        /// Encrypted and verified, or not at all.
        case require
    }

    public var host: String
    public var port: UInt16
    /// A unix socket to connect to instead of `host` and `port`.
    public var unixSocketPath: String?
    /// An ACL user. Nil authenticates as `default` when there is a password.
    public var username: String?
    public var password: String?
    public var database: Int = 0
    /// `.require` unless changed, as for PostgreSQL: a client that falls back
    /// to plaintext when TLS is refused lets anyone on the path refuse it.
    public var tls: TLS = .require
    /// The certificates to trust for `.require`, as PEM. Nil means the
    /// system's.
    public var trustedCertificates: String? = nil
    /// The name `CLIENT LIST` shows for this server's connections.
    public var clientName: String? = nil
    /// How long connecting, and each command, may take.
    public var timeout: Duration = .seconds(10)
    /// The largest string a reply may carry, and the most elements in one.
    public var maxBulkBytes = 64 * 1024 * 1024
    public var maxReplyElements = 1_000_000
    /// Told of every round trip -- a command, a pipeline, a transaction --
    /// with its command's name and none of its arguments.
    public var tracer: (any DatabaseTracer)? = nil

    public init(host: String, port: UInt16 = 6379, password: String? = nil) {
        self.host = host
        self.port = port
        self.password = password
    }

    public init(unixSocketPath: String, password: String? = nil) {
        self.host = unixSocketPath
        self.port = 0
        self.unixSocketPath = unixSocketPath
        self.password = password
        self.tls = .disable
    }

    var limits: RedisParser.Limits {
        var limits = RedisParser.Limits()
        limits.maxBulkBytes = maxBulkBytes
        limits.maxElements = maxReplyElements
        return limits
    }
}

extension RedisConfiguration {
    /// A round trip, for a tracer: named for its command, or PIPELINE for
    /// several sent together. Arguments -- keys and values alike -- are not
    /// recorded.
    func describe(_ commands: [RedisCommand]) -> DatabaseOperation {
        let name = commands.count == 1 ? commands[0].name : "PIPELINE"
        var operation = DatabaseOperation(system: "redis", name: name)
        operation.operation = name
        if commands.count > 1 { operation.batchSize = commands.count }
        operation.namespace = String(database)
        if let unixSocketPath {
            operation.serverAddress = unixSocketPath
        } else {
            operation.serverAddress = host
            operation.serverPort = Int(port)
        }
        return operation
    }
}

/// Why a Redis operation did not complete.
public enum RedisClientError: Error, Equatable {
    /// The connection could not be made: refused, unreachable, or TLS failed.
    case connect(IOError)
    /// The server would not authenticate, or refused the database.
    case handshake(RedisHandshakeError)
    /// The server went away, or the connection can no longer be trusted.
    case closed
    case timedOut
    /// Every connection in the pool stayed in use for the pool's
    /// `acquireTimeout`.
    case poolTimedOut
    case cancelled
    /// A reply that is not RESP, or past a limit.
    case protocolViolation(RedisProtocolError)
    /// The server refused the command: `WRONGTYPE`, `NOPERM`, `OOM`. The
    /// connection is fine.
    case server(RedisServerError)
    /// A reply that is not the shape the method asked for.
    case unexpectedReply(RedisValue)
    /// The connection failed with the command's bytes already written, so
    /// whether the server ran it is not known. A `SET` sent again would be
    /// the same `SET`; an `INCR` sent again would count twice, and a lost
    /// reply to `EXEC` does not mean the transaction was rolled back. So
    /// this is where a retry stops and the caller decides.
    indirect case unknownOutcome(RedisClientError)
    /// A batch failed part-way. `replies` holds the answer to each command
    /// that was answered, at its place in the batch, and nil for each that
    /// was not; the error is why the rest failed, `unknownOutcome` when they
    /// may have run. What was answered was settled -- run, or refused -- so
    /// sending the whole batch again would repeat it: send the rest, if
    /// anything.
    indirect case incomplete(replies: [RedisValue?], RedisClientError)
}

extension RedisClientError {
    /// What went wrong, with `unknownOutcome` taken off: the connection
    /// closed, or timed out, or whatever it was.
    public var cause: RedisClientError {
        switch self {
        case .unknownOutcome(let inner): return inner.cause
        case .incomplete(_, let inner): return inner.cause
        default: return self
        }
    }

    /// Whether the command -- or any command of the batch -- may already have
    /// run. Nothing else is known about it: it is not that it did, and not
    /// that it did not. A batch part-answered may have run if anything in it
    /// was answered with other than a refusal.
    public var mayHaveRun: Bool {
        switch self {
        case .unknownOutcome:
            return true
        case .incomplete(let replies, let inner):
            if inner.mayHaveRun { return true }
            return replies.contains { reply in
                guard let reply else { return false }
                if case .error = reply { return false }
                return true
            }
        default:
            return false
        }
    }

    /// The failure without the part-answers, for a transaction: MULTI's OK
    /// and a QUEUED are not answers anyone asked for, and a transaction ran
    /// whole or not at all.
    var withoutReplies: RedisClientError {
        if case .incomplete(_, let inner) = self { return inner }
        return self
    }

    /// `error`, carrying `replies` if any of them were answered.
    static func settled(_ replies: [RedisValue?], _ error: RedisClientError) -> RedisClientError {
        guard replies.contains(where: { $0 != nil }) else { return error }
        return .incomplete(replies: replies, error)
    }

    /// The same failure, marked as having happened after the bytes went out
    /// -- if it is one where that makes the outcome unknowable.
    ///
    /// A connection that closed or timed out with the command written is the
    /// case this is for. A reply that could not be read, or one the server
    /// refused, is not: the server answered, so the command ran, and what is
    /// unknown is only what it said about it.
    func afterSending() -> RedisClientError {
        switch self {
        case .closed, .timedOut, .cancelled: return .unknownOutcome(self)
        default: return self
        }
    }
}

extension RedisClientError {
    /// The failure, for a tracer: with the server's code -- WRONGTYPE, MOVED
    /// -- when it was the server that refused.
    var failure: DatabaseFailure {
        if case .server(let refused) = self, !refused.code.isEmpty {
            return DatabaseFailure(self, type: refused.code, statusCode: refused.code)
        }
        return DatabaseFailure(self, type: DatabaseFailure.caseName(of: self))
    }
}

/// What may be sent again after a failure that leaves it unknown whether the
/// server ran the command.
///
/// This is only about the uncertain case. A command that never reached the
/// server -- the connection refused, the pool timed out, the write failed on
/// its first byte -- is always sent again, whatever this says, because
/// sending it again cannot repeat anything.
public enum RedisReplay: Sendable, Equatable {
    /// Commands that only read. A write whose outcome is unknown is reported
    /// as `unknownOutcome` rather than repeated. The default.
    case reads
    /// Everything, for a cache where doing a write twice costs nothing.
    case anything
    /// Nothing: any failure after the bytes went out is the caller's.
    case nothing
}

extension RedisReplay {
    /// Whether `commands` may be sent again after `error`.
    func allows(_ error: RedisClientError, _ commands: [RedisCommand]) -> Bool {
        guard error.mayHaveRun else { return true }
        switch self {
        case .anything: return true
        case .nothing: return false
        case .reads: return RedisReads.only(commands)
        }
    }
}

extension RedisConfiguration {
    /// This configuration, with `DatabaseTracing.installed` as its tracer
    /// when it names none.
    var tracedByDefault: RedisConfiguration {
        var configuration = self
        configuration.tracer = DatabaseTracing.resolve(tracer)
        return configuration
    }
}
