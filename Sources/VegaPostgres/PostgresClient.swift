//===----------------------------------------------------------------------===//
// PostgreSQL on Vega: what a client is configured with, and how it fails.
//
// From Garuda's PostgreSQL driver. The protocol is VegaPostgresProtocol's:
// messages framed and interpreted there, with no socket in sight.
//===----------------------------------------------------------------------===//

import Vega
// Its values, rows and errors are part of this API, so `import VegaPostgres`
// is enough.
@_exported import VegaPostgresProtocol
// So is the tracing hook a configuration takes.
@_exported import VegaDBTracing

/// Where a PostgreSQL server is and how to reach it.
public struct PostgresConfiguration: Sendable {
    public enum TLS: Sendable {
        /// Plaintext. For a server on this machine, or a network nobody else is on.
        case disable
        /// Encrypted and verified, or not at all.
        case require
    }

    public var host: String
    public var port: UInt16
    /// A unix socket to connect to instead of `host` and `port`: either the
    /// socket itself, or the directory PostgreSQL keeps it in, which is what
    /// `unix_socket_directories` names and what libpq's `host=/...` means.
    public var unixSocketPath: String?
    public var user: String
    public var password: String
    public var database: String?
    /// `.require` unless changed. There is no "prefer": trying TLS and falling
    /// back to plaintext when the server declines lets anyone on the path
    /// decline for it.
    public var tls: TLS = .require
    /// The certificates to trust for `.require`, as PEM. Nil means the
    /// system's.
    public var trustedCertificates: String? = nil
    /// Allows a cleartext password -- over TLS only, whatever this says.
    public var allowCleartextPassword = false
    /// How long connecting, and each statement, may take.
    public var timeout: Duration = .seconds(10)
    /// Rows a single result may hold before the query fails.
    public var maxRows = 1_000_000
    /// The largest single message the server may send.
    public var maxMessageBytes = 64 * 1024 * 1024
    /// Statements each connection keeps prepared, by SQL, so a statement run
    /// again skips being parsed and planned. 0 prepares nothing, for a
    /// transaction-pooling proxy such as PgBouncer, where the next statement
    /// may reach a server session that never saw the first.
    public var statementCacheCapacity = 256
    /// Told of every statement, with its SQL and none of its values.
    public var tracer: (any DatabaseTracer)? = DatabaseTracing.installed

    public init(host: String, port: UInt16 = 5432, user: String, password: String,
                database: String? = nil) {
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
    }

    /// A server on this machine, over a unix socket.
    ///
    /// TLS is `.disable`, as it is for libpq: a socket has no network for
    /// anyone to be on. `peer` and `trust` authentication need no password,
    /// so it is empty unless given.
    public init(unixSocketPath: String, user: String, password: String = "",
                database: String? = nil, port: UInt16 = 5432) {
        self.host = unixSocketPath
        self.port = port
        self.unixSocketPath = unixSocketPath
        self.user = user
        self.password = password
        self.database = database
        self.tls = .disable
    }

    /// The socket to connect to: the path as given when it names the socket
    /// itself, and the directory's `.s.PGSQL.<port>` when it names a
    /// directory -- which is how PostgreSQL names its own.
    var socketPath: String? {
        guard let path = unixSocketPath else { return nil }
        if path.contains(".s.PGSQL.") { return path }
        let base = path.hasSuffix("/") ? String(path.dropLast()) : path
        return "\(base)/.s.PGSQL.\(port)"
    }

    /// A statement, for a tracer: named for what it does -- its first
    /// keyword -- with its SQL as written. Values are sent apart from the SQL
    /// and never recorded.
    func describe(_ sql: String) -> DatabaseOperation {
        let operation = sqlOperation(sql)
        var described = DatabaseOperation(system: "postgresql", name: operation ?? "postgresql")
        described.operation = operation
        described.queryText = sql
        described.namespace = database
        if let unixSocketPath {
            described.serverAddress = unixSocketPath
        } else {
            described.serverAddress = host
            described.serverPort = Int(port)
        }
        return described
    }
}

/// The first word of a statement, upper-cased: SELECT, INSERT, WITH, BEGIN.
func sqlOperation(_ sql: String) -> String? {
    var word: [UInt8] = []
    for byte in sql.utf8 {
        let letter = (byte >= 0x41 && byte <= 0x5A) || (byte >= 0x61 && byte <= 0x7A)
        if letter {
            word.append(byte & 0xDF)
            if word.count > 16 { return nil }
        } else if !word.isEmpty {
            break
        } else if byte != 0x20 && byte != 0x09 && byte != 0x0A && byte != 0x0D && byte != 0x28 {
            return nil
        }
    }
    return word.isEmpty ? nil : String(decoding: word, as: UTF8.self)
}

/// Why a PostgreSQL operation did not complete.
public enum PostgresClientError: Error, Equatable {
    /// The connection could not be made: refused, unreachable, or TLS failed.
    case connect(IOError)
    /// TLS was required and the server does not offer it.
    case tlsUnavailable
    /// The server went away, or sent something after which the connection
    /// cannot be trusted.
    case closed
    case timedOut
    /// Every connection in the pool stayed in use for the pool's
    /// `acquireTimeout`.
    case poolTimedOut
    case cancelled
    /// The protocol, authentication or the server itself refused.
    case postgres(PostgresError)
}

extension PostgresClientError {
    /// The SQLSTATE the server refused a statement with -- `23505` for a
    /// unique violation -- or nil when the failure was not the server's
    /// answer. What to branch on, unlike the message, which is localised.
    public var sqlState: String? {
        if case .postgres(.server(let fields)) = self { return fields.code }
        return nil
    }

    /// Whether the server refused the statement for breaking a constraint:
    /// unique, not null, foreign key, check, exclusion -- class 23 of the
    /// SQLSTATE codes.
    public var isConstraintViolation: Bool {
        sqlState.map { $0.hasPrefix("23") } ?? false
    }

    /// The failure, for a tracer: with the SQLSTATE when it was the server
    /// that refused.
    var failure: DatabaseFailure {
        if let code = sqlState, !code.isEmpty {
            return DatabaseFailure(self, type: code, statusCode: code)
        }
        return DatabaseFailure(self, type: DatabaseFailure.caseName(of: self))
    }
}

extension Optional where Wrapped == any DatabaseSpan {
    /// Ends the span, if there is one, as `result` ended.
    func finish<T>(_ result: Result<T, PostgresClientError>) {
        guard let span = self else { return }
        switch result {
        case .success: span.end()
        case .failure(let error): span.fail(error.failure)
        }
    }
}
