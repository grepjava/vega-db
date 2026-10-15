//===----------------------------------------------------------------------===//
// A hook for tracing what a client asks of a database.
//
// The clients depend on nothing but Vega, so they do not speak
// swift-distributed-tracing, or any tracer's API. They describe each round
// trip in OpenTelemetry's database terms instead -- `db.system.name`,
// `db.operation.name`, `server.address` -- and hand the description to a
// `DatabaseTracer`, which is the application's: Garuda's turns it into a span
// on its worker's tracer, a test's keeps a list.
//
//     var configuration = RedisConfiguration(host: "cache")
//     configuration.tracer = MyTracer()
//
// or, for every configuration made from then on, `DatabaseTracing.installed`:
// a framework that traces sets it once, before the application makes its
// pools, and every client is traced without being told.
//
// A client with no tracer builds no description and calls nothing. One with
// a tracer asks it first, with the description still unbuilt, so a tracer
// that is not recording at the moment costs a call and no more.
//===----------------------------------------------------------------------===//

import Synchronization

/// One round trip to a database: a command, a pipeline, a statement.
///
/// Values sent with it -- keys, arguments, bound parameters -- are never
/// here. A statement's text is, as written, because its values travel apart
/// from it.
public struct DatabaseOperation: Sendable {
    /// `db.system.name`: `redis`, `postgresql`, `sqlite`.
    public var system: String
    /// What the span is called: the command, `PIPELINE`, the statement's
    /// first keyword.
    public var name: String
    /// `db.operation.name`, when it is known.
    public var operation: String?
    /// `db.query.text`: a statement, as written.
    public var queryText: String?
    /// `db.operation.batch.size`: how many were sent together, when more
    /// than one was.
    public var batchSize: Int?
    /// `db.namespace`: the database, or Redis's database number.
    public var namespace: String?
    /// `server.address`: the host, or the unix socket's path.
    public var serverAddress: String?
    /// `server.port`, for a server reached over the network.
    public var serverPort: Int?

    public init(system: String, name: String) {
        self.system = system
        self.name = name
    }
}

/// Why a round trip failed, in a tracer's terms.
public struct DatabaseFailure: Sendable {
    /// What the client threw, or the server's refusal.
    public var error: any Error
    /// `error.type`: one of a few values -- the server's code when it gave
    /// one, else the error's case -- never a message.
    public var type: String
    /// `db.response.status_code`: the server's code for a refusal, as it gave
    /// it: `WRONGTYPE`, `23505`.
    public var statusCode: String?

    public init(_ error: any Error, type: String, statusCode: String? = nil) {
        self.error = error
        self.type = type
        self.statusCode = statusCode
    }
}

/// What a client tells about its round trips. The application's to write.
public protocol DatabaseTracer: Sendable {
    /// A round trip is starting, on the task that asked for it. Returns what
    /// to tell when it is over, or nil to leave it untraced; `describe` is
    /// only worth calling in the first case.
    func start(_ describe: () -> DatabaseOperation) -> (any DatabaseSpan)?
}

/// One round trip being traced. Exactly one of `end` and `fail` is called,
/// on the task that started it.
public protocol DatabaseSpan {
    /// It was answered.
    func end()
    /// It failed, or the server refused it.
    func fail(_ failure: DatabaseFailure)
}

extension DatabaseFailure {
    /// The case of `error` without what it carries: `closed`, `timedOut`,
    /// `unknownOutcome`.
    public static func caseName(of error: any Error) -> String {
        String(String(describing: error).prefix { $0 != "(" })
    }
}

/// The tracer a configuration starts with.
public enum DatabaseTracing {
    private static let current = Mutex<(any DatabaseTracer)?>(nil)

    /// What `tracer` is in a `PostgresConfiguration` or `RedisConfiguration`
    /// made from now on; nil, the default, traces nothing. Read once, when a
    /// configuration is made, never per round trip.
    public static var installed: (any DatabaseTracer)? {
        get { current.withLock { $0 } }
        set { current.withLock { $0 = newValue } }
    }
}
