import Synchronization
import Testing
import Vega
@testable import VegaRedis

// What a tracer is told: each round trip's name and where it went, how it
// ended, and nothing of its arguments.

/// A tracer that writes each round trip down as a line.
final class RecordingTracer: DatabaseTracer {
    let lines = Mutex<[String]>([])
    /// Whether it records: one that does not is not asked to describe.
    let recording: Bool
    let described = Atomic<Int>(0)

    init(recording: Bool = true) { self.recording = recording }

    func start(_ describe: () -> DatabaseOperation) -> (any DatabaseSpan)? {
        guard recording else { return nil }
        described.add(1, ordering: .relaxed)
        return Recorded(describe(), self)
    }

    var written: [String] { lines.withLock { $0 } }

    struct Recorded: DatabaseSpan {
        let operation: DatabaseOperation
        let tracer: RecordingTracer

        init(_ operation: DatabaseOperation, _ tracer: RecordingTracer) {
            self.operation = operation
            self.tracer = tracer
        }

        var head: String {
            var words = [operation.system, operation.name]
            if let batch = operation.batchSize { words.append("x\(batch)") }
            words.append("db=\(operation.namespace ?? "-")")
            words.append("\(operation.serverAddress ?? "-"):\(operation.serverPort.map(String.init) ?? "-")")
            return words.joined(separator: " ")
        }

        func end() {
            tracer.lines.withLock { $0.append(head + " ok") }
        }

        func fail(_ failure: DatabaseFailure) {
            tracer.lines.withLock { $0.append(head + " failed \(failure.type) \(failure.statusCode ?? "-")") }
        }
    }
}

@Suite("Redis tracing")
struct RedisTracingTests {
    private func run(_ script: [FakeRedis.Step], tracer: RecordingTracer,
                     _ body: @escaping @Sendable (RedisPool) async throws -> String) throws -> (String, FakeRedis) {
        try onRuntime({ rt in try FakeRedis(rt, script, answersHello: true) }) { fake in
            var configuration = fakeConfiguration(port: fake.port)
            configuration.tracer = tracer
            let pool = RedisPool(configuration, maxConnections: 1)
            defer { pool.close() }
            return try await body(pool)
        }
    }

    @Test func aPoolWhoseConfigurationNamesNoTracerTakesTheInstalledOne() {
        let tracer = RecordingTracer()
        let own = RecordingTracer()
        // Made before anything was installed, as a configuration often is.
        let configuration = RedisConfiguration(host: "cache")
        var naming = configuration
        naming.tracer = own
        DatabaseTracing.installed = tracer
        let pool = RedisPool(configuration)
        let named = RedisPool(naming)
        DatabaseTracing.installed = nil
        let untraced = RedisPool(configuration)
        defer { pool.close(); named.close(); untraced.close() }
        #expect(configuration.tracer == nil)
        #expect(pool.configuration.tracer as? RecordingTracer === tracer)
        #expect(named.configuration.tracer as? RecordingTracer === own)
        #expect(untraced.configuration.tracer == nil)
    }

    @Test func aCommandIsNamedForItself() throws {
        let tracer = RecordingTracer()
        let (result, fake) = try run([.init(expect: "GET", reply: "$2\r\nhi\r\n")], tracer: tracer) { pool in
            try await pool.get("session:secret") ?? "nil"
        }
        #expect(result == "hi")
        #expect(fake.mismatches == [])
        #expect(tracer.written == ["redis GET db=0 127.0.0.1:\(fake.port) ok"])
    }

    @Test func aPipelineIsOneRoundTrip() throws {
        let tracer = RecordingTracer()
        let (result, _) = try run([.init(expect: "INCR", reply: ":1\r\n:2\r\n")], tracer: tracer) { pool in
            let replies = try await pool.pipeline([RedisCommand("INCR", "a"), RedisCommand("INCR", "b")])
            return "\(replies.count)"
        }
        #expect(result == "2")
        #expect(tracer.written.count == 1)
        #expect(tracer.written.first?.hasPrefix("redis PIPELINE x2 db=0") == true)
    }

    /// A refusal fails the span with the server's code; the connection is
    /// fine, and so is the next command.
    @Test func aRefusalCarriesTheServersCode() throws {
        let tracer = RecordingTracer()
        let (result, fake) = try run([
            .init(expect: "LPUSH", reply: "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"),
            .init(expect: "PING", reply: "+PONG\r\n"),
        ], tracer: tracer) { pool in
            let refused = (try? await pool.lpush("a", "b")) == nil
            let pong = try await pool.send(RedisCommand("PING")).string ?? "?"
            return "\(refused) \(pong)"
        }
        #expect(result == "true PONG")
        #expect(tracer.written == [
            "redis LPUSH db=0 127.0.0.1:\(fake.port) failed WRONGTYPE WRONGTYPE",
            "redis PING db=0 127.0.0.1:\(fake.port) ok",
        ])
    }

    /// A connection lost with the command written fails as what the caller
    /// is told: that whether it ran is not known.
    @Test func aLostConnectionIsAnUnknownOutcome() throws {
        let tracer = RecordingTracer()
        let (result, _) = try run([.init(expect: "INCR", reply: nil, goAway: true)], tracer: tracer) { pool in
            "\(try await pool.incr("a"))"
        }
        #expect(result.hasPrefix("threw unknownOutcome"))
        #expect(tracer.written.count == 1)
        #expect(tracer.written.first?.hasSuffix("failed unknownOutcome -") == true)
    }

    /// A tracer that is not recording is asked, and its description is
    /// never built.
    @Test func aTracerNotRecordingIsNotAskedToDescribe() throws {
        let tracer = RecordingTracer(recording: false)
        let (result, _) = try run([.init(expect: "GET", reply: "$-1\r\n")], tracer: tracer) { pool in
            try await pool.get("a") ?? "nil"
        }
        #expect(result == "nil")
        #expect(tracer.described.load(ordering: .relaxed) == 0)
        #expect(tracer.written == [])
    }

    @Test func aUnixSocketIsItsPath() {
        let operation = RedisConfiguration(unixSocketPath: "/run/redis.sock").describe([RedisCommand("get", "k")])
        #expect(operation.name == "GET")
        #expect(operation.serverAddress == "/run/redis.sock")
        #expect(operation.serverPort == nil)
        #expect(operation.queryText == nil, "arguments are never recorded")
    }
}
