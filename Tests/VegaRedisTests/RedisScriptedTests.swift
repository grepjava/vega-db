import Synchronization
import Testing
import Vega
@testable import VegaRedis

// The Redis driver against a server that says exactly what the test wants:
// what no real server can be made to do on cue. An old server with no HELLO,
// a message that arrives between subscription confirmations, pushes among
// replies, a close part-way through a reply, silence.

/// A Redis server that answers each read with the next reply in its script,
/// on whichever connection the read came from.
final class FakeRedis: Sendable {
    struct Step: Sendable {
        /// What the read must contain, or nil for anything.
        var expect: String?
        /// Bytes to answer with, or nil to say nothing.
        var reply: String?
        var closeAfter = false
        /// Closes the connection after answering and stops listening, so the
        /// server is gone rather than merely rude.
        var goAway = false
    }

    private struct State {
        var script: [Step]
        var next = 0
        var received: [String] = []
        var mismatches: [String] = []
        var accepted = 0
    }

    /// The state the server's tasks share with the test.
    private final class Board: Sendable {
        let state: Mutex<State>
        /// The accept loop, cancelled to stop listening.
        let listening = Mutex<Task<Void, Never>?>(nil)
        init(_ script: [Step]) { state = Mutex(State(script: script)) }
    }

    let port: Int
    private let board: Board

    /// Listens on a free port of 127.0.0.1. Call on the runtime. With
    /// `answersHello`, HELLO is answered as RESP3 outside the script, for
    /// tests with more than one connection.
    init(_ rt: Runtime, _ script: [Step], answersHello: Bool = false) throws {
        let listener = try TCPListener.bind(host: "127.0.0.1", port: 0)
        port = try listener.localAddress.port ?? 0
        let board = Board(script)
        self.board = board
        let server = rt.spawn(with: listener) { listener in
            while true {
                guard let conn = try? await listener.accept() else { return }
                board.state.withLock { $0.accepted += 1 }
                rt.spawn(with: conn) { conn in await FakeRedis.serve(conn, board, answersHello) }
            }
        }
        board.listening.withLock { $0 = server }
    }

    deinit { board.listening.withLock { $0?.cancel() } }

    var address: String { "127.0.0.1:\(port)" }

    /// Replaces the script, for fakes that must know each other's ports.
    func script(_ steps: [Step]) {
        board.state.withLock { $0.script = steps; $0.next = 0 }
    }

    var received: [String] { board.state.withLock { $0.received } }
    var mismatches: [String] { board.state.withLock { $0.mismatches } }
    var accepted: Int { board.state.withLock { $0.accepted } }

    private static func serve(_ conn: consuming TCPStream, _ board: Board, _ answersHello: Bool) async {
        var buffer = IOBuffer(capacity: 65_536)
        while true {
            buffer.clear()
            guard let n = try? await conn.read(into: &buffer), n > 0 else { return }
            let text = buffer.string
            if answersHello && text.contains("HELLO") {
                var out = IOBuffer(capacity: hello3.utf8.count)
                out.append(hello3)
                try? await conn.write(out)
                continue
            }
            let step: Step? = board.state.withLock { state in
                state.received.append(text)
                guard state.next < state.script.count else { return nil }
                let step = state.script[state.next]
                state.next += 1
                if let expect = step.expect, !text.contains(expect) {
                    state.mismatches.append("expected \(expect) in \(text.debugDescription)")
                }
                return step
            }
            guard let step else { continue }
            if let reply = step.reply {
                var out = IOBuffer(capacity: max(reply.utf8.count, 1))
                out.append(reply)
                try? await conn.write(out)
            }
            if step.goAway {
                board.listening.withLock { $0?.cancel() }
                return
            }
            if step.closeAfter { return }
        }
    }
}

let hello3 = "%1\r\n$5\r\nproto\r\n:3\r\n"

/// A `CLUSTER SLOTS` reply: one range per node, each owned by a master with
/// no replicas.
func slotsReply(_ ranges: [(from: Int, to: Int, port: Int)]) -> String {
    var out = "*\(ranges.count)\r\n"
    for range in ranges {
        out += "*3\r\n:\(range.from)\r\n:\(range.to)\r\n"
        let id = "node\(range.port)"
        out += "*3\r\n$9\r\n127.0.0.1\r\n:\(range.port)\r\n$\(id.utf8.count)\r\n\(id)\r\n"
    }
    return out
}

/// Runs `body` on a two-worker runtime with the fakes `make` builds there,
/// and returns what it returned, or "threw <error>", with the fakes.
func onRuntime<Fakes: Sendable>(_ make: @escaping @Sendable (Runtime) throws -> Fakes,
                                _ body: @escaping @Sendable (Fakes) async throws -> String) throws -> (String, Fakes) {
    let rt = Runtime(workers: 2)
    defer { rt.shutdown() }
    return try rt.block { () async throws -> (String, Fakes) in
        let fakes = try make(rt)
        do {
            return (try await body(fakes), fakes)
        } catch {
            return ("threw \(error)", fakes)
        }
    }
}

/// A configuration for a fake: plaintext, a second to answer.
func fakeConfiguration(port: Int) -> RedisConfiguration {
    var configuration = RedisConfiguration(host: "127.0.0.1", port: UInt16(port))
    configuration.tls = .disable
    configuration.timeout = .seconds(1)
    return configuration
}

/// Runs `body` with a pool against a fake answering `script`, and returns
/// what it returned, or "threw <error>".
private func run(_ script: [FakeRedis.Step], answersHello: Bool = false, configure: @escaping @Sendable (inout RedisConfiguration) -> Void = { _ in },
                 _ body: @escaping @Sendable (RedisPool) async throws -> String) throws -> (String, FakeRedis) {
    let rt = Runtime(workers: 2)
    defer { rt.shutdown() }
    return try rt.block { () async throws -> (String, FakeRedis) in
        let fake = try FakeRedis(rt, script, answersHello: answersHello)
        var configuration = RedisConfiguration(host: "127.0.0.1", port: UInt16(fake.port))
        configuration.tls = .disable
        configuration.timeout = .seconds(1)
        configure(&configuration)
        let redis = RedisPool(configuration, maxConnections: 2)
        defer { redis.close() }
        do {
            return (try await body(redis), fake)
        } catch {
            return ("threw \(error)", fake)
        }
    }
}

@Suite("Redis against a scripted server")
struct RedisScriptedTests {
    @Test func anOldServerIsSpokenToInRESP2() throws {
        let (result, fake) = try run([
            .init(expect: "HELLO", reply: "-ERR unknown command 'HELLO', with args beginning with: '3'\r\n"),
            .init(expect: "AUTH", reply: "+OK\r\n"),
            .init(expect: "GET", reply: "$2\r\nhi\r\n"),
            .init(expect: "HGETALL", reply: "*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n"),
        ], configure: { $0.password = "secret" }) { redis in
            let value = try await redis.get("k") ?? "nil"
            let hash = try await redis.hgetall("h")
            return "\(value)|\(hash.sorted { $0.key < $1.key })"
        }
        #expect(fake.mismatches == [])
        #expect(result == #"hi|[(key: "a", value: "1"), (key: "b", value: "2")]"#)
        #expect(fake.received[1].contains("$6\r\nsecret\r\n"))
    }

    @Test func aMessageBetweenConfirmationsIsKept() throws {
        let (result, fake) = try run([
            .init(expect: "HELLO", reply: hello3),
            .init(expect: "SUBSCRIBE",
                  reply: ">3\r\n$9\r\nsubscribe\r\n$2\r\nc1\r\n:1\r\n"
                       + ">3\r\n$7\r\nmessage\r\n$2\r\nc1\r\n$5\r\nearly\r\n"
                       + ">3\r\n$9\r\nsubscribe\r\n$2\r\nc2\r\n:2\r\n"
                       + ">3\r\n$7\r\nmessage\r\n$2\r\nc2\r\n$5\r\nlater\r\n"),
        ]) { redis in
            let subscription = try await redis.subscribe(channels: ["c1", "c2"])
            let first = try await subscription.next(timeout: .milliseconds(500))
            let second = try await subscription.next(timeout: .milliseconds(500))
            return "\(first?.channel ?? "-") \(first?.text ?? "-")|\(second?.channel ?? "-") \(second?.text ?? "-")"
        }
        #expect(fake.mismatches == [])
        #expect(result == "c1 early|c2 later")
    }

    @Test func aRESP2SubscriptionReadsArrays() throws {
        let (result, _) = try run([
            .init(expect: "HELLO", reply: "-ERR unknown command 'HELLO'\r\n"),
            .init(expect: "PSUBSCRIBE",
                  reply: "*3\r\n$10\r\npsubscribe\r\n$3\r\nch*\r\n:1\r\n"
                       + "*4\r\n$8\r\npmessage\r\n$3\r\nch*\r\n$4\r\nchat\r\n$2\r\nhi\r\n"),
        ]) { redis in
            let subscription = try await redis.subscribe(patterns: ["ch*"])
            let message = try await subscription.next(timeout: .milliseconds(500))
            return "\(message?.pattern ?? "-") \(message?.channel ?? "-") \(message?.text ?? "-")"
        }
        #expect(result == "ch* chat hi")
    }

    @Test func aQuietSubscriptionReturnsNilAndStaysOpen() throws {
        let (result, _) = try run([
            .init(expect: "HELLO", reply: hello3),
            .init(expect: "SUBSCRIBE", reply: ">3\r\n$9\r\nsubscribe\r\n$2\r\nc1\r\n:1\r\n"),
        ]) { redis in
            let subscription = try await redis.subscribe(channels: ["c1"])
            let message = try await subscription.next(timeout: .milliseconds(50))
            return "\(message == nil)|\(subscription.isOpen)"
        }
        #expect(result == "true|true")
    }

    @Test func pushesAmongRepliesAreNotTakenForThem() throws {
        let (result, _) = try run([
            .init(expect: "HELLO", reply: hello3),
            .init(expect: "GET", reply: ">2\r\n$10\r\ninvalidate\r\n*1\r\n$1\r\nk\r\n$3\r\nabc\r\n:5\r\n"),
        ]) { redis in
            let replies = try await redis.pipeline([RedisCommand("GET", "k"), RedisCommand("INCR", "n")])
            return "\(replies)"
        }
        #expect(result == "[VegaRedisProtocol.RedisValue.bulkString([97, 98, 99]), VegaRedisProtocol.RedisValue.integer(5)]")
    }

    @Test func bytesNobodyAskedForAfterTheHandshakeAreRefused() throws {
        let (result, _) = try run([.init(expect: "HELLO", reply: hello3 + "+SURPRISE\r\n")]) { redis in
            "\(try await redis.send("PING"))"
        }
        #expect(result == "threw protocolViolation(VegaRedisProtocol.RedisProtocolError.badValue)")
    }

    @Test func aCloseInTheMiddleOfAReplyFailsAndIsNotReused() throws {
        let (result, fake) = try run([
            .init(expect: "HELLO", reply: hello3),
            .init(expect: "GET", reply: "$10\r\nhalf", closeAfter: true),
            .init(expect: "HELLO", reply: hello3),
            .init(expect: "PING", reply: "+PONG\r\n"),
        ]) { redis in
            var out: [String] = []
            do {
                _ = try await redis.get("k")
                out.append("read")
            } catch {
                out.append("\(error)")
            }
            out.append("\(redis.counts)")
            out.append("\(try await redis.send("PING"))")
            return out.joined(separator: "|")
        }
        // The GET was written and half answered, so what the server did with
        // it is not knowable from here. `unknownOutcome` says so, rather than
        // letting a bare "closed" be read as "it did not run".
        #expect(result == "unknownOutcome(VegaRedis.RedisClientError.closed)|(idle: 0, inUse: 0)|simpleString(\"PONG\")")
        #expect(fake.accepted == 2)
    }

    @Test func silenceTimesOut() throws {
        let (result, _) = try run([
            .init(expect: "HELLO", reply: hello3),
            .init(expect: "GET", reply: nil),
        ], configure: { $0.timeout = .milliseconds(100) }) { redis in
            let start = ContinuousClock.now
            do {
                _ = try await redis.get("k")
                return "answered"
            } catch {
                return "\(error)|\(start.duration(to: .now) >= .milliseconds(90))|\(redis.counts)"
            }
        }
        #expect(result == "unknownOutcome(VegaRedis.RedisClientError.timedOut)|true|(idle: 0, inUse: 0)")
    }

    @Test func aRefusedHandshakeClosesTheConnection() throws {
        let (result, _) = try run([
            .init(expect: "HELLO", reply: "-WRONGPASS invalid username-password pair\r\n"),
        ], configure: { $0.password = "wrong" }) { redis in
            do throws(RedisClientError) {
                _ = try await redis.send("PING")
                return "connected"
            } catch {
                if case .handshake(.authentication(let refusal)) = error { return "\(refusal.code)|\(redis.counts)" }
                return "\(error)"
            }
        }
        #expect(result == "WRONGPASS|(idle: 0, inUse: 0)")
    }

    @Test func aPoolOfTwoServesManyTasksAtOnce() throws {
        // Twenty tasks on two workers, ten PINGs each, through two
        // connections.
        let script = Array(repeating: FakeRedis.Step(expect: "PING", reply: "+PONG\r\n"), count: 200)
        let (result, fake) = try run(script, answersHello: true) { redis in
            try await withThrowingTaskGroup(of: Int.self) { group in
                for _ in 0..<20 {
                    group.addTask {
                        var ok = 0
                        for _ in 0..<10 where try await redis.send("PING") == .simpleString("PONG") { ok += 1 }
                        return ok
                    }
                }
                return "\(try await group.reduce(0, +))|\(redis.counts.inUse)"
            }
        }
        #expect(fake.mismatches == [])
        #expect(result == "200|0")
        #expect(fake.accepted <= 2)
    }
}
