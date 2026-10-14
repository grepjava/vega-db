import Synchronization
import Testing
import Vega
@testable import VegaPostgres

// A listener against a server that says exactly what the test wants, on cue:
// a notification that lands in the same read as the ReadyForQuery ending the
// statement that subscribed, a notice in the middle of waiting, a reply to a
// statement nobody ran, and a server that goes away. A real PostgreSQL cannot
// be asked to do these at a chosen moment.
//
// The fake runs on the same runtime as the client, as in the Redis scripted
// tests.

private func i32(_ v: Int32) -> [UInt8] {
    let u = UInt32(bitPattern: v)
    return [UInt8(u >> 24), UInt8(truncatingIfNeeded: u >> 16),
            UInt8(truncatingIfNeeded: u >> 8), UInt8(truncatingIfNeeded: u)]
}

private func cstr(_ s: String) -> [UInt8] { Array(s.utf8) + [0] }

/// One backend message: its type, its length, its body.
private func msg(_ type: Character, _ body: [UInt8] = []) -> [UInt8] {
    [UInt8(ascii: type.unicodeScalars.first!)] + i32(Int32(4 + body.count)) + body
}

private func notification(_ channel: String, _ payload: String, from pid: Int32 = 99) -> [UInt8] {
    msg("A", i32(pid) + cstr(channel) + cstr(payload))
}

/// What the server answers a statement with when it returns no rows.
private let listenReply = msg("1") + msg("2") + msg("n") + msg("C", cstr("LISTEN"))
    + msg("Z", [UInt8(ascii: "I")])

/// A PostgreSQL that authenticates anyone, answers each read with the next
/// reply in its script, and can be told to push bytes nobody asked for.
private final class FakePostgres: Sendable {
    struct Step: Sendable {
        /// What the read must contain, or nil for anything.
        var expect: String?
        var reply: [UInt8]?
        var closeAfter = false
    }

    private struct State {
        var script: [Step]
        var next = 0
        var received: [String] = []
        var mismatches: [String] = []
        /// Bytes to write without being asked for them.
        var queued: [UInt8] = []
        /// Bumped to close every connection, as a server restarting does.
        var drops = 0
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

    /// Listens on a free port of 127.0.0.1. Call on the runtime.
    init(_ rt: Runtime, _ script: [Step]) throws {
        let listener = try TCPListener.bind(host: "127.0.0.1", port: 0)
        port = try listener.localAddress.port ?? 0
        let board = Board(script)
        self.board = board
        let server = rt.spawn(with: listener) { listener in
            while true {
                guard let conn = try? await listener.accept() else { return }
                rt.spawn(with: conn) { conn in await FakePostgres.serve(conn, board) }
            }
        }
        board.listening.withLock { $0 = server }
    }

    deinit { board.listening.withLock { $0?.cancel() } }

    var received: [String] { board.state.withLock { $0.received } }
    var mismatches: [String] { board.state.withLock { $0.mismatches } }

    /// Bytes to write as soon as the server next looks, without being asked.
    func push(_ bytes: [UInt8]) { board.state.withLock { $0.queued.append(contentsOf: bytes) } }

    /// Closes whatever the client is connected on, as a server restarting does.
    func drop() { board.state.withLock { $0.drops += 1 } }

    private static func write(_ conn: borrowing TCPStream, _ bytes: [UInt8]) async {
        var out = IOBuffer(capacity: max(bytes.count, 1))
        bytes.withUnsafeBytes { _ = out.append($0) }
        try? await conn.write(out)
    }

    private static func serve(_ conn: consuming TCPStream, _ board: Board) async {
        let drops = board.state.withLock { $0.drops }
        var authenticated = false
        var buffer = IOBuffer(capacity: 65_536)
        while true {
            // What the test asked for since the last look.
            let (pushed, dropped) = board.state.withLock { state in
                defer { state.queued.removeAll() }
                return (state.queued, state.drops != drops)
            }
            if dropped { return }
            if !pushed.isEmpty { await write(conn, pushed) }

            // A short wait at a time, so a push or a drop is not kept waiting
            // for the client to say something.
            buffer.clear()
            let n: Int
            do throws(IOError) {
                n = try await Runtime.withIODeadline(.milliseconds(5)) { () async throws(IOError) -> Int in
                    try await conn.read(into: &buffer)
                }
            } catch .timedOut {
                continue
            } catch {
                return
            }
            guard n > 0 else { return }
            let text = String(decoding: buffer.withUnsafeBytes { Array($0) }, as: UTF8.self)
            guard authenticated else {
                // The startup message: nobody is turned away here.
                authenticated = true
                await write(conn, msg("R", i32(0)) + msg("K", i32(4_242) + i32(1))
                                + msg("Z", [UInt8(ascii: "I")]))
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
            if let reply = step.reply { await write(conn, reply) }
            if step.closeAfter { return }
        }
    }
}

/// Runs `body` with a pool against a fake answering `script`, and returns
/// what it returned, or "threw <error>", with the fake.
private func run(_ script: [FakePostgres.Step],
                 _ body: @escaping @Sendable (PostgresPool, FakePostgres) async throws -> String) throws -> (String, FakePostgres) {
    let rt = Runtime(workers: 2)
    defer { rt.shutdown() }
    return try rt.block { () async throws -> (String, FakePostgres) in
        let fake = try FakePostgres(rt, script)
        var configuration = PostgresConfiguration(host: "127.0.0.1", port: UInt16(fake.port),
                                                  user: "vega", password: "secret")
        configuration.tls = .disable
        configuration.timeout = .seconds(1)
        let pool = PostgresPool(configuration, maxConnections: 2)
        defer { pool.close() }
        do {
            return (try await body(pool, fake), fake)
        } catch {
            return ("threw \(error)", fake)
        }
    }
}

@Suite("A PostgreSQL listener against a scripted server")
struct PostgresListenScriptedTests {
    /// A notification the server sends as `LISTEN` finishes arrives in the
    /// same read as the ReadyForQuery. It belongs to the listener, not to the
    /// statement, and refusing it would end the listener for being told a
    /// moment too late.
    @Test func aNotificationInTheSameReadAsTheReplyIsKept() throws {
        let (result, fake) = try run([
            .init(expect: "listen", reply: listenReply + notification("jobs", "one")
                      + notification("jobs", "two")),
        ]) { pool, _ in
            let listener = try await pool.listen("jobs")
            defer { listener.close() }
            // Both are already in hand: no wait at all finds them, in order.
            let first = try await listener.next(timeout: .zero)
            let second = try await listener.next(timeout: .zero)
            let third = try await listener.next(timeout: .milliseconds(20))
            return "\(first?.payload ?? "-")|\(second?.payload ?? "-")"
                + "|\(third == nil ? "quiet" : "more")|\(listener.isOpen)"
        }
        #expect(fake.mismatches == [])
        #expect(result == "one|two|quiet|true")
    }

    /// A notice, and a setting that changed: neither is a notification, and
    /// neither ends the waiting.
    @Test func aNoticeWhileWaitingIsPassedOver() throws {
        let (result, fake) = try run([.init(expect: "listen", reply: listenReply)]) { pool, fake in
            let listener = try await pool.listen("jobs")
            defer { listener.close() }
            fake.push(msg("N", [UInt8(ascii: "S")] + cstr("NOTICE")
                               + [UInt8(ascii: "M")] + cstr("table created") + [0]))
            fake.push(msg("S", cstr("TimeZone") + cstr("UTC")))
            fake.push(notification("jobs", "after the noise"))
            let heard = try await listener.next(timeout: .seconds(2))
            return "\(heard?.payload ?? "-")|\(listener.isOpen)"
        }
        #expect(fake.mismatches == [])
        #expect(result == "after the noise|true")
    }

    /// A row description with no statement to describe: a server saying
    /// something after which nothing it says can be placed.
    @Test func aReplyNobodyAskedForEndsTheListener() throws {
        let (result, _) = try run([.init(expect: "listen", reply: listenReply)]) { pool, fake in
            let listener = try await pool.listen("jobs")
            defer { listener.close() }
            fake.push(msg("T", [0, 0]))
            do {
                _ = try await listener.next(timeout: .seconds(2))
                return "accepted"
            } catch {
                return "\(error)|\(listener.isOpen)"
            }
        }
        #expect(result == "postgres(VegaPostgresProtocol.PostgresError.unexpectedMessage(84))|false")
    }

    /// The server goes away. The listener says so, and every channel has to be
    /// listened to again on a new one.
    @Test func aServerThatGoesAwayIsReported() throws {
        let (result, _) = try run([.init(expect: "listen", reply: listenReply)]) { pool, fake in
            let listener = try await pool.listen("jobs")
            defer { listener.close() }
            fake.drop()
            do {
                _ = try await listener.next(timeout: .seconds(2))
                return "accepted"
            } catch {
                return "\(error)|\(listener.isOpen)"
            }
        }
        #expect(result == "closed|false")
    }
}
