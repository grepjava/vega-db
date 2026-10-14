import Testing
import Vega
@testable import VegaRedis

// A batch that fails part-way. Some of it was answered -- ran, or was refused
// -- and the rest failed, and the caller has to be told which is which: a
// plain `closed` says nothing ran, and a caller that believed it would send
// the whole batch again and repeat every write that was answered. So what
// was answered comes back in `incomplete`, and `mayHaveRun` stays true.

/// Runs `body` against fakes `make` builds, and says what it threw the
/// short way, with the fakes.
private func run<Fakes: Sendable>(_ make: @escaping @Sendable (Runtime) throws -> Fakes,
                                  _ body: @escaping @Sendable (Fakes) async throws -> String) throws -> (String, Fakes) {
    try onRuntime(make) { fakes in
        do {
            return try await body(fakes)
        } catch let error as RedisClientError {
            return describe(error)
        }
    }
}

/// `incomplete [1, -] unknownOutcome mayHaveRun`, and the like: short enough
/// to compare whole.
private func describe(_ error: RedisClientError) -> String {
    var words: [String] = []
    var inner = error
    if case .incomplete(let replies, let cause) = error {
        let shown = replies.map { reply -> String in
            guard let reply else { return "-" }
            if let n = reply.int { return "\(n)" }
            if case .error(let refused) = reply { return refused.code }
            return reply.string ?? "?"
        }
        words.append("incomplete [\(shown.joined(separator: ", "))]")
        inner = cause
    }
    switch inner {
    case .unknownOutcome: words.append("unknownOutcome")
    case .connect: words.append("connect")
    case .closed: words.append("closed")
    default: words.append("\(inner)")
    }
    if error.mayHaveRun { words.append("mayHaveRun") }
    return words.joined(separator: " ")
}

/// A key whose slot is on the chosen side of the middle.
private func key(lowSlot: Bool) -> String {
    for n in 0..<10_000 {
        let key = "k\(n)"
        if (RedisSlots.slot(of: key) < 8_192) == lowSlot { return key }
    }
    return "k"
}

/// A port nothing listens on: one the system picked, and let go.
private func deadPort() throws -> Int {
    try TCPListener.bind(host: "127.0.0.1", port: 0).localAddress.port ?? 1
}

private let masterRole = "*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n"

private func addressReply(_ port: Int) -> String {
    let host = "127.0.0.1"
    let number = "\(port)"
    return "*2\r\n$\(host.utf8.count)\r\n\(host)\r\n$\(number.utf8.count)\r\n\(number)\r\n"
}

@Suite("Redis batches that fail part-way")
struct RedisIncompleteTests {
    @Test func whatMayHaveRunIsToldApartFromWhatWasRefused() {
        let refused = RedisValue.error(RedisServerError("READONLY no"))
        #expect(RedisClientError.incomplete(replies: [.integer(1), nil], .closed).mayHaveRun,
                "an answered command ran, whatever became of the rest")
        #expect(!RedisClientError.incomplete(replies: [refused, nil], .closed).mayHaveRun,
                "a refusal is proof that it did not run, and the rest never reached the server")
        #expect(RedisClientError.incomplete(replies: [refused, nil], .unknownOutcome(.closed)).mayHaveRun)
        #expect(RedisClientError.incomplete(replies: [.integer(1), nil], .unknownOutcome(.timedOut)).cause == .timedOut)
        #expect(!RedisReplay.reads.allows(.incomplete(replies: [.integer(1), nil], .closed),
                                          [RedisCommand("INCR", "a"), RedisCommand("INCR", "b")]),
                "the whole batch may not go again: its first write would count twice")
    }

    /// A server that answers the first of two commands and goes away. The
    /// answer it gave is the caller's, and so is the fact that the second may
    /// have run.
    @Test func aReplyReadBeforeTheConnectionWentIsKept() throws {
        let (result, node) = try run({ rt in
            try FakeRedis(rt, [.init(expect: "INCR", reply: ":1\r\n", goAway: true)], answersHello: true)
        }) { node in
            let pool = RedisPool(fakeConfiguration(port: node.port), maxConnections: 1)
            defer { pool.close() }
            let replies = try await pool.pipeline([RedisCommand("INCR", "a"), RedisCommand("INCR", "b")])
            return "answered \(replies.count)"
        }
        #expect(node.mismatches == [])
        #expect(result == "incomplete [1, -] unknownOutcome mayHaveRun")
    }

    /// A transaction is one thing. A lost EXEC reply says nothing about what
    /// ran, and MULTI's OK is not an answer the caller asked for.
    @Test func aTransactionIsNeverPartlyAnswered() throws {
        let (result, node) = try run({ rt in
            try FakeRedis(rt, [.init(expect: "MULTI", reply: "+OK\r\n+QUEUED\r\n", goAway: true)], answersHello: true)
        }) { node in
            let pool = RedisPool(fakeConfiguration(port: node.port), maxConnections: 1)
            defer { pool.close() }
            _ = try await pool.transaction([RedisCommand("INCR", "a")])
            return "committed"
        }
        #expect(node.mismatches == [])
        #expect(result == "unknownOutcome mayHaveRun")
    }

    /// A cluster pipeline over two slots, whose second node cannot be
    /// reached. The first slot's write ran; the error has to say so rather
    /// than `connect`, which reads as "nothing happened".
    @Test func aPipelineAcrossSlotsSaysWhichSlotsRan() throws {
        let low = key(lowSlot: true)
        let high = key(lowSlot: false)
        let (result, node) = try run({ rt in
            let a = try FakeRedis(rt, [], answersHello: true)
            let map = slotsReply([(0, 8_191, a.port), (8_192, 16_383, try deadPort())])
            // The map, the first slot's write, then the map again for each
            // time the second node is looked for.
            a.script([.init(expect: "SLOTS", reply: map), .init(expect: low, reply: ":1\r\n")]
                     + Array(repeating: .init(expect: "SLOTS", reply: map), count: 6))
            return a
        }) { a in
            let cluster = RedisCluster(fakeConfiguration(port: a.port), maxConnectionsPerNode: 2)
            defer { cluster.close() }
            let replies = try await cluster.pipeline([RedisCommand("INCR", low), RedisCommand("INCR", high)])
            return "answered \(replies.count)"
        }
        #expect(node.mismatches == [])
        #expect(result == "incomplete [1, -] connect mayHaveRun")
        #expect(node.received.filter { $0.contains(low) }.count == 1, "the answered write is not sent again")
    }

    /// A failover in the middle of a batch, and a new master that takes the
    /// refused write and goes away. What the old master answered stands.
    @Test func whatTheOldMasterAnsweredSurvivesALaterFailure() throws {
        let (result, fakes) = try run({ rt in
            let fakes = try (0..<3).map { _ in try FakeRedis(rt, [], answersHello: true) }
            let (sentinel, old, new) = (fakes[0], fakes[1], fakes[2])
            sentinel.script([
                .init(expect: "get-master-addr-by-name", reply: addressReply(old.port)),
                .init(expect: "get-master-addr-by-name", reply: addressReply(new.port)),
            ])
            old.script([
                .init(expect: "ROLE", reply: masterRole),
                .init(expect: "alpha", reply: ":1\r\n-READONLY You can't write against a read only replica.\r\n"),
            ])
            new.script([.init(expect: "ROLE", reply: masterRole), .init(expect: "beta", reply: nil, goAway: true)])
            return fakes
        }) { fakes in
            let configuration = RedisSentinelConfiguration(sentinels: [fakeConfiguration(port: fakes[0].port)],
                                                           master: "cache", server: fakeConfiguration(port: 0))
            let redis = RedisSentinelPool(configuration, maxConnections: 2)
            defer { redis.close() }
            let replies = try await redis.pipeline([RedisCommand("INCR", "alpha"), RedisCommand("INCR", "beta")])
            return "answered \(replies.count)"
        }
        #expect(fakes.flatMap(\.mismatches) == [])
        #expect(result == "incomplete [1, -] unknownOutcome mayHaveRun")
        #expect(fakes[2].received.contains { $0.contains("alpha") } == false)
    }
}
