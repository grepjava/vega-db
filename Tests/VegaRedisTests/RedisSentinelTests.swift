import Testing
import Vega
@testable import VegaRedis

// Sentinel: where the master is, asked of the sentinels rather than
// configured. Against scripted sentinels -- one that is down, one that is
// behind and names a replica, a master that has been demoted.

/// The reply to `SENTINEL get-master-addr-by-name`: a host and a port.
private func addressReply(_ port: Int) -> String {
    let host = "127.0.0.1"
    let number = "\(port)"
    return "*2\r\n$\(host.utf8.count)\r\n\(host)\r\n$\(number.utf8.count)\r\n\(number)\r\n"
}

private let masterRole = "*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n"
private let replicaRole = "*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:1\r\n$9\r\nconnected\r\n:0\r\n"
private let readOnly = "-READONLY You can't write against a read only replica.\r\n"

/// Fake servers, by role in the test; the first `sentinels` are sentinels.
private struct Fakes: Sendable {
    let all: [FakeRedis]
    let sentinels: Int
    subscript(_ i: Int) -> FakeRedis { all[i] }
    var mismatches: [String] { all.flatMap(\.mismatches) }
}

/// Runs `body` against a sentinel pool over `count` fakes, the first
/// `sentinels` of them its sentinels, after `script` has set their scripts.
private func run(_ count: Int, sentinels: Int, _ script: @escaping @Sendable (Fakes) -> Void,
                 _ body: @escaping @Sendable (RedisSentinelPool, Fakes) async throws -> String) throws -> (String, Fakes) {
    try onRuntime({ rt in
        let fakes = Fakes(all: try (0..<count).map { _ in try FakeRedis(rt, [], answersHello: true) },
                          sentinels: sentinels)
        script(fakes)
        return fakes
    }) { fakes in
        let configuration = RedisSentinelConfiguration(
            sentinels: fakes.all.prefix(fakes.sentinels).map { fakeConfiguration(port: $0.port) },
            master: "cache", server: fakeConfiguration(port: 0))
        let redis = RedisSentinelPool(configuration, maxConnections: 2)
        defer { redis.close() }
        return try await body(redis, fakes)
    }
}

@Suite("Redis Sentinel against scripted sentinels")
struct RedisSentinelScriptedTests {
    @Test func theFirstSentinelThatAnswersNamesTheMaster() throws {
        let (result, fakes) = try run(2, sentinels: 1, { f in
            f[0].script([.init(expect: "get-master-addr-by-name", reply: addressReply(f[1].port))])
            f[1].script([.init(expect: "ROLE", reply: masterRole), .init(expect: "SET", reply: "+OK\r\n")])
        }) { redis, _ in
            try await redis.set("k", "v")
            return redis.masterAddress ?? "nil"
        }
        #expect(fakes.mismatches == [])
        #expect(result == fakes[1].address)
    }

    /// A sentinel that is not there is not an error: the next one is asked.
    @Test func aSentinelThatIsDownIsSkipped() throws {
        let (result, fakes) = try run(3, sentinels: 2, { f in
            // Answering nothing and closing, which is what a sentinel being
            // restarted looks like.
            f[0].script([.init(expect: nil, reply: nil, goAway: true)])
            f[1].script([.init(expect: "get-master-addr-by-name", reply: addressReply(f[2].port))])
            f[2].script([.init(expect: "ROLE", reply: masterRole), .init(expect: "GET", reply: "$5\r\nthere\r\n")])
        }) { redis, _ in
            try await redis.get("k") ?? "nil"
        }
        #expect(fakes[1].mismatches + fakes[2].mismatches == [])
        #expect(result == "there")
    }

    /// A sentinel can be behind and name a node that has been demoted. The
    /// node says so itself, and the next sentinel is asked.
    @Test func aSentinelNamingAReplicaIsNotBelieved() throws {
        let (result, fakes) = try run(4, sentinels: 2, { f in
            f[0].script([.init(expect: "get-master-addr-by-name", reply: addressReply(f[2].port))])
            f[1].script([.init(expect: "get-master-addr-by-name", reply: addressReply(f[3].port))])
            f[2].script([.init(expect: "ROLE", reply: replicaRole)])
            f[3].script([.init(expect: "ROLE", reply: masterRole), .init(expect: "GET", reply: "$7\r\ncorrect\r\n")])
        }) { redis, f in
            let value = try await redis.get("k") ?? "nil"
            return "\(value)|\(redis.masterAddress == f[3].address)"
        }
        #expect(fakes.mismatches == [])
        #expect(result == "correct|true")
    }

    /// READONLY is what a master that has just been demoted says to a write.
    /// It means the master has moved, so the sentinels are asked again.
    @Test func readOnlyMeansTheMasterHasMoved() throws {
        let (result, fakes) = try run(3, sentinels: 1, { f in
            f[0].script([
                .init(expect: "get-master-addr-by-name", reply: addressReply(f[1].port)),
                .init(expect: "get-master-addr-by-name", reply: addressReply(f[2].port)),
            ])
            f[1].script([.init(expect: "ROLE", reply: masterRole), .init(expect: "SET", reply: readOnly)])
            f[2].script([.init(expect: "ROLE", reply: masterRole), .init(expect: "SET", reply: "+OK\r\n")])
        }) { redis, f in
            try await redis.set("k", "v")
            return "\(redis.masterAddress == f[2].address)"
        }
        #expect(fakes.mismatches == [])
        #expect(result == "true")
    }

    /// Every sentinel down: the failure is the caller's to see, not something
    /// to be retried for ever.
    @Test func noSentinelAtAllIsReported() throws {
        let (result, _) = try run(1, sentinels: 1, { f in
            f[0].script([.init(expect: nil, reply: nil, goAway: true)])
        }) { redis, _ in
            try await redis.get("k") ?? "nil"
        }
        #expect(result.hasPrefix("threw"), "\(result)")
    }

    @Test func repliesThatAreNotAnAddressOrARoleAreRefused() throws {
        #expect(RedisSentinelPool.parseAddress(.array([.bulkString(Array("h".utf8)),
                                                       .bulkString(Array("1".utf8))])) == "h:1")
        #expect(RedisSentinelPool.parseAddress(.null) == nil, "a master nobody watches")
        #expect(RedisSentinelPool.parseAddress(.array([])) == nil)
        #expect(RedisSentinelPool.parseAddress(.array([.bulkString(Array("h".utf8)),
                                                       .bulkString(Array("0".utf8))])) == nil)
        #expect(RedisSentinelPool.parseAddress(.array([.bulkString([]),
                                                       .bulkString(Array("1".utf8))])) == nil)
        #expect(RedisSentinelPool.isMaster(.array([.bulkString(Array("master".utf8))])))
        #expect(!RedisSentinelPool.isMaster(.array([.bulkString(Array("slave".utf8))])))
        #expect(!RedisSentinelPool.isMaster(.simpleString("master")))
        #expect(!RedisSentinelPool.isMaster(.null))
    }

    // MARK: A failover and what may be sent again

    /// A failover in the middle of a batch: what the old master answered
    /// stands, and only what it refused with READONLY goes to the new one.
    @Test func onlyTheRefusedPartOfAPipelineFollowsTheFailover() throws {
        let (result, fakes) = try run(3, sentinels: 1, { f in
            f[0].script([
                .init(expect: "get-master-addr-by-name", reply: addressReply(f[1].port)),
                .init(expect: "get-master-addr-by-name", reply: addressReply(f[2].port)),
            ])
            f[1].script([.init(expect: "ROLE", reply: masterRole),
                         .init(expect: "alpha", reply: "+OK\r\n" + readOnly)])
            f[2].script([.init(expect: "ROLE", reply: masterRole), .init(expect: "beta", reply: "+OK\r\n")])
        }) { redis, _ in
            let replies = try await redis.pipeline([RedisCommand("SET", "alpha", "1"), RedisCommand("SET", "beta", "2")])
            return replies.map { $0.string ?? "?" }.joined(separator: ",")
        }
        #expect(fakes.mismatches == [])
        #expect(result == "OK,OK")
        #expect(fakes[2].received.contains { $0.contains("alpha") } == false,
                "the write the old master took is not done again on the new one")
        #expect(fakes[2].received.contains { $0.contains("beta") })
    }

    /// A master that takes a write and goes away without answering. The
    /// sentinels would name a new master, but nobody knows what the old one
    /// did, so the command stops here.
    @Test func aWriteLostInAFailoverIsReportedRatherThanRepeated() throws {
        let (result, fakes) = try run(3, sentinels: 1, { f in
            f[0].script([
                .init(expect: "get-master-addr-by-name", reply: addressReply(f[1].port)),
                .init(expect: "get-master-addr-by-name", reply: addressReply(f[2].port)),
            ])
            f[1].script([.init(expect: "ROLE", reply: masterRole), .init(expect: "SET", reply: nil, goAway: true)])
            f[2].script([.init(expect: "ROLE", reply: masterRole), .init(expect: "SET", reply: "+OK\r\n")])
        }) { redis, _ in
            try await redis.set("k", "v")
            return "wrote"
        }
        #expect(result.contains("unknownOutcome"), "\(result)")
        #expect(fakes[2].received.contains { $0.contains("SET") } == false)
    }
}
