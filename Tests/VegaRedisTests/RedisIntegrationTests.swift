import Testing
import Vega
@testable import VegaRedis

// The Redis driver against a real server: commands and their typed replies,
// pipelines, transactions and watches, sessions that must not be handed on,
// authentication, pub/sub, limits, and a connection the server dropped.
// Set VEGA_REDIS (and VEGA_REDIS_SOCKET) to run; see IntegrationSupport.

/// Runs `body` with a pool of its own.
private func run(maxConnections: Int = 4, acquireTimeout: Duration? = nil,
                 configure: (inout RedisConfiguration) -> Void = { _ in },
                 _ body: @escaping @Sendable (RedisPool) async throws -> String) -> String {
    var configuration = redisTarget!
    configure(&configuration)
    let settled = configuration
    return onRuntime({ RedisPool(settled, maxConnections: maxConnections, acquireTimeout: acquireTimeout) },
                     close: { $0.close() }, body)
}

/// What a refusal's code is, or the error as it is.
private func code(_ error: any Error) -> String {
    if case .server(let refusal)? = error as? RedisClientError { return refusal.code }
    return "\(error)"
}

@Suite("Redis against a real server", .serialized, .enabled(if: redisTarget != nil, "set VEGA_REDIS to run"))
struct RedisIntegrationTests {
    @Test func stringsKeysAndExpiry() {
        let k = uniqueKey("strings")
        let result = run { redis in
            var out: [String] = []
            out.append("\(try await redis.get(k) ?? "nil")")
            out.append("\(try await redis.set(k, "hello"))")
            out.append("\(try await redis.set(k, "again", condition: .ifAbsent))")
            out.append(try await redis.get(k) ?? "nil")
            out.append("\(try await redis.exists(k, k))")
            out.append("\(try await redis.pttl(k) ?? -9)")
            out.append("\(try await redis.pexpire(k, milliseconds: 60_000))")
            let ttl = try await redis.pttl(k) ?? -9
            out.append(ttl > 50_000 && ttl <= 60_000 ? "ttl ok" : "ttl \(ttl)")
            out.append("\(try await redis.del(k, k + ":none"))")
            out.append("\(try await redis.pttl(k) as Int?)")
            out.append("\(try await redis.set(k, "x", condition: .ifPresent))")
            out.append("\(try await redis.incr(k + ":n"))")
            out.append("\(try await redis.incr(k + ":n", by: 41))")
            out.append(try await redis.get(k + ":n") ?? "nil")
            try await redis.set(k + ":bin", [UInt8]([0, 13, 10, 255]))
            out.append("\(try await redis.getBytes(k + ":bin") ?? [])")
            try await redis.set(k + ":px", "gone", expireMilliseconds: 20)
            try await Runtime.sleep(for: .milliseconds(60))
            out.append(try await redis.get(k + ":px") ?? "expired")
            try await redis.del(k + ":n", k + ":bin")
            return out.joined(separator: "|")
        }
        #expect(result == "nil|true|false|hello|2|-1|true|ttl ok|1|nil|false|1|42|42|[0, 13, 10, 255]|expired")
    }

    @Test func hashesListsAndSets() {
        let k = uniqueKey("structures")
        let result = run { redis in
            var out: [String] = []
            out.append("\(try await redis.hset(k + ":h", [("name", "ada"), ("visits", 3)]))")
            out.append(try await redis.hget(k + ":h", "name") ?? "nil")
            out.append("\(try await redis.hgetall(k + ":h").sorted { $0.key < $1.key })")
            out.append("\(try await redis.hdel(k + ":h", "visits", "missing"))")
            out.append("\(try await redis.rpush(k + ":l", "b", "c"))")
            out.append("\(try await redis.lpush(k + ":l", "a"))")
            out.append("\(try await redis.lrange(k + ":l", 0, -1))")
            out.append(try await redis.lpop(k + ":l") ?? "nil")
            out.append(try await redis.rpop(k + ":l") ?? "nil")
            out.append("\(try await redis.sadd(k + ":s", "x", "y", "x"))")
            out.append("\(try await redis.smembers(k + ":s").sorted())")
            out.append("\(try await redis.srem(k + ":s", "x"))")
            try await redis.del(k + ":h", k + ":l", k + ":s")
            return out.joined(separator: "|")
        }
        #expect(result == #"2|ada|[(key: "name", value: "ada"), (key: "visits", value: "3")]|1|2|3|["a", "b", "c"]|a|c|2|["x", "y"]|1"#)
    }

    @Test func aRefusedCommandIsAServerErrorAndTheConnectionCarriesOn() {
        let k = uniqueKey("wrongtype")
        let result = run(maxConnections: 1) { redis in
            try await redis.rpush(k, "a")
            var out: [String] = []
            do {
                _ = try await redis.get(k)
                out.append("no error")
            } catch {
                out.append(code(error))
            }
            // The same, and only, connection answers the next command.
            out.append("\(try await redis.send("PING"))")
            out.append("\(redis.counts.idle)")
            try await redis.del(k)
            return out.joined(separator: "|")
        }
        #expect(result == "WRONGTYPE|simpleString(\"PONG\")|1")
    }

    @Test func aPipelineIsOneRoundTripWithEveryReply() {
        let k = uniqueKey("pipeline")
        let result = run { redis in
            let replies = try await redis.pipeline([
                RedisCommand("SET", k, "1"),
                RedisCommand("INCR", k),
                RedisCommand("NOSUCHCOMMAND"),
                RedisCommand("GET", k),
                RedisCommand("DEL", k),
            ])
            return replies.map { reply in
                if case .error(let e) = reply { return e.code }
                return reply.string ?? "\(reply)"
            }.joined(separator: "|")
        }
        #expect(result == "OK|2|ERR|2|1")
    }

    @Test func aTransactionRunsTogether() {
        let k = uniqueKey("multi")
        let result = run { redis in
            var out: [String] = []
            let replies = try await redis.transaction([RedisCommand("SET", k, "text"), RedisCommand("INCR", k),
                                                       RedisCommand("APPEND", k, "!")])
            // INCR fails as it runs; the others still ran.
            out.append(replies.map { reply -> String in
                if case .error(let e) = reply { return e.code }
                return reply.string ?? "\(reply)"
            }.joined(separator: ","))
            out.append(try await redis.get(k) ?? "nil")
            do {
                _ = try await redis.transaction([RedisCommand("SET", k, "never"), RedisCommand("NOSUCHCOMMAND")])
                out.append("ran")
            } catch {
                out.append(code(error))
            }
            out.append(try await redis.get(k) ?? "nil")
            try await redis.del(k)
            return out.joined(separator: "|")
        }
        #expect(result == "OK,ERR,5|text!|ERR|text!")
    }

    @Test func aWatchedKeyThatChangesStopsTheTransaction() {
        let k = uniqueKey("watch")
        let result = run { redis in
            try await redis.set(k, "10")
            let first = try await redis.session { s in
                try await s.watch(k)
                let stock = Int(try await s.get(k) ?? "0") ?? 0
                // Someone else, on another connection, between the read and
                // the transaction.
                try await redis.set(k, "0")
                return try await s.transaction([RedisCommand("SET", k, stock - 1)]) == nil ? "aborted" : "ran"
            }
            let second = try await redis.session { s in
                try await s.watch(k)
                let stock = Int(try await s.get(k) ?? "0") ?? 0
                return try await s.transaction([RedisCommand("SET", k, stock + 5)]) == nil ? "aborted" : "ran"
            }
            let value = try await redis.get(k) ?? "nil"
            try await redis.del(k)
            return "\(first)|\(second)|\(value)"
        }
        #expect(result == "aborted|ran|5")
    }

    @Test func aSessionLeftInsideATransactionIsNotHandedOn() {
        let k = uniqueKey("leftover")
        let result = run(maxConnections: 1) { redis in
            var out: [String] = []
            try await redis.session { s in
                _ = try await s.send("MULTI")
                _ = try await s.send("SET", k, "queued")
            }
            out.append("\(redis.counts)")
            // Were that connection reused, this would be QUEUED, not run.
            out.append("\(try await redis.set(k, "direct"))")
            out.append(try await redis.get(k) ?? "nil")
            try await redis.session { s in _ = try await s.send("SELECT", 1) }
            out.append("\(redis.counts)")
            try await redis.session { s in try await s.watch(k) }
            out.append("\(redis.counts)")
            out.append(try await redis.get(k) ?? "nil in database 1")
            try await redis.del(k)
            return out.joined(separator: "|")
        }
        #expect(result == "(idle: 0, inUse: 0)|true|direct|(idle: 0, inUse: 0)|(idle: 0, inUse: 0)|direct")
    }

    @Test func aFullPoolTimesOut() {
        let result = run(maxConnections: 1, acquireTimeout: .milliseconds(50)) { redis in
            try await redis.session { _ in
                do {
                    _ = try await redis.send("PING")
                    return "got a second connection"
                } catch {
                    return "\(error)"
                }
            }
        }
        #expect(result == "poolTimedOut")
    }

    @Test func authenticationIsRequiredAndRefusedWhenWrong() {
        let wrong = run(configure: { $0.password = "wrong" }) { redis in
            _ = try await redis.send("PING")
            return "authenticated"
        }
        #expect(wrong.hasPrefix("threw handshake("))
        #expect(wrong.contains("authentication") && wrong.contains("WRONGPASS"))

        // HELLO without credentials is itself refused by a server that wants
        // them, so the connection never opens.
        let none = run(configure: { $0.password = nil }) { redis in
            _ = try await redis.send("GET", "anything")
            return "ran without a password"
        }
        #expect(none.hasPrefix("threw handshake("))
        #expect(none.contains("NOAUTH"))
    }

    @Test func anACLUserIsHeldToItsKeys() {
        let result = run(configure: {
            $0.username = "app"
            $0.password = "app-secret"
            $0.clientName = "vega-tests"
        }) { redis in
            var out: [String] = []
            out.append("\(try await redis.set("app:" + uniqueKey("acl"), "mine", expireMilliseconds: 1000))")
            do {
                try await redis.set("other:key", "not mine")
                out.append("allowed")
            } catch {
                out.append(code(error))
            }
            out.append(try await redis.send("CLIENT", "GETNAME").string ?? "nil")
            return out.joined(separator: "|")
        }
        #expect(result == "true|NOPERM|vega-tests")
    }

    @Test func aDatabaseOtherThanZeroIsSelected() {
        let k = uniqueKey("database")
        let result = run(configure: { $0.database = 3 }) { redis in
            try await redis.set(k, "in three", expireMilliseconds: 5_000)
            let here = try await redis.get(k) ?? "nil"
            let info = try await redis.send("CLIENT", "INFO").string ?? ""
            return "\(here)|\(info.contains("db=3"))"
        }
        #expect(result == "in three|true")
        let elsewhere = run { redis in try await redis.get(k) ?? "not in zero" }
        #expect(elsewhere == "not in zero")
    }

    @Test(.enabled(if: redisSocket != nil, "set VEGA_REDIS_SOCKET to run"))
    func aUnixSocketConnects() {
        let result = run(configure: {
            let password = $0.password
            $0 = RedisConfiguration(unixSocketPath: redisSocket!, password: password)
        }) { redis in
            "\(try await redis.send("PING"))"
        }
        #expect(result == "simpleString(\"PONG\")")
    }

    @Test func largeRepliesArriveWhole() {
        let k = uniqueKey("large")
        let result = run { redis in
            let big = [UInt8]((0..<(3 << 20)).map { UInt8(truncatingIfNeeded: $0 &* 31) })
            try await redis.set(k, big)
            let back = try await redis.getBytes(k)
            var elements: [RedisCommand] = []
            for batch in 0..<10 {
                var push = RedisCommand("RPUSH", k + ":list")
                for i in 0..<10_000 { push.append("item-\(batch)-\(i)") }
                elements.append(push)
            }
            _ = try await redis.pipeline(elements)
            let all = try await redis.lrange(k + ":list", 0, -1)
            try await redis.del(k, k + ":list")
            return "\(back == big)|\(all.count)|\(all.last ?? "")"
        }
        #expect(result == "true|100000|item-9-9999")
    }

    @Test func aReplyPastTheLimitClosesTheConnection() {
        let k = uniqueKey("limit")
        let result = run(maxConnections: 1, configure: { $0.maxBulkBytes = 1024 }) { redis in
            try await redis.set(k, [UInt8](repeating: 7, count: 4096))
            var out: [String] = []
            do {
                _ = try await redis.getBytes(k)
                out.append("read it")
            } catch {
                out.append("\(error)")
            }
            out.append("\(redis.counts)")
            out.append("\(try await redis.del(k))")
            return out.joined(separator: "|")
        }
        #expect(result == "protocolViolation(VegaRedisProtocol.RedisProtocolError.tooLarge)|(idle: 0, inUse: 0)|1")
    }

    @Test func aConnectionTheServerDroppedIsReplacedBeforeUse() {
        let result = run(maxConnections: 1, configure: { $0.clientName = "vega-dropped" }) { redis in
            let id = try await redis.send("CLIENT", "ID")
            let killer = RedisPool(redis.configuration, maxConnections: 1)
            _ = try await killer.send("CLIENT", "KILL", "ID", id.int ?? 0)
            killer.close()
            try await Runtime.sleep(for: .milliseconds(50))
            let after = try await redis.send("CLIENT", "ID")
            return "\(after != id)|\(try await redis.send("PING"))"
        }
        #expect(result == "true|simpleString(\"PONG\")")
    }

    @Test func blockingCommandsWaitAsLongAsTheyAreGiven() {
        let k = uniqueKey("blocking")
        let result = run { redis in
            let start = ContinuousClock.now
            let reply = try await redis.send(RedisCommand("BLPOP", k, "0.2"), timeout: .seconds(5))
            return "\(reply.isNull)|\(start.duration(to: .now) >= .milliseconds(150))"
        }
        #expect(result == "true|true")
    }

    @Test func publishedMessagesReachASubscriber() {
        let channel = uniqueKey("channel")
        let result = run { redis in
            let subscription = try await redis.subscribe(channels: [channel], patterns: [channel + ":*"])
            defer { subscription.close() }
            var out: [String] = []
            out.append("\(try await redis.publish(channel, "hello"))")
            if let message = try await subscription.next(timeout: .seconds(2)) {
                out.append("\(message.channel == channel) \(message.pattern ?? "-") \(message.text)")
            }
            try await redis.publish(channel + ":room", [UInt8]([0, 1, 2]))
            if let message = try await subscription.next(timeout: .seconds(2)) {
                out.append("\(message.pattern == channel + ":*") \(message.payload)")
            }
            let quiet = try await subscription.next(timeout: .milliseconds(50))
            out.append(quiet == nil ? "quiet" : "unexpected")
            try await subscription.subscribe(channel + "-more")
            // Until the server has confirmed, a publish may reach no one.
            try await Runtime.sleep(for: .milliseconds(50))
            try await redis.publish(channel + "-more", "later")
            out.append(try await subscription.next(timeout: .seconds(2))?.text ?? "none")
            out.append("\(redis.counts.idle)")
            return out.joined(separator: "|")
        }
        #expect(result == "1|true - hello|true [0, 1, 2]|quiet|later|1")
    }

    /// Tasks on every worker through one pool of four, against the real
    /// server: every command answered, nothing left out.
    @Test func manyTasksShareAPool() {
        let k = uniqueKey("shared")
        let result = run { redis in
            try await withThrowingTaskGroup(of: Void.self) { group in
                for _ in 0..<32 {
                    group.addTask {
                        for _ in 0..<50 { _ = try await redis.incr(k) }
                    }
                }
                try await group.waitForAll()
            }
            let total = try await redis.get(k) ?? "nil"
            try await redis.del(k)
            return "\(total)|\(redis.counts.inUse)|\(redis.counts.idle <= 4)"
        }
        #expect(result == "1600|0|true")
    }
}

