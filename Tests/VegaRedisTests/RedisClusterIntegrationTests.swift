import Testing
import Vega
@testable import VegaRedis

// A real Redis Cluster: keys aimed at the node that owns them, a map put right
// by a redirect, keys that must share a slot, sharded channels, and a failover
// the cluster is asked to perform. Set VEGA_REDIS_CLUSTER to run; see
// IntegrationSupport.

/// Runs `body` with a cluster of its own.
private func onCluster(_ body: @escaping @Sendable (RedisCluster) async throws -> String) -> String {
    let seed = clusterSeed!
    return onRuntime({ RedisCluster(seed, maxConnectionsPerNode: 2) }, close: { $0.close() }, body)
}

/// The node that owns `slot`, by the cluster's map.
private func owner(_ cluster: RedisCluster, _ slot: Int) -> String {
    cluster.slotRanges.first { slot >= $0.from && slot <= $0.to }?.address ?? "?"
}

@Suite("Redis Cluster against a real cluster", .serialized,
       .enabled(if: clusterSeed != nil, "set VEGA_REDIS_CLUSTER to run"))
struct RedisClusterIntegrationTests {
    /// The cluster's own `CLUSTER KEYSLOT` against this implementation's hash,
    /// for the keys that make hashing interesting.
    @Test func everySlotAgreesWithTheCluster() {
        let text = onCluster { cluster in
            let keys = ["foo", "bar", "hello", "somekey", "", "a", "0", "\u{1F600}",
                        "user:{42}:name", "user:{42}:email", "{}{bar}", "foo{}{bar}",
                        "foo{{bar}}", "foo}{bar}", "{user1000}.following", "user1000",
                        "{}", "{{}}", "}{", "session:abc123", "refresh:s:ada",
                        String(repeating: "k", count: 200)]
            var wrong: [String] = []
            for key in keys {
                let reply = try await cluster.send(RedisCommand("CLUSTER", "KEYSLOT", key))
                guard case .integer(let said) = reply else {
                    wrong.append("\(key.debugDescription): \(reply)")
                    continue
                }
                let mine = RedisSlots.slot(of: key)
                if Int(said) != mine { wrong.append("\(key.debugDescription): \(said) not \(mine)") }
            }
            return wrong.isEmpty ? "all agree" : wrong.joined(separator: "; ")
        }
        #expect(text == "all agree", "\(text)")
    }

    /// Keys in different slots, which is most keys: each goes to its owner,
    /// and the map covers every slot exactly once.
    @Test func keysGoToTheNodeThatOwnsThem() {
        let text = onCluster { cluster in
            var out: [String] = []
            // From whatever an earlier run left: a counter counts from zero.
            for key in ["cl:foo", "cl:bar", "cl:hello", "cl:counter"] {
                _ = try await cluster.del(key)
            }
            // These three hash into three different slots, and on a cluster of
            // three masters they are almost certainly three different nodes.
            for (key, value) in [("foo", "1"), ("bar", "2"), ("hello", "3")] {
                try await cluster.set("cl:" + key, value)
            }
            for key in ["foo", "bar", "hello"] {
                out.append(try await cluster.get("cl:" + key) ?? "nil")
            }
            out.append("\(try await cluster.incr("cl:counter", by: 5))")
            // One key at a time: two keys in one DEL are two slots, which is
            // a CROSSSLOT and has its own test.
            var deleted = 0
            for key in ["cl:foo", "cl:bar"] { deleted += try await cluster.del(key) }
            out.append("\(deleted)")
            // Every slot is owned, once.
            var covered = 0
            var last = -1
            var ordered = true
            for range in cluster.slotRanges {
                covered += range.to - range.from + 1
                if range.from <= last { ordered = false }
                last = range.to
            }
            out.append("\(covered)")
            out.append("\(ordered)")
            out.append("\(cluster.addresses.count)")
            return out.joined(separator: "|")
        }
        #expect(text == "1|2|3|5|2|16384|true|3", "\(text)")
    }

    /// Keys touched together have to share a slot. The cluster says so, and a
    /// hash tag is the answer.
    @Test func keysTouchedTogetherMustShareASlot() {
        let text = onCluster { cluster in
            var out: [String] = []
            do {
                _ = try await cluster.send(RedisCommand("MGET", "cl:one", "cl:two"))
                out.append("allowed")
            } catch RedisClientError.server(let error) {
                out.append(error.code)
            }
            // Tagged, so one slot, so one node: the same command works.
            try await cluster.set("cl:{t}:one", "1")
            try await cluster.set("cl:{t}:two", "2")
            let both = try await cluster.send(RedisCommand("MGET", "cl:{t}:one", "cl:{t}:two"))
            out.append((both.array ?? []).compactMap(\.string).joined(separator: ","))
            // A transaction, and a watch, are one node's as well.
            let replies = try await cluster.transaction([
                RedisCommand("INCR", "cl:{t}:one"),
                RedisCommand("INCR", "cl:{t}:two"),
            ])
            out.append(replies.map { "\($0.int ?? -1)" }.joined(separator: ","))
            let watched = try await cluster.session(for: "cl:{t}:one") { session in
                try await session.watch("cl:{t}:one")
                let value = try await session.get("cl:{t}:one") ?? "0"
                let done = try await session.transaction([RedisCommand("SET", "cl:{t}:one", value + "!")])
                return done != nil
            }
            out.append("\(watched)")
            out.append(try await cluster.get("cl:{t}:one") ?? "nil")
            return out.joined(separator: "|")
        }
        #expect(text == "CROSSSLOT|1,2|2,3|true|2!", "\(text)")
    }

    /// A map that is wrong -- a slot has moved, a node was promoted -- is what
    /// `MOVED` is for. The command still lands, and the map is corrected.
    @Test func aWrongMapIsPutRightByARedirect() {
        let text = onCluster { cluster in
            try await cluster.refresh()
            let slot = RedisSlots.slot(of: "cl:moved")
            let right = owner(cluster, slot)
            // Point the slot at a node that does not own it, as a stale map
            // does, and then use it.
            let wrong = cluster.addresses.first { $0 != right } ?? right
            cluster.remember(slot: slot, at: wrong)
            try await cluster.set("cl:moved", "followed")
            var out: [String] = []
            out.append("\(wrong != right)")
            out.append(try await cluster.get("cl:moved") ?? "nil")
            out.append("\(owner(cluster, slot) == right)")
            return out.joined(separator: "|")
        }
        #expect(text == "true|followed|true", "\(text)")
    }

    /// An ordinary channel reaches every subscriber in the cluster. A sharded
    /// one belongs to a slot, and reaches the shard that owns it.
    @Test func channelsReachSubscribersAcrossTheCluster() {
        let text = onCluster { cluster in
            var out: [String] = []
            let channel = "cl:news"
            let subscription = try await cluster.subscribe(channels: [channel])
            defer { subscription.close() }
            // Published through the cluster, which sends it to whichever node
            // the command lands on -- not necessarily the subscriber's.
            _ = try await cluster.send(RedisCommand("PUBLISH", channel, "everywhere"))
            out.append(try await subscription.next(timeout: .seconds(2))?.text ?? "none")

            let sharded = "cl:{shard}:news"
            let shardedSubscription = try await cluster.subscribeSharded(channels: [sharded])
            defer { shardedSubscription.close() }
            out.append("\(try await cluster.spublish(sharded, "one shard"))")
            out.append(try await shardedSubscription.next(timeout: .seconds(2))?.text ?? "none")
            return out.joined(separator: "|")
        }
        #expect(text == "everywhere|1|one shard", "\(text)")
    }

    /// Tasks on every worker through one cluster, keys across every node.
    @Test func manyTasksShareOneCluster() {
        let text = onCluster { cluster in
            let run = UInt64.random(in: 0...UInt64.max)
            try await withThrowingTaskGroup(of: Void.self) { group in
                for task in 0..<16 {
                    group.addTask {
                        for i in 0..<25 { _ = try await cluster.incr("cl:many:\(run):\(task):\(i % 5)") }
                    }
                }
                try await group.waitForAll()
            }
            var total = 0
            for task in 0..<16 {
                for i in 0..<5 {
                    let key = "cl:many:\(run):\(task):\(i)"
                    total += Int(try await cluster.get(key) ?? "0") ?? 0
                    _ = try await cluster.del(key)
                }
            }
            return "\(total)"
        }
        #expect(text == "400", "\(text)")
    }

    /// The cluster is asked to promote a replica. Every slot that node owned
    /// is somewhere else afterwards, which the client finds out the way it
    /// finds out everything: by being told. Leaves the cluster with that
    /// master and replica swapped, which every other test is indifferent to.
    @Test func aFailoverIsFollowed() {
        let text = onCluster { cluster in
            try await cluster.refresh()
            let before = cluster.addresses.sorted()
            let slot = RedisSlots.slot(of: "cl:failover")
            try await cluster.set("cl:failover", "before")

            // A replica of the node that owns this slot, from the cluster's
            // own account of itself.
            let master = owner(cluster, slot)
            let reply = try await cluster.send(RedisCommand("CLUSTER", "SLOTS"))
            var replica: String? = nil
            for shard in reply.array ?? [] {
                guard let parts = shard.array, parts.count >= 4,
                      let node = parts[2].array, let host = node[0].string,
                      case .integer(let port) = node[1], "\(host):\(port)" == master else { continue }
                if let node = parts[3].array, let host = node[0].string, case .integer(let port) = node[1] {
                    replica = "\(host):\(port)"
                }
            }
            guard let replica else { return "no replica for \(master)" }

            var settings = cluster.seeds[0]
            let colon = replica.lastIndex(of: ":")!
            settings.host = String(replica[replica.startIndex..<colon])
            settings.port = UInt16(replica[replica.index(after: colon)...]) ?? 0
            let pool = RedisPool(settings, maxConnections: 1)
            defer { pool.close() }
            _ = try await pool.send(RedisCommand("CLUSTER", "FAILOVER"))

            // The promotion takes a moment. Meanwhile the old master redirects
            // for the slots it has given up, and the cluster follows.
            var now = master
            for _ in 0..<40 {
                _ = try? await cluster.send(RedisCommand("PING"))
                try await cluster.refresh()
                now = owner(cluster, slot)
                if now == replica { break }
                try await Runtime.sleep(for: .milliseconds(250))
            }
            var out: [String] = []
            out.append(now == replica ? "promoted" : "still \(now)")
            // The value is there, read through whoever owns the slot now, and
            // writing still works.
            out.append(try await cluster.get("cl:failover") ?? "nil")
            try await cluster.set("cl:failover", "after")
            out.append(try await cluster.get("cl:failover") ?? "nil")
            out.append("\(cluster.addresses.count == before.count)")
            return out.joined(separator: "|")
        }
        #expect(text == "promoted|before|after|true", "\(text)")
    }
}

/// Sentinel against a real set of three, which is asked to fail over. Set
/// VEGA_REDIS_SENTINEL to run.
@Suite("Redis Sentinel against a real set", .serialized,
       .enabled(if: sentinelTarget != nil, "set VEGA_REDIS_SENTINEL to run"))
struct RedisSentinelIntegrationTests {
    private func onSentinel(_ body: @escaping @Sendable (RedisSentinelPool) async throws -> String) -> String {
        let target = sentinelTarget!
        let configuration = RedisSentinelConfiguration(sentinels: [target.sentinel], master: target.master,
                                                       server: target.server)
        return onRuntime({ RedisSentinelPool(configuration, maxConnections: 2) }, close: { $0.close() }, body)
    }

    @Test func theMasterIsFoundAndUsed() {
        let text = onSentinel { redis in
            try await redis.set("sn:key", "value")
            var out: [String] = []
            out.append(try await redis.get("sn:key") ?? "nil")
            out.append("\(redis.masterAddress != nil)")
            // The node it found says it is the master, which is what was
            // checked before anything was sent to it.
            out.append("\(RedisSentinelPool.isMaster(try await redis.send(RedisCommand("ROLE"))))")
            return out.joined(separator: "|")
        }
        #expect(text == "value|true|true", "\(text)")
    }

    /// The sentinels are asked to promote the replica. Commands keep working
    /// across it, and the pool ends up on the new master.
    @Test func aFailoverIsFollowed() {
        let target = sentinelTarget!
        let text = onSentinel { redis in
            try await redis.set("sn:before", "written")
            let first = redis.masterAddress ?? "?"

            // Asked of a sentinel, which is not the master.
            let sentinels = RedisPool(target.sentinel, maxConnections: 1)
            defer { sentinels.close() }
            _ = try await sentinels.send(RedisCommand("SENTINEL", "failover", target.master))

            // A failover takes a moment. Until it is done there may be no
            // master to write to, which is a failure worth retrying rather
            // than one worth reporting.
            var moved = false
            var address = first
            for _ in 0..<60 {
                do {
                    try await redis.set("sn:after", "written")
                    address = redis.masterAddress ?? "?"
                    if address != first {
                        moved = true
                        break
                    }
                } catch {
                    // Keep trying: this is what an application would do.
                }
                try await Runtime.sleep(for: .milliseconds(500))
            }
            var out: [String] = []
            out.append(moved ? "moved" : "still \(address)")
            out.append(try await redis.get("sn:before") ?? "nil")
            out.append(try await redis.get("sn:after") ?? "nil")
            out.append("\(RedisSentinelPool.isMaster(try await redis.send(RedisCommand("ROLE"))))")
            return out.joined(separator: "|")
        }
        #expect(text == "moved|written|written|true", "\(text)")
    }
}
