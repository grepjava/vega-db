import Testing
import Vega
@testable import VegaRedis

// A cluster of servers that say exactly what the test wants, on cue: ASK for
// a key that has already moved, TRYAGAIN for a slot in the middle of moving,
// and a node that goes away with a command in flight. A real cluster does
// these only while it is being resharded, and not to order.

/// Two fake nodes, a and b, answering HELLO for themselves.
private struct Pair: Sendable {
    let a: FakeRedis
    let b: FakeRedis
    var mismatches: [String] { a.mismatches + b.mismatches }
}

private func pair(_ rt: Runtime) throws -> Pair {
    Pair(a: try FakeRedis(rt, [], answersHello: true), b: try FakeRedis(rt, [], answersHello: true))
}

/// Runs `body` against a cluster seeded with node a, after `script` has set
/// the nodes' scripts.
private func run(replay: RedisReplay = .reads, _ script: @escaping @Sendable (Pair) -> Void,
                 _ body: @escaping @Sendable (RedisCluster, Pair) async throws -> String) throws -> (String, Pair) {
    try onRuntime({ rt in
        let nodes = try pair(rt)
        script(nodes)
        return nodes
    }) { nodes in
        let cluster = RedisCluster(fakeConfiguration(port: nodes.a.port), maxConnectionsPerNode: 2, replay: replay)
        defer { cluster.close() }
        return try await body(cluster, nodes)
    }
}

@Suite("Redis Cluster against scripted nodes")
struct RedisClusterScriptedTests {
    /// `MOVED`: the slot is somewhere else for good. The command goes again to
    /// the node named, and the map remembers it.
    @Test func movedSendsTheCommandToTheNodeNamed() throws {
        let (result, nodes) = try run({ n in
            n.a.script([
                .init(expect: "SLOTS", reply: slotsReply([(0, 16_383, n.a.port)])),
                .init(expect: "GET", reply: "-MOVED 12182 \(n.b.address)\r\n"),
            ])
            n.b.script([.init(expect: "GET", reply: "$5\r\nthere\r\n")])
        }) { cluster, n in
            let value = try await cluster.get("foo") ?? "nil"
            let owner = cluster.slotRanges.first { 12_182 >= $0.from && 12_182 <= $0.to }?.address ?? "?"
            return "\(value)|\(owner == n.b.address)"
        }
        #expect(nodes.mismatches == [])
        #expect(result == "there|true")
    }

    /// `ASK`: this one key has moved already, the rest of the slot has not.
    /// The command goes to the node taking the slot on, behind an `ASKING`
    /// that says the client knows -- and the map is left alone.
    @Test func askSendsAskingAndLeavesTheMapAlone() throws {
        let (result, nodes) = try run({ n in
            n.a.script([
                .init(expect: "SLOTS", reply: slotsReply([(0, 16_383, n.a.port)])),
                .init(expect: "GET", reply: "-ASK 12182 \(n.b.address)\r\n"),
            ])
            // One read holding ASKING and the command, so one write holding
            // both replies.
            n.b.script([.init(expect: "ASKING", reply: "+OK\r\n$8\r\nmigrated\r\n")])
        }) { cluster, n in
            let value = try await cluster.get("foo") ?? "nil"
            let owner = cluster.slotRanges.first { 12_182 >= $0.from && 12_182 <= $0.to }?.address ?? "?"
            return "\(value)|\(owner == n.a.address)"
        }
        #expect(nodes.mismatches == [])
        #expect(nodes.b.received.last?.contains("GET") == true, "the command follows ASKING in one write")
        #expect(result == "migrated|true", "the slot still belongs to the node it is moving from")
    }

    /// `TRYAGAIN`: the keys are in a slot halfway between two nodes. Waiting
    /// and asking again is all a client can do, and is what it does.
    @Test func tryAgainIsTriedAgain() throws {
        let (result, nodes) = try run({ n in
            n.a.script([
                .init(expect: "SLOTS", reply: slotsReply([(0, 16_383, n.a.port)])),
                .init(expect: "MGET", reply: "-TRYAGAIN Multiple keys request during rehashing of slot\r\n"),
                .init(expect: "MGET", reply: "*2\r\n$1\r\n1\r\n$1\r\n2\r\n"),
            ])
        }) { cluster, _ in
            let reply = try await cluster.send(RedisCommand("MGET", "{t}:one", "{t}:two"))
            return (reply.array ?? []).compactMap(\.string).joined(separator: ",")
        }
        #expect(nodes.mismatches == [])
        #expect(result == "1,2")
    }

    /// A node that goes away with a command in flight. Whoever owns the slot
    /// now is in a map, and a map comes from whichever node is still there.
    @Test func aNodeThatGoesAwayIsLeftForWhoeverOwnsTheSlotNow() throws {
        let (result, nodes) = try run({ n in
            n.a.script([
                .init(expect: "SLOTS", reply: slotsReply([(0, 8_191, n.a.port), (8_192, 16_383, n.b.port)])),
                // Asked again once b has gone, and this time a owns everything.
                .init(expect: "SLOTS", reply: slotsReply([(0, 16_383, n.a.port)])),
                .init(expect: "GET", reply: "$5\r\nsaved\r\n"),
            ])
            n.b.script([.init(expect: "GET", reply: nil, goAway: true)])
        }) { cluster, _ in
            let value = try await cluster.get("foo") ?? "nil"
            return "\(value)|\(cluster.addresses)"
        }
        #expect(nodes.mismatches == [])
        #expect(result == "saved|[\"\(nodes.a.address)\"]")
    }

    /// A command that belongs to no slot -- `PING` -- goes to a node without
    /// the map being loaded for it.
    @Test func aCommandWithNoKeyGoesToAnyNode() throws {
        let (result, nodes) = try run({ n in
            n.a.script([.init(expect: "PING", reply: "+PONG\r\n")])
        }) { cluster, _ in
            "\(try await cluster.send(RedisCommand("PING")).string ?? "nil")"
        }
        #expect(nodes.mismatches == [])
        #expect(nodes.a.received.contains { $0.contains("SLOTS") } == false, "no map is needed to ping")
        #expect(result == "PONG")
    }

    /// Many tasks on two workers through one cluster: the map is loaded and
    /// shared, and every command is answered.
    @Test func tasksOnEveryWorkerShareOneMap() throws {
        let (result, nodes) = try run({ n in
            var steps: [FakeRedis.Step] = [.init(expect: "SLOTS", reply: slotsReply([(0, 16_383, n.a.port)]))]
            steps += Array(repeating: .init(expect: "PING", reply: "+PONG\r\n"), count: 100)
            n.a.script(steps)
        }) { cluster, _ in
            try await cluster.refresh()
            return try await withThrowingTaskGroup(of: Int.self) { group in
                for _ in 0..<10 {
                    group.addTask {
                        var ok = 0
                        for _ in 0..<10 where try await cluster.send(RedisCommand("PING")) == .simpleString("PONG") {
                            ok += 1
                        }
                        return ok
                    }
                }
                return "\(try await group.reduce(0, +))|\(cluster.addresses.count)"
            }
        }
        #expect(nodes.mismatches == [])
        #expect(result == "100|1")
    }
}

/// What may be sent again in a cluster, and what may not. A retry is only
/// safe when the client knows the command did not run.
@Suite("Redis Cluster retries that cannot repeat a write")
struct RedisClusterReplayTests {
    /// A slot half migrated answers some of a batch and redirects the rest.
    /// Only what was redirected goes again: sending the whole batch would
    /// count the first key twice.
    @Test func onlyTheRedirectedPartOfAPipelineGoesAgain() throws {
        let slot = RedisSlots.slot(of: "t")
        let (result, nodes) = try run({ n in
            n.a.script([
                .init(expect: "SLOTS", reply: slotsReply([(0, 16_383, n.a.port)])),
                // One read holding both commands, so one write holding an
                // answer for the first and a redirect for the second.
                .init(expect: "INCR", reply: ":1\r\n-ASK \(slot) \(n.b.address)\r\n"),
            ])
            n.b.script([.init(expect: "ASKING", reply: "+OK\r\n:1\r\n")])
        }) { cluster, _ in
            let replies = try await cluster.pipeline([RedisCommand("INCR", "{t}:one"), RedisCommand("INCR", "{t}:two")])
            return replies.map { "\($0.int ?? -1)" }.joined(separator: ",")
        }
        #expect(nodes.mismatches == [])
        #expect(result == "1,1")
        #expect(nodes.a.received.filter { $0.contains("{t}:one") }.count == 1,
                "the answered command is not sent a second time")
        #expect(nodes.b.received.contains { $0.contains("{t}:one") } == false,
                "and it does not follow the redirect that was not its own")
        #expect(nodes.b.received.contains { $0.contains("{t}:two") }, "the redirected one goes where it was sent")
    }

    /// A node that takes an `INCR` and goes away without answering. Whether it
    /// counted is not knowable, so the command is reported rather than aimed
    /// somewhere else.
    @Test func aWriteThatMayHaveRunIsNotSentAgain() throws {
        let (result, nodes) = try run({ n in
            n.a.script([.init(expect: "SLOTS", reply: slotsReply([(0, 8_191, n.a.port), (8_192, 16_383, n.b.port)]))])
            n.b.script([.init(expect: "INCR", reply: nil, goAway: true)])
        }) { cluster, _ in
            "\(try await cluster.send(RedisCommand("INCR", "foo")).int ?? -1)"
        }
        #expect(nodes.mismatches == [])
        #expect(result.contains("unknownOutcome"), "\(result)")
        #expect(nodes.a.received.contains { $0.contains("INCR") } == false,
                "no other node is asked to do what may already be done")
    }

    /// The same failure with `replay: .anything`, for a cache where counting
    /// twice costs nothing. The command does go again -- which is the point:
    /// it is the caller's choice, not the transport's guess.
    @Test func anythingMayBeSentAgainWhenTheCallerSaysSo() throws {
        let (result, nodes) = try run(replay: .anything, { n in
            n.a.script([
                .init(expect: "SLOTS", reply: slotsReply([(0, 8_191, n.a.port), (8_192, 16_383, n.b.port)])),
                .init(expect: "SLOTS", reply: slotsReply([(0, 16_383, n.a.port)])),
                .init(expect: "INCR", reply: ":7\r\n"),
            ])
            n.b.script([.init(expect: "INCR", reply: nil, goAway: true)])
        }) { cluster, _ in
            "\(try await cluster.send(RedisCommand("INCR", "foo")).int ?? -1)"
        }
        #expect(nodes.mismatches == [])
        #expect(result == "7")
    }
}
