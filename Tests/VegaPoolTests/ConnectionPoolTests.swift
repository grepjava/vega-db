import Synchronization
import Testing
import Vega
import VegaPool

// The pool with fake connections: reuse, the cap, the acquire timeout,
// failed connects, closing, cancellation, and many workers at once.

private struct Failed: Error, Equatable {
    var reason: PoolError?
}

private final class FakeConnection: PoolableConnection {
    let id: Int
    let healthy = Atomic<Bool>(true)
    let closes = Atomic<Int>(0)
    init(id: Int) { self.id = id }
    var isReusable: Bool { healthy.load(ordering: .relaxed) && closes.load(ordering: .relaxed) == 0 }
    func close() { closes.add(1, ordering: .relaxed) }
}

private final class Opener: Sendable {
    let opened = Atomic<Int>(0)
    let failNext = Atomic<Bool>(false)
    let connections = Mutex<[FakeConnection]>([])

    func open() throws(Failed) -> FakeConnection {
        if failNext.exchange(false, ordering: .relaxed) { throw Failed() }
        let c = FakeConnection(id: opened.add(1, ordering: .relaxed).newValue)
        connections.withLock { $0.append(c) }
        return c
    }
}

private final class Gauge: Sendable {
    let out = Atomic<Int>(0)
    let peak = Atomic<Int>(0)
    let served = Atomic<Int>(0)
}

private func pool(_ opener: Opener, max: Int, timeout: Duration = .seconds(5)) -> ConnectionPool<FakeConnection, Failed> {
    ConnectionPool(maxConnections: max, acquireTimeout: timeout,
                   connect: { () throws(Failed) -> FakeConnection in try opener.open() },
                   failure: { Failed(reason: $0) })
}

@Suite struct ConnectionPoolTests {
    @Test func reusesAConnectionGivenBack() throws {
        let rt = Runtime(workers: 2)
        defer { rt.shutdown() }
        let opener = Opener()
        let p = pool(opener, max: 4)
        let ids = try rt.block { () async throws -> [Int] in
            var ids: [Int] = []
            for _ in 0..<5 {
                ids.append(try await p.withConnection { (c: FakeConnection) throws(Failed) -> Int in c.id })
            }
            return ids
        }
        #expect(ids == [1, 1, 1, 1, 1])
        #expect(opener.opened.load(ordering: .relaxed) == 1)
        #expect(p.idleCount == 1)
        #expect(p.inUseCount == 0)
    }

    @Test func closesWhatComesBackUnusable() throws {
        let rt = Runtime(workers: 1)
        defer { rt.shutdown() }
        let opener = Opener()
        let p = pool(opener, max: 2)
        try rt.block { () async throws in
            try await p.withConnection { (c: FakeConnection) throws(Failed) in c.healthy.store(false, ordering: .relaxed) }
            let id = try await p.withConnection { (c: FakeConnection) throws(Failed) -> Int in c.id }
            #expect(id == 2)
        }
        let first = opener.connections.withLock { $0[0] }
        #expect(first.closes.load(ordering: .relaxed) == 1)
        #expect(p.idleCount == 1)
    }

    @Test func dropsAnIdleConnectionThatWentBad() throws {
        let rt = Runtime(workers: 1)
        defer { rt.shutdown() }
        let opener = Opener()
        let p = pool(opener, max: 2)
        try rt.block { () async throws in
            try await p.withConnection { (_: FakeConnection) throws(Failed) in }
            // Went away while idle: found when it is next taken.
            opener.connections.withLock { $0[0] }.healthy.store(false, ordering: .relaxed)
            let id = try await p.withConnection { (c: FakeConnection) throws(Failed) -> Int in c.id }
            #expect(id == 2)
        }
        #expect(opener.connections.withLock { $0[0] }.closes.load(ordering: .relaxed) == 1)
    }

    @Test func waitsForAConnectionThenTimesOut() throws {
        let rt = Runtime(workers: 2)
        defer { rt.shutdown() }
        let opener = Opener()
        let p = pool(opener, max: 1, timeout: .milliseconds(50))
        let outcome = try rt.block { () async throws -> Failed? in
            let held = try await p.acquire()
            defer { p.release(held) }
            do throws(Failed) {
                _ = try await p.acquire()
                return nil
            } catch {
                return error
            }
        }
        #expect(outcome == Failed(reason: .timedOut))
        #expect(p.inUseCount == 0)
    }

    @Test func aWaiterGetsTheConnectionReleased() throws {
        let rt = Runtime(workers: 2)
        defer { rt.shutdown() }
        let opener = Opener()
        let p = pool(opener, max: 1)
        let id = try rt.block { () async throws -> Int in
            let held = try await p.acquire()
            let waiter = Task(executorPreference: rt.executor) { () async throws -> Int in
                try await p.withConnection { (c: FakeConnection) throws(Failed) -> Int in c.id }
            }
            try await Task.sleep(for: .milliseconds(20))
            p.release(held)
            return try await waiter.value
        }
        #expect(id == 1)
        #expect(opener.opened.load(ordering: .relaxed) == 1)
    }

    @Test func aFailedConnectGivesBackItsPlace() throws {
        let rt = Runtime(workers: 1)
        defer { rt.shutdown() }
        let opener = Opener()
        let p = pool(opener, max: 1, timeout: .milliseconds(100))
        try rt.block { () async throws in
            opener.failNext.store(true, ordering: .relaxed)
            await #expect(throws: Failed()) {
                try await p.withConnection { (_: FakeConnection) throws(Failed) in }
            }
            // Its place is free again, so this connects rather than waiting.
            let id = try await p.withConnection { (c: FakeConnection) throws(Failed) -> Int in c.id }
            #expect(id == 1)
        }
        #expect(p.inUseCount == 0)
    }

    @Test func closeFailsWaitersAndClosesWhatComesBack() throws {
        let rt = Runtime(workers: 2)
        defer { rt.shutdown() }
        let opener = Opener()
        let p = pool(opener, max: 1)
        let outcome = try rt.block { () async throws -> Failed? in
            let held = try await p.acquire()
            let waiter = Task(executorPreference: rt.executor) { () async -> Failed? in
                do throws(Failed) {
                    _ = try await p.acquire()
                    return nil
                } catch {
                    return error
                }
            }
            try await Task.sleep(for: .milliseconds(20))
            p.close()
            p.release(held)
            #expect(held.closes.load(ordering: .relaxed) == 1)
            return await waiter.value
        }
        #expect(outcome == Failed(reason: .closed))
        #expect(p.idleCount == 0)
        rt.block {
            await #expect(throws: Failed(reason: .closed)) {
                _ = try await p.acquire()
            }
        }
    }

    @Test func aCancelledWaiterGivesUpItsPlace() throws {
        let rt = Runtime(workers: 2)
        defer { rt.shutdown() }
        let opener = Opener()
        let p = pool(opener, max: 1)
        let outcome = try rt.block { () async throws -> Failed? in
            let held = try await p.acquire()
            let waiter = Task(executorPreference: rt.executor) { () async -> Failed? in
                do throws(Failed) {
                    _ = try await p.acquire()
                    return nil
                } catch {
                    return error
                }
            }
            try await Task.sleep(for: .milliseconds(20))
            waiter.cancel()
            let outcome = await waiter.value
            p.release(held)
            return outcome
        }
        #expect(outcome == Failed(reason: .cancelled))
        #expect(p.inUseCount == 0)
        #expect(p.idleCount == 1)
    }

    /// Eight workers, 64 tasks, four connections: never more than four out at
    /// once, every task served, nothing left out at the end.
    @Test func manyWorkersShareFourConnections() throws {
        let rt = Runtime(workers: 8)
        defer { rt.shutdown() }
        let opener = Opener()
        let p = pool(opener, max: 4, timeout: .seconds(30))
        let gauge = Gauge()
        try rt.block { () async throws in
            try await withThrowingTaskGroup(of: Void.self) { group in
                for _ in 0..<64 {
                    group.addTask(executorPreference: rt.executor) {
                        for _ in 0..<200 {
                            try await p.withConnection { (c: FakeConnection) throws(Failed) in
                                let now = gauge.out.add(1, ordering: .relaxed).newValue
                                var seen = gauge.peak.load(ordering: .relaxed)
                                while now > seen {
                                    let (exchanged, original) = gauge.peak.compareExchange(
                                        expected: seen, desired: now, ordering: .relaxed)
                                    if exchanged { break }
                                    seen = original
                                }
                                #expect(c.closes.load(ordering: .relaxed) == 0)
                                await Task.yield()
                                gauge.out.subtract(1, ordering: .relaxed)
                                gauge.served.add(1, ordering: .relaxed)
                            }
                        }
                    }
                }
                try await group.waitForAll()
            }
        }
        #expect(gauge.served.load(ordering: .relaxed) == 64 * 200)
        #expect(gauge.peak.load(ordering: .relaxed) <= 4)
        #expect(opener.opened.load(ordering: .relaxed) <= 4)
        #expect(p.inUseCount == 0)
        #expect(p.idleCount == opener.opened.load(ordering: .relaxed))
    }
}
