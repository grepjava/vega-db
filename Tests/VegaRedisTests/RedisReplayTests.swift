import Testing
import VegaRedisProtocol
@testable import VegaRedis

// What may be sent again, and what may not. A retry is only safe when the
// client knows the command did not run: a connection that fails before its
// first byte gives that knowledge, and one that fails with the bytes already
// out gives nothing. The cluster and sentinel cases are with their scripted
// tests.

@Suite("Redis retries that cannot repeat a write")
struct RedisReplayTests {
    @Test func readsAreToldFromWrites() {
        #expect(RedisReads.only(RedisCommand("GET", "k")))
        #expect(RedisReads.only(RedisCommand("get", "k")), "the name is read whatever its case")
        #expect(RedisReads.only(RedisCommand("MGET", "a", "b")))
        #expect(RedisReads.only(RedisCommand("ZRANGEBYSCORE", "z", "0", "1")))
        #expect(RedisReads.only(RedisCommand("XREAD", "COUNT", "1", "STREAMS", "s", "0")))
        #expect(!RedisReads.only(RedisCommand("INCR", "k")), "the one a retry must never repeat")
        #expect(!RedisReads.only(RedisCommand("SET", "k", "v")))
        #expect(!RedisReads.only(RedisCommand("EXEC")), "a lost EXEC reply is not a rollback")
        #expect(!RedisReads.only(RedisCommand("XREADGROUP")), "it moves the group's cursor")
        #expect(!RedisReads.only(RedisCommand("SORT", "k", "STORE", "d")), "SORT_RO is the read")
        #expect(RedisReads.only(RedisCommand("SORT_RO", "k")))
        #expect(!RedisReads.only(RedisCommand("NEWCOMMAND", "k")),
                "anything unrecognised is a write, which is the safe way to be wrong")
        #expect(RedisReads.only([RedisCommand("GET", "a"), RedisCommand("TTL", "a")]))
        #expect(!RedisReads.only([RedisCommand("GET", "a"), RedisCommand("DEL", "a")]),
                "one write in a batch is a batch that cannot go again")
    }

    @Test func aFailureBeforeTheFirstByteIsAlwaysSentAgain() {
        let write = [RedisCommand("INCR", "k")]
        let read = [RedisCommand("GET", "k")]
        // Nothing reached the server, so nothing can be repeated.
        #expect(RedisReplay.reads.allows(.closed, write))
        #expect(RedisReplay.nothing.allows(.timedOut, write))
        // The bytes went out, and what the server did with them is unknown.
        #expect(!RedisReplay.reads.allows(.unknownOutcome(.closed), write))
        #expect(RedisReplay.reads.allows(.unknownOutcome(.closed), read))
        #expect(RedisReplay.anything.allows(.unknownOutcome(.timedOut), write))
        #expect(!RedisReplay.nothing.allows(.unknownOutcome(.timedOut), read))
    }

    @Test func theCauseIsReadableThroughTheWrapper() {
        let error = RedisClientError.unknownOutcome(.timedOut)
        #expect(error.cause == .timedOut)
        #expect(error.mayHaveRun)
        #expect(!RedisClientError.timedOut.mayHaveRun)
        #expect(RedisClientError.timedOut.cause == .timedOut)
    }
}
