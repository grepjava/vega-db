import Testing
import Vega
@testable import VegaPostgres

// The pool against a real PostgreSQL: create, read and conflict through typed
// rows, connections reused and shared, a wait for one that gives up, a
// connection the server closed while idle, and transactions -- committed,
// rolled back, failed, and left open.
//
// From Garuda's pool tests, which drove a JSON CRUD API through its routes.
// Here the statements those routes ran are run directly, on a runtime.
// Set VEGA_POSTGRES to run; see IntegrationSupport.

struct PoolUser: Codable, Equatable {
    let id: Int
    let name: String
    let email: String?
    let active: Bool
}

struct PoolFile: Decodable {
    let name: String
    let data: [UInt8]
    let thumbnail: [UInt8]?
}

/// A column the type expects that the query will not return.
struct PoolUserWithAge: Decodable {
    let id: Int
    let age: Int
}

/// Creates the users table, or empties it.
private func setUp(_ pool: PostgresPool) async throws {
    try await pool.execute("""
        create table if not exists vega_pool_users (
            id serial primary key,
            name text not null,
            email text unique,
            active boolean not null default true)
        """)
    try await pool.execute("truncate vega_pool_users restart identity")
}

private func count(_ pool: PostgresPool) async throws -> Int {
    try await pool.first(Int.self, "select count(*) from vega_pool_users") ?? -1
}

/// Inserts a user and returns it, or "conflict" when the email is taken.
private func create(_ pool: PostgresPool, _ name: String, _ email: String?) async throws -> String {
    do {
        let user = try await pool.first(
            PoolUser.self,
            "insert into vega_pool_users (name, email) values ($1, $2) returning id, name, email, active",
            name, email)
        return user.map { "\($0)" } ?? "none"
    } catch let error as PostgresClientError where error.sqlState == "23505" {
        // A unique violation is the caller asking for something that exists,
        // and says so -- branching on the SQLSTATE, never on the localised
        // message.
        return "conflict"
    }
}

@Suite("PostgreSQL pool", .serialized, .enabled(if: postgresTarget != nil, "set VEGA_POSTGRES to run"))
struct PostgresPoolTests {
    @Test func createReadAndConflict() {
        let result = onPool(maxConnections: 4) { pool in
            try await setUp(pool)
            var out: [String] = []
            out.append(try await create(pool, "Ada", "ada@example.com"))
            let read = try await pool.first(PoolUser.self,
                                            "select id, name, email, active from vega_pool_users where id = $1", 1)
            out.append("\(read == PoolUser(id: 1, name: "Ada", email: "ada@example.com", active: true))")
            // A NULL column into an Optional property.
            out.append(try await create(pool, "Grace", nil))
            // Missing is nil from `first`.
            let missing = try await pool.first(PoolUser.self,
                                               "select id, name, email, active from vega_pool_users where id = $1", 99)
            out.append("\(missing == nil)")
            out.append(try await create(pool, "Imposter", "ada@example.com"))
            out.append("\(try await count(pool))")
            return out.joined(separator: "|")
        }
        #expect(result == #"PoolUser(id: 1, name: "Ada", email: Optional("ada@example.com"), active: true)|true|"#
            + #"PoolUser(id: 2, name: "Grace", email: nil, active: true)|true|conflict|2"#)
    }

    @Test func connectionsAreReusedRatherThanOpenedPerStatement() {
        let result = onPool(maxConnections: 4) { pool in
            try await setUp(pool)
            for _ in 0..<20 { _ = try await count(pool) }
            return "\(pool.counts)"
        }
        #expect(result == "(idle: 1, inUse: 0)")
    }

    @Test func statementsBeyondThePoolWaitTheirTurn() {
        // Five slow statements at once, from tasks on two workers, through a
        // pool of two. None may fail for want of a connection, and the pool
        // may never open a third.
        let result = onPool(maxConnections: 2) { pool in
            try await setUp(pool)
            let answers = try await withThrowingTaskGroup(of: Int.self) { group in
                for _ in 0..<5 {
                    group.addTask { try await pool.first(Int.self, "select 1 from pg_sleep(0.05)") ?? -1 }
                }
                var answers: [Int] = []
                for try await answer in group { answers.append(answer) }
                return answers
            }
            let counts = pool.counts
            return "\(answers)|\(counts.idle + counts.inUse <= 2)"
        }
        #expect(result == "[1, 1, 1, 1, 1]|true")
    }

    @Test func bytesRoundTripAsBytea() {
        // Every byte value, an empty blob and a NULL, stored and read back.
        let result = onPool { pool in
            try await pool.execute(
                "create table if not exists vega_pool_files (name text primary key, data bytea not null, thumbnail bytea)")
            try await pool.execute("truncate vega_pool_files")
            let every = (0...255).map { UInt8($0) }
            try await pool.execute("insert into vega_pool_files values ($1, $2, $3)",
                                   "every", every, [UInt8]([0, 0, 1]))
            try await pool.execute("insert into vega_pool_files values ($1, $2, $3)",
                                   "empty", [UInt8](), [UInt8]?.none)
            let files = try await pool.query(PoolFile.self,
                                             "select name, data, thumbnail from vega_pool_files order by name")
            guard files.count == 2 else { return "count \(files.count)" }
            guard files[0].name == "empty", files[0].data == [], files[0].thumbnail == nil else { return "empty wrong" }
            guard files[1].data == every, files[1].thumbnail == [0, 0, 1] else { return "every wrong" }
            // As a scalar, and measured by the server: binary, not text.
            let alone = try await pool.first([UInt8].self, "select data from vega_pool_files where name = $1", "every")
            guard alone == every else { return "scalar wrong" }
            let length = try await pool.first(Int.self, "select length($1)", every)
            guard length == 256 else { return "length \(length ?? -1)" }
            return "ok"
        }
        #expect(result == "ok")
    }

    @Test func aWaitForAConnectionGivesUpAtItsDeadline() {
        // One connection, held far longer than the pool lets anyone wait.
        let result = onPool(maxConnections: 1, acquireTimeout: .milliseconds(50)) { pool in
            try await setUp(pool)
            let holder = Task {
                try await pool.transaction { tx in
                    try await tx.execute("select pg_sleep(0.4)")
                }
                return "held"
            }
            var out: [String] = []
            var held = false
            for _ in 0..<1_000 where !held {
                held = pool.counts == (idle: 0, inUse: 1)
                if !held { try await Runtime.sleep(for: .milliseconds(1)) }
            }
            out.append("\(held)")

            let began = ContinuousClock.now
            do {
                out.append("\(try await count(pool))")
            } catch {
                out.append("\(error)")
            }
            let waited = began.duration(to: .now)
            out.append(waited >= .milliseconds(50) && waited < .milliseconds(350) ? "in time" : "waited \(waited)")

            out.append(try await holder.value)
            // The wait that gave up left nothing queued: the next statement
            // gets the connection back.
            out.append("\(try await count(pool))")
            return out.joined(separator: "|")
        }
        #expect(result == "true|poolTimedOut|in time|held|0")
    }

    @Test func aConnectionTheServerClosedWhileIdleIsNotUsed() {
        // The server ends an idle connection -- an idle timeout, a restart,
        // here pg_terminate_backend from another session. The next statement
        // must not be written into the dead one: it cannot tell afterwards
        // whether the statement ran. The pool checks an idle connection for
        // pending input before handing it out.
        //
        // The termination has to come from outside the pool, while the pool's
        // one connection sits idle. Ending it while a statement uses it would
        // have that statement's own failed read close it, so no dead
        // connection would ever be left idle, and the test would pass with
        // the check deleted.
        let configuration = postgresTarget!
        let result = onPool(maxConnections: 2) { pool in
            try await setUp(pool)
            let pid = try await pool.first(Int.self, "select pg_backend_pid()") ?? -1
            var out = ["\(pool.counts)"]
            let outside = try await PostgresConnection.connect(configuration, tls: nil)
            let rows = try await outside.query("select pg_terminate_backend($1)", values: [PostgresValue(String(pid))])
            out.append(rows.text(row: 0, column: 0) ?? "null")
            outside.close()
            // pg_terminate_backend signals and returns; the backend's goodbye
            // reaches the socket a moment later.
            try await Runtime.sleep(for: .milliseconds(300))
            out.append("\(try await count(pool))")
            return out.joined(separator: "|")
        }
        #expect(result == "(idle: 1, inUse: 0)|t|0")
    }

    // MARK: Transactions

    @Test func aTransactionThatReturnsIsCommitted() {
        let result = onPool(maxConnections: 4) { pool in
            try await setUp(pool)
            try await pool.transaction { tx in
                try await tx.execute("insert into vega_pool_users (name) values ($1)", "one")
                try await tx.execute("insert into vega_pool_users (name) values ($1)", "two")
            }
            return "committed|\(try await count(pool))"
        }
        #expect(result == "committed|2")
    }

    @Test func aTransactionThatThrowsIsRolledBack() {
        struct Abandon: Error {}
        let result = onPool(maxConnections: 4) { pool in
            try await setUp(pool)
            var outcome = ""
            do {
                try await pool.transaction { tx in
                    try await tx.execute("insert into vega_pool_users (name) values ($1)", "gone")
                    throw Abandon()
                }
                outcome = "committed"
            } catch is Abandon {
                outcome = "rolled back"
            }
            return "\(outcome)|\(try await count(pool))"
        }
        #expect(result == "rolled back|0")
    }

    @Test func aFailedTransactionIsNeverReportedAsCommitted() {
        // PostgreSQL answers COMMIT on a failed transaction by rolling back
        // and saying ROLLBACK, not by failing. A body that swallowed the error
        // and returned would be told its work was saved.
        let result = onPool(maxConnections: 4) { pool in
            try await setUp(pool)
            var outcome = ""
            do {
                try await pool.transaction { tx in
                    try await tx.execute("insert into vega_pool_users (name) values ($1)", "lost")
                    _ = try? await tx.execute("select * from no_such_table")
                }
                outcome = "committed"
            } catch let error as PostgresClientError {
                outcome = "refused:\(error.sqlState ?? "")"
            }
            return "\(outcome)|\(try await count(pool))"
        }
        #expect(result == "refused:25P02|0")
    }

    @Test func theServersOwn25P02DoesNotLoseTheConnection() {
        // After a failure inside a transaction, every further statement is
        // refused by the server with 25P02 -- the same code the driver uses
        // for its own refusal. That error must go through the rollback and the
        // release like any other: a pool that took it for the driver's own
        // would lose the connection for good, and with one slot, every
        // statement after it would wait forever.
        let result = onPool(maxConnections: 1) { pool in
            try await setUp(pool)
            var outcome = ""
            do {
                try await pool.transaction { tx in
                    _ = try? await tx.execute("select * from no_such_table")
                    try await tx.execute("insert into vega_pool_users (name) values ($1)", "never")
                }
                outcome = "committed"
            } catch let error as PostgresClientError {
                outcome = "refused:\(error.sqlState ?? "")"
            }
            return "\(outcome)|\(try await count(pool))|\(pool.counts)"
        }
        #expect(result == "refused:25P02|0|(idle: 1, inUse: 0)")
    }

    @Test func aTransactionLeftOpenIsNotHandedToTheNextStatement() {
        // A caller runs `begin` itself and returns. Reused, that connection
        // would carry the open transaction into the next statement, which
        // would run inside it. Closed instead, the server rolls it back.
        //
        // The pool never pins a connection across separate calls, which is
        // what `transaction` is for, so an insert after the `begin` would run
        // on a fresh connection in autocommit. What can be tested is whether
        // the next statement finds itself inside a transaction it never began.
        let result = onPool(maxConnections: 1) { pool in
            try await setUp(pool)
            try await pool.execute("begin")
            let after = "\(pool.counts)"
            // Long enough for a frozen now() to differ from a fresh timestamp.
            try await Runtime.sleep(for: .milliseconds(30))
            // Inside an open transaction now() is frozen at BEGIN, so after a
            // pause the statement's own timestamp is well past it. In
            // autocommit the two are microseconds apart -- not equal, since
            // the extended protocol stamps them at different messages. Hence
            // a margin.
            let stale = try await pool.first(
                Bool.self, "select statement_timestamp() - now() > interval '20 milliseconds'")
            return "\(after)|\(stale == true ? "inside" : "outside")"
        }
        #expect(result == "(idle: 0, inUse: 0)|outside")
    }

    @Test func aRowMissingAColumnTheTypeNeedsIsRefused() {
        // The program's mistake, not the database's.
        let result = onPool { pool in
            try await setUp(pool)
            _ = try await create(pool, "Ada", nil)
            do {
                let rows = try await pool.query(PoolUserWithAge.self, "select id from vega_pool_users")
                return "decoded \(rows.count)"
            } catch let error as PostgresDecodingError {
                return "\(error)"
            }
        }
        #expect(result == #"missingColumn("age")"#)
    }
}
