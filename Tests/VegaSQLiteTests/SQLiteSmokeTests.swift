import CVegaSQLite
import Testing
import Vega
@testable import VegaSQLite

@Suite("SQLite on Vega", .enabled(if: vsq_available() != 0, "no libsqlite3"))
struct SQLiteSmokeTests {
    /// In memory, through the writer and the blocking pool, and back.
    @Test func aStatementRunsOnTheBlockingPool() throws {
        let rt = Runtime(workers: 2)
        defer { rt.shutdown() }
        let answer = try rt.block { () async throws -> String in
            let db = try SQLiteDatabase(SQLiteConfiguration(path: ":memory:"))
            defer { db.close() }
            try await db.migrate(["create table t (n integer)"])
            try await db.execute("insert into t values (?), (?)", 1, 2)
            return String(try await db.first(Int.self, "select sum(n) from t") ?? -1)
        }
        #expect(answer == "3")
    }
}
