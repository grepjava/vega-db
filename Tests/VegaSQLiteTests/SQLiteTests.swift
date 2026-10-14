#if canImport(Glibc)
import Glibc
#elseif canImport(Darwin)
import Darwin
#endif
import CVegaSQLite
import Synchronization
import Testing
import Vega
@testable import VegaSQLite

// SQLite through the system's libsqlite3: one connection driven directly, and
// a database shared by tasks on a Vega runtime, with its statements on the
// blocking pool. Every test has a file of its own, removed afterwards. From
// Garuda's SQLite tests, which ran the database in handlers on a test worker.

/// Atomic because the three suites in this file run beside one another, and
/// each of them makes databases. The names they ask for differ, so a torn
/// count never collided, but a plain `var` read and written from several
/// threads is a data race whether or not it is got away with.
private let fileCounter = Atomic<Int>(0)

/// A database path no other test uses, and its removal with its WAL files.
private final class TemporaryDatabase {
    let path: String

    init(_ name: String) {
        let number = fileCounter.wrappingAdd(1, ordering: .relaxed).newValue
        path = "/tmp/vega-sqlite-\(getpid())-\(number)-\(name).db"
        remove()
    }

    deinit { remove() }

    func remove() {
        for suffix in ["", "-wal", "-shm", "-journal"] {
            _ = unlink(path + suffix)
        }
    }
}

/// Which of a database's files exist, for a failure that turns on it.
private func databaseFiles(_ path: String) -> String {
    var out: [String] = []
    for suffix in ["", "-wal", "-shm"] {
        let fd = open(path + suffix, O_RDONLY)
        if fd >= 0 { _ = close(fd) }
        out.append("\(suffix.isEmpty ? "db" : suffix) \(fd >= 0 ? "there" : "missing")")
    }
    return out.joined(separator: ", ")
}

private func pause(_ milliseconds: Int) async {
    try? await Runtime.sleep(for: .milliseconds(milliseconds))
}

/// Polls `condition` every millisecond until it holds, for up to `seconds`.
private func eventually(seconds: Int = 5, _ condition: () -> Bool) async -> Bool {
    let deadline = ContinuousClock.now + .seconds(seconds)
    while !condition() {
        if ContinuousClock.now >= deadline { return false }
        await pause(1)
    }
    return true
}

private func milliseconds(since start: ContinuousClock.Instant) -> Int {
    let (seconds, attoseconds) = start.duration(to: .now).components
    return Int(seconds) * 1000 + Int(attoseconds / 1_000_000_000_000_000)
}

/// Runs `body` on a runtime and returns what it returns.
private func onRuntime<T: Sendable>(workers: Int = 2,
                                    _ body: @escaping @Sendable (Runtime) async throws -> T) throws -> T {
    let rt = Runtime(workers: workers)
    defer { rt.shutdown() }
    return try rt.block { () async throws -> T in try await body(rt) }
}

/// Runs `body` on a runtime with a database, and returns what it returns, or
/// the error it threw, written out.
private func run(_ file: TemporaryDatabase, maxReaders: Int = 4,
                 configure: (inout SQLiteConfiguration) -> Void = { _ in },
                 _ body: @escaping @Sendable (SQLiteDatabase) async throws -> String) -> String {
    var configuration = SQLiteConfiguration(path: file.path)
    configure(&configuration)
    let settled = configuration
    let rt = Runtime(workers: 2)
    defer { rt.shutdown() }
    return rt.block { () async -> String in
        do {
            let db = try SQLiteDatabase(settled, maxReaders: maxReaders)
            defer { db.close() }
            return try await body(db)
        } catch {
            return "threw \(error)"
        }
    }
}

/// A flag one task sets and another waits for.
private final class Flag: Sendable {
    private let value = Atomic<Bool>(false)
    func set() { value.store(true, ordering: .releasing) }
    var isSet: Bool { value.load(ordering: .acquiring) }
}

/// What tasks did, in the order they did it.
private final class Order: Sendable {
    private let names = Mutex<[String]>([])
    func append(_ name: String) { names.withLock { $0.append(name) } }
    var all: [String] { names.withLock { $0 } }
}

/// The answer of a task, or the error it threw, written out.
private func answer(_ task: Task<String, any Error>) async -> String {
    do {
        return try await task.value
    } catch {
        return "\(error)"
    }
}

struct Item: Codable, Equatable {
    let id: Int
    let name: String
    let price: Double?
    let active: Bool
}

/// An application's own type, decoded through `SQLiteCellConvertible` as
/// Garuda's UUID was: from its text, here hex, or its bytes.
struct Token: SQLiteCellConvertible, Equatable {
    let bytes: [UInt8]

    init(_ bytes: [UInt8]) { self.bytes = bytes }

    struct NotAToken: Error {}

    init(sqliteValue: SQLiteValue) throws {
        switch sqliteValue {
        case .blob(let bytes):
            self.bytes = bytes
        case .text(let hex):
            let digits = Array(hex.utf8)
            guard digits.count % 2 == 0 else { throw NotAToken() }
            var bytes: [UInt8] = []
            for i in stride(from: 0, to: digits.count, by: 2) {
                guard let byte = UInt8(String(decoding: digits[i..<i + 2], as: UTF8.self), radix: 16) else {
                    throw NotAToken()
                }
                bytes.append(byte)
            }
            self.bytes = bytes
        default:
            throw NotAToken()
        }
    }
}

/// A row holding one, by column name.
struct Tagged: Decodable, Equatable {
    let id: Int
    let token: Token
    let previous: Token?
}

@Suite("SQLite connection", .serialized, .enabled(if: vsq_available() != 0, "no libsqlite3"))
struct SQLiteConnectionTests {
    private func open(_ file: TemporaryDatabase, _ configure: (inout SQLiteConfiguration) -> Void = { _ in })
        throws -> SQLiteConnection {
        var configuration = SQLiteConfiguration(path: file.path)
        configure(&configuration)
        return try SQLiteConnection.open(configuration, readOnly: false)
    }

    @Test func everyStorageClassRoundTrips() throws {
        let file = TemporaryDatabase("classes")
        let c = try open(file)
        try c.runScript("create table t (v)")
        let values: [SQLiteValue] = [
            .null, .integer(0), .integer(.max), .integer(.min), .real(-2.5), .real(1e300),
            .text(""), .text("héllo ✓"), .text("a\u{0}b"), .blob([]), .blob((0...255).map { UInt8($0) }),
        ]
        for value in values { _ = try c.run("insert into t values (?)", [value]) }
        let rows = try c.run("select v from t order by rowid", [])
        #expect(rows.columns == ["v"])
        #expect((0..<rows.count).map { rows.value(row: $0, column: 0) } == values)
        // Empty text and an empty blob are not NULL.
        #expect(try c.run("select count(*) from t where v is null", []).value(row: 0, column: 0) == .integer(1))
    }

    @Test func onlyOneStatementRuns() throws {
        let file = TemporaryDatabase("one")
        let c = try open(file)
        try c.runScript("create table t (n integer)")
        #expect(throws: SQLiteClientError.multipleStatements) {
            try c.run("insert into t values (1); insert into t values (2)", [])
        }
        #expect(try c.run("select count(*) from t", []).value(row: 0, column: 0) == .integer(0))
        // A trailing semicolon, whitespace and comments are not a statement.
        _ = try c.run("insert into t values (3);  -- the end\n /* really */ ", [])
        #expect(try c.run("select count(*) from t", []).value(row: 0, column: 0) == .integer(1))
        // Nothing at all runs nothing.
        #expect(try c.run("  -- nothing", []).count == 0)
    }

    @Test func valuesMustMatchTheParameters() throws {
        let file = TemporaryDatabase("params")
        let c = try open(file)
        #expect(throws: SQLiteClientError.parameterCount(expected: 2, given: 1)) {
            try c.run("select ?, ?", [.integer(1)])
        }
        // The statement was reset: the next run of it binds afresh.
        #expect(try c.run("select ?, ?", [.integer(1), .text("x")]).value(row: 0, column: 1) == .text("x"))
    }

    @Test func resultsAreHeldToTheirLimits() throws {
        let file = TemporaryDatabase("limits")
        let c = try open(file)
        let series = "with recursive n(i) as (select 1 union all select i + 1 from n where i < 100) select i from n"
        #expect(try c.run(series, [], limits: SQLiteLimits(maxRows: 100, maxResultBytes: 10)).count == 100)
        #expect(throws: SQLiteClientError.tooManyRows) {
            try c.run(series, [], limits: SQLiteLimits(maxRows: 99, maxResultBytes: .max))
        }
        #expect(throws: SQLiteClientError.resultTooLarge) {
            try c.run("select zeroblob(11)", [], limits: SQLiteLimits(maxRows: 10, maxResultBytes: 10))
        }
        #expect(try c.run("select zeroblob(10)", [], limits: SQLiteLimits(maxRows: 10, maxResultBytes: 10)).count == 1)
        // Text counts the same, in bytes of UTF-8.
        #expect(throws: SQLiteClientError.resultTooLarge) {
            try c.run("select 'héllo', 'world'", [], limits: SQLiteLimits(maxRows: 10, maxResultBytes: 10))
        }
        #expect(try c.run("select 'hello', 'world'", [], limits: SQLiteLimits(maxRows: 10, maxResultBytes: 10)).count == 1)
    }

    @Test func refusalsCarrySQLitesCodes() throws {
        let file = TemporaryDatabase("codes")
        let c = try open(file)
        try c.runScript("""
            create table parent (id integer primary key, email text unique not null);
            create table child (parent integer references parent(id));
            insert into parent values (1, 'a@example.com');
            """)
        func refusal(_ sql: String) -> SQLiteClientError? {
            do {
                _ = try c.run(sql, [])
                return nil
            } catch {
                return error
            }
        }
        let unique = refusal("insert into parent values (2, 'a@example.com')")
        #expect(unique?.sqliteCode == 2067)
        #expect(unique?.isConstraintViolation == true)
        #expect(refusal("insert into parent values (3, null)")?.sqliteCode == 1299)
        // Enforced, because the configuration turns foreign keys on.
        #expect(refusal("insert into child values (42)")?.sqliteCode == 787)
        let syntax = refusal("selec 1")
        #expect(syntax?.sqliteCode == 1)
        #expect(syntax?.isConstraintViolation == false)
        if case .sqlite(let failure) = syntax { #expect(failure.message.contains("syntax error")) }
    }

    @Test func aStatementThatCannotWriteReportsNoChanges() throws {
        let file = TemporaryDatabase("changes")
        let c = try open(file)
        try c.runScript("create table t (n integer); insert into t values (1), (2), (3)")
        let update = try c.run("update t set n = n + 1 where n > 1", [])
        #expect(update.affected == 2)
        #expect(!update.readOnly)
        // sqlite3_changes still says 2 here; a select changed nothing.
        let select = try c.run("select n from t", [])
        #expect(select.affected == 0)
        #expect(select.readOnly)
        let insert = try c.run("insert into t values (9)", [])
        #expect(insert.lastInsertRowID == 4)
    }

    @Test func theStatementCacheIsBoundedAndFollowsTheSchema() throws {
        let file = TemporaryDatabase("cache")
        let c = try open(file) { $0.statementCacheCapacity = 3 }
        try c.runScript("create table t (a integer)")
        for i in 0..<10 { _ = try c.run("select \(i)", []) }
        #expect(c.cachedStatements == 3)
        _ = try c.run("insert into t values (1)", [])
        #expect(try c.run("select * from t", []).columns == ["a"])
        // The cached `select *` is prepared again by SQLite after the schema
        // changes, and has another column now.
        try c.runScript("alter table t add column b text default 'x'")
        let after = try c.run("select * from t", [])
        #expect(after.columns == ["a", "b"])
        #expect(after.value(row: 0, column: 1) == .text("x"))
    }

    @Test func aScriptRunsEveryStatementAndStopsAtAFailure() throws {
        let file = TemporaryDatabase("script")
        let c = try open(file)
        try c.runScript("""
            -- a comment first
            create table t (n integer);
            insert into t values (1); /* between */ insert into t values (2);
            """)
        #expect(try c.run("select count(*) from t", []).value(row: 0, column: 0) == .integer(2))
        #expect(throws: SQLiteClientError.self) {
            try c.runScript("insert into t values (3); insert into nowhere values (4); insert into t values (5)")
        }
        #expect(try c.run("select count(*) from t", []).value(row: 0, column: 0) == .integer(3))
    }

    @Test func aFileOpensInWriteAheadLogMode() throws {
        let file = TemporaryDatabase("wal")
        let c = try open(file)
        #expect(try c.run("pragma journal_mode", []).value(row: 0, column: 0) == .text("wal"))
        #expect(try c.run("pragma foreign_keys", []).value(row: 0, column: 0) == .integer(1))
        #expect(try c.run("pragma synchronous", []).value(row: 0, column: 0) == .integer(1))
        let other = TemporaryDatabase("rollback")
        let d = try open(other) { $0.writeAheadLog = false; $0.synchronous = .full; $0.foreignKeys = false }
        #expect(try d.run("pragma journal_mode", []).value(row: 0, column: 0) == .text("delete"))
        #expect(try d.run("pragma foreign_keys", []).value(row: 0, column: 0) == .integer(0))
        #expect(try d.run("pragma synchronous", []).value(row: 0, column: 0) == .integer(2))
    }

    @Test func theBusyTimeoutIsSetInMilliseconds() throws {
        let file = TemporaryDatabase("busy-timeout")
        let c = try open(file) { $0.busyTimeout = .milliseconds(1_250) }
        #expect(try c.run("pragma busy_timeout", []).value(row: 0, column: 0) == .integer(1_250))
    }

    @Test func aMissingFileIsNotCreatedReadOnly() throws {
        let file = TemporaryDatabase("missing")
        var configuration = SQLiteConfiguration(path: file.path)
        configuration.mode = .readOnly
        #expect(throws: SQLiteClientError.self) { try SQLiteConnection.open(configuration, readOnly: true) }
        #expect(throws: SQLiteClientError.self) { try SQLiteDatabase(configuration) }
    }

    /// A reader opens the file read-write, so that it can attach the
    /// shared-memory index a write-ahead log is read through; what stops it
    /// writing is `PRAGMA query_only`, not the open flags. This is that
    /// guarantee, and it is here because it used to be the open flag's to
    /// keep: a statement misrouted to a reader has to be refused, not run.
    ///
    /// The refusal is caught with a plain `catch` and a cast rather than
    /// `catch let error as SQLiteClientError`: Swift 6.3.3 crashes in its
    /// ownership verifier on the typed catch in this function, reliably, and
    /// the two spellings mean the same thing.
    @Test func aReaderIsHeldToReadsBySQLiteItself() throws {
        let file = TemporaryDatabase("query-only")
        let writer = try open(file)
        _ = try writer.run("create table t (n integer)", [])

        var configuration = SQLiteConfiguration(path: file.path)
        configuration.busyTimeout = .milliseconds(500)
        let reader = try SQLiteConnection.open(configuration, readOnly: true)
        #expect(try reader.run("select count(*) from t", []).value(row: 0, column: 0)
                    == .integer(0))
        var refused = 0
        do {
            _ = try reader.run("insert into t values (1)", [])
        } catch {
            refused = Int(((error as? SQLiteClientError)?.sqliteCode ?? 0) & 0xFF)
        }
        // SQLITE_READONLY, which is what the read-only open flag gave before.
        #expect(refused == 8)
        #expect(try writer.run("select count(*) from t", []).value(row: 0, column: 0)
                    == .integer(0))
    }

    /// The file is still not brought into being by a reader: it opens
    /// read-write, and without CREATE.
    @Test func aReaderDoesNotCreateAMissingFile() throws {
        let file = TemporaryDatabase("reader-missing")
        let configuration = SQLiteConfiguration(path: file.path)
        #expect(throws: SQLiteClientError.self) {
            try SQLiteConnection.open(configuration, readOnly: true)
        }
    }
}

@Suite("SQLite decoding", .enabled(if: vsq_available() != 0, "no libsqlite3"))
struct SQLiteDecodingTests {
    private func rows(_ columns: [String], _ values: [SQLiteValue]) -> SQLiteRows {
        SQLiteRows(columns: columns, values: values, affected: 0, lastInsertRowID: 0, readOnly: true)
    }

    private func one<T: Decodable>(_ type: T.Type, _ value: SQLiteValue) throws -> T {
        try decodeAllSQLite(type, rows(["v"], [value]))[0]
    }

    @Test func rowsDecodeByColumnName() throws {
        let result = rows(["active", "name", "id", "price"],
                          [.integer(1), .text("tea"), .integer(7), .null,
                           .integer(0), .text("cake"), .integer(8), .real(3.5)])
        #expect(try decodeAllSQLite(Item.self, result) == [
            Item(id: 7, name: "tea", price: nil, active: true),
            Item(id: 8, name: "cake", price: 3.5, active: false),
        ])
    }

    @Test func conversionsThatLoseNothingAreMade() throws {
        #expect(try one(Double.self, .integer(3)) == 3)
        #expect(try one(Int.self, .real(4)) == 4)
        #expect(try one(Int8.self, .integer(-128)) == -128)
        #expect(try one(String.self, .integer(12)) == "12")
        #expect(try one(Float.self, .real(0.1)) == Float(0.1))
        #expect(try one([UInt8].self, .text("ab")) == [97, 98])
        #expect(try one(Int?.self, .null) == nil)
    }

    /// An application's type decodes from the value as SQLite stored it, both
    /// as the only column and as a property; NULL is nil only in an Optional.
    @Test func anApplicationsTypeDecodesThroughCellConvertible() throws {
        let token = Token([0x01, 0xab, 0xff])
        #expect(try one(Token.self, .text("01abff")) == token)
        #expect(try one(Token.self, .blob([0x01, 0xab, 0xff])) == token)
        #expect(try one(Token?.self, .null) == nil)
        #expect(throws: SQLiteDecodingError.null(column: "v")) { try one(Token.self, .null) }
        #expect(throws: Token.NotAToken.self) { try one(Token.self, .integer(1)) }
        #expect(throws: Token.NotAToken.self) { try one(Token.self, .text("xyz")) }
        let result = rows(["id", "token", "previous"],
                          [.integer(1), .text("01abff"), .null,
                           .integer(2), .blob([0x02]), .text("01abff")])
        #expect(try decodeAllSQLite(Tagged.self, result) == [
            Tagged(id: 1, token: token, previous: nil),
            Tagged(id: 2, token: Token([0x02]), previous: token),
        ])
    }

    @Test func conversionsThatWouldGuessAreRefused() throws {
        #expect(throws: SQLiteDecodingError.notConvertible(column: "v", value: "4.5", expected: "Int")) {
            try one(Int.self, .real(4.5))
        }
        #expect(throws: SQLiteDecodingError.notConvertible(column: "v", value: "300", expected: "UInt8")) {
            try one(UInt8.self, .integer(300))
        }
        #expect(throws: SQLiteDecodingError.notConvertible(column: "v", value: "12", expected: "Int")) {
            try one(Int.self, .text("12"))
        }
        #expect(throws: SQLiteDecodingError.notConvertible(column: "v", value: "2", expected: "Bool")) {
            try one(Bool.self, .integer(2))
        }
        #expect(throws: SQLiteDecodingError.null(column: "v")) { try one(String.self, .null) }
        #expect(throws: SQLiteDecodingError.missingColumn("active")) {
            try decodeAllSQLite(Item.self, rows(["id", "name"], [.integer(1), .text("x")]))
        }
        // A missing column into an Optional is nil, as synthesized Decodable
        // asks with decodeIfPresent.
        #expect(try decodeAllSQLite(Item.self, rows(["id", "name", "active"], [.integer(1), .text("x"), .integer(1)]))
                == [Item(id: 1, name: "x", price: nil, active: true)])
        #expect(throws: SQLiteDecodingError.self) {
            try decodeAllSQLite(Int.self, rows(["a", "b"], [.integer(1), .integer(2)]))
        }
    }

    @Test func swiftValuesBindAsSQLiteStoresThem() {
        #expect(true.sqliteValue == .integer(1))
        #expect(Float(0.1).sqliteValue == .real(0.1))
        #expect(Int?.none.sqliteValue == .null)
        #expect([UInt8]().sqliteValue == .blob([]))
        #expect("x"[...].sqliteValue == .text("x"))
    }
}

@Suite("SQLite database", .serialized, .enabled(if: vsq_available() != 0, "no libsqlite3"))
struct SQLiteDatabaseTests {
    private let schema = [
        "create table items (id integer primary key, name text not null unique, price real, active integer not null default 1)",
    ]

    @Test func crudInTasks() throws {
        let file = TemporaryDatabase("crud")
        let result = run(file) { [schema] db in
            try await db.migrate(schema)
            var out: [String] = []
            let created = try await db.first(Item.self, "insert into items (name, price) values (?, ?) returning *",
                                             "tea", 2.5)
            out.append("\(created.map { "\($0.id) \($0.name) \($0.price ?? 0) \($0.active)" } ?? "nil")")
            try await db.execute("insert into items (name) values (?)", "cake")
            do {
                try await db.execute("insert into items (name) values (?)", "tea")
                out.append("duplicate inserted")
            } catch let error as SQLiteClientError where error.isConstraintViolation {
                out.append("conflict \(error.sqliteCode ?? 0)")
            }
            out.append("\(try await db.execute("update items set active = 0 where name = ?", "cake"))")
            let items = try await db.query(Item.self, "select id, name, price, active from items order by id")
            out.append(items.map { "\($0.name):\($0.active)" }.joined(separator: ","))
            out.append("\(try await db.first(Int.self, "select count(*) from items") ?? -1)")
            out.append("\(try await db.first(Item.self, "select * from items where id = ?", 99).map { $0.name } ?? "none")")
            return out.joined(separator: "|")
        }
        #expect(result == "1 tea 2.5 true|conflict 2067|1|tea:true,cake:false|2|none")
    }

    @Test func anApplicationsTypeRoundTrips() throws {
        let file = TemporaryDatabase("cell")
        let result = run(file) { db in
            try await db.migrate(["create table tokens (id integer primary key, token blob not null, previous text)"])
            try await db.execute("insert into tokens values (1, ?, null)", [UInt8]([0x01, 0xab]))
            try await db.execute("insert into tokens values (2, ?, '01ab')", [UInt8]([0x02]))
            let rows = try await db.query(Tagged.self, "select id, token, previous from tokens order by id")
            let only = try await db.first(Token.self, "select previous from tokens where id = 2")
            return "\(rows.map { "\($0.id):\($0.token.bytes):\($0.previous?.bytes ?? [])" }.joined(separator: ","))|"
                + "\(only?.bytes ?? [])"
        }
        #expect(result == "1:[1, 171]:[],2:[2]:[1, 171]|[1, 171]")
    }

    @Test func statementsThatCannotWriteMoveToReaders() throws {
        let file = TemporaryDatabase("readers")
        let result = run(file, maxReaders: 2) { [schema] db in
            try await db.migrate(schema)
            var out: [String] = []
            func openReaders() -> Int { db.counts.idleReaders + db.counts.readersInUse }
            try await db.execute("insert into items (name) values ('a')")
            out.append("\(openReaders())")
            // The first run is on the writer, which learns it cannot write.
            _ = try await db.first(Int.self, "select count(*) from items")
            out.append("\(openReaders())")
            // The second is on a reader, and sees what the writer committed.
            out.append("\(try await db.first(Int.self, "select count(*) from items") ?? -1)")
            out.append("\(openReaders())")
            try await db.execute("insert into items (name) values ('b')")
            out.append("\(try await db.first(Int.self, "select count(*) from items") ?? -1)")
            // A write is never learned as read-only, however often it runs.
            try await db.execute("insert into items (name) values ('c')")
            out.append("\(db.knownReadOnly)")
            // The reader went back idle.
            out.append("\(db.counts.idleReaders) \(db.counts.readersInUse)")
            return out.joined(separator: "|")
        }
        #expect(result == "0|0|1|1|2|1|1 0")
    }

    @Test func aMemoryDatabaseUsesOneConnection() throws {
        let result = run(TemporaryDatabase("unused"), configure: { $0.path = ":memory:" }) { [schema] db in
            try await db.migrate(schema)
            try await db.execute("insert into items (name) values ('a')")
            _ = try await db.first(Int.self, "select count(*) from items")
            let count = try await db.first(Int.self, "select count(*) from items") ?? -1
            return "\(db.maxReaders) \(db.counts.idleReaders + db.counts.readersInUse) \(db.knownReadOnly) \(count)"
        }
        #expect(result == "0 0 0 1")
    }

    @Test func aTransactionCommitsOrRollsBack() throws {
        let file = TemporaryDatabase("tx")
        let result = run(file) { [schema] db in
            try await db.migrate(schema)
            struct Abandon: Error {}
            var out: [String] = []
            try await db.transaction { tx in
                try await tx.execute("insert into items (name) values ('one')")
                try await tx.execute("insert into items (name) values ('two')")
            }
            out.append("\(try await db.first(Int.self, "select count(*) from items") ?? -1)")
            do {
                try await db.transaction { tx in
                    try await tx.execute("insert into items (name) values ('gone')")
                    throw Abandon()
                }
            } catch is Abandon {
                out.append("abandoned")
            }
            out.append("\(try await db.first(Int.self, "select count(*) from items") ?? -1)")
            // A failed statement undoes only itself, as in SQLite; the body
            // caught it and returned, so the rest commits.
            let inside = try await db.transaction { tx -> Int in
                try await tx.execute("insert into items (name) values ('three')")
                _ = try? await tx.execute("insert into items (name) values ('one')")
                return try await tx.first(Int.self, "select count(*) from items") ?? -1
            }
            out.append("\(inside)")
            out.append("\(try await db.first(Int.self, "select count(*) from items") ?? -1)")
            out.append("\(db.counts.writerBusy)")
            return out.joined(separator: "|")
        }
        #expect(result == "2|abandoned|2|3|3|false")
    }

    @Test func aTransactionLeftOpenIsRolledBackAndRefused() throws {
        let file = TemporaryDatabase("left-open")
        let result = run(file) { [schema] db in
            try await db.migrate(schema)
            var out: [String] = []
            do {
                try await db.execute("begin")
                out.append("left open")
            } catch let error as SQLiteClientError {
                out.append("\(error)")
            }
            // Run on the writer, then learned as read-only: begin twice, so
            // the second goes to a reader, which must refuse it the same way.
            do {
                try await db.execute("begin")
                out.append("left open")
            } catch let error as SQLiteClientError {
                out.append("\(error)")
            }
            do {
                try await db.execute("insert into items (name) values ('x')")
                out.append("inserted")
            } catch {
                out.append("\(error)")
            }
            // Both were rolled back: the insert commits on its own.
            try await db.transaction { tx in try await tx.execute("insert into items (name) values ('y')") }
            out.append("\(try await db.first(Int.self, "select count(*) from items") ?? -1)")
            return out.joined(separator: "|")
        }
        #expect(result == "transactionLeftOpen|transactionLeftOpen|inserted|2")
    }

    @Test func migrationsRunOnceInOrder() throws {
        let file = TemporaryDatabase("migrate")
        let first = [
            "create table a (n integer)",
            "insert into a values (1); insert into a values (2)",
        ]
        let result = run(file) { db in
            var out: [String] = []
            // How many ran, each time.
            out.append("\(try await db.migrate(first))")
            out.append("\(try await db.migrate(first))")
            out.append("\(try await db.first(Int.self, "select count(*) from a") ?? -1)")
            out.append("\(try await db.first(Int.self, "pragma user_version") ?? -1)")
            out.append("\(try await db.migrate(first + ["alter table a add column m text default 'z'"]))")
            out.append("\(try await db.first(Int.self, "pragma user_version") ?? -1)")
            // A failing script undoes the whole run, scripts before it
            // included, and leaves the version where it was.
            do {
                try await db.migrate(first + ["alter table a add column m text default 'z'",
                                              "create table b (n integer)",
                                              "insert into nowhere values (1)"])
                out.append("migrated")
            } catch let error as SQLiteClientError {
                out.append("failed \(error.sqliteCode ?? 0)")
            }
            out.append("\(try await db.first(Int.self, "pragma user_version") ?? -1)")
            out.append("\(try await db.first(Int.self, "select count(*) from sqlite_master where name = 'b'") ?? -1)")
            // The writer was given back, out of its transaction.
            out.append("\(db.counts.writerBusy)")
            // A database from a newer program is refused.
            do {
                try await db.migrate(first)
                out.append("migrated")
            } catch {
                out.append("\(error)")
            }
            return out.joined(separator: "|")
        }
        #expect(result == "2|0|2|2|1|3|failed 1|3|0|false|unknownSchemaVersion(found: 3, known: 2)")
    }

    @Test func tasksWaitTheirTurnForTheWriter() throws {
        let file = TemporaryDatabase("turns")
        let path = file.path
        let result = try onRuntime { rt in
            let db = try SQLiteDatabase(SQLiteConfiguration(path: path))
            defer { db.close() }
            try await db.migrate(["create table n (v integer)"])
            let holders = (0..<5).map { _ in
                rt.spawn { () async throws -> String in
                    try await db.transaction { tx in
                        try await tx.execute("insert into n values (1)")
                        await pause(30)
                    }
                    return "held"
                }
            }
            var answers: [String] = []
            for holder in holders { answers.append(await answer(holder)) }
            return answers.joined(separator: ",")
                + "|\(try await db.first(Int.self, "select count(*) from n") ?? -1)"
        }
        #expect(result == "held,held,held,held,held|5")
    }

    @Test func theWriterGoesToTheTaskThatWaitedForIt() throws {
        // Given back and only its waiter woken, the writer went to whoever
        // asked next before the waiter ran: here the task that gave it back,
        // starting its next transaction, every time, until it had run all
        // four. Handed over, the waiting task is next. VegaPool's pool is a
        // FIFO semaphore that hands a released permit straight to the task
        // queued for it, so the task that gave it back queues behind.
        let file = TemporaryDatabase("handover")
        let path = file.path
        let order = Order()
        let result = try onRuntime { rt in
            let db = try SQLiteDatabase(SQLiteConfiguration(path: path))
            defer { db.close() }
            /// Set as the other task asks for the writer.
            let asking = Flag()
            let greedy = rt.spawn { () async throws -> String in
                for i in 0..<4 {
                    try await db.transaction { _ in
                        order.append("greedy\(i)")
                        // The first holds the writer until the other task
                        // has asked for it, and then long enough for it to
                        // have queued.
                        if i == 0 { _ = await eventually { asking.isSet } }
                        await pause(20)
                    }
                }
                return "greedy"
            }
            guard await eventually({ !order.all.isEmpty }) else { return "greedy never began" }
            let other = rt.spawn { () async throws -> String in
                asking.set()
                try await db.transaction { _ in order.append("other") }
                return "other"
            }
            return "\(await answer(other)) \(await answer(greedy))"
        }
        #expect(result == "other greedy")
        #expect(order.all == ["greedy0", "other", "greedy1", "greedy2", "greedy3"], "\(order.all)")
    }

    @Test func aWaitForTheWriterGivesUpAtItsDeadline() throws {
        let file = TemporaryDatabase("deadline")
        let path = file.path
        let result = try onRuntime { rt in
            var configuration = SQLiteConfiguration(path: path)
            configuration.acquireTimeout = .milliseconds(50)
            let db = try SQLiteDatabase(configuration)
            defer { db.close() }
            let holder = rt.spawn { () async throws -> String in
                try await db.transaction { _ in await pause(400) }
                return "held"
            }
            guard await eventually({ db.counts.writerBusy }) else { return "the writer was never taken" }
            let began = ContinuousClock.now
            let refused: String
            do {
                refused = String(try await db.execute("create table if not exists t (n integer)"))
            } catch {
                refused = "\(error)"
            }
            let waited = milliseconds(since: began)
            let held = await answer(holder)
            let written = String(try await db.execute("create table if not exists t (n integer)"))
            return "\(refused)|\(waited >= 50 && waited < 350 ? "in time" : "waited \(waited) ms")|\(held)|\(written)"
        }
        #expect(result == "poolTimedOut|in time|held|0")
    }

    /// Garuda's request ended when its client went away; here a task is
    /// cancelled. A task cancelled while it waits for the writer stops
    /// waiting at once, and the writer's holder is not disturbed.
    @Test func aCancelledWaitForTheWriterEndsAtOnce() throws {
        let file = TemporaryDatabase("cancelled")
        let path = file.path
        let result = try onRuntime { rt in
            let db = try SQLiteDatabase(SQLiteConfiguration(path: path))
            defer { db.close() }
            try await db.migrate(["create table n (v integer)"])
            let holder = rt.spawn { () async throws -> String in
                try await db.transaction { tx in
                    try await tx.execute("insert into n values (1)")
                    await pause(400)
                }
                return "held"
            }
            guard await eventually({ db.counts.writerBusy }) else { return "the writer was never taken" }
            let waiter = rt.spawn { () async throws -> String in
                String(try await db.execute("insert into n values (2)"))
            }
            await pause(20)
            let began = ContinuousClock.now
            waiter.cancel()
            let cancelled = await answer(waiter)
            let waited = milliseconds(since: began)
            let held = await answer(holder)
            let count = try await db.first(Int.self, "select count(*) from n") ?? -1
            return "\(cancelled)|\(waited < 200 ? "at once" : "waited \(waited) ms")|\(held)|\(count)"
        }
        #expect(result == "cancelled|at once|held|1")
    }

    @Test func anotherConnectionsLockIsWaitedForOffTheWorker() throws {
        // A second database on the same file stands in for another process:
        // its write lock is SQLite's, not this pool's. While a statement
        // waits out the busy timeout on a blocking thread, other tasks on the
        // runtime still run -- on its only worker, here.
        let file = TemporaryDatabase("busy")
        let path = file.path
        let result = try onRuntime(workers: 1) { rt in
            let db = try SQLiteDatabase(SQLiteConfiguration(path: path))
            defer { db.close() }
            try await db.migrate(["create table n (v integer)"])
            var otherConfiguration = SQLiteConfiguration(path: path)
            otherConfiguration.busyTimeout = .milliseconds(50)
            let other = try SQLiteDatabase(otherConfiguration)
            defer { other.close() }
            var out: [String] = []

            /// Set once the other database has its write lock.
            let otherHeld = Flag()
            let holder = rt.spawn { () async throws -> String in
                try await other.transaction { tx in
                    try await tx.execute("insert into n values (1)")
                    otherHeld.set()
                    await pause(300)
                }
                return "released"
            }
            guard await eventually({ otherHeld.isSet }) else { return "the other never held its lock" }

            // A task that keeps counting while the write waits.
            let ticks = Atomic<Int>(0)
            let stop = Flag()
            let ticker = rt.spawn { () async -> Void in
                while !stop.isSet {
                    ticks.add(1, ordering: .relaxed)
                    await pause(5)
                }
            }
            let began = ContinuousClock.now
            // The write waits on SQLite's lock; the worker does not.
            do {
                try await db.execute("insert into n values (2)")
                out.append("written")
            } catch {
                out.append("\(error)")
            }
            let waited = milliseconds(since: began)
            let counted = ticks.load(ordering: .relaxed)
            stop.set()
            await ticker.value
            out.append(waited >= 150 ? "waited" : "waited only \(waited) ms")
            out.append(counted >= 10 ? "ticked" : "ticked \(counted) times in \(waited) ms")
            out.append(await answer(holder))

            // The other way round, with a busy timeout far shorter than the
            // hold: the wait gives up with SQLITE_BUSY (5).
            let mainHeld = Flag()
            let mainHolder = rt.spawn { () async throws -> String in
                try await db.transaction { tx in
                    try await tx.execute("insert into n values (4)")
                    mainHeld.set()
                    await pause(300)
                }
                return "released"
            }
            // Until the insert has run and holds the lock, not for a fixed
            // time: on a slow runner 50 ms was not always enough, and the
            // write that should have been refused went through.
            guard await eventually({ mainHeld.isSet }) else { return "the main database never held its lock" }
            let heldAt = ContinuousClock.now
            let refused: String
            do {
                try await other.execute("insert into n values (3)")
                refused = "written"
            } catch let error as SQLiteClientError {
                refused = "busy \(error.sqliteCode ?? 0)"
            } catch {
                refused = "\(error)"
            }
            let heldFor = milliseconds(since: heldAt)
            #if os(Linux)
            out.append(refused == "busy 5" ? "refused" : "answered \(refused) \(heldFor) ms after the lock was taken")
            #else
            // macOS's own libsqlite3 has been seen to wait out the whole
            // hold, well past the 50 ms busy timeout, and then write: the
            // worker is still free meanwhile, which is what this test is
            // about.
            out.append(refused == "busy 5" || (refused == "written" && heldFor >= 250)
                       ? "refused" : "answered \(refused) \(heldFor) ms after the lock was taken")
            #endif
            out.append(await answer(mainHolder))
            do {
                try await other.execute("insert into n values (3)")
                out.append("written")
            } catch {
                out.append("\(error)")
            }
            return out.joined(separator: "|")
        }
        #expect(result == "written|waited|ticked|released|refused|released|written")
    }

    @Test func aTransactionThatReadsFirstStillWrites() throws {
        // A transaction reads, then writes. Begun deferred, it would hold a
        // read snapshot, and another connection committing meanwhile would
        // make its write fail at once with SQLITE_BUSY_SNAPSHOT -- no busy
        // timeout helps. Begun IMMEDIATE, it holds the write lock from the
        // start, and the other connection waits for it instead.
        let file = TemporaryDatabase("immediate")
        let path = file.path
        let result = try onRuntime { rt in
            let db = try SQLiteDatabase(SQLiteConfiguration(path: path))
            defer { db.close() }
            try await db.migrate(["create table n (v integer)"])
            let other = try SQLiteDatabase(SQLiteConfiguration(path: path))
            defer { other.close() }
            let first = rt.spawn { () async throws -> String in
                try await db.transaction { tx in
                    let count = try await tx.first(Int.self, "select count(*) from n") ?? -1
                    await pause(150)
                    try await tx.execute("insert into n values (?)", count)
                }
                return "committed"
            }
            await pause(50)
            let second = rt.spawn { () async throws -> String in
                try await other.execute("insert into n values (100)")
                return "written"
            }
            return "\(await answer(first)) \(await answer(second))"
        }
        #expect(result == "committed written")
    }

    @Test func connectionsOpeningANewFileTogetherAllOpen() throws {
        // Every process opens the database as it starts, perhaps at the same
        // moment. Switching a new file to WAL takes an exclusive lock SQLite
        // does not wait for through the busy timeout; without retrying, all
        // but one of the opens could fail with SQLITE_BUSY.
        for round in 0..<10 {
            let file = TemporaryDatabase("together-\(round)")
            let configuration = SQLiteConfiguration(path: file.path)
            let result = try onRuntime { _ in
                await withTaskGroup(of: Result<SQLiteConnection, SQLiteClientError>.self) { group -> String in
                    for _ in 0..<8 {
                        group.addTask {
                            do throws(SQLiteClientError) {
                                return .success(try await Runtime.blocking {
                                    () throws(SQLiteClientError) -> SQLiteConnection in
                                    try SQLiteConnection.open(configuration, readOnly: false)
                                })
                            } catch {
                                return .failure(error)
                            }
                        }
                    }
                    var opened = 0
                    var failures: [String] = []
                    for await result in group {
                        switch result {
                        case .success(let c): opened += 1; c.close()
                        case .failure(let error): failures.append("\(error)")
                        }
                    }
                    return failures.isEmpty ? "\(opened)" : failures.joined(separator: ",")
                }
            }
            #expect(result == "8", "round \(round)")
        }
    }

    @Test func aReadOnlyDatabaseRefusesWrites() throws {
        let file = TemporaryDatabase("read-only")
        let made = run(file) { db in
            try await db.migrate(["create table t (n integer); insert into t values (7)"])
            return "made"
        }
        #expect(made == "made")
        let result = run(file, configure: { $0.mode = .readOnly }) { db in
            var out: [String] = []
            out.append("\(try await db.first(Int.self, "select n from t") ?? -1)")
            do {
                try await db.execute("insert into t values (8)")
                out.append("written")
            } catch let error as SQLiteClientError {
                out.append("refused \((error.sqliteCode ?? 0) & 0xFF)")
            }
            return out.joined(separator: "|")
        }
        #expect(result == "7|refused 8")
    }

    @Test func aClosedDatabaseRefusesStatements() throws {
        let file = TemporaryDatabase("closed")
        let path = file.path
        let result = run(file) { db in
            do {
                // The first of these runs on the writer, which is where the
                // pool learns that the statement only reads; the second is
                // the first to want a reader, and so the first to open a
                // second connection. That is where macOS failed, and a
                // read-only connection cannot create the -shm file a WAL
                // database is read through -- which is why a reader no longer
                // opens read-only. If this ever fails again, which of those
                // files exist is the thing to know.
                _ = try await db.first(Int.self, "select 1")
                _ = try await db.first(Int.self, "select 1")
            } catch {
                return "a read failed: \(error); files: \(databaseFiles(path))"
            }
            db.close()
            var out: [String] = []
            do {
                _ = try await db.first(Int.self, "select 1")
                out.append("ran")
            } catch {
                out.append("\(error) \(db.counts.idleReaders)")
            }
            // A write, on the writer, the same.
            do {
                _ = try await db.execute("create table t (n integer)")
                out.append("ran")
            } catch {
                out.append("\(error)")
            }
            do {
                try await db.migrate(["create table t (n integer)"])
                out.append("ran")
            } catch {
                out.append("\(error)")
            }
            return out.joined(separator: "|")
        }
        #expect(result == "closed 0|closed|closed")
    }
}
