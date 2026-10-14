//===----------------------------------------------------------------------===//
// A SQLite database: a writer and a few readers, shared by tasks on any
// worker.
//
//     let db = try SQLiteDatabase(SQLiteConfiguration(path: "/var/lib/app/app.db"))
//     try await db.migrate([
//         "create table users (id integer primary key, name text not null)",
//         "alter table users add column email text",
//     ])
//     let user = try await db.first(User.self, "select id, name, email from users where id = ?", id)
//
// SQLite lets one connection write at a time, across every process that has
// the file open, and with write-ahead logging lets any number read while it
// does. So a database keeps one connection that writes and up to `maxReaders`
// that only read, each a VegaPool pool. A statement goes to a reader once
// SQLite has said, on the writer, that it cannot write; until then, and for
// anything that can, it goes to the writer. A transaction holds the writer
// from BEGIN IMMEDIATE to COMMIT, so it never has to upgrade a read lock into
// a write lock halfway through -- the upgrade SQLite answers with
// SQLITE_BUSY at once rather than waiting.
//
// Every statement runs on Vega's blocking pool, never on a worker: SQLite
// reads and writes files on the calling thread, and waits there for other
// processes' locks, for up to `busyTimeout`. From Garuda's SQLiteDatabase,
// which belonged to one worker thread.
//===----------------------------------------------------------------------===//

import Synchronization
import Vega
import VegaPool

public final class SQLiteDatabase: Sendable {
    public let configuration: SQLiteConfiguration
    /// Connections that only read, opened as they are needed. 0 in memory,
    /// where another connection would be another database.
    public let maxReaders: Int

    private let writer: ConnectionPool<SQLiteConnection, SQLiteClientError>
    private let readers: ConnectionPool<SQLiteConnection, SQLiteClientError>?
    /// SQL SQLite has said cannot write, learned on the writer.
    private let readOnlySQL = Mutex<Set<String>>([])

    private var limits: SQLiteLimits {
        SQLiteLimits(maxRows: configuration.maxRows, maxResultBytes: configuration.maxResultBytes)
    }

    /// Opens the database and its writer now, so a file that cannot be opened
    /// fails here rather than at the first statement. Blocks while it opens:
    /// file I/O, and perhaps a wait on another process's lock.
    public init(_ configuration: SQLiteConfiguration, maxReaders: Int = 4) throws(SQLiteClientError) {
        self.configuration = configuration
        let readOnly = configuration.mode == .readOnly
        let maxReaders = configuration.isMemory ? 0 : max(0, maxReaders)
        self.maxReaders = maxReaders
        // The writer opened here is the pool's first; one it drops -- left
        // broken -- is opened again, on the blocking pool.
        let first = Mutex<SQLiteConnection?>(try SQLiteConnection.open(configuration, readOnly: readOnly))
        let failure: @Sendable (PoolError) -> SQLiteClientError = { error in
            switch error {
            case .timedOut: return .poolTimedOut
            case .cancelled: return .cancelled
            case .closed: return .closed
            }
        }
        writer = ConnectionPool(
            maxConnections: 1, acquireTimeout: configuration.acquireTimeout,
            connect: { () async throws(SQLiteClientError) -> SQLiteConnection in
                if let opened = first.withLock({ $0.take() }) { return opened }
                return try await Runtime.blocking { () throws(SQLiteClientError) -> SQLiteConnection in
                    try SQLiteConnection.open(configuration, readOnly: readOnly)
                }
            },
            failure: failure)
        readers = maxReaders == 0 ? nil : ConnectionPool(
            maxConnections: maxReaders, acquireTimeout: configuration.acquireTimeout,
            connect: { () async throws(SQLiteClientError) -> SQLiteConnection in
                try await Runtime.blocking { () throws(SQLiteClientError) -> SQLiteConnection in
                    try SQLiteConnection.open(configuration, readOnly: true)
                }
            },
            failure: failure)
    }

    deinit { close() }

    // MARK: Statements

    /// Every row, decoded.
    public func query<Row: Decodable>(_ type: Row.Type, _ sql: String,
                                      _ values: any SQLiteBindable...) async throws -> [Row] {
        try decodeAllSQLite(type, try await run(sql, values.map(\.sqliteValue)))
    }

    /// The first row decoded, or nil if there were none.
    public func first<Row: Decodable>(_ type: Row.Type, _ sql: String,
                                      _ values: any SQLiteBindable...) async throws -> Row? {
        let rows = try await run(sql, values.map(\.sqliteValue))
        guard rows.count > 0 else { return nil }
        return try decodeSQLiteRow(type, rows, 0, sqliteColumnIndex(rows))
    }

    /// Runs a statement and returns how many rows it changed.
    @discardableResult
    public func execute(_ sql: String, _ values: any SQLiteBindable...) async throws -> Int {
        try await run(sql, values.map(\.sqliteValue)).affected
    }

    /// Runs a statement and returns its whole result: the rows as SQLite
    /// values, the rows changed and the last rowid inserted.
    public func rows(_ sql: String, _ values: any SQLiteBindable...) async throws(SQLiteClientError) -> SQLiteRows {
        try await run(sql, values.map(\.sqliteValue))
    }

    /// Runs `body` in a transaction on the writer: committed if `body` returns,
    /// rolled back if it throws.
    ///
    /// ```
    /// try await db.transaction { tx in
    ///     try await tx.execute("update accounts set balance = balance - ? where id = ?", amount, from)
    ///     try await tx.execute("update accounts set balance = balance + ? where id = ?", amount, to)
    /// }
    /// ```
    ///
    /// A statement that fails inside it undoes only itself, as SQLite does: a
    /// `body` that catches the error and returns commits the rest. When SQLite
    /// rolls the whole transaction back itself -- on a full disk, say -- the
    /// commit fails and says so.
    ///
    /// The writer is held until the transaction ends, so a transaction started
    /// inside `body` waits for it, and fails with `poolTimedOut`.
    public func transaction<Result>(_ body: (SQLiteTransaction) async throws -> Result) async throws -> Result {
        let connection = try await writer.acquire()
        // Given back whatever happens: one left inside a transaction is
        // closed by the pool rather than handed to the next statement.
        defer { writer.release(connection) }
        do {
            _ = try await perform(connection, "BEGIN IMMEDIATE", [], allowTransaction: true)
            let result = try await body(SQLiteTransaction(database: self, connection: connection))
            _ = try await perform(connection, "COMMIT", [], allowTransaction: true)
            return result
        } catch {
            if connection.inTransaction {
                let rollback = limits
                _ = try? await Runtime.blocking { () throws(SQLiteClientError) -> SQLiteRows in
                    try connection.run("ROLLBACK", [], limits: rollback)
                }
            }
            throw error
        }
    }

    /// Brings the schema up to date: runs each script in `migrations` past the
    /// database's `user_version`, then sets it to their count, all in one
    /// transaction, and returns how many ran. A script may hold several
    /// statements.
    ///
    /// Append to the list; never change or remove a script that has run. Every
    /// process may call this as it starts, and the first to take the write
    /// lock migrates while the others wait for it and find nothing left to do.
    @discardableResult
    public func migrate(_ migrations: [String]) async throws(SQLiteClientError) -> Int {
        try await writer.withConnection { (writer: SQLiteConnection) async throws(SQLiteClientError) -> Int in
            try await Runtime.blocking { () throws(SQLiteClientError) -> Int in
                try writer.runScript("BEGIN IMMEDIATE")
                do throws(SQLiteClientError) {
                    let version = try writer.run("PRAGMA user_version", [], cached: false)
                    guard version.count == 1, case .integer(let found) = version.value(row: 0, column: 0) else {
                        throw .sqlite(SQLiteFailure(extendedCode: 1, message: "PRAGMA user_version returned no version"))
                    }
                    guard found <= migrations.count else {
                        throw .unknownSchemaVersion(found: Int(found), known: migrations.count)
                    }
                    for script in migrations[Int(found)...] {
                        try writer.runScript(script)
                    }
                    if Int(found) < migrations.count {
                        try writer.runScript("PRAGMA user_version = \(migrations.count)")
                    }
                    try writer.runScript("COMMIT")
                    return migrations.count - Int(found)
                } catch {
                    if writer.inTransaction { try? writer.runScript("ROLLBACK") }
                    throw error
                }
            }
        }
    }

    /// Closes every connection not in use now, and each one in use when its
    /// statement returns; fails every wait, now and later.
    public func close() {
        writer.close()
        readers?.close()
    }

    // MARK: Routing

    fileprivate func run(_ sql: String, _ values: [SQLiteValue]) async throws(SQLiteClientError) -> SQLiteRows {
        if let readers, readOnlySQL.withLock({ $0.contains(sql) }) {
            return try await readers.withConnection { connection throws(SQLiteClientError) in
                try await perform(connection, sql, values, allowTransaction: false)
            }
        }
        let rows = try await writer.withConnection { connection throws(SQLiteClientError) in
            try await perform(connection, sql, values, allowTransaction: false)
        }
        if rows.readOnly && readers != nil {
            readOnlySQL.withLock { known in
                // Bounded, because SQL built by concatenation would otherwise
                // grow this without end. Forgetting only costs one more trip
                // through the writer each.
                if known.count >= 4096 { known.removeAll(keepingCapacity: true) }
                known.insert(sql)
            }
        }
        return rows
    }

    /// Runs one statement on a blocking thread. Outside a transaction, a
    /// statement that leaves one open has it rolled back and is refused.
    fileprivate func perform(_ connection: SQLiteConnection, _ sql: String, _ values: [SQLiteValue],
                             allowTransaction: Bool) async throws(SQLiteClientError) -> SQLiteRows {
        let limits = self.limits
        return try await Runtime.blocking { () throws(SQLiteClientError) -> SQLiteRows in
            do throws(SQLiteClientError) {
                let rows = try connection.run(sql, values, limits: limits)
                if !allowTransaction && connection.inTransaction {
                    try? connection.runScript("ROLLBACK")
                    throw .transactionLeftOpen
                }
                return rows
            } catch {
                // A statement that failed inside a BEGIN it ran itself leaves
                // the transaction open just the same.
                if !allowTransaction && connection.inTransaction { try? connection.runScript("ROLLBACK") }
                throw error
            }
        }
    }

    /// For tests: readers idle and in use, and whether the writer is.
    var counts: (idleReaders: Int, readersInUse: Int, writerBusy: Bool) {
        (readers?.idleCount ?? 0, readers?.inUseCount ?? 0, writer.inUseCount > 0)
    }
    var knownReadOnly: Int { readOnlySQL.withLock { $0.count } }
}

/// Statements inside one transaction, all on the writer.
public struct SQLiteTransaction: @unchecked Sendable {
    let database: SQLiteDatabase
    let connection: SQLiteConnection

    public func query<Row: Decodable>(_ type: Row.Type, _ sql: String,
                                      _ values: any SQLiteBindable...) async throws -> [Row] {
        try decodeAllSQLite(type, try await run(sql, values.map(\.sqliteValue)))
    }

    public func first<Row: Decodable>(_ type: Row.Type, _ sql: String,
                                      _ values: any SQLiteBindable...) async throws -> Row? {
        let rows = try await run(sql, values.map(\.sqliteValue))
        guard rows.count > 0 else { return nil }
        return try decodeSQLiteRow(type, rows, 0, sqliteColumnIndex(rows))
    }

    @discardableResult
    public func execute(_ sql: String, _ values: any SQLiteBindable...) async throws -> Int {
        try await run(sql, values.map(\.sqliteValue)).affected
    }

    public func rows(_ sql: String, _ values: any SQLiteBindable...) async throws(SQLiteClientError) -> SQLiteRows {
        try await run(sql, values.map(\.sqliteValue))
    }

    private func run(_ sql: String, _ values: [SQLiteValue]) async throws(SQLiteClientError) -> SQLiteRows {
        try await database.perform(connection, sql, values, allowTransaction: true)
    }
}
