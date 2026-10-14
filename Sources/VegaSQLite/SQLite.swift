//===----------------------------------------------------------------------===//
// SQLite connections, used from the blocking pool.
//
// SQLite is a C library that reads and writes files on the thread that calls
// it, and waits on other processes' locks there too, so none of it may run on
// a worker. Each statement goes to a thread of Vega's blocking pool whole:
// prepared or taken from the connection's cache, bound, stepped to the end,
// its rows copied out and the statement reset. The task decodes the copied
// rows once it resumes.
//
// A connection is only ever used by one thread at a time -- the pool hands it
// to one statement, and takes it back before handing it to another -- which
// is what SQLite's multi-thread mode asks, so it is opened without its own
// mutex.
//
// The library is the system's, loaded when the first database opens
// (CVegaSQLite), so building needs no SQLite headers. From Garuda's SQLite
// driver.
//===----------------------------------------------------------------------===//

import Vega
import VegaPool
import CVegaSQLite

/// Where a SQLite database is and how to open it.
public struct SQLiteConfiguration: Sendable {
    public enum Mode: Sendable {
        /// Reads and writes, creating the file if it does not exist.
        case readWrite
        /// Reads only. The file must exist.
        case readOnly
    }

    /// How hard SQLite works to make a commit survive a power loss.
    public enum Synchronous: String, Sendable {
        /// Leaves flushing to the operating system. A crash of the machine can
        /// lose commits or corrupt the file.
        case off = "OFF"
        /// With WAL, a power loss can lose the last commits but never corrupts
        /// the file. The usual choice for WAL.
        case normal = "NORMAL"
        /// Every commit is flushed before it returns.
        case full = "FULL"
    }

    /// A file path, or `:memory:` for a database that lives in the connection
    /// and disappears with it.
    public var path: String
    public var mode: Mode = .readWrite
    /// Write-ahead logging, so readers do not wait for a writer, nor it for
    /// them. Kept in the file once set; ignored in memory and read-only.
    public var writeAheadLog = true
    public var synchronous: Synchronous = .normal
    /// Whether `REFERENCES` constraints are enforced. SQLite's own default is
    /// off, for compatibility with databases older than the feature.
    public var foreignKeys = true
    /// How long a statement waits for another connection's lock -- another
    /// process's write, say -- before failing with `SQLITE_BUSY`. The wait is
    /// on a blocking thread, not a worker.
    public var busyTimeout: Duration = .seconds(5)
    /// How long a statement waits for a connection of this database when
    /// every one is in use.
    public var acquireTimeout: Duration = .seconds(10)
    /// Statements each connection keeps prepared, by SQL.
    public var statementCacheCapacity = 64
    /// Rows a single result may hold before the statement fails.
    public var maxRows = 1_000_000
    /// Bytes of text and blobs a single result may hold before the statement
    /// fails.
    public var maxResultBytes = 64 * 1024 * 1024

    public init(path: String) {
        self.path = path
    }

    /// Whether the database lives in the connection rather than in a file, so
    /// that a second connection would open a different, empty database.
    var isMemory: Bool {
        path == ":memory:" || path.isEmpty || path.hasPrefix("file::memory:")
    }

    /// `busyTimeout` as SQLite takes it.
    var busyTimeoutMilliseconds: Int32 {
        let (seconds, attoseconds) = busyTimeout.components
        let ms = max(0, seconds) * 1000 + max(0, attoseconds) / 1_000_000_000_000_000
        return Int32(clamping: ms)
    }
}

// MARK: - Errors

/// What SQLite said when it refused something.
public struct SQLiteFailure: Error, Equatable, Sendable, CustomStringConvertible {
    /// The primary result code: 19 (`SQLITE_CONSTRAINT`) for any constraint.
    public var code: Int32 { extendedCode & 0xFF }
    /// The extended result code: 2067 (`SQLITE_CONSTRAINT_UNIQUE`), 787
    /// (`SQLITE_CONSTRAINT_FOREIGNKEY`).
    public let extendedCode: Int32
    public let message: String

    public init(extendedCode: Int32, message: String) {
        self.extendedCode = extendedCode
        self.message = message
    }

    public var description: String { "SQLite error \(extendedCode): \(message)" }
}

/// Why a SQLite operation did not complete.
public enum SQLiteClientError: Error, Equatable, Sendable {
    /// No usable libsqlite3 could be loaded: not installed, missing a function
    /// this library uses, or built without thread support.
    case unavailable
    /// The database could not be opened or set up.
    case open(SQLiteFailure)
    /// SQLite refused a statement.
    case sqlite(SQLiteFailure)
    /// The SQL held more than one statement. Only a migration may.
    case multipleStatements
    /// The statement has a different number of parameters than values given.
    case parameterCount(expected: Int, given: Int)
    /// The result had more than `maxRows` rows.
    case tooManyRows
    /// The result held more than `maxResultBytes` of text and blobs.
    case resultTooLarge
    /// A statement outside `transaction` began a transaction and did not end
    /// it. It was rolled back: the next statement on that connection could be
    /// anyone's.
    case transactionLeftOpen
    /// The database has migrations this program does not know: it was
    /// migrated by a newer version.
    case unknownSchemaVersion(found: Int, known: Int)
    /// Every connection stayed in use for `acquireTimeout`.
    case poolTimedOut
    /// The task was cancelled while waiting for a connection.
    case cancelled
    /// The database was closed.
    case closed
}

extension SQLiteClientError {
    /// SQLite's extended result code when SQLite refused the statement --
    /// 2067 for a unique violation -- or nil when the failure was not SQLite's.
    public var sqliteCode: Int32? {
        switch self {
        case .sqlite(let failure), .open(let failure): return failure.extendedCode
        default: return nil
        }
    }

    /// Whether SQLite refused the statement for breaking a constraint: unique,
    /// not null, foreign key, check.
    public var isConstraintViolation: Bool {
        sqliteCode.map { $0 & 0xFF == VSQ_CONSTRAINT } ?? false
    }
}

/// Why a row could not become the type asked for.
public enum SQLiteDecodingError: Error, Equatable {
    case missingColumn(String)
    case notConvertible(column: String, value: String, expected: String)
    case null(column: String)
    case unsupported(String)
}

// MARK: - Values

/// A value as SQLite stores it.
public enum SQLiteValue: Equatable, Sendable {
    case null
    case integer(Int64)
    case real(Double)
    case text(String)
    case blob([UInt8])
}

/// A Swift value that binds to a statement parameter.
///
/// `UInt` and `UInt64` do not conform: SQLite's integers are signed 64-bit,
/// and half their range would not fit.
public protocol SQLiteBindable {
    var sqliteValue: SQLiteValue { get }
}

extension SQLiteValue: SQLiteBindable { public var sqliteValue: SQLiteValue { self } }
extension String: SQLiteBindable { public var sqliteValue: SQLiteValue { .text(self) } }
extension Substring: SQLiteBindable { public var sqliteValue: SQLiteValue { .text(String(self)) } }
extension Int: SQLiteBindable { public var sqliteValue: SQLiteValue { .integer(Int64(self)) } }
extension Int8: SQLiteBindable { public var sqliteValue: SQLiteValue { .integer(Int64(self)) } }
extension Int16: SQLiteBindable { public var sqliteValue: SQLiteValue { .integer(Int64(self)) } }
extension Int32: SQLiteBindable { public var sqliteValue: SQLiteValue { .integer(Int64(self)) } }
extension Int64: SQLiteBindable { public var sqliteValue: SQLiteValue { .integer(self) } }
extension UInt8: SQLiteBindable { public var sqliteValue: SQLiteValue { .integer(Int64(self)) } }
extension UInt16: SQLiteBindable { public var sqliteValue: SQLiteValue { .integer(Int64(self)) } }
extension UInt32: SQLiteBindable { public var sqliteValue: SQLiteValue { .integer(Int64(self)) } }
extension Double: SQLiteBindable { public var sqliteValue: SQLiteValue { .real(self) } }
extension Float: SQLiteBindable {
    /// Through its shortest decimal form, so 0.1 is stored as 0.1 and not as
    /// the 0.10000000149011612 that widening the Float itself would give.
    public var sqliteValue: SQLiteValue { .real(Double(description) ?? Double(self)) }
}
extension Bool: SQLiteBindable { public var sqliteValue: SQLiteValue { .integer(self ? 1 : 0) } }
extension Array: SQLiteBindable where Element == UInt8 {
    public var sqliteValue: SQLiteValue { .blob(self) }
}
extension Optional: SQLiteBindable where Wrapped: SQLiteBindable {
    public var sqliteValue: SQLiteValue { self?.sqliteValue ?? .null }
}
// MARK: - Rows

/// A statement's result, copied out of SQLite.
public struct SQLiteRows: Sendable {
    public let columns: [String]
    /// Row by row, a column at a time.
    let values: [SQLiteValue]
    /// Rows changed by an INSERT, UPDATE or DELETE; 0 for anything else.
    public let affected: Int
    /// The rowid of the last row this connection inserted.
    public let lastInsertRowID: Int64
    /// Whether SQLite said the statement cannot write.
    let readOnly: Bool

    public var count: Int { columns.isEmpty ? 0 : values.count / columns.count }

    public func value(row: Int, column: Int) -> SQLiteValue {
        values[row * columns.count + column]
    }
}

// MARK: - One connection

struct SQLiteLimits: Sendable {
    var maxRows: Int
    var maxResultBytes: Int
}

final class SQLiteConnection: PoolableConnection, @unchecked Sendable {
    private struct Cached {
        let statement: OpaquePointer
        let readOnly: Bool
        var used: UInt64
    }

    private var db: OpaquePointer?
    private var cache: [String: Cached] = [:]
    private var uses: UInt64 = 0
    private let cacheCapacity: Int
    let readOnly: Bool

    private init(db: OpaquePointer, cacheCapacity: Int, readOnly: Bool) {
        self.db = db
        self.cacheCapacity = cacheCapacity
        self.readOnly = readOnly
    }

    deinit { close() }

    /// Opens and sets up a connection. Blocks: file I/O, and perhaps a wait
    /// on another process's lock to switch the file to WAL.
    static func open(_ configuration: SQLiteConfiguration, readOnly: Bool) throws(SQLiteClientError) -> SQLiteConnection {
        guard vsq_available() != 0 else { throw .unavailable }
        // A pool's reader opens the file read-write and is held to reads by
        // SQLite rather than by the open flags. A read-only connection cannot
        // create the -shm file a write-ahead log is read through, so it cannot
        // be the first to open a WAL database that nothing has written to yet:
        // on Darwin's SQLite that is "unable to open database file", and the
        // first read of such a database failed. The file was opened for
        // writing by this same pool a moment earlier, so nothing is given up
        // by opening it that way again -- and CREATE is not passed, so a
        // reader still never brings a database into being.
        //
        // A database configured read-only is a different thing: there the
        // whole pool is read-only, the file may be one this process cannot
        // write, and every connection opens read-only.
        let fileReadOnly = configuration.mode == .readOnly
        let holdToReads = readOnly && !fileReadOnly
        var flags = VSQ_OPEN_NOMUTEX | VSQ_OPEN_PRIVATECACHE
        if readOnly && fileReadOnly {
            flags |= VSQ_OPEN_READONLY
        } else if readOnly {
            flags |= VSQ_OPEN_READWRITE
        } else {
            flags |= VSQ_OPEN_READWRITE | VSQ_OPEN_CREATE
        }
        var handle: OpaquePointer? = nil
        let rc = configuration.path.withCString { vsq_open($0, Int32(flags), &handle) }
        guard let handle else { throw .open(SQLiteFailure(extendedCode: rc, message: String(cString: vsq_errstr(rc)))) }
        let connection = SQLiteConnection(db: handle, cacheCapacity: max(0, configuration.statementCacheCapacity),
                                          readOnly: readOnly)
        guard rc == VSQ_OK else {
            let failure = connection.failure()
            connection.close()
            throw .open(failure)
        }
        _ = vsq_extended_result_codes(handle, 1)
        _ = vsq_busy_timeout(handle, max(0, configuration.busyTimeoutMilliseconds))
        do throws(SQLiteClientError) {
            if !readOnly && configuration.writeAheadLog && !configuration.isMemory {
                let mode = try connection.whileBusy(configuration.busyTimeoutMilliseconds) {
                    () throws(SQLiteClientError) -> SQLiteRows in
                    try connection.run("PRAGMA journal_mode=WAL", [], cached: false)
                }
                guard mode.count == 1, case .text(let name) = mode.value(row: 0, column: 0),
                      name.lowercased() == "wal" else {
                    throw .open(SQLiteFailure(extendedCode: VSQ_ERROR,
                                              message: "the database would not switch to write-ahead logging"))
                }
            }
            _ = try connection.run("PRAGMA synchronous=\(configuration.synchronous.rawValue)", [], cached: false)
            _ = try connection.run("PRAGMA foreign_keys=\(configuration.foreignKeys ? "ON" : "OFF")", [], cached: false)
            // Last, so the settings above are still allowed to be made. From
            // here SQLite refuses anything that would change the database on
            // this connection, with SQLITE_READONLY, exactly as the read-only
            // open flag would have.
            if holdToReads {
                _ = try connection.run("PRAGMA query_only=1", [], cached: false)
            }
        } catch {
            connection.close()
            if case .sqlite(let failure) = error { throw .open(failure) }
            throw error
        }
        return connection
    }

    var isOpen: Bool { db != nil }

    /// For the pool: open, and outside any transaction.
    var isReusable: Bool { isOpen && !inTransaction }

    /// Runs `body` again while SQLite says the database is busy, for up to
    /// `milliseconds`. Switching a file to WAL takes an exclusive lock that
    /// SQLite does not wait for through the busy timeout, so workers starting
    /// together on a new file would otherwise fail all but one of them.
    func whileBusy<T>(_ milliseconds: Int32, _ body: () throws(SQLiteClientError) -> T) throws(SQLiteClientError) -> T {
        var waited: Int32 = 0
        var pause: Int32 = 1
        while true {
            do {
                return try body()
            } catch {
                guard case .sqlite(let failure) = error,
                      failure.code == VSQ_BUSY || failure.code == VSQ_LOCKED,
                      waited < milliseconds else { throw error }
                vsq_sleep(pause)
                waited += pause
                pause = min(pause * 2, 50)
            }
        }
    }

    /// Inside a transaction: one begun and not yet committed or rolled back.
    var inTransaction: Bool {
        guard let db else { return false }
        return vsq_get_autocommit(db) == 0
    }

    func close() {
        guard let db else { return }
        for entry in cache.values { _ = vsq_finalize(entry.statement) }
        cache.removeAll()
        _ = vsq_close(db)
        self.db = nil
    }

    private func failure() -> SQLiteFailure {
        guard let db else { return SQLiteFailure(extendedCode: VSQ_MISUSE, message: "the connection is closed") }
        return SQLiteFailure(extendedCode: vsq_extended_errcode(db), message: String(cString: vsq_errmsg(db)))
    }

    /// Runs one statement to its end and copies out its rows.
    func run(_ sql: String, _ values: [SQLiteValue], cached: Bool = true,
             limits: SQLiteLimits = SQLiteLimits(maxRows: .max, maxResultBytes: .max)) throws(SQLiteClientError) -> SQLiteRows {
        guard let db else { throw .closed }
        let entry = try statement(sql, cached: cached && cacheCapacity > 0)
        guard let entry else {
            // Only whitespace and comments: nothing to run.
            return SQLiteRows(columns: [], values: [], affected: 0,
                              lastInsertRowID: vsq_last_insert_rowid(db), readOnly: true)
        }
        let statement = entry.statement
        defer {
            _ = vsq_reset(statement)
            _ = vsq_clear_bindings(statement)
            if !(cached && cacheCapacity > 0) { _ = vsq_finalize(statement) }
        }

        let expected = Int(vsq_bind_parameter_count(statement))
        guard expected == values.count else { throw .parameterCount(expected: expected, given: values.count) }
        for (i, value) in values.enumerated() {
            let index = Int32(i + 1)
            let rc: Int32
            switch value {
            case .null:
                rc = vsq_bind_null(statement, index)
            case .integer(let n):
                rc = vsq_bind_int64(statement, index, n)
            case .real(let d):
                rc = vsq_bind_double(statement, index, d)
            case .text(let s):
                var s = s
                rc = s.withUTF8 { utf8 in
                    guard utf8.count <= Int32.max else { return VSQ_RANGE }
                    return utf8.withMemoryRebound(to: CChar.self) {
                        vsq_bind_text(statement, index, $0.baseAddress, Int32(utf8.count))
                    }
                }
            case .blob(let bytes):
                rc = bytes.withUnsafeBytes { raw in
                    guard raw.count <= Int32.max else { return VSQ_RANGE }
                    return vsq_bind_blob(statement, index, raw.baseAddress, Int32(raw.count))
                }
            }
            guard rc == VSQ_OK else { throw .sqlite(failure()) }
        }

        var width = 0
        var columns: [String] = []
        var out: [SQLiteValue] = []
        var rows = 0
        var bytes = 0
        var stepped = false
        while true {
            let rc = vsq_step(statement)
            if !stepped {
                // Read after the first step, every run: SQLite prepares a
                // cached statement again inside the step when the schema has
                // changed, and `select *` may then have other columns.
                stepped = true
                width = Int(vsq_column_count(statement))
                columns.reserveCapacity(width)
                for column in 0..<Int32(width) {
                    columns.append(vsq_column_name(statement, column).map { String(cString: $0) } ?? "")
                }
            }
            if rc == VSQ_DONE { break }
            guard rc == VSQ_ROW else { throw .sqlite(failure()) }
            rows += 1
            guard rows <= limits.maxRows else { throw .tooManyRows }
            for column in 0..<Int32(width) {
                switch vsq_column_type(statement, column) {
                case VSQ_INTEGER:
                    out.append(.integer(vsq_column_int64(statement, column)))
                case VSQ_FLOAT:
                    out.append(.real(vsq_column_double(statement, column)))
                case VSQ_TEXT:
                    let text = vsq_column_text(statement, column)
                    let count = Int(vsq_column_bytes(statement, column))
                    bytes += count
                    guard bytes <= limits.maxResultBytes else { throw .resultTooLarge }
                    guard let text else { throw .sqlite(failure()) }
                    out.append(.text(String(decoding: UnsafeBufferPointer(start: text, count: count), as: UTF8.self)))
                case VSQ_BLOB:
                    let blob = vsq_column_blob(statement, column)
                    let count = Int(vsq_column_bytes(statement, column))
                    bytes += count
                    guard bytes <= limits.maxResultBytes else { throw .resultTooLarge }
                    if let blob, count > 0 {
                        out.append(.blob(Array(UnsafeRawBufferPointer(start: blob, count: count))))
                    } else {
                        out.append(.blob([]))
                    }
                default:
                    out.append(.null)
                }
            }
        }
        // A statement that changed nothing leaves the count of the last one
        // that did, so only a statement that can write reports it.
        let affected = entry.readOnly ? 0 : Int(vsq_changes(db))
        return SQLiteRows(columns: columns, values: out, affected: affected,
                          lastInsertRowID: vsq_last_insert_rowid(db), readOnly: entry.readOnly)
    }

    /// Runs every statement in `script`, discarding rows. For migrations.
    func runScript(_ script: String) throws(SQLiteClientError) {
        guard let db else { throw .closed }
        var utf8 = Array(script.utf8)
        utf8.append(0)
        var offset = 0
        while offset < utf8.count - 1 {
            var statement: OpaquePointer? = nil
            var consumed = 0
            let rc = utf8.withUnsafeBufferPointer { buffer -> Int32 in
                buffer.withMemoryRebound(to: CChar.self) { chars in
                    let start = chars.baseAddress! + offset
                    var tail: UnsafePointer<CChar>? = nil
                    let rc = vsq_prepare(db, start, Int32(chars.count - 1 - offset), &statement, &tail)
                    consumed = tail.map { $0 - UnsafePointer(start) } ?? (chars.count - 1 - offset)
                    return rc
                }
            }
            guard rc == VSQ_OK else { throw .sqlite(failure()) }
            guard consumed > 0 || statement != nil else { break }
            offset += consumed
            guard let statement else { continue }
            var step = vsq_step(statement)
            while step == VSQ_ROW { step = vsq_step(statement) }
            let stepFailure = step == VSQ_DONE ? nil : failure()
            _ = vsq_finalize(statement)
            if let stepFailure { throw .sqlite(stepFailure) }
        }
    }

    /// The prepared statement for `sql`, from the cache or newly prepared; nil
    /// when the SQL holds no statement.
    private func statement(_ sql: String, cached: Bool) throws(SQLiteClientError) -> Cached? {
        if cached, var hit = cache[sql] {
            uses += 1
            hit.used = uses
            cache[sql] = hit
            return hit
        }
        guard let db else { throw .closed }
        var sql = sql
        var prepared: OpaquePointer? = nil
        var rest = false
        let rc = sql.withUTF8 { utf8 -> Int32 in
            guard utf8.count < Int32.max else { return VSQ_RANGE }
            return utf8.withMemoryRebound(to: CChar.self) { chars -> Int32 in
                var tail: UnsafePointer<CChar>? = nil
                let rc = vsq_prepare(db, chars.baseAddress, Int32(chars.count), &prepared, &tail)
                guard rc == VSQ_OK, let tail, let base = chars.baseAddress else { return rc }
                // What follows the first statement must hold none: preparing
                // it yields no statement when it is only whitespace and
                // comments. Running the first and ignoring the rest would
                // silently skip SQL the caller wrote.
                let used = tail - base
                if used < chars.count {
                    var extra: OpaquePointer? = nil
                    let extraRC = vsq_prepare(db, tail, Int32(chars.count - used), &extra, nil)
                    if extra != nil {
                        _ = vsq_finalize(extra)
                        rest = true
                    } else if extraRC != VSQ_OK {
                        rest = true
                    }
                }
                return rc
            }
        }
        guard rc == VSQ_OK else { throw .sqlite(failure()) }
        guard let prepared else { return nil }
        if rest {
            _ = vsq_finalize(prepared)
            throw .multipleStatements
        }
        uses += 1
        let entry = Cached(statement: prepared, readOnly: vsq_stmt_readonly(prepared) != 0, used: uses)
        guard cached else { return entry }
        if cache.count >= cacheCapacity, let oldest = cache.min(by: { $0.value.used < $1.value.used }) {
            _ = vsq_finalize(oldest.value.statement)
            cache.removeValue(forKey: oldest.key)
        }
        cache[sql] = entry
        return entry
    }

    /// For tests: statements held prepared.
    var cachedStatements: Int { cache.count }
}
