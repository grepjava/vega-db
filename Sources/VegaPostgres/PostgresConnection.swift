//===----------------------------------------------------------------------===//
// One session with a PostgreSQL server, on Vega's streams.
//
// This file only moves bytes: it opens the connection, negotiates TLS, and
// hands each whole message to whichever machine is in progress, writing what
// it says to write. One task uses a connection at a time -- the pool's, the
// transaction's or the listener's owner -- so its state needs no lock, only
// the pool's hand-over.
//===----------------------------------------------------------------------===//

import Vega
import VegaIO
import VegaPool
import VegaTLS
import VegaPostgresProtocol

/// The socket under a connection, with or without TLS.
enum PostgresLink: ~Copyable {
    case tcp(TCPStream)
    case tls(TLSStream<TCPStream>)
    case unix(UnixStream)

    func read(into buffer: inout IOBuffer) async throws(IOError) -> Int {
        switch self {
        case .tcp(let s): return try await s.read(into: &buffer)
        case .tls(let s): return try await s.read(into: &buffer)
        case .unix(let s): return try await s.read(into: &buffer)
        }
    }

    func write(_ buffer: borrowing IOBuffer) async throws(IOError) {
        switch self {
        case .tcp(let s): try await s.write(buffer)
        case .tls(let s): try await s.write(buffer)
        case .unix(let s): try await s.write(buffer)
        }
    }

    /// Whether a read would return at once, asked without reading.
    var hasPendingInput: Bool {
        switch self {
        case .tcp(let s): s.hasPendingInput
        case .tls(let s): s.hasPendingInput
        case .unix(let s): s.hasPendingInput
        }
    }
}

/// One session with a PostgreSQL server.
///
/// A class, because it carries the one piece of session state the pool must
/// see after every statement: whether the session was left inside a
/// transaction.
final class PostgresConnection: PoolableConnection, @unchecked Sendable {
    let configuration: PostgresConfiguration
    let parameters: [String: String]
    /// The server's process ID for this session, from BackendKeyData: what a
    /// notification this session sent carries as its sender.
    let processID: Int32
    /// As the last ReadyForQuery reported it.
    private(set) var transactionStatus: PostgresTransactionStatus = .idle {
        didSet { readyAt = ContinuousClock.now }
    }
    /// When the server last said it was ready for a query: it was there then.
    private var readyAt = ContinuousClock.now
    /// How recently the server must have answered for `isReusable` to take
    /// it as still there without asking the socket.
    static let answeredRecently: Duration = .milliseconds(100)

    private var link: PostgresLink?

    /// A statement kept prepared, by the SQL and the parameter types it was
    /// parsed with: the same SQL with a value of another declared type is
    /// another statement.
    struct StatementKey: Hashable {
        let sql: String
        let types: [UInt32]
    }

    struct PreparedStatement {
        let name: String
        /// The columns its last result described, for choosing formats.
        var columns: [PostgresColumn] {
            didSet { formats = PostgresBinary.resultFormats(columns) }
        }
        /// That RowDescription as it came on the wire. The next one that is
        /// the same is these columns again (`PostgresQuery.knownDescription`).
        var description: [UInt8]
        /// The format to ask for each column in, worked out once from
        /// `columns` rather than for every run.
        private(set) var formats: [Int16]
        /// When it was last used, for evicting the least recent.
        var used: UInt64

        init(name: String, columns: [PostgresColumn], description: [UInt8], used: UInt64) {
            self.name = name
            self.columns = columns
            self.description = description
            self.formats = PostgresBinary.resultFormats(columns)
            self.used = used
        }
    }

    private(set) var prepared: [StatementKey: PreparedStatement] = [:]
    /// Set by a listener, which is the only owner with somewhere to put a
    /// notification. A pooled connection drops them: whoever ran `LISTEN` on
    /// one has already handed it back.
    var collectsNotifications = false
    private var notifications: [PostgresNotification] = []
    /// What a statement is written into, kept from one statement to the next
    /// instead of allocated for each.
    private var sending: [UInt8] = []
    /// What the server sent and nobody has handled yet. A listener's
    /// notifications wait here between calls.
    private var receiving: IOBuffer
    private var uses: UInt64 = 0
    private var nextStatement = 0
    /// Statements the server holds that the cache has let go of, closed with
    /// the next query.
    private var toClose: [String] = []

    private init(link: consuming PostgresLink, configuration: PostgresConfiguration,
                 parameters: [String: String], processID: Int32, receiving: consuming IOBuffer) {
        self.link = .some(link)
        var configuration = configuration
        configuration.tracer = DatabaseTracing.resolve(configuration.tracer)
        self.configuration = configuration
        self.parameters = parameters
        self.processID = processID
        self.receiving = receiving
    }

    var isOpen: Bool { link != nil }

    /// For the pool: open, outside any transaction, and with nothing waiting
    /// on the socket. Kept only if the session is back outside a transaction:
    /// a caller that ran `begin` itself and returned would otherwise hand its
    /// open transaction to the next, whose statements would run inside it.
    /// Closing it makes the server roll back. And anything arriving on a
    /// connection nobody was using is almost always the server going away --
    /// an idle timeout, a restart -- found now rather than after a statement
    /// is written into it. Asking the socket is a system call, and the pool
    /// asks twice a statement (on release, and on the next acquire), so a
    /// connection the server answered within `answeredRecently` is taken as
    /// it is: under load that is every one, and a server going away in that
    /// window is found by the statement, as it would be a moment later anyway.
    var isReusable: Bool {
        guard transactionStatus == .idle && receiving.isEmpty else { return false }
        switch link {
        case .none: return false
        case .some(let link):
            if ContinuousClock.now - readyAt < Self.answeredRecently { return true }
            return !link.hasPendingInput
        }
    }

    func close() {
        // Terminate, so the server logs a goodbye rather than a lost client.
        // Only where it can go without waiting; elsewhere the close says it.
        switch link {
        case .some(.tcp(let stream)):
            var out: [UInt8] = []
            PostgresFrontend.terminate(into: &out)
            out.withUnsafeBytes { bytes in
                withUnsafeTemporaryAllocation(of: UnsafeRawBufferPointer.self, capacity: 1) { parts in
                    parts[0] = bytes
                    _ = try? stream.tryWrite(vectored: UnsafeBufferPointer(parts))
                }
            }
        default:
            break
        }
        _ = link.take() // dropping the stream closes it
    }

    // MARK: Connecting

    /// Connects, negotiates TLS if required, and authenticates.
    static func connect(_ configuration: PostgresConfiguration,
                        tls: TLSContext?) async throws(PostgresClientError) -> PostgresConnection {
        do {
            return try await Runtime.timeout(for: configuration.timeout) { () async throws -> PostgresConnection in
                try await open(configuration, tls: tls)
            }
        } catch let error as PostgresClientError {
            throw error
        } catch is TimeoutError {
            throw .timedOut
        } catch {
            throw .cancelled
        }
    }

    private static func open(_ configuration: PostgresConfiguration,
                             tls: TLSContext?) async throws(PostgresClientError) -> PostgresConnection {
        let link: PostgresLink
        var encrypted = false
        if let path = configuration.socketPath {
            // A socket carries no TLS: there is no network on it to encrypt,
            // and PostgreSQL will not negotiate it there.
            guard configuration.tls == .disable else { throw .tlsUnavailable }
            do throws(IOError) {
                link = .unix(try await UnixStream.connect(path: path))
            } catch {
                throw .connect(error)
            }
        } else {
            let stream: TCPStream
            do throws(IOError) {
                stream = try await TCPStream.connect(host: configuration.host, port: Int(configuration.port))
            } catch {
                throw .connect(error)
            }
            if let tls {
                var request: [UInt8] = []
                PostgresFrontend.sslRequest(into: &request)
                // One byte, and exactly one. CVE-2021-23222 was a client
                // buffering whatever followed the S and later treating it as
                // if it had come through TLS. Reading a single byte leaves
                // anything injected after it for the handshake to reject.
                var answer = IOBuffer(capacity: 1)
                let n: Int
                do throws(IOError) {
                    try await stream.write(ioBuffer(request))
                    n = try await stream.read(into: &answer)
                } catch {
                    throw map(error)
                }
                guard n == 1 else { throw .closed }
                let accepted = answer.withUnsafeBytes { $0[0] == UInt8(ascii: "S") }
                guard accepted else { throw .tlsUnavailable }
                do throws(IOError) {
                    link = .tls(try await TLSStream.connect(stream, serverName: configuration.host, context: tls))
                } catch {
                    throw .connect(error)
                }
                encrypted = true
            } else {
                link = .tcp(stream)
            }
        }

        var policy = PostgresStartup.Policy()
        policy.allowCleartext = configuration.allowCleartextPassword
        policy.encrypted = encrypted
        var startup = PostgresStartup(user: configuration.user, password: configuration.password,
                                      database: configuration.database, policy: policy)
        let first: [UInt8]
        do { first = try startup.start() } catch { throw .postgres(error) }
        try await write(link, first)

        var buffer = IOBuffer(capacity: 4096)
        while true {
            var step = PostgresStep.wait
            try await receive(link, &buffer, configuration) { type, reader throws(PostgresClientError) in
                do throws(PostgresError) {
                    step = try startup.receive(type, reader)
                } catch {
                    throw .postgres(error)
                }
                return step != .wait
            }
            switch step {
            case .wait:
                continue
            case .send(let bytes):
                try await write(link, bytes)
            case .ready:
                // Nothing may follow ReadyForQuery unasked. Bytes left over
                // would be read as the start of the first query's answer.
                guard buffer.isEmpty else {
                    throw .postgres(.unexpectedMessage(buffer.withUnsafeBytes { $0[0] }))
                }
                return PostgresConnection(link: link, configuration: configuration,
                                          parameters: startup.parameters, processID: startup.processID,
                                          receiving: buffer)
            }
        }
    }

    // MARK: Statements

    /// Runs one statement, with its values sent beside it, and tells the
    /// configuration's tracer.
    func query(_ sql: String, values: [PostgresValue] = []) async throws(PostgresClientError) -> PostgresRows {
        guard let tracer = configuration.tracer,
              let span = tracer.start({ configuration.describe(sql) }) else {
            return try await untracedQuery(sql, values: values)
        }
        do throws(PostgresClientError) {
            let rows = try await untracedQuery(sql, values: values)
            span.end()
            return rows
        } catch {
            span.fail(error.failure)
            throw error
        }
    }

    /// `query`, for a caller that keeps a span of its own around it.
    ///
    /// A prepared statement the server no longer has, or can no longer run as
    /// planned, is dropped and the statement run once more from its SQL -- but
    /// only when it failed outside a transaction. There a failed statement has
    /// been rolled back whole, so running it again cannot do anything twice;
    /// inside one, the failure has already failed the transaction.
    func untracedQuery(_ sql: String, values: [PostgresValue]) async throws(PostgresClientError) -> PostgresRows {
        do {
            return try await bounded { () async throws(PostgresClientError) in try await self.run(sql, values) }
        } catch {
            guard reusedStaleStatement, transactionStatus == .idle else { throw error }
            return try await bounded { () async throws(PostgresClientError) in try await self.run(sql, values) }
        }
    }

    /// Whether the last statement failed because the prepared statement it
    /// reused was stale: 26000, gone -- `DEALLOCATE`, `DISCARD ALL` -- or
    /// 0A000, which is what the server says when a table under a plan changed
    /// shape ("cached plan must not change result type"). By code, not by that
    /// message, which the server translates.
    private var reusedStaleStatement = false

    private func run(_ sql: String, _ values: [PostgresValue]) async throws(PostgresClientError) -> PostgresRows {
        let types = values.contains { $0.declaredType != 0 } ? values.map(\.declaredType) : []
        let key = StatementKey(sql: sql, types: types)
        var name = ""
        var prepare = true
        var formats: [Int16] = []
        var knownDescription: [UInt8] = []
        var knownColumns: [PostgresColumn] = []
        uses &+= 1
        if configuration.statementCacheCapacity > 0 {
            // Looked up once, and updated through the index: the SQL is
            // hashed for the lookup and not again.
            if let found = prepared.index(forKey: key) {
                let statement = prepared.values[found]
                name = statement.name
                prepare = false
                // The types its last result had. Should they have changed,
                // the server refuses the plan (0A000) before sending a row, so
                // a format chosen for the old type never meets the new one.
                formats = statement.formats
                knownDescription = statement.description
                knownColumns = statement.columns
                prepared.values[found].used = uses
            } else {
                if prepared.count >= configuration.statementCacheCapacity,
                   let oldest = prepared.min(by: { $0.value.used < $1.value.used }) {
                    toClose.append(oldest.value.name)
                    prepared.removeValue(forKey: oldest.key)
                }
                name = "vega_\(nextStatement)"
                nextStatement &+= 1
            }
        }
        let closing = toClose
        toClose.removeAll()
        reusedStaleStatement = false
        var query = PostgresQuery(sql, values, maxRows: configuration.maxRows, statement: name,
                                  prepare: prepare, resultFormats: formats, closing: closing,
                                  knownDescription: knownDescription, knownColumns: knownColumns)
        sending.removeAll(keepingCapacity: true)
        do { try query.write(into: &sending) } catch { throw .postgres(error) }
        do throws(PostgresClientError) {
            try await send(sending)
            // The replies are handled as they are buffered, all of them in
            // one go, so the one await left is for the server to answer.
            try await receive { type, reader throws(PostgresClientError) in
                do throws(PostgresError) {
                    return try query.receive(type, reader)
                } catch {
                    throw .postgres(error)
                }
            }
            if collectsNotifications {
                // Kept whether the statement succeeded or not: a notification
                // that arrived while it ran was still sent. What came after
                // ReadyForQuery in the same read stays buffered: on a
                // listening connection it is a notification the server sent
                // as the statement ended, not a reply nobody asked for.
                notifications.append(contentsOf: query.notifications)
            } else if !receiving.isEmpty {
                throw .postgres(.unexpectedMessage(receiving.withUnsafeBytes { $0[0] }))
            }
            // Recorded whether the statement succeeded or not: a refused
            // statement inside a transaction leaves it failed, and that is
            // exactly what must not be handed on.
            transactionStatus = query.transactionStatus
            if !name.isEmpty { remember(key, name, prepare: prepare, query) }
            switch query.result() {
            case .success(let rows): return rows
            // The server refused this statement and is ready for the next:
            // the connection is fine, the query is not.
            case .failure(let error): throw .postgres(error)
            }
        } catch {
            // Anything but the server refusing the statement leaves the
            // connection in a state nobody should use.
            if case .postgres(.server) = error {} else { close() }
            throw error
        }
    }

    /// Keeps what a finished query taught the cache.
    private func remember(_ key: StatementKey, _ name: String, prepare: Bool, _ query: PostgresQuery) {
        if !prepare, case .failure(.server(let fields)) = query.result(),
           fields.code == "26000" || fields.code == "0A000" {
            // Gone, or unusable as planned. The 0A000 one still exists.
            prepared.removeValue(forKey: key)
            if fields.code == "0A000" { toClose.append(name) }
            reusedStaleStatement = true
            return
        }
        if prepare {
            // Refused before it was parsed -- a syntax error, a failed
            // transaction -- and there is nothing to keep.
            guard query.parsed else { return }
            prepared[key] = PreparedStatement(name: name, columns: query.rows.columns,
                                              description: query.freshDescription ?? [], used: uses)
        } else if let description = query.freshDescription {
            // Only when the description changed: the same one again is the
            // columns already kept.
            prepared[key]?.columns = query.rows.columns
            prepared[key]?.description = description
        }
        // Statements the session dropped wholesale. Their names would each be
        // refused once and retried; forgetting them now saves the round trips.
        if case .success(let rows) = query.result(), rows.tag == "DISCARD ALL" || rows.tag == "DEALLOCATE ALL" {
            prepared.removeAll()
        }
    }

    // MARK: Notifications

    /// What arrived while this connection's own statements ran, in order,
    /// and clears it. For a listener, which has nowhere else to look.
    func takeNotifications() -> [PostgresNotification] {
        defer { notifications.removeAll(keepingCapacity: true) }
        return notifications
    }

    /// Waits for the server to say something unasked -- a notification -- and
    /// returns it, or nil when nothing came within `timeout`.
    ///
    /// For a listener, whose connection has nothing else in flight. A notice
    /// or a changed setting is passed over and the wait goes on; anything else
    /// is a server sending a reply to a statement nobody ran, after which the
    /// connection is not to be trusted.
    func nextNotification(timeout: Duration) async throws(PostgresClientError) -> PostgresNotification? {
        var notification: PostgresNotification? = nil
        do throws(PostgresClientError) {
            try await Runtime.withIOTimeout(for: timeout) { () async throws(PostgresClientError) in
                try await self.receive { type, reader throws(PostgresClientError) in
                    switch type {
                    case UInt8(ascii: "A"):
                        do throws(PostgresProtocolError) {
                            notification = try PostgresBackend.notification(reader)
                        } catch {
                            throw .postgres(.protocolViolation(error))
                        }
                        return true
                    // A notice -- a warning, a raised message -- or a setting
                    // that changed under the session. Neither ends it.
                    case UInt8(ascii: "N"), UInt8(ascii: "S"):
                        return false
                    default:
                        throw .postgres(.unexpectedMessage(type))
                    }
                }
            }
        } catch .timedOut {
            return nil
        } catch {
            close()
            throw error
        }
        return notification
    }

    // MARK: Bytes

    /// Runs `body` with a deadline on its reads and writes: the
    /// configuration's timeout.
    private func bounded<T>(_ body: () async throws(PostgresClientError) -> T) async throws(PostgresClientError) -> T {
        try await Runtime.withIOTimeout(for: configuration.timeout, body)
    }

    private func send(_ bytes: [UInt8]) async throws(PostgresClientError) {
        switch link {
        case .none: throw .closed
        case .some(let link): try await Self.write(link, bytes)
        }
    }

    /// Hands `handle` every whole message, reading more as needed, until it
    /// returns true. What follows that message stays buffered.
    private func receive(_ handle: (UInt8, PostgresReader) throws(PostgresClientError) -> Bool)
        async throws(PostgresClientError) {
        switch link {
        case .none: throw .closed
        case .some(let link): try await Self.receive(link, &receiving, configuration, handle)
        }
    }

    private static func write(_ link: borrowing PostgresLink, _ bytes: [UInt8]) async throws(PostgresClientError) {
        do throws(IOError) {
            try await link.write(ioBuffer(bytes))
        } catch {
            throw map(error)
        }
    }

    private static func receive(_ link: borrowing PostgresLink, _ buffer: inout IOBuffer,
                                _ configuration: PostgresConfiguration,
                                _ handle: (UInt8, PostgresReader) throws(PostgresClientError) -> Bool)
        async throws(PostgresClientError) {
        while true {
            if try handleBuffered(&buffer, configuration, handle) { return }
            if buffer.spareCapacity < 4096 { buffer.reserveCapacity(buffer.count + 16_384) }
            let n: Int
            do {
                n = try await link.read(into: &buffer)
            } catch {
                throw map(error)
            }
            if n == 0 { throw .closed }
        }
    }

    /// Hands `handle` the whole messages buffered, consuming each, until it
    /// returns true -- then true -- or they run out: false.
    private static func handleBuffered(_ buffer: inout IOBuffer, _ configuration: PostgresConfiguration,
                                       _ handle: (UInt8, PostgresReader) throws(PostgresClientError) -> Bool)
        throws(PostgresClientError) -> Bool {
        guard !buffer.isEmpty else { return false }
        var consumed = 0
        var finished = false
        var failure: PostgresClientError? = nil
        buffer.withUnsafeBytes { raw in
            let base = raw.baseAddress!.assumingMemoryBound(to: UInt8.self)
            while !finished && failure == nil {
                switch PostgresBackend.frame(base + consumed, raw.count - consumed,
                                             maxLength: configuration.maxMessageBytes) {
                case .frame(let frame):
                    let reader = PostgresReader(base + consumed + frame.bodyOffset, frame.bodyLength)
                    do throws(PostgresClientError) {
                        finished = try handle(frame.type, reader)
                    } catch {
                        failure = error
                    }
                    consumed += frame.total
                case .failure(let error):
                    failure = .postgres(.protocolViolation(error))
                case .incomplete:
                    return
                }
            }
        }
        buffer.consume(consumed)
        if let failure { throw failure }
        return finished
    }

    static func map(_ error: IOError) -> PostgresClientError {
        switch error {
        case .timedOut: .timedOut
        case .cancelled: .cancelled
        default: .closed
        }
    }
}

extension PostgresConnection {
    /// Runs a `COPY ... TO STDOUT` and hands each chunk of it to `chunk` as it
    /// arrives.
    ///
    /// A chunk is not a row: the server sends a byte stream and splits it
    /// where it likes. `PostgresCopyText.rows` makes rows of it, or
    /// `copyOut(_:rows:)` does that for you.
    ///
    /// A `chunk` that throws ends the copy by closing the connection: a COPY
    /// out cannot be stopped politely, and reading a table to its end to be
    /// tidy about it would be worse.
    @discardableResult
    func copyOut(_ sql: String, _ chunk: (ArraySlice<UInt8>) throws -> Void) async throws -> Int {
        var copy = PostgresCopy(sql)
        let bytes: [UInt8]
        do { bytes = try copy.messages() } catch { throw PostgresClientError.postgres(error) }
        var thrown: (any Error)? = nil
        do throws(PostgresClientError) {
            try await send(bytes)
            // Each wait on the server bounded, not the whole copy: a table
            // takes as long as it takes.
            try await bounded { () async throws(PostgresClientError) in
                try await self.receive { type, reader throws(PostgresClientError) in
                    let step: PostgresCopyStep
                    do throws(PostgresError) {
                        step = try copy.receive(type, reader)
                    } catch {
                        throw .postgres(error)
                    }
                    if case .data(let range) = step {
                        // The reader's bytes belong to the buffer, which the
                        // next read may move: the closure sees them before then.
                        do {
                            try chunk(ArraySlice(UnsafeBufferPointer(start: reader.base + range.lowerBound,
                                                                     count: range.count)))
                        } catch {
                            thrown = error
                            // Nothing can be said to stop it, so the
                            // connection goes.
                            return true
                        }
                    }
                    return step == .finished
                }
            }
        } catch {
            if case .postgres(.server) = error {} else { close() }
            throw error
        }
        if let thrown {
            close()
            throw thrown
        }
        return try finish(&copy)
    }

    /// Runs a `COPY ... FROM STDIN`, asking `next` for data until it returns
    /// nil, and returns how many rows the server took.
    ///
    /// A `next` that throws tells the server so with CopyFail, which makes it
    /// refuse the whole load rather than keep half of it, and then the error
    /// is thrown on.
    @discardableResult
    func copyIn(_ sql: String, _ next: () throws -> [UInt8]?) async throws -> Int {
        var copy = PostgresCopy(sql)
        let bytes: [UInt8]
        do { bytes = try copy.messages() } catch { throw PostgresClientError.postgres(error) }
        var thrown: (any Error)? = nil
        do throws(PostgresClientError) {
            try await send(bytes)
            while true {
                var step = PostgresCopyStep.wait
                try await bounded { () async throws(PostgresClientError) in
                    try await self.receive { type, reader throws(PostgresClientError) in
                        do throws(PostgresError) {
                            step = try copy.receive(type, reader)
                        } catch {
                            throw .postgres(error)
                        }
                        return step == .ready || step == .finished
                    }
                }
                if step == .finished { break }
                // The server is taking data. Whatever `next` gives goes in one
                // message at a time, and its size is the caller's choice: a
                // row, a batch, a file's worth.
                var out: [UInt8] = []
                while true {
                    let piece: [UInt8]?
                    do {
                        piece = try next()
                    } catch {
                        thrown = error
                        break
                    }
                    guard let piece, !piece.isEmpty else { break }
                    out.removeAll(keepingCapacity: true)
                    PostgresFrontend.copyData(piece, into: &out)
                    try await bounded { () async throws(PostgresClientError) in try await self.send(out) }
                }
                out.removeAll(keepingCapacity: true)
                if let thrown {
                    // Said out loud, so the server discards the load rather
                    // than committing what arrived before the trouble.
                    _ = PostgresFrontend.copyFail("the client stopped: \(thrown)", into: &out)
                } else {
                    PostgresFrontend.copyDone(into: &out)
                }
                try await bounded { () async throws(PostgresClientError) in try await self.send(out) }
            }
        } catch {
            if case .postgres(.server) = error {} else { close() }
            throw error
        }
        // A CopyFail was sent and the server's answer read, so the connection
        // is fine; it is the caller's error that is thrown.
        if let thrown {
            transactionStatus = copy.transactionStatus
            throw thrown
        }
        return try finish(&copy)
    }

    /// What a copy the server has finished leaves: its status, its
    /// notifications, and its count or its refusal.
    private func finish(_ copy: inout PostgresCopy) throws(PostgresClientError) -> Int {
        transactionStatus = copy.transactionStatus
        if collectsNotifications {
            notifications.append(contentsOf: copy.notifications)
        } else if !receiving.isEmpty {
            close()
            throw .postgres(.unexpectedMessage(receiving.withUnsafeBytes { $0[0] }))
        }
        switch copy.result() {
        case .success: return copy.copied
        case .failure(let error): throw .postgres(error)
        }
    }
}

/// `bytes` in a buffer the streams can write.
private func ioBuffer(_ bytes: [UInt8]) -> IOBuffer {
    var buffer = IOBuffer(capacity: max(bytes.count, 1))
    bytes.withUnsafeBytes { _ = buffer.append($0) }
    return buffer
}
