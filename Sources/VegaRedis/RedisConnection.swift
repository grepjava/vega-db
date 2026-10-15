//===----------------------------------------------------------------------===//
// One connection to a Redis server, on Vega's streams.
//
// This file only moves bytes: it opens the connection, puts TLS on it, runs
// the handshake, and writes commands and reads their replies. One task uses a
// connection at a time -- the pool's, the session's or the subscription's
// owner -- so its state needs no lock, only the pool's hand-over.
//===----------------------------------------------------------------------===//

import Vega
import VegaIO
import VegaPool
import VegaTLS
import VegaRedisProtocol

/// The socket under a connection, with or without TLS.
enum RedisLink: ~Copyable {
    case tcp(TCPStream)
    case tcpTLS(TLSStream<TCPStream>)
    case unix(UnixStream)
    case unixTLS(TLSStream<UnixStream>)

    func read(into buffer: inout IOBuffer) async throws(IOError) -> Int {
        switch self {
        case .tcp(let s): return try await s.read(into: &buffer)
        case .tcpTLS(let s): return try await s.read(into: &buffer)
        case .unix(let s): return try await s.read(into: &buffer)
        case .unixTLS(let s): return try await s.read(into: &buffer)
        }
    }

    func write(_ buffer: borrowing IOBuffer) async throws(IOError) {
        switch self {
        case .tcp(let s): try await s.write(buffer)
        case .tcpTLS(let s): try await s.write(buffer)
        case .unix(let s): try await s.write(buffer)
        case .unixTLS(let s): try await s.write(buffer)
        }
    }

    /// Whether a read would return at once, asked without reading.
    var hasPendingInput: Bool {
        switch self {
        case .tcp(let s): s.hasPendingInput
        case .tcpTLS(let s): s.hasPendingInput
        case .unix(let s): s.hasPendingInput
        case .unixTLS(let s): s.hasPendingInput
        }
    }
}

/// One connection to a Redis server.
final class RedisConnection: PoolableConnection, @unchecked Sendable {
    let configuration: RedisConfiguration
    /// 3, or 2 for a server without HELLO.
    let protocolVersion: Int
    /// What HELLO said: `version`, `mode`, `role`.
    let server: [String: String]

    private var link: RedisLink?
    private var parser: RedisParser
    private var buffer: IOBuffer

    /// Whether the session is as the handshake left it, and so fit for the
    /// next caller. Cleared by anything that changes what later commands mean:
    /// a transaction or watch left open, another database, subscribing.
    var reusable = true
    private var inMulti = false
    private var watching = false

    private init(link: consuming RedisLink, configuration: RedisConfiguration,
                 protocolVersion: Int, server: [String: String], parser: RedisParser, buffer: consuming IOBuffer) {
        self.link = .some(link)
        var configuration = configuration
        configuration.tracer = DatabaseTracing.resolve(configuration.tracer)
        self.configuration = configuration
        self.protocolVersion = protocolVersion
        self.server = server
        self.parser = parser
        self.buffer = buffer
    }

    var isOpen: Bool { link != nil }

    /// Whether the session is outside any MULTI and WATCH.
    var isClean: Bool { reusable && !inMulti && !watching }

    /// For the pool: open, clean, and with nothing waiting on the socket.
    /// Redis says nothing unasked, so anything waiting on an idle connection
    /// is it going away -- a timeout, a restart -- found now rather than
    /// after a command is written into it.
    var isReusable: Bool {
        guard isClean else { return false }
        switch link {
        case .none: return false
        case .some(let link): return !link.hasPendingInput
        }
    }

    func close() {
        _ = link.take() // dropping the stream closes it
    }

    // MARK: Connecting

    static func connect(_ configuration: RedisConfiguration,
                        tls: TLSContext?) async throws(RedisClientError) -> RedisConnection {
        do {
            return try await Runtime.timeout(configuration.timeout) { () async throws -> RedisConnection in
                try await open(configuration, tls: tls)
            }
        } catch let error as RedisClientError {
            throw error
        } catch is TimeoutError {
            throw .timedOut
        } catch {
            throw .cancelled
        }
    }

    private static func open(_ configuration: RedisConfiguration,
                             tls: TLSContext?) async throws(RedisClientError) -> RedisConnection {
        let link: RedisLink
        do throws(IOError) {
            if let path = configuration.unixSocketPath {
                let stream = try await UnixStream.connect(path: path)
                if let tls {
                    link = .unixTLS(try await TLSStream.connect(stream, serverName: nil, context: tls))
                } else {
                    link = .unix(stream)
                }
            } else {
                let stream = try await TCPStream.connect(host: configuration.host, port: Int(configuration.port))
                if let tls {
                    link = .tcpTLS(try await TLSStream.connect(stream, serverName: configuration.host, context: tls))
                } else {
                    link = .tcp(stream)
                }
            }
        } catch {
            throw .connect(error)
        }

        var parser = RedisParser(limits: configuration.limits)
        var buffer = IOBuffer(capacity: 4096)
        var handshake = RedisHandshake(username: configuration.username, password: configuration.password,
                                       clientName: configuration.clientName, database: configuration.database)
        var command = handshake.start()
        while true {
            do throws(IOError) {
                try await link.write(ioBuffer(command.bytes()))
            } catch {
                throw .closed
            }
            let reply = try await readValue(link, &parser, &buffer)
            let step: RedisStep
            do {
                step = try handshake.receive(reply)
            } catch {
                throw .handshake(error)
            }
            switch step {
            case .send(let next):
                command = next
            case .ready:
                // Nothing may follow unasked: bytes left over would be read
                // as the first command's reply.
                guard buffer.isEmpty, !parser.isMidReply else { throw .protocolViolation(.badValue) }
                return RedisConnection(link: link, configuration: configuration,
                                       protocolVersion: handshake.protocolVersion, server: handshake.server,
                                       parser: parser, buffer: buffer)
            }
        }
    }

    // MARK: Commands

    /// Writes every command at once, then reads a reply for each, in order.
    /// A command the server refused is an `.error` among the replies; only a
    /// failure of the connection throws.
    ///
    /// A failure once the write has begun throws `unknownOutcome`, because
    /// from here the command having run and its reply having been lost look
    /// the same.
    func send(_ commands: [RedisCommand], timeout: Duration? = nil) async throws(RedisClientError) -> [RedisValue] {
        guard !commands.isEmpty else { return [] }
        guard let tracer = configuration.tracer,
              let span = tracer.start({ configuration.describe(commands) }) else {
            return try await untracedSend(commands, timeout: timeout)
        }
        do throws(RedisClientError) {
            let replies = try await untracedSend(commands, timeout: timeout)
            // A command the server refused is answered, not failed, at this
            // level; its caller throws it. A pipeline's refusals are its
            // commands', each answered in its place.
            if replies.count == 1, case .error(let refused) = replies[0] {
                span.fail(RedisClientError.server(refused).failure)
            } else {
                span.end()
            }
            return replies
        } catch {
            span.fail(error.failure)
            throw error
        }
    }

    private func untracedSend(_ commands: [RedisCommand], timeout: Duration?) async throws(RedisClientError) -> [RedisValue] {
        var bytes: [UInt8] = []
        bytes.reserveCapacity(64 * commands.count)
        for command in commands { command.write(into: &bytes) }
        let written = bytes
        let outcome = await bounded(timeout ?? configuration.timeout) { () async -> SendOutcome in
            await exchange(written, commands)
        }
        switch outcome {
        case .replies(let replies):
            return replies
        case .failed(let replies, let failure):
            // A command half answered leaves a stream nobody can pick up.
            close()
            // What was read before it failed was answered, and is the
            // caller's: those commands ran or were refused, and saying only
            // "unknown" would have them sent again.
            guard !replies.isEmpty else { throw failure }
            throw .incomplete(replies: replies.map { Optional($0) }
                                + Array(repeating: nil, count: commands.count - replies.count),
                              failure)
        }
    }

    /// The next value the server sends, whatever it is, waiting at most
    /// `timeout`.
    func next(timeout: Duration) async throws(RedisClientError) -> RedisValue {
        let outcome = await bounded(timeout) { () async -> SendOutcome in
            do throws(RedisClientError) {
                return .replies([try await self.readValue()])
            } catch {
                return .failed([], error)
            }
        }
        switch outcome {
        case .replies(let values): return values[0]
        case .failed(_, let error): throw error
        }
    }

    /// Writes bytes without reading anything back.
    func write(_ commands: [RedisCommand]) async throws(RedisClientError) {
        var bytes: [UInt8] = []
        for command in commands { command.write(into: &bytes) }
        let written = bytes
        let outcome = await bounded(configuration.timeout) { () async -> SendOutcome in
            do throws(RedisClientError) {
                try await self.writeAll(written)
                return .replies([])
            } catch {
                return .failed([], error.afterSending())
            }
        }
        if case .failed(_, let error) = outcome {
            close()
            throw error
        }
    }

    private enum SendOutcome: Sendable {
        case replies([RedisValue])
        case failed([RedisValue], RedisClientError)
    }

    private func exchange(_ bytes: [UInt8], _ commands: [RedisCommand]) async -> SendOutcome {
        do throws(RedisClientError) {
            try await writeAll(bytes)
        } catch {
            return .failed([], error.afterSending())
        }
        var replies: [RedisValue] = []
        replies.reserveCapacity(commands.count)
        do throws(RedisClientError) {
            while replies.count < commands.count {
                let reply = try await readValue()
                // Out of band -- a tracking invalidation -- and not the answer
                // to anything sent.
                if case .push = reply { continue }
                note(commands[replies.count], reply)
                replies.append(reply)
            }
            return .replies(replies)
        } catch {
            return .failed(replies, error.afterSending())
        }
    }

    /// Runs `body` with a deadline on its reads and writes. Past it the one
    /// waiting gives up, and fails as `timedOut`.
    ///
    /// Not `Runtime.timeout`: that runs the body as a child task, which
    /// costs more than the rest of a command's work on this side.
    private func bounded(_ timeout: Duration,
                         _ body: () async -> SendOutcome) async -> SendOutcome {
        await Runtime.withIODeadline(timeout, body)
    }

    /// Keeps track of what a command did to the session.
    private func note(_ command: RedisCommand, _ reply: RedisValue) {
        let failed: Bool
        if case .error = reply { failed = true } else { failed = false }
        switch command.name {
        case "MULTI":
            if !failed { inMulti = true }
        case "EXEC", "DISCARD":
            // Either way the transaction is over, and so is any watch.
            inMulti = false
            watching = false
        case "WATCH":
            if !failed { watching = true }
        case "UNWATCH":
            if !failed { watching = false }
        case "SELECT", "SUBSCRIBE", "PSUBSCRIBE", "SSUBSCRIBE", "MONITOR", "HELLO",
             "AUTH", "RESET", "QUIT", "READONLY", "READWRITE":
            // What later commands mean, or whether they are answered, may have
            // changed. Not worth modelling: the connection is simply not
            // handed on.
            reusable = false
        case "CLIENT":
            // CLIENT ID, INFO, LIST and KILL only look. These change the
            // session: whether replies come, what is tracked, its name.
            guard command.arguments.count > 1 else { break }
            let sub = String(decoding: command.arguments[1].map { $0 >= 97 && $0 <= 122 ? $0 - 32 : $0 },
                             as: UTF8.self)
            if ["REPLY", "TRACKING", "CACHING", "SETNAME", "SETINFO", "NO-EVICT", "NO-TOUCH"].contains(sub) {
                reusable = false
            }
        default:
            break
        }
    }

    // MARK: Bytes

    private func writeAll(_ bytes: [UInt8]) async throws(RedisClientError) {
        switch link {
        case .none:
            throw .closed
        case .some(let link):
            do throws(IOError) {
                try await link.write(ioBuffer(bytes))
            } catch {
                throw Self.map(error)
            }
        }
    }

    private func readValue() async throws(RedisClientError) -> RedisValue {
        switch link {
        case .none:
            throw .closed
        case .some(let link):
            return try await Self.readValue(link, &parser, &buffer)
        }
    }

    private static func readValue(_ link: borrowing RedisLink, _ parser: inout RedisParser,
                                  _ buffer: inout IOBuffer) async throws(RedisClientError) -> RedisValue {
        while true {
            if !buffer.isEmpty {
                let outcome: RedisParser.Outcome
                let consumed: Int
                do {
                    (outcome, consumed) = try buffer.withUnsafeBytes { raw throws(RedisProtocolError) in
                        try parser.parse(raw.baseAddress!.assumingMemoryBound(to: UInt8.self), raw.count)
                    }
                } catch {
                    throw .protocolViolation(error)
                }
                buffer.consume(consumed)
                if case .value(let value) = outcome { return value }
            }
            if buffer.spareCapacity < 4096 { buffer.reserveCapacity(buffer.count + 16384) }
            let n: Int
            do {
                n = try await link.read(into: &buffer)
            } catch {
                throw map(error)
            }
            if n == 0 { throw .closed }
        }
    }

    private static func map(_ error: IOError) -> RedisClientError {
        switch error {
        case .timedOut: .timedOut
        case .cancelled: .cancelled
        default: .closed
        }
    }
}

/// `bytes` in a buffer the streams can write.
private func ioBuffer(_ bytes: [UInt8]) -> IOBuffer {
    var buffer = IOBuffer(capacity: max(bytes.count, 1))
    bytes.withUnsafeBytes { _ = buffer.append($0) }
    return buffer
}
