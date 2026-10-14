//===----------------------------------------------------------------------===//
// LISTEN and NOTIFY: PostgreSQL telling a client that something happened,
// without the client asking.
//
//     let listener = try await pool.listen("jobs")
//     while let notification = try await listener.next(timeout: .seconds(5)) {
//         print(notification.channel, notification.payload)
//     }
//
//     try await pool.notify("jobs", "42")
//
// A listener holds a connection of its own, which the pool does not count: a
// session that has listened is told at any moment, so it cannot be handed to
// the next statement, and `LISTEN` on a pooled connection registers interest
// that goes back into the pool with it.
//
// Notifications are not a queue. The server holds none for a session that is
// not connected, so a listener that reconnects has missed whatever was sent
// while it was away, and the recipe for that is to read the work from a table
// once on reconnect and let the notification only say there is work. Nothing
// here pretends otherwise.
//===----------------------------------------------------------------------===//

import Vega
import VegaPostgresProtocol

/// A session listening for `NOTIFY`, on a connection of its own.
public final class PostgresListener: @unchecked Sendable {
    private let connection: PostgresConnection
    /// Notifications that arrived while a statement of this listener's own
    /// was in flight, oldest first.
    private var pending: [PostgresNotification] = []
    private var pendingHead = 0
    /// The channels listened to, in the order they were added.
    public private(set) var channels: [String] = []

    init(connection: PostgresConnection) {
        self.connection = connection
        connection.collectsNotifications = true
    }

    deinit {
        connection.close()
    }

    /// Whether the connection is still there. It goes false when the server
    /// restarts or the network drops, and every channel has to be listened to
    /// again on a new listener.
    public var isOpen: Bool { connection.isOpen }

    /// The next notification, waiting at most `timeout`; nil when none came
    /// in time. Throws once the connection has gone.
    ///
    /// Nothing else may run on this listener while a call is waiting.
    public func next(timeout: Duration = .seconds(1)) async throws(PostgresClientError) -> PostgresNotification? {
        // What arrived while this listener's own statements ran comes first,
        // in the order it came.
        collect()
        if pendingHead < pending.count {
            let notification = pending[pendingHead]
            pendingHead += 1
            if pendingHead == pending.count {
                pending.removeAll(keepingCapacity: true)
                pendingHead = 0
            }
            return notification
        }
        return try await connection.nextNotification(timeout: timeout)
    }

    /// Listens on more channels, which are listening before this returns.
    public func listen(_ channels: String...) async throws(PostgresClientError) {
        try await listen(channels)
    }

    public func listen(_ channels: [String]) async throws(PostgresClientError) {
        for channel in channels {
            _ = try await connection.query("listen " + quotedIdentifier(channel))
            if !self.channels.contains(channel) { self.channels.append(channel) }
        }
        collect()
    }

    /// Stops listening on channels. A notification already on its way still
    /// arrives, so a channel unlistened is not a channel silenced at once.
    public func unlisten(_ channels: String...) async throws(PostgresClientError) {
        for channel in channels {
            _ = try await connection.query("unlisten " + quotedIdentifier(channel))
            self.channels.removeAll { $0 == channel }
        }
        collect()
    }

    /// Sends a notification on this listener's own connection, which is also
    /// how a listener hears its own: `senderProcessID` says whose it was.
    public func notify(_ channel: String, _ payload: String = "") async throws(PostgresClientError) {
        _ = try await connection.query("select pg_notify($1, $2)",
                                       values: [PostgresValue(channel), PostgresValue(payload)])
        collect()
    }

    /// The process ID the server gave this session, which is what a
    /// notification this listener sent itself carries as its sender.
    public var processID: Int32 { connection.processID }

    /// Ends the listening and closes its connection.
    public func close() {
        connection.close()
    }

    private func collect() {
        pending.append(contentsOf: connection.takeNotifications())
    }
}

// MARK: - The pool

extension PostgresPool {
    /// Listens for `NOTIFY` on a connection of its own, which the pool does
    /// not count: a listener holds its connection for as long as it lasts.
    /// The channels are listening before this returns.
    public func listen(_ channels: String...) async throws -> PostgresListener {
        try await listen(channels)
    }

    public func listen(_ channels: [String]) async throws -> PostgresListener {
        precondition(!channels.isEmpty, "listen to at least one channel")
        let connection = try await PostgresConnection.connect(configuration, tls: try tls.context())
        let listener = PostgresListener(connection: connection)
        do {
            try await listener.listen(channels)
        } catch {
            connection.close()
            throw error
        }
        return listener
    }

    /// Sends a notification, through `pg_notify` so that the channel and the
    /// payload are values rather than part of the statement.
    ///
    /// Only a session listening at the time is told. A payload of 8,000 bytes
    /// or more is refused by the server, so what a notification carries is a
    /// key, not a document.
    public func notify(_ channel: String, _ payload: String = "") async throws {
        try await execute("select pg_notify($1, $2)", channel, payload)
    }
}

extension PostgresTransaction {
    /// Sends a notification when this transaction commits, and not at all if
    /// it rolls back -- so the work and the news of it are one decision.
    public func notify(_ channel: String, _ payload: String = "") async throws {
        try await execute("select pg_notify($1, $2)", channel, payload)
    }
}

/// A name as an identifier in a statement, quoted, with any quote inside it
/// doubled: `LISTEN` takes no parameters, so the channel cannot be a value.
///
/// PostgreSQL cuts an identifier at 63 bytes, here and in `NOTIFY` alike, so
/// two names that agree that far are one channel.
func quotedIdentifier(_ identifier: String) -> String {
    var out = "\""
    for character in identifier {
        if character == "\"" { out.append(character) }
        out.append(character)
    }
    out.append("\"")
    return out
}
