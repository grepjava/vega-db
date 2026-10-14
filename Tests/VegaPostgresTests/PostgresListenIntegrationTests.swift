import Testing
import Vega
@testable import VegaPostgres

// LISTEN and NOTIFY against a real server: a listener on its own connection,
// notifications from another session and from its own, channels added and
// dropped, and a notification that waits for its transaction to commit.
//
// Set VEGA_POSTGRES to run; see IntegrationSupport.

private struct Refuse: Error {}

@Suite("PostgreSQL notifications round trip", .serialized,
       .enabled(if: postgresTarget != nil, "set VEGA_POSTGRES to run"))
struct PostgresListenIntegrationTests {
    @Test func notificationsReachAListener() {
        let text = onPool { pool in
            let channel = "vega_jobs"
            // A name that has to be quoted to survive being an identifier.
            let odd = #"vega "odd" channel"#
            let listener = try await pool.listen(channel)
            defer { listener.close() }
            var out: [String] = []

            // From another session, through the pool.
            try await pool.notify(channel, "42")
            if let heard = try await listener.next(timeout: .seconds(2)) {
                out.append("\(heard.channel == channel) \(heard.payload) "
                               + "\(heard.senderProcessID != listener.processID)")
            } else {
                out.append("none")
            }
            out.append(try await listener.next(timeout: .milliseconds(50)) == nil ? "quiet" : "more")

            // Its own, which the server sends while its own statement is
            // still running: waiting no time at all still finds it.
            try await listener.notify(channel, "self")
            if let mine = try await listener.next(timeout: .zero) {
                out.append("\(mine.payload) \(mine.senderProcessID == listener.processID)")
            } else {
                out.append("none")
            }

            // A channel added later, and a name that needs quoting.
            try await listener.listen(odd)
            try await pool.notify(odd, "quoted")
            if let heard = try await listener.next(timeout: .seconds(2)) {
                out.append("\(heard.channel == odd) \(heard.payload)")
            } else {
                out.append("none")
            }
            out.append(listener.channels.count == 2 ? "two" : listener.channels.joined(separator: ","))

            // Dropped: what is sent on it is no longer heard.
            try await listener.unlisten(channel)
            try await pool.notify(channel, "after")
            out.append(try await listener.next(timeout: .milliseconds(300)) == nil ? "deaf" : "heard")

            // The listener's connection is its own: the pool counts one,
            // the one its own statements used.
            out.append("\(pool.counts)")

            // A payload just inside what the server takes, and one past it
            // -- which is the server's refusal, not a broken listener.
            try await pool.notify(odd, String(repeating: "x", count: 7_000))
            out.append("\(try await listener.next(timeout: .seconds(2))?.payload.count ?? 0)")
            do {
                try await pool.notify(odd, String(repeating: "x", count: 8_001))
                out.append("accepted")
            } catch let error as PostgresClientError {
                out.append(error.sqlState ?? "\(error)")
            }
            out.append("\(listener.isOpen)")
            return out.joined(separator: "|")
        }
        #expect(text == "true 42 true|quiet|self true|true quoted|two|deaf|(idle: 1, inUse: 0)|7000|22023|true",
                "\(text)")
    }

    /// A notification sent in a transaction is news of what that transaction
    /// did: it arrives when the transaction commits, and never if it does not.
    @Test func aTransactionsNotificationWaitsForItsCommit() {
        let text = onPool { pool in
            let channel = "vega_orders"
            let listener = try await pool.listen(channel)
            defer { listener.close() }
            var out: [String] = []

            // Rolled back, so it was never sent.
            do {
                try await pool.transaction { tx in
                    try await tx.notify(channel, "rolled back")
                    throw Refuse()
                }
                out.append("committed")
            } catch is Refuse {
                out.append("rolled back")
            }
            out.append(try await listener.next(timeout: .milliseconds(300)) == nil ? "silent" : "leaked")

            // Committed, so it was -- and not before.
            try await pool.transaction { tx in
                try await tx.notify(channel, "committed")
                let early = try await listener.next(timeout: .milliseconds(200))
                out.append(early == nil ? "not yet" : "early")
            }
            out.append(try await listener.next(timeout: .seconds(2))?.payload ?? "none")
            return out.joined(separator: "|")
        }
        #expect(text == "rolled back|silent|not yet|committed", "\(text)")
    }
}
