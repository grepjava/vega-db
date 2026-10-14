import Testing
import Vega
@testable import VegaPostgres

// SASLprep against a real server: what VegaPostgresProtocolTests's
// PostgresSaslPrepTests check a password is mapped to, checked here by
// authenticating with it. Set VEGA_POSTGRES to run; see IntegrationSupport.

@Suite("SASLprep against a real server", .serialized, .enabled(if: postgresTarget != nil, "set VEGA_POSTGRES to run"))
struct SaslPrepIntegrationTests {
    /// A role whose password holds a non-breaking space. The server maps it
    /// when it stores the verifier, so a client that does not map it cannot
    /// authenticate -- and one that does can, with either spelling.
    @Test func aPasswordWithANonAsciiSpaceAuthenticates() {
        let configuration = postgresTarget!
        let text = onPool { pool in
            let role = "vega_prep_test"
            let withSpace = "pa\u{00A0}ss-\u{00AD}word"
            @Sendable func connects(as password: String) async -> String {
                var settings = configuration
                settings.user = role
                settings.password = password
                do {
                    let connection = try await PostgresConnection.connect(settings, tls: nil)
                    defer { connection.close() }
                    let rows = try await connection.query("select current_user")
                    return rows.text(row: 0, column: 0) == role ? "in" : "wrong user"
                } catch {
                    if case .postgres(.server(let fields)) = error { return fields.code }
                    return "\(error)"
                }
            }
            do {
                // The server SASLpreps this before it stores the verifier.
                try await pool.execute("drop role if exists \"\(role)\"")
                try await pool.execute("create role \"\(role)\" login password '\(withSpace)'")
                var out: [String] = []
                // As it was written.
                out.append(await connects(as: withSpace))
                // And as SASLprep leaves it, which is the same password: an
                // ordinary space where the non-breaking one was, and the soft
                // hyphen gone.
                out.append(await connects(as: "pa ss-word"))
                // Something else is still refused.
                out.append(await connects(as: "pa ss-word!"))
                try await pool.execute("drop role \"\(role)\"")
                return out.joined(separator: "|")
            } catch {
                _ = try? await pool.execute("drop role if exists \"\(role)\"")
                return "threw \(error)"
            }
        }
        #expect(text == "in|in|28P01", "\(text)")
    }
}
