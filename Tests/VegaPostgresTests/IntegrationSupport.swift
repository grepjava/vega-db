#if canImport(Glibc)
import Glibc
#elseif canImport(Darwin)
import Darwin
#endif
import Vega
@testable import VegaPostgres

// A real server for the integration suites, named by environment variables
// that `scripts/postgres-test-server.sh up` prints. Each suite is skipped
// without them, so the unit suite never depends on a database being up.

func environment(_ name: String) -> String? {
    getenv(name).map { String(cString: $0) }
}

/// VEGA_POSTGRES: host:port:user:password:database, for example
///
///     VEGA_POSTGRES=127.0.0.1:46432:vega:vega-secret:vega swift test
let postgresTarget: PostgresConfiguration? = {
    guard let raw = environment("VEGA_POSTGRES") else { return nil }
    let parts = raw.split(separator: ":", omittingEmptySubsequences: false)
    guard parts.count == 5, let port = UInt16(parts[1]) else { return nil }
    var configuration = PostgresConfiguration(host: String(parts[0]), port: port,
                                              user: String(parts[2]), password: String(parts[3]),
                                              database: String(parts[4]))
    configuration.tls = .disable
    configuration.timeout = .seconds(5)
    return configuration
}()

/// The same server over a unix socket, when VEGA_POSTGRES_SOCKET names the
/// directory it keeps one in.
let postgresSocketTarget: PostgresConfiguration? = {
    guard let path = environment("VEGA_POSTGRES_SOCKET"), let over = postgresTarget else { return nil }
    var configuration = PostgresConfiguration(unixSocketPath: path, user: over.user, password: over.password,
                                              database: over.database, port: over.port)
    configuration.timeout = .seconds(5)
    return configuration
}()

/// Runs `body` on a two-worker runtime and returns what it returned, or
/// "threw <error>".
func onRuntime(_ body: @escaping @Sendable () async throws -> String) -> String {
    let rt = Runtime(workers: 2)
    defer { rt.shutdown() }
    return rt.block { () async -> String in
        do {
            return try await body()
        } catch {
            return "threw \(error)"
        }
    }
}

/// Runs `body` with a pool of its own on the real server, closes it, and
/// returns what `body` returned, or "threw <error>".
func onPool(maxConnections: Int = 2, acquireTimeout: Duration? = nil,
            _ body: @escaping @Sendable (PostgresPool) async throws -> String) -> String {
    let configuration = postgresTarget!
    return onRuntime {
        let pool = PostgresPool(configuration, maxConnections: maxConnections, acquireTimeout: acquireTimeout)
        defer { pool.close() }
        return try await body(pool)
    }
}

/// A server's refusal as its SQLSTATE, and anything else as it is.
func describe(_ error: any Error) -> String {
    if let error = error as? PostgresClientError, case .postgres(.server(let fields)) = error {
        return "server:\(fields.code)"
    }
    return "\(error)"
}
