#if canImport(Glibc)
import Glibc
#elseif canImport(Darwin)
import Darwin
#endif
import Vega
@testable import VegaRedis

// Real servers for the integration suites, named by environment variables
// that `scripts/redis-test-servers.sh up` prints. Each suite is skipped
// without its variable, so the unit suite never depends on a server.

func environment(_ name: String) -> String? {
    getenv(name).map { String(cString: $0) }
}

/// "host:port:password..." split at its colons.
private func fields(_ name: String) -> [Substring]? {
    environment(name)?.split(separator: ":", omittingEmptySubsequences: false)
}

private func plain(host: Substring, port: UInt16, password: Substring?) -> RedisConfiguration {
    var configuration = RedisConfiguration(host: String(host), port: port,
                                           password: password.flatMap { $0.isEmpty ? nil : String($0) })
    configuration.tls = .disable
    configuration.timeout = .seconds(5)
    return configuration
}

/// VEGA_REDIS: host:port:password of one server. The ACL tests expect a user
/// `app` with password `app-secret` allowed only keys under `app:`.
let redisTarget: RedisConfiguration? = {
    guard let parts = fields("VEGA_REDIS"), parts.count >= 2, let port = UInt16(parts[1]) else { return nil }
    return plain(host: parts[0], port: port, password: parts.count > 2 ? parts[2] : nil)
}()

/// VEGA_REDIS_SOCKET: a unix socket on the same server.
let redisSocket: String? = environment("VEGA_REDIS_SOCKET")

/// VEGA_REDIS_CLUSTER: host:port:password of any node of a cluster.
let clusterSeed: RedisConfiguration? = {
    guard let parts = fields("VEGA_REDIS_CLUSTER"), parts.count == 3, let port = UInt16(parts[1]) else { return nil }
    return plain(host: parts[0], port: port, password: parts[2])
}()

/// VEGA_REDIS_SENTINEL: host:port:master:password -- a sentinel, the name
/// its master is watched under, and the master's password.
let sentinelTarget: (sentinel: RedisConfiguration, master: String, server: RedisConfiguration)? = {
    guard let parts = fields("VEGA_REDIS_SENTINEL"), parts.count == 4, let port = UInt16(parts[1]) else { return nil }
    return (plain(host: parts[0], port: port, password: nil), String(parts[2]),
            plain(host: parts[0], port: 0, password: parts[3]))
}()

/// A key of its own for each test and run.
func uniqueKey(_ name: String) -> String {
    "vega-test:\(name):\(UInt64.random(in: 0...UInt64.max))"
}

/// Runs `body` on a two-worker runtime with what `make` builds, closes it,
/// and returns what `body` returned, or "threw <error>".
func onRuntime<Client: Sendable>(_ make: @escaping @Sendable () -> Client, close: @escaping @Sendable (Client) -> Void,
                                 _ body: @escaping @Sendable (Client) async throws -> String) -> String {
    let rt = Runtime(workers: 2)
    defer { rt.shutdown() }
    return rt.block { () async -> String in
        let client = make()
        defer { close(client) }
        do {
            return try await body(client)
        } catch {
            return "threw \(error)"
        }
    }
}
