# VegaDB

Database clients on [Vega](https://github.com/grepjava/vega), ported from [Garuda](https://github.com/grepjava/garuda)'s in-tree drivers so they run on Vega's work-stealing runtime: connections and pools are shared by tasks on any worker, not tied to one thread.

| Product | What |
|---|---|
| `VegaPool` | `ConnectionPool`: a fair, cancellable pool of connections to one server, any client's |
| `VegaRedis` | Redis and Valkey: RESP3 (RESP2 for servers before 6), pipelines, transactions, sessions with WATCH, pub/sub, TLS, Unix sockets; `RedisCluster` (slot routing, MOVED/ASK) and `RedisSentinelPool` (master discovery and failover) |
| `VegaRedisProtocol` | Redis's protocol with no sockets: replies parsed and commands written as bytes |
| `VegaDBTracing` | `DatabaseTracer`: each round trip described in OpenTelemetry's database terms, for an application to turn into spans |

```swift
import VegaRedis

let redis = RedisPool(RedisConfiguration(host: "cache"))
let visits = try await redis.incr("visits:home")
```

**Status:** pre-alpha.
- **Ported:** the shared pool, and Redis: the single-server client (connection, pool, sessions, pub/sub), Cluster and Sentinel.
- **Still to port from Garuda:** PostgreSQL (`VegaPostgres`) and SQLite (`VegaSQLite`).

## Tracing

Set a configuration's `tracer` to hear about every round trip: a command, a pipeline, a transaction. Each is described in OpenTelemetry's database terms (`db.system.name`, `db.operation.name`, `server.address`), and arguments are never included. The clients depend on no tracing library, so the application bridges the description to its own tracer, such as swift-distributed-tracing. With no tracer set, nothing is described or called.

```swift
struct Spans: DatabaseTracer {
    func start(_ describe: () -> DatabaseOperation) -> (any DatabaseSpan)? {
        guard tracing else { return nil }   // not recording: describe is never built
        return MySpan(describe())           // MySpan's end() and fail(_:) close it
    }
}

var configuration = RedisConfiguration(host: "cache")
configuration.tracer = Spans()
```

## TLS

`VegaRedis` uses VegaTLS, which runs on the system's OpenSSL by default. Enable this package's `BoringSSL` trait to use Vega's vendored BoringSSL instead. The trait passes through to Vega.

## Build

```sh
swift build --scratch-path ~/.cache/vega-db-build
swift test  --scratch-path ~/.cache/vega-db-build
```

Vega is a path dependency (`../vega`) while the two are developed together.

## Integration tests

The unit suite needs no server: scripted fake servers stand in for Redis, a cluster and sentinels. The integration suites run against real ones, and each is skipped unless its variable is set. `scripts/redis-test-servers.sh` starts everything they need in one Docker container (Docker Desktop works too):

```sh
eval "$(scripts/redis-test-servers.sh up)"   # one server, a 3+3 cluster, a master, a replica, 3 sentinels
swift test --scratch-path ~/.cache/vega-db-build
scripts/redis-test-servers.sh down
```

| Variable | Value | Suite |
|---|---|---|
| `VEGA_REDIS` | `host:port:password` | one server: commands, transactions, ACL, pub/sub, limits |
| `VEGA_REDIS_SOCKET` | a unix socket on that server | the unix-socket test; set by the script only where the engine shares the filesystem (not Docker Desktop) |
| `VEGA_REDIS_CLUSTER` | `host:port:password` of any node | the cluster, including a failover it is asked to do |
| `VEGA_REDIS_SENTINEL` | `host:port:master:password` | sentinel discovery and a failover |

The servers listen on 127.0.0.1 ports 46001–46006 and 46379–46392, outside the ranges Windows reserves for Hyper-V. The failover tests leave the promoted replicas as masters. The other tests don't depend on which node is master, so the suites can be run again without restarting the servers.
