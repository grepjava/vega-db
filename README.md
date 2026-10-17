# VegaDB

Database clients on [Vega](https://github.com/grepjava/vega), ported from [Garuda](https://github.com/grepjava/garuda)'s in-tree drivers so they run on Vega's work-stealing runtime: connections and pools are shared by tasks on any worker, not tied to one thread.

| Product | What |
|---|---|
| `VegaPool` | `ConnectionPool`: a fair, cancellable pool of connections to one server, any client's |
| `VegaRedis` | Redis and Valkey: RESP3 (RESP2 for servers before 6), pipelines, transactions, sessions with WATCH, pub/sub, TLS, Unix sockets; `RedisCluster` (slot routing, MOVED/ASK) and `RedisSentinelPool` (master discovery and failover) |
| `VegaRedisProtocol` | Redis's protocol with no sockets: replies parsed and commands written as bytes |
| `VegaPostgres` | PostgreSQL: SCRAM and TLS, prepared-statement caching, binary results, rows decoded into `Decodable` types, transactions, COPY in and out, LISTEN/NOTIFY, connection URLs, migrations |
| `VegaPostgresProtocol` | PostgreSQL's wire protocol with no sockets: startup, SCRAM-SHA-256, queries, COPY, binary and text values |
| `VegaSQLite` | SQLite through the system's libsqlite3 (loaded at run time): a writer and pooled readers, statements on Vega's blocking pool, migrations |
| `VegaDBTracing` | `DatabaseTracer`: each round trip described in OpenTelemetry's database terms, for an application to turn into spans |

```swift
import VegaRedis

let redis = RedisPool(RedisConfiguration(host: "cache"))
let visits = try await redis.incr("visits:home")
```

```swift
import VegaPostgres

struct User: Decodable { let id: Int; let name: String }

let db = PostgresPool(try PostgresConfiguration(url: "postgres://app:secret@db/shop"))
let user = try await db.first(User.self, "select id, name from users where id = $1", 42)
```

**Status:** pre-alpha.
- **Ported:** the shared pool; Redis (single server, Cluster, Sentinel); PostgreSQL; SQLite.
- **Garuda's own types stay in Garuda:** its `UUID`, `Timestamp` and `PostgresJSON`, and the `@PostgresRow` macro. Here a uuid or timestamptz column reads as `PostgresUUID` or `PostgresTimestamp`, and `PostgresCellConvertible` and `SQLiteCellConvertible` let an application's own type decode from a cell.

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

`VegaRedis` and `VegaPostgres` use VegaTLS, which runs on the system's OpenSSL by default. Enable this package's `BoringSSL` trait to use Vega's vendored BoringSSL instead. The trait passes through to Vega.

## Build

```sh
swift build --scratch-path ~/.cache/vega-db-build
swift test  --scratch-path ~/.cache/vega-db-build
```

vega-db takes [Vega](https://github.com/grepjava/vega) 0.5 from GitHub. To build against a local checkout instead, set `VEGA_PATH` to it (`VEGA_PATH=../vega swift build`). Like Vega, vega-db builds with `NonisolatedNonsendingByDefault`: its async functions run on the caller's executor.

In an application:

```swift
.package(url: "https://github.com/grepjava/vega-db.git", .upToNextMinor(from: "0.2.0")),
```

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
