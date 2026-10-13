# VegaDB

Database clients on [Vega](https://github.com/grepjava/vega), ported from [Garuda](https://github.com/grepjava/garuda)'s in-tree drivers so they run on Vega's work-stealing runtime: connections and pools are shared by tasks on any worker, not tied to one thread.

| Product | What |
|---|---|
| `VegaPool` | `ConnectionPool`: a fair, cancellable pool of connections to one server, any client's |
| `VegaRedis` | Redis and Valkey: RESP3 (RESP2 for servers before 6), pipelines, transactions, sessions with WATCH, pub/sub, TLS, Unix sockets |
| `VegaRedisProtocol` | Redis's protocol with no sockets: replies parsed and commands written as bytes |

```swift
import VegaRedis

let redis = RedisPool(RedisConfiguration(host: "cache"))
let visits = try await redis.incr("visits:home")
```

**Status:** pre-alpha.
- **Ported:** the shared pool and Redis's single-server client (connection, pool, sessions, pub/sub).
- **Still to port from Garuda:** Redis Cluster and Sentinel, PostgreSQL (`VegaPostgres`), and SQLite (`VegaSQLite`).

## TLS

`VegaRedis` uses VegaTLS, which runs on the system's OpenSSL by default. Enable this package's `BoringSSL` trait to use Vega's vendored BoringSSL instead. The trait passes through to Vega.

## Build

```sh
swift build --scratch-path ~/.cache/vega-db-build
swift test  --scratch-path ~/.cache/vega-db-build
```

Vega is a path dependency (`../vega`) while the two are developed together.
