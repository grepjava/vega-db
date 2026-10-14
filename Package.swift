// swift-tools-version: 6.2
import PackageDescription

let swiftSettings: [SwiftSetting] = [.swiftLanguageMode(.v6)]

let package = Package(
    name: "VegaDB",
    platforms: [.macOS("26.4"), .iOS("26.4")],
    products: [
        .library(name: "VegaPool", targets: ["VegaPool"]),
        .library(name: "VegaRedis", targets: ["VegaRedis"]),
        .library(name: "VegaPostgres", targets: ["VegaPostgres"]),
        .library(name: "VegaDBTracing", targets: ["VegaDBTracing"]),
    ],
    // Vega's TLS library trait passes through: `traits: ["BoringSSL"]` on this
    // package puts VegaTLS on BoringSSL for its clients too.
    traits: [
        .trait(name: "OpenSSL"),
        .trait(name: "BoringSSL"),
        .default(enabledTraits: ["OpenSSL"]),
    ],
    dependencies: [
        // A path while both are developed together; the URL once vega-db is
        // published.
        .package(path: "../vega", traits: [
            .trait(name: "OpenSSL", condition: .when(traits: ["OpenSSL"])),
            .trait(name: "BoringSSL", condition: .when(traits: ["BoringSSL"])),
        ]),
    ],
    targets: [
        // Connections to a server, shared by tasks on any worker.
        .target(name: "VegaPool", dependencies: [.product(name: "Vega", package: "vega")],
                swiftSettings: swiftSettings),
        // RESP2/RESP3: replies parsed and commands written as bytes, with no
        // sockets. From Garuda's GarudaRedis.
        .target(name: "VegaRedisProtocol", swiftSettings: swiftSettings),
        // PostgreSQL's wire protocol, startup and SCRAM, COPY, and binary and
        // text values, with no sockets. From Garuda's GarudaPostgres.
        .target(name: "VegaPostgresProtocol",
                dependencies: [.product(name: "VegaTLS", package: "vega")],
                swiftSettings: swiftSettings),
        // What a client tells a tracer about each round trip, in
        // OpenTelemetry's database terms, with no tracer's API.
        .target(name: "VegaDBTracing", swiftSettings: swiftSettings),
        .target(name: "VegaRedis",
                dependencies: ["VegaPool", "VegaRedisProtocol", "VegaDBTracing",
                               .product(name: "Vega", package: "vega"),
                               .product(name: "VegaTLS", package: "vega")],
                swiftSettings: swiftSettings),
        .target(name: "VegaPostgres",
                dependencies: ["VegaPool", "VegaPostgresProtocol", "VegaDBTracing",
                               .product(name: "Vega", package: "vega"),
                               .product(name: "VegaTLS", package: "vega")],
                swiftSettings: swiftSettings),
        .testTarget(name: "VegaPostgresProtocolTests", dependencies: ["VegaPostgresProtocol"],
                    swiftSettings: swiftSettings),
        .testTarget(name: "VegaPostgresTests",
                    dependencies: ["VegaPostgres", "VegaPostgresProtocol", "VegaPool",
                                   .product(name: "Vega", package: "vega")],
                    swiftSettings: swiftSettings),
        .testTarget(name: "VegaPoolTests",
                    dependencies: ["VegaPool", .product(name: "Vega", package: "vega")],
                    swiftSettings: swiftSettings),
        .testTarget(name: "VegaRedisTests",
                    dependencies: ["VegaRedis", "VegaRedisProtocol", "VegaPool",
                                   .product(name: "Vega", package: "vega")],
                    swiftSettings: swiftSettings),
    ]
)
