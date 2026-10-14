//===----------------------------------------------------------------------===//
// The values a uuid and a timestamptz column hold, and the way an
// application's own types read from a cell.
//
// Garuda has its own `UUID` and `Timestamp`, used well beyond the database;
// this library has neither, and no Foundation. So it reads those columns into
// `PostgresUUID` and `PostgresTimestamp`, which say no more than PostgreSQL
// does, and an application's type that is one of them by another name
// conforms to `PostgresCellConvertible`:
//
//     extension UUID: PostgresCellConvertible {
//         public init(postgresRepresentation value: PostgresUUID) throws {
//             self.init(bytes: value.bytes)!
//         }
//     }
//
// after which `first(User.self, ...)` decodes a `UUID` property from a uuid
// column, in binary or in text, exactly as it decodes a `PostgresUUID`.
//===----------------------------------------------------------------------===//

import VegaPostgresProtocol

/// A type that a cell decodes into by way of one this library reads: a uuid
/// column into the application's own UUID type through `PostgresUUID`, a
/// json column into a document through its text.
public protocol PostgresCellConvertible: Decodable {
    /// What the cell is read as first: `PostgresUUID`, `PostgresTimestamp`,
    /// `String`, any type a cell decodes into.
    associatedtype PostgresRepresentation: Decodable
    init(postgresRepresentation: PostgresRepresentation) throws
}

extension PostgresCellConvertible {
    /// A cell's value: read as the representation, then converted.
    static func converted(from cell: PostgresCell) throws -> Self {
        try Self(postgresRepresentation: try cell.decode(PostgresRepresentation.self))
    }

    /// An array element's value, the same way.
    static func converted(from element: PostgresTextValue) throws -> Self {
        try Self(postgresRepresentation: try element.decode(PostgresRepresentation.self))
    }
}

/// A uuid column's value: sixteen bytes.
public struct PostgresUUID: Hashable, Comparable, Sendable, Codable, CustomStringConvertible {
    public let bytes: [UInt8]

    public init?(bytes: [UInt8]) {
        guard bytes.count == 16 else { return nil }
        self.bytes = bytes
    }

    /// From the canonical text, `a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11`.
    public init?(_ text: String) {
        guard let bytes = UUIDText.parse(text.utf8) else { return nil }
        self.bytes = bytes
    }

    public var description: String { UUIDText.format(bytes) }

    public static func < (a: PostgresUUID, b: PostgresUUID) -> Bool {
        a.bytes.lexicographicallyPrecedes(b.bytes)
    }

    public init(from decoder: any Decoder) throws {
        let text = try decoder.singleValueContainer().decode(String.self)
        guard let value = PostgresUUID(text) else {
            throw DecodingError.dataCorrupted(.init(codingPath: decoder.codingPath,
                                                    debugDescription: "not a UUID: \(text)"))
        }
        self = value
    }

    public func encode(to encoder: any Encoder) throws {
        try description.encode(to: encoder)
    }
}

extension PostgresUUID: PostgresBindable {
    public var postgresValue: PostgresValue { .binary(bytes, type: PostgresType.uuid) }
}

/// A timestamptz -- or timestamp -- column's value: an instant, to the
/// microsecond. Infinity has no instant, and is refused.
public struct PostgresTimestamp: Hashable, Comparable, Sendable, Codable, CustomStringConvertible {
    public var microsecondsSinceEpoch: Int64

    public init(microsecondsSinceEpoch: Int64) {
        self.microsecondsSinceEpoch = microsecondsSinceEpoch
    }

    /// From ISO 8601 or PostgreSQL's ISO DateStyle, with any UTC offset.
    public init?(_ text: String) {
        guard let micros = CivilTime.parse(text.utf8) else { return nil }
        microsecondsSinceEpoch = micros
    }

    /// ISO 8601, in UTC: `2026-09-16T20:53:43.196Z`.
    public var description: String { CivilTime.format(microseconds: microsecondsSinceEpoch, .iso8601) }

    public static func < (a: PostgresTimestamp, b: PostgresTimestamp) -> Bool {
        a.microsecondsSinceEpoch < b.microsecondsSinceEpoch
    }

    /// PostgreSQL counts from 2000-01-01, not 1970.
    static let postgresEpochOffset: Int64 = 946_684_800_000_000

    public init(from decoder: any Decoder) throws {
        let text = try decoder.singleValueContainer().decode(String.self)
        guard let value = PostgresTimestamp(text) else {
            throw DecodingError.dataCorrupted(.init(codingPath: decoder.codingPath,
                                                    debugDescription: "not a timestamp: \(text)"))
        }
        self = value
    }

    public func encode(to encoder: any Encoder) throws {
        try description.encode(to: encoder)
    }
}

extension PostgresTimestamp: PostgresBindable {
    public var postgresValue: PostgresValue {
        let (since2000, overflow) = microsecondsSinceEpoch.subtractingReportingOverflow(Self.postgresEpochOffset)
        // Only within 32 years of Int64's floor, far outside what the server
        // accepts anyway; as text it gets the server's own range error.
        if overflow { return PostgresValue(CivilTime.format(microseconds: microsecondsSinceEpoch, .postgres)) }
        return .binary(withUnsafeBytes(of: since2000.bigEndian) { Array($0) }, type: PostgresType.timestamptz)
    }
}

// MARK: - Text

/// `%XX` undone.
func percentDecoded(_ text: String) -> String {
    let percent = UInt8(ascii: "%")
    guard text.utf8.contains(percent) else { return text }
    func hex(_ byte: UInt8) -> Int {
        switch byte {
        case UInt8(ascii: "0")...UInt8(ascii: "9"): return Int(byte - UInt8(ascii: "0"))
        case UInt8(ascii: "a")...UInt8(ascii: "f"): return Int(byte - UInt8(ascii: "a")) + 10
        case UInt8(ascii: "A")...UInt8(ascii: "F"): return Int(byte - UInt8(ascii: "A")) + 10
        default: return -1
        }
    }
    var out: [UInt8] = []
    out.reserveCapacity(text.utf8.count)
    let bytes = Array(text.utf8)
    var i = 0
    while i < bytes.count {
        if bytes[i] == percent, i + 2 < bytes.count {
            let high = hex(bytes[i + 1])
            let low = hex(bytes[i + 2])
            if high >= 0 && low >= 0 {
                out.append(UInt8(high << 4 | low))
                i += 3
                continue
            }
        }
        out.append(bytes[i])
        i += 1
    }
    return String(decoding: out, as: UTF8.self)
}

extension String {
    /// Without whitespace at either end, by the Unicode property.
    func trimmingWhitespace() -> String {
        let scalars = unicodeScalars
        guard let start = scalars.firstIndex(where: { !$0.properties.isWhitespace }),
              let end = scalars.lastIndex(where: { !$0.properties.isWhitespace }) else { return "" }
        return String(scalars[start...end])
    }
}
