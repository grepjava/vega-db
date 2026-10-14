import Testing
import Vega
@testable import VegaPostgres

// Composite types and enums: the text form a composite takes, and both
// against a real server -- where an enum turns out to need nothing of its own.

@Suite("PostgreSQL composite types")
struct PostgresRecordTests {
    @Test func compositesAreReadFromTheirTextForm() throws {
        #expect(PostgresRecordText.parse("(1,ada)") == ["1", "ada"])
        // A field that is empty and unquoted is NULL -- which is a composite's
        // way, and not an array's word NULL.
        #expect(PostgresRecordText.parse("(1,)") == ["1", nil])
        #expect(PostgresRecordText.parse("(,)") == [nil, nil])
        #expect(PostgresRecordText.parse("()") == [nil])
        #expect(PostgresRecordText.parse(#"(1,"")"#) == ["1", ""], "quoted, so empty and not NULL")
        #expect(PostgresRecordText.parse(#"(1,"NULL")"#) == ["1", "NULL"])
        #expect(PostgresRecordText.parse("(1,NULL)") == ["1", "NULL"], "the word is a value here")
        // What quoting is for.
        #expect(PostgresRecordText.parse(#"("a,b",c)"#) == ["a,b", "c"])
        #expect(PostgresRecordText.parse(#"("say ""hi""")"#) == [#"say "hi""#])
        #expect(PostgresRecordText.parse(#"("back\\slash")"#) == [#"back\slash"#])
        #expect(PostgresRecordText.parse(#"("(parens)")"#) == ["(parens)"])
        // Whitespace outside quotes is not part of a field; inside it is.
        #expect(PostgresRecordText.parse("( a , b )") == ["a", "b"])
        #expect(PostgresRecordText.parse(#"(" a ")"#) == [" a "])
        #expect(PostgresRecordText.parse("  (1,2)  ") == ["1", "2"])

        // Not a composite.
        #expect(PostgresRecordText.parse("1,2") == nil)
        #expect(PostgresRecordText.parse("") == nil)
        #expect(PostgresRecordText.parse("(1,2") == nil)
        #expect(PostgresRecordText.parse(#"("unterminated)"#) == nil)
    }

    @Test func compositesAreWrittenBackTheSameWay() throws {
        #expect(PostgresRecordText.format(["1", "ada"]) == #"("1","ada")"#)
        #expect(PostgresRecordText.format(["1", nil]) == #"("1",)"#, "NULL is nothing at all")
        #expect(PostgresRecordText.format([nil]) == "()")
        #expect(PostgresRecordText.format([""]) == #"("")"#)
        #expect(PostgresRecordText.format(["a,b"]) == #"("a,b")"#)
        #expect(PostgresRecordText.format([#"say "hi""#]) == #"("say ""hi""")"#)
        #expect(PostgresRecordText.format([#"back\slash"#]) == #"("back\\slash")"#)

        // Whatever is written comes back.
        for fields in [["1", "ada"], [nil], ["", nil, "x"], [#"a,b"c\d"#], [" padded "],
                       ["(", ")", ","]] as [[String?]] {
            #expect(PostgresRecordText.parse(PostgresRecordText.format(fields)) == fields, "\(fields)")
        }
    }

    @Test func aRecordIsItsFields() throws {
        let record = try #require(PostgresRecord("(12 Mill Lane,Cambridge,)"))
        #expect(record.fields == ["12 Mill Lane", "Cambridge", nil])
        #expect(record[0] == "12 Mill Lane")
        #expect(record[2] == nil)
        #expect(record[9] == nil, "a field that is not there")
        #expect(PostgresRecord("nonsense") == nil)
        #expect(boundRecordText(PostgresRecord(["a", nil]).postgresValue) == #"("a",)"#)
    }
}

/// What a bound value carries, as text.
private func boundRecordText(_ value: PostgresValue) -> String? {
    guard case .text(let bytes) = value else { return nil }
    return String(decoding: bytes, as: UTF8.self)
}

// MARK: - Against a real server

private enum Condition: String, Codable, Sendable {
    case new
    case used
}

@Suite("PostgreSQL composite types round trip", .serialized,
       .enabled(if: postgresTarget != nil, "set VEGA_POSTGRES to run"))
struct PostgresRecordRoundTripTests {
    /// A composite and an enum through a real server: made, written, read, and
    /// the types dropped again, all in one transaction.
    @Test func compositesAndEnumsSurviveTheServer() {
        let text = onPool { pool in
            try await pool.transaction { tx -> String in
                try await tx.execute("drop type if exists vega_address cascade")
                try await tx.execute("drop type if exists vega_condition cascade")
                try await tx.execute("create type vega_address as (street text, city text)")
                try await tx.execute("create type vega_condition as enum ('new', 'used')")
                try await tx.execute("create temp table record_test "
                                         + "(home vega_address, state vega_condition)")

                struct Row: Decodable {
                    let home: PostgresRecord
                    let state: Condition
                    let empty: PostgresRecord
                }
                var out: [String] = []
                // Bound as a composite and as an enum's label, both text
                // the server parses into its own types.
                try await tx.execute(
                    "insert into record_test (home, state) values ($1::vega_address, $2::vega_condition)",
                    PostgresRecord(["12 Mill Lane, flat 2", #"O"Brien"#]), Condition.used.rawValue)
                let row = try await tx.first(Row.self, """
                    select home, state, row('a', null)::vega_address as empty
                    from record_test
                    """)
                out.append(row?.home.fields.count == 2 ? "two" : "?")
                out.append(row?.home[0] ?? "nil")
                out.append(row?.home[1] ?? "nil")
                out.append("\(row?.state == .used)")
                out.append("\(row?.empty.fields == ["a", nil])")

                // What the server itself says the composite is, so a wrong
                // reading cannot agree with a wrong writing.
                struct Text: Decodable { let text: String }
                let said = try await tx.first(Text.self, "select home::text as text from record_test")
                out.append("\(PostgresRecordText.parse(said?.text ?? "") == row?.home.fields)")

                // An enum needs no type of its own: it is its label.
                let state = try await tx.first(Condition.self, "select state from record_test")
                out.append("\(state == .used)")

                try await tx.execute("drop table record_test")
                try await tx.execute("drop type vega_address cascade")
                try await tx.execute("drop type vega_condition cascade")
                return out.joined(separator: "|")
            }
        }
        #expect(text == #"two|12 Mill Lane, flat 2|O"Brien|true|true|true|true"#, "\(text)")
    }
}
