import Testing
@testable import VegaPostgresProtocol

// SASLprep's mapping step, and what it is for: a password whose bytes the
// client and the server have to agree on before SCRAM can work.

@Suite("SASLprep")
struct SaslPrepTests {
    @Test func asciiPasswordsAreLeftAlone() throws {
        for password in ["", "secret", "p@ssw0rd!", "a b c", "\t\n"] {
            #expect(SaslPrep.mapped(password) == password)
        }
    }

    @Test func nonAsciiSpacesBecomeOneSpace() throws {
        // The one that costs people an afternoon: a non-breaking space that
        // came in with a paste.
        #expect(SaslPrep.mapped("pa\u{00A0}ss") == "pa ss")
        #expect(SaslPrep.mapped("pa\u{2000}ss") == "pa ss")
        #expect(SaslPrep.mapped("pa\u{3000}ss") == "pa ss")
        #expect(SaslPrep.mapped("pa\u{205F}ss") == "pa ss")
        // A zero-width space is in both tables, and the server maps it to a
        // space before it would map it to nothing.
        #expect(SaslPrep.mapped("pa\u{200B}ss") == "pa ss")
    }

    @Test func whatIsCommonlyMappedToNothingGoes() throws {
        #expect(SaslPrep.mapped("pa\u{00AD}ss") == "pass", "a soft hyphen")
        #expect(SaslPrep.mapped("pa\u{200C}ss") == "pass", "a zero-width non-joiner")
        #expect(SaslPrep.mapped("pa\u{FEFF}ss") == "pass", "a byte-order mark")
        #expect(SaslPrep.mapped("pa\u{FE0F}ss") == "pass", "a variation selector")
    }

    @Test func charactersWithNoRuleAreKept() throws {
        // Accents, other scripts, emoji: SASLprep's mapping does nothing to
        // them, and neither does this.
        for password in ["pässwörd", "пароль", "密碼", "p🔒ss"] {
            #expect(SaslPrep.mapped(password) == password)
        }
    }
}
