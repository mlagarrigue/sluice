package postgres

import (
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

//go:generate go run saslprep_gen.go

// saslprep prepares a password for SCRAM as SASLprep (RFC 4013) defines it,
// the way PostgreSQL's pg_saslprep does. RFC 4013 and the stringprep of RFC
// 3454 are obsolete (RFC 7613, then 8265, and RFC 7564 replace them), but
// SCRAM (RFC 5802 §5.1) and PostgreSQL still name SASLprep, so it is the
// one that derives the stored key — and it must be that way rather than
// the RFC's alone, because the server ran the same function on the password
// when it stored the role's key, and a client that prepares the password any
// other way derives another key. The steps are RFC 3454 §2's:
//
//  1. map: non-ASCII spaces (table C.1.2) become U+0020, and the characters
//     of table B.1 — soft hyphen, zero-width joiners, variation selectors —
//     are removed;
//  2. normalise to Unicode NFKC, which folds compatibility forms: "Ⅸ" becomes
//     "IX", "ｐａｓｓ" becomes "pass";
//  3. prohibit control, private-use and non-character code points, among
//     others (RFC 4013 §2.3), and code points unassigned in Unicode 3.2
//     (§2.5);
//  4. check bidirectional text (RFC 3454 §6): a string that holds a
//     right-to-left character starts and ends with one and holds no
//     left-to-right character.
//
// A password that is not valid UTF-8, that maps to nothing, or that fails
// step 3 or 4 is returned unchanged, as pg_saslprep's callers do on both
// ends: the raw bytes are what the server used too. Like pg_saslprep, steps 3
// and 4 judge the mapped string before normalisation, not its output as the
// RFC writes it; the two differ only on a code point NFKC turns from
// prohibited to allowed (U+0340 to U+0300), and the server's answer is the
// one that authenticates.
func saslprep(password string) string {
	if isASCII(password) {
		// The whole procedure is the identity on ASCII: no ASCII character
		// is mapped, NFKC leaves it alone, and the only prohibited ones —
		// the controls — send the password back unchanged anyway.
		return password
	}
	if !utf8.ValidString(password) {
		return password
	}
	mapped := make([]rune, 0, len(password))
	for _, r := range password {
		switch {
		case unicode.Is(saslprepSpace, r):
			mapped = append(mapped, ' ')
		case unicode.Is(saslprepNothing, r):
		default:
			mapped = append(mapped, r)
		}
	}
	if len(mapped) == 0 {
		return password
	}
	rtl := false
	for _, r := range mapped {
		if unicode.Is(saslprepProhibited, r) {
			return password
		}
		rtl = rtl || unicode.Is(saslprepRandALCat, r)
	}
	if rtl {
		if !unicode.Is(saslprepRandALCat, mapped[0]) || !unicode.Is(saslprepRandALCat, mapped[len(mapped)-1]) {
			return password
		}
		for _, r := range mapped {
			if unicode.Is(saslprepLCat, r) {
				return password
			}
		}
	}
	return norm.NFKC.String(string(mapped))
}

func isASCII(s string) bool {
	for i := range len(s) {
		if s[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}
