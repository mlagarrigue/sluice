package postgres

import "fmt"

// OIDs of PostgreSQL's two JSON types. They are not interchangeable on the
// wire and mixing them is silent: see [AppendJSONB].
const (
	OIDJSON  uint32 = 114
	OIDJSONB uint32 = 3802
)

// JSONBVersion is the format byte every binary jsonb value begins with.
//
// It exists so that the layout can change without every reader guessing, and
// this package refuses any other value rather than parsing what follows as if
// it were version 1. A jsonb whose first byte is not this is not a jsonb this
// code has ever seen.
const JSONBVersion = 1

// AppendJSONB appends a jsonb value: the version byte, then the JSON text.
//
// # json and jsonb are one byte apart, and the byte is silent
//
// `json` stores the text exactly as it was given — whitespace, key order,
// duplicate keys and all. `jsonb` stores a parsed, normalised form, and its
// binary representation is the JSON text prefixed by [JSONBVersion].
//
// Sending a jsonb value without that byte makes the server read the first
// character of the JSON as the version and reject it, which is the loud
// failure. Reading a jsonb *as* json is the quiet one: the value comes back
// with a stray 0x01 in front of it, which is not valid JSON but is a perfectly
// good string, and it will travel some distance before anything notices.
//
// The text is not validated here. The server parses it and rejects what it
// cannot read, with a SQLSTATE that says where — which is a better error than
// anything this package could produce, and it is the same parser that will
// have to agree.
func AppendJSONB(dst, json []byte) []byte {
	return append(append(dst, JSONBVersion), json...)
}

// DecodeJSONB returns the JSON text of a jsonb value, checking its version
// byte.
//
// The slice borrows the batch's storage, like every other value here: it is
// valid until the next read. Copy it to keep it.
func DecodeJSONB(b []byte) ([]byte, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("%w: jsonb value is empty, with not even a version byte", ErrCodec)
	}
	if b[0] != JSONBVersion {
		// Refused rather than skipped. A future version would change what
		// follows, and reading it as version 1 would return text that is
		// plausible and wrong.
		return nil, fmt.Errorf("%w: jsonb version byte is %d, this package reads %d", ErrCodec, b[0], JSONBVersion)
	}
	return b[1:], nil
}

// AppendJSON appends a json value, which is the text and nothing else.
//
// It is one line and it exists anyway, because the call site is where the
// difference with [AppendJSONB] has to be visible. A caller reaching for
// "append some JSON" and finding one function would pick wrong half the time.
func AppendJSON(dst, json []byte) []byte { return append(dst, json...) }

// DecodeJSON returns the text of a json value, which has no version byte.
// The slice borrows the batch's storage.
func DecodeJSON(b []byte) []byte { return b }
