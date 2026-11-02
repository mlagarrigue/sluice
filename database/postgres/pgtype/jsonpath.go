package pgtype

import "fmt"

// jsonpathVersion is the one version byte the server writes before a
// jsonpath's text (JSONPATH_VERSION, src/include/utils/jsonpath.h) and the
// only one it reads back.
const jsonpathVersion = 1

// AppendJSONPath appends a jsonpath: the version byte, then the path's text,
// which the server parses on receipt — a path it cannot parse is its error
// to report, as for any other text.
//
// The other text-like types need no pair of their own: xml and refcursor
// are text on the wire ([AppendText], [OIDXML], [OIDRefcursor]; xml in the
// client encoding, which this connector sets to UTF-8), and an enum value is
// its label, as text, under the OID its database gave the type — read with
// postgres.ScanText, which takes any type a database created, and written
// as text with OID 0 where the query names the type ($1::mood).
func AppendJSONPath(dst []byte, path string) []byte {
	return append(append(dst, jsonpathVersion), path...)
}

// DecodeJSONPath decodes a jsonpath to its text, refusing a version byte
// other than the one the server writes.
func DecodeJSONPath(b []byte) (string, error) {
	if len(b) == 0 || b[0] != jsonpathVersion {
		return "", fmt.Errorf("%w: jsonpath without version byte %d", ErrCodec, jsonpathVersion)
	}
	return string(b[1:]), nil
}
