package audit

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
)

// The TLS a server negotiates, read from what `openssl s_client` printed,
// saved: one handshake per connection, "New, TLSv1.3, Cipher is
// TLS_AES_128_GCM_SHA256", "Reused, …" for a connection that resumed its
// session (-reconnect), "New, (NONE), Cipher is (NONE)" for a handshake
// the server refused. A transcript asked for one version (-tls1_2) that
// the server accepts is that version accepted. As everywhere else, the
// audit dials nothing: the files can be seeded.

// TLSHandshake is the connections of every transcript read.
type TLSHandshake struct {
	Paths []string
	Conns []tlsConn
}

type tlsConn struct {
	path    string
	reused  bool
	version string // "TLSv1.3", "TLSv1.2", "SSLv3"…
	cipher  string // OpenSSL's name
}

var sessionLine = regexp.MustCompile(`(?m)^(New|Reused), (\S+), Cipher is (\S+)`)

// openHandshakes reads every transcript named, comma-separated. A file
// with no session line is an error, not an empty reading: a transcript cut
// before its handshake would pass every rule.
func openHandshakes(paths string) (*TLSHandshake, error) {
	h := &TLSHandshake{}
	for _, path := range strings.Split(paths, ",") {
		path = strings.TrimSpace(path)
		b, err := os.ReadFile(path) //nolint:gosec // G304: the path is the caller's
		if err != nil {
			return nil, err
		}
		found := sessionLine.FindAllStringSubmatch(string(b), -1)
		if len(found) == 0 {
			return nil, errors.New(path + `: no "New, …, Cipher is …" line: want what openssl s_client printed`)
		}
		h.Paths = append(h.Paths, path)
		for _, m := range found {
			if m[2] == "(NONE)" || m[3] == "(NONE)" {
				continue // refused: nothing negotiated
			}
			h.Conns = append(h.Conns, tlsConn{path: path, reused: m[1] == "Reused", version: m[2], cipher: m[3]})
		}
	}
	return h, nil
}

// versions is each version negotiated, once, with the transcript that shows it.
func (h *TLSHandshake) versions(keep func(string) bool) []string {
	var out, seen []string
	for _, c := range h.Conns {
		if keep(c.version) && !slices.Contains(seen, c.version) {
			seen = append(seen, c.version)
			out = append(out, c.version+" ("+c.path+")")
		}
	}
	return out
}

// weakCipher says why a suite OpenSSL negotiated is weak, "" for none.
// TLS 1.3's suites are all AEAD with an ephemeral key exchange; before it,
// the name says both (RFC 9325 §4.2: AEAD, forward secrecy).
func weakCipher(name string) string {
	n := strings.ToUpper(name)
	switch {
	case strings.HasPrefix(n, "TLS_"):
		return ""
	case strings.Contains(n, "NULL"), strings.Contains(n, "EXP"):
		return "no encryption, or an export-grade one"
	case strings.Contains(n, "RC4"), strings.Contains(n, "DES"), strings.Contains(n, "MD5"):
		return "RC4, DES or MD5, broken"
	case strings.HasPrefix(n, "ADH") || strings.HasPrefix(n, "AECDH") || strings.Contains(n, "ANON"):
		return "anonymous: the server is not authenticated"
	case !strings.HasPrefix(n, "ECDHE-") && !strings.HasPrefix(n, "DHE-"):
		return "a static key exchange: no forward secrecy"
	case !strings.Contains(n, "GCM") && !strings.Contains(n, "CHACHA20") && !strings.Contains(n, "CCM"):
		return "CBC with a MAC, not an AEAD"
	}
	return ""
}

func init() {
	const tlsDoc = "https://www.rfc-editor.org/rfc/rfc9325"
	handshakes := func(t *Target) *TLSHandshake {
		if t.TLSHandshake == nil {
			return &TLSHandshake{}
		}
		return t.TLSHandshake
	}

	Register(Rule{
		Name:  "tls/legacy-version",
		Needs: Selection{Components: {"app"}, Goals: {"security"}, Sources: {"config"}},
		Doc:   tlsDoc,
		Check: func(t *Target, _ Answers) []Finding {
			old := handshakes(t).versions(func(v string) bool {
				return v == "SSLv3" || v == "TLSv1" || v == "TLSv1.1"
			})
			if len(old) == 0 {
				return nil
			}
			return []Finding{{
				Current:  "the server accepts " + strings.Join(old, ", "),
				Proposed: "TLS 1.2 at least, TLS 1.3 preferred: the older versions off in the server's TLS configuration",
				Why: "TLS 1.0 and 1.1 are deprecated (RFC 8996) and SSL 3.0 broken: a client can be led to " +
					"them, and their suites and their handshake carry the weaknesses later versions removed",
			}}
		},
	})

	Register(Rule{
		Name:  "tls/version-in-comparison",
		Needs: Selection{Components: {"app"}, Goals: {"performance"}, Sources: {"config"}, Profile: {"comparative"}},
		Doc:   tlsDoc,
		Check: func(t *Target, _ Answers) []Finding {
			v12 := handshakes(t).versions(func(v string) bool { return v == "TLSv1.2" })
			if len(v12) == 0 {
				return nil
			}
			return []Finding{{
				Current:  "the server accepts " + strings.Join(v12, ", "),
				Proposed: "TLS 1.3 alone, as every contender of the comparison",
				Why: "the study fixes TLS 1.3 for everyone: a TLS 1.2 handshake costs one more round trip, " +
					"and a client that ends on it measures another protocol than its neighbours",
			}}
		},
	})

	Register(Rule{
		Name:  "tls/weak-cipher",
		Needs: Selection{Components: {"app"}, Goals: {"security"}, Sources: {"config"}},
		Doc:   tlsDoc + "#section-4.2",
		Check: func(t *Target, _ Answers) []Finding {
			var out []Finding
			var seen []string
			for _, c := range handshakes(t).Conns {
				why := weakCipher(c.cipher)
				if why == "" || slices.Contains(seen, c.cipher) {
					continue
				}
				seen = append(seen, c.cipher)
				out = append(out, Finding{
					Current:  fmt.Sprintf("%s negotiated under %s (%s)", c.cipher, c.version, c.path),
					Proposed: "ECDHE with AES-GCM or ChaCha20-Poly1305 alone before TLS 1.3, the others off",
					Why:      "the suite is " + why,
				})
			}
			return out
		},
	})

	resumption := func(t *Target) (resumed, fresh int) {
		for _, c := range handshakes(t).Conns {
			if c.reused {
				resumed++
			} else {
				fresh++
			}
		}
		return resumed, fresh
	}

	Register(Rule{
		Name:  "tls/resumption-off",
		Needs: Selection{Components: {"app"}, Goals: {"performance"}, Sources: {"config"}, Profile: {"production"}},
		Doc:   "https://www.rfc-editor.org/rfc/rfc9846#section-2.2",
		Check: func(t *Target, _ Answers) []Finding {
			// Within one transcript only: s_client -reconnect offers the
			// session of its first connection to the next ones, two runs
			// share nothing.
			var out []Finding
			for _, path := range handshakes(t).Paths {
				n, resumed := 0, false
				for _, c := range handshakes(t).Conns {
					if c.path == path {
						n++
						resumed = resumed || c.reused
					}
				}
				if n < 2 || resumed {
					continue // one connection says nothing of the next
				}
				out = append(out, Finding{
					Current:  fmt.Sprintf("%d connections of one run, none resumed its session (%s)", n, path),
					Proposed: "session tickets or a session cache on, the tickets' keys rotated",
					Why: "a returning client then does the whole handshake again — the certificate sent and " +
						"checked, a key exchange — where a resumed one skips both",
				})
			}
			return out
		},
	})

	Register(Rule{
		Name:  "tls/resumption-in-comparison",
		Needs: Selection{Components: {"app"}, Goals: {"performance"}, Sources: {"config"}, Profile: {"comparative"}},
		Doc:   connBudgetDoc,
		Check: func(t *Target, _ Answers) []Finding {
			resumed, fresh := resumption(t)
			if resumed == 0 {
				return nil
			}
			return []Finding{{
				Current:  fmt.Sprintf("%d of %d connections resumed their session (%s)", resumed, resumed+fresh, strings.Join(handshakes(t).Paths, ", ")),
				Proposed: "resumption off in the comparison, as for every contender (see app/tls-resume)",
				Why: "the study's new connection is a new visitor: a resumed handshake skips the certificate " +
					"and the key exchange, and measures a cheaper connection than the others pay",
			}}
		},
	})
}
