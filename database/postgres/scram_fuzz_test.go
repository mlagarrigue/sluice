package postgres

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// The SCRAM parsers read whatever answered the socket, and they read it before
// the far end has proved anything at all — the same class of surface as the
// framing layer next door, one message further in. What is asserted is the
// contract, not a value: they must not panic, must classify every failure as
// one of this package's own errors, and must never return a result that the
// checks above them were supposed to have refused.

func FuzzParseServerFirst(f *testing.F) {
	const clientNonce = "clientnonce"
	f.Add("r=" + clientNonce + "SERVER,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096")
	f.Add("r=,s=,i=")
	f.Add("")
	f.Add(",,,")
	f.Add("i=99999999999999999999999999")            // past int64
	f.Add("i=-4096,r=" + clientNonce + "S,s=AAAA")   // negative
	f.Add("m=mandatory,r=" + clientNonce + ",s=,i=") // an extension we must refuse
	f.Add("r=" + clientNonce + "S,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096,r=other")

	f.Fuzz(func(t *testing.T, msg string) {
		nonce, salt, iter, err := parseServerFirst(msg, clientNonce)
		if err != nil {
			if !errors.Is(err, ErrAuth) {
				t.Fatalf("unclassified error %v for %q", err, msg)
			}
			return
		}
		// Accepted. Everything the bounds promise must hold, because what
		// comes next spends CPU on it and then trusts the answer.
		switch {
		case !strings.HasPrefix(nonce, clientNonce) || len(nonce) == len(clientNonce):
			t.Fatalf("accepted a nonce %q that does not extend %q", nonce, clientNonce)
		case iter < minIterations || iter > maxIterations:
			t.Fatalf("accepted %d iterations, outside [%d, %d]", iter, minIterations, maxIterations)
		case len(salt) < minSaltLen:
			t.Fatalf("accepted a %d-byte salt, below %d", len(salt), minSaltLen)
		}
	})
}

func FuzzParseServerFinal(f *testing.F) {
	f.Add("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
	f.Add("e=invalid-proof")
	f.Add("")
	f.Add("v")
	f.Add("v=,e=,x=")
	f.Add("v=!!!!")

	f.Fuzz(func(t *testing.T, msg string) {
		sig, err := parseServerFinal(msg)
		if err != nil {
			if !errors.Is(err, ErrAuth) {
				t.Fatalf("unclassified error %v for %q", err, msg)
			}
			return
		}
		// A parsed signature is not an accepted one — verify still compares
		// it — but a nil returned beside a nil error would make that
		// comparison meaningless.
		if sig == nil {
			t.Fatalf("parseServerFinal(%q) returned no signature and no error", msg)
		}
	})
}

func FuzzParseMechanisms(f *testing.F) {
	f.Add([]byte("SCRAM-SHA-256\x00\x00"))
	f.Add([]byte("SCRAM-SHA-256\x00SCRAM-SHA-256-PLUS\x00\x00"))
	f.Add([]byte("\x00"))
	f.Add([]byte(""))
	f.Add([]byte("SCRAM-SHA-256"))

	f.Fuzz(func(t *testing.T, body []byte) {
		names, err := parseMechanisms(body)
		if err != nil {
			if !errors.Is(err, ErrProtocol) {
				t.Fatalf("unclassified error %v for %q", err, body)
			}
			return
		}
		// A terminated list was read: every name must have come from the body
		// it was read out of, which is what makes the choice that follows a
		// choice among what the server actually offered.
		for _, n := range names {
			if n == "" {
				t.Fatalf("parseMechanisms returned an empty name from %q", body)
			}
			if !strings.Contains(string(body), n) {
				t.Fatalf("parseMechanisms invented %q from %q", n, body)
			}
		}
	})
}

// parseAuthentication is the first thing read from whatever answered the
// socket: the code that picks the authentication method, from a peer that has
// proved nothing yet.
func FuzzParseAuthentication(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{0, 0, 0, 10, 'S', 'C', 'R', 'A', 'M', '-', 'S', 'H', 'A', '-', '2', '5', '6', 0, 0})
	f.Add([]byte{0, 0, 0, 11, 'r', '='})
	f.Add([]byte{0, 0, 0})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, body []byte) {
		code, data, err := parseAuthentication(body)
		if err != nil {
			if !errors.Is(err, ErrProtocol) {
				t.Fatalf("unclassified error %v for %x", err, body)
			}
			if len(body) >= 4 {
				t.Fatalf("refused a body of %d bytes, which holds a code", len(body))
			}
			return
		}
		// The code is the first four bytes and the data is exactly the rest:
		// a byte dropped or kept twice would shift every SCRAM field after it.
		if len(body) < 4 || len(data) != len(body)-4 || !bytes.Equal(data, body[4:]) {
			t.Fatalf("parseAuthentication(%x) = %d, %x", body, code, data)
		}
		if want := uint32(body[0])<<24 | uint32(body[1])<<16 | uint32(body[2])<<8 | uint32(body[3]); code != want {
			t.Fatalf("code = %d, want %d", code, want)
		}
	})
}

// A ParameterStatus can arrive at any point in a session, and during startup
// it arrives before the server has authenticated: name, value, and the
// bounds recordParameter enforces on what it keeps.
func FuzzParseParameterStatus(f *testing.F) {
	f.Add([]byte("TimeZone\x00UTC\x00"))
	f.Add([]byte("server_version\x0016.2\x00"))
	f.Add([]byte("\x00\x00"))
	f.Add([]byte("name-only\x00"))
	f.Add([]byte("unterminated"))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, body []byte) {
		name, val, err := parseParameterStatus(body)
		if err != nil {
			if !errors.Is(err, ErrProtocol) {
				t.Fatalf("unclassified error %v for %q", err, body)
			}
		} else {
			// Both C strings came out of the body, terminator-free.
			if strings.IndexByte(name, 0) >= 0 || strings.IndexByte(val, 0) >= 0 {
				t.Fatalf("a NUL survived into %q = %q", name, val)
			}
			if !bytes.HasPrefix(body, []byte(name+"\x00"+val+"\x00")) {
				t.Fatalf("parseParameterStatus(%q) = %q, %q: not what the body holds", body, name, val)
			}
		}

		// The remembered map stays within its bounds whatever arrives.
		c := &Conn{}
		rerr := c.recordParameter(body)
		if rerr != nil {
			if !errors.Is(rerr, ErrProtocol) {
				t.Fatalf("recordParameter: unclassified error %v for %q", rerr, body)
			}
			if len(c.params) != 0 {
				t.Fatalf("recordParameter kept %d entries while refusing", len(c.params))
			}
			return
		}
		if err != nil {
			t.Fatalf("recordParameter accepted %q, which parseParameterStatus refused", body)
		}
		if len(name)+len(val) > MaxServerParameterBytes {
			t.Fatalf("recordParameter kept %d bytes, over the limit", len(name)+len(val))
		}
		if c.Parameter(name) != val {
			t.Fatalf("Parameter(%q) = %q, want %q", name, c.Parameter(name), val)
		}
	})
}

// fuzzPeer is a transport whose reads are a fixed byte stream and whose
// writes go nowhere: the server's half of a startup, as a fuzzer wrote it.
type fuzzPeer struct{ r *bytes.Reader }

func (p fuzzPeer) Read(b []byte) (int, error)  { return p.r.Read(b) }
func (p fuzzPeer) Write(b []byte) (int, error) { return len(b), nil }
func (p fuzzPeer) Close() error                { return nil }

// The whole authentication exchange, driven by an arbitrary server: the
// method selection, the SASL messages and their SCRAM payloads, interleaved
// with ParameterStatus and errors. It must not panic, must classify every
// failure, and may only succeed by reaching a ReadyForQuery.
func FuzzAuthenticate(f *testing.F) {
	msg := func(typ byte, body []byte) []byte {
		out := []byte{typ, 0, 0, 0, 0}
		binary.BigEndian.PutUint32(out[1:], uint32(len(body)+4)) //nolint:gosec // G115: a fuzz seed
		return append(out, body...)
	}
	auth := func(code uint32, data string) []byte {
		return msg(pgwire.BackendAuthentication, append(binary.BigEndian.AppendUint32(nil, code), data...))
	}
	ready := msg(pgwire.BackendReadyForQuery, []byte("I"))
	f.Add(append(auth(authOK, ""), ready...))
	f.Add(append(auth(authSASL, "SCRAM-SHA-256\x00\x00"), auth(authSASLContinue, "r=x,s=AAAAAAAAAAAAAAAAAAAAAA==,i=4096")...))
	f.Add(append(auth(authSASL, "SCRAM-SHA-256\x00\x00"), ready...))
	f.Add(append(auth(authSASLFinal, "v=AAAA"), ready...))
	f.Add(append(msg(pgwire.BackendParameterStatus, []byte("a\x00b\x00")), ready...))
	f.Add(auth(authMD5Password, "salt"))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, stream []byte) {
		c := NewConn(fuzzPeer{bytes.NewReader(stream)})
		err := c.authenticate(StartupConfig{User: "u", Password: "pw"}, nil)
		if err == nil {
			// Success ends at a ReadyForQuery and leaves a usable connection.
			if c.Err() != nil || bytes.IndexByte(stream, pgwire.BackendReadyForQuery) < 0 {
				t.Fatalf("authenticate succeeded on %x without a ReadyForQuery", stream)
			}
			return
		}
		for _, want := range []error{ErrConnBroken, ErrAuth, ErrAuthUnsupported, ErrProtocol} {
			if errors.Is(err, want) {
				return
			}
		}
		var pgErr *Error
		if errors.As(err, &pgErr) {
			return
		}
		t.Fatalf("unclassified error %v for %x", err, stream)
	})
}
