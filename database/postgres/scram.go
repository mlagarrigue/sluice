package postgres

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"hash"
	"slices"
	"strconv"
	"strings"
)

// SCRAM-SHA-256, as RFC 5802 defines the family and RFC 7677 pins the SHA-256
// member of it. PostgreSQL layers it into the protocol as three messages —
// AuthenticationSASL, SASLContinue, SASLFinal — but the exchange itself is the
// RFC's, and the values this package computes are pinned against the RFC's own
// test vector rather than against its own decoder (see scram_test.go).
//
// The property that makes it worth the round trips: the password never leaves
// the client, in any form, and the server proves it knew the password too. The
// second half is not decoration. A client that computes a proof and skips the
// server's signature has authenticated itself to whatever answered the socket,
// which against a redirected connection is exactly backwards.

// Iteration bounds this client will accept from a server.
//
// The floor is RFC 7677's recommendation, and it is PostgreSQL's own default:
// a server declaring a low count would weaken the derivation for a client that
// simply believed it, and believing it is not required. The ceiling is a
// denial-of-service bound — the iteration count is a number the *server*
// chooses and the *client* burns CPU on, so an unbounded one is a stall a
// hostile or misconfigured server can impose for free. A million iterations is
// already far above any real deployment.
const (
	minIterations = 4096
	maxIterations = 1 << 20
)

// minSaltLen is the shortest salt this client will derive against.
//
// It is the same argument as the iteration floor, applied to the other input
// the server chooses: a short salt shrinks the space a precomputed table has
// to cover, and a client that simply used whatever it was handed would inherit
// that. Sixteen bytes is PostgreSQL's own default, and it is also the floor
// crypto/pbkdf2 enforces under GOFIPS140=only — so anything below it would
// fail there regardless, and failing here says why.
const minSaltLen = 16

// nonceBytes is the entropy behind a client nonce, before base64. It is what
// makes a replayed server-first message useless, and 18 bytes is what libpq
// uses.
const nonceBytes = 18

// randRead is the seam the tests replace to make an exchange reproducible.
// Production never assigns it.
var randRead = rand.Read

// scram holds one exchange's state. It lives for the duration of a startup and
// is then discarded — nothing here belongs on a Conn, and a password-derived
// key kept past its use is a key that can leak.
type scram struct {
	started  bool
	verified bool

	// nonce is this client's half of the nonce.
	nonce string

	// auth is the AuthMessage: client-first-bare, server-first, and
	// client-final-without-proof, joined by commas. It is accumulated as a
	// string rather than kept as slices of the reader's buffer, because that
	// buffer is overwritten by the next message and the last step needs the
	// whole thing again to check the server's signature.
	auth string

	// sig is the server signature this client will require to be shown.
	sig []byte

	// gs2 is the header this exchange declared, and cbind the channel-binding
	// data that follows it in c= — empty unless the exchange is -PLUS.
	gs2   string
	cbind []byte
}

// begin answers AuthenticationSASL with the client's first message, and
// names the mechanism it chose.
//
// cs is the TLS state of the transport, or nil when it is not TLS. With it,
// SCRAM-SHA-256-PLUS is preferred whenever the server offers it: the proof is
// then bound to the server's certificate, so a man in the middle holding a
// different certificate cannot relay the exchange even if the client was
// persuaded to trust it.
func (s *scram) begin(mechanisms []byte, password string, cs *tls.ConnectionState) (mech string, first []byte, err error) {
	if password == "" {
		return "", nil, fmt.Errorf("%w: the server asked for SCRAM and StartupConfig.Password is empty", ErrAuth)
	}
	names, err := parseMechanisms(mechanisms)
	if err != nil {
		return "", nil, err
	}
	if cs != nil && (!cs.HandshakeComplete || len(cs.PeerCertificates) == 0) {
		cs = nil // no certificate to bind to: as good as no TLS here
	}
	offersPlus := slices.Contains(names, mechSCRAMSHA256Plus)
	mech = mechSCRAMSHA256
	switch {
	case cs != nil && offersPlus:
		cbind, err := tlsServerEndPoint(cs.PeerCertificates[0])
		if err != nil {
			// Refused rather than downgraded: the server offered binding over
			// this connection, and a server that can bind and a client that
			// will not is the shape a stripped binding takes.
			return "", nil, err
		}
		mech, s.gs2, s.cbind = mechSCRAMSHA256Plus, gs2HeaderPlus, cbind
	case !slices.Contains(names, mechSCRAMSHA256):
		if offersPlus {
			return "", nil, fmt.Errorf("%w: the server offers only %s, which binds the exchange to a TLS connection and this one is not TLS. It will not be downgraded silently, because a stripped mechanism list is what channel binding exists to detect",
				ErrAuthUnsupported, mechSCRAMSHA256Plus)
		}
		return "", nil, fmt.Errorf("%w: the server offers %v, none of which this package speaks", ErrAuthUnsupported, names)
	case cs != nil:
		// TLS, but the server did not offer -PLUS. 'y' says this client could
		// have bound the exchange: a server that does support binding — and
		// whose -PLUS was stripped on the way — sees the claim and refuses.
		s.gs2 = gs2HeaderCapable
	default:
		s.gs2 = gs2Header
	}

	s.nonce = newNonce()
	// The username field is sent empty. PostgreSQL takes the role from the
	// startup packet and ignores this one, and filling it would mean applying
	// SASLprep and the RFC's comma/equals escaping to a value that is
	// discarded — three ways to be wrong for no effect. libpq sends it empty
	// for the same reason.
	clientFirstBare := "n=,r=" + s.nonce
	s.auth = clientFirstBare
	s.started = true
	return mech, []byte(s.gs2 + clientFirstBare), nil
}

// tlsServerEndPoint computes RFC 5929 §4.1's tls-server-end-point binding:
// the hash of the server's certificate, with the hash its signature
// algorithm uses — SHA-256 where that is MD5 or SHA-1. An algorithm with no
// such hash (Ed25519) has no defined binding, and PostgreSQL refuses it too.
func tlsServerEndPoint(cert *x509.Certificate) ([]byte, error) {
	var h hash.Hash
	switch cert.SignatureAlgorithm {
	case x509.MD5WithRSA, x509.SHA1WithRSA, x509.ECDSAWithSHA1, x509.DSAWithSHA1,
		x509.SHA256WithRSA, x509.SHA256WithRSAPSS, x509.ECDSAWithSHA256, x509.DSAWithSHA256:
		h = sha256.New()
	case x509.SHA384WithRSA, x509.SHA384WithRSAPSS, x509.ECDSAWithSHA384:
		h = sha512.New384()
	case x509.SHA512WithRSA, x509.SHA512WithRSAPSS, x509.ECDSAWithSHA512:
		h = sha512.New()
	default:
		return nil, fmt.Errorf("%w: the server offers %s, and its certificate's signature algorithm %v defines no tls-server-end-point hash",
			ErrAuthUnsupported, mechSCRAMSHA256Plus, cert.SignatureAlgorithm)
	}
	h.Write(cert.Raw)
	return h.Sum(nil), nil
}

// respond answers AuthenticationSASLContinue with the client's final message,
// and records the server signature that must come back.
func (s *scram) respond(serverFirst []byte, password string) ([]byte, error) {
	if !s.started {
		return nil, fmt.Errorf("%w: a SASLContinue arrived before any SASL exchange was started", ErrProtocol)
	}
	msg := string(serverFirst) // copied: the reader's buffer is about to be reused
	nonce, salt, iter, err := parseServerFirst(msg, s.nonce)
	if err != nil {
		return nil, err
	}

	// c= is the gs2 header echoed back, base64-encoded, so that a server can
	// see whether the channel-binding declaration it received is the one the
	// client sent. That is the check a downgrade would have to survive. Under
	// -PLUS the certificate hash follows the header, and the server compares
	// it with its own: a relaying middlebox has a different certificate.
	clientFinalBare := "c=" + base64.StdEncoding.EncodeToString(append([]byte(s.gs2), s.cbind...)) + ",r=" + nonce
	s.auth += "," + msg + "," + clientFinalBare

	proof, serverSig, err := scramProof(password, salt, iter, s.auth)
	if err != nil {
		// Unreachable from here: parseServerFirst has already judged the salt
		// and the iteration count, which are the only things scramProof
		// objects to. Kept because the alternative is discarding an error,
		// and because the function's contract is its own.
		return nil, err
	}
	s.sig = serverSig
	return []byte(clientFinalBare + ",p=" + base64.StdEncoding.EncodeToString(proof)), nil
}

// verify checks AuthenticationSASLFinal against the signature respond computed.
func (s *scram) verify(serverFinal []byte) error {
	if s.sig == nil {
		return fmt.Errorf("%w: a SASLFinal arrived before the challenge was answered", ErrProtocol)
	}
	got, err := parseServerFinal(string(serverFinal))
	if err != nil {
		return err
	}
	// Constant time, because a comparison that returns early leaks how many
	// leading bytes were right, and a signature that can be guessed a byte at
	// a time is a signature that proves nothing.
	if subtle.ConstantTimeCompare(got, s.sig) != 1 {
		return fmt.Errorf("%w: the server's signature does not verify. It does not know this role's password, whatever else it may be", ErrAuth)
	}
	s.verified = true
	return nil
}

// scramProof derives the client proof and the server signature from the
// password and the AuthMessage, per RFC 5802 §3.
//
// It takes the AuthMessage whole rather than building it, which is what lets
// the RFC's published test vector drive it directly — the vector's messages
// are not the ones this client sends, and a function that assembled them
// itself could only ever be tested against its own assembly.
func scramProof(password string, salt []byte, iter int, authMessage string) (proof, serverSig []byte, err error) {
	// SASLprep (RFC 4013) is not applied. It requires Unicode NFKC
	// normalisation, which the standard library does not carry and which this
	// repository will not take a dependency for. For a password of printable
	// ASCII — every one this will meet in practice — SASLprep is the identity
	// function, so the two agree exactly. For a password containing
	// non-ASCII characters, a server that stored it normalised will compute a
	// different SaltedPassword and authentication will fail: a clear failure,
	// not a weakened one. libpq falls back to the same raw bytes when its
	// SASLprep cannot run.
	if len(salt) < minSaltLen {
		// Reached only by a caller other than the exchange, which has already
		// judged the salt. It is checked again rather than assumed, because
		// this function is what a future codepath will reuse.
		return nil, nil, fmt.Errorf("%w: a %d-byte salt is below the %d this client accepts", ErrAuth, len(salt), minSaltLen)
	}
	salted, err := pbkdf2.Key(sha256.New, password, salt, iter, sha256.Size)
	if err != nil {
		// crypto/pbkdf2 rejects the arguments only under GOFIPS140=only, and
		// the two things it objects to there — a short salt and an
		// unapproved hash — are fixed above and by construction. The branch
		// stays because a silent nil key would be catastrophic and the cost
		// of keeping it is one line.
		return nil, nil, fmt.Errorf("%w: deriving the salted password: %w", ErrAuth, err)
	}

	clientKey := hmacSHA256(salted, "Client Key")
	storedKey := sha256.Sum256(clientKey)
	clientSig := hmacSHA256(storedKey[:], authMessage)

	// The proof is the key masked by the signature: the server, holding only
	// StoredKey, recovers ClientKey from it, hashes that, and compares. This
	// is why the server can verify a client it cannot impersonate.
	proof = clientKey
	for i := range proof {
		proof[i] ^= clientSig[i]
	}

	serverKey := hmacSHA256(salted, "Server Key")
	return proof, hmacSHA256(serverKey, authMessage), nil
}

func hmacSHA256(key []byte, msg string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(msg))
	return h.Sum(nil)
}

// newNonce returns a fresh client nonce, base64-encoded so that it carries no
// comma and cannot be mistaken for an attribute separator.
func newNonce() string {
	b := make([]byte, nonceBytes)
	// crypto/rand.Read is documented never to return an error: it fills the
	// buffer or the program dies. Handling a failure that cannot be reported
	// would be a branch no test can reach.
	_, _ = randRead(b)
	return base64.StdEncoding.EncodeToString(b)
}

// parseMechanisms reads the list of C strings in an AuthenticationSASL body,
// which ends at an empty one.
func parseMechanisms(body []byte) ([]string, error) {
	var names []string
	for {
		name, rest, ok := cstring(body)
		if !ok {
			return nil, fmt.Errorf("%w: unterminated mechanism name in AuthenticationSASL", ErrProtocol)
		}
		if name == "" {
			return names, nil
		}
		names = append(names, name)
		body = rest
	}
}

// parseServerFirst reads r=, s= and i= out of the server's first message and
// judges them.
//
// Every check here is a downgrade this client refuses, which is the reason to
// parse the message rather than pick the fields out of it.
func parseServerFirst(msg, clientNonce string) (nonce string, salt []byte, iter int, err error) {
	var haveNonce, haveSalt, haveIter bool
	for attr := range strings.SplitSeq(msg, ",") {
		name, val, ok := splitAttr(attr)
		if !ok {
			return "", nil, 0, fmt.Errorf("%w: malformed attribute %q in the server's first message", ErrAuth, attr)
		}
		switch name {
		case 'r':
			nonce, haveNonce = val, true
		case 's':
			if salt, err = base64.StdEncoding.DecodeString(val); err != nil {
				return "", nil, 0, fmt.Errorf("%w: the salt is not base64: %w", ErrAuth, err)
			}
			haveSalt = true
		case 'i':
			if iter, err = strconv.Atoi(val); err != nil {
				return "", nil, 0, fmt.Errorf("%w: the iteration count is not a number: %w", ErrAuth, err)
			}
			haveIter = true
		case 'm':
			// RFC 5802: a mandatory extension the client does not recognise
			// must abort the exchange. Skipping it would be agreeing to a
			// term this client did not read.
			return "", nil, 0, fmt.Errorf("%w: the server requires an extension this package does not implement (m=%s)", ErrAuth, val)
		default:
			// Optional attributes are reserved by the RFC for extension, and
			// refusing an unknown one would turn a server upgrade into an
			// outage.
		}
	}
	switch {
	case !haveNonce || !haveSalt || !haveIter:
		return "", nil, 0, fmt.Errorf("%w: the server's first message is missing r=, s= or i=", ErrAuth)
	case !strings.HasPrefix(nonce, clientNonce) || len(nonce) == len(clientNonce):
		// The server must extend the nonce this client sent, not replace it
		// and not echo it. This is the check that ties the answer to this
		// exchange: without it a recorded server-first message replays.
		return "", nil, 0, fmt.Errorf("%w: the server's nonce does not extend the client's", ErrAuth)
	case iter < minIterations:
		return "", nil, 0, fmt.Errorf("%w: the server asked for %d PBKDF2 iterations, below the %d this client accepts", ErrAuth, iter, minIterations)
	case iter > maxIterations:
		return "", nil, 0, fmt.Errorf("%w: the server asked for %d PBKDF2 iterations, above the %d this client accepts", ErrAuth, iter, maxIterations)
	case len(salt) < minSaltLen:
		return "", nil, 0, fmt.Errorf("%w: the server sent a %d-byte salt, below the %d this client accepts", ErrAuth, len(salt), minSaltLen)
	}
	return nonce, salt, iter, nil
}

// parseServerFinal reads the server's signature, or the error it sent instead.
func parseServerFinal(msg string) ([]byte, error) {
	for attr := range strings.SplitSeq(msg, ",") {
		name, val, ok := splitAttr(attr)
		if !ok {
			return nil, fmt.Errorf("%w: malformed attribute %q in the server's final message", ErrAuth, attr)
		}
		switch name {
		case 'v':
			sig, err := base64.StdEncoding.DecodeString(val)
			if err != nil {
				return nil, fmt.Errorf("%w: the server's signature is not base64: %w", ErrAuth, err)
			}
			return sig, nil
		case 'e':
			// The server-error attribute: the exchange failed on its side and
			// it said why, which is more useful than the generic refusal that
			// would otherwise follow.
			return nil, fmt.Errorf("%w: the server rejected the exchange: %s", ErrAuth, val)
		}
	}
	return nil, fmt.Errorf("%w: the server's final message carries no signature", ErrAuth)
}

// splitAttr splits one "x=value" attribute. The name is a single byte, which
// is the RFC's shape and not a shortcut.
func splitAttr(attr string) (name byte, val string, ok bool) {
	if len(attr) < 2 || attr[1] != '=' {
		return 0, "", false
	}
	return attr[0], attr[2:], true
}
