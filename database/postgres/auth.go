package postgres

import (
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"

	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// ErrAuth reports an authentication exchange this package refused to complete.
//
// It is distinct from the [*Error] a server returns when it rejects a
// password: that is the server saying no, and it carries SQLSTATE 28P01. This
// one is the client saying no — a server signature that does not verify, a
// nonce that does not extend the one we sent, an iteration count outside the
// range a client should accept. Those are the checks that make the exchange
// mutual, and failing them loudly is the whole point of running it.
var ErrAuth = errors.New("postgres: the authentication exchange failed")

// ErrAuthUnsupported reports an authentication method this package does not
// implement. The error names the method, because "authentication failed" with
// no method named is the least actionable message a connector can produce.
var ErrAuthUnsupported = errors.New("postgres: unsupported authentication method")

// Authentication request codes, from the AuthenticationRequest message family.
// They all arrive as type 'R' with the code in the first four bytes, which is
// why they are one switch rather than one message type each.
const (
	authOK                = 0
	authKerberosV5        = 2
	authCleartextPassword = 3
	authMD5Password       = 5
	authGSS               = 7
	authGSSContinue       = 8
	authSSPI              = 9
	authSASL              = 10
	authSASLContinue      = 11
	authSASLFinal         = 12
)

// The SASL mechanisms this package speaks. The -PLUS one binds the exchange
// to the TLS connection it runs in, and is used only when there is one.
const (
	mechSCRAMSHA256     = "SCRAM-SHA-256"
	mechSCRAMSHA256Plus = "SCRAM-SHA-256-PLUS"
)

// The gs2 headers, in RFC 5802's channel-binding vocabulary.
//
// The three values are not interchangeable and the distinction is the
// downgrade protection: 'n' means "I cannot do channel binding", 'y' means "I
// can, but I believe you cannot", and 'p' requires it. Over a plain socket
// there is nothing to bind to, so 'n' is the truth; over TLS 'y' is, when the
// server did not offer -PLUS — and a server that can bind refuses a 'y',
// which is how a stripped mechanism list is caught.
const (
	gs2Header        = "n,,"
	gs2HeaderCapable = "y,,"
	gs2HeaderPlus    = "p=tls-server-end-point,,"
)

// tlsStater is what channel binding needs from a transport: *tls.Conn
// satisfies it, and so does a wrapper that forwards the method.
type tlsStater interface {
	ConnectionState() tls.ConnectionState
}

// startupVersion is protocol 3.0, packed as the major/minor pair the startup
// packet carries.
const startupVersion = 3 << 16

// maxStartupMessage bounds what a server may send before it has proved
// anything.
//
// [pgwire.DefaultMaxMessage] is sized for the rows and COPY chunks a real workload
// moves, and it is the right bound once a connection is established. It is the
// wrong one for the handshake: every message of that phase is a few hundred
// bytes, nothing has authenticated yet, and a peer that merely answered the
// socket could otherwise make each connection attempt cost 64 MiB with four
// bytes of length prefix. A pool opening a hundred connections would spend six
// gigabytes on a server it had not yet verified.
//
// 64 KiB is two orders of magnitude above the largest message this phase
// legitimately carries. The limit is raised to [pgwire.DefaultMaxMessage] once the
// server is ready, so it costs a real workload nothing.
const maxStartupMessage = 64 << 10

// StartupConfig is what the server needs to know before it will answer a
// query. Only User has no sensible default.
type StartupConfig struct {
	// User is the role to connect as. Required: the protocol has no default
	// for it, and a server given none refuses the connection.
	User string

	// Database is the database to connect to. Empty means the server's own
	// default, which is a database named after User.
	Database string

	// Password is used when the server asks for one. It is unused against a
	// server configured for trust, and a server that asks for a password
	// while this is empty fails immediately rather than after a round trip.
	//
	// It is a string rather than a []byte because that is the shape it
	// arrives in from every configuration source, and a []byte that a caller
	// builds from a string has already put the secret in memory twice. The
	// consequence is that it cannot be zeroed after use; a deployment that
	// needs that guarantee should not be passing credentials to a library at
	// all.
	Password string

	// AllowCleartextPassword permits answering an
	// AuthenticationCleartextPassword request. The zero value refuses it,
	// and the refusal is the security property: a client that answers
	// whatever method the peer asks for hands the password to an active
	// MITM, who simply asks for cleartext before SCRAM ever starts — at
	// which point every downgrade protection in the SCRAM exchange is moot,
	// because the credential already crossed the wire.
	//
	// Set it only when both halves hold: the server genuinely authenticates
	// with a cleartext method (`password`, or the LDAP/RADIUS/PAM methods
	// that need the literal password), and the transport is one a credential
	// may cross — a *tls.Conn whose certificate was verified, or a local
	// UNIX socket. This package cannot check either for you; the flag is the
	// caller signing off on both.
	AllowCleartextPassword bool

	// Params sets additional runtime parameters in the startup packet —
	// application_name, search_path, statement_timeout and the rest. It may
	// not contain "user" or "database", which have fields of their own, and
	// setting "client_encoding" overrides the UTF8 this package sends by
	// default. Without that override, [Startup] fails if the server reports
	// any other encoding, since every text decoder here reads UTF-8.
	//
	// The keys are written in sorted order, so one configuration always
	// produces one packet.
	Params map[string]string
}

// Startup performs the startup and authentication exchange over an established
// connection and returns a [Conn] ready for queries.
//
//	nc, err := net.Dial("tcp", "db.internal:5432")
//	// ... handle err, set a deadline: the transport stays the caller's
//	conn, err := postgres.Startup(nc, postgres.StartupConfig{
//	    User:     "orders",
//	    Database: "orders",
//	    Password: os.Getenv("PGPASSWORD"),
//	})
//
// # What it does not do
//
// It does not dial and it does not negotiate TLS. The transport is the
// caller's: which host, which timeout, which certificate pool and whether the
// connection is encrypted at all are deployment decisions, and a library that
// took them would be guessing at the one part of the setup it cannot see.
// Pass a *tls.Conn and everything below runs inside it unchanged.
//
// It also sets no deadline of its own. This package takes its deadlines from
// the connection it is given, the same way [Conn.Query] does, so a handshake
// against an unresponsive server is bounded by the caller's SetDeadline and
// not by a timeout invented here.
//
// # Which methods work
//
// SCRAM-SHA-256 (the default on every modern PostgreSQL) and its -PLUS form
// over TLS, trust, and — only when [StartupConfig.AllowCleartextPassword]
// says so — cleartext password.
// The server's final signature is verified, which is what makes the
// exchange mutual — a client that skips that check has authenticated itself to
// whoever answered the socket.
//
// md5 is refused rather than implemented, and the error says so along with the
// server-side fix.
//
// # Channel binding
//
// When rw is a *tls.Conn — or anything with its ConnectionState method — and
// the server offers SCRAM-SHA-256-PLUS, the exchange is bound to the server's
// certificate with tls-server-end-point (RFC 5929): a man in the middle that
// terminates TLS with a certificate of its own cannot relay the proof. Over
// TLS without -PLUS on offer, the client says it could have bound, so a
// server whose -PLUS was stripped refuses. Over a plain socket binding is
// declined explicitly, and a server offering only -PLUS is refused rather
// than downgraded.
//
// # On failure
//
// The connection is closed. A failed startup leaves the socket at an unknown
// position in the protocol, and since no [Conn] is returned there is nothing
// the caller could close it through.
func Startup(rw io.ReadWriteCloser, cfg StartupConfig) (*Conn, error) {
	body, err := startupBody(cfg)
	if err != nil {
		_ = rw.Close()
		return nil, err
	}

	c := NewConn(rw)
	c.r.SetMaxMessage(maxStartupMessage)
	c.w.Startup(body)
	if err := c.w.Flush(); err != nil {
		_ = rw.Close()
		return nil, c.breakConn(fmt.Errorf("sending the startup packet: %w", err))
	}
	ts, _ := rw.(tlsStater)
	if err := c.authenticate(cfg, ts); err != nil {
		_ = rw.Close()
		return nil, err
	}
	// The handshake is over and the server has proved itself, so the ordinary
	// bound applies from here.
	c.r.SetMaxMessage(pgwire.DefaultMaxMessage)
	return c, nil
}

// startupBody builds the startup packet's payload: the protocol version, then
// key/value pairs, then a terminating zero byte.
//
// Everything the caller supplied is judged before anything is written, so a
// configuration this refuses costs no bytes on the wire and leaves the server
// with nothing to half-read.
func startupBody(cfg StartupConfig) ([]byte, error) {
	if cfg.User == "" {
		return nil, errors.New("postgres: StartupConfig.User is empty, and the protocol has no default for it")
	}
	if _, ok := cfg.Params["user"]; ok {
		return nil, errors.New(`postgres: set the role in StartupConfig.User, not in Params["user"]`)
	}
	if _, ok := cfg.Params["database"]; ok {
		return nil, errors.New(`postgres: set the database in StartupConfig.Database, not in Params["database"]`)
	}

	// Sorted, not map order: ranging a map here would make the same
	// configuration produce different bytes on different runs, which is a
	// packet no test can assert and no capture can be compared against.
	keys := slices.Sorted(maps.Keys(cfg.Params))

	// The password does not travel in this packet, but this is the one place
	// the configuration is judged, and a NUL in it is the same defect: sent as
	// a C string for cleartext authentication it would truncate, and the
	// client would authenticate with a prefix of the password it was given
	// while believing it sent the whole thing.
	if err := checkNoNUL("the password", cfg.Password); err != nil {
		return nil, err
	}
	if err := checkNoNUL("user", cfg.User); err != nil {
		return nil, err
	}
	if err := checkNoNUL("database", cfg.Database); err != nil {
		return nil, err
	}
	for _, k := range keys {
		if err := checkNoNUL("a parameter name", k); err != nil {
			return nil, err
		}
		if err := checkNoNUL(k, cfg.Params[k]); err != nil {
			return nil, err
		}
	}

	b := binary.BigEndian.AppendUint32(nil, startupVersion)
	b = appendParam(b, "user", cfg.User)
	if cfg.Database != "" {
		// An empty Database means the server's default, which is a database
		// named after the role. Sending the key with an empty value would
		// instead ask for a database whose name is the empty string.
		b = appendParam(b, "database", cfg.Database)
	}
	if _, ok := cfg.Params["client_encoding"]; !ok {
		// Every text value this package decodes is read as UTF-8, so the
		// encoding is a property of the codecs rather than a preference. A
		// caller who overrides it is on their own for what comes back.
		b = appendParam(b, "client_encoding", "UTF8")
	}
	for _, k := range keys {
		b = appendParam(b, k, cfg.Params[k])
	}
	return append(b, 0), nil
}

// checkNoNUL refuses a startup string that carries a NUL.
//
// The check is not pedantry. These strings routinely come from configuration
// or from a user record, they are framed by their terminator, and a NUL in the
// middle of one truncates it and promotes everything after it to a parameter
// of its own — a role named "app\x00options\x00-c search_path=evil" would
// otherwise arrive as a role plus an option the caller never wrote.
func checkNoNUL(what, s string) error {
	if strings.IndexByte(s, 0) >= 0 {
		return fmt.Errorf("postgres: %s contains a NUL byte, which would truncate it and turn what follows into parameters of their own", what)
	}
	return nil
}

// checkSQLText refuses statement text that carries a NUL.
//
// Query and Parse frame the SQL as a C string, so a NUL ends it early and the
// server reads the rest of the message as whatever fields follow — answered
// with "invalid message format" or a syntax error at a position the caller
// never wrote. Refused here, where the error can say what is wrong.
func checkSQLText(sql string) error {
	if strings.IndexByte(sql, 0) >= 0 {
		return errors.New("postgres: the SQL text contains a NUL byte, which the protocol cannot carry: it would end the statement early")
	}
	return nil
}

// appendParam appends one key/value pair as the two C strings the packet wants.
// Both are already known to be free of NULs.
func appendParam(dst []byte, key, val string) []byte {
	dst = append(append(dst, key...), 0)
	return append(append(dst, val...), 0)
}

// authenticate drives the exchange from the startup packet to ReadyForQuery.
func (c *Conn) authenticate(cfg StartupConfig, ts tlsStater) error {
	var sc scram
	for {
		m, err := c.r.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// A server that closes here has usually refused the
				// connection at a layer below the protocol — pg_hba, or TLS
				// required on a plaintext socket — and says nothing.
				err = io.ErrUnexpectedEOF
			}
			return c.breakConn(fmt.Errorf("during startup: %w", err))
		}

		switch m.Type {
		case pgwire.BackendAuthentication:
			code, data, err := parseAuthentication(m.Body)
			if err != nil {
				return c.breakConn(err)
			}
			if err := c.authStep(&sc, code, data, cfg, ts); err != nil {
				return err
			}

		case pgwire.BackendParameterStatus:
			if err := c.recordParameter(m.Body); err != nil {
				return c.breakConn(err)
			}

		case pgwire.BackendBackendKeyData:
			// The cancellation key, kept for [CancelRequest]. It is a secret
			// and it stays one: [Conn.CancelKey] is the only way out of here,
			// and [CancelKey.String] refuses to print it.
			key, err := parseBackendKeyData(m.Body)
			if err != nil {
				return c.breakConn(err)
			}
			c.cancelKey = key

		case pgwire.BackendNoticeResponse:
			// Out of band, and may arrive at any point.

		case pgwire.BackendErrorResponse:
			serverErr, parseErr := ParseError(m.Body)
			if parseErr != nil {
				return c.breakConn(parseErr)
			}
			// Unlike a query error, this one is terminal: there is no
			// ReadyForQuery coming and no position to resynchronise to.
			return c.breakConn(serverErr)

		case pgwire.BackendReadyForQuery:
			// Exactly one status byte. Elsewhere a malformed body is
			// tolerated because the caller is recovering a connection; here
			// it is the server's first word that the session is usable, and a
			// server that cannot frame it is not one to hand queries to.
			if len(m.Body) != 1 || (m.Body[0] != 'I' && m.Body[0] != 'T' && m.Body[0] != 'E') {
				return c.breakConn(fmt.Errorf("%w: ReadyForQuery with a %d-byte body %q during startup, want one status byte", ErrProtocol, len(m.Body), m.Body))
			}
			c.noteTxStatus(m.Body)
			// The single exit, and therefore the single place the mutual half
			// of the exchange is enforced. A server that ran a SCRAM exchange
			// and then skipped its final message would otherwise have proved
			// nothing about knowing the password, and this client would have
			// handed one over for it.
			if sc.started && !sc.verified {
				return c.breakConn(fmt.Errorf("%w: the server declared the connection ready without completing the SCRAM exchange it started", ErrAuth))
			}
			// UTF8 was asked for, not granted: a pooler or a server-side
			// default can answer with another encoding, and every text
			// decoder here would then read its bytes as UTF-8 without error.
			// A caller who set client_encoding themselves chose that.
			if _, chosen := cfg.Params["client_encoding"]; !chosen {
				if enc, reported := c.params["client_encoding"]; reported && !strings.EqualFold(enc, "UTF8") {
					return c.breakConn(fmt.Errorf("postgres: the server reports client_encoding %q after UTF8 was requested, and this package decodes text as UTF-8", enc))
				}
			}
			return nil

		default:
			return c.breakConn(fmt.Errorf("%w: unexpected message type %q during startup", ErrProtocol, m.Type))
		}
	}
}

// authStep answers one AuthenticationRequest.
func (c *Conn) authStep(sc *scram, code uint32, data []byte, cfg StartupConfig, ts tlsStater) error {
	switch code {
	case authOK:
		return nil

	case authCleartextPassword:
		// Refused before the password is even considered. The server does
		// not prove anything by asking, so "the server asked" cannot be the
		// authorisation to send a credential in the clear — on an
		// unencrypted transport anyone on the path can ask the same question
		// and keep the answer. The caller who knows the method and the
		// transport are both sound says so with the flag.
		if !cfg.AllowCleartextPassword {
			return c.breakConn(fmt.Errorf("%w: the server asked for a cleartext password and StartupConfig.AllowCleartextPassword is false — set it only over a transport the credential may cross, such as a verified *tls.Conn", ErrAuth))
		}
		if cfg.Password == "" {
			return c.breakConn(fmt.Errorf("%w: the server asked for a password and StartupConfig.Password is empty", ErrAuth))
		}
		// The password goes out as the server asked for it: in the clear.
		// Whether that is acceptable is the transport's business, and the
		// transport is the caller's — over a *tls.Conn it is fine, over a
		// plain socket it is a credential on the wire.
		c.w.PasswordMessage(cfg.Password)
		return c.flushAuth("sending the password")

	case authSASL:
		// Read here rather than at Startup's entry: a *tls.Conn shakes hands
		// on its first write, so only now is the certificate known.
		var cs *tls.ConnectionState
		if ts != nil {
			state := ts.ConnectionState()
			cs = &state
		}
		mech, msg, err := sc.begin(data, cfg.Password, cs)
		if err != nil {
			return c.breakConn(err)
		}
		c.w.SASLInitialResponse(mech, msg)
		return c.flushAuth("starting the SCRAM exchange")

	case authSASLContinue:
		msg, err := sc.respond(data, cfg.Password)
		if err != nil {
			return c.breakConn(err)
		}
		c.w.SASLResponse(msg)
		return c.flushAuth("answering the SCRAM challenge")

	case authSASLFinal:
		if err := sc.verify(data); err != nil {
			return c.breakConn(err)
		}
		return nil

	case authMD5Password:
		// Refused rather than implemented. md5 authentication stores a
		// password equivalent on the server: whoever reads pg_authid can
		// authenticate as that role without ever learning the password, so
		// the hash is the credential. PostgreSQL has deprecated it, and a
		// connector that offers it invites a server to keep using it.
		return c.breakConn(fmt.Errorf("%w: md5, which this package declines. Set password_encryption = scram-sha-256 on the server and re-set the role's password with \\password so it is stored for SCRAM", ErrAuthUnsupported))

	case authKerberosV5, authGSS, authGSSContinue, authSSPI:
		return c.breakConn(fmt.Errorf("%w: GSSAPI/SSPI (request %d)", ErrAuthUnsupported, code))

	default:
		return c.breakConn(fmt.Errorf("%w: authentication request %d", ErrAuthUnsupported, code))
	}
}

// flushAuth sends what authStep queued, naming the step in any failure.
func (c *Conn) flushAuth(what string) error {
	if err := c.w.Flush(); err != nil {
		return c.breakConn(fmt.Errorf("%s: %w", what, err))
	}
	return nil
}

// parseAuthentication splits an Authentication message into its code and the
// method-specific bytes that follow.
func parseAuthentication(body []byte) (code uint32, data []byte, err error) {
	if len(body) < 4 {
		return 0, nil, fmt.Errorf("%w: Authentication message shorter than its code", ErrProtocol)
	}
	return binary.BigEndian.Uint32(body[:4]), body[4:], nil
}

// MaxServerParameters bounds how many distinct runtime parameters a Conn will
// remember.
//
// The server chooses both the names and how many it sends, and it sends them
// before it has proved anything — so an unbounded map here is memory a peer
// that answered the socket can grow for free, per connection. PostgreSQL
// reports about a dozen; this is two orders of magnitude of headroom, and
// reaching it means the far end is not a PostgreSQL.
const MaxServerParameters = 1024

// MaxServerParameterBytes bounds one remembered parameter: its name plus its
// value.
//
// Capping the count alone is not enough. After authentication the message
// ceiling is [pgwire.DefaultMaxMessage], so a single ParameterStatus could otherwise
// carry a value of nearly 64 MiB into the retained map — and a thousand of
// those is gigabytes a hostile server grows for free, kept for the life of
// the connection. Real values are a timezone or a version string, tens of
// bytes; 16 KiB is generous headroom, and together with
// [MaxServerParameters] it bounds the whole map at 16 MiB.
const MaxServerParameterBytes = 16 << 10

// recordParameter files a ParameterStatus under its name.
//
// The protocol allows one at any point in a session, not only during startup:
// a SET of a reportable parameter produces one, and so does a function that
// changes one mid-statement. Every read loop in this package therefore records
// them, because the alternative is [Conn.Parameter] quietly reporting what was
// true when the connection opened.
func (c *Conn) recordParameter(body []byte) error {
	name, val, err := parseParameterStatus(body)
	if err != nil {
		return err
	}
	if len(name)+len(val) > MaxServerParameterBytes {
		// The name is not quoted: it is part of what is oversized.
		return fmt.Errorf("%w: a ParameterStatus carries %d bytes of name and value, over the %d-byte limit", ErrProtocol, len(name)+len(val), MaxServerParameterBytes)
	}
	if c.params == nil {
		c.params = make(map[string]string)
	}
	if _, known := c.params[name]; !known && len(c.params) >= MaxServerParameters {
		return fmt.Errorf("%w: the server announced more than %d distinct runtime parameters", ErrProtocol, MaxServerParameters)
	}
	c.params[name] = val
	return nil
}

// parseParameterStatus splits a ParameterStatus message into its two C strings.
func parseParameterStatus(body []byte) (name, val string, err error) {
	name, rest, ok := cstring(body)
	if !ok {
		return "", "", fmt.Errorf("%w: unterminated name in ParameterStatus", ErrProtocol)
	}
	val, _, ok = cstring(rest)
	if !ok {
		return "", "", fmt.Errorf("%w: unterminated value in ParameterStatus", ErrProtocol)
	}
	return name, val, nil
}
