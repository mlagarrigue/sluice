package postgres

import (
	"bytes"
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

// startupRefusal is the server refusing the connection during startup — a
// wrong password, pg_hba, a database that does not exist.
//
// It reads as the server's refusal. Wrapped by [Conn.breakConn] it would read
// "connection is out of sync", which sends whoever sees a 28P01 looking for a
// protocol fault; it still matches [ErrConnBroken] for a caller that tests it,
// and [errors.As] still finds the [*Error].
type startupRefusal struct {
	server *Error
	hint   string
}

func (e *startupRefusal) Error() string   { return e.server.Error() + e.hint }
func (e *startupRefusal) Unwrap() []error { return []error{e.server, ErrConnBroken} }

// refusal builds the error for a server's refusal at startup. A password
// failure after a SCRAM exchange with a non-ASCII password gets the one likely
// cause this package can name: it sends the password without SASLprep (see
// scramProof), and a server that stored it normalised derives another key.
func refusal(server *Error, scram bool, password string) error {
	e := &startupRefusal{server: server}
	if server.Code == "28P01" && scram && !isASCII(password) {
		e.hint = "; the password contains non-ASCII characters, which this package sends without SASLprep (RFC 4013) normalisation: unless the password is already in NFKC form, the server derived its key from a different string"
	}
	return e
}

func isASCII(s string) bool {
	for i := range len(s) {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

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

// protocolMajor is the major version this package speaks, packed in the high
// half of the startup packet's version word; the minor goes in the low half.
const protocolMajor = 3 << 16

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

// maxMaxMessage is the largest [Config.MaxMessage] accepted. PostgreSQL
// cannot build a message over 1 GiB — its allocation ceiling — so a bound
// above that admits nothing a server can send and only widens what a
// corrupted length prefix may ask for.
const maxMaxMessage = 1 << 30

// Config is what the server needs to know before it will answer a
// query. Only User has no sensible default.
type Config struct {
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

	// RequireAuth refuses a server that declares the connection
	// authenticated without having asked for a credential. The zero value
	// accepts it, which is what a server configured for trust does; so does
	// a server that is not the one the caller meant to reach, since
	// AuthenticationOk is one message and proves nothing about who sent it.
	// Set it when the role is meant to authenticate with a password: the
	// absence of the exchange then means a misconfigured pg_hba.conf or a
	// peer standing in for the server, and [Connect] fails with [ErrAuth]
	// rather than hand queries to it.
	RequireAuth bool

	// RequireChannelBinding refuses a connection whose SCRAM exchange was
	// not bound to the server's certificate (SCRAM-SHA-256-PLUS). The zero
	// value binds when the server offers it and goes on without it
	// otherwise, which protects against a stripped mechanism list only
	// through the 'y' flag of RFC 5802 — a check the server makes, not this
	// client. Set, it is this client that refuses: a connection that is not
	// a *tls.Conn, before anything is sent; a server that offers
	// SCRAM-SHA-256 without -PLUS, before the client's first SCRAM message;
	// a server that asks for a cleartext password or asks for nothing at
	// all. Each refusal is [ErrAuth] and names this field. It implies
	// RequireAuth.
	RequireChannelBinding bool

	// Params sets additional runtime parameters in the startup packet —
	// application_name, search_path, statement_timeout and the rest. It may
	// not contain "user" or "database", which have fields of their own, nor
	// "replication": this package does not speak the replication protocol.
	// Setting "client_encoding" overrides the UTF8 this package sends by
	// default. Without that override, the connection breaks whenever the
	// server reports any other encoding — at startup, or after a SET
	// client_encoding mid-session — since every text decoder here reads
	// UTF-8.
	//
	// A key starting with "_pq_." is a protocol option rather than a
	// runtime parameter. A server that does not recognise one says so with
	// NegotiateProtocolVersion and ignores it; the connection proceeds.
	//
	// The keys are written in sorted order, so one configuration always
	// produces one packet.
	Params map[string]string

	// MaxMessage is the largest message accepted from the server once the
	// connection is up, in bytes: a DataRow with all its values, a CopyData
	// chunk. Zero, the default, is 64 MiB — enough for the rows a real
	// workload sends, small enough that a hostile or corrupted length
	// prefix cannot exhaust the process. A workload that reads larger
	// values (PostgreSQL allows up to 1 GiB per field) raises it
	// deliberately: a message over the bound is refused unread, and since
	// the stream position is then lost the connection is broken with it
	// ([ErrMessageTooLarge], then [ErrConnBroken]).
	//
	// [Connect] refuses a value below 64 KiB — the handshake's own bound,
	// which this does not change — or above 1 GiB, before anything is
	// sent. This bounds what is read; [CopyConfig.MaxMessage] is how much
	// is written in one CopyData chunk.
	MaxMessage int

	// ProtocolMinor is the minor protocol version asked for: 0, the default,
	// asks for 3.0, which every server since 7.4 speaks; 2 asks for 3.2,
	// which PostgreSQL 18 introduced. [Connect] refuses any other value
	// before anything is sent — there is no 3.1, and nothing past 3.2 exists
	// yet.
	//
	// What 3.2 changes here is the cancellation key: its secret grows from
	// four bytes to as many as 256 ([CancelKey]), which a server can no
	// longer have guessed. A server older than 18 answers 3.2 with
	// NegotiateProtocolVersion and the connection continues under 3.0;
	// nothing breaks, and the key is the four-byte one.
	ProtocolMinor int
}

// startup performs the startup and authentication exchange over a
// connection already open: [Connect] calls it once it has dialled, and the
// tests call it over a scripted server. It sets no deadline of its own, and it
// closes rw on failure — the socket is then at an unknown position in the
// protocol, and no [Conn] is returned to close it through.
func startup(rw io.ReadWriteCloser, cfg Config) (*Conn, error) {
	body, err := startupBody(cfg)
	if err != nil {
		_ = rw.Close()
		return nil, err
	}

	ts, _ := rw.(tlsStater)
	if cfg.RequireChannelBinding && ts == nil {
		// Judged here rather than in startupBody because it is the transport,
		// not the configuration, that is at fault — and before the startup
		// packet, since no exchange over this connection could satisfy it.
		_ = rw.Close()
		return nil, fmt.Errorf("%w: Config.RequireChannelBinding is set and the connection is not a *tls.Conn — channel binding ties the exchange to the server's certificate, so the DialFunc must return the *tls.Conn itself, not a wrapper around it — DialTLS does", ErrAuth)
	}

	c := NewConn(rw)
	_, c.encodingChosen = cfg.Params["client_encoding"]
	c.r.SetMaxMessage(maxStartupMessage)
	c.w.Startup(body)
	if err := c.w.Flush(); err != nil {
		_ = rw.Close()
		return nil, c.breakConn(fmt.Errorf("sending the startup packet: %w", err))
	}
	if err := c.authenticate(cfg, ts); err != nil {
		_ = rw.Close()
		return nil, err
	}
	// The handshake is over and the server has proved itself, so the ordinary
	// bound applies from here — the caller's when it named one.
	limit := pgwire.DefaultMaxMessage
	if cfg.MaxMessage != 0 {
		limit = cfg.MaxMessage
	}
	c.r.SetMaxMessage(limit)
	return c, nil
}

// startupBody builds the startup packet's payload: the protocol version, then
// key/value pairs, then a terminating zero byte.
//
// Everything the caller supplied is judged before anything is written, so a
// configuration this refuses costs no bytes on the wire and leaves the server
// with nothing to half-read.
func startupBody(cfg Config) ([]byte, error) {
	if cfg.User == "" {
		return nil, errors.New("postgres: Config.User is empty, and the protocol has no default for it")
	}
	if _, ok := cfg.Params["user"]; ok {
		return nil, errors.New(`postgres: set the role in Config.User, not in Params["user"]`)
	}
	if _, ok := cfg.Params["database"]; ok {
		return nil, errors.New(`postgres: set the database in Config.Database, not in Params["database"]`)
	}
	// A walsender accepts only the simple query protocol ("Streaming
	// Replication Protocol" in the protocol chapter): Query and ExecParams
	// speak the extended one and would fail, and START_REPLICATION through
	// Exec answers with a CopyBothResponse no drain here reads — the WAL
	// would be discarded forever. Refused whatever the value, "off" included: the key buys
	// nothing a plain connection lacks.
	if _, ok := cfg.Params["replication"]; ok {
		return nil, errors.New(`postgres: Params["replication"] asks for a replication connection, which this package does not speak`)
	}
	if m := cfg.MaxMessage; m != 0 && (m < maxStartupMessage || m > maxMaxMessage) {
		return nil, fmt.Errorf("postgres: Config.MaxMessage %d is outside %d..%d bytes", m, maxStartupMessage, maxMaxMessage)
	}
	if v := cfg.ProtocolMinor; v != 0 && v != 2 {
		return nil, fmt.Errorf("postgres: Config.ProtocolMinor %d asks for protocol 3.%d — this package speaks 3.0 (0) and 3.2 (2)", v, v)
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

	b := binary.BigEndian.AppendUint32(nil, protocolMajor|uint32(cfg.ProtocolMinor)) //nolint:gosec // G115: 0 or 2, checked above
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

// startupState is what authenticate carries from one server message to the
// next: the SCRAM exchange, and what the server asked for on the way.
type startupState struct {
	scram

	// asked is set once the server asked for a credential — a password or a
	// SCRAM exchange — rather than declaring the connection authenticated
	// outright. It is what Config.RequireAuth requires.
	asked bool

	// bound is set once the SCRAM exchange began as SCRAM-SHA-256-PLUS. It is
	// what Config.RequireChannelBinding requires.
	bound bool

	// minor is the minor protocol version in force: the one asked for, until
	// a NegotiateProtocolVersion lowers it. It decides how long a
	// BackendKeyData may be.
	minor uint32

	// negotiated is set once a NegotiateProtocolVersion has been read, and
	// authenticating once any Authentication message has: the server settles
	// the version from the startup packet, before it authenticates, and says
	// so once.
	negotiated, authenticating bool
}

// authenticate drives the exchange from the startup packet to ReadyForQuery.
func (c *Conn) authenticate(cfg Config, ts tlsStater) error {
	st := startupState{minor: uint32(cfg.ProtocolMinor)} //nolint:gosec // G115: 0 or 2, checked by startupBody
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
		case pgwire.BackendNegotiateProtocolVersion:
			if st.negotiated || st.authenticating {
				return c.breakConn(fmt.Errorf("%w: NegotiateProtocolVersion after the server had already settled the version", ErrProtocol))
			}
			st.negotiated = true
			minor, err := negotiateProtocol(m.Body, st.minor, cfg.Params)
			if err != nil {
				return c.breakConn(err)
			}
			st.minor = minor

		case pgwire.BackendAuthentication:
			st.authenticating = true
			code, data, err := parseAuthentication(m.Body)
			if err != nil {
				return c.breakConn(err)
			}
			if err := c.authStep(&st, code, data, cfg, ts); err != nil {
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
			key, err := parseBackendKeyData(m.Body, st.minor)
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
			c.broken = refusal(serverErr, st.step != scramIdle, cfg.Password)
			return c.broken

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
			if st.step != scramIdle && st.step != scramVerified {
				return c.breakConn(fmt.Errorf("%w: the server declared the connection ready without completing the SCRAM exchange it started", ErrAuth))
			}
			// The same exit judges what the caller required of the exchange.
			// A trust server, or a peer standing in for the server, sends
			// AuthenticationOk and nothing before it; checked here rather
			// than at that message so that a server skipping even that one
			// is caught too.
			if cfg.RequireAuth && !st.asked {
				return c.breakConn(fmt.Errorf("%w: the server declared the connection ready without asking for a credential and Config.RequireAuth is set — pg_hba.conf authenticates this role with trust, or this is not the server it claims to be", ErrAuth))
			}
			if cfg.RequireChannelBinding && !st.bound {
				return c.breakConn(fmt.Errorf("%w: the server declared the connection ready without a SCRAM-SHA-256-PLUS exchange and Config.RequireChannelBinding is set — pg_hba.conf authenticates this role with a method that cannot bind to the certificate (trust, password)", ErrAuth))
			}
			return nil

		default:
			return c.breakConn(fmt.Errorf("%w: unexpected message type %q during startup", ErrProtocol, m.Type))
		}
	}
}

// negotiateProtocol reads a NegotiateProtocolVersion and returns the minor
// version the connection continues under.
//
// The server sends it when it speaks an older minor version than the one
// asked for, or when the startup packet named protocol options — parameters
// starting with "_pq_." — it does not recognise. The second is tolerated: an
// option is a request the server may decline, and the caller who set one in
// [Config.Params] asked for it knowing that. What is refused is a
// message no server sends: a version above the one asked for, one this
// package does not speak, an option nobody asked for, or a message that
// declines nothing at all.
func negotiateProtocol(body []byte, asked uint32, params map[string]string) (uint32, error) {
	if len(body) < 8 {
		return 0, fmt.Errorf("%w: NegotiateProtocolVersion is %d bytes, want at least 8", ErrProtocol, len(body))
	}
	// The documentation calls this field the newest minor version, but the
	// server writes the whole version word, major in the high half, as the
	// startup packet carries it: PostgreSQL 17 answers 3.2 with 0x00030000.
	version := binary.BigEndian.Uint32(body[0:4])
	count := binary.BigEndian.Uint32(body[4:8])
	rest := body[8:]
	if version&^0xffff != protocolMajor {
		return 0, fmt.Errorf("%w: NegotiateProtocolVersion offers protocol %d.%d, under a request for 3.%d", ErrProtocol, version>>16, version&0xffff, asked)
	}
	minor := version & 0xffff
	if minor > asked {
		return 0, fmt.Errorf("%w: NegotiateProtocolVersion offers protocol 3.%d, above the 3.%d asked for", ErrProtocol, minor, asked)
	}
	if minor != 0 && minor != 2 {
		// 3.1 was reserved and never released: a server naming it is not
		// one this package can follow, and the protocol says to close.
		return 0, fmt.Errorf("%w: NegotiateProtocolVersion offers protocol 3.%d, which this package does not speak", ErrProtocol, minor)
	}
	// Each name is at least its "_pq_." prefix and a NUL: a count the body
	// cannot hold is refused before the loop trusts it.
	if uint64(count)*6 > uint64(len(rest)) {
		return 0, fmt.Errorf("%w: NegotiateProtocolVersion lists %d options in %d bytes", ErrProtocol, count, len(rest))
	}
	for range count {
		end := bytes.IndexByte(rest, 0)
		if end < 0 {
			return 0, fmt.Errorf("%w: NegotiateProtocolVersion option name without its terminator", ErrProtocol)
		}
		name := string(rest[:end])
		rest = rest[end+1:]
		if _, sent := params[name]; !sent || !strings.HasPrefix(name, "_pq_.") {
			return 0, fmt.Errorf("%w: NegotiateProtocolVersion declines the option %q, which the startup packet did not carry", ErrProtocol, name)
		}
	}
	if len(rest) != 0 {
		return 0, fmt.Errorf("%w: NegotiateProtocolVersion has %d bytes after its last option", ErrProtocol, len(rest))
	}
	if minor == asked && count == 0 {
		return 0, fmt.Errorf("%w: NegotiateProtocolVersion declines neither the version nor any option", ErrProtocol)
	}
	return minor, nil
}

// authStep answers one AuthenticationRequest.
func (c *Conn) authStep(st *startupState, code uint32, data []byte, cfg Config, ts tlsStater) error {
	sc := &st.scram
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
		if cfg.RequireChannelBinding {
			// A password method binds to nothing, so there is no answer that
			// would satisfy the caller: refused before the credential moves.
			return c.breakConn(fmt.Errorf("%w: the server asked for a cleartext password and Config.RequireChannelBinding is set — only a SCRAM-SHA-256-PLUS exchange satisfies it", ErrAuth))
		}
		if !cfg.AllowCleartextPassword {
			return c.breakConn(fmt.Errorf("%w: the server asked for a cleartext password and Config.AllowCleartextPassword is false — set it only over a transport the credential may cross, such as a verified *tls.Conn", ErrAuth))
		}
		if cfg.Password == "" {
			return c.breakConn(fmt.Errorf("%w: the server asked for a password and Config.Password is empty", ErrAuth))
		}
		// The password goes out as the server asked for it: in the clear.
		// Whether that is acceptable is the transport's business, and the
		// transport is the caller's — over a *tls.Conn it is fine, over a
		// plain socket it is a credential on the wire.
		st.asked = true
		c.w.PasswordMessage(cfg.Password)
		return c.flushAuth("sending the password")

	case authSASL:
		// Read here rather than at startup's entry: a *tls.Conn shakes hands
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
		st.asked, st.bound = true, mech == mechSCRAMSHA256Plus
		if cfg.RequireChannelBinding && !st.bound {
			// Over TLS — a plain connection was refused in startup — and the
			// unbound mechanism was chosen all the same: the server did not
			// offer -PLUS, or this connection shows no certificate to bind
			// to. Refused before the client's first message, which is what
			// the 'y' flag would otherwise leave to the server to judge.
			return c.breakConn(fmt.Errorf("%w: the server offers %s without %s and Config.RequireChannelBinding is set — the server has no certificate the exchange can bind to (ssl = off, a pooler terminating TLS), a man in the middle removed the mechanism, or this connection's handshake shows no peer certificate", ErrAuth, mechSCRAMSHA256, mechSCRAMSHA256Plus))
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
	if name == "client_encoding" && !c.encodingChosen && !strings.EqualFold(val, "UTF8") {
		// UTF8 was asked for, not granted: a pooler or a server-side default
		// can answer with another encoding at startup, and a SET can change
		// it at any point after. Every text decoder here would then read its
		// bytes as UTF-8 without error, so the connection ends at the
		// announcement. Breaking it here is what makes the read loops stop
		// rather than carry on as they do for a payload they cannot read.
		return c.breakConn(fmt.Errorf("the server reports client_encoding %q, and this package decodes text as UTF-8 — set Config.Params[\"client_encoding\"] to decode another encoding yourself", val))
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
