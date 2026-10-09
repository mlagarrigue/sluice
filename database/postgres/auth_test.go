package postgres

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// What these tests are for, and what they deliberately leave to scram_test.go.
//
// The values a SCRAM exchange computes are pinned against RFC 7677 next door,
// because a scripted server built on this package's own primitives would agree
// with a wrong derivation. What is tested here is the other half: the framing
// and the ordering — which message carries which field, what the client sends
// and in what order, and which server behaviours it refuses to continue past.
// A script can check that honestly, because framing is not what the script and
// the implementation share.

// Backend startup-phase message builders.
func beAuth(code uint32, data string) []byte {
	body := binary.BigEndian.AppendUint32(nil, code)
	return frame(pgwire.BackendAuthentication, string(body)+data)
}

func beParameterStatus(name, val string) []byte {
	return frame(pgwire.BackendParameterStatus, name+"\x00"+val+"\x00")
}

func beBackendKeyData() []byte {
	body := binary.BigEndian.AppendUint32(nil, 1234)
	body = binary.BigEndian.AppendUint32(body, 5678)
	return frame(pgwire.BackendBackendKeyData, string(body))
}

// beReady is the tail every successful startup ends with.
func beReady() []byte {
	var b []byte
	b = append(b, beParameterStatus("server_version", "17.2")...)
	b = append(b, beParameterStatus("client_encoding", "UTF8")...)
	b = append(b, beBackendKeyData()...)
	b = append(b, beReadyForQuery()...)
	return b
}

// fixNonce pins the client nonce so a whole exchange is reproducible. The seam
// is a package variable rather than a field on scram, because a nonce is not a
// thing a caller should be able to choose.
func fixNonce(t *testing.T, seed byte) string {
	t.Helper()
	saved := randRead
	t.Cleanup(func() { randRead = saved })
	randRead = func(b []byte) (int, error) {
		for i := range b {
			b[i] = seed + byte(i)
		}
		return len(b), nil
	}
	raw := make([]byte, nonceBytes)
	for i := range raw {
		raw[i] = seed + byte(i)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

// clientTraffic parses back everything the client wrote: the startup packet
// first, which is the one message with no type byte, then the rest.
func clientTraffic(t *testing.T, raw []byte) (startup, types []byte, bodies [][]byte) {
	t.Helper()
	rd := pgwire.NewReader(bytes.NewReader(raw))
	m, err := rd.NextStartup()
	if err != nil {
		t.Fatalf("reading back the startup packet: %v", err)
	}
	startup = slices.Clone(m.Body)
	for {
		m, err := rd.Next()
		if err != nil {
			return startup, types, bodies
		}
		types = append(types, m.Type)
		bodies = append(bodies, slices.Clone(m.Body))
	}
}

// A server configured for trust: the connection is up after one round trip,
// and what the server said about itself is readable.
func TestStartupTrust(t *testing.T) {
	var script []byte
	script = append(script, beAuth(authOK, "")...)
	script = append(script, beReady()...)

	srv := &fakeServer{responses: [][]byte{script}}
	conn, err := Startup(srv, StartupConfig{User: "orders", Database: "shop"})
	if err != nil {
		t.Fatalf("Startup: %v", err)
	}
	if got := conn.Parameter("server_version"); got != "17.2" {
		t.Errorf("Parameter(server_version) = %q, want %q", got, "17.2")
	}
	if got := conn.Parameter("nothing_sent"); got != "" {
		t.Errorf("Parameter of an unsent name = %q, want empty", got)
	}

	startup, types, _ := clientTraffic(t, srv.sent.Bytes())
	if len(types) != 0 {
		t.Errorf("client sent %q after the startup packet, want nothing", types)
	}
	if v := binary.BigEndian.Uint32(startup[:4]); v != startupVersion {
		t.Errorf("protocol version = %#x, want %#x", v, startupVersion)
	}
	want := "user\x00orders\x00database\x00shop\x00client_encoding\x00UTF8\x00\x00"
	if got := string(startup[4:]); got != want {
		t.Errorf("startup parameters = %q,\n                 want %q", got, want)
	}
}

// UTF8 is requested in the startup packet, but the server's ParameterStatus
// is what it actually uses. Another encoding, unasked for, would have every
// text decoder misread bytes without an error, so startup fails instead; a
// caller who set client_encoding themselves is taken at their word.
func TestStartupRefusesAnEncodingItDidNotAskFor(t *testing.T) {
	script := func(enc string) []byte {
		var b []byte
		b = append(b, beAuth(authOK, "")...)
		b = append(b, beParameterStatus("client_encoding", enc)...)
		return append(b, beReadyForQuery()...)
	}

	srv := &fakeServer{responses: [][]byte{script("LATIN1")}}
	if _, err := Startup(srv, StartupConfig{User: "orders"}); err == nil || !strings.Contains(err.Error(), "LATIN1") {
		t.Errorf("Startup with a LATIN1 session = %v, want the encoding refused", err)
	}

	srv = &fakeServer{responses: [][]byte{script("LATIN1")}}
	if _, err := Startup(srv, StartupConfig{User: "orders", Params: map[string]string{"client_encoding": "LATIN1"}}); err != nil {
		t.Errorf("Startup with client_encoding chosen by the caller = %v, want it accepted", err)
	}

	srv = &fakeServer{responses: [][]byte{script("utf8")}}
	if _, err := Startup(srv, StartupConfig{User: "orders"}); err != nil {
		t.Errorf("Startup with a lower-case utf8 report = %v, want it accepted", err)
	}
}

// ReadyForQuery is the server's first word that the session is usable; one
// whose body is not a single status byte is refused rather than believed.
func TestStartupRefusesAMalformedReadyForQuery(t *testing.T) {
	for name, body := range map[string]string{
		"empty":      "",
		"two bytes":  "II",
		"four bytes": "IIII",
		"bad status": "X",
	} {
		var script []byte
		script = append(script, beAuth(authOK, "")...)
		script = append(script, frame(pgwire.BackendReadyForQuery, body)...)
		srv := &fakeServer{responses: [][]byte{script}}
		if _, err := Startup(srv, StartupConfig{User: "orders", Database: "shop"}); !errors.Is(err, ErrProtocol) {
			t.Errorf("%s: Startup = %v, want ErrProtocol", name, err)
		}
	}
}

// The whole SCRAM exchange, end to end. The proof and signature values are
// this package's own — pinned elsewhere against the RFC — so what is asserted
// here is the shape: three client messages of the right types, the mechanism
// named with its own length prefix, and the exchange refusing to end without
// the server having proved itself.
func TestStartupSCRAM(t *testing.T) {
	const password = "pencil"
	clientNonce := fixNonce(t, 0x40)

	salt, err := base64.StdEncoding.DecodeString(rfcSalt)
	if err != nil {
		t.Fatal(err)
	}
	serverNonce := clientNonce + "ServerHalf"
	serverFirst := "r=" + serverNonce + ",s=" + rfcSalt + ",i=4096"

	// What the client will have signed by the time it answers.
	authMessage := "n=,r=" + clientNonce + "," + serverFirst +
		",c=biws,r=" + serverNonce
	_, serverSig, err := scramProof(password, salt, 4096, authMessage)
	if err != nil {
		t.Fatal(err)
	}

	var tail []byte
	tail = append(tail, beAuth(authSASLFinal, "v="+base64.StdEncoding.EncodeToString(serverSig))...)
	tail = append(tail, beAuth(authOK, "")...)
	tail = append(tail, beReady()...)

	srv := &fakeServer{responses: [][]byte{
		beAuth(authSASL, mechSCRAMSHA256+"\x00\x00"),
		beAuth(authSASLContinue, serverFirst),
		tail,
	}}

	conn, err := Startup(srv, StartupConfig{User: "orders", Password: password})
	if err != nil {
		t.Fatalf("Startup: %v", err)
	}
	if conn.Err() != nil {
		t.Errorf("the connection is broken after a successful startup: %v", conn.Err())
	}

	_, types, bodies := clientTraffic(t, srv.sent.Bytes())
	wantTypes := []byte{pgwire.FrontendPassword, pgwire.FrontendPassword}
	if !slices.Equal(types, wantTypes) {
		t.Fatalf("client sent %q, want %q", types, wantTypes)
	}

	// SASLInitialResponse: mechanism as a C string, then the response with its
	// own int32 length. The length is separate from the message's because the
	// response is binary-safe and a C string cannot be followed by bytes whose
	// extent nothing declares.
	mech, rest, ok := cstring(bodies[0])
	if !ok {
		t.Fatal("the initial response has no mechanism name")
	}
	if mech != mechSCRAMSHA256 {
		t.Errorf("mechanism = %q, want %q", mech, mechSCRAMSHA256)
	}
	n := binary.BigEndian.Uint32(rest[:4])
	if int(n) != len(rest)-4 {
		t.Errorf("initial response declares %d bytes, %d follow", n, len(rest)-4)
	}
	if got, want := string(rest[4:]), gs2Header+"n=,r="+clientNonce; got != want {
		t.Errorf("client first message = %q, want %q", got, want)
	}

	// SASLResponse: the message bytes and nothing else.
	final := string(bodies[1])
	if !strings.HasPrefix(final, "c=biws,r="+serverNonce+",p=") {
		t.Errorf("client final message = %q, want c=biws with the server's nonce and a proof", final)
	}
}

// A server that answers with a signature it could not have computed does not
// get authenticated to. This is the check that makes the exchange mutual, and
// the one an implementation is most tempted to skip because everything works
// without it.
func TestStartupSCRAMRefusesAWrongServerSignature(t *testing.T) {
	clientNonce := fixNonce(t, 0x20)
	serverFirst := "r=" + clientNonce + "ServerHalf,s=" + rfcSalt + ",i=4096"
	bogus := base64.StdEncoding.EncodeToString(make([]byte, 32))

	srv := &fakeServer{responses: [][]byte{
		beAuth(authSASL, mechSCRAMSHA256+"\x00\x00"),
		beAuth(authSASLContinue, serverFirst),
		append(beAuth(authSASLFinal, "v="+bogus), beReady()...),
	}}

	_, err := Startup(srv, StartupConfig{User: "orders", Password: "pencil"})
	if err == nil {
		t.Fatal("Startup accepted a server that does not know the password")
	}
	if !errors.Is(err, ErrAuth) {
		t.Errorf("error = %v, want it to wrap ErrAuth", err)
	}
	if !srv.closed {
		t.Error("the transport was left open after a failed startup")
	}
}

// A server that starts a SCRAM exchange and then declares the connection ready
// without finishing it has proved nothing — and this client has already sent a
// proof derived from the password. Continuing would be the downgrade.
func TestStartupRefusesAnUnfinishedSCRAMExchange(t *testing.T) {
	clientNonce := fixNonce(t, 0x60)
	serverFirst := "r=" + clientNonce + "ServerHalf,s=" + rfcSalt + ",i=4096"

	srv := &fakeServer{responses: [][]byte{
		beAuth(authSASL, mechSCRAMSHA256+"\x00\x00"),
		beAuth(authSASLContinue, serverFirst),
		append(beAuth(authOK, ""), beReady()...), // no SASLFinal
	}}

	_, err := Startup(srv, StartupConfig{User: "orders", Password: "pencil"})
	if err == nil {
		t.Fatal("Startup accepted a SCRAM exchange that never completed")
	}
	if !errors.Is(err, ErrAuth) {
		t.Errorf("error = %v, want it to wrap ErrAuth", err)
	}
}

func TestStartupCleartextPassword(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{
		beAuth(authCleartextPassword, ""),
		append(beAuth(authOK, ""), beReady()...),
	}}

	if _, err := Startup(srv, StartupConfig{User: "orders", Password: "pencil", AllowCleartextPassword: true}); err != nil {
		t.Fatalf("Startup: %v", err)
	}
	_, types, bodies := clientTraffic(t, srv.sent.Bytes())
	if !slices.Equal(types, []byte{pgwire.FrontendPassword}) {
		t.Fatalf("client sent %q, want one password message", types)
	}
	if got := string(bodies[0]); got != "pencil\x00" {
		t.Errorf("password message = %q, want the password as a C string", got)
	}
}

func TestStartupCleartextWithoutAPassword(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{beAuth(authCleartextPassword, "")}}
	_, err := Startup(srv, StartupConfig{User: "orders", AllowCleartextPassword: true})
	if !errors.Is(err, ErrAuth) {
		t.Errorf("error = %v, want ErrAuth rather than a password sent as an empty string", err)
	}
}

// The downgrade M-9 names: against a client that answers whatever method the
// peer asks for, an active MITM on a plain socket asks for cleartext before
// SCRAM ever starts and keeps the password. The default must be a refusal
// that happens before the credential is touched.
func TestStartupRefusesCleartextByDefault(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{beAuth(authCleartextPassword, "")}}
	_, err := Startup(srv, StartupConfig{User: "orders", Password: "pencil"})
	if !errors.Is(err, ErrAuth) {
		t.Fatalf("error = %v, want ErrAuth", err)
	}
	if !strings.Contains(err.Error(), "AllowCleartextPassword") {
		t.Errorf("error = %v, want it to name the knob that opts in", err)
	}
	// The assertion that matters: the password never left the client. The
	// startup packet does not carry it, so any occurrence in the sent bytes
	// is the credential crossing the wire.
	if bytes.Contains(srv.sent.Bytes(), []byte("pencil")) {
		t.Error("the password reached the wire although cleartext was refused")
	}
}

// md5 is declined, and the error names the server-side fix. A connector that
// refuses a method without saying what to do instead produces a support ticket
// rather than a change.
func TestStartupRefusesMD5(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{beAuth(authMD5Password, "salt")}}
	_, err := Startup(srv, StartupConfig{User: "orders", Password: "pencil"})
	if !errors.Is(err, ErrAuthUnsupported) {
		t.Fatalf("error = %v, want ErrAuthUnsupported", err)
	}
	for _, want := range []string{"md5", "scram-sha-256", `\password`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %v, want it to mention %q", err, want)
		}
	}
}

// The mechanism list is judged before a proof is computed, and both refusals
// travel out through Startup rather than stopping inside the exchange.
func TestStartupRefusesAnUnusableMechanismList(t *testing.T) {
	tests := []struct {
		name string
		list string
		want string
	}{
		{
			"only channel binding, which is not silently downgraded",
			mechSCRAMSHA256Plus + "\x00\x00", "channel binding",
		},
		{"an unterminated list", mechSCRAMSHA256, "unterminated mechanism"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := &fakeServer{responses: [][]byte{beAuth(authSASL, tc.list)}}
			_, err := Startup(srv, StartupConfig{User: "orders", Password: "pencil"})
			if err == nil {
				t.Fatal("Startup accepted it")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			}
			if srv.sent.Len() == 0 {
				t.Error("nothing was sent at all, so the startup packet never left")
			}
		})
	}
}

func TestStartupRefusesGSSAPI(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{beAuth(authGSS, "")}}
	_, err := Startup(srv, StartupConfig{User: "orders"})
	if !errors.Is(err, ErrAuthUnsupported) {
		t.Errorf("error = %v, want ErrAuthUnsupported", err)
	}
}

// A refusal from the server arrives as an ErrorResponse, and the SQLSTATE is
// what a caller acts on: 28P01 is a wrong password, 28000 is pg_hba refusing
// the connection outright, and they need different fixes.
func TestStartupServerRefusal(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{
		beAuth(authCleartextPassword, ""),
		beErrorResponse("28P01", "password authentication failed for user \"orders\""),
	}}

	_, err := Startup(srv, StartupConfig{User: "orders", Password: "wrong", AllowCleartextPassword: true})
	if err == nil {
		t.Fatal("Startup accepted a refused connection")
	}
	var pgErr *Error
	if !errors.As(err, &pgErr) {
		t.Fatalf("error = %v, want a *postgres.Error carrying the SQLSTATE", err)
	}
	if pgErr.Code != "28P01" {
		t.Errorf("SQLSTATE = %q, want %q", pgErr.Code, "28P01")
	}
	if !errors.Is(err, ErrConnBroken) {
		t.Errorf("error = %v, want it to also report the connection as unusable", err)
	}
}

// A server that closes the socket during startup usually refused the
// connection below the protocol — pg_hba, or TLS required on a plain socket —
// and said nothing. That must not read as a clean end of stream.
func TestStartupTruncated(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{}}
	_, err := Startup(srv, StartupConfig{User: "orders"})
	if err == nil {
		t.Fatal("Startup accepted a connection the server hung up on")
	}
	if !srv.closed {
		t.Error("the transport was left open after a failed startup")
	}
}

// The startup packet is built in a fixed order, so one configuration always
// produces one packet. Ranging a map here would make it differ between runs.
func TestStartupBodyIsDeterministic(t *testing.T) {
	cfg := StartupConfig{
		User:     "orders",
		Database: "shop",
		Params: map[string]string{
			"application_name":  "sluice",
			"search_path":       "public",
			"statement_timeout": "30s",
		},
	}
	first, err := startupBody(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for range 32 {
		again, err := startupBody(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(first, again) {
			t.Fatalf("startupBody is not deterministic:\n %q\n %q", first, again)
		}
	}
	got := string(first[4:])
	wantOrdered := "user\x00orders\x00database\x00shop\x00client_encoding\x00UTF8\x00" +
		"application_name\x00sluice\x00search_path\x00public\x00statement_timeout\x0030s\x00\x00"
	if got != wantOrdered {
		t.Errorf("startup parameters = %q,\n                 want %q", got, wantOrdered)
	}
}

func TestStartupBodyOverridesClientEncoding(t *testing.T) {
	b, err := startupBody(StartupConfig{
		User:   "orders",
		Params: map[string]string{"client_encoding": "LATIN1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(b), "client_encoding\x00"); n != 1 {
		t.Errorf("client_encoding appears %d times, want 1: the caller's value replaces the default", n)
	}
	if !strings.Contains(string(b), "client_encoding\x00LATIN1\x00") {
		t.Errorf("startup parameters = %q, want the caller's encoding", b)
	}
}

func TestStartupBodyRefusals(t *testing.T) {
	tests := []struct {
		name string
		cfg  StartupConfig
		want string
	}{
		{"no user", StartupConfig{}, "User is empty"},
		{
			"a role smuggling extra parameters through a NUL",
			StartupConfig{User: "app\x00options\x00-c search_path=evil"},
			"NUL byte",
		},
		{
			"a parameter name smuggling another parameter",
			StartupConfig{User: "orders", Params: map[string]string{"a\x00b": "c"}},
			"NUL byte",
		},
		{
			"a parameter value smuggling another parameter",
			StartupConfig{User: "orders", Params: map[string]string{"application_name": "a\x00b"}},
			"NUL byte",
		},
		{
			"the role set twice, in two places that could disagree",
			StartupConfig{User: "orders", Params: map[string]string{"user": "someone_else"}},
			"StartupConfig.User",
		},
		{
			"a client_encoding smuggling another parameter",
			StartupConfig{User: "orders", Params: map[string]string{"client_encoding": "UTF8\x00x\x00y"}},
			"NUL byte",
		},
		{
			"the database set twice",
			StartupConfig{User: "orders", Params: map[string]string{"database": "other"}},
			"StartupConfig.Database",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := startupBody(tc.cfg)
			if err == nil {
				t.Fatal("startupBody accepted it")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// An empty Database means the server's default, which is a database named
// after the role. Sending "database" with an empty value would instead ask for
// a database whose name is the empty string.
func TestStartupBodyOmitsAnEmptyDatabase(t *testing.T) {
	b, err := startupBody(StartupConfig{User: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "database\x00") {
		t.Errorf("startup parameters = %q, want no database key at all", b)
	}
}

// A Conn from NewConn never saw the startup phase, and says so by reporting
// nothing rather than by panicking on a nil map.
func TestParameterOnAConnThatSkippedStartup(t *testing.T) {
	conn := NewConn(&fakeServer{})
	if got := conn.Parameter("server_version"); got != "" {
		t.Errorf("Parameter = %q, want empty", got)
	}
}

// A notice may arrive at any point, including in the middle of a handshake,
// and it is not an error. A startup that broke on one would fail on a server
// configured to warn about a deprecated setting.
func TestStartupIgnoresANoticeDuringTheHandshake(t *testing.T) {
	var script []byte
	script = append(script, frame(pgwire.BackendNoticeResponse, string(errBody("S", "NOTICE", "C", "01000", "M", "a warning")))...)
	script = append(script, beAuth(authOK, "")...)
	script = append(script, beReady()...)

	srv := &fakeServer{responses: [][]byte{script}}
	if _, err := Startup(srv, StartupConfig{User: "orders"}); err != nil {
		t.Fatalf("Startup: %v", err)
	}
}

// Malformed startup-phase messages. Each one means the stream is being read at
// the wrong offset, so the connection is finished rather than retried — and
// each one is a fuzzing target's landing spot, reachable before any credential
// has been checked.
func TestStartupRefusesMalformedMessages(t *testing.T) {
	tests := []struct {
		name   string
		script []byte
		want   error
	}{
		{
			"an Authentication message with no room for its code",
			frame(pgwire.BackendAuthentication, "ab"),
			ErrProtocol,
		},
		{
			"an authentication method that does not exist",
			beAuth(99, ""),
			ErrAuthUnsupported,
		},
		{
			"a message type that has no business in a handshake",
			frame(pgwire.BackendDataRow, "\x00\x00"),
			ErrProtocol,
		},
		{
			"a ParameterStatus with an unterminated name",
			append(beAuth(authOK, ""), frame(pgwire.BackendParameterStatus, "server_version")...),
			ErrProtocol,
		},
		{
			"a ParameterStatus with an unterminated value",
			append(beAuth(authOK, ""), frame(pgwire.BackendParameterStatus, "server_version\x0017.2")...),
			ErrProtocol,
		},
		{
			"an ErrorResponse with no terminator",
			append(beAuth(authOK, ""), frame(pgwire.BackendErrorResponse, "SERROR\x00")...),
			ErrProtocol,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := &fakeServer{responses: [][]byte{tc.script}}
			_, err := Startup(srv, StartupConfig{User: "orders"})
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			if !errors.Is(err, ErrConnBroken) {
				t.Errorf("error = %v, want the connection reported as unusable", err)
			}
			if !srv.closed {
				t.Error("the transport was left open after a failed startup")
			}
		})
	}
}

// A socket that cannot be written to fails at the step it failed on, named.
// "authentication failed" with no step named is the least diagnosable message
// a connector can produce.
func TestStartupReportsWhichStepFailedToSend(t *testing.T) {
	t.Run("the startup packet", func(t *testing.T) {
		srv := &fakeServer{writeErr: errors.New("broken pipe")}
		_, err := Startup(srv, StartupConfig{User: "orders"})
		if err == nil || !strings.Contains(err.Error(), "sending the startup packet") {
			t.Fatalf("error = %v, want it to name the startup packet", err)
		}
		if !srv.closed {
			t.Error("the transport was left open after a failed startup")
		}
	})

	t.Run("the password", func(t *testing.T) {
		// The write fails only once the handshake is under way, so the server
		// gets its chance to ask before the socket gives out.
		srv := &failOnNthWrite{fakeServer: fakeServer{
			responses: [][]byte{beAuth(authCleartextPassword, "")},
		}, after: 1}
		_, err := Startup(srv, StartupConfig{User: "orders", Password: "pencil", AllowCleartextPassword: true})
		if err == nil || !strings.Contains(err.Error(), "sending the password") {
			t.Fatalf("error = %v, want it to name the password step", err)
		}
	})
}

// failOnNthWrite lets a handshake start and then takes the socket away, which
// is what a server restart looks like from this side.
type failOnNthWrite struct {
	fakeServer
	after int
	n     int
}

func (f *failOnNthWrite) Write(p []byte) (int, error) {
	f.n++
	if f.n > f.after {
		return 0, errors.New("broken pipe")
	}
	return f.fakeServer.Write(p)
}

// A server whose first message cannot be believed stops the exchange there,
// with the reason travelling out through Startup rather than being flattened.
func TestStartupSCRAMRefusesABadServerFirstMessage(t *testing.T) {
	fixNonce(t, 0x10)
	srv := &fakeServer{responses: [][]byte{
		beAuth(authSASL, mechSCRAMSHA256+"\x00\x00"),
		beAuth(authSASLContinue, "r=notthisclientsnonce,s="+rfcSalt+",i=4096"),
	}}
	_, err := Startup(srv, StartupConfig{User: "orders", Password: "pencil"})
	if !errors.Is(err, ErrAuth) {
		t.Fatalf("error = %v, want ErrAuth", err)
	}
	if !strings.Contains(err.Error(), "does not extend") {
		t.Errorf("error = %v, want it to name the nonce as the reason", err)
	}
}

// A SASLFinal this client cannot read is not a signature it may skip.
func TestStartupSCRAMRefusesAnUnreadableFinalMessage(t *testing.T) {
	for _, final := range []string{"v", "v=!!!not-base64!!!"} {
		t.Run(final, func(t *testing.T) {
			clientNonce := fixNonce(t, 0x30)
			srv := &fakeServer{responses: [][]byte{
				beAuth(authSASL, mechSCRAMSHA256+"\x00\x00"),
				beAuth(authSASLContinue, "r="+clientNonce+"ServerHalf,s="+rfcSalt+",i=4096"),
				beAuth(authSASLFinal, final),
			}}
			_, err := Startup(srv, StartupConfig{User: "orders", Password: "pencil"})
			if !errors.Is(err, ErrAuth) {
				t.Errorf("error = %v, want ErrAuth", err)
			}
		})
	}
}

// A NUL anywhere in the startup packet is refused before a byte is sent, and
// the transport is closed rather than left half-opened.
func TestStartupRefusesABadConfigWithoutWriting(t *testing.T) {
	srv := &fakeServer{}
	_, err := Startup(srv, StartupConfig{User: "orders", Database: "shop\x00evil"})
	if err == nil || !strings.Contains(err.Error(), "NUL byte") {
		t.Fatalf("error = %v, want the NUL refused", err)
	}
	if srv.sent.Len() != 0 {
		t.Errorf("the client wrote %d bytes for a configuration it refused", srv.sent.Len())
	}
	if !srv.closed {
		t.Error("the transport was left open after a failed startup")
	}
}

// A server that has proved nothing yet does not get to choose how much memory
// this client spends on it. The handshake runs under a bound two orders of
// magnitude below the one a real workload needs, raised only once the server
// is ready.
func TestStartupBoundsWhatAnUnprovenServerCanSend(t *testing.T) {
	// A ParameterStatus claiming more than the startup bound allows. Nothing
	// is allocated for it: the length is judged before the buffer is grown.
	oversized := frame(pgwire.BackendParameterStatus, strings.Repeat("x", maxStartupMessage))

	srv := &fakeServer{responses: [][]byte{append(beAuth(authOK, ""), oversized...)}}
	_, err := Startup(srv, StartupConfig{User: "orders"})
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("error = %v, want ErrMessageTooLarge", err)
	}
	if !srv.closed {
		t.Error("the transport was left open after a failed startup")
	}
}

// And once the server is ready the ordinary bound applies, so the tighter one
// costs a real workload nothing.
func TestStartupRestoresTheOrdinaryLimit(t *testing.T) {
	srv := &fakeServer{responses: [][]byte{append(beAuth(authOK, ""), beReady()...)}}
	conn, err := Startup(srv, StartupConfig{User: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if conn.r.MaxMessage() != pgwire.DefaultMaxMessage {
		t.Errorf("message limit after startup = %d, want %d", conn.r.MaxMessage(), pgwire.DefaultMaxMessage)
	}
}

// The protocol allows a ParameterStatus at any point in a session, and every
// read loop records it. Without that, Parameter reports what was true when the
// connection opened, forever — which is worse than reporting nothing, because
// it looks like an answer.
func TestParameterTracksAMidSessionChange(t *testing.T) {
	var result []byte
	result = append(result, beSimple(pgwire.BackendParseComplete)...)
	result = append(result, beSimple(pgwire.BackendBindComplete)...)
	result = append(result, beSimple(pgwire.BackendNoData)...)
	// The announcement arrives inside the result, which is where a client
	// that only handles it during startup would break the connection.
	result = append(result, beParameterStatus("application_name", "after-the-set")...)
	result = append(result, beCommandComplete("SET")...)

	srv := &fakeServer{responses: [][]byte{
		append(beAuth(authOK, ""), beReady()...),
		result,
		append(beParameterStatus("TimeZone", "Etc/UTC"), beReadyForQuery()...),
	}}
	conn, err := Startup(srv, StartupConfig{User: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if got := conn.Parameter("client_encoding"); got != "UTF8" {
		t.Fatalf("startup parameters were not recorded: client_encoding = %q", got)
	}

	src := conn.Query(t.Context(), "SET application_name = 'after-the-set'", nil, QueryConfig{})
	src.Stream()(func(sluice.Batch[Row]) bool { return true })
	if err := src.Err(); err != nil {
		t.Fatalf("Err = %v", err)
	}
	if got := conn.Parameter("application_name"); got != "after-the-set" {
		t.Errorf("application_name = %q, want the value the server announced mid-result", got)
	}
	// And the one the resync swallows on the way to ReadyForQuery.
	if got := conn.Parameter("TimeZone"); got != "Etc/UTC" {
		t.Errorf("TimeZone = %q, want the value announced during the resync", got)
	}
}

// A peer that answers the socket does not get to grow this client's map
// without end, before it has proved anything.
func TestStartupBoundsTheParameterMap(t *testing.T) {
	script := beAuth(authOK, "")
	for i := range MaxServerParameters + 1 {
		script = append(script, beParameterStatus("p"+strconv.Itoa(i), "v")...)
	}
	script = append(script, beReadyForQuery()...)

	srv := &fakeServer{responses: [][]byte{script}}
	_, err := Startup(srv, StartupConfig{User: "orders"})
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("error = %v, want ErrProtocol", err)
	}
	if !strings.Contains(err.Error(), "distinct runtime parameters") {
		t.Errorf("error = %v, want it to name the cause", err)
	}
}

// Capping how many parameters are remembered is not enough once the message
// ceiling is the post-auth 64 MiB: one ParameterStatus could still carry a
// near-ceiling value into the retained map. The size of each is bounded too.
func TestStartupBoundsAParametersSize(t *testing.T) {
	script := beAuth(authOK, "")
	script = append(script, beParameterStatus("application_name", strings.Repeat("v", MaxServerParameterBytes))...)
	script = append(script, beReadyForQuery()...)

	srv := &fakeServer{responses: [][]byte{script}}
	_, err := Startup(srv, StartupConfig{User: "orders"})
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("error = %v, want ErrProtocol", err)
	}
	if !strings.Contains(err.Error(), "name and value") {
		t.Errorf("error = %v, want it to name the cause", err)
	}
}

// A NUL in the password would truncate the cleartext message and authenticate
// with a prefix, while the caller believes the whole password was sent.
func TestStartupRefusesANULInThePassword(t *testing.T) {
	srv := &fakeServer{}
	_, err := Startup(srv, StartupConfig{User: "orders", Password: "pen\x00cil"})
	if err == nil || !strings.Contains(err.Error(), "NUL byte") {
		t.Fatalf("error = %v, want the NUL refused", err)
	}
	// And the password itself must not travel into the error text.
	if strings.Contains(err.Error(), "pen") {
		t.Errorf("the error quotes the password: %v", err)
	}
}
