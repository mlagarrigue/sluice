package postgres_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"math"
	"net/netip"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// Everything else in this package is tested against a scripted server, which
// is what makes those tests deterministic and fast — and is also their limit:
// a script cannot disagree with the implementation about what the protocol
// means, because the same reading of the specification wrote both.
//
// This file is where that limit is lifted. It runs against a real PostgreSQL
// when SLUICE_PG is set to a connection target and skips otherwise, so the
// gap is a missing server rather than missing code:
//
//	SLUICE_PG=127.0.0.1:5432 SLUICE_PG_USER=postgres SLUICE_PG_DB=postgres \
//	SLUICE_PG_PASSWORD=secret go test ./postgres/ -run TestIntegration
//
// It deliberately covers the things a script cannot check: that the server
// accepts the startup packet this package builds, that it agrees the
// parameters are binary, that `= ANY($1)` over an encoded array returns what
// the array named, and that the type OIDs in a RowDescription are the ones the
// codecs expect.
//
// Authentication runs for real here, which is the point of running it against
// a server at all: SCRAM-SHA-256 is a five-message exchange whose every value
// depends on the ones before it, and the only thing that can disagree with
// this package's reading of RFC 7677 is an implementation that did not read it
// with the same eyes. Set SLUICE_PG_PASSWORD where the server asks for one.

func pgTarget(t *testing.T) (addr string, cfg postgres.Config) {
	t.Helper()
	addr = os.Getenv("SLUICE_PG")
	if addr == "" {
		t.Skip("SLUICE_PG is not set: no server to talk to")
	}
	cfg.User = os.Getenv("SLUICE_PG_USER")
	if cfg.User == "" {
		cfg.User = "postgres"
	}
	cfg.Database = os.Getenv("SLUICE_PG_DB")
	if cfg.Database == "" {
		cfg.Database = cfg.User
	}
	cfg.Password = os.Getenv("SLUICE_PG_PASSWORD")
	return addr, cfg
}

// dial opens a connection and completes the startup exchange, which is what
// this file exists to exercise against something that can disagree.
func dial(t *testing.T) *postgres.Conn {
	t.Helper()
	addr, cfg := pgTarget(t)
	return dialWith(t, addr, cfg)
}

// dialWith is dial with the Config the test chose.
func dialWith(t *testing.T, addr string, cfg postgres.Config) *postgres.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	conn, err := postgres.Connect(ctx, postgres.DialTCP(addr), cfg)
	if err != nil {
		t.Fatalf("connecting to %s as %s: %v", addr, cfg.User, err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// The server describing itself is the cheapest possible check that the
	// startup phase was read correctly, and it costs no round trip.
	if v := conn.Parameter("server_version"); v == "" {
		t.Error("the server announced no server_version, which every version does")
	} else {
		t.Logf("connected to PostgreSQL %s as %s", v, cfg.User)
	}
	return conn
}

// A value over the default message bound — PostgreSQL allows 1 GiB per
// field, the reader 64 MiB per message — is refused unread and breaks the
// connection, with an error that names the field to raise; raised, the same
// value is read whole. One message of 70 MB, not more: the runner's memory
// is not the subject.
func TestIntegrationMaxMessageLiftsTheDefaultBound(t *testing.T) {
	const size = 70_000_000
	const query = "SELECT repeat('x', 70000000)"
	addr, cfg := pgTarget(t)

	without := dialWith(t, addr, cfg)
	src := without.Query(t.Context(), query, nil, postgres.QueryConfig{})
	src.Stream()(func(sluice.Batch[postgres.Row]) bool { return true })
	err := src.Err()
	if !errors.Is(err, postgres.ErrMessageTooLarge) {
		t.Fatalf("a %d-byte value under the default bound: %v, want ErrMessageTooLarge", size, err)
	}
	if !strings.Contains(err.Error(), "Config.MaxMessage") {
		t.Errorf("the error does not name the field that raises the bound: %v", err)
	}
	if err := without.Err(); !errors.Is(err, postgres.ErrConnBroken) {
		t.Errorf("the connection after the refusal: %v, want ErrConnBroken", err)
	}

	cfg.MaxMessage = 128 << 20
	with := dialWith(t, addr, cfg)
	got := -1
	src = with.Query(t.Context(), query, nil, postgres.QueryConfig{})
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		for _, row := range b.Items {
			v, _ := row.Value(0)
			got = len(v)
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("the same value with MaxMessage raised: %v", err)
	}
	if got != size {
		t.Errorf("read a value of %d bytes, want %d", got, size)
	}
	if err := with.Err(); err != nil {
		t.Errorf("the connection after the large value: %v", err)
	}
}

// The claim the whole connector is built around, against a server that can
// disagree: a thousand keys leave as one parameter and come back as rows.
func TestIntegrationAnyArray(t *testing.T) {
	conn := dial(t)

	const n = 1000
	keys := make([]int64, n)
	for i := range keys {
		keys[i] = int64(i)
	}

	src := conn.Query(t.Context(),
		"SELECT k FROM unnest($1::bigint[]) AS k WHERE k = ANY($1) ORDER BY k",
		[]postgres.Param{{OID: pgtype.OIDInt8Array, Value: pgtype.AppendInt8Array(nil, keys)}},
		postgres.QueryConfig{BatchRows: 256},
	)

	var got []int64
	batches := 0
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		batches++
		for _, row := range b.Items {
			v, isNull := row.Value(0)
			if isNull {
				t.Error("a key came back NULL")
				continue
			}
			k, err := pgtype.DecodeInt8(v)
			if err != nil {
				t.Fatalf("decoding a key: %v", err)
			}
			got = append(got, k)
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("Err = %v", err)
	}

	if len(got) != n {
		t.Fatalf("got %d rows, want %d", len(got), n)
	}
	for i, k := range got {
		if k != int64(i) {
			t.Fatalf("row %d is %d", i, k)
		}
	}
	// The portal was resumed a batch at a time, which is the back-pressure
	// reaching the server rather than a claim about it.
	if want := n / 256; batches < want {
		t.Errorf("%d batches for %d rows at 256 per batch, want at least %d", batches, n, want)
	}
}

// The type OIDs the codecs expect must be the ones the server reports, which
// is the assumption a scripted test bakes in on both sides.
func TestIntegrationTypeOIDs(t *testing.T) {
	conn := dial(t)

	src := conn.Query(t.Context(),
		`SELECT 1::bigint, 1::int, 1::smallint, 1.5::float8, true, 'x'::text,
		        now()::timestamptz, current_date, '1.25'::numeric`,
		nil, postgres.QueryConfig{BatchRows: 8},
	)

	want := []uint32{
		pgtype.OIDInt8, pgtype.OIDInt4, pgtype.OIDInt2, pgtype.OIDFloat8,
		pgtype.OIDBool, pgtype.OIDText, pgtype.OIDTimestampTZ,
		pgtype.OIDDate, pgtype.OIDNumeric,
	}
	var fields []postgres.Field
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		if b.Len() > 0 {
			fields = b.Items[0].Fields()
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("Err = %v", err)
	}
	if len(fields) != len(want) {
		t.Fatalf("got %d columns, want %d", len(fields), len(want))
	}
	for i, f := range fields {
		if f.TypeOID != want[i] {
			t.Errorf("column %d (%s) is OID %d, want %d", i, f.Name, f.TypeOID, want[i])
		}
		if f.Format != pgwire.FormatBinary {
			t.Errorf("column %d came back in format %d, want binary", i, f.Format)
		}
	}
}

// numeric is the codec with the most room to be subtly wrong, and the one a
// scripted test can least vouch for: this asks the server to agree.
func TestIntegrationNumericRoundTrip(t *testing.T) {
	conn := dial(t)

	values := []string{
		"0", "1", "-1", "1.5", "10.00", "0.000000000000000000001",
		"12345678901234567890.12345678901234567890",
		"-99999999999999999999999999999999999999",
	}
	for _, want := range values {
		t.Run(want, func(t *testing.T) {
			n, err := pgtype.ParseNumeric(want)
			if err != nil {
				t.Fatal(err)
			}
			src := conn.Query(t.Context(), "SELECT $1::numeric", []postgres.Param{{OID: pgtype.OIDNumeric, Value: pgtype.AppendNumeric(nil, n)}},
				postgres.QueryConfig{BatchRows: 4})

			var got string
			src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
				for _, row := range b.Items {
					v, _ := row.Value(0)
					back, err := pgtype.DecodeNumeric(v)
					if err != nil {
						t.Fatalf("decoding: %v", err)
					}
					got = back.String()
				}
				return true
			})
			if err := src.Err(); err != nil {
				t.Fatalf("Err = %v", err)
			}
			if got != want {
				t.Errorf("the server returned %q for %q", got, want)
			}
		})
	}
}

// A binary COPY the server actually accepts, which is where a wrong signature
// or a miscounted field would show.
func TestIntegrationCopy(t *testing.T) {
	conn := dial(t)

	if err := exec(t, conn, "CREATE TEMP TABLE sluice_copy_test (id bigint, label text)"); err != nil {
		t.Fatalf("creating the temp table: %v", err)
	}

	cp, err := conn.CopyFrom(t.Context(),
		"COPY sluice_copy_test (id, label) FROM STDIN WITH (FORMAT BINARY)",
		postgres.CopyConfig{RowsPerTx: 100},
	)
	if err != nil {
		t.Fatalf("CopyFrom: %v", err)
	}
	const rows = 250
	var buf []byte
	for i := range rows {
		buf = buf[:0]
		buf = postgres.AppendTupleHeader(buf, 2)
		buf = postgres.AppendField(buf, pgtype.AppendInt8(nil, int64(i)))
		if i%2 == 0 {
			buf = postgres.AppendField(buf, []byte("even"))
		} else {
			buf = postgres.AppendFieldNull(buf)
		}
		if err := cp.WriteTuples(buf, 1); err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
	}
	if err := cp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The rows must be there, with the NULLs still NULL.
	src := conn.Query(t.Context(),
		"SELECT count(*), count(label) FROM sluice_copy_test", nil,
		postgres.QueryConfig{BatchRows: 4},
	)
	var total, labelled int64
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		for _, row := range b.Items {
			v, _ := row.Value(0)
			total, _ = pgtype.DecodeInt8(v)
			v, _ = row.Value(1)
			labelled, _ = pgtype.DecodeInt8(v)
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("Err = %v", err)
	}
	if total != rows {
		t.Errorf("the table holds %d rows, want %d", total, rows)
	}
	if want := int64(rows / 2); labelled != want {
		t.Errorf("%d rows have a label, want %d — the NULLs did not survive", labelled, want)
	}
}

// A server error must come back as an *Error with its SQLSTATE, from a server
// rather than from a script.
func TestIntegrationServerError(t *testing.T) {
	conn := dial(t)

	const sql = "SELECT * FROM a_table_that_does_not_exist"
	src := conn.Query(t.Context(), sql, nil, postgres.QueryConfig{})
	src.Stream()(func(sluice.Batch[postgres.Row]) bool { return true })

	var pgErr *postgres.Error
	if err := src.Err(); !errors.As(err, &pgErr) {
		t.Fatalf("Err = %v, want a *postgres.Error", err)
	}
	if pgErr.Code != "42P01" {
		t.Errorf("SQLSTATE = %q, want 42P01 (undefined_table)", pgErr.Code)
	}
	// The parser points at the token it failed on, counted in characters
	// from 1; the text is ASCII, so the byte index is the same.
	if want := strings.Index(sql, "a_table") + 1; pgErr.Position != want {
		t.Errorf("Position = %d, want %d — the start of the missing table's name", pgErr.Position, want)
	}
	// And the connection survives a query error, which is the distinction the
	// connector makes between a failed statement and a broken stream.
	if conn.Err() != nil {
		t.Errorf("a query error broke the connection: %v", conn.Err())
	}
	if err := exec(t, conn, "SELECT 1"); err != nil {
		t.Errorf("the connection was unusable after a query error: %v", err)
	}
}

// A wrong password is refused by the server, and the error says so: a 28P01
// a caller can find with errors.As, in a text that reads as a refusal rather
// than as a protocol fault.
func TestIntegrationWrongPassword(t *testing.T) {
	addr, cfg := pgTarget(t)
	if cfg.Password == "" {
		t.Skip("SLUICE_PG_PASSWORD is not set: the server is reached without a password")
	}
	cfg.Password += "-wrong"
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	conn, err := postgres.Connect(ctx, postgres.DialTCP(addr), cfg)
	if err == nil {
		_ = conn.Close()
		t.Skip("the server accepted a wrong password: pg_hba.conf trusts this connection")
	}
	var pgErr *postgres.Error
	if !errors.As(err, &pgErr) || pgErr.Code != "28P01" {
		t.Fatalf("error = %v, want a *postgres.Error with SQLSTATE 28P01 (invalid_password)", err)
	}
	if strings.Contains(err.Error(), "out of sync") {
		t.Errorf("error = %q reads as a protocol fault, not as the server's refusal", err)
	}
}

// A password outside NFKC authenticates: the server ran SASLprep on it when
// it stored the role's key, and the client must derive its proof from the
// same prepared string. Its NFKC form is then the same password, which shows
// the preparation happened on both ends rather than on neither.
func TestIntegrationSASLprepPassword(t *testing.T) {
	// A name of this run's own: a role belongs to the whole server, and two
	// `go test` processes sharing one name dropped it under each other.
	role := "sluice_saslprep_" + strings.ToLower(rand.Text())
	const password = "Ⅸ-ｐａｓｓ" //nolint:gosec // G101: a throwaway role's password; "Ⅸ" and fullwidth letters
	addr, cfg := pgTarget(t)
	admin := dial(t)
	// The cleanup runs after t.Context() is cancelled, so it drops the role
	// under a context of its own.
	drop := func(ctx context.Context) error {
		return execContext(ctx, admin, "DROP ROLE IF EXISTS "+role)
	}
	if err := drop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := exec(t, admin, "SET password_encryption = 'scram-sha-256'"); err != nil {
		t.Fatal(err)
	}
	if err := exec(t, admin, "CREATE ROLE "+role+" LOGIN PASSWORD '"+password+"'"); err != nil {
		var pgErr *postgres.Error
		if errors.As(err, &pgErr) && pgErr.Code == "42501" {
			t.Skipf("%s may not create roles: %v", cfg.User, err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := drop(ctx); err != nil {
			t.Errorf("drop role %s: %v", role, err)
		}
	})

	for _, p := range []string{password, "IX-pass"} {
		c := cfg
		c.User, c.Password = role, p
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		conn, err := postgres.Connect(ctx, postgres.DialTCP(addr), c)
		cancel()
		if err != nil {
			t.Errorf("password %+q: %v", p, err)
			continue
		}
		_ = conn.Close()
	}
}

// exec runs a statement for its effect, draining whatever it returns.
// A session the server ends on idle_session_timeout is reported as that: the
// next call finds the FATAL 57P05 the server sent before closing, under
// ErrConnBroken, not the end of the stream that followed it.
func TestIntegrationIdleSessionTimeoutIsReported(t *testing.T) {
	conn := dial(t)
	if err := conn.Exec(t.Context(), "SET idle_session_timeout = '100ms'"); err != nil {
		t.Fatalf("SET idle_session_timeout: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	err := conn.Exec(t.Context(), "SELECT 1")
	var pgErr *postgres.Error
	if !errors.As(err, &pgErr) || pgErr.Code != "57P05" {
		t.Fatalf("err = %v; want the server's 57P05 (idle_session_timeout)", err)
	}
	if !errors.Is(err, postgres.ErrConnBroken) {
		t.Errorf("err = %v; want ErrConnBroken as well", err)
	}
}

func exec(t *testing.T, conn *postgres.Conn, sql string) error {
	t.Helper()
	return execContext(t.Context(), conn, sql)
}

func execContext(ctx context.Context, conn *postgres.Conn, sql string) error {
	src := conn.Query(ctx, sql, nil, postgres.QueryConfig{BatchRows: 16})
	src.Stream()(func(sluice.Batch[postgres.Row]) bool { return true })
	return src.Err()
}

// A runtime parameter changed by the statement itself, announced by the server
// while the result is still arriving. A script deferred these to a tidy place;
// only a server decides where they actually land.
func TestIntegrationParameterStatusMidSession(t *testing.T) {
	conn := dial(t)

	before := conn.Parameter("application_name")
	src := conn.Query(t.Context(),
		`SELECT i, set_config('application_name', 'sluice-'||i, false)
		 FROM generate_series(1, 3) AS i`,
		nil, postgres.QueryConfig{BatchRows: 2})

	rows := 0
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool { rows += len(b.Items); return true })
	if err := src.Err(); err != nil {
		t.Fatalf("Err = %v", err)
	}
	if rows != 3 {
		t.Errorf("rows = %d, want 3", rows)
	}
	if conn.Err() != nil {
		t.Fatalf("the announcement broke the connection: %v", conn.Err())
	}

	after := conn.Parameter("application_name")
	if after != "sluice-3" {
		t.Errorf("application_name = %q (was %q), want the value the server last announced", after, before)
	}
}

// The server judging this package's encoding, rather than this package judging
// itself.
//
// Each case sends a value as a binary parameter and asks the server whether it
// equals the literal it was meant to be. The answer is the server's own
// comparison operator for that type, so a byte sequence that is merely
// plausible fails here — which is what a round trip through this package
// cannot detect, since an encoder and a decoder that share a misreading agree
// with each other forever.
func TestIntegrationCodecsMeanWhatTheyShould(t *testing.T) {
	conn := dial(t)

	tests := []struct {
		name    string
		literal string // SQL that builds the value the parameter should equal
		oid     uint32
		param   []byte
	}{
		{
			"jsonb", `'{"a": 1}'::jsonb`, pgtype.OIDJSONB,
			pgtype.AppendJSONB(nil, []byte(`{"a": 1}`)),
		},
		{
			// Compared as text, because PostgreSQL gives `json` no equality
			// operator at all — equality on unparsed text is ill-defined, and
			// that refusal is the clearest statement of what separates the two
			// types: `jsonb` is a parsed value that can be compared, `json` is
			// the bytes you sent. Text comparison is therefore the *right*
			// check here rather than a workaround: it is exactly what the type
			// promises to preserve.
			"json", `'{"a":1}'`, pgtype.OIDJSON,
			pgtype.AppendJSON(nil, []byte(`{"a":1}`)),
		},
		{
			"timestamp without time zone", `'2024-03-01 12:34:56.789012'::timestamp`, pgtype.OIDTimestamp,
			pgtype.AppendTimestamp(nil, time.Date(2024, 3, 1, 12, 34, 56, 789012000, time.UTC)),
		},
		{
			// Far outside what a time.Duration spans, which is where the
			// decoder used to wrap and return a date six centuries off with
			// no error. The server's own equality decides.
			"a timestamp beyond a Duration's reach", `'3000-01-01 00:00:00'::timestamp`, pgtype.OIDTimestamp,
			pgtype.AppendTimestamp(nil, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)),
		},
		{
			"the type's upper bound", `'294276-12-31 23:59:59.999999'::timestamp`, pgtype.OIDTimestamp,
			pgtype.AppendTimestamp(nil, time.Date(294276, 12, 31, 23, 59, 59, 999999000, time.UTC)),
		},
		{
			"a timestamptz beyond a Duration's reach", `'3000-01-01 00:00:00+00'::timestamptz`, pgtype.OIDTimestampTZ,
			pgtype.AppendTimestampTZ(nil, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)),
		},
		{
			"an interval longer than a Duration", `'100000000 hours'::interval`, pgtype.OIDInterval,
			pgtype.AppendInterval(nil, pgtype.Interval{Micros: 100_000_000 * 3600 * 1_000_000}),
		},
		{
			"interval with all three fields", `'1 year 2 mons 3 days 04:05:06.789'::interval`, pgtype.OIDInterval,
			pgtype.AppendInterval(nil, pgtype.Interval{Months: 14, Days: 3, Micros: 14_706_789_000}),
		},
		{
			"a negative interval", `'-1 mon -1 day -00:00:01'::interval`, pgtype.OIDInterval,
			pgtype.AppendInterval(nil, pgtype.Interval{Months: -1, Days: -1, Micros: -1_000_000}),
		},
		{
			"inet keeping its host bits", `'192.168.1.1/24'::inet`, pgtype.OIDInet,
			pgtype.AppendInet(nil, netip.MustParsePrefix("192.168.1.1/24")),
		},
		{
			"an IPv6 address", `'2001:db8::1'::inet`, pgtype.OIDInet,
			pgtype.AppendInet(nil, netip.MustParsePrefix("2001:db8::1/128")),
		},
		{
			// The server is the authority on this one: it reports family 6 for
			// a 4-in-6 address and answers false when it is compared with the
			// plain IPv4 spelling. Unmapping it on the way out would write a
			// different value than the one that was read.
			"an IPv4-mapped IPv6 address, which stays IPv6", `'::ffff:1.2.3.4'::inet`, pgtype.OIDInet,
			pgtype.AppendInet(nil, netip.PrefixFrom(netip.MustParseAddr("::ffff:1.2.3.4"), 128)),
		},
		{
			"cidr", `'10.0.0.0/8'::cidr`, pgtype.OIDCIDR,
			pgtype.AppendCIDR(nil, netip.MustParsePrefix("10.0.0.0/8")),
		},
		{
			// PostgreSQL considers numeric NaN equal to NaN, unlike a float.
			"numeric NaN", `'NaN'::numeric`, pgtype.OIDNumeric,
			pgtype.AppendNumeric(nil, pgtype.Numeric{Sign: pgtype.NumericNaN}),
		},
		{
			"numeric Infinity", `'Infinity'::numeric`, pgtype.OIDNumeric,
			pgtype.AppendNumeric(nil, pgtype.Numeric{Sign: pgtype.NumericPosInf}),
		},
		{
			"numeric -Infinity", `'-Infinity'::numeric`, pgtype.OIDNumeric,
			pgtype.AppendNumeric(nil, pgtype.Numeric{Sign: pgtype.NumericNegInf}),
		},
		{
			"uuid[]", `ARRAY['00000000-0000-0000-0000-000000000001']::uuid[]`, pgtype.OIDUUIDArray,
			pgtype.AppendUUIDArray(nil, [][16]byte{{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}}),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			expr := "SELECT $1 = " + tc.literal
			if tc.oid == pgtype.OIDJSON {
				expr = "SELECT $1::text = " + tc.literal
			}
			src := conn.Query(t.Context(), expr, []postgres.Param{{OID: tc.oid, Value: tc.param}},
				postgres.QueryConfig{})

			var equal, got bool
			src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
				for _, row := range b.Items {
					v, isNull := row.Value(0)
					if isNull {
						return false
					}
					equal, _ = pgtype.DecodeBool(v)
					got = true
				}
				return true
			})
			if err := src.Err(); err != nil {
				t.Fatalf("the server refused the value: %v", err)
			}
			if !got {
				t.Fatal("no row came back")
			}
			if !equal {
				t.Errorf("the server says these bytes are not %s", tc.literal)
			}
		})
	}
}

// And the other direction: what the server sends back must decode to the value
// the literal names.
func TestIntegrationCodecsDecodeWhatTheServerSends(t *testing.T) {
	conn := dial(t)

	src := conn.Query(t.Context(), `SELECT '{"a": 1}'::jsonb,
	                          '2024-03-01 12:34:56.789012'::timestamp,
	                          '1 year 2 mons 3 days 04:05:06.789'::interval,
	                          '192.168.1.1/24'::inet,
	                          ARRAY['00000000-0000-0000-0000-000000000001']::uuid[]`,
		nil, postgres.QueryConfig{})

	var seen bool
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		for _, row := range b.Items {
			seen = true

			// The OIDs the server declares must be the ones the codecs claim,
			// or every value after them is read as something it is not.
			wantOIDs := []uint32{
				pgtype.OIDJSONB, pgtype.OIDTimestamp, pgtype.OIDInterval,
				pgtype.OIDInet, pgtype.OIDUUIDArray,
			}
			for i, want := range wantOIDs {
				if got := row.Fields()[i].TypeOID; got != want {
					t.Errorf("column %d has OID %d, the codec expects %d", i, got, want)
				}
			}

			v, _ := row.Value(0)
			if j, err := pgtype.DecodeJSONB(v); err != nil || string(j) != `{"a": 1}` {
				t.Errorf("jsonb = %q, %v", j, err)
			}
			v, _ = row.Value(1)
			if ts, err := pgtype.DecodeTimestamp(v); err != nil ||
				!ts.Equal(time.Date(2024, 3, 1, 12, 34, 56, 789012000, time.UTC)) {
				t.Errorf("timestamp = %v, %v", ts, err)
			}
			v, _ = row.Value(2)
			if iv, err := pgtype.DecodeInterval(v); err != nil ||
				iv != (pgtype.Interval{Months: 14, Days: 3, Micros: 14_706_789_000}) {
				t.Errorf("interval = %v, %v", iv, err)
			}
			v, _ = row.Value(3)
			if p, err := pgtype.DecodeInet(v); err != nil || p.String() != "192.168.1.1/24" {
				t.Errorf("inet = %v, %v", p, err)
			}
			v, _ = row.Value(4)
			ids, err := pgtype.DecodeUUIDArray(nil, v)
			if err != nil || len(ids) != 1 || ids[0][15] != 1 {
				t.Errorf("uuid[] = %x, %v", ids, err)
			}
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("Err = %v", err)
	}
	if !seen {
		t.Fatal("no row came back")
	}
}

// A consumer that stops mid-result, against a server that is still sending.
//
// This is the path a script cannot judge. Stopping leaves rows already on
// their way — the server was asked for a batch and is delivering it — and the
// connection is only reusable if the client finds its way back to a known
// position through them. A scripted server hands out exactly what the script
// says and stops; a real one is mid-flight.
func TestIntegrationEarlyStopLeavesTheConnectionUsable(t *testing.T) {
	conn := dial(t)

	// Far more rows than the batch, so the stop lands with the portal
	// suspended and more still to come.
	src := conn.Query(t.Context(), "SELECT i FROM generate_series(1, 10000) AS i", nil,
		postgres.QueryConfig{BatchRows: 64})

	batches := 0
	src.Stream()(func(sluice.Batch[postgres.Row]) bool {
		batches++
		return batches < 2 // take one batch, then stop
	})
	if err := src.Err(); err != nil {
		t.Fatalf("Err = %v", err)
	}
	if batches != 2 {
		t.Fatalf("saw %d batches, want the stop to land on the second", batches)
	}
	if conn.Err() != nil {
		t.Fatalf("stopping early broke the connection: %v", conn.Err())
	}

	// The next query must get its own rows, not the tail of the abandoned one.
	// That confusion is the failure this resynchronisation exists to prevent,
	// and it would not look like an error — it would look like wrong data.
	var got int64
	next := conn.Query(t.Context(), "SELECT 4242::bigint", nil, postgres.QueryConfig{BatchRows: 4})
	next.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		for _, row := range b.Items {
			v, _ := row.Value(0)
			got, _ = pgtype.DecodeInt8(v)
		}
		return true
	})
	if err := next.Err(); err != nil {
		t.Fatalf("the connection was unusable after an early stop: %v", err)
	}
	if got != 4242 {
		t.Errorf("the next query returned %d, want 4242 — it read the abandoned result's tail", got)
	}
}

// The Executes sent ahead of delivery reach the portal after its end whenever
// the result is shorter than what was asked for, and the backend's answer is
// what makes that safe: no rows and a CommandComplete, never a second run. A
// script can only say what it is told; this asks the server, on the two
// shapes that suspend — a SELECT whose row count is an exact multiple of the
// batch, and an INSERT ... RETURNING, whose rows the backend holds in a store
// and which must have inserted once.
func TestIntegrationExecutesAheadReachAFinishedPortalHarmlessly(t *testing.T) {
	conn := dial(t)

	count := func(sql string) int64 {
		t.Helper()
		var n int64
		src := conn.Query(t.Context(), sql, nil, postgres.QueryConfig{BatchRows: 32})
		src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
			n += int64(b.Len())
			return true
		})
		if err := src.Err(); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return n
	}

	if n := count("SELECT i FROM generate_series(1, 64) AS i"); n != 64 {
		t.Errorf("exact multiple of the batch: %d rows, want 64", n)
	}

	if err := exec(t, conn, "CREATE TEMP TABLE sluice_ahead_test (id bigint)"); err != nil {
		t.Fatal(err)
	}
	if n := count("INSERT INTO sluice_ahead_test SELECT i FROM generate_series(1, 40) AS i RETURNING id"); n != 40 {
		t.Errorf("INSERT ... RETURNING delivered %d rows, want 40", n)
	}
	var stored int64
	src := conn.Query(t.Context(), "SELECT count(*) FROM sluice_ahead_test", nil, postgres.QueryConfig{BatchRows: 4})
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		v, _ := b.Items[0].Value(0)
		stored, _ = pgtype.DecodeInt8(v)
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatal(err)
	}
	if stored != 40 {
		t.Errorf("the table holds %d rows, want 40 — an Execute sent ahead ran the INSERT again", stored)
	}
}

// An error that arrives after rows have already been delivered, which is a
// different path from one that fails before the first row: the consumer has
// seen data, and the error has to reach it anyway.
//
// It is what [sluice.Source] exists for (§4.7) — consume, then check — and the
// case that makes the "then check" half load-bearing rather than a formality.
func TestIntegrationErrorAfterRowsHaveBeenDelivered(t *testing.T) {
	conn := dial(t)

	// The division fails on the two-hundredth row, well past the first batch.
	src := conn.Query(t.Context(),
		"SELECT i, 1 / (200 - i) FROM generate_series(1, 10000) AS i", nil,
		postgres.QueryConfig{BatchRows: 32})

	rows := 0
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		rows += len(b.Items)
		return true
	})

	var pgErr *postgres.Error
	if err := src.Err(); !errors.As(err, &pgErr) {
		t.Fatalf("Err = %v, want a *postgres.Error", err)
	}
	if pgErr.Code != "22012" {
		t.Errorf("SQLSTATE = %q, want 22012 (division_by_zero)", pgErr.Code)
	}
	if rows == 0 {
		t.Error("no rows were delivered before the error; the failure is meant to land mid-result")
	}
	// A statement failing is not a connection failing, and the difference is
	// the whole reason this connector distinguishes them.
	if conn.Err() != nil {
		t.Fatalf("a query error broke the connection: %v", conn.Err())
	}
	if err := exec(t, conn, "SELECT 1"); err != nil {
		t.Errorf("the connection was unusable after a mid-result error: %v", err)
	}
}

// The far end of the range, read back rather than sent. A server that returns
// a date this package silently shifts is the failure that has no error
// attached to it.
func TestIntegrationTimestampRangeDecodesBack(t *testing.T) {
	conn := dial(t)

	if err := exec(t, conn, "SET TimeZone = 'UTC'"); err != nil {
		t.Fatal(err)
	}
	src := conn.Query(t.Context(), `SELECT '3000-01-01 00:00:00'::timestamp,
	                          '1700-01-01 00:00:00'::timestamp,
	                          '294276-12-31 23:59:59.999999'::timestamp,
	                          '3000-01-01 00:00:00+00'::timestamptz`,
		nil, postgres.QueryConfig{})

	want := []time.Time{
		time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(1700, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(294276, 12, 31, 23, 59, 59, 999999000, time.UTC),
		time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	var seen bool
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		for _, row := range b.Items {
			seen = true
			for i, w := range want {
				v, _ := row.Value(i)
				var got time.Time
				var err error
				if i == 3 {
					got, err = pgtype.DecodeTimestampTZ(v)
				} else {
					got, err = pgtype.DecodeTimestamp(v)
				}
				if err != nil {
					t.Errorf("column %d: %v", i, err)
					continue
				}
				if !got.Equal(w) {
					t.Errorf("column %d = %v, want %v", i, got.UTC(), w)
				}
			}
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("Err = %v", err)
	}
	if !seen {
		t.Fatal("no row came back")
	}
}

// The two network spellings the server keeps apart, and the array numbering it
// allows — both read back from the server rather than asserted here.
func TestIntegrationInetAndArrayEdgeCases(t *testing.T) {
	conn := dial(t)

	t.Run("a 4-in-6 address is not its IPv4 spelling", func(t *testing.T) {
		src := conn.Query(t.Context(),
			`SELECT '::ffff:1.2.3.4'::inet, '1.2.3.4'::inet,
			        '::ffff:1.2.3.4'::inet = '1.2.3.4'::inet`,
			nil, postgres.QueryConfig{})

		var seen bool
		src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
			for _, row := range b.Items {
				seen = true
				v, _ := row.Value(0)
				mapped, err := pgtype.DecodeInet(v)
				if err != nil {
					t.Fatalf("decoding the mapped address: %v", err)
				}
				if !mapped.Addr().Is4In6() {
					t.Errorf("the mapped address came back as %v, want it still 4-in-6", mapped)
				}
				v, _ = row.Value(1)
				plain, err := pgtype.DecodeInet(v)
				if err != nil {
					t.Fatalf("decoding the plain address: %v", err)
				}
				if plain.Addr().Is4In6() {
					t.Errorf("the plain address came back as %v, want it still IPv4", plain)
				}
				// And re-encoding each must reproduce what the server sent,
				// which is the property that makes read-then-write safe.
				orig0, _ := row.Value(0)
				if got := pgtype.AppendInet(nil, mapped); !bytes.Equal(got, orig0) {
					t.Errorf("re-encoded the mapped address as %x, want %x", got, orig0)
				}
				v, _ = row.Value(2)
				equal, _ := pgtype.DecodeBool(v)
				if equal {
					t.Error("the server says the two spellings are equal; this test's premise is wrong")
				}
			}
			return true
		})
		if err := src.Err(); err != nil {
			t.Fatalf("Err = %v", err)
		}
		if !seen {
			t.Fatal("no row came back")
		}
	})

	t.Run("an array numbered from something other than one is refused", func(t *testing.T) {
		src := conn.Query(t.Context(), `SELECT '[5:7]={10,20,30}'::bigint[]`, nil, postgres.QueryConfig{})

		var decodeErr error
		src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
			for _, row := range b.Items {
				v, _ := row.Value(0)
				_, decodeErr = pgtype.DecodeInt8Array(nil, v)
			}
			return true
		})
		if err := src.Err(); err != nil {
			t.Fatalf("Err = %v", err)
		}
		if decodeErr == nil {
			t.Fatal("the decoder accepted an array the server numbers from 5")
		}
		// The ordinary array must still decode, or the check is a wall.
		var got []int64
		ok := conn.Query(t.Context(), `SELECT ARRAY[10,20,30]::bigint[]`, nil, postgres.QueryConfig{})
		ok.Stream()(func(b sluice.Batch[postgres.Row]) bool {
			for _, row := range b.Items {
				v, _ := row.Value(0)
				got, _ = pgtype.DecodeInt8Array(nil, v)
			}
			return true
		})
		if err := ok.Err(); err != nil {
			t.Fatalf("Err = %v", err)
		}
		if len(got) != 3 || got[0] != 10 {
			t.Errorf("the ordinary array decoded to %v, want [10 20 30]", got)
		}
	})
}

// This package's encoding against the server's own, byte for byte, with the
// server producing both sides of the comparison at run time.
//
// Every other vector in this repository is hex someone copied into a test, and
// two of those have been wrong. This one cannot be: it asks the running server
// for `numeric_send(<literal>)` and compares that to what AppendNumeric
// produces for the same literal. A value that is merely *accepted* — the same
// number in a shape the server would never send — fails here and nowhere else,
// which is exactly what ParseNumeric was doing to every value below 0.0001.
func TestIntegrationNumericMatchesTheServersEncoding(t *testing.T) {
	conn := dial(t)

	literals := []string{
		"0", "0.00", "1", "-1", "1.5", "10.00",
		"0.0001", "0.00001", "0.00010000",
		"0.000000000000000000001",
		"10000.0001", "100000000", "1.00000000",
		"12345678901234567890.12345678901234567890",
		"-99999999999999999999999999999999999999",
	}
	for _, lit := range literals {
		t.Run(lit, func(t *testing.T) {
			mine, err := pgtype.ParseNumeric(lit)
			if err != nil {
				t.Fatalf("ParseNumeric: %v", err)
			}

			// numeric_send is the function the server itself uses to put a
			// value on the wire, so this is the encoder judging the encoder.
			src := conn.Query(t.Context(), "SELECT numeric_send($1::text::numeric)",
				[]postgres.Param{{OID: pgtype.OIDText, Value: pgtype.AppendText(nil, lit)}},
				postgres.QueryConfig{})

			var theirs []byte
			src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
				for _, row := range b.Items {
					v, isNull := row.Value(0)
					if !isNull {
						theirs = slices.Clone(v) // the batch's storage is reused
					}
				}
				return true
			})
			if err := src.Err(); err != nil {
				t.Fatalf("Err = %v", err)
			}
			if theirs == nil {
				t.Fatal("the server returned nothing")
			}

			if got := pgtype.AppendNumeric(nil, mine); !bytes.Equal(got, theirs) {
				t.Errorf("AppendNumeric(%s) = %x,\n           the server sends %x", lit, got, theirs)
			}
			// And the server's bytes must decode to the same value, so the two
			// encodings are one shape rather than two that agree by accident.
			back, err := pgtype.DecodeNumeric(theirs)
			if err != nil {
				t.Fatalf("DecodeNumeric on the server's own bytes: %v", err)
			}
			if back.String() != mine.String() {
				t.Errorf("the server's bytes render %q, this package's %q", back.String(), mine.String())
			}
		})
	}
}

// Every codec against the server's own encoder, byte for byte, with both sides
// produced at run time.
//
// This is the check the whole package should have had first. `<type>send` is
// the function PostgreSQL itself calls to put a value on the wire, so
// comparing against it is the encoder judging the encoder — and unlike a hex
// vector copied into a test, it cannot be mistyped and cannot go stale against
// a newer server.
//
// It catches the failure no other test here can see: a value the server
// *accepts* but would never *send*. That is not a wrong number, so a round trip
// through the server returns it unchanged and every assertion about the value
// passes. It is a wrong shape, and shape is bytes.
func TestIntegrationEveryCodecMatchesTheServersEncoding(t *testing.T) {
	conn := dial(t)

	// The session zone is pinned so a literal without an offset means one
	// thing. It is set for timestamptz's benefit and costs the others nothing.
	if err := exec(t, conn, "SET TimeZone = 'UTC'"); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		expr string // SQL producing the value, whose <type>send output is the oracle
		mine []byte
	}{
		{"bool true", `true`, pgtype.AppendBool(nil, true)},
		{"bool false", `false`, pgtype.AppendBool(nil, false)},
		{"int2", `(-12345)::smallint`, pgtype.AppendInt2(nil, -12345)},
		{"int4", `(-1234567)::int`, pgtype.AppendInt4(nil, -1234567)},
		{"int8", `(-1234567890123)::bigint`, pgtype.AppendInt8(nil, -1234567890123)},
		{"float4", `(1.5)::real`, pgtype.AppendFloat4(nil, 1.5)},
		{"float8", `(-0.125)::double precision`, pgtype.AppendFloat8(nil, -0.125)},
		{"float8 infinity", `('Infinity')::double precision`, pgtype.AppendFloat8(nil, math.Inf(1))},
		{"text", `'héllo wörld'::text`, pgtype.AppendText(nil, "héllo wörld")},
		{"text empty", `''::text`, pgtype.AppendText(nil, "")},
		{"bytea", `'\xdeadbeef'::bytea`, pgtype.AppendBytea(nil, []byte{0xde, 0xad, 0xbe, 0xef})},
		{
			"date", `'2024-03-01'::date`,
			pgtype.AppendDate(nil, time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)),
		},
		{
			"date before the epoch", `'1970-01-01'::date`,
			pgtype.AppendDate(nil, time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)),
		},
		{
			"timestamp", `'2024-03-01 12:34:56.789012'::timestamp`,
			pgtype.AppendTimestamp(nil, time.Date(2024, 3, 1, 12, 34, 56, 789012000, time.UTC)),
		},
		{
			"timestamptz", `'2024-03-01 12:34:56.789012+00'::timestamptz`,
			pgtype.AppendTimestampTZ(nil, time.Date(2024, 3, 1, 12, 34, 56, 789012000, time.UTC)),
		},
		{
			"uuid", `'0102030405060708090a0b0c0d0e0f10'::uuid`,
			pgtype.AppendUUID(nil, [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
		},
		{"jsonb", `'{"a": 1}'::jsonb`, pgtype.AppendJSONB(nil, []byte(`{"a": 1}`))},
		{"json", `'{"a":1}'::json`, pgtype.AppendJSON(nil, []byte(`{"a":1}`))},
		{
			"interval", `'1 year 2 mons 3 days 04:05:06.789'::interval`,
			pgtype.AppendInterval(nil, pgtype.Interval{Months: 14, Days: 3, Micros: 14_706_789_000}),
		},
		{
			"inet", `'192.168.1.1/24'::inet`,
			pgtype.AppendInet(nil, netip.MustParsePrefix("192.168.1.1/24")),
		},
		{
			"inet v6", `'2001:db8::1'::inet`,
			pgtype.AppendInet(nil, netip.MustParsePrefix("2001:db8::1/128")),
		},
		{
			"cidr", `'10.0.0.0/8'::cidr`,
			pgtype.AppendCIDR(nil, netip.MustParsePrefix("10.0.0.0/8")),
		},
		{
			"int4[]", `ARRAY[1,-2,3]::int[]`,
			pgtype.AppendInt4Array(nil, []int32{1, -2, 3}),
		},
		{
			"int8[]", `ARRAY[1,-2,3]::bigint[]`,
			pgtype.AppendInt8Array(nil, []int64{1, -2, 3}),
		},
		{
			"text[]", `ARRAY['a','','ccc']::text[]`,
			pgtype.AppendTextArray(nil, []string{"a", "", "ccc"}),
		},
		{
			"uuid[]", `ARRAY['00000000-0000-0000-0000-000000000001']::uuid[]`,
			pgtype.AppendUUIDArray(nil, [][16]byte{{15: 1}}),
		},
		{
			"empty int8[]", `ARRAY[]::bigint[]`,
			pgtype.AppendInt8Array(nil, nil),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			theirs := serverEncoding(t, conn, tc.expr)
			if !bytes.Equal(tc.mine, theirs) {
				t.Errorf("this package encodes %x\n          the server %x", tc.mine, theirs)
			}
		})
	}
}

// serverEncoding asks the server for the wire bytes of an expression, through
// the same send function it would use to answer a query.
//
// The function is looked up in `pg_type` rather than derived from the type
// name. Most are `<typname>send` and several are not — `uuid_send`,
// `inet_send`, `timestamp_send`, `array_send` for every array — and a table of
// those written by hand is a table of guesses, which is the mistake this whole
// test exists to stop making. The catalog knows.
func serverEncoding(t *testing.T, conn *postgres.Conn, expr string) []byte {
	t.Helper()

	send := queryOne(t, conn, "SELECT typsend::text FROM pg_type WHERE oid = pg_typeof("+expr+")::oid",
		func(row postgres.Row) string {
			v, _ := row.Value(0)
			return pgtype.DecodeText(v)
		})
	if send == "" || send == "-" {
		t.Fatalf("no send function for the type of %s", expr)
	}

	return queryOne(t, conn, "SELECT "+send+"("+expr+")", func(row postgres.Row) []byte {
		v, isNull := row.Value(0)
		if isNull {
			t.Fatalf("%s(%s) is NULL", send, expr)
		}
		// Copied, not borrowed: the batch's storage is reused. An empty
		// result is a legitimate answer — textsend('') is zero bytes — so it
		// must not be confused with no answer, which is why the row count is
		// what this checks rather than the length.
		return append([]byte{}, v...)
	})
}

// queryOne runs a query expected to return exactly one row and maps it.
func queryOne[T any](t *testing.T, conn *postgres.Conn, sql string, f func(postgres.Row) T) T {
	t.Helper()

	var out T
	rows := 0
	src := conn.Query(context.Background(), sql, nil, postgres.QueryConfig{BatchRows: 4})
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		for _, row := range b.Items {
			out = f(row)
			rows++
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if rows != 1 {
		t.Fatalf("%s returned %d rows, want 1", sql, rows)
	}
	return out
}

// The package's headline query shape with nothing to match: `= ANY($1)` over an
// empty array. It is what a batch that filtered down to nothing produces, so it
// is not an edge case — it is Tuesday.
func TestIntegrationAnyOverAnEmptyArray(t *testing.T) {
	conn := dial(t)

	src := conn.Query(t.Context(), "SELECT k FROM unnest($1::bigint[]) AS k WHERE k = ANY($1)",
		[]postgres.Param{{OID: pgtype.OIDInt8Array, Value: pgtype.AppendInt8Array(nil, nil)}},
		postgres.QueryConfig{BatchRows: 16})

	rows := 0
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool { rows += len(b.Items); return true })
	if err := src.Err(); err != nil {
		t.Fatalf("the server refused an empty array: %v", err)
	}
	if rows != 0 {
		t.Errorf("got %d rows for an empty key list, want none", rows)
	}
	if conn.Err() != nil {
		t.Fatalf("the connection broke: %v", conn.Err())
	}
}

// Binary COPY carrying the types whose framing is not fixed-width.
//
// The existing COPY test moves bigints and text, which is the shape a bulk load
// usually has and the shape least able to be wrong: an eight-byte field and a
// length-prefixed string. This one moves numeric, timestamp, jsonb, inet, uuid
// and an array — every type whose field length is computed rather than known —
// and reads them back through the connector's own decoders.
//
// The server is the check. A field length off by one turns the next field into
// nonsense and the COPY into an error, so this either round-trips exactly or
// fails loudly; there is no quiet middle.
func TestIntegrationCopyVariableWidthTypes(t *testing.T) {
	conn := dial(t)

	const ddl = `CREATE TEMP TABLE sluice_copy_wide (
		id bigint, amount numeric, at timestamp, doc jsonb,
		addr inet, ref uuid, tags bigint[], note text)`
	if err := exec(t, conn, ddl); err != nil {
		t.Fatalf("creating the temp table: %v", err)
	}

	amount := mustParseNumeric("12345678901234567890.12345678901234567890")
	at := time.Date(2024, 3, 1, 12, 34, 56, 789012000, time.UTC)
	ref := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	cp, err := conn.CopyFrom(t.Context(),
		"COPY sluice_copy_wide FROM STDIN WITH (FORMAT BINARY)",
		postgres.CopyConfig{RowsPerTx: 8})
	if err != nil {
		t.Fatalf("CopyFrom: %v", err)
	}

	const rows = 20
	var buf []byte
	for i := range rows {
		buf = buf[:0]
		buf = postgres.AppendTupleHeader(buf, 8)
		buf = postgres.AppendField(buf, pgtype.AppendInt8(nil, int64(i)))
		buf = postgres.AppendField(buf, pgtype.AppendNumeric(nil, amount))
		buf = postgres.AppendField(buf, pgtype.AppendTimestamp(nil, at))
		buf = postgres.AppendField(buf, pgtype.AppendJSONB(nil, []byte(`{"i": 1}`)))
		buf = postgres.AppendField(buf, pgtype.AppendInet(nil, netip.MustParsePrefix("192.168.1.1/24")))
		buf = postgres.AppendField(buf, pgtype.AppendUUID(nil, ref))
		buf = postgres.AppendField(buf, pgtype.AppendInt8Array(nil, []int64{int64(i), -1}))
		// Every other row leaves the last column NULL, so the marker travels
		// beside real values rather than only on its own.
		if i%2 == 0 {
			buf = postgres.AppendField(buf, pgtype.AppendText(nil, "héllo"))
		} else {
			buf = postgres.AppendFieldNull(buf)
		}
		if err := cp.WriteTuples(buf, 1); err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
	}
	if err := cp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Read it all back and check every column of every row.
	src := conn.Query(t.Context(),
		"SELECT id, amount, at, doc, addr, ref, tags, note FROM sluice_copy_wide ORDER BY id",
		nil, postgres.QueryConfig{BatchRows: 8})

	seen := 0
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		for _, row := range b.Items {
			i := seen
			seen++

			v, _ := row.Value(1)
			gotAmount, err := pgtype.DecodeNumeric(v)
			if err != nil || gotAmount.String() != amount.String() {
				t.Errorf("row %d amount = %v (%v), want %v", i, gotAmount, err, amount)
			}
			v, _ = row.Value(2)
			gotAt, err := pgtype.DecodeTimestamp(v)
			if err != nil || !gotAt.Equal(at) {
				t.Errorf("row %d at = %v (%v), want %v", i, gotAt, err, at)
			}
			v, _ = row.Value(3)
			doc, err := pgtype.DecodeJSONB(v)
			if err != nil || string(doc) != `{"i": 1}` {
				t.Errorf("row %d doc = %q (%v)", i, doc, err)
			}
			v, _ = row.Value(4)
			addr, err := pgtype.DecodeInet(v)
			if err != nil || addr.String() != "192.168.1.1/24" {
				t.Errorf("row %d addr = %v (%v)", i, addr, err)
			}
			v, _ = row.Value(5)
			gotRef, err := pgtype.DecodeUUID(v)
			if err != nil || gotRef != ref {
				t.Errorf("row %d ref = %x (%v)", i, gotRef, err)
			}
			v, _ = row.Value(6)
			tags, err := pgtype.DecodeInt8Array(nil, v)
			if err != nil || len(tags) != 2 || tags[0] != int64(i) || tags[1] != -1 {
				t.Errorf("row %d tags = %v (%v)", i, tags, err)
			}
			v, isNull := row.Value(7)
			switch {
			case i%2 == 0 && (isNull || pgtype.DecodeText(v) != "héllo"):
				t.Errorf("row %d note = %q, null=%v; want héllo", i, v, isNull)
			case i%2 == 1 && !isNull:
				// A NULL that came back as an empty string would be the
				// merge this package refuses everywhere else.
				t.Errorf("row %d note came back as %q, want NULL", i, v)
			}
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("Err = %v", err)
	}
	if seen != rows {
		t.Errorf("read back %d rows, wrote %d", seen, rows)
	}
}

func mustParseNumeric(s string) pgtype.Numeric {
	n, err := pgtype.ParseNumeric(s)
	if err != nil {
		panic(err)
	}
	return n
}

// Connect's context bounds the handshake and nothing after it. Until
// 2026-10-15 the handshake was bounded by a SetDeadline the caller set and
// had to lift, and every program that copied the guide without lifting it
// lost its connection to an i/o timeout at the 30 s mark. Connect sets no
// deadline on the socket, so a query run after its context has expired is
// bounded by its own context alone.
func TestIntegrationConnectLeavesNoDeadline(t *testing.T) {
	addr, cfg := pgTarget(t)

	const handshake = time.Second
	ctx, cancel := context.WithTimeout(t.Context(), handshake)
	defer cancel()
	conn, err := postgres.Connect(ctx, postgres.DialTCP(addr), cfg)
	if err != nil {
		t.Fatalf("connecting to %s as %s: %v", addr, cfg.User, err)
	}
	defer func() { _ = conn.Close() }()

	// Outlive the handshake's context, then ask for something.
	time.Sleep(handshake + handshake/2)
	src := conn.Query(t.Context(), "SELECT 1", nil, postgres.QueryConfig{})
	rows := 0
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		rows += len(b.Items)
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("a query after the handshake deadline: %v", err)
	}
	if rows != 1 {
		t.Errorf("SELECT 1 returned %d rows", rows)
	}
	if err := conn.Err(); err != nil {
		t.Errorf("the connection is broken after the query: %v", err)
	}
}

// A context that ends while the server is still executing the statement
// reaches the server through the recipe Connect keeps: pg_sleep(5) under a
// 300 ms deadline ends well inside a second, with the deadline as the error
// and the server's 57014 behind it, and the connection serves the next
// query. Only a real server can prove this one — it is the server's
// cancellation handling that the scripted tests cannot disagree with.
func TestIntegrationCancelInterruptsTheRunningStatement(t *testing.T) {
	cancelInterrupts(t, dialCancellable(t, postgres.Protocol30))
}

// Protocol 3.2 against the server in front of it. Before 18 the server
// negotiates down and the key stays four bytes — checked, then the rest is
// skipped by name. From 18 on the secret is longer, and the CancelRequest
// that carries it whole is the one that interrupts pg_sleep.
func TestIntegrationProtocol32LongCancelKey(t *testing.T) {
	conn := dialCancellable(t, postgres.Protocol32)
	secret := len(conn.CancelKey().Secret)
	version := conn.Parameter("server_version")
	major, _, _ := strings.Cut(version, ".")
	if n, err := strconv.Atoi(major); err == nil && n < 18 {
		if secret != 4 {
			t.Fatalf("PostgreSQL %s negotiated down from 3.2, and the secret is %d bytes, want 4", version, secret)
		}
		t.Skipf("PostgreSQL %s speaks protocol 3.0: the long key needs 18", version)
	}
	if secret <= 4 {
		t.Fatalf("PostgreSQL %s under protocol 3.2 sent a %d-byte secret, want more than 4", version, secret)
	}
	t.Logf("PostgreSQL %s sent a %d-byte secret", version, secret)
	cancelInterrupts(t, conn)
}

// The default asks for 3.2 and Protocol reports what the server settled on:
// 3.0 from a server before 18, which negotiates down, 3.2 from 18 on. CI runs
// both majors; the container here has one of them.
func TestIntegrationProtocolNegotiated(t *testing.T) {
	addr, cfg := pgTarget(t)
	conn := dialWith(t, addr, cfg)
	version := conn.Parameter("server_version")
	want := postgres.Protocol32
	if serverMajor(t, version) < 18 {
		want = postgres.Protocol30
	}
	if got := conn.Protocol(); got != want {
		t.Fatalf("PostgreSQL %s: Protocol() = %v, want %v", version, got, want)
	}
	cfg.MaxProtocol = postgres.Protocol30
	if got := dialWith(t, addr, cfg).Protocol(); got != postgres.Protocol30 {
		t.Errorf("PostgreSQL %s, MaxProtocol 3.0: Protocol() = %v, want 3.0", version, got)
	}
}

// MinProtocol 3.2 refuses a server before 18 with the named error, and is
// satisfied from 18 on.
func TestIntegrationMinProtocolRefusesAnOlderServer(t *testing.T) {
	addr, cfg := pgTarget(t)
	version := dialWith(t, addr, cfg).Parameter("server_version")
	cfg.MinProtocol = postgres.Protocol32
	if serverMajor(t, version) >= 18 {
		if got := dialWith(t, addr, cfg).Protocol(); got != postgres.Protocol32 {
			t.Fatalf("PostgreSQL %s, MinProtocol 3.2: Protocol() = %v, want 3.2", version, got)
		}
		return
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	conn, err := postgres.Connect(ctx, postgres.DialTCP(addr), cfg)
	if err == nil {
		_ = conn.Close()
		t.Fatalf("PostgreSQL %s, MinProtocol 3.2: Connect succeeded under %v", version, conn.Protocol())
	}
	if !errors.Is(err, postgres.ErrProtocolVersion) {
		t.Fatalf("PostgreSQL %s, MinProtocol 3.2: Connect returned %v, want ErrProtocolVersion", version, err)
	}
	t.Logf("PostgreSQL %s refused: %v", version, err)
}

// serverMajor reads the major version out of server_version, "17.2" or
// "18beta1".
func serverMajor(t *testing.T, version string) int {
	t.Helper()
	end := 0
	for end < len(version) && version[end] >= '0' && version[end] <= '9' {
		end++
	}
	n, err := strconv.Atoi(version[:end])
	if err != nil {
		t.Fatalf("server_version %q has no major version", version)
	}
	return n
}

// A protocol option the server does not know comes back in
// NegotiateProtocolVersion, and the connection goes on without it.
func TestIntegrationDeclinedProtocolOption(t *testing.T) {
	addr, cfg := pgTarget(t)
	cfg.Params = map[string]string{"_pq_.sluice_unknown": "on"}
	_ = dialWith(t, addr, cfg)
}

// dialCancellable opens a connection asking for protocol version; the
// recipe it was opened with is the one each cancellation dials.
func dialCancellable(t *testing.T, version postgres.ProtocolVersion) *postgres.Conn {
	t.Helper()
	addr, cfg := pgTarget(t)
	cfg.MaxProtocol = version
	conn := dialWith(t, addr, cfg)
	if !conn.CancelKey().Known() {
		t.Fatal("the server sent no cancellation key")
	}
	return conn
}

// cancelInterrupts runs pg_sleep(5) under a 300 ms deadline on conn and
// checks the server cut it short, then that conn serves the next query.
func cancelInterrupts(t *testing.T, conn *postgres.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	src := conn.Query(ctx, "SELECT pg_sleep(5)", nil, postgres.QueryConfig{})
	src.Stream()(func(sluice.Batch[postgres.Row]) bool { return true })
	took := time.Since(start)
	err := src.Err()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pg_sleep(5) under a 300ms deadline ended after %v with %v, want context.DeadlineExceeded", took, err)
	}
	var pgErr *postgres.Error
	if !errors.As(err, &pgErr) || pgErr.Code != "57014" {
		t.Errorf("Err = %v, want the server's 57014 reachable through errors.As", err)
	}
	if took > time.Second {
		t.Errorf("the cancellation took %v to end the statement, want well under a second", took)
	}
	t.Logf("pg_sleep(5) cancelled after %v", took)

	src = conn.Query(t.Context(), "SELECT 1", nil, postgres.QueryConfig{})
	rows := 0
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		rows += len(b.Items)
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("the query after the cancellation: %v", err)
	}
	if rows != 1 {
		t.Errorf("SELECT 1 after the cancellation returned %d rows", rows)
	}
	if err := conn.Err(); err != nil {
		t.Errorf("the connection is broken after the cancellation: %v", err)
	}
}

// Exec waits on the server the way Query does, and the same deadline reaches
// it through the same recipe.
func TestIntegrationCancelInterruptsExec(t *testing.T) {
	conn := dialCancellable(t, postgres.Protocol30)
	cancelEnds(t, conn, func(ctx context.Context) error {
		return conn.Exec(ctx, "SELECT pg_sleep(5)")
	})
}

// A COPY's commit is where the server runs what the rows set off: here a
// trigger sleeping five seconds per row. The deadline reaches it, the
// transaction is rolled back, and the connection serves the next query.
func TestIntegrationCancelInterruptsCopy(t *testing.T) {
	conn := dialCancellable(t, postgres.Protocol30)
	for _, sql := range []string{
		"CREATE TEMP TABLE sluice_cancel_copy (id bigint)",
		"CREATE FUNCTION pg_temp.sluice_slow_row() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(5); RETURN NEW; END $$",
		"CREATE TRIGGER sluice_slow BEFORE INSERT ON sluice_cancel_copy FOR EACH ROW EXECUTE FUNCTION pg_temp.sluice_slow_row()",
	} {
		if err := exec(t, conn, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	cancelEnds(t, conn, func(ctx context.Context) error {
		cp, err := conn.CopyFrom(ctx, "COPY sluice_cancel_copy (id) FROM STDIN WITH (FORMAT BINARY)",
			postgres.CopyConfig{RowsPerTx: 1})
		if err != nil {
			return err
		}
		row := postgres.AppendField(postgres.AppendTupleHeader(nil, 1), pgtype.AppendInt8(nil, 1))
		return cp.WriteTuples(row, 1)
	})
}

// A pipeline runs its parts without a pause the client could stop at, so
// the cancellation is what ends a part that sleeps.
func TestIntegrationCancelInterruptsPipeline(t *testing.T) {
	conn := dialCancellable(t, postgres.Protocol30)
	cancelEnds(t, conn, func(ctx context.Context) error {
		tx, err := conn.Begin(t.Context(), postgres.TxConfig{})
		if err != nil {
			return err
		}
		defer tx.Rollback() //nolint:errcheck // the pipeline's error is the one checked
		return tx.Pipeline(ctx, []postgres.Part{{SQL: "SELECT 1"}, {SQL: "SELECT pg_sleep(5)"}, {SQL: "SELECT 2"}},
			postgres.QueryConfig{}, func(int, sluice.Batch[postgres.Row]) bool { return true })
	})
}

// cancelEnds runs a call that waits five seconds on the server under a
// 300 ms deadline, checks the server cut it short, then that conn serves
// the next query.
func cancelEnds(t *testing.T, conn *postgres.Conn, run func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := run(ctx)
	took := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ended after %v with %v, want context.DeadlineExceeded", took, err)
	}
	var pgErr *postgres.Error
	if !errors.As(err, &pgErr) || pgErr.Code != "57014" {
		t.Errorf("err = %v, want the server's 57014 reachable through errors.As", err)
	}
	if took > time.Second {
		t.Errorf("the cancellation took %v to end the call, want well under a second", took)
	}
	t.Logf("cancelled after %v", took)
	if err := exec(t, conn, "SELECT 1"); err != nil {
		t.Fatalf("the statement after the cancellation: %v", err)
	}
}

// A SET client_encoding is announced by the server mid-session. Unasked for,
// it ends the connection, since every text decoder here reads UTF-8; with the
// encoding chosen at startup, the caller decodes and the connection stays.
func TestIntegrationAnUnaskedEncodingBreaksTheConnection(t *testing.T) {
	conn := dial(t)
	if err := exec(t, conn, "SET client_encoding = 'LATIN1'"); !errors.Is(err, postgres.ErrConnBroken) {
		t.Fatalf("SET client_encoding = 'LATIN1' returned %v, want ErrConnBroken", err)
	}
	if !errors.Is(conn.Err(), postgres.ErrConnBroken) {
		t.Errorf("conn.Err() = %v after the encoding changed, want ErrConnBroken", conn.Err())
	}

	addr, cfg := pgTarget(t)
	cfg.Params = map[string]string{"client_encoding": "UTF8"}
	conn = dialWith(t, addr, cfg)
	if err := exec(t, conn, "SET client_encoding = 'LATIN1'"); err != nil {
		t.Fatalf("SET client_encoding with the encoding chosen at startup = %v", err)
	}
	if err := exec(t, conn, "SELECT 1"); err != nil {
		t.Errorf("the connection was unusable after a chosen encoding change: %v", err)
	}
	if got := conn.Parameter("client_encoding"); got != "LATIN1" {
		t.Errorf("Parameter(client_encoding) = %q, want LATIN1", got)
	}
}

// ExecParams against a real server: the tag of each command is the server's,
// RowsAffected reads its count, and a statement without one says so. A
// temporary table, so no other suite's table is touched.
func TestIntegrationExecParamsRowsAffected(t *testing.T) {
	conn := dial(t)
	ctx := t.Context()
	if err := exec(t, conn, "CREATE TEMP TABLE sluice_exec_params (x int8, v text)"); err != nil {
		t.Fatal(err)
	}

	tag, err := conn.ExecParams(ctx, "INSERT INTO sluice_exec_params SELECT $1, 'a' FROM generate_series(1, 3)",
		[]postgres.Param{{OID: pgtype.OIDInt8, Value: pgtype.AppendInt8(nil, 7)}})
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := tag.RowsAffected(); tag != "INSERT 0 3" || n != 3 || !ok {
		t.Errorf("INSERT: tag %q, RowsAffected() = %d, %v; want INSERT 0 3, 3, true", tag, n, ok)
	}

	tx, err := conn.Begin(ctx, postgres.TxConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	tag, err = tx.ExecParams(ctx, "UPDATE sluice_exec_params SET v = $2 WHERE x = $1",
		[]postgres.Param{{OID: pgtype.OIDInt8, Value: pgtype.AppendInt8(nil, 7)}, {OID: pgtype.OIDText, Value: pgtype.AppendText(nil, "b'; DROP TABLE x; --")}})
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := tag.RowsAffected(); tag != "UPDATE 3" || n != 3 || !ok {
		t.Errorf("UPDATE: tag %q, RowsAffected() = %d, %v; want UPDATE 3, 3, true", tag, n, ok)
	}
	tag, err = tx.ExecParams(ctx, "DELETE FROM sluice_exec_params WHERE x = $1",
		[]postgres.Param{{OID: pgtype.OIDInt8, Value: pgtype.AppendInt8(nil, 8)}})
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := tag.RowsAffected(); tag != "DELETE 0" || n != 0 || !ok {
		t.Errorf("DELETE: tag %q, RowsAffected() = %d, %v; want DELETE 0, 0, true", tag, n, ok)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	tag, err = conn.ExecParams(ctx, "SET application_name = 'sluice_exec_params'", nil)
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := tag.RowsAffected(); tag != "SET" || ok {
		t.Errorf("SET: tag %q, RowsAffected() = %d, %v; want SET and no count", tag, n, ok)
	}
}

// An UPDATE ... RETURNING read as a stream, in batches smaller than its
// result: the tag reaches QueryConfig.Tag and counts the rows delivered.
func TestIntegrationQueryTagOfAStreamedUpdate(t *testing.T) {
	conn := dial(t)
	ctx := t.Context()
	if err := exec(t, conn, "CREATE TEMP TABLE sluice_query_tag AS SELECT i AS x FROM generate_series(1, 5) AS i"); err != nil {
		t.Fatal(err)
	}

	var tag postgres.CommandTag
	src := conn.Query(ctx, "UPDATE sluice_query_tag SET x = x + $1 RETURNING x",
		[]postgres.Param{{OID: pgtype.OIDInt8, Value: pgtype.AppendInt8(nil, 10)}},
		postgres.QueryConfig{BatchRows: 2, Tag: &tag})
	var rows int64
	for b := range src.Stream() {
		rows += int64(len(b.Items))
	}
	if err := src.Err(); err != nil {
		t.Fatal(err)
	}
	if n, ok := tag.RowsAffected(); tag != "UPDATE 5" || n != rows || !ok {
		t.Errorf("tag %q, RowsAffected() = %d, %v; want UPDATE 5 and the %d rows delivered", tag, n, ok, rows)
	}

	// A SELECT in batches: the server's tag counts the last one, here too.
	src = conn.Query(ctx, "SELECT x FROM sluice_query_tag", nil,
		postgres.QueryConfig{BatchRows: 2, Fetch: postgres.FetchBatches, Tag: &tag})
	src.Stream()(func(sluice.Batch[postgres.Row]) bool { return true })
	if err := src.Err(); err != nil {
		t.Fatal(err)
	}
	if tag != "SELECT 5" {
		t.Errorf("tag %q, want SELECT 5", tag)
	}
}

// copyRows writes n one-column rows numbered from first, a thousand per
// batch, and returns the first error — on its own goroutine, so a copy that
// deadlocks is reported by the caller's timer rather than by the test
// binary's.
func copyRows(cp *postgres.Copier, first, n int) <-chan error {
	done := make(chan error, 1)
	go func() {
		var buf []byte
		for i := 0; i < n; {
			buf = buf[:0]
			batch := min(1000, n-i)
			for j := range batch {
				buf = postgres.AppendTupleHeader(buf, 1)
				buf = postgres.AppendField(buf, pgtype.AppendInt8(nil, int64(first+i+j)))
			}
			if err := cp.WriteTuples(buf, batch); err != nil {
				done <- err
				return
			}
			i += batch
		}
		done <- nil
	}()
	return done
}

// A trigger raising a NOTICE per row makes the server write while the client
// writes. Read only at CopyDone, the notices filled the socket, the server
// blocked sending one and stopped reading CopyData, and the client blocked
// sending the next: on PostgreSQL 17.11 the load stopped after about 100 000
// rows, for good.
func TestIntegrationCopyWithANoticePerRow(t *testing.T) {
	conn := dial(t)
	for _, sql := range []string{
		"CREATE TEMP TABLE sluice_copy_notice (id bigint)",
		"CREATE FUNCTION pg_temp.sluice_notice_row() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'row %', NEW.id; RETURN NEW; END $$",
		"CREATE TRIGGER sluice_notice BEFORE INSERT ON sluice_copy_notice FOR EACH ROW EXECUTE FUNCTION pg_temp.sluice_notice_row()",
	} {
		if err := exec(t, conn, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	cp, err := conn.CopyFrom(t.Context(), "COPY sluice_copy_notice (id) FROM STDIN WITH (FORMAT BINARY)",
		postgres.CopyConfig{RowsPerTx: 250_000})
	if err != nil {
		t.Fatal(err)
	}
	const rows = 1_000_000
	select {
	case err := <-copyRows(cp, 0, rows):
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Minute):
		t.Fatal("the copy did not finish within 3 minutes: client and server are each waiting for the other")
	}
	if err := cp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := countRows(t, conn, "sluice_copy_notice"); got != rows {
		t.Errorf("the table holds %d rows, want %d", got, rows)
	}
}

// A refused row ends the COPY on the server at once; the client learns it
// from a WriteTuples, not from the commit RowsPerTx rows later.
func TestIntegrationCopyRefusedRowFailsAWrite(t *testing.T) {
	conn := dial(t)
	if err := exec(t, conn, "CREATE TEMP TABLE sluice_copy_refused (id bigint PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	cp, err := conn.CopyFrom(t.Context(), "COPY sluice_copy_refused (id) FROM STDIN WITH (FORMAT BINARY)",
		postgres.CopyConfig{RowsPerTx: 100_000_000})
	if err != nil {
		t.Fatal(err)
	}
	// Two batches of the same ids: the second one's first row is refused.
	if err := <-copyRows(cp, 0, 1000); err != nil {
		t.Fatal(err)
	}
	var writeErr error
	select {
	case writeErr = <-copyRows(cp, 0, 50_000_000):
	case <-time.After(time.Minute):
		t.Fatal("the refusal never reached a WriteTuples")
	}
	var pgErr *postgres.Error
	if !errors.As(writeErr, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("WriteTuples returned %v, want the server's 23505 before any commit", writeErr)
	}
	if conn.Err() != nil {
		t.Fatalf("the refusal broke the connection: %v", conn.Err())
	}
	if got := countRows(t, conn, "sluice_copy_refused"); got != 0 {
		t.Errorf("the table holds %d rows after a refused transaction, want 0", got)
	}
}

// countRows reads count(*) of a table.
func countRows(t *testing.T, conn *postgres.Conn, table string) int64 {
	t.Helper()
	src := conn.Query(t.Context(), "SELECT count(*) FROM "+table, nil, postgres.QueryConfig{})
	var n int64
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		for _, row := range b.Items {
			v, _ := row.Value(0)
			n, _ = pgtype.DecodeInt8(v)
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("counting %s: %v", table, err)
	}
	return n
}

// The early stop of a result asked for whole, against the real server: ten
// million rows, stopped at the first batch. A read the package may cancel
// comes back once the server has cancelled it; the same read behind a WITH,
// which it must not cancel, drains every row. What the stop costs is the time
// from the first batch to the return — the server builds generate_series
// whole before its first row, the same for both — and the cancelled one is
// bounded in absolute terms and against the drain. The connection then serves
// what follows: a statement that sleeps a little, so that a cancellation
// landing late would cut it.
func TestIntegrationFetchAllEarlyStopCancelsTheRest(t *testing.T) {
	conn := dialCancellable(t, postgres.Protocol30)
	stop := func(sql string) time.Duration {
		t.Helper()
		src := conn.Query(t.Context(), sql, nil, postgres.QueryConfig{Fetch: postgres.FetchAll, BatchRows: 1024})
		batches := 0
		var first time.Time
		src.Stream()(func(sluice.Batch[postgres.Row]) bool { batches++; first = time.Now(); return false })
		took := time.Since(first)
		if err := src.Err(); err != nil {
			t.Fatalf("%s stopped at the first batch: %v", sql, err)
		}
		if batches != 1 {
			t.Fatalf("%s delivered %d batches, want 1", sql, batches)
		}
		if err := conn.Err(); err != nil {
			t.Fatalf("the early stop broke the connection: %v", err)
		}
		return took
	}
	cancelled := stop("SELECT i FROM generate_series(1, 10000000) AS i")
	drained := stop("WITH s AS (SELECT i FROM generate_series(1, 10000000) AS i) SELECT i FROM s")
	t.Logf("early stop of 10M rows: cancelled in %v, drained in %v", cancelled, drained)
	if cancelled > 200*time.Millisecond || cancelled*4 > drained {
		t.Errorf("the cancelled stop took %v, want under 200ms and under a quarter of the %v drain", cancelled, drained)
	}

	var got int64
	next := conn.Query(t.Context(), "SELECT 4242::bigint FROM pg_sleep(0.2)", nil, postgres.QueryConfig{})
	next.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		for _, row := range b.Items {
			v, _ := row.Value(0)
			got, _ = pgtype.DecodeInt8(v)
		}
		return true
	})
	if err := next.Err(); err != nil {
		t.Fatalf("the statement after the cancelled stop: %v", err)
	}
	if got != 4242 {
		t.Errorf("the statement after the cancelled stop returned %d, want 4242", got)
	}
}

// A COPY ... FROM STDIN reached through ExecParams — or any FetchAll read,
// which is the same wire sequence — is refused at once and leaves the
// connection usable. The query's Sync left with the query and the backend
// ignored it while it waited for CopyData (§53.2.6), so the CopyFail must
// carry a Sync of its own: without it the server discards everything until a
// Sync that never comes, and the call only returns when the context expires.
func TestIntegrationCopyFromStdinThroughExecParamsReturnsAtOnce(t *testing.T) {
	conn := dial(t)
	if err := exec(t, conn, "CREATE TEMP TABLE sluice_copy_execparams (a int)"); err != nil {
		t.Fatal(err)
	}
	// Well above what a round trip takes and well below the context: a
	// return inside the budget proves the call did not wait for the context.
	const budget = time.Second
	for _, path := range []string{"ExecParams", "FetchAll"} {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		start := time.Now()
		var err error
		switch path {
		case "ExecParams":
			_, err = conn.ExecParams(ctx, "COPY sluice_copy_execparams FROM STDIN", nil)
		case "FetchAll":
			src := conn.Query(ctx, "COPY sluice_copy_execparams FROM STDIN", nil, postgres.QueryConfig{Fetch: postgres.FetchAll})
			for range src.Stream() {
			}
			err = src.Err()
		}
		elapsed := time.Since(start)
		cancel()
		if err == nil || !strings.Contains(err.Error(), "CopyFrom") {
			t.Fatalf("%s: error = %v, want a refusal naming Conn.CopyFrom", path, err)
		}
		if elapsed > budget {
			t.Errorf("%s: returned after %v, want under %v — it waited for the context, not the server", path, elapsed, budget)
		}
		if conn.Err() != nil {
			t.Fatalf("%s: the refusal broke the connection: %v", path, conn.Err())
		}
		var got int64
		src := conn.Query(t.Context(), "SELECT 1::int8", nil, postgres.QueryConfig{})
		for b := range src.Stream() {
			for _, row := range b.Items {
				v, _ := row.Value(0)
				got, _ = pgtype.DecodeInt8(v)
			}
		}
		if err := src.Err(); err != nil {
			t.Fatalf("%s: the statement after the refusal: %v", path, err)
		}
		if got != 1 {
			t.Errorf("%s: the statement after the refusal returned %d, want 1", path, got)
		}
	}
}

// A stale plan drops the cache entry, and the server must drop the statement
// with it: PostgreSQL discards the plan but keeps a named statement until a
// Close releases it, so forgetting the name here alone leaves one statement
// behind on the backend per schema change under a live connection. After the
// stale plan and a retry, pg_prepared_statements holds exactly what the cache
// does.
func TestStalePlanStatementIsClosedOnTheServer(t *testing.T) {
	conn := dial(t)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	run := func(sql string) error {
		src := conn.Query(ctx, sql, nil, postgres.QueryConfig{})
		src.Stream()(func(sluice.Batch[postgres.Row]) bool { return true })
		return src.Err()
	}
	// names lists the statements the server holds for this session; the query
	// goes through the cache too, and counts once on both sides.
	names := func() []string {
		var out []string
		src := conn.Query(ctx, "SELECT name FROM pg_prepared_statements WHERE name LIKE 'sluice\\_%' ORDER BY name", nil, postgres.QueryConfig{})
		src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
			for _, r := range b.Items {
				v, _ := r.Value(0)
				out = append(out, string(v))
			}
			return true
		})
		if err := src.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	if err := conn.Exec(ctx, "CREATE TEMP TABLE stale_plan_closed (a int)"); err != nil {
		t.Fatal(err)
	}
	const sql = "SELECT * FROM stale_plan_closed"
	if err := run(sql); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, "ALTER TABLE stale_plan_closed ADD COLUMN b int"); err != nil {
		t.Fatal(err)
	}
	if err := run(sql); !errors.Is(err, postgres.ErrStalePlan) {
		t.Fatalf("after the schema change, error = %v, want ErrStalePlan", err)
	}
	// The retry re-prepares under a fresh name and closes the stale one in
	// front of its Parse; the runs after it are hits and change nothing.
	for range 3 {
		if err := run(sql); err != nil {
			t.Fatalf("the retry after the stale plan returned %v", err)
		}
	}
	held := names()
	cached := conn.StatementCacheStats().Prepared
	if len(held) != cached {
		t.Errorf("the server holds %d statements %q, the cache %d: the stale statement was forgotten, not closed", len(held), held, cached)
	}
}

// A role stored with fewer PBKDF2 iterations than the default floor — what
// scram_iterations = 1000 produces on PostgreSQL 16+, a fast fixture for a
// test database — is refused under Config{} with the field to set named,
// and accepted once Config.MinSCRAMIterations says 1000. The server's own
// count is what makes this a check a scripted server cannot stand in for:
// the refusal reads the i= the real server derived the stored key with.
func TestIntegrationSCRAMIterationBounds(t *testing.T) {
	const role = "sluice_scram_iter_test"
	const password = "iter-pass" //nolint:gosec // G101: a throwaway role's password
	addr, cfg := pgTarget(t)
	admin := dial(t)
	drop := func(ctx context.Context) error {
		return execContext(ctx, admin, "DROP ROLE IF EXISTS "+role)
	}
	if err := drop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := exec(t, admin, "SET scram_iterations = 1000"); err != nil {
		var pgErr *postgres.Error
		if errors.As(err, &pgErr) && pgErr.Code == "42704" {
			t.Skipf("scram_iterations needs PostgreSQL 16 or later: %v", err)
		}
		t.Fatal(err)
	}
	if err := exec(t, admin, "SET password_encryption = 'scram-sha-256'"); err != nil {
		t.Fatal(err)
	}
	if err := exec(t, admin, "CREATE ROLE "+role+" LOGIN PASSWORD '"+password+"'"); err != nil {
		var pgErr *postgres.Error
		if errors.As(err, &pgErr) && pgErr.Code == "42501" {
			t.Skipf("%s may not create roles: %v", cfg.User, err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := drop(ctx); err != nil {
			t.Errorf("drop role %s: %v", role, err)
		}
	})

	c := cfg
	c.User, c.Password = role, password
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	_, err := postgres.Connect(ctx, postgres.DialTCP(addr), c)
	switch {
	case err == nil:
		t.Fatal("a role stored at 1000 iterations authenticated under the default floor of 4096")
	case !errors.Is(err, postgres.ErrAuth):
		t.Fatalf("refused with %v, want ErrAuth", err)
	case !strings.Contains(err.Error(), "1000 PBKDF2 iterations") || !strings.Contains(err.Error(), "Config.MinSCRAMIterations"):
		t.Fatalf("refused with %v, want the server's count and the field to set named", err)
	}

	c.MinSCRAMIterations = 1000
	dialWith(t, addr, c)
}
