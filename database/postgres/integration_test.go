package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"math"
	"net"
	"net/netip"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
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

func pgTarget(t *testing.T) (addr string, cfg postgres.StartupConfig) {
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

	nc, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dialling %s: %v", addr, err)
	}
	// The deadline is set here rather than inside the package: this is the one
	// place that knows how long a handshake is allowed to take.
	if err := nc.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}

	conn, err := postgres.Startup(nc, cfg)
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
		[][]byte{postgres.AppendInt8Array(nil, keys)},
		postgres.QueryConfig{BatchRows: 256, ParamOIDs: []uint32{postgres.OIDInt8Array}},
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
			k, err := postgres.DecodeInt8(v)
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
		postgres.OIDInt8, postgres.OIDInt4, postgres.OIDInt2, postgres.OIDFloat8,
		postgres.OIDBool, postgres.OIDText, postgres.OIDTimestampTZ,
		postgres.OIDDate, postgres.OIDNumeric,
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
			n, err := postgres.ParseNumeric(want)
			if err != nil {
				t.Fatal(err)
			}
			src := conn.Query(t.Context(), "SELECT $1::numeric", [][]byte{postgres.AppendNumeric(nil, n)},
				postgres.QueryConfig{BatchRows: 4, ParamOIDs: []uint32{postgres.OIDNumeric}})

			var got string
			src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
				for _, row := range b.Items {
					v, _ := row.Value(0)
					back, err := postgres.DecodeNumeric(v)
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
		buf = postgres.AppendField(buf, postgres.AppendInt8(nil, int64(i)))
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
			total, _ = postgres.DecodeInt8(v)
			v, _ = row.Value(1)
			labelled, _ = postgres.DecodeInt8(v)
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

	src := conn.Query(t.Context(), "SELECT * FROM a_table_that_does_not_exist", nil, postgres.QueryConfig{})
	src.Stream()(func(sluice.Batch[postgres.Row]) bool { return true })

	var pgErr *postgres.Error
	if err := src.Err(); !errors.As(err, &pgErr) {
		t.Fatalf("Err = %v, want a *postgres.Error", err)
	}
	if pgErr.Code != "42P01" {
		t.Errorf("SQLSTATE = %q, want 42P01 (undefined_table)", pgErr.Code)
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

// exec runs a statement for its effect, draining whatever it returns.
func exec(t *testing.T, conn *postgres.Conn, sql string) error {
	t.Helper()
	src := conn.Query(t.Context(), sql, nil, postgres.QueryConfig{BatchRows: 16})
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
			"jsonb", `'{"a": 1}'::jsonb`, postgres.OIDJSONB,
			postgres.AppendJSONB(nil, []byte(`{"a": 1}`)),
		},
		{
			// Compared as text, because PostgreSQL gives `json` no equality
			// operator at all — equality on unparsed text is ill-defined, and
			// that refusal is the clearest statement of what separates the two
			// types: `jsonb` is a parsed value that can be compared, `json` is
			// the bytes you sent. Text comparison is therefore the *right*
			// check here rather than a workaround: it is exactly what the type
			// promises to preserve.
			"json", `'{"a":1}'`, postgres.OIDJSON,
			postgres.AppendJSON(nil, []byte(`{"a":1}`)),
		},
		{
			"timestamp without time zone", `'2024-03-01 12:34:56.789012'::timestamp`, postgres.OIDTimestamp,
			postgres.AppendTimestamp(nil, time.Date(2024, 3, 1, 12, 34, 56, 789012000, time.UTC)),
		},
		{
			// Far outside what a time.Duration spans, which is where the
			// decoder used to wrap and return a date six centuries off with
			// no error. The server's own equality decides.
			"a timestamp beyond a Duration's reach", `'3000-01-01 00:00:00'::timestamp`, postgres.OIDTimestamp,
			postgres.AppendTimestamp(nil, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)),
		},
		{
			"the type's upper bound", `'294276-12-31 23:59:59.999999'::timestamp`, postgres.OIDTimestamp,
			postgres.AppendTimestamp(nil, time.Date(294276, 12, 31, 23, 59, 59, 999999000, time.UTC)),
		},
		{
			"a timestamptz beyond a Duration's reach", `'3000-01-01 00:00:00+00'::timestamptz`, postgres.OIDTimestampTZ,
			postgres.AppendTimestampTZ(nil, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)),
		},
		{
			"an interval longer than a Duration", `'100000000 hours'::interval`, postgres.OIDInterval,
			postgres.AppendInterval(nil, postgres.Interval{Micros: 100_000_000 * 3600 * 1_000_000}),
		},
		{
			"interval with all three fields", `'1 year 2 mons 3 days 04:05:06.789'::interval`, postgres.OIDInterval,
			postgres.AppendInterval(nil, postgres.Interval{Months: 14, Days: 3, Micros: 14_706_789_000}),
		},
		{
			"a negative interval", `'-1 mon -1 day -00:00:01'::interval`, postgres.OIDInterval,
			postgres.AppendInterval(nil, postgres.Interval{Months: -1, Days: -1, Micros: -1_000_000}),
		},
		{
			"inet keeping its host bits", `'192.168.1.1/24'::inet`, postgres.OIDInet,
			postgres.AppendInet(nil, netip.MustParsePrefix("192.168.1.1/24")),
		},
		{
			"an IPv6 address", `'2001:db8::1'::inet`, postgres.OIDInet,
			postgres.AppendInet(nil, netip.MustParsePrefix("2001:db8::1/128")),
		},
		{
			// The server is the authority on this one: it reports family 6 for
			// a 4-in-6 address and answers false when it is compared with the
			// plain IPv4 spelling. Unmapping it on the way out would write a
			// different value than the one that was read.
			"an IPv4-mapped IPv6 address, which stays IPv6", `'::ffff:1.2.3.4'::inet`, postgres.OIDInet,
			postgres.AppendInet(nil, netip.PrefixFrom(netip.MustParseAddr("::ffff:1.2.3.4"), 128)),
		},
		{
			"cidr", `'10.0.0.0/8'::cidr`, postgres.OIDCIDR,
			postgres.AppendCIDR(nil, netip.MustParsePrefix("10.0.0.0/8")),
		},
		{
			// PostgreSQL considers numeric NaN equal to NaN, unlike a float.
			"numeric NaN", `'NaN'::numeric`, postgres.OIDNumeric,
			postgres.AppendNumeric(nil, postgres.Numeric{Sign: postgres.NumericNaN}),
		},
		{
			"numeric Infinity", `'Infinity'::numeric`, postgres.OIDNumeric,
			postgres.AppendNumeric(nil, postgres.Numeric{Sign: postgres.NumericPosInf}),
		},
		{
			"numeric -Infinity", `'-Infinity'::numeric`, postgres.OIDNumeric,
			postgres.AppendNumeric(nil, postgres.Numeric{Sign: postgres.NumericNegInf}),
		},
		{
			"uuid[]", `ARRAY['00000000-0000-0000-0000-000000000001']::uuid[]`, postgres.OIDUUIDArray,
			postgres.AppendUUIDArray(nil, [][16]byte{{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}}),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			expr := "SELECT $1 = " + tc.literal
			if tc.oid == postgres.OIDJSON {
				expr = "SELECT $1::text = " + tc.literal
			}
			src := conn.Query(t.Context(), expr, [][]byte{tc.param},
				postgres.QueryConfig{ParamOIDs: []uint32{tc.oid}})

			var equal, got bool
			src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
				for _, row := range b.Items {
					v, isNull := row.Value(0)
					if isNull {
						return false
					}
					equal, _ = postgres.DecodeBool(v)
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
				postgres.OIDJSONB, postgres.OIDTimestamp, postgres.OIDInterval,
				postgres.OIDInet, postgres.OIDUUIDArray,
			}
			for i, want := range wantOIDs {
				if got := row.Fields()[i].TypeOID; got != want {
					t.Errorf("column %d has OID %d, the codec expects %d", i, got, want)
				}
			}

			v, _ := row.Value(0)
			if j, err := postgres.DecodeJSONB(v); err != nil || string(j) != `{"a": 1}` {
				t.Errorf("jsonb = %q, %v", j, err)
			}
			v, _ = row.Value(1)
			if ts, err := postgres.DecodeTimestamp(v); err != nil ||
				!ts.Equal(time.Date(2024, 3, 1, 12, 34, 56, 789012000, time.UTC)) {
				t.Errorf("timestamp = %v, %v", ts, err)
			}
			v, _ = row.Value(2)
			if iv, err := postgres.DecodeInterval(v); err != nil ||
				iv != (postgres.Interval{Months: 14, Days: 3, Micros: 14_706_789_000}) {
				t.Errorf("interval = %v, %v", iv, err)
			}
			v, _ = row.Value(3)
			if p, err := postgres.DecodeInet(v); err != nil || p.String() != "192.168.1.1/24" {
				t.Errorf("inet = %v, %v", p, err)
			}
			v, _ = row.Value(4)
			ids, err := postgres.DecodeUUIDArray(nil, v)
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
			got, _ = postgres.DecodeInt8(v)
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
					got, err = postgres.DecodeTimestampTZ(v)
				} else {
					got, err = postgres.DecodeTimestamp(v)
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
				mapped, err := postgres.DecodeInet(v)
				if err != nil {
					t.Fatalf("decoding the mapped address: %v", err)
				}
				if !mapped.Addr().Is4In6() {
					t.Errorf("the mapped address came back as %v, want it still 4-in-6", mapped)
				}
				v, _ = row.Value(1)
				plain, err := postgres.DecodeInet(v)
				if err != nil {
					t.Fatalf("decoding the plain address: %v", err)
				}
				if plain.Addr().Is4In6() {
					t.Errorf("the plain address came back as %v, want it still IPv4", plain)
				}
				// And re-encoding each must reproduce what the server sent,
				// which is the property that makes read-then-write safe.
				orig0, _ := row.Value(0)
				if got := postgres.AppendInet(nil, mapped); !bytes.Equal(got, orig0) {
					t.Errorf("re-encoded the mapped address as %x, want %x", got, orig0)
				}
				v, _ = row.Value(2)
				equal, _ := postgres.DecodeBool(v)
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
				_, decodeErr = postgres.DecodeInt8Array(nil, v)
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
				got, _ = postgres.DecodeInt8Array(nil, v)
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
			mine, err := postgres.ParseNumeric(lit)
			if err != nil {
				t.Fatalf("ParseNumeric: %v", err)
			}

			// numeric_send is the function the server itself uses to put a
			// value on the wire, so this is the encoder judging the encoder.
			src := conn.Query(t.Context(), "SELECT numeric_send($1::text::numeric)",
				[][]byte{postgres.AppendText(nil, lit)},
				postgres.QueryConfig{ParamOIDs: []uint32{postgres.OIDText}})

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

			if got := postgres.AppendNumeric(nil, mine); !bytes.Equal(got, theirs) {
				t.Errorf("AppendNumeric(%s) = %x,\n           the server sends %x", lit, got, theirs)
			}
			// And the server's bytes must decode to the same value, so the two
			// encodings are one shape rather than two that agree by accident.
			back, err := postgres.DecodeNumeric(theirs)
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
		{"bool true", `true`, postgres.AppendBool(nil, true)},
		{"bool false", `false`, postgres.AppendBool(nil, false)},
		{"int2", `(-12345)::smallint`, postgres.AppendInt2(nil, -12345)},
		{"int4", `(-1234567)::int`, postgres.AppendInt4(nil, -1234567)},
		{"int8", `(-1234567890123)::bigint`, postgres.AppendInt8(nil, -1234567890123)},
		{"float4", `(1.5)::real`, postgres.AppendFloat4(nil, 1.5)},
		{"float8", `(-0.125)::double precision`, postgres.AppendFloat8(nil, -0.125)},
		{"float8 infinity", `('Infinity')::double precision`, postgres.AppendFloat8(nil, math.Inf(1))},
		{"text", `'héllo wörld'::text`, postgres.AppendText(nil, "héllo wörld")},
		{"text empty", `''::text`, postgres.AppendText(nil, "")},
		{"bytea", `'\xdeadbeef'::bytea`, postgres.AppendBytea(nil, []byte{0xde, 0xad, 0xbe, 0xef})},
		{
			"date", `'2024-03-01'::date`,
			postgres.AppendDate(nil, time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)),
		},
		{
			"date before the epoch", `'1970-01-01'::date`,
			postgres.AppendDate(nil, time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)),
		},
		{
			"timestamp", `'2024-03-01 12:34:56.789012'::timestamp`,
			postgres.AppendTimestamp(nil, time.Date(2024, 3, 1, 12, 34, 56, 789012000, time.UTC)),
		},
		{
			"timestamptz", `'2024-03-01 12:34:56.789012+00'::timestamptz`,
			postgres.AppendTimestampTZ(nil, time.Date(2024, 3, 1, 12, 34, 56, 789012000, time.UTC)),
		},
		{
			"uuid", `'0102030405060708090a0b0c0d0e0f10'::uuid`,
			postgres.AppendUUID(nil, [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
		},
		{"jsonb", `'{"a": 1}'::jsonb`, postgres.AppendJSONB(nil, []byte(`{"a": 1}`))},
		{"json", `'{"a":1}'::json`, postgres.AppendJSON(nil, []byte(`{"a":1}`))},
		{
			"interval", `'1 year 2 mons 3 days 04:05:06.789'::interval`,
			postgres.AppendInterval(nil, postgres.Interval{Months: 14, Days: 3, Micros: 14_706_789_000}),
		},
		{
			"inet", `'192.168.1.1/24'::inet`,
			postgres.AppendInet(nil, netip.MustParsePrefix("192.168.1.1/24")),
		},
		{
			"inet v6", `'2001:db8::1'::inet`,
			postgres.AppendInet(nil, netip.MustParsePrefix("2001:db8::1/128")),
		},
		{
			"cidr", `'10.0.0.0/8'::cidr`,
			postgres.AppendCIDR(nil, netip.MustParsePrefix("10.0.0.0/8")),
		},
		{
			"int4[]", `ARRAY[1,-2,3]::int[]`,
			postgres.AppendInt4Array(nil, []int32{1, -2, 3}),
		},
		{
			"int8[]", `ARRAY[1,-2,3]::bigint[]`,
			postgres.AppendInt8Array(nil, []int64{1, -2, 3}),
		},
		{
			"text[]", `ARRAY['a','','ccc']::text[]`,
			postgres.AppendTextArray(nil, []string{"a", "", "ccc"}),
		},
		{
			"uuid[]", `ARRAY['00000000-0000-0000-0000-000000000001']::uuid[]`,
			postgres.AppendUUIDArray(nil, [][16]byte{{15: 1}}),
		},
		{
			"empty int8[]", `ARRAY[]::bigint[]`,
			postgres.AppendInt8Array(nil, nil),
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
			return postgres.DecodeText(v)
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
		[][]byte{postgres.AppendInt8Array(nil, nil)},
		postgres.QueryConfig{BatchRows: 16, ParamOIDs: []uint32{postgres.OIDInt8Array}})

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
		buf = postgres.AppendField(buf, postgres.AppendInt8(nil, int64(i)))
		buf = postgres.AppendField(buf, postgres.AppendNumeric(nil, amount))
		buf = postgres.AppendField(buf, postgres.AppendTimestamp(nil, at))
		buf = postgres.AppendField(buf, postgres.AppendJSONB(nil, []byte(`{"i": 1}`)))
		buf = postgres.AppendField(buf, postgres.AppendInet(nil, netip.MustParsePrefix("192.168.1.1/24")))
		buf = postgres.AppendField(buf, postgres.AppendUUID(nil, ref))
		buf = postgres.AppendField(buf, postgres.AppendInt8Array(nil, []int64{int64(i), -1}))
		// Every other row leaves the last column NULL, so the marker travels
		// beside real values rather than only on its own.
		if i%2 == 0 {
			buf = postgres.AppendField(buf, postgres.AppendText(nil, "héllo"))
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
			gotAmount, err := postgres.DecodeNumeric(v)
			if err != nil || gotAmount.String() != amount.String() {
				t.Errorf("row %d amount = %v (%v), want %v", i, gotAmount, err, amount)
			}
			v, _ = row.Value(2)
			gotAt, err := postgres.DecodeTimestamp(v)
			if err != nil || !gotAt.Equal(at) {
				t.Errorf("row %d at = %v (%v), want %v", i, gotAt, err, at)
			}
			v, _ = row.Value(3)
			doc, err := postgres.DecodeJSONB(v)
			if err != nil || string(doc) != `{"i": 1}` {
				t.Errorf("row %d doc = %q (%v)", i, doc, err)
			}
			v, _ = row.Value(4)
			addr, err := postgres.DecodeInet(v)
			if err != nil || addr.String() != "192.168.1.1/24" {
				t.Errorf("row %d addr = %v (%v)", i, addr, err)
			}
			v, _ = row.Value(5)
			gotRef, err := postgres.DecodeUUID(v)
			if err != nil || gotRef != ref {
				t.Errorf("row %d ref = %x (%v)", i, gotRef, err)
			}
			v, _ = row.Value(6)
			tags, err := postgres.DecodeInt8Array(nil, v)
			if err != nil || len(tags) != 2 || tags[0] != int64(i) || tags[1] != -1 {
				t.Errorf("row %d tags = %v (%v)", i, tags, err)
			}
			v, isNull := row.Value(7)
			switch {
			case i%2 == 0 && (isNull || postgres.DecodeText(v) != "héllo"):
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

func mustParseNumeric(s string) postgres.Numeric {
	n, err := postgres.ParseNumeric(s)
	if err != nil {
		panic(err)
	}
	return n
}
