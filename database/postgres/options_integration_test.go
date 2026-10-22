package postgres_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
)

// Session options that change how a server writes values as text change
// nothing here: every value is read in binary, and a bound parameter is never
// parsed as SQL. Each option is set at startup through Config.Params, and the
// same query must return the same bytes as on a connection with the
// defaults. The query names no backslash and no ambiguous date, so its own
// text means the same under every option; what is tested is the connector.
//
// lc_messages is not among them: only a superuser may set it, and the role
// these tests run as is not one. Errors are matched by SQLSTATE, never by
// their text, which is what that option would change.
func TestIntegrationSessionOptions(t *testing.T) {
	addr, cfg := pgTarget(t)
	const sql = `SELECT '2026-03-04 05:06:07.89+00'::timestamptz, '2026-03-04'::date,
		'2026-03-04 05:06:07'::timestamp, '1 day 02:03:04.5'::interval, 0.1::float8,
		0.1::float4, 1.5::numeric, decode('00ff5c', 'hex'), $1::text`
	// A backslash in a bound parameter: under standard_conforming_strings =
	// off it would be an escape in a literal, and must not be in a parameter.
	param := []byte(`a\b`)
	baseline := optionRow(t, addr, cfg, sql, param)

	for _, opt := range []struct{ name, value string }{
		{"DateStyle", "SQL, DMY"},
		{"IntervalStyle", "postgres_verbose"},
		{"extra_float_digits", "-15"},
		{"bytea_output", "escape"},
		{"standard_conforming_strings", "off"},
		{"TimeZone", "Asia/Kolkata"}, // +05:30: half an hour off any whole-hour zone
	} {
		t.Run(opt.name, func(t *testing.T) {
			c := cfg
			c.Params = map[string]string{opt.name: opt.value}
			// The server took the option, or the comparison below would
			// hold for want of anything to resist.
			if set := optionRow(t, addr, c, "SELECT current_setting($1)", []byte(opt.name)); string(set[0]) != opt.value {
				t.Fatalf("%s is %q on the connection, want %q", opt.name, set[0], opt.value)
			}
			got := optionRow(t, addr, c, sql, param)
			for i := range baseline {
				if !bytes.Equal(got[i], baseline[i]) {
					t.Errorf("column %d under %s = %q: %x, want %x as with the defaults", i, opt.name, opt.value, got[i], baseline[i])
				}
			}
		})
	}

	// client_encoding named by the caller is the caller's to decode: the
	// server converts text to it, and the connection does not refuse it.
	t.Run("client_encoding", func(t *testing.T) {
		c := cfg
		c.Params = map[string]string{"client_encoding": "LATIN1"}
		got := optionRow(t, addr, c, "SELECT chr(233)::text, $1::text = 'caf' || chr(233)", []byte("caf\xe9"))
		if !bytes.Equal(got[0], []byte{0xe9}) {
			t.Errorf("é under LATIN1 arrived as %x, want e9", got[0])
		}
		if ok, err := postgres.DecodeBool(got[1]); err != nil || !ok {
			t.Errorf("a LATIN1 parameter was not read as LATIN1: %v %v", ok, err)
		}
	})
}

// optionRow connects with cfg, runs sql with one text parameter and returns
// the first row's values, copied.
func optionRow(t *testing.T, addr string, cfg postgres.Config, sql string, param []byte) [][]byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	conn, err := postgres.Connect(ctx, postgres.DialTCP(addr), cfg)
	if err != nil {
		t.Fatalf("connecting with %v: %v", cfg.Params, err)
	}
	defer conn.Close()
	var out [][]byte
	src := conn.Query(ctx, sql, [][]byte{param}, postgres.QueryConfig{BatchRows: 1, ParamOIDs: []uint32{postgres.OIDText}})
	src.Stream()(func(b sluice.Batch[postgres.Row]) bool {
		for _, row := range b.Items {
			if out != nil {
				continue
			}
			for i := range row.Fields() {
				v, _ := row.Value(i)
				out = append(out, bytes.Clone(v))
			}
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatalf("%s with %v: %v", sql, cfg.Params, err)
	}
	if conn.Err() != nil {
		t.Fatalf("the connection broke with %v: %v", cfg.Params, conn.Err())
	}
	return out
}
