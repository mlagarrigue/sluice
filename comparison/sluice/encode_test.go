package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
)

// The hand encoder must write what json.Marshal writes, byte for byte: the
// study's comparison normalises before comparing, but a variant that wrote
// a different document would be measuring a different answer.
func TestAppendListDocMatchesMarshal(t *testing.T) {
	labels := []string{
		"", "plain-ascii", `with "quotes"`, `back\slash`, "<tag> & amp",
		"tab\tnl\ncr\rbs\bff\f", "ctl\x01\x1f", "\x7f", "é à ü — 漢字 🎉",
		"\u2028\u2029", "bad\xffutf8\xc0", "end\\",
	}
	// Every byte once, as a one-byte label, to leave no case to chance.
	for b := range 256 {
		labels = append(labels, string([]byte{byte(b)}))
	}
	rows := make([]orderRow, 0, len(labels))
	for i, l := range labels {
		rows = append(rows, orderRow{ID: int64(i) - 3, Total: math.MaxInt64 - int64(i), Label: l})
	}
	rows = append(rows, orderRow{ID: math.MinInt64, Total: 0, Label: strings.Repeat("x", 300)})

	cases := []struct {
		from  int64
		limit int
		rows  []orderRow
	}{
		// An empty list is a made, empty slice (listAll), never nil: `[]`.
		{0, 1, []orderRow{}}, {-1, 1000, rows[:1]}, {math.MaxInt64, 7, rows},
	}
	for _, c := range cases {
		want, err := json.Marshal(listDoc{From: c.from, Limit: c.limit, Items: c.rows})
		if err != nil {
			t.Fatal(err)
		}
		got := appendListDoc(nil, c.from, c.limit, c.rows)
		if string(got) != string(want) {
			t.Errorf("from=%d limit=%d rows=%d:\n got %s\nwant %s", c.from, c.limit, len(c.rows), got, want)
		}
	}
	// The streamed fragments, concatenated, are the same document.
	var doc []byte
	doc = appendListHead(doc, 5, 10)
	doc = appendRows(doc, rows[:4], true)
	doc = appendRows(doc, rows[4:], false)
	doc = append(doc, "]}"...)
	want, _ := json.Marshal(listDoc{From: 5, Limit: 10, Items: rows})
	if string(doc) != string(want) {
		t.Errorf("fragments:\n got %s\nwant %s", doc, want)
	}
}

// BenchmarkListEncode is the encoder alone, 1 000 rows of the shape of
// bench_orders, both ways: what the server pays for the list document
// before the transport. Figures in done.md only.
func BenchmarkListEncode(b *testing.B) {
	rows := syntheticRows(listLimit)
	b.Run("std", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := json.Marshal(listDoc{From: 1, Limit: listLimit, Items: rows}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("append", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			appendListDoc(make([]byte, 0, listDocSize(len(rows))), 1, listLimit, rows)
		}
	})
}

// BenchmarkListDecode is the connector's decoding alone, the second CPU
// item of the R profile (about 14 %): 1 000 DataRow bodies of the shape of
// bench_orders, delivered as one batch by a query against a backend that
// replays them, then each row read back into an orderRow as decodeOrder
// reads it (Value, DecodeInt8, DecodeText). No socket: the figure says what
// the column decoders cost, nothing about the connection. What appending
// the rows to the batch costs is BenchmarkRowsAppend in database/postgres.
// Figures in done.md only; no fix of the connector lives here.
func BenchmarkListDecode(b *testing.B) {
	rows := syntheticRows(listLimit)
	bodies := make([][]byte, len(rows))
	for i, r := range rows {
		bodies[i] = dataRow(pgtype.AppendInt8(nil, r.ID), pgtype.AppendInt8(nil, r.Total), []byte(r.Label))
	}
	out := make([]orderRow, 0, listLimit)
	decode := func(b *testing.B, acc *postgres.Rows) {
		b.Helper()
		out = out[:0]
		for i := range acc.Len() {
			idB, _ := acc.Value(i, 0)
			totalB, _ := acc.Value(i, 1)
			labelB, _ := acc.Value(i, 2)
			id, err := pgtype.DecodeInt8(idB)
			if err != nil {
				b.Fatal(err)
			}
			total, err := pgtype.DecodeInt8(totalB)
			if err != nil {
				b.Fatal(err)
			}
			out = append(out, orderRow{ID: id, Total: total, Label: pgtype.DecodeText(labelB)})
		}
	}
	conn := postgres.NewConn(replay{bytes.NewReader(replayedList(bodies))}, postgres.Config{})
	src := conn.Query(b.Context(), "SELECT id, total, label FROM bench_orders", nil,
		postgres.QueryConfig{BatchRows: listLimit + 1})
	for batch := range src.Stream() {
		acc := batch.Items[0].Rows()
		b.ReportAllocs()
		for b.Loop() {
			decode(b, acc)
		}
	}
	if err := src.Err(); err != nil {
		b.Fatal(err)
	}
}

// replay is a backend that has already answered: it drops what the client
// sends and hands back one scripted result.
type replay struct{ *bytes.Reader }

func (replay) Write(p []byte) (int, error) { return len(p), nil }
func (replay) Close() error                { return nil }

// replayedList is what a backend answers to the list query: the three
// columns of bench_orders, every DataRow in bodies, and the end of the
// result.
func replayedList(bodies [][]byte) []byte {
	var script []byte
	msg := func(typ byte, body []byte) {
		script = append(script, typ)
		script = binary.BigEndian.AppendUint32(script, uint32(4+len(body))) //nolint:gosec // G115: short messages
		script = append(script, body...)
	}
	msg('1', nil) // ParseComplete
	msg('2', nil) // BindComplete
	desc := binary.BigEndian.AppendUint16(nil, 3)
	for _, col := range []struct {
		name string
		oid  uint32
	}{{"id", pgtype.OIDInt8}, {"total", pgtype.OIDInt8}, {"label", pgtype.OIDText}} {
		desc = append(desc, col.name...)
		desc = append(desc, 0)
		desc = binary.BigEndian.AppendUint32(desc, 0) // table OID
		desc = binary.BigEndian.AppendUint16(desc, 0) // column number
		desc = binary.BigEndian.AppendUint32(desc, col.oid)
		desc = binary.BigEndian.AppendUint16(desc, 0) // type size
		desc = binary.BigEndian.AppendUint32(desc, 0) // type modifier
		desc = binary.BigEndian.AppendUint16(desc, 1) // binary
	}
	msg('T', desc)
	for _, body := range bodies {
		msg('D', body)
	}
	msg('C', fmt.Appendf(nil, "SELECT %d\x00", len(bodies)))
	msg('Z', []byte("I"))
	return script
}

// syntheticRows are n rows shaped like the ones pgfixtures writes into
// bench_orders: ids from 1, a total, a label of about twenty bytes.
func syntheticRows(n int) []orderRow {
	rows := make([]orderRow, n)
	for i := range rows {
		rows[i] = orderRow{ID: int64(i + 1), Total: int64(i*37 + 11), Label: fmt.Sprintf("order-%06d-label", i+1)}
	}
	return rows
}

// dataRow is the body of a DataRow message (PostgreSQL protocol 3.0): the
// field count, then each value's length and bytes, in the binary format the
// connector asks for (int8 as eight bytes, text as it is).
func dataRow(values ...[]byte) []byte {
	b := binary.BigEndian.AppendUint16(nil, uint16(len(values))) //nolint:gosec // G115: three fields
	for _, v := range values {
		b = binary.BigEndian.AppendUint32(b, uint32(len(v))) //nolint:gosec // G115: short values
		b = append(b, v...)
	}
	return b
}
