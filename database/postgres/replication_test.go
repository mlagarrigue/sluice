package postgres

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// pgoutput message builders, written from "Logical Replication Message
// Formats" in the protocol chapter.

func relMsg(oid uint32, ns, name string, cols ...string) []byte {
	b := binary.BigEndian.AppendUint32([]byte{'R'}, oid)
	b = append(append(b, ns...), 0)
	b = append(append(b, name...), 0)
	b = append(b, 'd')
	b = binary.BigEndian.AppendUint16(b, uint16(len(cols)))
	for i, c := range cols {
		flag := byte(0)
		if i == 0 {
			flag = 1
		}
		b = append(b, flag)
		b = append(append(b, c...), 0)
		b = binary.BigEndian.AppendUint32(b, 20) // int8
		b = binary.BigEndian.AppendUint32(b, 0xFFFFFFFF)
	}
	return b
}

func tupleData(vals ...[]byte) []byte {
	b := binary.BigEndian.AppendUint16(nil, uint16(len(vals)))
	for _, v := range vals {
		if v == nil {
			b = append(b, 'n')
			continue
		}
		b = append(b, 'b')
		b = binary.BigEndian.AppendUint32(b, uint32(len(v)))
		b = append(b, v...)
	}
	return b
}

func i8(v int64) []byte { return binary.BigEndian.AppendUint64(nil, uint64(v)) }

func beginMsg(final LSN, xid uint32) []byte {
	b := binary.BigEndian.AppendUint64([]byte{'B'}, uint64(final))
	b = binary.BigEndian.AppendUint64(b, 0)
	return binary.BigEndian.AppendUint32(b, xid)
}

func commitMsg(at, end LSN) []byte {
	b := binary.BigEndian.AppendUint64([]byte{'C', 0}, uint64(at))
	b = binary.BigEndian.AppendUint64(b, uint64(end))
	return binary.BigEndian.AppendUint64(b, 1_000_000) // one second past 2000-01-01
}

func rowMsg(typ byte, oid uint32, parts ...any) []byte {
	b := binary.BigEndian.AppendUint32([]byte{typ}, oid)
	for _, p := range parts {
		switch p := p.(type) {
		case byte:
			b = append(b, p)
		case []byte:
			b = append(b, p...)
		}
	}
	return b
}

// decodeAll runs messages through one decoder and materialises the tuples,
// as the stream does at the end of a batch.
func decodeAll(t *testing.T, msgs ...[]byte) ([]Change, error) {
	t.Helper()
	var d replDecoder
	var changes []Change
	var spans []tupleSpans
	var vals []Value
	var err error
	for i, m := range msgs {
		changes, spans, vals, err = d.decode(m, LSN(100+i), changes, spans, vals)
		if err != nil {
			return nil, err
		}
	}
	for i, sp := range spans {
		if sp.hasOld {
			changes[i].Old = vals[sp.old[0]:sp.old[1]]
		}
		if sp.hasNew {
			changes[i].New = vals[sp.new[0]:sp.new[1]]
		}
	}
	return changes, nil
}

func TestReplDecoderTransaction(t *testing.T) {
	truncate := binary.BigEndian.AppendUint32([]byte{'T'}, 1)
	truncate = binary.BigEndian.AppendUint32(append(truncate, 0), 42)
	changes, err := decodeAll(t,
		beginMsg(0x10, 7),
		relMsg(42, "public", "orders", "id", "note"),
		rowMsg('I', 42, byte('N'), tupleData(i8(1), []byte("a"))),
		rowMsg('U', 42, byte('K'), tupleData(i8(1), nil), byte('N'), tupleData(i8(2), nil)),
		rowMsg('U', 42, byte('N'), tupleData(i8(2), []byte("b"))),
		rowMsg('D', 42, byte('O'), tupleData(i8(2), []byte("b"))),
		truncate,
		commitMsg(0x20, 0x28),
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []ChangeKind{ChangeBegin, ChangeInsert, ChangeUpdate, ChangeUpdate, ChangeDelete, ChangeTruncate, ChangeCommit}
	if len(changes) != len(want) {
		t.Fatalf("%d changes, want %d", len(changes), len(want))
	}
	for i, c := range changes {
		if c.Kind != want[i] {
			t.Errorf("change %d is %v, want %v", i, c.Kind, want[i])
		}
	}
	if b := changes[0]; b.LSN != 0x10 || b.Xid != 7 {
		t.Errorf("Begin = %+v", b)
	}
	ins := changes[1]
	if ins.Relation.Name != "orders" || !ins.Relation.Columns[0].Key || ins.Relation.Columns[1].Key {
		t.Errorf("relation = %+v", ins.Relation)
	}
	if len(ins.New) != 2 || ins.New[0].Status != ValueBinary || string(ins.New[1].Data) != "a" || ins.Old != nil {
		t.Errorf("Insert tuples = %+v / %+v", ins.New, ins.Old)
	}
	if ins.LSN != 102 {
		t.Errorf("Insert LSN = %v, want the WAL position it came at", ins.LSN)
	}
	if u := changes[2]; !u.OldIsKey || u.Old[1].Status != ValueNull || binary.BigEndian.Uint64(u.New[0].Data) != 2 {
		t.Errorf("keyed Update = %+v", u)
	}
	if u := changes[3]; u.Old != nil || u.New == nil {
		t.Errorf("Update without its key part = %+v", u)
	}
	if d := changes[4]; d.OldIsKey || d.New != nil || string(d.Old[1].Data) != "b" {
		t.Errorf("Delete = %+v", d)
	}
	if tr := changes[5]; tr.Relation.OID != 42 {
		t.Errorf("Truncate relation = %+v", tr.Relation)
	}
	c := changes[6]
	if c.LSN != 0x20 || c.EndLSN != 0x28 || !c.Time.Equal(time.Date(2000, 1, 1, 0, 0, 1, 0, time.UTC)) {
		t.Errorf("Commit = %+v", c)
	}
}

// A relation described again keeps the changes already decoded on the old
// description, and the next ones use the new.
func TestReplDecoderRelationRedescribed(t *testing.T) {
	changes, err := decodeAll(t,
		relMsg(1, "s", "t", "a"),
		rowMsg('I', 1, byte('N'), tupleData(i8(1))),
		relMsg(1, "s", "t", "a", "b"),
		rowMsg('I', 1, byte('N'), tupleData(i8(1), i8(2))),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes[0].Relation.Columns) != 1 || len(changes[1].Relation.Columns) != 2 {
		t.Errorf("descriptions = %d then %d columns, want 1 then 2", len(changes[0].Relation.Columns), len(changes[1].Relation.Columns))
	}
}

// Everything a server could send wrong is refused with ErrProtocol rather
// than decoded into something plausible.
func TestReplDecoderRefuses(t *testing.T) {
	rel := relMsg(1, "s", "t", "a")
	for name, msgs := range map[string][][]byte{
		"a change before its relation": {rowMsg('I', 1, byte('N'), tupleData(i8(1)))},
		"a truncated Begin":            {beginMsg(1, 1)[:10]},
		"a message not asked for":      {{'M', 0, 0, 0, 0}},
		"bytes past the end":           {append(commitMsg(1, 2), 0)},
		"a tuple of the wrong width":   {rel, rowMsg('I', 1, byte('N'), tupleData(i8(1), i8(2)))},
		"an Insert without N":          {rel, rowMsg('I', 1, byte('K'), tupleData(i8(1)))},
		"a Delete without its old row": {rel, rowMsg('D', 1, byte('N'), tupleData(i8(1)))},
		"a value kind that is not one": {rel, rowMsg('I', 1, byte('N'), []byte{0, 1, 'x'})},
		"a value longer than the body": {rel, rowMsg('I', 1, byte('N'), []byte{0, 1, 'b', 0, 0, 1, 0, 1})},
		"more columns than the body":   {{'R', 0, 0, 0, 1, 's', 0, 't', 0, 'd', 0xFF, 0xFF}},
		"an empty message":             {{}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeAll(t, msgs...); !errors.Is(err, ErrProtocol) {
				t.Errorf("err = %v, want ErrProtocol", err)
			}
		})
	}
}

// The relation table is bounded: a server describing relation after
// relation is refused once it passes the bound, never followed into memory.
func TestReplDecoderBoundsTheRelations(t *testing.T) {
	var d replDecoder
	var err error
	for i := range maxReplicationRelations {
		if _, _, _, err = d.decode(relMsg(uint32(i), "s", "t", "a"), 0, nil, nil, nil); err != nil {
			t.Fatalf("relation %d: %v", i, err)
		}
	}
	// Describing a known relation again is not growth.
	if _, _, _, err = d.decode(relMsg(0, "s", "t", "a", "b"), 0, nil, nil, nil); err != nil {
		t.Fatalf("redescribing a known relation: %v", err)
	}
	if _, _, _, err = d.decode(relMsg(maxReplicationRelations, "s", "t", "a"), 0, nil, nil, nil); !errors.Is(err, ErrProtocol) {
		t.Fatalf("relation past the bound: %v, want ErrProtocol", err)
	}
}

func TestLSN(t *testing.T) {
	l := LSN(0x16_B374D848)
	if s := l.String(); s != "16/B374D848" {
		t.Errorf("String = %q", s)
	}
	back, err := ParseLSN("16/b374d848")
	if err != nil || back != l {
		t.Errorf("ParseLSN = %v, %v", back, err)
	}
	for _, bad := range []string{"", "16", "x/1", "1/100000000"} {
		if _, err := ParseLSN(bad); err == nil {
			t.Errorf("ParseLSN(%q) accepted", bad)
		}
	}
}

// The START_REPLICATION command quotes what the caller named, so a slot or
// publication name is never read as syntax.
func TestStartReplicationSQL(t *testing.T) {
	sql, err := startReplicationSQL(ReplicationConfig{Slot: `my"slot`, Publications: []string{"a", "it's"}, Start: 0x1_00000002})
	if err != nil {
		t.Fatal(err)
	}
	want := `START_REPLICATION SLOT "my""slot" LOGICAL 1/2 (proto_version '1', publication_names '"a","it''s"', binary 'true')`
	if sql != want {
		t.Errorf("sql =\n%s\nwant\n%s", sql, want)
	}
	for name, rc := range map[string]ReplicationConfig{
		"no slot":           {Publications: []string{"p"}},
		"no publication":    {Slot: "s"},
		"an empty name":     {Slot: "s", Publications: []string{""}},
		"a negative batch":  {Slot: "s", Publications: []string{"p"}, BatchChanges: -1},
		"a negative status": {Slot: "s", Publications: []string{"p"}, StatusInterval: -time.Second},
		"a NUL in the slot": {Slot: "s\x00", Publications: []string{"p"}},
	} {
		if _, err := startReplicationSQL(rc); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// Params may still not open a replication connection: Replicate is the way.
func TestParamsRefuseReplication(t *testing.T) {
	_, err := startupBody(Config{User: "u", Params: map[string]string{"replication": "database"}})
	if err == nil {
		t.Fatal(`Params["replication"] accepted`)
	}
	body, err := startupBody(Config{User: "u", replication: true})
	if err != nil {
		t.Fatal(err)
	}
	if !containsParam(body, "replication", "database") {
		t.Error("the startup packet of a replication connection lacks replication=database")
	}
}

func containsParam(body []byte, key, val string) bool {
	want := append(append(append([]byte(key), 0), val...), 0)
	for i := 0; i+len(want) <= len(body); i++ {
		if string(body[i:i+len(want)]) == string(want) {
			return true
		}
	}
	return false
}

// FuzzReplicationDecode feeds arbitrary bodies to a decoder that already
// knows a relation: it may refuse, never panic, and what it accepts points
// inside the value slab.
func FuzzReplicationDecode(f *testing.F) {
	f.Add(beginMsg(1, 2))
	f.Add(commitMsg(1, 2))
	f.Add(relMsg(9, "s", "t", "a", "b"))
	f.Add(rowMsg('I', 1, byte('N'), tupleData(i8(1), nil)))
	f.Add(rowMsg('U', 1, byte('O'), tupleData(i8(1), []byte("x")), byte('N'), tupleData(i8(2), nil)))
	f.Add(rowMsg('D', 1, byte('K'), tupleData(i8(1), nil)))
	f.Fuzz(func(t *testing.T, body []byte) {
		var d replDecoder
		if _, _, _, err := d.decode(relMsg(1, "s", "t", "a", "b"), 0, nil, nil, nil); err != nil {
			t.Fatal(err)
		}
		changes, spans, vals, err := d.decode(body, 5, nil, nil, nil)
		if err != nil {
			return
		}
		if len(changes) != len(spans) {
			t.Fatalf("%d changes, %d spans", len(changes), len(spans))
		}
		for _, sp := range spans {
			for _, s := range [][2]int{sp.old, sp.new} {
				if s[0] < 0 || s[0] > s[1] || s[1] > len(vals) {
					t.Fatalf("span %v outside %d values", s, len(vals))
				}
			}
		}
	})
}

// scriptedRW replays a fixed server stream and records what the client sends.
type scriptedRW struct {
	*bytes.Reader
	sent bytes.Buffer
}

func (s *scriptedRW) Write(p []byte) (int, error) { return s.sent.Write(p) }
func (s *scriptedRW) Close() error                { return nil }

func copyData(p []byte) []byte { return frame(pgwire.BackendCopyData, string(p)) }

func xlog(at LSN, msg []byte) []byte {
	b := binary.BigEndian.AppendUint64([]byte{'w'}, uint64(at))
	b = binary.BigEndian.AppendUint64(b, uint64(at))
	b = binary.BigEndian.AppendUint64(b, 0)
	return copyData(append(b, msg...))
}

func keepalive(reply bool) []byte {
	b := binary.BigEndian.AppendUint64([]byte{'k'}, 0x50)
	b = binary.BigEndian.AppendUint64(b, 0)
	if reply {
		return copyData(append(b, 1))
	}
	return copyData(append(b, 0))
}

// The stream answers a keepalive that asks for a reply with the acknowledged
// position, delivers what arrived in one read as one batch, and when the
// server ends copy mode answers with CopyDone and reads to ReadyForQuery.
func TestReplicationStreamScripted(t *testing.T) {
	var script []byte
	script = append(script, keepalive(true)...)
	script = append(script, xlog(0x10, beginMsg(0x30, 1))...)
	script = append(script, xlog(0x20, relMsg(1, "s", "t", "a"))...)
	script = append(script, xlog(0x20, rowMsg('I', 1, byte('N'), tupleData(i8(7))))...)
	script = append(script, xlog(0x30, commitMsg(0x30, 0x38))...)
	script = append(script, frame(pgwire.BackendCopyDone, "")...)
	script = append(script, frame(pgwire.BackendCommandComplete, "START_REPLICATION\x00")...)
	script = append(script, frame(pgwire.BackendReadyForQuery, "I")...)
	rw := &scriptedRW{Reader: bytes.NewReader(script)}
	r := &Replication{c: NewConn(rw), batch: 100, interval: time.Hour}
	r.Ack(0x08)

	var batches [][]ChangeKind
	src := r.Changes(t.Context())
	src.Stream()(func(b sluice.Batch[Change]) bool {
		var kinds []ChangeKind
		for _, c := range b.Items {
			kinds = append(kinds, c.Kind)
			if c.Kind == ChangeInsert && binary.BigEndian.Uint64(c.New[0].Data) != 7 {
				t.Errorf("Insert value = %x", c.New[0].Data)
			}
		}
		batches = append(batches, kinds)
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatal(err)
	}
	if len(batches) != 1 || len(batches[0]) != 3 {
		t.Fatalf("batches = %v, want one of Begin, Insert, Commit", batches)
	}

	// What the client sent: a status update carrying 0x08, then CopyDone.
	rd := pgwire.NewReader(&rw.sent)
	m, err := rd.Next()
	if err != nil || m.Type != pgwire.FrontendCopyData || m.Body[0] != 'r' {
		t.Fatalf("first message sent = %q %q, %v; want a status update", m.Type, m.Body, err)
	}
	for i := range 3 {
		if got := binary.BigEndian.Uint64(m.Body[1+8*i:]); got != 0x08 {
			t.Errorf("status position %d = %#x, want the acknowledged 0x08", i, got)
		}
	}
	if m, err = rd.Next(); err != nil || m.Type != pgwire.FrontendCopyDone {
		t.Fatalf("second message sent = %q, %v; want CopyDone", m.Type, err)
	}
	if !r.done {
		t.Error("the stream did not record that copy mode ended")
	}
}

// A second call to Changes is refused: one connection streams a slot once.
func TestReplicationChangesOnce(t *testing.T) {
	r := &Replication{c: NewConn(&scriptedRW{Reader: bytes.NewReader(nil)}), batch: 1, interval: time.Hour}
	r.started.Store(true)
	if _, err := r.Changes(t.Context()).Collect(); err == nil {
		t.Error("a second Changes was accepted")
	}
}
