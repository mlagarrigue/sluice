package postgres

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
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
	r := &Replication{c: NewConn(rw, Config{}), batch: 100, batchBytes: defaultBatchBytes, interval: time.Hour}
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
	r := &Replication{c: NewConn(&scriptedRW{Reader: bytes.NewReader(nil)}, Config{}), batch: 1, batchBytes: defaultBatchBytes, interval: time.Hour}
	r.started.Store(true)
	if _, err := r.Changes(t.Context()).Collect(); err == nil {
		t.Error("a second Changes was accepted")
	}
}

// noChangeLimit is how much payload the scripted server below hands out
// before it gives up: more than the byte bound a batch carries, so a batch
// that honours the bound closes long before the server runs out, and one
// that does not is caught rather than left running.
const noChangeLimit = defaultBatchBytes + 16<<20

var errNoChangeExhausted = errors.New("test: the server gave up after the limit")

// noChangeServer feeds an endless stream of CopyData/XLogData messages whose
// pgoutput payload carries no change at all, and counts the payload bytes it
// handed out.
type noChangeServer struct {
	payload []byte // the pgoutput message, repeated
	buf     []byte
	given   int
}

func (s *noChangeServer) Read(p []byte) (int, error) {
	if len(s.buf) == 0 {
		if s.given >= noChangeLimit {
			return 0, errNoChangeExhausted
		}
		body := make([]byte, 0, 25+len(s.payload))
		body = append(body, 'w')
		body = binary.BigEndian.AppendUint64(body, uint64(s.given)+1) // WAL start
		body = binary.BigEndian.AppendUint64(body, uint64(s.given)+1) // WAL end
		body = binary.BigEndian.AppendUint64(body, 0)                 // server clock
		body = append(body, s.payload...)
		s.buf = append(s.buf, pgwire.BackendCopyData, 0, 0, 0, 0)
		binary.BigEndian.PutUint32(s.buf[1:5], uint32(len(body)+4)) //nolint:gosec // G115: a message this test builds
		s.buf = append(s.buf, body...)
		s.given += len(s.payload)
	}
	n := copy(p, s.buf)
	s.buf = s.buf[n:]
	return n, nil
}

func (s *noChangeServer) Write(p []byte) (int, error) { return len(p), nil }
func (s *noChangeServer) Close() error                { return nil }

// sameRelationPayload is a valid Relation message ('R') for one fixed OID,
// made wide by a long relation name. Sent more than once, every message
// after the first describes an OID the decoder already knows, so the
// relation bound never counts it.
func sameRelationPayload(n int) []byte {
	name := bytes.Repeat([]byte{'a'}, n)
	p := []byte{'R', 0, 0, 0, 0}
	binary.BigEndian.PutUint32(p[1:5], 42) // the same OID every time
	p = append(p, 's', 0)                  // namespace
	p = append(p, name...)                 // relation name
	p = append(p, 0)
	p = append(p, 'd')    // replica identity
	p = append(p, 0, 1)   // one column
	p = append(p, 0)      // flags: not a key
	p = append(p, 'c', 0) // column name
	p = binary.BigEndian.AppendUint32(p, 23)
	p = binary.BigEndian.AppendUint32(p, 0xffffffff)
	return p
}

// A batch that holds no change short-circuits the loop's other exit — "the
// next message is not already received" — so before ReplicationConfig.
// BatchBytes existed the read had no stopping condition at all. copyData
// copies every payload into the batch's arena before asking the decoder what
// it carries, and a Relation redescribing a relation already known carries
// no change (Type and Origin did too, before they were delivered). A server
// sending an uninterrupted run of them grew the arena at whatever rate it
// chose, and the batch could never close. It closes at the bound, empty.
func TestReplicationBoundsPayloadThatCarriesNoChange(t *testing.T) {
	for _, c := range []struct {
		name    string
		payload []byte
	}{
		{"the same relation redescribed", sameRelationPayload(64 << 10)},
	} {
		t.Run(c.name, func(t *testing.T) {
			srv := &noChangeServer{payload: c.payload}
			r := &Replication{
				c: NewConn(srv, Config{}), batch: sluice.DefaultBatchSize,
				batchBytes: defaultBatchBytes, interval: time.Minute,
			}
			b, err := r.readBatch()
			arena := 0
			for _, chunk := range r.arena {
				arena += cap(chunk)
			}
			if errors.Is(err, errNoChangeExhausted) {
				t.Fatalf("readBatch held %d bytes of payload in a batch of %d changes without closing it (arena %d bytes): no bound stops a batch that carries no change",
					srv.given, len(b.Items), arena)
			}
			if err != nil || len(b.Items) != 0 {
				t.Fatalf("readBatch returned %d changes and %v after %d bytes without a change; want an empty batch, closed at the bound", len(b.Items), err, srv.given)
			}
			if srv.given > defaultBatchBytes+len(c.payload) {
				t.Errorf("readBatch read %d bytes before closing the batch, more than the bound of %d plus one message", srv.given, defaultBatchBytes)
			}
		})
	}
}

// A connection that once met a large batch kept its peak for good: readBatch
// truncated arena[0] and left every other chunk allocated. It is the rule
// Rows.reset keeps, and for the same reason — a pool of a hundred
// connections that each met one large batch held that peak a hundred times.
func TestReplicationGivesBackAnOversizedArena(t *testing.T) {
	r := &Replication{batchBytes: defaultBatchBytes}
	// A batch that held well over the retained size.
	r.arena = [][]byte{make([]byte, 0, 2*rowsRetainBytes)}
	r.arena[0] = r.arena[0][:2*rowsRetainBytes]
	r.kept = 2 * rowsRetainBytes
	held := func() (n int) {
		for _, c := range r.arena {
			n += cap(c)
		}
		return n
	}
	if held() <= rowsRetainBytes {
		t.Fatalf("the arena starts at %d bytes, not over the retained %d; the sequence under test cannot be reached", held(), rowsRetainBytes)
	}
	// Narrow batches from here on: the first of them is the one that held
	// the large payload, and the count starts after it.
	for i := range rowsShrinkAfter + 1 {
		r.resetArena()
		r.kept = 1 << 10
		if i < rowsShrinkAfter-1 && held() <= rowsRetainBytes {
			t.Fatalf("the arena was given back after %d narrow batches, before the %d the rule asks for", i+1, rowsShrinkAfter)
		}
	}
	if n := held(); n > rowsRetainBytes {
		t.Errorf("the arena still holds %d bytes after %d narrow batches, over the retained %d", n, rowsShrinkAfter+1, rowsRetainBytes)
	}
}

// The relation table is bounded in bytes as well as in count: a Relation
// message is bounded only by Config.MaxMessage, so the count alone let a
// server keep gigabytes of names in the table for the life of the stream
// (64 relations with 4 MiB names kept: 256 MiB, measured). A relation past
// the budget is ErrProtocol; a redescription of a known OID is charged the
// difference, so a publication whose tables change shape is not charged
// twice; and a table of ordinary shape decodes as before.
func TestReplicationBoundsTheRelationTableInBytes(t *testing.T) {
	t.Run("names past the budget", func(t *testing.T) {
		var d replDecoder
		name := strings.Repeat("n", 4<<20)
		var err error
		kept := 0
		for oid := uint32(1); oid <= 64 && err == nil; oid++ {
			_, _, _, err = d.decode(relMsg(oid, "public", name, "id"), 100, nil, nil, nil)
			if err == nil {
				kept++
			}
		}
		if !errors.Is(err, ErrProtocol) {
			t.Fatalf("64 relations of 4 MiB names: err = %v, %d kept, want ErrProtocol once the budget is passed", err, kept)
		}
		if kept > maxReplicationRelationBytes/(4<<20) {
			t.Errorf("%d relations of 4 MiB kept before the refusal, past the %d-byte budget", kept, maxReplicationRelationBytes)
		}
	})
	t.Run("a redescription is charged the difference", func(t *testing.T) {
		var d replDecoder
		name := strings.Repeat("n", maxReplicationRelationBytes*3/4)
		if _, _, _, err := d.decode(relMsg(1, "public", name, "id"), 100, nil, nil, nil); err != nil {
			t.Fatal(err)
		}
		// Described again under the same OID, a column added: the table
		// holds one relation, not two, and stays under the budget.
		if _, _, _, err := d.decode(relMsg(1, "public", name, "id", "name"), 101, nil, nil, nil); err != nil {
			t.Fatalf("redescribing the relation: %v (charged as a second relation?)", err)
		}
		if len(d.relations) != 1 || d.relationBytes > len(name)+len("public")+2*(len("id")+relationColumnCost)+len("name") {
			t.Errorf("table holds %d relations, %d bytes; want one relation at its own cost", len(d.relations), d.relationBytes)
		}
		// A second OID of the same size is the one past the budget.
		if _, _, _, err := d.decode(relMsg(2, "public", name, "id"), 102, nil, nil, nil); !errors.Is(err, ErrProtocol) {
			t.Errorf("a second relation of three quarters of the budget: err = %v, want ErrProtocol", err)
		}
	})
	t.Run("an ordinary publication fits", func(t *testing.T) {
		var d replDecoder
		cols := make([]string, 100)
		for i := range cols {
			cols[i] = fmt.Sprintf("column_%d_of_a_wide_table", i)
		}
		for oid := uint32(1); oid <= 1000; oid++ {
			if _, _, _, err := d.decode(relMsg(oid, "public", fmt.Sprintf("table_%d", oid), cols...), 100, nil, nil, nil); err != nil {
				t.Fatalf("table %d of a thousand hundred-column tables: %v", oid, err)
			}
		}
		if d.relationBytes > maxReplicationRelationBytes/8 {
			t.Errorf("a thousand tables of a hundred columns cost %d bytes, more than an eighth of the %d-byte budget", d.relationBytes, maxReplicationRelationBytes)
		}
	})
}

// pgoutput sends a table's Relation inside the transaction, before its first
// change, and a batch may open on it. A Relation wider than
// ReplicationConfig.BatchBytes is a conformant message under a small bound:
// the batch keeps the message that crosses its bound and closes after it,
// as QueryConfig.BatchBytes keeps the row that crosses its own. It closes
// empty, and the relation is known for the changes that follow.
func TestReplicationOpensABatchOnARelationWiderThanBatchBytes(t *testing.T) {
	rel := relMsg(42, "public", "t", "a", "b")
	rw := &scriptedRW{Reader: bytes.NewReader(xlog(0x100, rel))}
	r := &Replication{c: NewConn(rw, Config{}), batch: 100, batchBytes: 8, interval: time.Hour}
	b, err := r.readBatch()
	if err != nil {
		t.Fatalf("a %d-byte Relation under BatchBytes = 8: %v", len(rel), err)
	}
	if len(b.Items) != 0 {
		t.Fatalf("a batch of %d changes from a lone Relation, want none", len(b.Items))
	}
	if r.dec.relations[42] == nil {
		t.Fatal("the Relation that opened the batch is not known afterwards")
	}
}

// PostgreSQL 18 §54.9: a Truncate carries "Option bits for TRUNCATE: 1 for
// CASCADE, 2 for RESTART IDENTITY", and both change what replaying it
// elsewhere does. They reach the Change.
func TestReplDecoderKeepsTheTruncateOptions(t *testing.T) {
	for _, tc := range []struct {
		bits             byte
		cascade, restart bool
	}{{0, false, false}, {1, true, false}, {2, false, true}, {3, true, true}} {
		truncate := binary.BigEndian.AppendUint32([]byte{'T'}, 1)
		truncate = binary.BigEndian.AppendUint32(append(truncate, tc.bits), 42)
		changes, err := decodeAll(t, relMsg(42, "public", "orders", "id"), truncate)
		if err != nil {
			t.Fatal(err)
		}
		if len(changes) != 1 || changes[0].Kind != ChangeTruncate {
			t.Fatalf("bits %d: %d changes, want one Truncate", tc.bits, len(changes))
		}
		if c := changes[0]; c.Cascade != tc.cascade || c.RestartIdentity != tc.restart {
			t.Errorf("bits %d: Cascade=%v RestartIdentity=%v, want %v %v", tc.bits, c.Cascade, c.RestartIdentity, tc.cascade, tc.restart)
		}
	}
}

// ReplicationConfig.BatchChanges is the most changes one batch holds, and a
// Truncate brings a Change per relation it names in one message: a Truncate
// of 25 tables under a bound of 10 arrives as batches of 10, 10 and 5, every
// relation once and in order, also when the server leaves copy mode right
// after it. The count used to be compared between messages only, and the
// first batch held all 25.
func TestReplicationSplitsATruncateAtBatchChanges(t *testing.T) {
	const relations, batch = 25, 10
	var script []byte
	truncate := binary.BigEndian.AppendUint32([]byte{'T'}, relations)
	truncate = append(truncate, 0)
	for i := range relations {
		oid := uint32(1000 + i) //nolint:gosec // G115: a small loop index
		script = append(script, xlog(LSN(0x100+i), relMsg(oid, "s", fmt.Sprintf("t%d", i), "a"))...)
		truncate = binary.BigEndian.AppendUint32(truncate, oid)
	}
	script = append(script, xlog(0x900, truncate)...)
	script = append(script, frame(pgwire.BackendCopyDone, "")...)
	script = append(script, frame(pgwire.BackendCommandComplete, "START_REPLICATION\x00")...)
	script = append(script, frame(pgwire.BackendReadyForQuery, "I")...)
	r := &Replication{c: NewConn(&scriptedRW{Reader: bytes.NewReader(script)}, Config{}), batch: batch, batchBytes: defaultBatchBytes, interval: time.Hour}

	var sizes []int
	var names []string
	src := r.Changes(t.Context())
	src.Stream()(func(b sluice.Batch[Change]) bool {
		sizes = append(sizes, b.Len())
		for _, c := range b.Items {
			if c.Kind != ChangeTruncate {
				t.Errorf("a %v change in a stream of one Truncate", c.Kind)
			}
			names = append(names, c.Relation.Name)
		}
		return true
	})
	if err := src.Err(); err != nil {
		t.Fatal(err)
	}
	if want := []int{10, 10, 5}; !slices.Equal(sizes, want) {
		t.Errorf("batches of %v changes under BatchChanges = %d, want %v", sizes, batch, want)
	}
	if len(names) != relations {
		t.Fatalf("%d relations truncated, want %d", len(names), relations)
	}
	for i, n := range names {
		if want := fmt.Sprintf("t%d", i); n != want {
			t.Errorf("change %d truncates %s, want %s: a carried change lost its place", i, n, want)
		}
	}
}

// §54.4: XLogData and the primary keepalive each carry "the current end of
// WAL on the server" — the one figure the stream offers for how far behind
// the consumer is. Replication.ServerWALEnd returns the latest.
func TestReplicationReportsTheServersEndOfWAL(t *testing.T) {
	w := binary.BigEndian.AppendUint64([]byte{'w'}, 0x100)
	w = binary.BigEndian.AppendUint64(w, 0x9000)
	w = binary.BigEndian.AppendUint64(w, 0)
	w = append(w, beginMsg(0x10, 7)...)
	k := binary.BigEndian.AppendUint64([]byte{'k'}, 0x9500)
	k = append(binary.BigEndian.AppendUint64(k, 0), 0)
	r := &Replication{batch: 100, batchBytes: defaultBatchBytes, interval: time.Hour}
	if got := r.ServerWALEnd(); got != 0 {
		t.Fatalf("ServerWALEnd before any message = %v, want 0", got)
	}
	if err := r.copyData(w); err != nil {
		t.Fatal(err)
	}
	if got := r.ServerWALEnd(); got != 0x9000 {
		t.Fatalf("ServerWALEnd after an XLogData = %v, want %v", got, LSN(0x9000))
	}
	if err := r.copyData(k); err != nil {
		t.Fatal(err)
	}
	if got := r.ServerWALEnd(); got != 0x9500 {
		t.Fatalf("ServerWALEnd after a keepalive = %v, want %v", got, LSN(0x9500))
	}
}

// PostgreSQL 18 §54.9: a Type message names a type that is not built in
// (OID, namespace, name) and an Origin message the node a transaction came
// from (its commit LSN there, its name), "multiple Origin messages inside a
// single transaction" allowed. Both reach the consumer as a Change, the
// stream's one rule: what is read is delivered. A replica that applies a row
// of an enum column needs the type's name, since the OID is the publisher's.
func TestReplicationDeliversTypeAndOrigin(t *testing.T) {
	typ := binary.BigEndian.AppendUint32([]byte{'Y'}, 16385)
	typ = append(typ, "public\x00mood\x00"...)
	origin := binary.BigEndian.AppendUint64([]byte{'O'}, 0x5000)
	origin = append(origin, "node_b\x00"...)
	enumRel := relMsg(42, "public", "people", "name")
	changes, err := decodeAll(t, beginMsg(0x60, 7), origin, typ, enumRel, commitMsg(0x60, 0x68))
	if err != nil {
		t.Fatal(err)
	}
	var kinds []ChangeKind
	for _, c := range changes {
		kinds = append(kinds, c.Kind)
	}
	if want := []ChangeKind{ChangeBegin, ChangeOrigin, ChangeType, ChangeCommit}; !slices.Equal(kinds, want) {
		t.Fatalf("kinds = %v, want %v", kinds, want)
	}
	if c := changes[1]; c.Origin != "node_b" || c.LSN != 0x5000 {
		t.Errorf("Origin = %q at %v, want node_b at 0/5000", c.Origin, c.LSN)
	}
	if got, want := changes[2].Type, (DataType{OID: 16385, Namespace: "public", Name: "mood"}); got != want {
		t.Errorf("Type = %+v, want %+v", got, want)
	}

	// A truncated message is refused, not delivered half-read.
	if _, err := decodeAll(t, typ[:7]); !errors.Is(err, ErrProtocol) {
		t.Errorf("a Type message cut after its OID: err = %v, want ErrProtocol", err)
	}
}

// The decoder's refusals say which rule the message broke, so that a test
// of one is not passed by another tripping later in the same message.
func TestReplDecoderErrorsSayWhatTheyFound(t *testing.T) {
	rel := relMsg(1, "s", "t", "a")
	oneColumn := []byte{1, 'a', 0, 0, 0, 0, 20, 0xFF, 0xFF, 0xFF, 0xFF}
	for _, tc := range []struct {
		name string
		msgs [][]byte
		want string
	}{
		{"a Delete without its old row", [][]byte{rel, rowMsg('D', 1, byte('N'), tupleData(i8(1)))}, "pgoutput Delete with part 'N'"},
		{"an Insert without N", [][]byte{rel, rowMsg('I', 1, byte('K'), tupleData(i8(1)))}, "where the new tuple belongs"},
		{
			"more columns than the body",
			[][]byte{append([]byte{'R', 0, 0, 0, 1, 's', 0, 't', 0, 'd', 0, 2}, oneColumn...)},
			"declares 2 columns in 11 bytes",
		},
	} {
		if _, err := decodeAll(t, tc.msgs...); !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want ErrProtocol saying %q", tc.name, err, tc.want)
		}
	}
	trunc := binary.BigEndian.AppendUint32([]byte{'T'}, 2)
	trunc = binary.BigEndian.AppendUint32(append(trunc, 0), 7)
	if _, err := decodeAll(t, trunc); !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "names 2 relations in 4 bytes") {
		t.Errorf("a Truncate count past its bytes: %v", err)
	}
}

// The relation table's budget may be reached exactly, and a relation costs
// its names and relationColumnCost per column.
func TestReplDecoderRelationBudgetAtItsBound(t *testing.T) {
	cost := (&Relation{Namespace: "s", Name: "t", Columns: []RelationColumn{{Name: "a"}}}).cost()
	if cost != 1+1+1+relationColumnCost {
		t.Fatalf("cost = %d, want %d", cost, 3+relationColumnCost)
	}
	d := replDecoder{relationBytes: maxReplicationRelationBytes - cost}
	if _, _, _, err := d.decode(relMsg(1, "s", "t", "a"), 0, nil, nil, nil); err != nil {
		t.Fatalf("a relation that reaches the budget exactly: %v", err)
	}
}

// The reader takes a field that is exactly what is left, and a byte string
// of no bytes.
func TestReplReaderAtTheEndOfTheBody(t *testing.T) {
	r := replReader{b: []byte{0, 5}}
	if v := r.u16("x"); v != 5 || r.err != nil {
		t.Errorf("u16 of the last two bytes = %d, %v", v, r.err)
	}
	if v := r.bytes(0, "x"); len(v) != 0 || r.err != nil {
		t.Errorf("bytes(0) = %v, %v", v, r.err)
	}
}

func TestChangeKindString(t *testing.T) {
	for k, want := range map[ChangeKind]string{0: "ChangeKind(0)", ChangeBegin: "Begin", ChangeOrigin: "Origin", ChangeOrigin + 1: "ChangeKind(9)"} {
		if got := k.String(); got != want {
			t.Errorf("ChangeKind(%d).String() = %q, want %q", int(k), got, want)
		}
	}
}

// Replicate keeps what the caller configured and fills in only what was
// left at zero; BatchBytes may be the ceiling itself.
func TestReplicateKeepsTheConfiguredBounds(t *testing.T) {
	open := func(rc ReplicationConfig) *Replication {
		t.Helper()
		script := slices.Concat(beAuth(0, ""), beReady(), frame(pgwire.BackendCopyBothResponse, "\x00\x00\x00"))
		dial := func(context.Context) (io.ReadWriteCloser, error) {
			return &scriptedRW{Reader: bytes.NewReader(script)}, nil
		}
		rc.Slot, rc.Publications = "s", []string{"p"}
		r, err := Replicate(t.Context(), dial, Config{User: "u"}, rc)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	if r := open(ReplicationConfig{StatusInterval: time.Second, BatchChanges: 7, BatchBytes: maxBatchBytes}); r.interval != time.Second || r.batch != 7 || r.batchBytes != maxBatchBytes {
		t.Errorf("configured: interval %v, batch %d, bytes %d", r.interval, r.batch, r.batchBytes)
	}
	if r := open(ReplicationConfig{}); r.interval != defaultStatusInterval || r.batchBytes != defaultBatchBytes {
		t.Errorf("defaults: interval %v, bytes %d", r.interval, r.batchBytes)
	}
}

// A stream stopped by its ctx says so, wrapping the cause rather than
// replacing the message.
func TestReplicationStoppedByItsContextSaysSo(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	r := &Replication{c: NewConn(client, Config{}), batch: 10, batchBytes: defaultBatchBytes, interval: time.Hour}
	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(10*time.Millisecond, cancel)
	src := r.Changes(ctx)
	src.Stream()(func(sluice.Batch[Change]) bool { return true })
	if err := src.Err(); !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "replication stopped") {
		t.Fatalf("Err = %v, want context.Canceled under \"replication stopped\"", err)
	}
}

// A stream that ends without a change yields no batch, and a batch is cut
// at BatchChanges and at BatchBytes exactly; a change past the cut, row
// included, opens the next batch whole.
func TestReplicationCutsBatchesExactly(t *testing.T) {
	end := slices.Concat(frame(pgwire.BackendCopyDone, ""), frame(pgwire.BackendCommandComplete, "START_REPLICATION\x00"), beReadyForQuery())
	run := func(t *testing.T, batch, batchBytes int, msgs ...[]byte) [][]Change {
		t.Helper()
		var script []byte
		for _, m := range msgs {
			script = append(script, xlog(0x10, m)...)
		}
		script = append(script, end...)
		r := &Replication{c: NewConn(&scriptedRW{Reader: bytes.NewReader(script)}, Config{}), batch: batch, batchBytes: batchBytes, interval: time.Hour}
		var batches [][]Change
		src := r.Changes(t.Context())
		src.Stream()(func(b sluice.Batch[Change]) bool {
			batches = append(batches, slices.Clone(b.Items))
			return true
		})
		if err := src.Err(); err != nil {
			t.Fatal(err)
		}
		return batches
	}

	if got := run(t, 10, defaultBatchBytes); len(got) != 0 {
		t.Errorf("a stream of no change yielded %d batches", len(got))
	}

	rel := relMsg(1, "s", "t", "a")
	ins := func(v int64) []byte { return rowMsg('I', 1, byte('N'), tupleData(i8(v))) }
	got := run(t, 2, defaultBatchBytes, rel, ins(1), ins(2), ins(3))
	if len(got) != 2 || len(got[0]) != 2 || len(got[1]) != 1 {
		t.Fatalf("batches of %v, want 2 then 1", got)
	}
	if v := got[1][0].New; len(v) != 1 || binary.BigEndian.Uint64(v[0].Data) != 3 {
		t.Errorf("the change that opened the second batch carries %v, want its row", v)
	}

	got = run(t, 10, len(rel)+len(ins(1)), rel, ins(1), ins(2))
	if len(got) != 2 || len(got[0]) != 1 {
		t.Errorf("batches of %v under a byte bound the first row reaches exactly, want it alone first", got)
	}
}

// The arena takes a payload into a chunk it fits exactly, opens a new chunk
// for one it does not, and its retention follows the rows' rule: a store of
// exactly the retained size is never given back, a batch of exactly the
// retained size counts towards giving a larger one back.
func TestReplicationArenaAtItsEdges(t *testing.T) {
	r := &Replication{arena: [][]byte{make([]byte, 0, 10)}}
	r.keep(make([]byte, 6))
	r.keep(make([]byte, 4))
	if len(r.arena) != 1 || r.chunk != 0 {
		t.Errorf("a payload that fits the chunk exactly opened chunk %d of %d", r.chunk, len(r.arena))
	}
	r.keep(make([]byte, 1))
	if len(r.arena) != 2 || cap(r.arena[0]) != 10 {
		t.Errorf("a payload past the chunk: %d chunks, the first of cap %d", len(r.arena), cap(r.arena[0]))
	}

	r = &Replication{arena: [][]byte{make([]byte, 0, rowsRetainBytes)}}
	kept := &r.arena[0][:1][0]
	for range 2 * rowsShrinkAfter {
		r.kept = 8
		r.resetArena()
	}
	if len(r.arena) == 0 || &r.arena[0][:1][0] != kept {
		t.Error("an arena of exactly the retained size was given back")
	}

	r = &Replication{arena: [][]byte{make([]byte, 0, 4*rowsRetainBytes)}}
	for range rowsShrinkAfter {
		r.kept = rowsRetainBytes
		r.resetArena()
	}
	if len(r.arena) != 0 {
		t.Error("an oversized arena outlived batches of exactly the retained size")
	}
}

// An XLogData of its header alone reaches the decoder, which refuses the
// empty message.
func TestReplicationXLogDataOfItsHeaderAlone(t *testing.T) {
	r := &Replication{}
	p := append([]byte{'w'}, make([]byte, 24)...)
	if err := r.copyData(p); !errors.Is(err, ErrProtocol) || strings.Contains(err.Error(), "XLogData of") {
		t.Errorf("copyData of a bare header = %v, want the decoder's refusal", err)
	}
}

// A status report goes when the acknowledged position moved and the
// interval passed, not before; it carries the clock from 2000; and a stream
// the consumer stops reports and leaves copy mode.
func TestReplicationReportsWhenDue(t *testing.T) {
	report := func(acked, sent LSN, sentAt time.Time) []byte {
		rw := &scriptedRW{Reader: bytes.NewReader(nil)}
		r := &Replication{c: NewConn(rw, Config{}), interval: time.Hour, sent: sent, sentAt: sentAt}
		r.acked.Store(uint64(acked))
		if err := r.report(false); err != nil {
			t.Fatal(err)
		}
		return rw.sent.Bytes()
	}
	if sent := report(8, 0, time.Now().Add(-2*time.Hour)); len(sent) == 0 {
		t.Error("no report for a moved position past the interval")
	} else {
		at := int64(binary.BigEndian.Uint64(sent[5+1+24:]))
		if want := time.Now().UnixMicro() - pgEpochMicros; at > want || want-at > int64(time.Minute/time.Microsecond) {
			t.Errorf("report clock = %d, want about %d microseconds since 2000", at, want)
		}
	}
	if sent := report(8, 0, time.Now()); len(sent) != 0 {
		t.Error("a report sent before the interval passed")
	}
	if sent := report(8, 8, time.Now().Add(-2*time.Hour)); len(sent) != 0 {
		t.Error("a report sent for a position that did not move")
	}

	var script []byte
	script = append(script, xlog(0x10, beginMsg(0x30, 1))...)
	script = append(script, xlog(0x20, commitMsg(0x30, 0x38))...)
	script = append(script, frame(pgwire.BackendCopyDone, "")...)
	script = append(script, frame(pgwire.BackendCommandComplete, "START_REPLICATION\x00")...)
	script = append(script, beReadyForQuery()...)
	rw := &scriptedRW{Reader: bytes.NewReader(script)}
	r := &Replication{c: NewConn(rw, Config{}), batch: 1, batchBytes: defaultBatchBytes, interval: time.Hour}
	src := r.Changes(t.Context())
	src.Stream()(func(sluice.Batch[Change]) bool { return false })
	if err := src.Err(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(rw.sent.Bytes(), frame(pgwire.FrontendCopyDone, "")) {
		t.Error("a stream the consumer stopped did not leave copy mode")
	}
}
