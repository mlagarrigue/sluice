package postgres

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// What the register cited neighbouring tests for: the
// pgoutput fields, the copy-both end and refuseCopyIn's CopyFail. Each goes red
// under the mutant its doc comment names; the test the register cited for the
// same requirement stayed green under it.

func originMsg(at LSN, name string) []byte {
	b := binary.BigEndian.AppendUint64([]byte{'O'}, uint64(at))
	return append(append(b, name...), 0)
}

// 54.9 Relation: namespace, replica identity, column type OID and atttypmod
// are all read into the Relation. TestReplDecoderTransaction reads only the
// name and the key flag.
//
// Mutants (pgoutput.go relation), one each: `rel.Namespace = r.cstring(…)`
// → `_ = r.cstring(…)`; the same for ReplicaIdentity, col.TypeOID, col.TypeMod.
func TestReplProofRelationFieldsAreRead(t *testing.T) {
	changes, err := decodeAll(t,
		relMsg(42, "public", "orders", "id", "note"),
		rowMsg('I', 42, byte('N'), tupleData(i8(1), []byte("a"))),
	)
	if err != nil {
		t.Fatal(err)
	}
	rel := changes[0].Relation
	if rel.Namespace != "public" {
		t.Errorf("Namespace = %q, want public", rel.Namespace)
	}
	if rel.ReplicaIdentity != 'd' {
		t.Errorf("ReplicaIdentity = %q, want d", rel.ReplicaIdentity)
	}
	for i, c := range rel.Columns {
		if c.TypeOID != 20 || c.TypeMod != -1 {
			t.Errorf("column %d: TypeOID = %d, TypeMod = %d, want 20 and -1", i, c.TypeOID, c.TypeMod)
		}
	}
}

// 54.9 Update 'O', Delete 'K', TupleData 'u' and 't', none built by
// TestReplDecoderTransaction or TestReplDecoderRefuses.
//
// Mutants (pgoutput.go): `c.OldIsKey = part == 'K'` → `part == 'K' && typ != 'D'`
// (a Delete's key lost); → `part == 'K' || (typ == 'U' && part == 'O')`;
// `v.Status = ValueUnchanged` → `ValueNull`; `if kind == 't' {` → `if false {`.
func TestReplProofOldPartKindsAndTupleValueKinds(t *testing.T) {
	text := []byte{0, 1, 't', 0, 0, 0, 1, 'x'}
	changes, err := decodeAll(t,
		relMsg(1, "s", "t", "a"),
		rowMsg('U', 1, byte('O'), tupleData(i8(1)), byte('N'), []byte{0, 1, 'u'}),
		rowMsg('D', 1, byte('K'), tupleData(i8(1))),
		rowMsg('I', 1, byte('N'), text),
	)
	if err != nil {
		t.Fatal(err)
	}
	if u := changes[0]; u.Kind != ChangeUpdate || u.OldIsKey || len(u.Old) != 1 || u.New[0].Status != ValueUnchanged {
		t.Errorf("Update with O = %+v, want an old row that is not the key and an unchanged value", u)
	}
	if d := changes[1]; d.Kind != ChangeDelete || !d.OldIsKey {
		t.Errorf("Delete with K = %+v, want OldIsKey", d)
	}
	if ins := changes[2]; ins.New[0].Status != ValueText || string(ins.New[0].Data) != "x" {
		t.Errorf("Insert with a 't' value = %+v, want ValueText x", ins.New[0])
	}
}

// 54.9 Update: the new tuple is introduced by 'N', and an Update whose part
// is anything else is refused. TestReplDecoderRefuses has no Update line.
//
// Mutant (pgoutput.go): `case part != 'N':` → `case part != 'N' && typ == 'I':`.
func TestReplProofAnUpdateWithoutNIsRefused(t *testing.T) {
	rel := relMsg(1, "s", "t", "a")
	for name, msgs := range map[string][][]byte{
		"a part that is not N after the key":  {rel, rowMsg('U', 1, byte('K'), tupleData(i8(1)), byte('X'), tupleData(i8(2)))},
		"a part that is not N, nothing older": {rel, rowMsg('U', 1, byte('X'), tupleData(i8(2)))},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeAll(t, msgs...); !errors.Is(err, ErrProtocol) {
				t.Errorf("decode = %v, want ErrProtocol", err)
			}
		})
	}
}

// 54.9 Origin: several can precede one transaction's changes, and each is
// delivered. TestReplicationDeliversTypeAndOrigin builds one.
//
// Mutant (pgoutput.go, case 'O'): the Change is appended only when no
// ChangeOrigin is already in dst.
func TestReplProofEveryOriginIsDelivered(t *testing.T) {
	changes, err := decodeAll(t,
		beginMsg(0x30, 1),
		originMsg(0x10, "a"),
		originMsg(0x20, "b"),
		commitMsg(0x30, 0x38),
	)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, c := range changes {
		if c.Kind == ChangeOrigin {
			names = append(names, c.Origin)
		}
	}
	if strings.Join(names, ",") != "a,b" {
		t.Errorf("origins delivered = %v, want [a b]", names)
	}
}

// 54.2.6 copy-both: "the frontend should treat ErrorResponse as
// terminating the copy in both directions and send no CopyDone".
// TestReplicationStreamScripted scripts a CopyDone from the server only.
//
// Mutant (replicate.go, case BackendErrorResponse): a FrontendCopyDone is
// written and flushed before breakConn.
func TestReplProofErrorResponseInCopyBothSendsNoCopyDone(t *testing.T) {
	var script []byte
	script = append(script, xlog(0x10, beginMsg(0x30, 1))...)
	script = append(script, beErrorResponse("57P01", "terminating connection due to administrator command")...)
	rw := &scriptedRW{Reader: bytes.NewReader(script)}
	r := &Replication{c: NewConn(rw, Config{}), batch: 100, batchBytes: defaultBatchBytes, interval: time.Hour}

	src := r.Changes(t.Context())
	src.Stream()(func(sluice.Batch[Change]) bool { return true })
	if src.Err() == nil {
		t.Fatal("the stream ended without an error after an ErrorResponse")
	}
	rd := pgwire.NewReader(&rw.sent)
	for {
		m, err := rd.Next()
		if err != nil {
			break
		}
		if m.Type == pgwire.FrontendCopyDone {
			t.Errorf("the client sent CopyDone after the server's ErrorResponse")
		}
	}
}

// 54.7 CopyFail: 'f', Int32, String error message. The CopyFail that
// refuseCopyIn sends is a well-formed C string carrying its reason; the
// tests cited for it read only the message's type.
//
// Mutant (copy.go refuseCopyIn): the body becomes `[]byte("x")`, no
// terminator and no reason.
func TestReplProofRefusedCopyInSendsAWellFormedCopyFail(t *testing.T) {
	var first []byte
	first = append(first, beSimple(pgwire.BackendParseComplete)...)
	first = append(first, beSimple(pgwire.BackendBindComplete)...)
	first = append(first, frame(pgwire.BackendCopyInResponse, "\x01\x00\x01\x00\x01")...)
	srv := &fakeServer{responses: [][]byte{
		first,
		beErrorResponse("57014", "COPY from stdin failed"),
		beReadyForQuery(),
	}}
	conn := NewConn(srv, Config{})
	_ = drain(t, conn.Query(t.Context(), "COPY t FROM STDIN", nil, QueryConfig{BatchRows: 4}))

	rd := pgwire.NewReader(bytes.NewReader(srv.sent.Bytes()))
	found := false
	for {
		m, err := rd.Next()
		if err != nil {
			break
		}
		if m.Type == pgwire.FrontendCopyFail {
			found = true
			if string(m.Body) != copyFailReason+"\x00" {
				t.Errorf("CopyFail body = %q, want the reason and its terminator", m.Body)
			}
		}
	}
	if !found {
		t.Error("no CopyFail was sent")
	}
}
