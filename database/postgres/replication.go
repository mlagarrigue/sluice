package postgres

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// LSN is a position in the server's write-ahead log, the order every change
// is replicated in. It prints the way PostgreSQL does, as two hexadecimal
// halves: 16/B374D848.
type LSN uint64

// String formats the position as PostgreSQL does.
func (l LSN) String() string {
	return strconv.FormatUint(uint64(l>>32), 16) + "/" + strings.ToUpper(strconv.FormatUint(uint64(l)&0xFFFFFFFF, 16))
}

// ParseLSN reads a position written as PostgreSQL writes it, such as the
// confirmed_flush_lsn column of pg_replication_slots.
func ParseLSN(s string) (LSN, error) {
	hi, lo, ok := strings.Cut(s, "/")
	if !ok {
		return 0, fmt.Errorf("postgres: LSN %q is not two hexadecimal halves separated by a slash", s)
	}
	h, err := strconv.ParseUint(hi, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("postgres: LSN %q: %w", s, err)
	}
	l, err := strconv.ParseUint(lo, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("postgres: LSN %q: %w", s, err)
	}
	return LSN(h<<32 | l), nil
}

// ChangeKind says what a [Change] reports.
type ChangeKind uint8

// The kinds of change a logical replication stream delivers. A transaction
// arrives as a Begin, its row changes, then a Commit; nothing of it arrives
// before the server has committed it.
const (
	ChangeBegin ChangeKind = iota + 1
	ChangeCommit
	ChangeInsert
	ChangeUpdate
	ChangeDelete
	ChangeTruncate
)

var changeKindNames = [...]string{"", "Begin", "Commit", "Insert", "Update", "Delete", "Truncate"}

func (k ChangeKind) String() string {
	if int(k) < len(changeKindNames) && k != 0 {
		return changeKindNames[k]
	}
	return "ChangeKind(" + strconv.Itoa(int(k)) + ")"
}

// Change is one message of a logical replication stream, decoded: the bounds
// of a transaction or one row it changed.
//
// Like a [Row], a Change borrows its batch: the bytes of its tuples are valid
// until the stream moves to the next batch, and a consumer that keeps a value
// past that copies it. Relation is the exception, and stays valid for good.
type Change struct {
	Kind ChangeKind

	// LSN is, for a Begin, the transaction's final position; for a Commit,
	// the commit's; for a row change, the position of the WAL data that
	// carried it.
	LSN LSN

	// EndLSN is set on a Commit: the end of the transaction, which is the
	// position to hand [Replication.Ack] once the transaction is processed.
	EndLSN LSN

	// Time is the commit timestamp, on a Begin and a Commit.
	Time time.Time

	// Xid is the transaction's identifier, on a Begin.
	Xid uint32

	// Relation describes the table a row change or a Truncate applies to,
	// as the server last described it.
	Relation *Relation

	// New is the row after an Insert or an Update. Old is the row before an
	// Update or a Delete, when the server sends it: the key columns only
	// when OldIsKey, the whole row under REPLICA IDENTITY FULL, and nil when
	// an Update left the key alone.
	New, Old Tuple
	OldIsKey bool
}

// Relation is a table as the replication stream describes it: the columns a
// tuple's values are in the order of, and their types.
type Relation struct {
	OID       uint32
	Namespace string // empty for pg_catalog
	Name      string

	// ReplicaIdentity is pg_class.relreplident: 'd' (the primary key), 'n'
	// (nothing), 'f' (the whole row) or 'i' (an index).
	ReplicaIdentity byte

	Columns []RelationColumn
}

// RelationColumn is one column of a [Relation].
type RelationColumn struct {
	Name    string
	TypeOID uint32
	TypeMod int32

	// Key marks a column of the replica identity.
	Key bool
}

// Tuple is a row's values, in the order of its relation's columns.
type Tuple []Value

// Value is one column of a [Tuple].
type Value struct {
	Status ValueStatus

	// Data is the value in the representation Status names; it is empty for
	// ValueNull and ValueUnchanged. A binary value decodes with the pgtype
	// function of its column's type.
	Data []byte
}

// ValueStatus says how a [Value] arrived.
type ValueStatus uint8

const (
	// ValueBinary is a value in the type's binary representation, which
	// [Replicate] asks the server for.
	ValueBinary ValueStatus = iota + 1
	// ValueText is a value in the type's text representation.
	ValueText
	// ValueNull is SQL NULL.
	ValueNull
	// ValueUnchanged is a TOASTed value an Update left alone: the server
	// does not send it again, and the consumer keeps the one it had.
	ValueUnchanged
)

// maxReplicationRelations bounds the relation table a stream keeps. A
// publication's tables are described once each and again when their shape
// changes, under the same OID, so the table grows with the tables published;
// a server describing more distinct relations than this is refused rather
// than followed into memory.
const maxReplicationRelations = 1 << 14

// maxReplicationRelationBytes bounds the relation table together: the
// namespace, name and column names of every relation kept, plus
// relationColumnCost per column for the [RelationColumn] itself.
//
// Capping the count alone is not enough. A Relation message is bounded only
// by [Config.MaxMessage], 64 MiB by default and up to 1 GiB, so the count
// alone bounds the table at 16,384 times that — a figure, not a bound — and
// a server can describe relations with names as long as a message for free,
// kept for the life of the stream (measured: 64 relations with 4 MiB names
// kept, 256 MiB). What PostgreSQL sends is small by construction: a name is
// at most 63 bytes (NAMEDATALEN) and a table has at most 1,600 columns
// (MaxHeapAttributeNumber), so a relation of ordinary shape costs a few
// hundred bytes and the whole of a large publication a few megabytes: a
// thousand tables of a hundred columns cost under 6 MiB (measured). The
// budget is 64 MiB, four times maxServerParameterBytes, so that the count
// bound is reachable by a conformant server before this one is: 16,384
// tables of thirty columns, FOR ALL TABLES on a very large schema, cost
// about 28 MiB.
const maxReplicationRelationBytes = 64 << 20

// relationColumnCost is what a [RelationColumn] costs the table beyond its
// name: the struct itself, the name's header included.
const relationColumnCost = 32

// cost is what the table keeps for rel: its names, and relationColumnCost
// per column.
func (rel *Relation) cost() int {
	n := len(rel.Namespace) + len(rel.Name)
	for i := range rel.Columns {
		n += len(rel.Columns[i].Name) + relationColumnCost
	}
	return n
}

// pgEpochMicros is 2000-01-01 in Unix microseconds, the origin the protocol
// counts its timestamps from.
const pgEpochMicros = 946_684_800_000_000

// replDecoder turns pgoutput messages (protocol version 1) into changes. It
// keeps the relation table the row changes refer to.
type replDecoder struct {
	relations map[uint32]*Relation
	// relationBytes is the cost of every relation in the table, bounded by
	// maxReplicationRelationBytes.
	relationBytes int
}

// replReader walks one message body; every read checks what remains.
type replReader struct {
	b   []byte
	err error
}

func (r *replReader) fail(what string) {
	if r.err == nil {
		r.err = fmt.Errorf("%w: pgoutput message truncated in %s", ErrProtocol, what)
	}
}

func (r *replReader) u8(what string) byte {
	if len(r.b) < 1 {
		r.fail(what)
		return 0
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v
}

func (r *replReader) u16(what string) uint16 {
	if len(r.b) < 2 {
		r.fail(what)
		return 0
	}
	v := binary.BigEndian.Uint16(r.b)
	r.b = r.b[2:]
	return v
}

func (r *replReader) u32(what string) uint32 {
	if len(r.b) < 4 {
		r.fail(what)
		return 0
	}
	v := binary.BigEndian.Uint32(r.b)
	r.b = r.b[4:]
	return v
}

func (r *replReader) u64(what string) uint64 {
	if len(r.b) < 8 {
		r.fail(what)
		return 0
	}
	v := binary.BigEndian.Uint64(r.b)
	r.b = r.b[8:]
	return v
}

func (r *replReader) cstring(what string) string {
	i := 0
	for i < len(r.b) && r.b[i] != 0 {
		i++
	}
	if i == len(r.b) {
		r.fail(what)
		return ""
	}
	s := string(r.b[:i])
	r.b = r.b[i+1:]
	return s
}

func (r *replReader) bytes(n int, what string) []byte {
	if n < 0 || len(r.b) < n {
		r.fail(what)
		return nil
	}
	v := r.b[:n:n]
	r.b = r.b[n:]
	return v
}

func pgTime(micros uint64) time.Time {
	return time.UnixMicro(int64(micros) + pgEpochMicros).UTC() //nolint:gosec // G115: the protocol field is a signed int64
}

// decode reads one pgoutput message. It appends the changes the message
// carries to dst — none for a Relation, Type or Origin, one per relation for
// a Truncate — and the tuples' values to vals, which the caller owns; a
// Change's tuples are left nil and described by the spans returned with it,
// so that vals may grow without leaving a Change pointing at a stale array.
func (d *replDecoder) decode(body []byte, at LSN, dst []Change, spans []tupleSpans, vals []Value) ([]Change, []tupleSpans, []Value, error) {
	r := replReader{b: body}
	typ := r.u8("the message type")
	switch typ {
	case 'B':
		c := Change{Kind: ChangeBegin}
		c.LSN = LSN(r.u64("Begin"))
		c.Time = pgTime(r.u64("Begin"))
		c.Xid = r.u32("Begin")
		if r.err == nil {
			dst, spans = append(dst, c), append(spans, tupleSpans{})
		}
	case 'C':
		r.u8("Commit")
		c := Change{Kind: ChangeCommit}
		c.LSN = LSN(r.u64("Commit"))
		c.EndLSN = LSN(r.u64("Commit"))
		c.Time = pgTime(r.u64("Commit"))
		if r.err == nil {
			dst, spans = append(dst, c), append(spans, tupleSpans{})
		}
	case 'R':
		d.relation(&r)
	case 'Y', 'O':
		// Type names a type that is not built in, and Origin the node a
		// transaction came from; both are read and dropped (ADR 0023). A
		// built-in type loses nothing: its OID is fixed and pgtype decodes
		// it. A non-built-in one — an enum, a domain, a composite — leaves
		// the column's TypeOID an OID of the publishing database, which the
		// Type message names and nothing here keeps; and without Origin a
		// cascaded setup cannot tell what came from where. Both losses are
		// written in docs/guide/limits.md.
		return dst, spans, vals, nil
	case 'I', 'U', 'D':
		c := Change{Kind: ChangeInsert, LSN: at}
		switch typ {
		case 'U':
			c.Kind = ChangeUpdate
		case 'D':
			c.Kind = ChangeDelete
		}
		rel := d.relationOf(&r)
		c.Relation = rel
		var sp tupleSpans
		part := r.u8("a row change")
		if typ != 'I' && (part == 'K' || part == 'O') {
			c.OldIsKey = part == 'K'
			sp.old, vals = d.tuple(&r, rel, vals)
			sp.hasOld = true
			if typ == 'U' {
				part = r.u8("an Update")
			}
		}
		switch {
		case typ == 'D':
			if !sp.hasOld && r.err == nil {
				r.err = fmt.Errorf("%w: pgoutput Delete with part %q, want K or O", ErrProtocol, part)
			}
		case part != 'N':
			if r.err == nil {
				r.err = fmt.Errorf("%w: pgoutput %s with part %q where the new tuple belongs", ErrProtocol, c.Kind, part)
			}
		default:
			sp.new, vals = d.tuple(&r, rel, vals)
			sp.hasNew = true
		}
		if r.err == nil {
			dst, spans = append(dst, c), append(spans, sp)
		}
	case 'T':
		n := r.u32("Truncate")
		r.u8("Truncate")
		if r.err == nil && int(n) > len(r.b)/4 {
			r.err = fmt.Errorf("%w: pgoutput Truncate names %d relations in %d bytes", ErrProtocol, n, len(r.b))
		}
		for i := uint32(0); i < n && r.err == nil; i++ {
			rel := d.relationOf(&r)
			if r.err == nil {
				dst, spans = append(dst, Change{Kind: ChangeTruncate, LSN: at, Relation: rel}), append(spans, tupleSpans{})
			}
		}
	default:
		// Logical decoding messages and the streamed-transaction messages
		// of protocol 2 and later are sent only when asked for, and
		// Replicate asks for neither.
		return dst, spans, vals, fmt.Errorf("%w: pgoutput message type %q was not asked for", ErrProtocol, typ)
	}
	if r.err == nil && len(r.b) != 0 {
		r.err = fmt.Errorf("%w: pgoutput message %q has %d bytes past its end", ErrProtocol, typ, len(r.b))
	}
	return dst, spans, vals, r.err
}

// tupleSpans locates a change's tuples in the batch's value slab.
type tupleSpans struct {
	old, new       [2]int
	hasOld, hasNew bool
}

func (d *replDecoder) relation(r *replReader) {
	rel := &Relation{OID: r.u32("Relation")}
	rel.Namespace = r.cstring("Relation")
	rel.Name = r.cstring("Relation")
	rel.ReplicaIdentity = r.u8("Relation")
	n := int(r.u16("Relation"))
	// Each column takes at least ten bytes, so a count the body cannot hold
	// is refused before anything is allocated for it.
	if r.err == nil && n > len(r.b)/10 {
		r.err = fmt.Errorf("%w: pgoutput Relation declares %d columns in %d bytes", ErrProtocol, n, len(r.b))
		return
	}
	rel.Columns = make([]RelationColumn, n)
	for i := range rel.Columns {
		col := &rel.Columns[i]
		col.Key = r.u8("a Relation column")&1 != 0
		col.Name = r.cstring("a Relation column")
		col.TypeOID = r.u32("a Relation column")
		col.TypeMod = int32(r.u32("a Relation column")) //nolint:gosec // G115: atttypmod is a signed int32
	}
	if r.err != nil {
		return
	}
	if d.relations == nil {
		d.relations = make(map[uint32]*Relation)
	}
	old, known := d.relations[rel.OID]
	if !known && len(d.relations) >= maxReplicationRelations {
		r.err = fmt.Errorf("%w: the server described more than %d relations", ErrProtocol, maxReplicationRelations)
		return
	}
	// A redescription replaces its predecessor in the table, so it is
	// charged the difference, not the whole.
	total := d.relationBytes + rel.cost()
	if known {
		total -= old.cost()
	}
	if total > maxReplicationRelationBytes {
		r.err = fmt.Errorf("%w: a Relation of %d bytes takes the relation table past its %d-byte budget", ErrProtocol, rel.cost(), maxReplicationRelationBytes)
		return
	}
	// A new value rather than an update in place: changes already delivered
	// keep the description they were decoded under.
	d.relations[rel.OID] = rel
	d.relationBytes = total
}

func (d *replDecoder) relationOf(r *replReader) *Relation {
	oid := r.u32("a relation OID")
	if r.err != nil {
		return nil
	}
	rel, ok := d.relations[oid]
	if !ok {
		r.err = fmt.Errorf("%w: pgoutput change on relation %d before its Relation message", ErrProtocol, oid)
	}
	return rel
}

// tuple reads a TupleData part into vals and returns where it lies there.
func (d *replDecoder) tuple(r *replReader, rel *Relation, vals []Value) ([2]int, []Value) {
	n := int(r.u16("TupleData"))
	if r.err != nil {
		return [2]int{}, vals
	}
	if rel != nil && n != len(rel.Columns) {
		r.err = fmt.Errorf("%w: pgoutput tuple of %d values for %s.%s, which has %d columns", ErrProtocol, n, rel.Namespace, rel.Name, len(rel.Columns))
		return [2]int{}, vals
	}
	from := len(vals)
	for range n {
		var v Value
		switch kind := r.u8("TupleData"); kind {
		case 'n':
			v.Status = ValueNull
		case 'u':
			v.Status = ValueUnchanged
		case 'b', 't':
			v.Status = ValueBinary
			if kind == 't' {
				v.Status = ValueText
			}
			size := int(int32(r.u32("TupleData"))) //nolint:gosec // G115: the protocol field is int32
			v.Data = r.bytes(size, "a TupleData value")
		default:
			if r.err == nil {
				r.err = fmt.Errorf("%w: pgoutput tuple value of kind %q", ErrProtocol, kind)
			}
		}
		if r.err != nil {
			return [2]int{}, vals
		}
		vals = append(vals, v)
	}
	return [2]int{from, len(vals)}, vals
}
