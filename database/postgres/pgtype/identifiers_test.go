package pgtype

import (
	"bytes"
	"encoding/hex"
	"errors"
	"slices"
	"testing"
)

// # identifiers and positions

func TestIdentifiersAgainstHandWrittenVectors(t *testing.T) {
	tests := []struct {
		name string
		got  []byte
		want string
	}{
		{"oid 1259", AppendOID(nil, 1259), "000004eb"},
		{"oid8", AppendOID8(nil, 1<<40), "0000010000000000"},
		{"xid", AppendXID(nil, 0xfffffffe), "fffffffe"},
		{"xid8", AppendXID8(nil, 1<<32|5), "0000000100000005"},
		{"cid", AppendCID(nil, 3), "00000003"},
		{"pg_lsn 16/B374D848", AppendPGLSN(nil, 0x16_B374D848), "00000016b374d848"},
		{"tid (7,3)", AppendTID(nil, TID{Block: 7, Offset: 3}), "000000070003"},
		{`"char" r`, AppendChar(nil, 'r'), "72"},
		{
			"snapshot 10:20:12,15", AppendSnapshot(nil, Snapshot{Xmin: 10, Xmax: 20, Xip: []uint64{12, 15}}),
			"00000002" + "000000000000000a" + "0000000000000014" + "000000000000000c" + "000000000000000f",
		},
		{
			"int2vector 1 3", AppendInt2Vector(nil, []int16{1, 3}),
			"00000001" + "00000000" + "00000015" + "00000002" + "00000000" + "00000002" + "0001" + "00000002" + "0003",
		},
		{"empty oidvector", AppendOIDVector(nil, nil), "00000001" + "00000000" + "0000001a" + "00000000" + "00000000"},
	}
	for _, tt := range tests {
		if got := hex.EncodeToString(tt.got); got != tt.want {
			t.Errorf("%s = %s, want %s", tt.name, got, tt.want)
		}
	}
}

func TestIdentifiersRoundTrip(t *testing.T) {
	if v, err := DecodeOID(AppendOID(nil, 4294967295)); err != nil || v != 4294967295 {
		t.Errorf("oid = %d, %v", v, err)
	}
	if v, err := DecodeOID8(AppendOID8(nil, 1<<63)); err != nil || v != 1<<63 {
		t.Errorf("oid8 = %d, %v", v, err)
	}
	if v, err := DecodeXID(AppendXID(nil, 7)); err != nil || v != 7 {
		t.Errorf("xid = %d, %v", v, err)
	}
	if v, err := DecodeXID8(AppendXID8(nil, 7)); err != nil || v != 7 {
		t.Errorf("xid8 = %d, %v", v, err)
	}
	if v, err := DecodeCID(AppendCID(nil, 7)); err != nil || v != 7 {
		t.Errorf("cid = %d, %v", v, err)
	}
	if v, err := DecodePGLSN(AppendPGLSN(nil, 7)); err != nil || v != 7 {
		t.Errorf("pg_lsn = %d, %v", v, err)
	}
	if v, err := DecodeTID(AppendTID(nil, TID{Block: 1 << 31, Offset: 65535})); err != nil || v != (TID{Block: 1 << 31, Offset: 65535}) {
		t.Errorf("tid = %v, %v", v, err)
	}
	if v, err := DecodeChar(AppendChar(nil, 0)); err != nil || v != 0 {
		t.Errorf(`"char" = %d, %v`, v, err)
	}
	s := Snapshot{Xmin: 100, Xmax: 105, Xip: []uint64{101, 103}}
	if v, err := DecodeSnapshot(AppendSnapshot(nil, s)); err != nil || v.Xmin != s.Xmin || v.Xmax != s.Xmax || !slices.Equal(v.Xip, s.Xip) {
		t.Errorf("snapshot = %v, %v", v, err)
	}
	if v, err := DecodeInt2Vector(nil, AppendInt2Vector(nil, []int16{-1, 2})); err != nil || !slices.Equal(v, []int16{-1, 2}) {
		t.Errorf("int2vector = %v, %v", v, err)
	}
	if v, err := DecodeOIDVector(nil, AppendOIDVector(nil, []uint32{23, 25})); err != nil || !slices.Equal(v, []uint32{23, 25}) {
		t.Errorf("oidvector = %v, %v", v, err)
	}
}

// What the server never sends is refused with ErrCodec: a width off by one,
// a snapshot or vector whose count its bytes do not hold — checked before
// any allocation — and a vector header other than the one the server
// builds (one dimension, no NULLs, its element type, numbered from 0).
func TestIdentifiersRefuseMalformedValues(t *testing.T) {
	ordinary := AppendInt2Array(nil, []int16{1})
	tests := []struct {
		name string
		err  error
	}{
		{"oid short", func() error { _, err := DecodeOID(make([]byte, 3)); return err }()},
		{"pg_lsn short", func() error { _, err := DecodePGLSN(make([]byte, 7)); return err }()},
		{"tid long", func() error { _, err := DecodeTID(make([]byte, 7)); return err }()},
		{`"char" empty`, func() error { _, err := DecodeChar(nil); return err }()},
		{"snapshot count past its bytes", func() error {
			_, err := DecodeSnapshot(append([]byte{0x7f, 0xff, 0xff, 0xff}, make([]byte, 16)...))
			return err
		}()},
		{"int2vector count past its bytes", func() error {
			b := AppendInt2Vector(nil, nil)
			b[15] = 0xff
			_, err := DecodeInt2Vector(nil, b)
			return err
		}()},
		{"an int2[] numbered from 1 is no int2vector", func() error { _, err := DecodeInt2Vector(nil, ordinary); return err }()},
		{"oidvector of int2", func() error { _, err := DecodeOIDVector(nil, AppendInt2Vector(nil, []int16{1})); return err }()},
	}
	for _, tt := range tests {
		if !errors.Is(tt.err, ErrCodec) {
			t.Errorf("%s: %v, want ErrCodec", tt.name, tt.err)
		}
	}
}

func FuzzRoundTripTID(f *testing.F) {
	fuzzRoundTrip(f, DecodeTID, AppendTID, make([]byte, 6))
}

func FuzzRoundTripSnapshot(f *testing.F) {
	f.Add(AppendSnapshot(nil, Snapshot{Xmin: 1, Xmax: 3, Xip: []uint64{2}}))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		s, err := DecodeSnapshot(b)
		if err != nil {
			if !errors.Is(err, ErrCodec) {
				t.Fatalf("unclassified error %v for %x", err, b)
			}
			return
		}
		if got := AppendSnapshot(nil, s); !bytes.Equal(got, b) {
			t.Fatalf("decode(%x) -> %v -> encode = %x: the value did not survive", b, s, got)
		}
	})
}

func FuzzRoundTripInt2Vector(f *testing.F) {
	f.Add(AppendInt2Vector(nil, []int16{1, 2}))
	f.Add(AppendInt2Vector(nil, nil))
	f.Fuzz(func(t *testing.T, b []byte) {
		v, err := DecodeInt2Vector(nil, b)
		if err != nil {
			if !errors.Is(err, ErrCodec) {
				t.Fatalf("unclassified error %v for %x", err, b)
			}
			return
		}
		if got := AppendInt2Vector(nil, v); !bytes.Equal(got, b) {
			t.Fatalf("decode(%x) -> %v -> encode = %x: the value did not survive", b, v, got)
		}
	})
}
