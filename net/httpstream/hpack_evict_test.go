package httpstream

import (
	"runtime"
	"strings"
	"testing"
	"weak"
)

// hpackIndexedName encodes a literal with incremental indexing whose name is
// the table entry idx (RFC 7541 §6.2.1, 6-bit prefix) and whose value is v.
func hpackIndexedName(idx uint64, v string) []byte {
	b := appendHPACKInt(nil, idx, 6, 0x40)
	return appendHPACKString(b, v)
}

// hpackNewName encodes a literal with incremental indexing and a new name.
func hpackNewName(name, v string) []byte {
	b := []byte{0x40}
	b = appendHPACKString(b, name)
	return appendHPACKString(b, v)
}

// evictedEntryAlive reports whether the first entry's allocation survives a
// collection once the table has evicted that entry, a second entry having
// taken its name by index (byIndex) or as a literal of its own.
func evictedEntryAlive(t *testing.T, byIndex bool) bool {
	t.Helper()
	d := newHPACKDecoder(4096, 0)
	if _, err := d.Decode(nil, hpackNewName("x-a", strings.Repeat("v", 1000))); err != nil {
		t.Fatal(err)
	}
	wp := weak.Make(&d.entry(0).Value[0])
	second := hpackNewName("x-a", "s")
	if byIndex {
		second = hpackIndexedName(62, "s") // 62: the newest dynamic entry
	}
	if _, err := d.Decode(nil, second); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"x-f1", "x-f2"} {
		if _, err := d.Decode(nil, hpackNewName(name, strings.Repeat("f", 1900))); err != nil {
			t.Fatal(err)
		}
	}
	if d.n != 3 {
		t.Fatalf("table holds %d entries, want 3 (the first evicted)", d.n)
	}
	if got := string(d.entry(2).Name); got != "x-a" {
		t.Fatalf("the entry named by index is %q, want x-a", got)
	}
	runtime.GC()
	runtime.GC()
	alive := wp.Value() != nil
	runtime.KeepAlive(d)
	return alive
}

// An entry that names its field by the index of another holds a name of its
// own: once the table evicts the source and counts its bytes free, the
// source's value is garbage, not kept alive through the later entry's name
// beyond what the table accounts.
func TestHPACKNameByIndexLetsTheEvictedEntryGo(t *testing.T) {
	if evictedEntryAlive(t, false) {
		t.Fatal("witness: with a literal name the evicted entry's value should be collectable")
	}
	if evictedEntryAlive(t, true) {
		t.Error("the 1000-byte value of an evicted entry is still reachable through a later entry's name")
	}
}
