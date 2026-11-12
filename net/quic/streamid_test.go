package quic

import (
	"reflect"
	"testing"
)

// Every public place a stream identifier goes in or comes out has the
// StreamID type, and the byte counts beside it stay uint64: swapping the two
// in ReleaseStreamBytes(id, n) does not compile. Go has no negative
// compilation test, so the types are asserted here.
func TestStreamIdentifiersAreTyped(t *testing.T) {
	sid, u64 := reflect.TypeFor[StreamID](), reflect.TypeFor[uint64]()
	conn := reflect.TypeFor[*Conn]()
	for _, tc := range []struct {
		method string
		// in lists the parameters after the receiver, as they must be.
		in []reflect.Type
	}{
		{"ReleaseStreamBytes", []reflect.Type{sid, u64}},
		{"ReleaseStreamCredit", []reflect.Type{sid, u64}},
		{"Stream", []reflect.Type{sid}},
	} {
		m, ok := conn.MethodByName(tc.method)
		if !ok {
			t.Fatalf("Conn.%s is gone", tc.method)
		}
		for i, want := range tc.in {
			if got := m.Type.In(i + 1); got != want {
				t.Errorf("Conn.%s parameter %d is %v, want %v", tc.method, i, got, want)
			}
		}
	}
	if m, ok := reflect.TypeFor[*Stream]().MethodByName("ID"); !ok || m.Type.Out(0) != sid {
		t.Errorf("Stream.ID does not return a StreamID")
	}
	if f, ok := reflect.TypeFor[Frame]().FieldByName("StreamID"); !ok || f.Type != sid {
		t.Errorf("Frame.StreamID is not a StreamID")
	}
}
