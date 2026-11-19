package httpstream

import (
	"bytes"
	"testing"
)

// Every slice an HTTP/1.1 request hands to the handler — method, target,
// field names and values — is capped at its length. They view the read
// buffer, and the next request of a pipelined batch lies behind them: an
// append to request 1's header value rewrote request 2's target.
func TestH1RequestSlicesAreCappedAtTheirLength(t *testing.T) {
	cfg, err := Config{}.resolve()
	if err != nil {
		t.Fatal(err)
	}
	buf := []byte("GET /a HTTP/1.1\r\nHost: h\r\nX-A: aaa\r\n\r\nGET /b HTTP/1.1\r\nHost: h\r\nX-B: bbb\r\n\r\n")
	var arena []Header
	r1, n1, arena, err := parseRequest(buf, cfg, arena, nil)
	if err != nil {
		t.Fatal(err)
	}
	r2, _, _, err := parseRequest(buf[n1:], cfg, arena, nil)
	if err != nil {
		t.Fatal(err)
	}
	views := map[string][]byte{"method": r1.Method, "target": r1.Target}
	for _, h := range r1.Headers {
		views["name "+string(h.Name)] = h.Name
		views["value of "+string(h.Name)] = h.Value
	}
	for what, v := range views {
		if cap(v) != len(v) {
			t.Errorf("request 1's %s: len %d, cap %d", what, len(v), cap(v))
		}
	}
	before := string(r2.Target)
	_ = append(r1.Headers[len(r1.Headers)-1].Value, bytes.Repeat([]byte("Z"), 40)...)
	if string(r2.Target) != before {
		t.Errorf("an append to request 1's header value rewrote request 2's target: %q, was %q", r2.Target, before)
	}
}

// A literal HPACK value entered in the dynamic table is capped at its
// length, as its name is: the entry is shared with every later block that
// names it, and an append wrote into the room bytes.Clone left after it.
func TestHPACKDynamicValueIsCappedAtItsLength(t *testing.T) {
	d := newHPACKDecoder(4096, 0)
	// A literal with incremental indexing, new name "x-a", value "abc".
	fs, err := d.Decode(nil, []byte{0x40, 3, 'x', '-', 'a', 3, 'a', 'b', 'c'})
	if err != nil {
		t.Fatal(err)
	}
	if cap(fs[0].Name) != len(fs[0].Name) || cap(fs[0].Value) != len(fs[0].Value) {
		t.Errorf("a dynamic-table field: name len %d cap %d, value len %d cap %d",
			len(fs[0].Name), cap(fs[0].Name), len(fs[0].Value), cap(fs[0].Value))
	}
}
