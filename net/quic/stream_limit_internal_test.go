package quic

import (
	"errors"
	"testing"
)

// Conn.Stream refuses an identifier past the stream limit of its kind and
// registers nothing for it: Stream(4_000_001) on a server used to register
// the million streams below it, whatever the peer had granted.
func TestStreamRefusesAnIdentifierPastTheLimit(t *testing.T) {
	c, _ := newBenchConn(t) // a server: its streams are 4k+1 and 4k+3
	defer c.Close()
	c.mu.Lock()
	c.peerMaxStreamsBidi, c.peerMaxStreamsUni = 2, 1
	c.localMaxStreamsBidi, c.localMaxStreamsUni = 3, 1
	c.mu.Unlock()

	for _, tc := range []struct {
		name string
		id   uint64
	}{
		{"local bidirectional past the peer's grant", 4*2 + 1},
		{"local bidirectional far past the peer's grant", 4_000_001},
		{"local unidirectional past the peer's grant", 4*1 + 3},
		{"peer bidirectional past this end's limit", 4 * 3},
		{"peer unidirectional past this end's limit", 4*1 + 2},
	} {
		c.mu.Lock()
		before := len(c.streams)
		c.mu.Unlock()
		var err error
		allocs := testing.AllocsPerRun(10, func() { _, err = c.Stream(tc.id) })
		if !errors.Is(err, ErrStreamLimit) {
			t.Errorf("%s: Stream(%d) returned %v, want ErrStreamLimit", tc.name, tc.id, err)
		}
		if allocs != 0 {
			t.Errorf("%s: a refused Stream(%d) allocated %v times, want 0", tc.name, tc.id, allocs)
		}
		c.mu.Lock()
		after := len(c.streams)
		c.mu.Unlock()
		if after != before {
			t.Errorf("%s: a refused Stream(%d) took the table from %d streams to %d", tc.name, tc.id, before, after)
		}
	}

	// The last identifier each limit covers still opens.
	for _, id := range []uint64{4*1 + 1, 3, 4 * 2, 2} {
		s, err := c.Stream(id)
		if err != nil || s == nil || s.id != id {
			t.Errorf("Stream(%d) = %v, %v; want the stream", id, s, err)
		}
	}
}
