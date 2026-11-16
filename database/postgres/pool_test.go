package postgres

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// scriptedConnects is a connect function handing out a new connection per
// call, each answering the next script, and counting them.
type scriptedConnects struct {
	scripts [][][]byte
	conns   []*Conn
	servers []*fakeServer
}

func (s *scriptedConnects) connect(context.Context) (*Conn, error) {
	if len(s.conns) == len(s.scripts) {
		return nil, errors.New("no script left for another connection")
	}
	srv := &fakeServer{responses: s.scripts[len(s.conns)]}
	c := NewConn(srv, Config{})
	s.conns, s.servers = append(s.conns, c), append(s.servers, srv)
	return c, nil
}

func insertOne(t *testing.T, ctx context.Context, x interface {
	ExecParams(context.Context, string, []Param) (CommandTag, error)
},
) error {
	t.Helper()
	_, err := x.ExecParams(ctx, "INSERT INTO t VALUES ($1)", []Param{Arg(pgtype.Int8Codec(), 1)})
	return err
}

// The case the pool exists for: a connection the server ended is closed when
// it comes back and the next call gets a new one, where a connection kept by
// hand — the channel a program writes — fails every call after.
func TestPoolReplacesABrokenConnection(t *testing.T) {
	fatal := beFatal("57P01", "terminating connection due to administrator command")
	s := &scriptedConnects{scripts: [][][]byte{{fatal}, {extendedCycle("INSERT 0 1", "I")}}}
	p := NewPool(1, s.connect)
	defer func() { _ = p.Close() }()
	ctx := t.Context()

	if err := insertOne(t, ctx, p); err == nil || s.conns[0].Err() == nil {
		t.Fatalf("first call = %v, connection error %v; want the server's end and a broken connection", err, s.conns[0].Err())
	}
	if err := insertOne(t, ctx, p); err != nil {
		t.Fatalf("the call after a broken connection = %v, want it served by a new one", err)
	}
	if len(s.conns) != 2 || !s.servers[0].closed {
		t.Errorf("%d connections made, the broken one closed: %v; want 2 and true", len(s.conns), s.servers[0].closed)
	}
	// The control: the broken connection, kept, fails again.
	if err := insertOne(t, ctx, s.conns[0]); err == nil {
		t.Error("the broken connection served a call: the test above proves nothing")
	}
}

// A sound connection is lent again rather than replaced.
func TestPoolReusesASoundConnection(t *testing.T) {
	s := &scriptedConnects{scripts: [][][]byte{{extendedCycle("INSERT 0 1", "I"), extendedCycle("INSERT 0 1", "I")}}}
	p := NewPool(4, s.connect)
	defer func() { _ = p.Close() }()
	for range 2 {
		if err := insertOne(t, t.Context(), p); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.conns) != 1 {
		t.Errorf("%d connections for two calls in turn, want 1", len(s.conns))
	}
}

// With every connection lent, a call waits for one as long as its context
// allows; a connect that fails gives its place back.
func TestPoolWaitsWithinTheContext(t *testing.T) {
	s := &scriptedConnects{scripts: [][][]byte{{}}}
	p := NewPool(1, s.connect)
	defer func() { _ = p.Close() }()
	c, err := p.lend(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := p.lend(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a call with every connection lent = %v, want its deadline", err)
	}
	p.give(c)
	again, err := p.lend(t.Context())
	if err != nil || again != c {
		t.Errorf("after the give: %v, %v; want the same connection back", again, err)
	}
	p.give(again)

	failing := NewPool(1, func(context.Context) (*Conn, error) { return nil, errors.New("refused") })
	for range 2 { // the second would wait for good if the first kept its place
		if _, err := failing.lend(t.Context()); err == nil {
			t.Fatal("a failed connect lent something")
		}
	}
}

// Close waits for what is lent, closes every connection, and refuses after.
func TestPoolClose(t *testing.T) {
	s := &scriptedConnects{scripts: [][][]byte{{}, {}}}
	p := NewPool(2, s.connect)
	a, _ := p.lend(t.Context())
	b, _ := p.lend(t.Context())
	p.give(b)
	closed := make(chan error, 1)
	go func() { closed <- p.Close() }()
	select {
	case <-closed:
		t.Fatal("Close returned with a connection still lent")
	case <-time.After(20 * time.Millisecond):
	}
	p.give(a)
	if err := <-closed; err != nil {
		t.Errorf("Close = %v", err)
	}
	if !s.servers[0].closed || !s.servers[1].closed {
		t.Error("a connection outlived Close")
	}
	if _, err := p.ExecParams(t.Context(), "SELECT 1", nil); !errors.Is(err, ErrPoolClosed) {
		t.Errorf("a call after Close = %v, want ErrPoolClosed", err)
	}
	if err := p.Query(t.Context(), "SELECT 1", nil, QueryConfig{}).Consume(func(sluice.Batch[Row]) bool { return true }); !errors.Is(err, ErrPoolClosed) {
		t.Errorf("a query after Close = %v, want ErrPoolClosed", err)
	}
}

// replayed is a transport that answers whatever is asked with one script.
type replayed struct{ *bytes.Reader }

func (replayed) Write(p []byte) (int, error) { return len(p), nil }
func (replayed) Close() error                { return nil }

// A query holds its connection while its stream is read and gives it back
// at the end; nothing is lent before the stream is read.
func TestPoolQueryLendsForTheStream(t *testing.T) {
	var script []byte
	for _, m := range [][]byte{
		beSimple(pgwire.BackendParseComplete), beSimple(pgwire.BackendBindComplete),
		beRowDescription([]string{"id"}, []uint32{pgtype.OIDInt8}),
		beDataRow(int8Val(7)), beCommandComplete("SELECT 1"), beReadyForQuery(),
	} {
		script = append(script, m...)
	}
	made := 0
	p := NewPool(1, func(context.Context) (*Conn, error) {
		made++
		return NewConn(replayed{bytes.NewReader(script)}, Config{}), nil
	})
	defer func() { _ = p.Close() }()
	src := p.Query(t.Context(), "SELECT id FROM t", nil, QueryConfig{BatchRows: 1, Fetch: FetchBatches})
	if made != 0 {
		t.Error("a connection was lent before the stream was read")
	}
	rows := 0
	err := src.Consume(func(b sluice.Batch[Row]) bool {
		rows += b.Len()
		if len(p.idle) != 0 {
			t.Error("the connection came back while its stream was read")
		}
		return true
	})
	if err != nil || rows != 1 || made != 1 || len(p.idle) != 1 {
		t.Errorf("%d rows, %v, %d made, %d idle; want 1 row, nil, 1 made and back", rows, err, made, len(p.idle))
	}
}

func TestNewPoolRefusals(t *testing.T) {
	for _, tc := range []struct {
		size    int
		connect func(context.Context) (*Conn, error)
	}{{0, func(context.Context) (*Conn, error) { return nil, nil }}, {1, nil}} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("NewPool(%d, connect nil: %v) did not panic", tc.size, tc.connect == nil)
				}
			}()
			NewPool(tc.size, tc.connect)
		}()
	}
}
