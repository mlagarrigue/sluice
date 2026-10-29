package httpstream

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// An HTTP/2 connection's phase is busy before the handler of the request it
// carries starts: marked after, a forced stop could read the connection as
// idle while that handler runs, and report no cut. The handler reads the
// phase from inside, which is the behaviour. The race the order closes is
// not one a test can provoke — the reader sets the phase within nanoseconds
// of starting the handler's goroutine, with no yield between — so the order
// itself is also read from serveH2Conn's source: between the consume that
// yields the requests and the calls.Go that starts their handler, the phase
// is set. A line moved after the calls.Go fails here.
func TestH2PhaseIsSetBeforeTheHandlerRuns(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	cli, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	srv, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	_ = cli.SetDeadline(time.Now().Add(5 * time.Second))
	st := &connState{drain: t.Context()}
	st.set(connIdle)
	seen := make(chan int32, 1)
	handler := func(ctx context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		select {
		case seen <- st.phase.Load():
		default:
		}
		return echoTarget(ctx, b)
	}
	cfg, err := Config{Logger: discardLogger()}.resolve()
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = serveH2Conn(t.Context(), srv, st, cfg, handler) }()
	c := h2clientOn(t, cli)
	c.request(1, "GET", "/phase")
	c.flush()
	select {
	case p := <-seen:
		if p != connBusy {
			t.Fatalf("the handler ran under phase %d, want connBusy (%d)", p, connBusy)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler never ran")
	}

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "h2conn.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var consume, set, start token.Pos
	ast.Inspect(f, func(n ast.Node) bool {
		fd, ok := n.(*ast.FuncDecl)
		if !ok || fd.Name.Name != "serveH2Conn" {
			return true
		}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			recv, _ := sel.X.(*ast.Ident)
			switch {
			case recv != nil && recv.Name == "s" && sel.Sel.Name == "consume":
				consume = call.Pos()
			case recv != nil && recv.Name == "st" && sel.Sel.Name == "set" && consume.IsValid() && !start.IsValid():
				set = call.Pos()
			case recv != nil && recv.Name == "calls" && sel.Sel.Name == "Go" && consume.IsValid() && !start.IsValid():
				start = call.Pos()
			}
			return true
		})
		return false
	})
	if !consume.IsValid() || !start.IsValid() {
		t.Fatalf("serveH2Conn no longer has s.consume then calls.Go (%v, %v): the reading of its order needs updating", consume, start)
	}
	if !set.IsValid() {
		t.Fatalf("%s: calls.Go starts the handler before st.set marks the phase", fset.Position(start))
	}
}
