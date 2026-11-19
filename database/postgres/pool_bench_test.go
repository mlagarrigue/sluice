package postgres

import (
	"context"
	"net"
	"testing"
)

// BenchmarkPoolLendIdle is one lend and one give of an idle connection over
// a loopback socket nobody writes to: what the pool adds to every call
// before the call's own round trip, the probe of the idle connection
// included.
func BenchmarkPoolLendIdle(b *testing.B) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	held := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			held <- c
		}
	}()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	server := <-held
	defer func() { _ = server.Close() }()
	c := NewConn(conn, Config{})
	p := NewPool(1, func(context.Context) (*Conn, error) { return c, nil })
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		lent, err := p.lend(ctx)
		if err != nil {
			b.Fatal(err)
		}
		p.give(lent)
	}
}
