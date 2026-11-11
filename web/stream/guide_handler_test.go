package stream_test

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/httpstream"
	webstream "github.com/mlagarrigue/sluice/web/stream"
)

// The handler docs/guide/httpstream.md gives as its model for web/stream,
// served for real over the zero Config: Route, then PathInt64 on every
// exchange with no Refused() in front. A client pipelines a request the table
// knows and one it does not, in one write. The table's 404 must reach the
// client as a 404, and the neighbour must be served 200; a panic in
// PathInt64 on the refused exchange answered 500 instead.
func TestTheGuideHandlerAnswersAnUnknownTarget404(t *testing.T) {
	rt := webstream.NewRouter(webstream.Route{Method: "GET", Pattern: "/orders/{order}"})
	handler := func(_ context.Context, b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
		ex := webstream.Exchanges(nil, b)
		rt.Route(ex)
		results := make([]int64, len(ex))
		for i := range ex {
			order, _ := webstream.PathInt64(&ex[i], 0, "order") // as the guide writes it
			results[i] = order
		}
		return webstream.Render(ex, func(i int) (int, any) { return 200, results[i] })
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &httpstream.Server{Handler: handler}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-done
	})

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(20 * time.Second))
	host := ln.Addr().String()
	if _, err := fmt.Fprintf(c, "GET /orders/7 HTTP/1.1\r\nHost: %s\r\n\r\nGET /nope HTTP/1.1\r\nHost: %s\r\n\r\n", host, host); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	var got []int
	for range 2 {
		resp, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatalf("after %v: %v", got, err)
		}
		resp.Body.Close()
		got = append(got, resp.StatusCode)
		if resp.Close {
			break
		}
	}
	if len(got) != 2 || got[0] != 200 || got[1] != 404 {
		t.Errorf("pipelined GET /orders/7, GET /nope answered %v; want [200 404]", got)
	}
}
