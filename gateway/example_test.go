package gateway_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/gateway"
)

// A service that answers one HTTP request per call, batching the calls its
// handlers make so the store behind it sees one lookup for many requests.
//
// There is deliberately no HTTP adapter in this package. The glue below is
// eight lines, it is completely explicit about status codes and encoding, and
// an adapter taking decode and encode functions would save three of those
// lines while hiding the two decisions that matter. A thin abstraction over
// something already thin is surface for nothing.
func Example() {
	// The store stands in for a database: one call answers many keys, which is
	// the asymmetry the gateway exists to exploit.
	lookups := 0
	loadAll := func(ids []int) []string {
		lookups++
		out := make([]string, len(ids))
		for i, id := range ids {
			out[i] = "order-" + strconv.Itoa(id)
		}
		return out
	}

	gw := gateway.New(gateway.Config{Size: 8, Within: 5 * time.Millisecond},
		func(calls sluice.Stream[*gateway.Call[int, string]]) {
			calls(func(b sluice.Batch[*gateway.Call[int, string]]) bool {
				ids := make([]int, b.Len())
				for i, c := range b.Items {
					ids[i] = c.In
				}
				for i, order := range loadAll(ids) {
					b.Items[i].Reply(order)
				}
				return true
			})
		})
	defer func() { _ = gw.Close() }()

	handler := func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.Atoi(r.URL.Query().Get("id"))
		if err != nil {
			http.Error(w, "bad id", http.StatusBadRequest)
			return
		}
		order, err := gw.Do(r.Context(), id)
		if err != nil {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprintln(w, order)
	}

	srv := httptest.NewServer(http.HandlerFunc(handler))
	defer srv.Close()

	// Two requests, sequential here so the output is deterministic; under real
	// concurrency they would share a batch and a single lookup.
	for _, id := range []int{7, 9} {
		resp, err := http.Get(srv.URL + "?id=" + strconv.Itoa(id))
		if err != nil {
			fmt.Println("request failed:", err)
			return
		}
		var body string
		_, _ = fmt.Fscanln(resp.Body, &body)
		_ = resp.Body.Close()
		fmt.Println(body)
	}

	// Output:
	// order-7
	// order-9
}

// The value proposition, made visible: sixteen concurrent callers reach the
// store as one lookup instead of sixteen.
func Example_batching() {
	lookups := make(chan int, 1)
	lookups <- 0

	gw := gateway.New(gateway.Config{Size: 16, Within: 50 * time.Millisecond},
		func(calls sluice.Stream[*gateway.Call[int, int]]) {
			calls(func(b sluice.Batch[*gateway.Call[int, int]]) bool {
				n := <-lookups
				lookups <- n + 1
				for _, c := range b.Items {
					c.Reply(c.In * 2)
				}
				return true
			})
		})
	defer func() { _ = gw.Close() }()

	done := make(chan struct{})
	for i := range 16 {
		go func() {
			defer func() { done <- struct{}{} }()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := gw.Do(ctx, i); err != nil {
				fmt.Println("call failed:", err)
			}
		}()
	}
	for range 16 {
		<-done
	}

	fmt.Println("lookups for 16 concurrent calls:", <-lookups)
	// Output:
	// lookups for 16 concurrent calls: 1
}

// A transport that read several requests in one batch submits them in one
// motion: no goroutine per element, and the batch joins the same window
// every lone caller shares.
func ExampleGateway_DoBatch() {
	gw := gateway.New(gateway.Config{Size: 8, Within: 5 * time.Millisecond},
		func(calls sluice.Stream[*gateway.Call[int, string]]) {
			calls(func(b sluice.Batch[*gateway.Call[int, string]]) bool {
				// One round trip for the whole batch, then a reply per element.
				for _, c := range b.Items {
					c.Reply("order-" + strconv.Itoa(c.In))
				}
				return true
			})
		})
	defer func() { _ = gw.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// What a batching transport holds after one read: several requests that
	// arrived together. Outcome i answers input i, always — a refused element
	// carries its error in place rather than going missing.
	for _, o := range gw.DoBatch(ctx, []int{7, 3, 12}) {
		if o.Err != nil {
			fmt.Println("failed:", o.Err)
			continue
		}
		fmt.Println(o.Out)
	}
	// Output:
	// order-7
	// order-3
	// order-12
}
