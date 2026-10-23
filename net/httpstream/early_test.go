package httpstream

import (
	"bufio"
	"context"
	"fmt"
	"testing"

	"github.com/mlagarrigue/sluice"
)

// A request an intermediary received in early data carries Early-Data
// (RFC 8470 §5.1). Unless Config.EarlyData allows it, it is answered 425 in
// its place and never reaches the handler, which answers the rest of the
// batch in order; allowed, it is handled like the others. Any value of the
// field counts, as §5.1 has an invalid one read as "1".
func TestApplyAnswersEarlyDataWith425UnlessAllowed(t *testing.T) {
	batch := func() sluice.Batch[Request] {
		return sluice.Batch[Request]{Items: []Request{
			{Method: []byte("GET"), Target: []byte("/a")},
			{Method: []byte("POST"), Target: []byte("/b"), Headers: []Header{{Name: []byte("Early-Data"), Value: []byte("1")}}},
			{Method: []byte("GET"), Target: []byte("/c"), Headers: []Header{{Name: []byte("early-data"), Value: []byte("yes")}}},
		}}
	}
	var seen []string
	handle := func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, b.Len())
		for i, r := range b.Items {
			seen = append(seen, string(r.Target))
			out[i] = Response{Status: 200, Body: r.Target}
		}
		return sluice.Batch[Response]{Items: out}
	}
	getOnly := func(r *Request) bool { return string(r.Method) == "GET" }

	for _, tc := range []struct {
		name   string
		early  func(*Request) bool
		status []int
		seen   []string
	}{
		{"nothing allowed", nil, []int{200, 425, 425}, []string{"/a"}},
		{"GET allowed", getOnly, []int{200, 425, 200}, []string{"/a", "/c"}},
		{"everything allowed", func(*Request) bool { return true }, []int{200, 200, 200}, []string{"/a", "/b", "/c"}},
	} {
		seen = nil
		res, err := apply(context.Background(), handle, batch(), tc.early)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		for i, want := range tc.status {
			if res[i].Status != want {
				t.Errorf("%s: response %d is %d, want %d", tc.name, i, res[i].Status, want)
			}
		}
		if fmt.Sprint(seen) != fmt.Sprint(tc.seen) {
			t.Errorf("%s: the handler saw %v, want %v", tc.name, seen, tc.seen)
		}
		if tc.status[1] == 200 && string(res[1].Body) != "/b" {
			t.Errorf("%s: an answer left its position: %q", tc.name, res[1].Body)
		}
	}
}

// End to end on HTTP/1.1, pipelined: the 425 leaves in its place between the
// two answers the handler gave.
func TestServeAnswersEarlyDataWith425(t *testing.T) {
	addr, _ := serveTest(t, Config{}, echoTarget)
	c := dial(t, addr)

	fmt.Fprint(c, "GET /one HTTP/1.1\r\nHost: h\r\n\r\n"+
		"POST /two HTTP/1.1\r\nHost: h\r\nEarly-Data: 1\r\nContent-Length: 0\r\n\r\n"+
		"GET /three HTTP/1.1\r\nHost: h\r\n\r\n")
	r := bufio.NewReader(c)
	for _, want := range []struct{ status, body string }{
		{"HTTP/1.1 200 OK", "/one"},
		{"HTTP/1.1 425 Too Early", "425 Too Early\n"},
		{"HTTP/1.1 200 OK", "/three"},
	} {
		status, body := readResponse(t, r)
		if status != want.status || body != want.body {
			t.Errorf("got %q %q, want %q %q", status, body, want.status, want.body)
		}
	}
}
