package httpstream

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/textproto"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/quic"
)

const earlyLink = "</app.css>; rel=preload; as=style"

// hinting answers /hint with a 103 carrying a Link, then a 200, and records
// what Interim returned for every request it saw, by target.
type hinting struct {
	mu   sync.Mutex
	errs map[string]error
	kept []Request // requests kept past their batch
}

func (h *hinting) handle(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
	out := make([]Response, b.Len())
	for i := range b.Items {
		r := &b.Items[i]
		err := r.Interim(103, []Header{{Name: []byte("Link"), Value: []byte(earlyLink)}})
		h.mu.Lock()
		if h.errs == nil {
			h.errs = map[string]error{}
		}
		h.errs[string(r.Target)] = err
		h.kept = append(h.kept, *r)
		h.mu.Unlock()
		out[i] = Response{Status: 200, Body: []byte("ok")}
	}
	return sluice.Batch[Response]{Items: out}
}

// get asks for /hint through a net/http client and reports the interim
// responses it saw before the final one.
func getHinted(t *testing.T, client *http.Client, url string) (interim []int, link string) {
	t.Helper()
	trace := &httptrace.ClientTrace{Got1xxResponse: func(code int, h textproto.MIMEHeader) error {
		interim = append(interim, code)
		link = h.Get("Link")
		return nil
	}}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(t.Context(), trace), "GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	if body, _ := io.ReadAll(res.Body); res.StatusCode != 200 || string(body) != "ok" {
		t.Fatalf("final response %d %q", res.StatusCode, body)
	}
	return interim, link
}

// A 103 with its Link reaches a net/http client ahead of the 200, over
// HTTP/1.1 and over HTTP/2.
func TestInterimReachesTheClient(t *testing.T) {
	for _, proto := range []string{"HTTP/1.1", "HTTP/2"} {
		t.Run(proto, func(t *testing.T) {
			h := &hinting{}
			base := interopServer(t, Config{}, h.handle)
			var protocols http.Protocols
			if proto == "HTTP/2" {
				protocols.SetUnencryptedHTTP2(true)
			} else {
				protocols.SetHTTP1(true)
			}
			client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Protocols: &protocols}}
			defer client.CloseIdleConnections()
			interim, link := getHinted(t, client, base+"/hint")
			if len(interim) != 1 || interim[0] != 103 || link != earlyLink {
				t.Errorf("interim responses %v, Link %q: want one 103 with %q", interim, link, earlyLink)
			}
			if err := h.errs["/hint"]; err != nil {
				t.Errorf("Interim = %v", err)
			}
		})
	}
}

// Over HTTP/3, the 103 is a HEADERS frame of its own ahead of the final
// one, on the request's stream.
func TestInterimOverHTTP3(t *testing.T) {
	h := &hinting{}
	conn, _, _ := startH3(t, quic.DefaultConfig(), h.handle)
	var statuses []string
	var link string
	for _, f := range h3Ask(t, conn, h3Request("GET", "/hint")) {
		if f.Type != h3Headers {
			continue
		}
		fields, err := newQPACKDecoder(nil, 0).Decode(nil, f.Payload)
		if err != nil {
			t.Fatal(err)
		}
		for _, fl := range fields {
			switch string(fl.Name) {
			case ":status":
				statuses = append(statuses, string(fl.Value))
			case "link":
				link = string(fl.Value)
			}
		}
	}
	if strings.Join(statuses, " ") != "103 200" || link != earlyLink {
		t.Errorf("heads %v, Link %q: want 103 then 200, the Link on the 103", statuses, link)
	}
}

// What Interim refuses: on HTTP/1.1 the second request of a pipelined
// batch, whose response waits behind the first; any request once its batch
// has returned; a status outside 102..199; a connection field; a request
// built by hand.
func TestInterimRefusals(t *testing.T) {
	h := &hinting{}
	base := interopServer(t, Config{}, h.handle)
	c, err := net.Dial("tcp", strings.TrimPrefix(base, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if _, err := io.WriteString(c, "GET /first HTTP/1.1\r\nHost: x\r\n\r\nGET /second HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	for final := 0; final < 2; {
		res, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, res.Body)
		_ = res.Body.Close()
		if res.StatusCode >= 200 {
			final++
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	t.Logf("requests seen %d, Interim answered %v", len(h.kept), h.errs)
	if len(h.kept) == 2 && h.kept[1].interim != nil && h.kept[0].interim == h.kept[1].interim {
		t.Fatal("the two pipelined requests share a sink: the second could send")
	}
	if err := h.errs["/second"]; len(h.kept) == 2 && h.errs["/first"] == nil && !errors.Is(err, ErrNoInterim) {
		t.Errorf("the second request of a pipelined batch: Interim = %v, want ErrNoInterim", err)
	}
	if err := h.kept[0].Interim(103, nil); !errors.Is(err, ErrNoInterim) {
		t.Errorf("after its batch returned: Interim = %v, want ErrNoInterim", err)
	}
	if err := (Request{}).Interim(103, nil); !errors.Is(err, ErrNoInterim) {
		t.Errorf("a request built by hand: Interim = %v, want ErrNoInterim", err)
	}
	for _, status := range []int{100, 101, 200} {
		if err := (Request{}).Interim(status, nil); !errors.Is(err, ErrMalformed) {
			t.Errorf("Interim(%d) = %v, want ErrMalformed", status, err)
		}
	}
	if _, err := interimFields([]Header{{Name: []byte("Connection"), Value: []byte("close")}}); !errors.Is(err, ErrMalformed) {
		t.Errorf("a connection field in an interim response: %v, want ErrMalformed", err)
	}
}
