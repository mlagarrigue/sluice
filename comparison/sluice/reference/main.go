// Command reference is Sluice.go in the study's reference configuration R
// and nothing else, written as the guides teach it (docs/guide/web.md,
// postgres.md, httpstream.md): the program the study counts the writing
// cost of (§1.1). The server of the internal study, one directory up,
// serves the same four scenarios but carries every axis of the study
// behind a flag — two drivers, five transports, eight codings — and its
// lines would count all of them.
//
// What R is, and so what this program does:
//
//   - HTTP/1.1 and HTTP/2 through net/http, a ServeMux with method
//     patterns, the web package's helpers, and web.ServeTLS for the clean
//     stop; HTTP/3 through net/httpstream on a QUIC listener, the batch of
//     requests routed by web/stream;
//   - database/postgres, one connection for the gateway and a channel of
//     STUDY_DB_POOL connections for lists and writes: the connector has no
//     pool, and a fixed number of connections needs none;
//   - a gateway of 64 keys or 2 ms in front of the unit reads, one
//     `= ANY($1)` query per batch;
//   - buffered answers, and gzip at level 1 when STUDY_COMPRESSION=gzip and
//     the client accepts it.
//
// The contract — environment, TLS, static document, write table, metrics
// point — is internal/contract, shared with the study's server and counted
// apart. The answers, error documents included, are R's: comparison/chain/
// conform.sh checks it.
package main

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/comparison/sluice/internal/contract"
	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
	"github.com/mlagarrigue/sluice/gateway"
	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/net/quic"
	"github.com/mlagarrigue/sluice/web"
	"github.com/mlagarrigue/sluice/web/stream"
)

type order struct {
	ID    int64  `json:"id"`
	Total int64  `json:"total"`
	Label string `json:"label"`
}

type listDoc struct {
	From  int64   `json:"from"`
	Limit int     `json:"limit"`
	Items []order `json:"items"`
}

type writeReq struct {
	Key   int64 `json:"key"`
	Total int64 `json:"total"`
}

type writeDoc struct {
	Key int64 `json:"key"`
}

type errDoc struct {
	Error string `json:"error"`
	Key   *int64 `json:"key,omitempty"`
}

const (
	unitQuery  = "SELECT id, total, label FROM bench_orders WHERE id = ANY($1)"
	listQuery  = "SELECT id, total, label FROM bench_orders WHERE id > $1 ORDER BY id LIMIT $2"
	writeQuery = "INSERT INTO comparison_writes (key, total) VALUES ($1, $2)"
	batchSize  = 64      // the gateway's Size
	maxBody    = 1 << 16 // the largest request body read
)

var errNotFound = errors.New("no such key")

type server struct {
	gw      *gateway.Gateway[int64, order]
	conns   chan *postgres.Conn // lists and writes; the gateway has a connection of its own
	batches contract.BatchStats
	static  contract.Static
	gzip    bool
}

// connect opens one connection to the server PGHOST and its siblings name.
func connect() (*postgres.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	addr := net.JoinHostPort(os.Getenv("PGHOST"), cmp.Or(os.Getenv("PGPORT"), "5432"))
	return postgres.Connect(ctx, postgres.DialTCP(addr), postgres.Config{
		User: os.Getenv("PGUSER"), Database: os.Getenv("PGDATABASE"), Password: os.Getenv("PGPASSWORD"),
	})
}

func decodeOrder(r postgres.Row) (order, error) {
	id, _ := r.Value(0)
	total, _ := r.Value(1)
	label, _ := r.Value(2)
	var o order
	var err1, err2 error
	o.ID, err1 = pgtype.DecodeInt8(id)
	o.Total, err2 = pgtype.DecodeInt8(total)
	o.Label = pgtype.DecodeText(label)
	return o, errors.Join(err1, err2)
}

// pipeline is the gateway's, built once: the keys of a batch leave as one
// array parameter, and every call gets its row or a miss.
func (s *server) pipeline(conn *postgres.Conn) func(sluice.Stream[*gateway.Call[int64, order]]) {
	return func(calls sluice.Stream[*gateway.Call[int64, order]]) {
		var ids []int64
		found := make(map[int64]order, batchSize)
		calls(func(b sluice.Batch[*gateway.Call[int64, order]]) bool {
			var err error
			ids = ids[:0]
			clear(found)
			for _, c := range b.Items {
				ids = append(ids, c.In)
			}
			s.batches.Record(len(ids))
			keys := pgtype.AppendArray(nil, pgtype.Array[int64]{Elems: ids}, pgtype.Int8Codec())
			src := conn.Query(context.Background(), unitQuery, []postgres.Param{{OID: pgtype.OIDInt8Array, Value: keys}},
				postgres.QueryConfig{BatchRows: batchSize + 1}) // one more than a batch can hold: one round trip
			for rows := range src.Stream() {
				for _, r := range rows.Items {
					o, derr := decodeOrder(r)
					err = cmp.Or(err, derr)
					found[o.ID] = o
				}
			}
			err = cmp.Or(err, src.Err())
			for _, c := range b.Items {
				o, ok := found[c.In]
				switch {
				case err != nil:
					c.Fail(err)
				case !ok:
					c.Fail(errNotFound)
				default:
					c.Reply(o)
				}
			}
			return true
		})
	}
}

func unitAnswer(id int64, o order, err error) (status int, body any) {
	switch {
	case errors.Is(err, errNotFound):
		return http.StatusNotFound, errDoc{Error: "no such key", Key: &id}
	case err != nil:
		log.Printf("unit: %v", err)
		return http.StatusServiceUnavailable, errDoc{Error: "unavailable", Key: &id}
	}
	return http.StatusOK, o
}

func int8Params(vs ...int64) []postgres.Param {
	ps := make([]postgres.Param, len(vs))
	for i, v := range vs {
		ps[i] = postgres.Param{OID: pgtype.OIDInt8, Value: pgtype.AppendInt8(nil, v)}
	}
	return ps
}

func (s *server) list(ctx context.Context, from int64, limit int) (status int, body any) {
	conn := <-s.conns
	defer func() { s.conns <- conn }()
	src := conn.Query(ctx, listQuery, int8Params(from, int64(limit)), postgres.QueryConfig{BatchRows: 1024})
	items := make([]order, 0, limit)
	for rows := range src.Stream() {
		for _, r := range rows.Items {
			o, err := decodeOrder(r)
			if err != nil {
				return http.StatusInternalServerError, errDoc{Error: "decode", Key: &from}
			}
			items = append(items, o)
		}
	}
	if err := src.Err(); err != nil {
		log.Printf("list: %v", err)
		return http.StatusServiceUnavailable, errDoc{Error: "unavailable", Key: &from}
	}
	return http.StatusOK, listDoc{From: from, Limit: limit, Items: items}
}

// listArgs reads from and limit, refused as R refuses them.
func listArgs(fromArg, limitArg string) (from int64, limit int, refused *errDoc) {
	from, err := strconv.ParseInt(fromArg, 10, 64)
	if err != nil {
		return 0, 0, &errDoc{Error: "bad from"}
	}
	limit, err = strconv.Atoi(limitArg)
	if err != nil || limit <= 0 || limit > contract.ListMax {
		key := from // a copy: the return below zeroes from
		return 0, 0, &errDoc{Error: "bad limit", Key: &key}
	}
	return from, limit, nil
}

func (s *server) write(ctx context.Context, req writeReq) (status int, body any) {
	conn := <-s.conns
	defer func() { s.conns <- conn }()
	if _, err := conn.ExecParams(ctx, writeQuery, int8Params(req.Key, req.Total)); err != nil {
		log.Printf("insert: %v", err)
		return http.StatusServiceUnavailable, errDoc{Error: "unavailable", Key: &req.Key}
	}
	return http.StatusOK, writeDoc{Key: req.Key}
}

// --- HTTP/1.1 and HTTP/2: net/http and web ---------------------------------

// handle adapts a handler that returns its status and body: the two
// decisions of a handler, on one line each.
func (s *server) handle(h func(r *http.Request) (int, any)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		status, body := h(r)
		if !s.gzip || !acceptsGzip(r.Header.Get("Accept-Encoding")) {
			_ = web.WriteJSON(w, status, body)
			return
		}
		b, err := json.Marshal(body)
		if err != nil {
			_ = web.WriteJSON(w, http.StatusInternalServerError, errDoc{Error: "encode"})
			return
		}
		hdr := w.Header()
		hdr.Set("Content-Type", "application/json")
		hdr.Set("Content-Encoding", "gzip")
		hdr.Set("Vary", "Accept-Encoding")
		w.WriteHeader(status)
		_, _ = w.Write(gzipped(b))
	}
}

func (s *server) mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /static", s.handle(func(*http.Request) (int, any) { return http.StatusOK, s.static }))
	mux.HandleFunc("GET /study/metrics", s.handle(func(*http.Request) (int, any) {
		return http.StatusOK, contract.Read(&s.batches)
	}))
	mux.HandleFunc("GET /orders/{id}", s.handle(func(r *http.Request) (int, any) {
		id, _, ok := web.PathInt64(r, "id")
		if !ok {
			return http.StatusBadRequest, errDoc{Error: "bad key"}
		}
		o, err := s.gw.Do(r.Context(), id)
		return unitAnswer(id, o, err)
	}))
	mux.HandleFunc("GET /orders", s.handle(func(r *http.Request) (int, any) {
		q := r.URL.Query()
		from, limit, refused := listArgs(q.Get("from"), q.Get("limit"))
		if refused != nil {
			return http.StatusBadRequest, *refused
		}
		return s.list(r.Context(), from, limit)
	}))
	mux.HandleFunc("POST /orders", s.handle(func(r *http.Request) (int, any) {
		var req writeReq
		if _, ok := web.DecodeJSON(r, maxBody, &req); !ok || req.Key == 0 {
			return http.StatusBadRequest, errDoc{Error: "bad body"}
		}
		return s.write(r.Context(), req)
	}))
	return mux
}

// --- HTTP/3: net/httpstream and web/stream ---------------------------------

const (
	routeStatic = iota
	routeMetrics
	routeUnit
	routeList
	routeWrite
)

var router = stream.NewRouter(
	stream.Route{Method: "GET", Pattern: "/static"},
	stream.Route{Method: "GET", Pattern: "/study/metrics"},
	stream.Route{Method: "GET", Pattern: "/orders/{id}"},
	stream.Route{Method: "GET", Pattern: "/orders"},
	stream.Route{Method: "POST", Pattern: "/orders"},
)

// batch answers a batch of requests: the unit reads go to the gateway
// together (DoBatch), the others are answered one by one. The parameters
// are read with strconv rather than stream.PathInt64 and DecodeJSON,
// whose refusals are a web.Report: R's error document is the contract.
func (s *server) batch(ctx context.Context, b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
	ex := stream.Exchanges(nil, b)
	router.Route(ex)
	status, bodies := make([]int, len(ex)), make([]any, len(ex))
	var units []int
	var keys []int64
	for i := range ex {
		e := &ex[i]
		switch e.RouteIndex {
		case routeStatic:
			status[i], bodies[i] = http.StatusOK, s.static
		case routeMetrics:
			status[i], bodies[i] = http.StatusOK, contract.Read(&s.batches)
		case routeUnit:
			id, err := strconv.ParseInt(string(e.Params[0]), 10, 64)
			if err != nil {
				status[i], bodies[i] = http.StatusBadRequest, errDoc{Error: "bad key"}
				continue
			}
			units, keys = append(units, i), append(keys, id)
		case routeList:
			_, query, _ := strings.Cut(string(e.Request.Target), "?")
			q, _ := url.ParseQuery(query)
			from, limit, refused := listArgs(q.Get("from"), q.Get("limit"))
			if refused != nil {
				status[i], bodies[i] = http.StatusBadRequest, *refused
				continue
			}
			status[i], bodies[i] = s.list(ctx, from, limit)
		case routeWrite:
			var req writeReq
			body, err := e.Request.ReadAll(nil, maxBody) // a body sent after the headers is streamed
			if err != nil || json.Unmarshal(body, &req) != nil || req.Key == 0 {
				status[i], bodies[i] = http.StatusBadRequest, errDoc{Error: "bad body"}
				continue
			}
			status[i], bodies[i] = s.write(ctx, req)
		}
	}
	if len(keys) > 0 {
		for j, oc := range s.gw.DoBatch(ctx, keys) {
			status[units[j]], bodies[units[j]] = unitAnswer(keys[j], oc.Out, oc.Err)
		}
	}
	out := stream.Render(ex, func(i int) (int, any) { return status[i], bodies[i] })
	for i := range out.Items {
		if r := &out.Items[i]; s.gzip && r.Body != nil && acceptsGzip(string(b.Items[i].Get("accept-encoding"))) {
			r.Body = gzipped(r.Body)
			r.Headers = append(r.Headers[:len(r.Headers):len(r.Headers)],
				httpstream.Header{Name: []byte("content-encoding"), Value: []byte("gzip")},
				httpstream.Header{Name: []byte("vary"), Value: []byte("Accept-Encoding")})
		}
	}
	return out
}

// serveH3 serves HTTP/3 until ctx is cancelled, then drains as the
// httpstream guide's two-step stop does.
func (s *server) serveH3(ctx context.Context, cfg contract.Config, tlsCfg *tls.Config) error {
	pc, err := net.ListenPacket("udp", cfg.Listen)
	if err != nil {
		return err
	}
	hcfg := httpstream.Config{MaxBodyBytes: maxBody}
	params, err := hcfg.QUICConfig()
	if err != nil {
		return err
	}
	ln, err := quic.NewListener(pc, tlsCfg, params, quic.ListenerConfig{SessionTickets: cfg.Resume})
	if err != nil {
		return err
	}
	srv := &httpstream.Server{Config: hcfg, Handler: s.batch}
	served := make(chan error, 1)
	go func() { served <- srv.ServeQUIC(ln) }()
	select {
	case err := <-served:
		return err
	case <-ctx.Done():
	}
	grace, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(grace); err != nil {
		_ = srv.Close()
	}
	<-served
	return s.gw.Close()
}

// --- gzip ------------------------------------------------------------------

var gzipWriters = sync.Pool{New: func() any {
	zw, _ := gzip.NewWriterLevel(nil, gzip.BestSpeed) // a constant level: no error
	return zw
}}

func gzipped(b []byte) []byte {
	var buf bytes.Buffer
	zw := gzipWriters.Get().(*gzip.Writer)
	defer gzipWriters.Put(zw)
	zw.Reset(&buf)
	_, _ = zw.Write(b) // a bytes.Buffer does not fail
	_ = zw.Close()
	return buf.Bytes()
}

// acceptsGzip reads Accept-Encoding as RFC 9110 §12.5.3 does: gzip, or *,
// with a weight above zero; a coding named outweighs *.
func acceptsGzip(header string) bool {
	named, star := -1.0, -1.0
	for member := range strings.SplitSeq(header, ",") {
		token, params, _ := strings.Cut(member, ";")
		q := 1.0
		for p := range strings.SplitSeq(params, ";") {
			if name, v, ok := strings.Cut(p, "="); ok && strings.EqualFold(strings.TrimSpace(name), "q") {
				f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
				if err != nil {
					f = -1 // a weight that does not parse: the member says nothing
				}
				q = f
			}
		}
		switch token = strings.TrimSpace(token); {
		case strings.EqualFold(token, "gzip"):
			named = q
		case token == "*":
			star = q
		}
	}
	if named >= 0 {
		return named > 0
	}
	return star > 0
}

// --- startup ---------------------------------------------------------------

func run(ctx context.Context) error {
	cfg, err := contract.FromEnv(os.Getenv)
	if err != nil {
		return err
	}
	if err := cfg.Offers([]string{"h1", "h2", "h3"}, []string{"gzip"}); err != nil {
		return err
	}
	s := &server{conns: make(chan *postgres.Conn, cfg.Pool), static: contract.StaticDoc(), gzip: cfg.Compression == "gzip"}
	defer func() {
		for len(s.conns) > 0 {
			_ = (<-s.conns).Close()
		}
	}()
	for range cfg.Pool {
		conn, err := connect()
		if err != nil {
			return err
		}
		s.conns <- conn
	}
	gwConn, err := connect()
	if err != nil {
		return err
	}
	defer func() { _ = gwConn.Close() }()
	for _, ddl := range contract.WriteTable {
		if err := gwConn.Exec(ctx, ddl); err != nil {
			return err
		}
	}
	s.gw = gateway.New(gateway.Config{Size: batchSize, Within: 2 * time.Millisecond}, s.pipeline(gwConn))

	srv := &http.Server{Addr: cfg.Listen, Handler: s.mux(), ReadHeaderTimeout: web.DefaultReadHeaderTimeout}
	if !cfg.TLS {
		fmt.Fprintf(os.Stderr, "sluice-reference: h1 on %s (%s)\n", cfg.Listen, cfg.Summary(nil))
		return web.Serve(ctx, srv, 10*time.Second, s.gw)
	}
	tlsCfg, err := cfg.TLSConfig()
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "sluice-reference: %s on %s (%s)\n", cfg.Protocol, cfg.Listen, cfg.Summary(tlsCfg))
	if cfg.Protocol == "h3" {
		return s.serveH3(ctx, cfg, tlsCfg)
	}
	// The run's one protocol, so that ALPN offers it alone.
	srv.TLSConfig = tlsCfg
	srv.Protocols = new(http.Protocols)
	srv.Protocols.SetHTTP1(cfg.Protocol == "h1")
	srv.Protocols.SetHTTP2(cfg.Protocol == "h2")
	return web.ServeTLS(ctx, srv, 10*time.Second, "", "", s.gw)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	err := run(ctx)
	stop()
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "sluice-reference:", err)
		os.Exit(1)
	}
}
