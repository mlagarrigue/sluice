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
//   - one ServeMux with method patterns and the web package's helpers for
//     the three protocols: HTTP/1.1 and HTTP/2 through net/http and
//     web.ServeTLS, HTTP/3 through web/stream's ServeH3, which serves the
//     same mux over net/httpstream on a QUIC listener;
//   - database/postgres, one connection for the gateway and a pool of
//     STUDY_DB_POOL connections for lists and writes;
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
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
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
	"github.com/mlagarrigue/sluice/web"
	"github.com/mlagarrigue/sluice/web/stream"
)

//go:generate go run github.com/mlagarrigue/sluice/cmd/sluicegen -type order

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
	db      *postgres.Pool // lists and writes; the gateway has a connection of its own
	batches contract.BatchStats
	static  contract.Static
	gzip    bool
}

// connect opens one connection to the server PGHOST and its siblings name.
func connect(ctx context.Context) (*postgres.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	addr := net.JoinHostPort(os.Getenv("PGHOST"), cmp.Or(os.Getenv("PGPORT"), "5432"))
	return postgres.Connect(ctx, postgres.DialTCP(addr), postgres.Config{
		User: os.Getenv("PGUSER"), Database: os.Getenv("PGDATABASE"), Password: os.Getenv("PGPASSWORD"),
	})
}

// pipeline is the gateway's, built once: the keys of a batch leave as one
// array parameter, and every call gets its row or a miss.
func (s *server) pipeline(conn *postgres.Conn) func(sluice.Stream[*gateway.Call[int64, order]]) {
	var rows []order
	return gateway.Lookup(errNotFound, func(ids []int64, found map[int64]order) error {
		s.batches.Record(len(ids))
		src := conn.Query(context.Background(), unitQuery, []postgres.Param{postgres.ArrayArg(pgtype.Int8Codec(), ids)},
			postgres.QueryConfig{BatchRows: batchSize + 1}) // one more than a batch can hold: one round trip
		var err error
		rows, err = collectOrder(rows[:0], src)
		for _, o := range rows {
			found[o.ID] = o
		}
		return err
	})
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

func (s *server) list(ctx context.Context, from int64, limit int) (status int, body any) {
	params := []postgres.Param{postgres.Arg(pgtype.Int8Codec(), from), postgres.Arg(pgtype.Int8Codec(), int64(limit))}
	src := s.db.Query(ctx, listQuery, params, postgres.QueryConfig{BatchRows: 1024})
	items, err := collectOrder(make([]order, 0, limit), src)
	switch {
	case errors.Is(err, pgtype.ErrCodec):
		return http.StatusInternalServerError, errDoc{Error: "decode", Key: &from}
	case err != nil:
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
	if _, err := s.db.ExecParams(ctx, writeQuery, []postgres.Param{postgres.Arg(pgtype.Int8Codec(), req.Key), postgres.Arg(pgtype.Int8Codec(), req.Total)}); err != nil {
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
	s := &server{db: postgres.NewPool(cfg.Pool, connect), static: contract.StaticDoc(), gzip: cfg.Compression == "gzip"}
	defer func() { _ = s.db.Close() }()
	gwConn, err := connect(ctx)
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
	// The run's one protocol, so that ALPN offers it alone; HTTP/3 serves
	// the same mux.
	srv.TLSConfig = tlsCfg
	if cfg.Protocol == "h3" {
		return stream.ServeH3(ctx, srv, 10*time.Second, "", "", s.gw)
	}
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
