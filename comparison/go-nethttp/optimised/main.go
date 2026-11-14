// Command optimised is the study's Go contender after its optimisation
// budget (study §2.4, "Optimised"): the same routes, documents and queries
// as the standard version, with each change listed, and its source, in
// comparison/README.md. The guard rails of §2.4 hold: no response cache, no
// validation or error log removed, TLS as the contract fixes it.
//
// What changes:
//   - HTTP/3 through quic-go's http3, the HTTP/3 server of the Go ecosystem
//     (net/http has none): a different server, listed, as httpstream is for
//     Sluice.go's optimised version;
//   - every answer encoded into a pooled buffer and written once with its
//     Content-Length, instead of streamed by a json.Encoder, chunked on
//     HTTP/1.1 past net/http's small-response buffer;
//   - gzip at level 1 (STUDY_COMPRESSION=gzip) by compress/gzip, the writer
//     reset and pooled rather than built per answer;
//   - the list scanned by position into a slice sized to the limit, rather
//     than through pgx.RowToStructByName, which matches columns to fields by
//     name on every row.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/quic-go/quic-go/http3"

	"github.com/mlagarrigue/sluice/comparison/go-nethttp/internal/contract"
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

var (
	buffers = sync.Pool{New: func() any { return new(bytes.Buffer) }}
	gzips   = sync.Pool{New: func() any {
		zw, _ := gzip.NewWriterLevel(nil, gzip.BestSpeed)
		return zw
	}}
)

type server struct {
	db     *pgxpool.Pool
	static contract.Static
	gzip   bool // STUDY_COMPRESSION=gzip
}

// writeJSON encodes v into a pooled buffer, compresses it when the run
// compresses and the client accepts gzip, and writes it in one go with its
// length.
func (s *server) writeJSON(w http.ResponseWriter, r *http.Request, status int, v any) {
	buf := buffers.Get().(*bytes.Buffer)
	defer buffers.Put(buf)
	buf.Reset()
	if err := json.NewEncoder(buf).Encode(v); err != nil {
		log.Printf("encode: %v", err)
		http.Error(w, "encode", http.StatusInternalServerError)
		return
	}
	body := buf.Bytes()
	h := w.Header()
	h.Set("Content-Type", "application/json")
	if s.gzip {
		h.Set("Vary", "Accept-Encoding")
		if acceptsGzip(r.Header.Get("Accept-Encoding")) {
			zbuf := buffers.Get().(*bytes.Buffer)
			defer buffers.Put(zbuf)
			zbuf.Reset()
			zw := gzips.Get().(*gzip.Writer)
			zw.Reset(zbuf)
			_, _ = zw.Write(body)
			_ = zw.Close()
			gzips.Put(zw)
			body = zbuf.Bytes()
			h.Set("Content-Encoding", "gzip")
		}
	}
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	if _, err := w.Write(body); err != nil {
		log.Printf("write: %v", err)
	}
}

// acceptsGzip reads Accept-Encoding as RFC 9110 §12.5.3 asks: gzip, or
// "*", with a weight above zero; a member it cannot read is skipped, so a
// malformed header answers identity.
func acceptsGzip(header string) bool {
	named, star := -1.0, -1.0
	for member := range strings.SplitSeq(header, ",") {
		token, params, _ := strings.Cut(member, ";")
		q := 1.0
		if name, value, ok := strings.Cut(strings.TrimSpace(params), "="); ok && strings.EqualFold(strings.TrimSpace(name), "q") {
			f, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
			if err != nil || f < 0 || f > 1 {
				continue
			}
			q = f
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

func (s *server) unit(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		s.writeJSON(w, r, http.StatusBadRequest, errDoc{Error: "bad key"})
		return
	}
	var o order
	err = s.db.QueryRow(r.Context(), "SELECT id, total, label FROM bench_orders WHERE id = $1", id).
		Scan(&o.ID, &o.Total, &o.Label)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		s.writeJSON(w, r, http.StatusNotFound, errDoc{Error: "no such key", Key: &id})
	case err != nil:
		log.Printf("unit %d: %v", id, err)
		s.writeJSON(w, r, http.StatusServiceUnavailable, errDoc{Error: "unavailable", Key: &id})
	default:
		s.writeJSON(w, r, http.StatusOK, o)
	}
}

func (s *server) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	from, err := strconv.ParseInt(q.Get("from"), 10, 64)
	if err != nil {
		s.writeJSON(w, r, http.StatusBadRequest, errDoc{Error: "bad from"})
		return
	}
	limit, err := strconv.Atoi(q.Get("limit"))
	if err != nil || limit <= 0 || limit > contract.ListMax {
		s.writeJSON(w, r, http.StatusBadRequest, errDoc{Error: "bad limit", Key: &from})
		return
	}
	items, err := s.listRows(r.Context(), from, limit)
	if err != nil {
		log.Printf("list %d: %v", from, err)
		s.writeJSON(w, r, http.StatusServiceUnavailable, errDoc{Error: "unavailable", Key: &from})
		return
	}
	s.writeJSON(w, r, http.StatusOK, listDoc{From: from, Limit: limit, Items: items})
}

func (s *server) listRows(ctx context.Context, from int64, limit int) ([]order, error) {
	rows, err := s.db.Query(ctx, "SELECT id, total, label FROM bench_orders WHERE id > $1 ORDER BY id LIMIT $2", from, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]order, 0, limit)
	for rows.Next() {
		var o order
		if err := rows.Scan(&o.ID, &o.Total, &o.Label); err != nil {
			return nil, err
		}
		items = append(items, o)
	}
	return items, rows.Err()
}

func (s *server) write(w http.ResponseWriter, r *http.Request) {
	var req writeReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil || req.Key == 0 {
		s.writeJSON(w, r, http.StatusBadRequest, errDoc{Error: "bad body"})
		return
	}
	if _, err := s.db.Exec(r.Context(), "INSERT INTO comparison_writes (key, total) VALUES ($1, $2)", req.Key, req.Total); err != nil {
		log.Printf("insert: %v", err)
		s.writeJSON(w, r, http.StatusServiceUnavailable, errDoc{Error: "unavailable", Key: &req.Key})
		return
	}
	s.writeJSON(w, r, http.StatusOK, writeDoc{Key: req.Key})
}

func run() error {
	cfg, err := contract.FromEnv(os.Getenv)
	if err != nil {
		return err
	}
	if err := cfg.Offers([]string{"h1", "h2", "h3"}, []string{"gzip"}); err != nil {
		return err
	}
	ctx := context.Background()
	poolCfg, err := pgxpool.ParseConfig("")
	if err != nil {
		return err
	}
	poolCfg.MaxConns = cfg.Pool
	db, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return err
	}
	defer db.Close()
	for _, ddl := range contract.WriteTable {
		if _, err := db.Exec(ctx, ddl); err != nil {
			return err
		}
	}

	s := &server{db: db, static: contract.StaticDoc(), gzip: cfg.Compression == "gzip"}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /static", func(w http.ResponseWriter, r *http.Request) {
		s.writeJSON(w, r, http.StatusOK, s.static)
	})
	mux.HandleFunc("GET /orders/{id}", s.unit)
	mux.HandleFunc("GET /orders", s.list)
	mux.HandleFunc("POST /orders", s.write)
	mux.HandleFunc("GET /study/metrics", contract.Metrics(nil))

	if !cfg.TLS {
		fmt.Fprintf(os.Stderr, "go-nethttp-optimised: %s on %s (%s)\n", cfg.Protocol, cfg.Listen, cfg.Summary(nil))
		return (&http.Server{Addr: cfg.Listen, Handler: mux, ReadHeaderTimeout: contract.ReadHeaderTimeout}).ListenAndServe()
	}
	tlsCfg, err := cfg.TLSConfig()
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "go-nethttp-optimised: %s on %s (%s)\n", cfg.Protocol, cfg.Listen, cfg.Summary(tlsCfg))
	if cfg.Protocol == "h3" {
		return (&http3.Server{Addr: cfg.Listen, Handler: mux, TLSConfig: http3.ConfigureTLSConfig(tlsCfg)}).ListenAndServe()
	}
	srv := &http.Server{Addr: cfg.Listen, Handler: mux, TLSConfig: tlsCfg, Protocols: new(http.Protocols), ReadHeaderTimeout: contract.ReadHeaderTimeout}
	srv.Protocols.SetHTTP1(cfg.Protocol == "h1")
	srv.Protocols.SetHTTP2(cfg.Protocol == "h2")
	return srv.ListenAndServeTLS("", "")
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "go-nethttp-optimised:", err)
		os.Exit(1)
	}
}
