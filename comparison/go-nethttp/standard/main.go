// Command standard is the study's Go contender as the documentation of
// net/http and pgx teaches it (study §2.4, "Standard"): a ServeMux with
// method patterns, encoding/json written to the ResponseWriter, a pgxpool
// with one query per request, no grouping. Nothing here is an optimisation;
// the deviations from the documentation, each with its reason, are listed in
// comparison/README.md.
//
// HTTP/1.1 and HTTP/2 over TLS, and HTTP/1.1 in clear; net/http has no
// HTTP/3, so STUDY_PROTOCOL=h3 is refused and published "not supported".
// No compression: net/http offers none.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

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

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("write: %v", err)
	}
}

type server struct {
	db     *pgxpool.Pool
	static contract.Static
}

func (s *server) unit(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errDoc{Error: "bad key"})
		return
	}
	var o order
	err = s.db.QueryRow(r.Context(), "SELECT id, total, label FROM bench_orders WHERE id = $1", id).
		Scan(&o.ID, &o.Total, &o.Label)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		writeJSON(w, http.StatusNotFound, errDoc{Error: "no such key", Key: &id})
	case err != nil:
		log.Printf("unit %d: %v", id, err)
		writeJSON(w, http.StatusServiceUnavailable, errDoc{Error: "unavailable", Key: &id})
	default:
		writeJSON(w, http.StatusOK, o)
	}
}

func (s *server) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	from, err := strconv.ParseInt(q.Get("from"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errDoc{Error: "bad from"})
		return
	}
	limit, err := strconv.Atoi(q.Get("limit"))
	if err != nil || limit <= 0 || limit > contract.ListMax {
		writeJSON(w, http.StatusBadRequest, errDoc{Error: "bad limit", Key: &from})
		return
	}
	rows, _ := s.db.Query(r.Context(), "SELECT id, total, label FROM bench_orders WHERE id > $1 ORDER BY id LIMIT $2", from, limit)
	items, err := pgx.CollectRows(rows, pgx.RowToStructByName[order])
	if err != nil {
		log.Printf("list %d: %v", from, err)
		writeJSON(w, http.StatusServiceUnavailable, errDoc{Error: "unavailable", Key: &from})
		return
	}
	writeJSON(w, http.StatusOK, listDoc{From: from, Limit: limit, Items: items})
}

func (s *server) write(w http.ResponseWriter, r *http.Request) {
	var req writeReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil || req.Key == 0 {
		writeJSON(w, http.StatusBadRequest, errDoc{Error: "bad body"})
		return
	}
	if _, err := s.db.Exec(r.Context(), "INSERT INTO comparison_writes (key, total) VALUES ($1, $2)", req.Key, req.Total); err != nil {
		log.Printf("write %d: %v", req.Key, err)
		writeJSON(w, http.StatusServiceUnavailable, errDoc{Error: "unavailable", Key: &req.Key})
		return
	}
	writeJSON(w, http.StatusOK, writeDoc{Key: req.Key})
}

func run() error {
	cfg, err := contract.FromEnv(os.Getenv)
	if err != nil {
		return err
	}
	if err := cfg.Offers([]string{"h1", "h2"}, nil); err != nil {
		return err
	}
	ctx := context.Background()
	// An empty connection string: pgx reads PGHOST, PGUSER and the rest,
	// as libpq does.
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

	s := &server{db: db, static: contract.StaticDoc()}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /static", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, s.static)
	})
	mux.HandleFunc("GET /orders/{id}", s.unit)
	mux.HandleFunc("GET /orders", s.list)
	mux.HandleFunc("POST /orders", s.write)
	mux.HandleFunc("GET /study/metrics", contract.ServeMetrics)

	srv := &http.Server{Addr: cfg.Listen, Handler: mux, ReadHeaderTimeout: contract.ReadHeaderTimeout}
	if !cfg.TLS {
		fmt.Fprintf(os.Stderr, "go-nethttp-standard: %s on %s (%s)\n", cfg.Protocol, cfg.Listen, cfg.Summary(nil))
		return srv.ListenAndServe()
	}
	if srv.TLSConfig, err = cfg.TLSConfig(); err != nil {
		return err
	}
	// The run's one protocol, so that ALPN offers it alone.
	srv.Protocols = new(http.Protocols)
	srv.Protocols.SetHTTP1(cfg.Protocol == "h1")
	srv.Protocols.SetHTTP2(cfg.Protocol == "h2")
	fmt.Fprintf(os.Stderr, "go-nethttp-standard: %s on %s (%s)\n", cfg.Protocol, cfg.Listen, cfg.Summary(srv.TLSConfig))
	return srv.ListenAndServeTLS("", "")
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "go-nethttp-standard:", err)
		os.Exit(1)
	}
}
