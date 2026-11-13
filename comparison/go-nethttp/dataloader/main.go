// Command dataloader is the standard version with grouping (study §2.1,
// "with"): the unit reads go through a DataLoader of vikstrous/dataloadgen,
// which gathers the keys loaded within its window and fetches them in one
// `= ANY($1)` query. Everything else is the standard version, line for line.
//
// One deviation from dataloadgen's documentation, declared (§2.1): its
// examples build a loader per request, which with one key per HTTP request
// groups nothing; this one is shared by the process, and its memoisation
// cache is off — every key differs, so the cache would never serve and would
// only grow. The loader's settings are its defaults: a 16 ms window, no
// bound on a batch. Every fetch is counted in the metrics point's "batches".
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
	"github.com/vikstrous/dataloadgen"

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
	db      *pgxpool.Pool
	orders  *dataloadgen.Loader[int64, order]
	batches contract.BatchStats
	static  contract.Static
}

// fetchOrders is the loader's fetch: one query for the keys of a batch,
// the rows back as a map; a key without a row is dataloadgen.ErrNotFound.
func (s *server) fetchOrders(ctx context.Context, ids []int64) (map[int64]order, error) {
	s.batches.Record(len(ids))
	rows, _ := s.db.Query(ctx, "SELECT id, total, label FROM bench_orders WHERE id = ANY($1)", ids)
	found, err := pgx.CollectRows(rows, pgx.RowToStructByName[order])
	if err != nil {
		return nil, err
	}
	out := make(map[int64]order, len(found))
	for _, o := range found {
		out[o.ID] = o
	}
	return out, nil
}

func (s *server) unit(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errDoc{Error: "bad key"})
		return
	}
	o, err := s.orders.Load(r.Context(), id)
	switch {
	case errors.Is(err, dataloadgen.ErrNotFound):
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
	s.orders = dataloadgen.NewMappedLoader(s.fetchOrders, dataloadgen.WithoutCache())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /static", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, s.static)
	})
	mux.HandleFunc("GET /orders/{id}", s.unit)
	mux.HandleFunc("GET /orders", s.list)
	mux.HandleFunc("POST /orders", s.write)
	mux.HandleFunc("GET /study/metrics", contract.Metrics(&s.batches))

	srv := &http.Server{Addr: cfg.Listen, Handler: mux, ReadHeaderTimeout: contract.ReadHeaderTimeout}
	if !cfg.TLS {
		fmt.Fprintf(os.Stderr, "go-nethttp-dataloader: %s on %s (%s)\n", cfg.Protocol, cfg.Listen, cfg.Summary(nil))
		return srv.ListenAndServe()
	}
	if srv.TLSConfig, err = cfg.TLSConfig(); err != nil {
		return err
	}
	srv.Protocols = new(http.Protocols)
	srv.Protocols.SetHTTP1(cfg.Protocol == "h1")
	srv.Protocols.SetHTTP2(cfg.Protocol == "h2")
	fmt.Fprintf(os.Stderr, "go-nethttp-dataloader: %s on %s (%s)\n", cfg.Protocol, cfg.Listen, cfg.Summary(srv.TLSConfig))
	return srv.ListenAndServeTLS("", "")
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "go-nethttp-dataloader:", err)
		os.Exit(1)
	}
}
