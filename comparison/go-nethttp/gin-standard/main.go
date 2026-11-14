// Command gin-standard is the study's Gin contender as Gin's quick start
// writes it (study §2.4, "Standard"; level 2, §2.5): gin.Default() with its
// Logger and Recovery, c.JSON, ShouldBindJSON, a pgxpool with one query per
// request, no grouping. Gin runs in its default (debug) mode and writes an
// access log line per request to stdout: both are the quick start's, and
// published with the figures (§4, declared logs).
//
// The server is an http.Server around the router, the form Gin's
// documentation gives for a custom configuration ("Custom HTTP
// configuration"), so that the contract's TLS applies; HTTP/3 is quic-go's
// http3 around the same router, which Gin documents as RunQUIC. No
// compression: Gin's is a separate module (gin-contrib/gzip).
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/gin-gonic/gin"
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

type server struct {
	db *pgxpool.Pool
}

func (s *server) unit(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, errDoc{Error: "bad key"})
		return
	}
	var o order
	err = s.db.QueryRow(c.Request.Context(), "SELECT id, total, label FROM bench_orders WHERE id = $1", id).
		Scan(&o.ID, &o.Total, &o.Label)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		c.JSON(http.StatusNotFound, errDoc{Error: "no such key", Key: &id})
	case err != nil:
		log.Printf("unit %d: %v", id, err)
		c.JSON(http.StatusServiceUnavailable, errDoc{Error: "unavailable", Key: &id})
	default:
		c.JSON(http.StatusOK, o)
	}
}

func (s *server) list(c *gin.Context) {
	from, err := strconv.ParseInt(c.Query("from"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, errDoc{Error: "bad from"})
		return
	}
	limit, err := strconv.Atoi(c.Query("limit"))
	if err != nil || limit <= 0 || limit > contract.ListMax {
		c.JSON(http.StatusBadRequest, errDoc{Error: "bad limit", Key: &from})
		return
	}
	rows, _ := s.db.Query(c.Request.Context(), "SELECT id, total, label FROM bench_orders WHERE id > $1 ORDER BY id LIMIT $2", from, limit)
	items, err := pgx.CollectRows(rows, pgx.RowToStructByName[order])
	if err != nil {
		log.Printf("list %d: %v", from, err)
		c.JSON(http.StatusServiceUnavailable, errDoc{Error: "unavailable", Key: &from})
		return
	}
	c.JSON(http.StatusOK, listDoc{From: from, Limit: limit, Items: items})
}

func (s *server) write(c *gin.Context) {
	var req writeReq
	if err := c.ShouldBindJSON(&req); err != nil || req.Key == 0 {
		c.JSON(http.StatusBadRequest, errDoc{Error: "bad body"})
		return
	}
	if _, err := s.db.Exec(c.Request.Context(), "INSERT INTO comparison_writes (key, total) VALUES ($1, $2)", req.Key, req.Total); err != nil {
		log.Printf("insert: %v", err)
		c.JSON(http.StatusServiceUnavailable, errDoc{Error: "unavailable", Key: &req.Key})
		return
	}
	c.JSON(http.StatusOK, writeDoc{Key: req.Key})
}

func run() error {
	cfg, err := contract.FromEnv(os.Getenv)
	if err != nil {
		return err
	}
	if err := cfg.Offers([]string{"h1", "h2", "h3"}, nil); err != nil {
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

	s := &server{db: db}
	static := contract.StaticDoc()
	metrics := contract.Metrics(nil)
	r := gin.Default()
	r.GET("/static", func(c *gin.Context) { c.JSON(http.StatusOK, static) })
	r.GET("/orders/:id", s.unit)
	r.GET("/orders", s.list)
	r.POST("/orders", s.write)
	r.GET("/study/metrics", func(c *gin.Context) { metrics(c.Writer, c.Request) })

	srv := &http.Server{Addr: cfg.Listen, Handler: r, ReadHeaderTimeout: contract.ReadHeaderTimeout}
	if !cfg.TLS {
		fmt.Fprintf(os.Stderr, "go-gin-standard: %s on %s (%s)\n", cfg.Protocol, cfg.Listen, cfg.Summary(nil))
		return srv.ListenAndServe()
	}
	tlsCfg, err := cfg.TLSConfig()
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "go-gin-standard: %s on %s (%s)\n", cfg.Protocol, cfg.Listen, cfg.Summary(tlsCfg))
	if cfg.Protocol == "h3" {
		return (&http3.Server{Addr: cfg.Listen, Handler: r, TLSConfig: http3.ConfigureTLSConfig(tlsCfg)}).ListenAndServe()
	}
	srv.TLSConfig = tlsCfg
	srv.Protocols = new(http.Protocols)
	srv.Protocols.SetHTTP1(cfg.Protocol == "h1")
	srv.Protocols.SetHTTP2(cfg.Protocol == "h2")
	return srv.ListenAndServeTLS("", "")
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "go-gin-standard:", err)
		os.Exit(1)
	}
}
