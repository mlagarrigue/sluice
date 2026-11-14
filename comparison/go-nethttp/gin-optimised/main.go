// Command gin-optimised is the study's Gin contender after its optimisation
// budget (study §2.4, "Optimised"): the routes, documents, queries and
// protocols of gin-standard, with each change listed, and its source, in
// comparison/README.md. Every change comes from Gin's own documentation,
// except the list's scan, which the Go contender's optimised version makes
// too:
//   - gin.New() with Recovery alone: no access log ("Blank Gin without
//     middleware by default"; §4 allows cutting it as a listed
//     optimisation);
//   - release mode, which Gin's debug mode asks for in production;
//   - SetTrustedProxies(nil): no proxy in front, so Gin computes no client
//     address from headers ("Don't trust all proxies");
//   - goccy/go-json as Gin's JSON codec, set at runtime through
//     codec/json.API ("Custom json codec at runtime"; the same codec as the
//     go_json build tag of "Build with json replacement");
//   - the list scanned by position into a slice sized to the limit.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/gin-gonic/gin"
	ginjson "github.com/gin-gonic/gin/codec/json"
	gojson "github.com/goccy/go-json"
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

// writeReq holds the numbers as written: goccy/go-json decodes a number past
// int64 into an int64 field without an error, overflowed, where
// encoding/json -- and R -- refuse it. They are parsed as R decodes them.
type writeReq struct {
	Key   gojson.Number `json:"key"`
	Total gojson.Number `json:"total"`
}

// int64s returns the key and the total, or false where R's decoding into
// int64 fails; an absent total is 0, as it is there.
func (w writeReq) int64s() (key, total int64, ok bool) {
	key, err := strconv.ParseInt(string(w.Key), 10, 64)
	if err != nil {
		return 0, 0, false
	}
	if w.Total != "" {
		if total, err = strconv.ParseInt(string(w.Total), 10, 64); err != nil {
			return 0, 0, false
		}
	}
	return key, total, true
}

type writeDoc struct {
	Key int64 `json:"key"`
}

type errDoc struct {
	Error string `json:"error"`
	Key   *int64 `json:"key,omitempty"`
}

// goJSON is Gin's codec interface over goccy/go-json, as Gin's own go_json
// build writes it.
type goJSON struct{}

func (goJSON) Marshal(v any) ([]byte, error)      { return gojson.Marshal(v) }
func (goJSON) Unmarshal(data []byte, v any) error { return gojson.Unmarshal(data, v) }
func (goJSON) MarshalIndent(v any, prefix, indent string) ([]byte, error) {
	return gojson.MarshalIndent(v, prefix, indent)
}
func (goJSON) NewEncoder(w io.Writer) ginjson.Encoder { return gojson.NewEncoder(w) }
func (goJSON) NewDecoder(r io.Reader) ginjson.Decoder { return gojson.NewDecoder(r) }

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
	items, err := s.listRows(c.Request.Context(), from, limit)
	if err != nil {
		log.Printf("list %d: %v", from, err)
		c.JSON(http.StatusServiceUnavailable, errDoc{Error: "unavailable", Key: &from})
		return
	}
	c.JSON(http.StatusOK, listDoc{From: from, Limit: limit, Items: items})
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

func (s *server) write(c *gin.Context) {
	var req writeReq
	err := c.ShouldBindJSON(&req)
	key, total, ok := req.int64s()
	if err != nil || !ok || key == 0 {
		c.JSON(http.StatusBadRequest, errDoc{Error: "bad body"})
		return
	}
	if _, err := s.db.Exec(c.Request.Context(), "INSERT INTO comparison_writes (key, total) VALUES ($1, $2)", key, total); err != nil {
		log.Printf("insert: %v", err)
		c.JSON(http.StatusServiceUnavailable, errDoc{Error: "unavailable", Key: &key})
		return
	}
	c.JSON(http.StatusOK, writeDoc{Key: key})
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

	ginjson.API = goJSON{}
	gin.SetMode(gin.ReleaseMode)
	s := &server{db: db}
	static := contract.StaticDoc()
	metrics := contract.Metrics(nil)
	r := gin.New()
	r.Use(gin.Recovery())
	if err := r.SetTrustedProxies(nil); err != nil {
		return err
	}
	r.GET("/static", func(c *gin.Context) { c.JSON(http.StatusOK, static) })
	r.GET("/orders/:id", s.unit)
	r.GET("/orders", s.list)
	r.POST("/orders", s.write)
	r.GET("/study/metrics", func(c *gin.Context) { metrics(c.Writer, c.Request) })

	srv := &http.Server{Addr: cfg.Listen, Handler: r, ReadHeaderTimeout: contract.ReadHeaderTimeout}
	if !cfg.TLS {
		fmt.Fprintf(os.Stderr, "go-gin-optimised: %s on %s (%s)\n", cfg.Protocol, cfg.Listen, cfg.Summary(nil))
		return srv.ListenAndServe()
	}
	tlsCfg, err := cfg.TLSConfig()
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "go-gin-optimised: %s on %s (%s)\n", cfg.Protocol, cfg.Listen, cfg.Summary(tlsCfg))
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
		fmt.Fprintln(os.Stderr, "go-gin-optimised:", err)
		os.Exit(1)
	}
}
