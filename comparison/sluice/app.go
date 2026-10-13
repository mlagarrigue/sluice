package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/gateway"
)

// The four scenarios of the study's level 1 (§2.2), served by one app that
// does not know which transport carried the request:
//
//	GET  /static                 static small: a fixed ~1 KB document
//	GET  /orders/{id}            unit read: one row by key, through the gateway
//	GET  /orders?from=K&limit=N  list: N rows above K, in key order
//	POST /orders  {"key":K,...}  write: one row inserted, the key echoed
//
// Every dynamic answer carries the key it was asked for (§2.1), which is what
// lets a load generator check every response under load (§4). The mixed
// scenario is the generator's business — 80/15/5 of the three dynamic cases
// — and needs nothing of its own here.
//
// Answers are Go values rather than bytes: the transport adapters encode them,
// net/http through web.WriteJSON as R does and httpstream through
// encoding/json, so the two produce the same document from the same value.
type app struct {
	static  staticDoc
	gw      *gateway.Gateway[int64, orderRow]
	gwConn  *postgres.Conn
	pool    chan *postgres.Conn
	listMax int
}

type answer struct {
	status int
	body   any
}

type orderRow struct {
	ID    int64  `json:"id"`
	Total int64  `json:"total"`
	Label string `json:"label"`
}

type staticDoc struct {
	Status  string   `json:"status"`
	Service string   `json:"service"`
	Version string   `json:"version"`
	Padding []string `json:"padding"`
}

type listDoc struct {
	From  int64      `json:"from"`
	Limit int        `json:"limit"`
	Items []orderRow `json:"items"`
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
	unitQuery = `SELECT id, total, label FROM bench_orders WHERE id = ANY($1) ORDER BY id`
	listQuery = `SELECT id, total, label FROM bench_orders WHERE id > $1 ORDER BY id LIMIT $2`
	// The write table is never read by a scenario (§2.2): the expected answer
	// of a read must not depend on the writes before it. Autovacuum is off on
	// it so that nothing fires mid-step; the campaign truncates it between
	// repetitions.
	writeTable = `CREATE TABLE IF NOT EXISTS comparison_writes (
		id bigserial PRIMARY KEY, key bigint NOT NULL, total bigint NOT NULL,
		created timestamptz NOT NULL DEFAULT now())`
	writeNoVacuum = `ALTER TABLE comparison_writes SET (autovacuum_enabled = off)`
	writeQuery    = `INSERT INTO comparison_writes (key, total) VALUES ($1, $2)`
)

func dial(cfg dbConfig) (*postgres.Conn, error) {
	nc, err := net.DialTimeout("tcp", cfg.addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	conn, err := postgres.Startup(nc, postgres.StartupConfig{
		User: cfg.user, Database: cfg.database, Password: cfg.password,
	})
	if err != nil {
		return nil, fmt.Errorf("postgres startup: %w", err)
	}
	return conn, nil
}

func newApp(o options) (*app, error) {
	a := &app{
		static:  newStaticDoc(),
		pool:    make(chan *postgres.Conn, o.dbConns),
		listMax: o.listMax,
	}
	for range o.dbConns {
		c, err := dial(o.db)
		if err != nil {
			_ = a.Close()
			return nil, err
		}
		a.pool <- c
	}
	c := <-a.pool
	err := c.Exec(context.Background(), writeTable)
	if err == nil {
		err = c.Exec(context.Background(), writeNoVacuum)
	}
	a.pool <- c
	if err != nil {
		_ = a.Close()
		return nil, fmt.Errorf("write table: %w", err)
	}

	if a.gwConn, err = dial(o.db); err != nil {
		_ = a.Close()
		return nil, err
	}
	a.gw = gateway.New(gateway.Config{Size: o.gatewaySize, Within: o.gatewayWithin},
		unitPipeline(a.gwConn, o.gatewaySize))
	return a, nil
}

func (a *app) Close() error {
	var errs []error
	if a.gw != nil {
		errs = append(errs, a.gw.Close())
	}
	if a.gwConn != nil {
		errs = append(errs, a.gwConn.Close())
	}
	for len(a.pool) > 0 {
		errs = append(errs, (<-a.pool).Close())
	}
	return errors.Join(errs...)
}

// newStaticDoc is about 1 KB once encoded, the size §2.2 names.
func newStaticDoc() staticDoc {
	d := staticDoc{Status: "ok", Service: "sluice-study", Version: "r"}
	for i := range 40 {
		d.Padding = append(d.Padding, fmt.Sprintf("line-%02d-abcdefghijklmnop", i))
	}
	return d
}

// unitPipeline is the gateway's pipeline: every key of a batch leaves as one
// array parameter and comes back in one round trip. Mostly the example of
// package gateway, with the label added.
func unitPipeline(conn *postgres.Conn, size int) func(sluice.Stream[*gateway.Call[int64, orderRow]]) {
	return func(calls sluice.Stream[*gateway.Call[int64, orderRow]]) {
		ids := make([]int64, 0, size)
		buf := make([]byte, 0, 4+size*12)
		found := make(map[int64]orderRow, size)
		calls(func(batch sluice.Batch[*gateway.Call[int64, orderRow]]) bool {
			ids = ids[:0]
			clear(found)
			for _, c := range batch.Items {
				ids = append(ids, c.In)
			}
			buf = postgres.AppendInt8Array(buf[:0], ids)
			src := conn.Query(context.Background(), unitQuery, [][]byte{buf},
				postgres.QueryConfig{BatchRows: size + 1, ParamOIDs: []uint32{postgres.OIDInt8Array}})
			src.Stream()(func(rows sluice.Batch[postgres.Row]) bool {
				for _, r := range rows.Items {
					row, err := decodeOrder(r)
					if err != nil {
						continue
					}
					found[row.ID] = row
				}
				return true
			})
			if err := src.Err(); err != nil {
				for _, c := range batch.Items {
					c.Fail(err)
				}
				return true
			}
			for _, c := range batch.Items {
				row, ok := found[c.In]
				if !ok {
					c.Fail(errNotFound)
					continue
				}
				c.Reply(row)
			}
			return true
		})
	}
}

var errNotFound = errors.New("no such key")

func decodeOrder(r postgres.Row) (orderRow, error) {
	idB, _ := r.Value(0)
	totalB, _ := r.Value(1)
	labelB, _ := r.Value(2)
	id, err := postgres.DecodeInt8(idB)
	if err != nil {
		return orderRow{}, err
	}
	total, err := postgres.DecodeInt8(totalB)
	if err != nil {
		return orderRow{}, err
	}
	return orderRow{ID: id, Total: total, Label: postgres.DecodeText(labelB)}, nil
}

// route answers one request. target is the raw origin-form target, query
// attached, as both transports hand it over.
func (a *app) route(ctx context.Context, method, target string, body []byte) answer {
	path, rawQuery, _ := strings.Cut(target, "?")
	switch {
	case path == "/static" && method == "GET":
		return answer{200, a.static}
	case strings.HasPrefix(path, "/orders/") && method == "GET":
		return a.unit(ctx, path[len("/orders/"):])
	case path == "/orders" && method == "GET":
		return a.list(ctx, rawQuery)
	case path == "/orders" && method == "POST":
		return a.write(ctx, body)
	}
	return answer{404, errDoc{Error: "no such route"}}
}

func (a *app) unit(ctx context.Context, key string) answer {
	id, err := strconv.ParseInt(key, 10, 64)
	if err != nil {
		return answer{400, errDoc{Error: "bad key"}}
	}
	row, err := a.gw.Do(ctx, id)
	return unitAnswer(id, row, err)
}

func unitAnswer(id int64, row orderRow, err error) answer {
	switch {
	case errors.Is(err, errNotFound):
		return answer{404, errDoc{Error: "no such key", Key: &id}}
	case err != nil:
		return answer{503, errDoc{Error: "unavailable", Key: &id}}
	}
	return answer{200, row}
}

func (a *app) list(ctx context.Context, rawQuery string) answer {
	q, err := url.ParseQuery(rawQuery)
	if err != nil {
		return answer{400, errDoc{Error: "bad query"}}
	}
	from, err := strconv.ParseInt(q.Get("from"), 10, 64)
	if err != nil {
		return answer{400, errDoc{Error: "bad from"}}
	}
	limit, err := strconv.Atoi(q.Get("limit"))
	if err != nil || limit <= 0 || limit > a.listMax {
		return answer{400, errDoc{Error: "bad limit", Key: &from}}
	}
	conn := <-a.pool
	defer func() { a.pool <- conn }()

	params := [][]byte{postgres.AppendInt8(nil, from), postgres.AppendInt8(nil, int64(limit))}
	doc := listDoc{From: from, Limit: limit, Items: make([]orderRow, 0, limit)}
	src := conn.Query(ctx, listQuery, params, postgres.QueryConfig{
		BatchRows: 1024, ParamOIDs: []uint32{postgres.OIDInt8, postgres.OIDInt8},
	})
	for b := range src.Stream() {
		for _, r := range b.Items {
			row, err := decodeOrder(r)
			if err != nil {
				return answer{500, errDoc{Error: "decode", Key: &from}}
			}
			doc.Items = append(doc.Items, row)
		}
	}
	if err := src.Err(); err != nil {
		return answer{503, errDoc{Error: "unavailable", Key: &from}}
	}
	return answer{200, doc}
}

func (a *app) write(ctx context.Context, body []byte) answer {
	var req writeReq
	if err := json.Unmarshal(body, &req); err != nil || req.Key == 0 {
		return answer{400, errDoc{Error: "bad body"}}
	}
	conn := <-a.pool
	defer func() { a.pool <- conn }()

	params := [][]byte{postgres.AppendInt8(nil, req.Key), postgres.AppendInt8(nil, req.Total)}
	src := conn.Query(ctx, writeQuery, params, postgres.QueryConfig{
		BatchRows: 2, ParamOIDs: []uint32{postgres.OIDInt8, postgres.OIDInt8},
	})
	for range src.Stream() {
	}
	if err := src.Err(); err != nil {
		return answer{503, errDoc{Error: "unavailable", Key: &req.Key}}
	}
	return answer{200, writeDoc{Key: req.Key}}
}
