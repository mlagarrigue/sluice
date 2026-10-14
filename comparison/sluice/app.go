package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/mlagarrigue/sluice"
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
//
// The app does not know which driver answers either: the store (store.go) is
// what -driver swaps, the gateway in front of it stays.
type app struct {
	static  staticDoc
	st      store
	gw      *gateway.Gateway[int64, orderRow]
	listMax int
	codec   *codec // axis F; nil in R
	stream  bool   // axis E: the list is written as it comes
}

// answer is a status and either a body, encoded by the transport in one
// write, or a stream of fragments of the document (axis E), which the
// transport writes and flushes batch by batch. The stream runs when the
// transport pulls it, after the status has gone out: an error it meets then
// can only cut the document short.
type answer struct {
	status int
	body   any
	stream sluice.Stream[[]byte]
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

func newApp(o options) (*app, error) {
	st, err := newStore(o)
	if err != nil {
		return nil, err
	}
	if err := st.createWriteTable(context.Background()); err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("write table: %w", err)
	}
	a := &app{
		static: newStaticDoc(), st: st, listMax: o.listMax,
		codec: lookupCodec(o.compression), stream: o.response == responseStream,
	}
	a.gw = gateway.New(gateway.Config{Size: o.gatewaySize, Within: o.gatewayWithin},
		unitPipeline(st, o.gatewaySize))
	return a, nil
}

func (a *app) Close() error {
	var errs []error
	if a.gw != nil {
		errs = append(errs, a.gw.Close())
	}
	if a.st != nil {
		errs = append(errs, a.st.Close())
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

// unitPipeline is the gateway's pipeline, the same whatever the driver: the
// keys of a batch go to the store as one slice and come back as one map, and
// every caller gets its row or a miss.
func unitPipeline(st store, size int) func(sluice.Stream[*gateway.Call[int64, orderRow]]) {
	return func(calls sluice.Stream[*gateway.Call[int64, orderRow]]) {
		ids := make([]int64, 0, size)
		found := make(map[int64]orderRow, size)
		calls(func(batch sluice.Batch[*gateway.Call[int64, orderRow]]) bool {
			ids = ids[:0]
			clear(found)
			for _, c := range batch.Items {
				ids = append(ids, c.In)
			}
			if err := st.unit(context.Background(), ids, found); err != nil {
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

var (
	errNotFound = errors.New("no such key")
	errDecode   = errors.New("decode")
)

// route answers one request. target is the raw origin-form target, query
// attached, as both transports hand it over.
func (a *app) route(ctx context.Context, method, target string, body []byte) answer {
	path, rawQuery, _ := strings.Cut(target, "?")
	switch {
	case path == "/static" && method == "GET":
		return answer{status: 200, body: a.static}
	case strings.HasPrefix(path, "/orders/") && method == "GET":
		return a.unit(ctx, path[len("/orders/"):])
	case path == "/orders" && method == "GET":
		return a.list(ctx, rawQuery)
	case path == "/orders" && method == "POST":
		return a.write(ctx, body)
	}
	return answer{status: 404, body: errDoc{Error: "no such route"}}
}

func (a *app) unit(ctx context.Context, key string) answer {
	id, err := strconv.ParseInt(key, 10, 64)
	if err != nil {
		return answer{status: 400, body: errDoc{Error: "bad key"}}
	}
	row, err := a.gw.Do(ctx, id)
	return unitAnswer(id, row, err)
}

func unitAnswer(id int64, row orderRow, err error) answer {
	switch {
	case errors.Is(err, errNotFound):
		return answer{status: 404, body: errDoc{Error: "no such key", Key: &id}}
	case err != nil:
		return answer{status: 503, body: errDoc{Error: "unavailable", Key: &id}}
	}
	return answer{status: 200, body: row}
}

func (a *app) list(ctx context.Context, rawQuery string) answer {
	q, err := url.ParseQuery(rawQuery)
	if err != nil {
		return answer{status: 400, body: errDoc{Error: "bad query"}}
	}
	from, err := strconv.ParseInt(q.Get("from"), 10, 64)
	if err != nil {
		return answer{status: 400, body: errDoc{Error: "bad from"}}
	}
	limit, err := strconv.Atoi(q.Get("limit"))
	if err != nil || limit <= 0 || limit > a.listMax {
		return answer{status: 400, body: errDoc{Error: "bad limit", Key: &from}}
	}
	if a.stream {
		return answer{status: 200, stream: a.listStream(ctx, from, limit)}
	}
	items, err := listAll(ctx, a.st, from, limit)
	switch {
	case errors.Is(err, errDecode):
		return answer{status: 500, body: errDoc{Error: "decode", Key: &from}}
	case err != nil:
		return answer{status: 503, body: errDoc{Error: "unavailable", Key: &from}}
	}
	return answer{status: 200, body: listDoc{From: from, Limit: limit, Items: items}}
}

// listStream is axis E: the same document as listDoc encodes, byte for
// byte, produced as the store delivers it — the head with the key, then
// each batch of rows as one fragment, then the closing brackets. Each
// fragment is a batch of one element so that the transport flushes once
// per batch of rows, which is the grain the axis measures. The one
// difference with the buffered path is what an error can do: the status is
// already on the wire, so a store that fails mid-way cuts the document
// short and the client's parser says so.
func (a *app) listStream(ctx context.Context, from int64, limit int) sluice.Stream[[]byte] {
	return func(yield func(sluice.Batch[[]byte]) bool) {
		emit := func(b []byte) bool { return yield(sluice.Batch[[]byte]{Items: [][]byte{b}}) }
		if !emit(fmt.Appendf(nil, `{"from":%d,"limit":%d,"items":[`, from, limit)) {
			return
		}
		first := true
		err := a.st.listBatches(ctx, from, limit, func(rows []orderRow) bool {
			if len(rows) == 0 {
				return true
			}
			b, err := json.Marshal(rows)
			if err != nil {
				return false
			}
			b = b[1 : len(b)-1] // the rows without the array's own brackets
			if !first {
				b = append([]byte{','}, b...)
			}
			first = false
			return emit(b)
		})
		if err != nil {
			return
		}
		emit([]byte("]}"))
	}
}

func (a *app) write(ctx context.Context, body []byte) answer {
	var req writeReq
	if err := json.Unmarshal(body, &req); err != nil || req.Key == 0 {
		return answer{status: 400, body: errDoc{Error: "bad body"}}
	}
	if err := a.st.write(ctx, req.Key, req.Total); err != nil {
		return answer{status: 503, body: errDoc{Error: "unavailable", Key: &req.Key}}
	}
	return answer{status: 200, body: writeDoc{Key: req.Key}}
}
