package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand/v2"
)

// listLimit is the list scenario's page: 1 000 rows (§2.2).
const listLimit = 1000

type kind int

const (
	static kind = iota
	unit
	list
	write
)

// request is one of the stage: what to send, the key the answer must echo
// under which field, and whether this one is fully validated (§4, 1 %).
type request struct {
	kind     kind
	method   string
	path     string
	body     []byte
	keyField string
	key      int64
	full     bool
}

// scenario draws the i-th request of a stage from one seed: the keys form
// a permutation of 1..keyspace, so a stage never asks twice for the same
// one (§2.1), and every contender of a round sees the same sequence.
type scenario func(i int, key int64, full bool) request

func (sc scenario) draw(seed uint64, keyspace int) func(i int) request {
	r := rand.New(rand.NewPCG(seed, seed)) //nolint:gosec // G404: a seeded draw is the point
	perm := r.Perm(keyspace)
	return func(i int) request {
		// The 1 % is a function of (seed, i), not of the calls before it:
		// the i-th request is the same whoever asks for it.
		full := rand.New(rand.NewPCG(seed, uint64(i))).IntN(100) == 0 //nolint:gosec // G404: same
		return sc(i, int64(perm[i]+1), full)
	}
}

var scenarios = map[string]scenario{
	"static": func(_ int, _ int64, full bool) request {
		return request{kind: static, method: "GET", path: "/static", full: full}
	},
	"unit":  unitRequest,
	"list":  listRequest,
	"write": writeRequest,
	// mixed is 80 % unit, 15 % list, 5 % write (README), by position in
	// the sequence so that the mix is exact whatever the seed.
	"mixed": func(i int, key int64, full bool) request {
		switch i % 20 {
		case 3, 7, 11:
			return listRequest(i, key, full)
		case 15:
			return writeRequest(i, key, full)
		}
		return unitRequest(i, key, full)
	},
}

func unitRequest(_ int, key int64, full bool) request {
	return request{kind: unit, method: "GET", path: fmt.Sprintf("/orders/%d", key), keyField: "id", key: key, full: full}
}

func listRequest(_ int, key int64, full bool) request {
	return request{kind: list, method: "GET", path: fmt.Sprintf("/orders?from=%d&limit=%d", key, listLimit), keyField: "from", key: key, full: full}
}

func writeRequest(_ int, key int64, full bool) request {
	body := fmt.Appendf(nil, `{"key":%d,"total":%d}`, key, key*3)
	return request{kind: write, method: "POST", path: "/orders", body: body, keyField: "key", key: key, full: full}
}

// orderRow is a row of bench_orders as the study's server serves it.
type orderRow struct {
	ID    int64  `json:"id"`
	Total int64  `json:"total"`
	Label string `json:"label"`
}

// check is the cheap control of §4 on every answer: the body parses to the
// end, the key asked for is the key served, a list's ids climb strictly
// above its bound and the page is full. On the sampled 1 %, the whole
// document is compared to the one the dataset predicts.
func check(rq request, body []byte) error {
	if rq.keyField == "" {
		var v any
		if err := json.Unmarshal(body, &v); err != nil {
			return fmt.Errorf("body is not JSON: %w", err)
		}
		return nil
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(body, &doc); err != nil {
		return fmt.Errorf("body is not an object: %w", err)
	}
	var got int64
	if err := json.Unmarshal(doc[rq.keyField], &got); err != nil {
		return fmt.Errorf("field %q: %w", rq.keyField, err)
	}
	if got != rq.key {
		return fmt.Errorf("asked for key %d, answer carries %d", rq.key, got)
	}
	if rq.kind == list {
		var items []orderRow
		if err := json.Unmarshal(doc["items"], &items); err != nil {
			return fmt.Errorf("items: %w", err)
		}
		prev := rq.key
		for _, it := range items {
			if it.ID <= prev {
				return fmt.Errorf("list from %d: ids not strictly increasing at %d", rq.key, it.ID)
			}
			prev = it.ID
		}
		if len(items) == 0 {
			return fmt.Errorf("list from %d: empty", rq.key)
		}
		if rq.full {
			return validateList(rq.key, items)
		}
		return nil
	}
	if rq.full {
		return validate(rq, body)
	}
	return nil
}

// validate is the full validation of §4 against the document the dataset
// predicts: bench_orders holds id g, total 7·g, label "order-g" (the
// fixtures of internal/bench/cmd/pgfixtures), and a write answers with its
// key alone.
func validate(rq request, body []byte) error {
	var want any
	switch rq.kind {
	case unit:
		want = expectedRow(rq.key)
	case write:
		want = map[string]any{"key": rq.key}
	default:
		return nil
	}
	var got any
	if err := json.Unmarshal(body, &got); err != nil {
		return err
	}
	w, err := json.Marshal(want)
	if err != nil {
		return err
	}
	if !equalJSON(got, w) {
		return fmt.Errorf("full validation: got %s, want %s", body, w)
	}
	return nil
}

func validateList(from int64, items []orderRow) error {
	for i, it := range items {
		if want := expectedRow(from + int64(i) + 1); it != want {
			return fmt.Errorf("full validation: list from %d, row %d is %+v, want %+v", from, i, it, want)
		}
	}
	return nil
}

func expectedRow(id int64) orderRow {
	return orderRow{ID: id, Total: id * 7, Label: fmt.Sprintf("order-%d", id)}
}

// equalJSON compares a parsed document to an encoded one after both went
// through encoding/json: key order and number format cannot fail it, only
// the values (§4).
func equalJSON(got any, want []byte) bool {
	var w any
	if err := json.Unmarshal(want, &w); err != nil {
		return false
	}
	x, err := json.Marshal(got)
	if err != nil {
		return false
	}
	y, err := json.Marshal(w)
	if err != nil {
		return false
	}
	return bytes.Equal(x, y)
}
