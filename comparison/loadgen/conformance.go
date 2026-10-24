package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math/big"
	"mime"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

// The conformance check of §4 ("same answer"): before a series, a
// contender answers the requests a seed draws, and every answer is
// compared with the reference's — status, Content-Type, and the JSON after
// parsing and normalisation (key order, number format, spaces), never byte
// for byte, which would fail everywhere for legitimate reasons. It stops at
// the first difference and names it: the request, and where in the
// document the two part.
//
// The requests are the mixed scenario's (unit reads, lists, writes) with a
// static request every twentieth, in the order the seed draws them, one at
// a time on one connection to each server. A write's effect in the
// database is not checked here: the generator has no access to it.

// conformance is one run of the check: the contender, the reference, how
// many requests.
type conformance struct {
	stage
	reference string
	n         int
}

// difference is the first place two servers part.
type difference struct {
	Request   string `json:"request"`   // method and path
	Where     string `json:"where"`     // status, content-type, or a path in the document ($.items[3].label)
	Contender string `json:"contender"` // what the contender answered there
	Reference string `json:"reference"` // what the reference answered there
}

// conformanceReport is the line the check writes.
type conformanceReport struct {
	URL        string      `json:"url"`
	Reference  string      `json:"reference"`
	Proto      string      `json:"proto"`
	Encoding   string      `json:"encoding"`
	Seed       uint64      `json:"seed"`
	Checked    int         `json:"checked"`
	Conformant bool        `json:"conformant"`
	First      *difference `json:"first_difference,omitempty"`
}

// answer is one server's reply, as the check compares it.
type answer struct {
	status      int
	contentType string
	doc         any // the body parsed, numbers kept as written
	raw         string
}

func (cf conformance) run(ctx context.Context) (conformanceReport, error) {
	rep := conformanceReport{URL: cf.url, Reference: cf.reference, Proto: cf.proto, Encoding: cmp.Or(cf.encoding, "identity"), Seed: cf.seed}
	// One connection to each server, closed at the end.
	var hcs [2]*http.Client
	for k, url := range []string{cf.url, cf.reference} {
		st := cf.stage
		st.url = url
		cl, err := campaign{stage: st}.newClient()
		if err != nil {
			return rep, err
		}
		cn := cl.dial(false)
		defer cn.close() //nolint:gocritic // deferInLoop: two connections, both kept to the end of the check
		hcs[k] = cn.hc
	}
	draw := scenarios["mixed"].draw(cf.seed, cf.keyspace)
	for i := range cf.n {
		rq := draw(i)
		if i%20 == 19 {
			rq = scenarios["static"](i, 0, false)
		}
		var got [2]answer
		for k, url := range []string{cf.url, cf.reference} {
			a, err := cf.fetch(ctx, hcs[k], url, rq)
			if err != nil {
				return rep, fmt.Errorf("%s %s on %s: %w", rq.method, rq.path, url, err)
			}
			got[k] = a
		}
		rep.Checked++
		if d := compareAnswers(got[0], got[1]); d != nil {
			d.Request = rq.method + " " + rq.path
			rep.First = d
			return rep, nil
		}
	}
	rep.Conformant = true
	return rep, nil
}

// fetch sends one request and reads its answer, decoded.
func (cf conformance) fetch(ctx context.Context, hc *http.Client, url string, rq request) (answer, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, rq.method, url+rq.path, bytes.NewReader(rq.body)) //nolint:gosec // G704: the URLs are the flags
	if err != nil {
		return answer{}, err
	}
	if rq.body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if cf.encoding != "" {
		req.Header.Set("Accept-Encoding", cf.encoding)
	}
	resp, err := hc.Do(req) //nolint:gosec // G704: same
	if err != nil {
		return answer{}, err
	}
	defer resp.Body.Close() //nolint:errcheck // read to the end below
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return answer{}, err
	}
	d, err := decode(cf.encoding, resp.Header.Get("Content-Encoding"), body)
	if err != nil {
		return answer{}, err
	}
	a := answer{status: resp.StatusCode, contentType: normalType(resp.Header.Get("Content-Type")), raw: string(d.doc)}
	dec := json.NewDecoder(bytes.NewReader(d.doc))
	dec.UseNumber()
	if err := dec.Decode(&a.doc); err != nil || dec.More() {
		a.doc = nil // not JSON: compared as text
	}
	return a, nil
}

// normalType is a Content-Type as the check compares it: the media type
// and its parameters, lower case, in a fixed order (a charset of utf-8 is
// JSON's own and counts as none, RFC 8259 §8.1).
func normalType(ct string) string {
	mt, params, err := mime.ParseMediaType(ct)
	if err != nil {
		return strings.ToLower(strings.TrimSpace(ct))
	}
	if strings.EqualFold(params["charset"], "utf-8") && mt == "application/json" {
		delete(params, "charset")
	}
	return mime.FormatMediaType(mt, params)
}

// compareAnswers is nil when the contender's answer c matches the
// reference's r, or the first difference.
func compareAnswers(c, r answer) *difference {
	switch {
	case c.status != r.status:
		return &difference{Where: "status", Contender: strconv.Itoa(c.status), Reference: strconv.Itoa(r.status)}
	case c.contentType != r.contentType:
		return &difference{Where: "content-type", Contender: c.contentType, Reference: r.contentType}
	case c.doc == nil || r.doc == nil:
		if c.doc == nil && r.doc == nil && c.raw == r.raw {
			return nil
		}
		return &difference{Where: "$", Contender: clip(c.raw), Reference: clip(r.raw)}
	}
	return compareJSON("$", c.doc, r.doc)
}

// compareJSON walks two parsed documents in the reference's key order and
// returns the first place they part: numbers compared by value (1, 1.0
// and 1e0 are one number), objects by key whatever their order.
func compareJSON(at string, c, r any) *difference {
	differ := func() *difference {
		return &difference{Where: at, Contender: clip(show(c)), Reference: clip(show(r))}
	}
	switch rv := r.(type) {
	case map[string]any:
		cv, ok := c.(map[string]any)
		if !ok {
			return differ()
		}
		for _, k := range slices.Sorted(maps.Keys(rv)) {
			cvk, ok := cv[k]
			if !ok {
				return &difference{Where: at + "." + k, Contender: "(missing)", Reference: clip(show(rv[k]))}
			}
			if d := compareJSON(at+"."+k, cvk, rv[k]); d != nil {
				return d
			}
		}
		for k := range cv {
			if _, ok := rv[k]; !ok {
				return &difference{Where: at + "." + k, Contender: clip(show(cv[k])), Reference: "(missing)"}
			}
		}
		return nil
	case []any:
		cv, ok := c.([]any)
		if !ok {
			return differ()
		}
		for i := range min(len(cv), len(rv)) {
			if d := compareJSON(fmt.Sprintf("%s[%d]", at, i), cv[i], rv[i]); d != nil {
				return d
			}
		}
		if len(cv) != len(rv) {
			return &difference{Where: at + ".length", Contender: strconv.Itoa(len(cv)), Reference: strconv.Itoa(len(rv))}
		}
		return nil
	case json.Number:
		cv, ok := c.(json.Number)
		if !ok || !sameNumber(cv, rv) {
			return differ()
		}
		return nil
	}
	if c != r { // strings, booleans, null
		return differ()
	}
	return nil
}

// sameNumber compares two JSON numbers by value, exactly.
func sameNumber(a, b json.Number) bool {
	x, okx := new(big.Rat).SetString(string(a))
	y, oky := new(big.Rat).SetString(string(b))
	return okx && oky && x.Cmp(y) == 0
}

func show(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

// clip keeps a value short enough to read on one line.
func clip(s string) string {
	const most = 120
	if len(s) <= most {
		return s
	}
	return s[:most] + "…"
}
