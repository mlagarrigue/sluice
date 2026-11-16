package audit

import (
	"fmt"
	"strings"
	"testing"
)

// stageLine is a generator's stage line: sent requests over conns kept
// connections, pct of them on fresh ones, closed connections the server
// ended.
func stageLine(sent, conns, pct, closed int) string {
	return fmt.Sprintf(`{"scenario":"unit","rate":1000,"conns":%d,"new_conns_pct":%d,"sent":%d,"ok":%d,"closed":%d,"reconnects":%d}`+"\n",
		conns, pct, sent, sent, closed, closed)
}

func perConnection(t *testing.T, line string, extra Spec) []Finding {
	t.Helper()
	extra.LoadLine = seedFile(t, "stage.jsonl", line)
	target, err := Open(extra)
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(Selection{Components: {"app", "linux"}, Sources: {"logs"}}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	var out []Finding
	for i := range findings {
		f := &findings[i]
		if f.Rule == "app/requests-per-connection" {
			out = append(out, *f)
		}
	}
	return out
}

// TestServerClosingEveryNRequests: a server that ends each connection
// after N requests is found from the generator's own count of the
// closures, and N is put on the defaults that give it — Tomcat's 100,
// httpstream's 1 024 when the server read is httpstream; a load that opens
// fresh connections on purpose, the server closing none, says nothing.
func TestServerClosingEveryNRequests(t *testing.T) {
	// 20 connections, 500 requests each, closed after every 100th.
	got := perConnection(t, stageLine(10000, 20, 0, 100), Spec{})
	if len(got) != 1 || !strings.Contains(got[0].Current, "Tomcat") || !strings.Contains(got[0].Proposed, "server.tomcat.max-keep-alive-requests") {
		t.Errorf("every 100 requests: %+v", got)
	}
	// 20 connections, 5 120 requests each, closed after every 1 024th: 1 000
	// and 1 024 both fit the count, the server read says which.
	app := Spec{AppArgs: seedFile(t, "start.txt", appR+"-transport httpstream-h1\n")}
	got = perConnection(t, stageLine(102400, 20, 0, 100), app)
	if len(got) != 1 || !strings.Contains(got[0].Current, "httpstream") || strings.Contains(got[0].Current, "nginx") {
		t.Errorf("every 1 024 requests, httpstream read: %+v", got)
	}
	got = perConnection(t, stageLine(102400, 20, 0, 100), Spec{})
	if len(got) != 1 || !strings.Contains(got[0].Current, "httpstream") || !strings.Contains(got[0].Current, "nginx 1.19.10 and later") {
		t.Errorf("every 1 024 requests, nothing else read: %+v", got)
	}
	// 10 % fresh connections on purpose, none ended by the server.
	if got := perConnection(t, stageLine(10000, 20, 10, 0), Spec{}); len(got) != 0 {
		t.Errorf("fresh connections on purpose: %+v", got)
	}
	// Closures too rare for any published percentile.
	if got := perConnection(t, stageLine(1000000, 20, 0, 100), Spec{}); len(got) != 0 {
		t.Errorf("one closure every 10 000 requests: %+v", got)
	}
	// A limit no default gives.
	got = perConnection(t, stageLine(10000, 20, 0, 20), Spec{})
	if len(got) != 1 || !strings.Contains(got[0].Current, "no default the audit knows") {
		t.Errorf("every 500 requests: %+v", got)
	}
}

// TestFreshConnectionsAreNoReconnections: with the generator's line read,
// the fresh connections a load opens on purpose are taken out of the
// kernel's count before linux/reconnections divides.
func TestFreshConnectionsAreNoReconnections(t *testing.T) {
	line := Spec{LoadLine: seedFile(t, "stage.jsonl", stageLine(10000, 20, 10, 0))}
	if got := reconnections(t, 1020, line, "unknown"); len(got) != 0 {
		t.Errorf("1 000 fresh connections and the budget's 20: %+v", got)
	}
	if got := reconnections(t, 1020, Spec{}, "10000"); len(got) != 1 {
		t.Errorf("the same counters without the line: %+v", got)
	}
}

func TestLoadLineRefused(t *testing.T) {
	for _, text := range []string{"not json\n", `{"sent": 3}` + "\n", `{"window": 0, "sent": 1, "conns": 1, "closed": 0}` + "\n"} {
		if _, err := Open(Spec{LoadLine: seedFile(t, "stage.jsonl", text)}); err == nil {
			t.Errorf("%q: read", text)
		}
	}
}
