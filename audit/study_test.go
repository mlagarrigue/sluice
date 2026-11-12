package audit

import (
	"fmt"
	"strings"
	"testing"
)

func studyDocument(cpu, gc float64, batches, keys int) string {
	return fmt.Sprintf(`{"runtime":"go1.26","cpu_seconds":%g,"gc":{"cpu_seconds":%g,"pauses":{"count":0,"seconds":0}},"batches":{"count":%d,"keys":%d}}`,
		cpu, gc, batches, keys)
}

func studyRules(t *testing.T, before, after string, extra Spec) []string {
	t.Helper()
	extra.MetricsBefore = seedFile(t, "before.json", before)
	extra.MetricsAfter = seedFile(t, "after.json", after)
	target, err := Open(extra)
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(Selection{Components: {"app"}, Sources: {"logs"}}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for i := range findings {
		out = append(out, findings[i].Rule+": "+findings[i].Current)
	}
	return out
}

func TestTheCollectorsShareIsReported(t *testing.T) {
	app := Spec{AppArgs: seedFile(t, "start.txt", "GOGC=50\n"+appR)}
	// 100 CPU-seconds in the stage, 30 of them the collector's.
	got := studyRules(t, studyDocument(10, 1, 0, 0), studyDocument(110, 31, 0, 0), app)
	if len(got) != 1 || !strings.Contains(got[0], "app/gc-share: the collector took 30%") || !strings.Contains(got[0], "GOGC 50, GOMEMLIMIT unset") {
		t.Errorf("30%%: %v", got)
	}
	if got := studyRules(t, studyDocument(10, 1, 0, 0), studyDocument(110, 4, 0, 0), Spec{}); len(got) != 0 {
		t.Errorf("3%%: %v", got)
	}
}

func TestBatchesOfOneAreReported(t *testing.T) {
	app := Spec{AppArgs: seedFile(t, "start.txt", appR)}
	// 1,000 batches, 1,200 keys: 1.2 a batch for a gateway of 64.
	got := studyRules(t, studyDocument(1, 0, 50, 60), studyDocument(2, 0, 1050, 1260), app)
	if len(got) != 1 || !strings.Contains(got[0], "app/batch-size: 1000 gateway batches of 1.2 keys") || !strings.Contains(got[0], "-gateway-size 64") {
		t.Errorf("1.2 a batch: %v", got)
	}
	if got := studyRules(t, studyDocument(1, 0, 0, 0), studyDocument(2, 0, 1000, 30000), app); len(got) != 0 {
		t.Errorf("30 a batch: %v", got)
	}
	if got := studyRules(t, studyDocument(1, 0, 0, 0), studyDocument(2, 0, 10, 10), app); len(got) != 0 {
		t.Errorf("ten batches say nothing: %v", got)
	}
}

func TestMetricsAreReadTwiceInOrder(t *testing.T) {
	for name, spec := range map[string]Spec{
		"one":     {MetricsBefore: seedFile(t, "b", studyDocument(1, 0, 0, 0))},
		"swapped": {MetricsBefore: seedFile(t, "b", studyDocument(9, 0, 9, 9)), MetricsAfter: seedFile(t, "a", studyDocument(1, 0, 0, 0))},
		"foreign": {MetricsBefore: seedFile(t, "b", `{"cpu_seconds":1}`), MetricsAfter: seedFile(t, "a", `{"cpu_seconds":2}`)},
	} {
		if _, err := Open(spec); err == nil {
			t.Errorf("%s: read", name)
		}
	}
}

// ss writes ss -tan's output: n sockets in TIME-WAIT from local ports
// counted up from first, to the database, and one listening socket.
func ss(n, first int) string {
	var b strings.Builder
	b.WriteString("State      Recv-Q Send-Q Local Address:Port  Peer Address:Port Process\n")
	b.WriteString("LISTEN     0      4096  0.0.0.0:8443        0.0.0.0:*\n")
	for i := range n {
		fmt.Fprintf(&b, "TIME-WAIT  0      0     10.0.0.4:%d     10.0.0.5:5432\n", first+i%28000)
	}
	return b.String()
}

func TestTimeWaitIsPutOnTheEphemeralPorts(t *testing.T) {
	root := seedRoot(t, map[string]string{"proc/sys/net/ipv4/ip_local_port_range": "32768\t60999"})
	run := func(text string) []Finding {
		target, err := Open(Spec{Root: root, SS: seedFile(t, "ss.txt", text)})
		if err != nil {
			t.Fatal(err)
		}
		findings, _ := Run(Selection{Components: {"linux"}, Sources: {"logs"}}, nil, target)
		return findings
	}
	if got := run(ss(25000, 32768)); len(got) != 1 || !strings.Contains(got[0].Current, "25000 sockets in TIME-WAIT") ||
		!strings.Contains(got[0].Current, "28232 ports") {
		t.Errorf("25,000 of 28,232: %+v", got)
	}
	if got := run(ss(500, 32768)); len(got) != 0 {
		t.Errorf("500: %+v", got)
	}
	// TIME-WAIT on the server's own port takes no ephemeral port.
	if got := run(ss(25000, 1000)); len(got) != 0 {
		t.Errorf("below the range: %+v", got)
	}
	if _, err := Open(Spec{SS: seedFile(t, "x", "TIME-WAIT 0 0 a:1 b:2\n")}); err == nil {
		t.Error("output without ss's header was read")
	}
}
