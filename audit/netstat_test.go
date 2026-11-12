package audit

import (
	"strconv"
	"strings"
	"testing"
)

// netstat writes /proc/net/netstat's TcpExt pair with the counters given,
// and /proc/net/snmp's Tcp pair after it, as cat of both prints them.
func netstat(overflows, drops, timeouts, retrans int) string {
	return "TcpExt: SyncookiesSent ListenOverflows ListenDrops TCPTimeouts\n" +
		"TcpExt: 0 " + itoa(overflows) + " " + itoa(drops) + " " + itoa(timeouts) + "\n" +
		"Tcp: RtoAlgorithm ActiveOpens RetransSegs\n" +
		"Tcp: 1 10 " + itoa(retrans) + "\n"
}

func itoa(n int) string { return strconv.Itoa(n) }

func countersFindings(t *testing.T, before, after string, root string) []Finding {
	t.Helper()
	target, err := Open(Spec{
		NetstatBefore: seedFile(t, "before", before),
		NetstatAfter:  seedFile(t, "after", after),
		Root:          root,
	})
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(Selection{Components: {"linux"}, Sources: {"logs"}}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	return findings
}

// TestOverflowsAreTheListenQueue: a step that overflowed the accept queue
// is put on the queue, with somaxconn when the host was read.
func TestOverflowsAreTheListenQueue(t *testing.T) {
	root := seedRoot(t, map[string]string{"proc/sys/net/core/somaxconn": "128"})
	got := countersFindings(t, netstat(5, 5, 0, 0), netstat(125, 125, 0, 0), root)
	if len(got) != 1 || got[0].Rule != "linux/listen-overflows" ||
		!strings.Contains(got[0].Current, "ListenOverflows +120") || !strings.Contains(got[0].Current, "somaxconn = 128") ||
		!strings.Contains(got[0].Proposed, "somaxconn") {
		t.Fatalf("%+v", got)
	}
}

// TestOtherCountersAreNotTheQueue: retransmissions and timeouts move under
// any load on a lossy link; they say nothing of the listen queue.
func TestOtherCountersAreNotTheQueue(t *testing.T) {
	if got := countersFindings(t, netstat(7, 7, 0, 0), netstat(7, 7, 300, 4000), ""); len(got) != 0 {
		t.Fatalf("%+v", got)
	}
}

func TestNstatOutputIsReadToo(t *testing.T) {
	before := "#kernel\nTcpExtListenOverflows 3 0.0\nTcpExtListenDrops 3 0.0\n"
	after := "#kernel\nTcpExtListenOverflows 9 0.0\nTcpExtListenDrops 9 0.0\n"
	if got := countersFindings(t, before, after, ""); len(got) != 1 || !strings.Contains(got[0].Current, "+6") {
		t.Fatalf("%+v", got)
	}
}

func TestCountersAreReadTwiceAndWhole(t *testing.T) {
	if _, err := Open(Spec{NetstatBefore: seedFile(t, "b", netstat(0, 0, 0, 0))}); err == nil {
		t.Error("one reading was accepted")
	}
	for name, body := range map[string]string{
		"names without values": "TcpExt: ListenOverflows\n",
		"mismatched":           "TcpExt: A B\nTcpExt: 1\n",
		"not a number":         "TcpExt: A\nTcpExt: x\n",
		"empty":                "",
	} {
		if _, err := Open(Spec{NetstatBefore: seedFile(t, "b", body), NetstatAfter: seedFile(t, "a", body)}); err == nil {
			t.Errorf("%s: read", name)
		}
	}
}
