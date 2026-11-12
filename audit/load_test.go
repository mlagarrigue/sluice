package audit

import (
	"strconv"
	"strings"
	"testing"
)

func snmp(passive int) string {
	return "Tcp: RtoAlgorithm ActiveOpens PassiveOpens\nTcp: 1 0 " + strconv.Itoa(passive) + "\n"
}

func reconnections(t *testing.T, opens int, extra Spec, requests string) []Finding {
	t.Helper()
	extra.NetstatBefore = seedFile(t, "before", snmp(100))
	extra.NetstatAfter = seedFile(t, "after", snmp(100+opens))
	target, err := Open(extra)
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(Selection{Components: {"linux"}, Sources: {"logs"}}, Answers{"requests": requests}, target)
	if err != nil {
		t.Fatal(err)
	}
	return findings
}

// TestReconnectionsArePutOnWhatClosesThem: the setting that closes the
// connections is named — the server's budget when it is read, the proxy's
// next, the clients last.
func TestReconnectionsArePutOnWhatClosesThem(t *testing.T) {
	app := Spec{AppArgs: seedFile(t, "start.txt", appR+"-transport httpstream-h1 -max-requests-per-conn 4\n")}
	if got := reconnections(t, 2500, app, "10000"); len(got) != 1 || !strings.Contains(got[0].Current, "-max-requests-per-conn 4") {
		t.Errorf("the server's budget: %+v", got)
	}
	proxy := Spec{ProxyConfig: seedFile(t, "nginx.conf", nginxDefaults)}
	if got := reconnections(t, 2500, proxy, "10000"); len(got) != 1 || !strings.Contains(got[0].Current, "the proxy closes") {
		t.Errorf("the proxy: %+v", got)
	}
	if got := reconnections(t, 2500, Spec{}, "10000"); len(got) != 1 || !strings.Contains(got[0].Proposed, "clients that keep") {
		t.Errorf("the clients: %+v", got)
	}
	if got := reconnections(t, 12, app, "10000"); len(got) != 0 {
		t.Errorf("833 requests a connection: %+v", got)
	}
	if got := reconnections(t, 2500, app, "unknown"); len(got) != 0 {
		t.Errorf("no request count, nothing to divide: %+v", got)
	}
}

func activity(clients, idleInTx, background int) string {
	var b strings.Builder
	b.WriteString("state,backend_type\n")
	for i := range clients {
		state := "active"
		if i < idleInTx {
			state = "idle in transaction"
		}
		b.WriteString(state + ",client backend\n")
	}
	for range background {
		b.WriteString(",autovacuum launcher\n")
	}
	return b.String()
}

func activityFindings(t *testing.T, readings ...string) []string {
	t.Helper()
	target, err := Open(Spec{
		PGSettings: seedFile(t, "settings.csv", defaultSettings), // 100, 3 reserved
		PGActivity: seedFile(t, "activity.csv", strings.Join(readings, "")),
	})
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(Selection{Components: {"postgres"}, Sources: {"logs"}, Goals: {"reliability"}}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, f := range findings {
		out = append(out, f.Rule+": "+f.Current)
	}
	return out
}

func TestEveryClientSlotTakenIsReported(t *testing.T) {
	got := activityFindings(t, activity(40, 0, 3), activity(97, 0, 3))
	if len(got) != 1 || !strings.Contains(got[0], "postgres/connections-saturated: 97 client connections") {
		t.Errorf("97 of 97 at the peak: %v", got)
	}
	if got := activityFindings(t, activity(40, 0, 6)); len(got) != 0 {
		t.Errorf("40 of 97: %v", got)
	}
}

func TestSessionsIdleInTransactionAreReported(t *testing.T) {
	if got := activityFindings(t, activity(40, 12, 0)); len(got) != 1 || !strings.Contains(got[0], "12 of 40") {
		t.Errorf("12 of 40 idle in transaction: %v", got)
	}
	if got := activityFindings(t, activity(80, 4, 0)); len(got) != 0 {
		t.Errorf("4 of 80: %v", got)
	}
}

func TestActivityMustBePsqlsCSV(t *testing.T) {
	for name, body := range map[string]string{"empty": "", "no state": "pid,query\n1,select\n"} {
		if _, err := Open(Spec{PGActivity: seedFile(t, "a.csv", body)}); err == nil {
			t.Errorf("%s: read", name)
		}
	}
}
