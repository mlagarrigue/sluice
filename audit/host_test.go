package audit

import (
	"slices"
	"strings"
	"testing"
)

// stat writes /proc/stat's first lines with the times given, in ticks:
// user nice system idle iowait irq softirq steal guest guest_nice.
func stat(user, idle, steal string) string {
	return "cpu  " + user + " 0 100 " + idle + " 0 0 0 " + steal + " 0 0\ncpu0 1 0 1 1 0 0 0 0 0 0\nctxt 1\n"
}

func hostTarget(t *testing.T, spec Spec) *Target {
	t.Helper()
	target, err := Open(spec)
	if err != nil {
		t.Fatal(err)
	}
	return target
}

func hostRules(t *testing.T, target *Target, sel Selection) []string {
	t.Helper()
	findings, err := Run(sel, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, f := range findings {
		out = append(out, f.Rule+": "+f.Current)
	}
	return out
}

// TestStolenTimeIsReported: 8% of the step's CPU time stolen is a
// measurement of the neighbours; 0.5% is noise.
func TestStolenTimeIsReported(t *testing.T) {
	sel := Selection{Components: {"host"}, Sources: {"logs"}}
	// 1,000 ticks pass: 300 user, 620 idle, 80 stolen.
	high := hostTarget(t, Spec{StatBefore: seedFile(t, "b", stat("1000", "5000", "100")), StatAfter: seedFile(t, "a", stat("1300", "5620", "180"))})
	if got := hostRules(t, high, sel); len(got) != 1 || !strings.Contains(got[0], "host/steal: 8.0%") {
		t.Errorf("8%%: %v", got)
	}
	// 1,000 ticks pass, 5 stolen.
	low := hostTarget(t, Spec{StatBefore: seedFile(t, "b", stat("1000", "5000", "100")), StatAfter: seedFile(t, "a", stat("1300", "5695", "105"))})
	if got := hostRules(t, low, sel); len(got) != 0 {
		t.Errorf("0.5%%: %v", got)
	}
}

func TestStatIsReadTwiceInOrder(t *testing.T) {
	for name, spec := range map[string]Spec{
		"one reading":    {StatBefore: seedFile(t, "b", stat("1", "1", "0"))},
		"swapped":        {StatBefore: seedFile(t, "b", stat("9", "9", "0")), StatAfter: seedFile(t, "a", stat("1", "1", "0"))},
		"not /proc/stat": {StatBefore: seedFile(t, "b", "intr 1\n"), StatAfter: seedFile(t, "a", "intr 2\n")},
	} {
		if _, err := Open(spec); err == nil {
			t.Errorf("%s: read", name)
		}
	}
}

// fourThreadsOnTwoCores is a root with SMT on: CPUs 0 and 2 share core 0,
// 1 and 3 core 1, one socket.
func fourThreadsOnTwoCores(t *testing.T, extra map[string]string) string {
	t.Helper()
	files := map[string]string{"sys/devices/system/cpu/smt/active": "1"}
	for i, core := range []string{"0", "1", "0", "1"} {
		dir := "sys/devices/system/cpu/cpu" + string(rune('0'+i)) + "/topology/"
		files[dir+"core_id"], files[dir+"physical_package_id"] = core, "0"
	}
	for k, v := range extra {
		files[k] = v
	}
	return seedRoot(t, files)
}

func TestSMTIsAComparisonsQuestion(t *testing.T) {
	target := hostTarget(t, Spec{Root: fourThreadsOnTwoCores(t, nil)})
	if got := hostRules(t, target, Selection{Components: {"host"}, Profile: {"comparative"}}); len(got) != 1 || !strings.Contains(got[0], "4 CPUs on 2 cores") {
		t.Errorf("comparative: %v", got)
	}
	if got := hostRules(t, target, Selection{Components: {"host"}, Profile: {"production"}}); len(got) != 0 {
		t.Errorf("production: %v", got)
	}
}

const imds = `{"compute": {"azEnvironment": "AZUREPUBLICCLOUD", "location": "westeurope", "vmSize": "Standard_D8ds_v5",
 "zone": "2", "placementGroupId": ""}, "network": {"interface": []}}`

func systemOf(t *testing.T, spec Spec) map[string]string {
	t.Helper()
	got := map[string]string{}
	for _, s := range System(hostTarget(t, spec)) {
		got[s.Name] = s.Value
	}
	return got
}

// TestTheProviderIsPublished: Azure's size, region and zone from its
// metadata; WSL2 recognised by its kernel, with the common lines alone.
func TestTheProviderIsPublished(t *testing.T) {
	azure := systemOf(t, Spec{Root: fourThreadsOnTwoCores(t, nil), IMDS: seedFile(t, "imds.json", imds)})
	for name, want := range map[string]string{
		"host provider": "azure", "host azure vm size": "Standard_D8ds_v5", "host azure location": "westeurope",
		"host azure zone": "2", "host smt active": "1", "host cpus": "4", "host cores": "2", "host sockets": "1",
	} {
		if azure[name] != want {
			t.Errorf("azure: %s = %q, want %q", name, azure[name], want)
		}
	}
	wsl := systemOf(t, Spec{Root: fourThreadsOnTwoCores(t, map[string]string{"proc/sys/kernel/osrelease": "6.6.87.2-microsoft-standard-WSL2"})})
	if wsl["host provider"] != "wsl2" || wsl["host cores"] != "2" {
		t.Errorf("wsl2: %v", wsl)
	}
	if slices.ContainsFunc(keys(wsl), func(k string) bool { return strings.HasPrefix(k, "host azure") }) {
		t.Errorf("wsl2 publishes Azure's lines: %v", wsl)
	}
	if _, err := Open(Spec{IMDS: seedFile(t, "imds.json", `{"network": {}}`)}); err == nil {
		t.Error("metadata without compute.vmSize was read")
	}
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
