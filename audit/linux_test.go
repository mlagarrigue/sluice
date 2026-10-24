package audit

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// The host is read under a root, so these tests seed one. That is the
// whole reason the root exists: the settings worth reporting are the ones
// nobody would set on their own machine to see the report, and a tool
// tried only against the machine running it is a tool tried once.

// seedRoot writes a root holding the files named, and nothing else: a file
// left out is a knob the kernel does not have.
func seedRoot(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for rel, body := range files {
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// lowRoot is a machine on the defaults a desktop ships, under a cgroup of
// two cores, with the process of a server started without its own limits.
func lowRoot() map[string]string {
	return map[string]string{
		"proc/sys/net/core/somaxconn":                          "128\n",
		"proc/sys/net/ipv4/tcp_max_syn_backlog":                "128\n",
		"proc/sys/net/core/netdev_max_backlog":                 "300\n",
		"proc/sys/net/core/rmem_max":                           "212992\n",
		"proc/sys/net/core/wmem_max":                           "212992\n",
		"proc/sys/net/netfilter/nf_conntrack_max":              "16384\n",
		"proc/sys/net/ipv4/ip_local_port_range":                "60000\t61000\n",
		"proc/swaps":                                           "Filename\tType\tSize\tUsed\tPriority\n/swapfile\tfile\t2097148\t0\t-2\n",
		"proc/sys/vm/swappiness":                               "60\n",
		"sys/kernel/mm/transparent_hugepage/enabled":           "[always] madvise never\n",
		"sys/devices/system/cpu/cpu0/cpufreq/scaling_governor": "powersave\n",
		"sys/fs/cgroup/cpu.max":                                "200000 100000\n",
		"proc/42/limits": "Limit                     Soft Limit           Hard Limit           Units\n" +
			"Max open files            1024                 4096                 files\n",
	}
}

// healthyRoot is the same machine, set for a server.
func healthyRoot() map[string]string {
	return map[string]string{
		"proc/sys/net/core/somaxconn":                          "4096\n",
		"proc/sys/net/ipv4/tcp_max_syn_backlog":                "4096\n",
		"proc/sys/net/core/netdev_max_backlog":                 "4096\n",
		"proc/sys/net/core/rmem_max":                           "7340032\n",
		"proc/sys/net/core/wmem_max":                           "7340032\n",
		"proc/sys/net/netfilter/nf_conntrack_max":              "262144\n",
		"proc/sys/net/ipv4/ip_local_port_range":                "10240\t65535\n",
		"proc/swaps":                                           "Filename\tType\tSize\tUsed\tPriority\n",
		"proc/sys/vm/swappiness":                               "1\n",
		"sys/kernel/mm/transparent_hugepage/enabled":           "always [madvise] never\n",
		"sys/devices/system/cpu/cpu0/cpufreq/scaling_governor": "performance\n",
		"sys/fs/cgroup/cpu.max":                                "max 100000\n",
		"proc/42/limits": "Limit                     Soft Limit           Hard Limit           Units\n" +
			"Max open files            65536                65536                files\n",
	}
}

// firedOnRoot returns the rules that spoke about a seeded host.
func firedOnRoot(t *testing.T, files map[string]string, module string) []string {
	t.Helper()
	target, err := Open(Spec{Root: seedRoot(t, files), PID: 42, Module: module})
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(Selection{Components: {"linux", "go"}, Sources: {"config"}}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, f := range findings {
		if f.Current == "" || f.Proposed == "" || f.Why == "" || f.Doc == "" || f.Goal == "" {
			t.Errorf("a finding of %s is missing one of its five fields: %+v", f.Rule, f)
		}
		out = append(out, f.Rule)
	}
	return out
}

func TestALowHostIsReadSettingBySetting(t *testing.T) {
	// A module whose language version is below the one that reads the
	// cgroup quota: the GOMAXPROCS rule then has something to say.
	old := t.TempDir()
	if err := os.WriteFile(filepath.Join(old, "go.mod"), []byte("module probe\n\ngo 1.24.0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fired := firedOnRoot(t, lowRoot(), old)
	for _, want := range []string{
		"linux/somaxconn", "linux/syn-backlog", "linux/netdev-backlog",
		"linux/udp-rmem", "linux/udp-wmem", "linux/conntrack",
		"linux/ephemeral-ports", "linux/swap", "linux/transparent-hugepages",
		"linux/cpu-governor", "linux/nofile", "go/gomaxprocs",
	} {
		if !slices.Contains(fired, want) {
			t.Errorf("%s stayed silent on the low host: %v", want, fired)
		}
	}
}

func TestAHealthyHostIsSilent(t *testing.T) {
	recent := t.TempDir()
	if err := os.WriteFile(filepath.Join(recent, "go.mod"), []byte("module probe\n\ngo 1.26.0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if fired := firedOnRoot(t, healthyRoot(), recent); len(fired) != 0 {
		t.Errorf("a host set for a server was reported: %v", fired)
	}
}

// TestAKnobTheKernelDoesNotHaveIsNotASettingAtZero: the one mistake that
// would fill every report with noise.
func TestAKnobTheKernelDoesNotHaveIsNotASettingAtZero(t *testing.T) {
	if fired := firedOnRoot(t, map[string]string{}, ""); len(fired) != 0 {
		t.Errorf("an empty root produced findings: %v", fired)
	}
	// A kernel that tracks no connection has no nf_conntrack_max, and the
	// rest of the host is fine: only that rule may stay silent, and no
	// other may speak.
	files := healthyRoot()
	delete(files, "proc/sys/net/netfilter/nf_conntrack_max")
	delete(files, "sys/kernel/mm/transparent_hugepage/enabled")
	if fired := firedOnRoot(t, files, ""); len(fired) != 0 {
		t.Errorf("two absent knobs were read as low values: %v", fired)
	}
	// A knob that exists and holds nothing says nothing either.
	files = healthyRoot()
	files["proc/sys/net/core/somaxconn"] = "\n"
	if fired := firedOnRoot(t, files, ""); slices.Contains(fired, "linux/somaxconn") {
		t.Errorf("an empty knob was read as a value: %v", fired)
	}
}

// TestTheHostIsReadOnlyWhereItWasAllowed: no root, no host read; and the
// governor is the machine's role, which is a question and not a reading.
func TestTheHostIsReadOnlyWhereItWasAllowed(t *testing.T) {
	target, err := Open(Spec{})
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(Selection{Components: {"linux"}, Sources: {"config"}}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Errorf("the host was read although no root was given: %+v", findings)
	}

	target, err = Open(Spec{Root: seedRoot(t, lowRoot()), PID: 42})
	if err != nil {
		t.Fatal(err)
	}
	onWorkstation, err := Run(Selection{Components: {"linux"}, Sources: {"config"}}, Answers{"role": "workstation"}, target)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range onWorkstation {
		if f.Rule == "linux/cpu-governor" {
			t.Error("a workstation was told to pin its clock to performance")
		}
	}
	// And on a server, the same host, the same reading, the rule speaks.
	onServer, err := Run(Selection{Components: {"linux"}, Sources: {"config"}}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, f := range onServer {
		names = append(names, f.Rule)
	}
	if !slices.Contains(names, "linux/cpu-governor") {
		t.Errorf("the governor rule is silent on a server: %v", names)
	}
}

// TestGomaxprocsIsSilentWithoutAQuotaOrWithARecentLanguage: the rule has
// two conditions, and each one alone must keep it quiet.
func TestGomaxprocsIsSilentWithoutAQuotaOrWithARecentLanguage(t *testing.T) {
	old := t.TempDir()
	if err := os.WriteFile(filepath.Join(old, "go.mod"), []byte("module probe\n\ngo 1.24.0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// An old language version, but no quota: nothing to say.
	noQuota := lowRoot()
	noQuota["sys/fs/cgroup/cpu.max"] = "max 100000\n"
	if fired := firedOnRoot(t, noQuota, old); slices.Contains(fired, "go/gomaxprocs") {
		t.Error("the rule spoke although the cgroup caps nothing")
	}
	// A quota, but no module to read the language version from.
	if fired := firedOnRoot(t, lowRoot(), ""); slices.Contains(fired, "go/gomaxprocs") {
		t.Error("the rule spoke although no module said which language version applies")
	}
	// And the figure it reports is the quota in cores, not the raw file.
	target, err := Open(Spec{Root: seedRoot(t, lowRoot()), Module: old})
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(Selection{Components: {"go"}, Sources: {"config"}}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || !strings.Contains(findings[0].Current, "2.00 CPU") ||
		!strings.Contains(findings[0].Current, "1.24") {
		t.Fatalf("%+v: want the quota in cores and the language version", findings)
	}
}
