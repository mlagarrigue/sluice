package audit

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// A container is read from what docker inspect printed, so these tests
// seed that output: the containers worth reporting are the ones started
// with the flags nobody should use, and nobody should start one to see the
// report.

// seededContainer is the container the comparison's stack must never run:
// root, every capability, the Docker socket, no bound on anything, a
// bridge network and a log nobody rotates.
const seededContainer = `[{
  "Name": "/web",
  "AppArmorProfile": "unconfined",
  "Config": {"User": "", "Image": "study-measured:opt",
    "Env": ["PATH=/usr/bin", "GODEBUG=tlsrsakex=1,x509sha1=1"]},
  "HostConfig": {
    "Privileged": true, "CapAdd": ["ALL"], "CapDrop": null,
    "SecurityOpt": ["seccomp=unconfined", "label=disable"],
    "ReadonlyRootfs": false, "NetworkMode": "bridge",
    "CpusetCpus": "", "NanoCpus": 0, "Memory": 0,
    "Ulimits": [{"Name": "nofile", "Soft": 1024, "Hard": 4096}],
    "LogConfig": {"Type": "json-file", "Config": {}}
  },
  "Mounts": [{"Type": "bind", "Source": "/var/run/docker.sock", "Destination": "/var/run/docker.sock"}]
}]`

// setContainer is the same server started the way the study's protocol
// asks.
const setContainer = `[{
  "Name": "/web",
  "AppArmorProfile": "docker-default",
  "Config": {"User": "65532:65532", "Image": "study-measured:opt", "Env": ["PATH=/usr/bin"]},
  "HostConfig": {
    "Privileged": false, "CapAdd": ["NET_BIND_SERVICE"], "CapDrop": ["ALL"],
    "SecurityOpt": ["no-new-privileges"],
    "ReadonlyRootfs": true, "NetworkMode": "host",
    "CpusetCpus": "0-3", "NanoCpus": 0, "Memory": 2147483648,
    "Ulimits": [{"Name": "nofile", "Soft": 65536, "Hard": 65536}],
    "LogConfig": {"Type": "local", "Config": {}}
  },
  "Mounts": []
}]`

// seedFile writes one file and returns its path.
func seedFile(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// fired returns the rules that spoke, and checks that each finding carries
// its five fields.
func fired(t *testing.T, sel Selection, s Spec) []string {
	t.Helper()
	target, err := Open(s)
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(sel, nil, target)
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

var containerRules = []string{
	"container/nofile", "container/cpuset", "container/memory", "container/network",
	"container/log-driver", "container/capabilities", "container/no-new-privileges",
	"container/seccomp", "container/apparmor", "container/read-only", "container/user",
	"container/docker-socket", "go/godebug",
}

func TestASeededContainerIsReadFlagByFlag(t *testing.T) {
	got := fired(t, Selection{Components: {"docker", "go"}},
		Spec{ContainerJSON: seedFile(t, "web.json", seededContainer)})
	for _, want := range containerRules {
		if !slices.Contains(got, want) {
			t.Errorf("%s stayed silent on the seeded container: %v", want, got)
		}
	}
}

func TestAContainerSetForTheStudyIsSilent(t *testing.T) {
	if got := fired(t, Selection{Components: {"docker", "go"}},
		Spec{ContainerJSON: seedFile(t, "web.json", setContainer)}); len(got) != 0 {
		t.Errorf("a container set as the protocol asks was reported: %v", got)
	}
}

// TestEveryCapabilityMistakeIsItsOwnFinding: privileged, ALL added and
// nothing dropped are three different flags to remove, and a report that
// named one would leave the other two in place.
func TestEveryCapabilityMistakeIsItsOwnFinding(t *testing.T) {
	target, err := Open(Spec{ContainerJSON: seedFile(t, "web.json",
		strings.Replace(seededContainer, `"Privileged": true`, `"Privileged": false`, 1))})
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(Selection{Components: {"docker"}, Goals: {"security"}}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	var currents []string
	for _, f := range findings {
		if f.Rule == "container/capabilities" {
			currents = append(currents, f.Current)
		}
	}
	if len(currents) != 2 || !strings.Contains(currents[0], "--cap-add ALL") || !strings.Contains(currents[1], "default capabilities") {
		t.Errorf("container/capabilities said %q, want --cap-add ALL then the default set kept", currents)
	}
}

// TestTheProductionProfileLeavesTheComparisonsRulesOut: pinned cores and a
// host network are what a comparison needs to measure the server alone; a
// deployment may have good reasons for neither.
func TestTheProductionProfileLeavesTheComparisonsRulesOut(t *testing.T) {
	got := fired(t, Selection{Components: {"docker"}, Profile: {"production"}},
		Spec{ContainerJSON: seedFile(t, "web.json", seededContainer)})
	for _, rule := range []string{"container/cpuset", "container/network"} {
		if slices.Contains(got, rule) {
			t.Errorf("%s spoke under the production profile: %v", rule, got)
		}
	}
	if !slices.Contains(got, "container/memory") {
		t.Errorf("container/memory is advice for any deployment, and stayed silent: %v", got)
	}
}

func TestAContainerRecordMustHoldOneContainer(t *testing.T) {
	for name, body := range map[string]string{
		"none":        `[]`,
		"two":         `[{"Name": "/a"}, {"Name": "/b"}]`,
		"not inspect": `{"Name": "/a"}`,
	} {
		if _, err := Open(Spec{ContainerJSON: seedFile(t, "c.json", body)}); err == nil {
			t.Errorf("%s: a record that is not one container was read", name)
		}
	}
}

func TestTheKernelLogNamesTheBoundReached(t *testing.T) {
	log := "[  1.000000] Linux version 6.18\n" +
		"[ 812.334455] nf_conntrack: nf_conntrack: table full, dropping packet\n" +
		"[ 812.334460] nf_conntrack: nf_conntrack: table full, dropping packet\n" +
		"[ 901.000000] Memory cgroup out of memory: Killed process 4242 (python3) total-vm:2048kB\n"
	target, err := Open(Spec{KernelLog: seedFile(t, "dmesg.txt", log)})
	if err != nil {
		t.Fatal(err)
	}
	findings, err := Run(Selection{Sources: {"logs"}}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	byRule := map[string]string{}
	for _, f := range findings {
		byRule[f.Rule] = f.Current
	}
	if c := byRule["linux/conntrack-full"]; !strings.HasPrefix(c, "2 line(s)") {
		t.Errorf("linux/conntrack-full said %q, want the two lines counted", c)
	}
	if c := byRule["linux/oom-kill"]; !strings.Contains(c, "1 process(es)") || !strings.HasSuffix(c, ": python3") {
		t.Errorf("linux/oom-kill said %q, want the one victim named", c)
	}

	quiet, err := Open(Spec{KernelLog: seedFile(t, "dmesg.txt", "[  1.000000] Linux version 6.18\n")})
	if err != nil {
		t.Fatal(err)
	}
	if fs, _ := Run(Selection{Sources: {"logs"}}, nil, quiet); len(fs) != 0 {
		t.Errorf("a quiet kernel log was reported: %v", fs)
	}
}

func TestTheSystemTableReadsWhatTheComparisonPublishes(t *testing.T) {
	files := healthyRoot()
	files["proc/sys/kernel/osrelease"] = "6.18.58\n"
	delete(files, "proc/sys/net/netfilter/nf_conntrack_max")
	target, err := Open(Spec{Root: seedRoot(t, files), PID: 42, ContainerJSON: seedFile(t, "web.json", setContainer)})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, s := range System(target) {
		got[s.Name] = s.Value
	}
	for name, want := range map[string]string{
		"kernel":                         "6.18.58",
		"net.core.somaxconn":             "4096",
		"net.ipv4.ip_local_port_range":   "10240 65535",
		"net.netfilter.nf_conntrack_max": "absent",
		"transparent_hugepage/enabled":   "always [madvise] never",
		"process 42 nofile":              "65536:65536",
		"container nofile":               "65536:65536",
		"container cpuset":               "0-3",
		"container memory":               "2147483648 bytes",
		"container network":              "host",
		"container log driver":           "local",
	} {
		if got[name] != want {
			t.Errorf("%s = %q, want %q", name, got[name], want)
		}
	}
	if len(System(&Target{})) != 0 {
		t.Error("a target nobody pointed at has a system table")
	}
}
