package audit

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// What the machine imposes on the program, and what the program cannot
// fix by itself. Every value is read under [Target.Root], so a test seeds
// a root of its own: the settings that matter are the ones nobody wants
// to apply to their own machine to find out.
//
// A knob that is not there is not a knob set to zero. A kernel without
// nf_conntrack has no bound to report, and a rule that read an absent
// file as a low value would fill a report with settings nobody set.
//
// Every reason says where the value shows up under load, because that is
// what makes it actionable: a listen queue too short is ListenOverflows
// in nstat, not an abstraction.

// A floor is a numeric setting whose value below a bound costs something
// named. One table rather than one function each: the shape is identical,
// and a reader compares the bounds at a glance.
type floor struct {
	name  string // the rule's name, after "linux/"
	path  string // under the root
	goal  string
	doc   string
	least int64
	unit  string
	why   string
	fix   string
}

var floors = []floor{
	{
		name: "somaxconn", path: "proc/sys/net/core/somaxconn", goal: "performance",
		doc: "https://docs.kernel.org/networking/ip-sysctl.html", least: 1024, unit: "connections",
		why: "the accept queue of every listening socket is capped by this value whatever the " +
			"program asks for: a burst larger than it is dropped, the client retries after a " +
			"timeout, and the server sees nothing. It shows up as ListenOverflows and " +
			"ListenDrops in nstat, never in the application's logs",
		fix: "sysctl -w net.core.somaxconn=1024, and the same in sysctl.d",
	},
	{
		name: "syn-backlog", path: "proc/sys/net/ipv4/tcp_max_syn_backlog", goal: "performance",
		doc: "https://docs.kernel.org/networking/ip-sysctl.html", least: 1024, unit: "half-open connections",
		why: "connections waiting for the third packet of the handshake queue here. Under a burst, " +
			"or a SYN flood, a short queue drops them and the kernel starts answering with " +
			"syncookies, which costs the window scaling and the timestamps of those connections",
		fix: "sysctl -w net.ipv4.tcp_max_syn_backlog=1024",
	},
	{
		name: "netdev-backlog", path: "proc/sys/net/core/netdev_max_backlog", goal: "performance",
		doc: "https://docs.kernel.org/networking/ip-sysctl.html", least: 1000, unit: "packets",
		why: "packets taken off the card queue here before a core processes them. Too short and " +
			"they are dropped under a burst — which a UDP server, so QUIC, pays in loss and the " +
			"congestion controller reads as congestion",
		fix: "sysctl -w net.core.netdev_max_backlog=4096",
	},
	{
		name: "udp-rmem", path: "proc/sys/net/core/rmem_max", goal: "performance",
		doc: "https://github.com/quic-go/quic-go/wiki/UDP-Buffer-Sizes", least: 7 << 20, unit: "bytes",
		why: "a QUIC implementation asks for a receive buffer of several megabytes and is capped " +
			"here: the kernel grants what this allows and the library warns that it could not " +
			"increase it. A buffer too small drops datagrams in a burst, which the connection " +
			"reads as loss and answers by sending less",
		fix: "sysctl -w net.core.rmem_max=7340032",
	},
	{
		name: "udp-wmem", path: "proc/sys/net/core/wmem_max", goal: "performance",
		doc: "https://github.com/quic-go/quic-go/wiki/UDP-Buffer-Sizes", least: 7 << 20, unit: "bytes",
		why: "the send side of the same bound: a writer that fills the socket faster than the " +
			"card drains it blocks or drops, and a server answering many connections at once is " +
			"exactly that writer",
		fix: "sysctl -w net.core.wmem_max=7340032",
	},
	{
		name: "conntrack", path: "proc/sys/net/netfilter/nf_conntrack_max", goal: "reliability",
		doc: "https://docs.kernel.org/networking/nf_conntrack-sysctl.html", least: 65536, unit: "flows",
		why: "when a firewall or a container network tracks connections, every one of them takes a " +
			"slot: the table full drops new connections and writes nf_conntrack: table full in " +
			"dmesg, while the application only sees timeouts. A host that tracks nothing has no " +
			"such file, and nothing to set",
		fix: "sysctl -w net.netfilter.nf_conntrack_max=262144",
	},
}

// init registers the host rules, in this order: one linux/<name> rule per
// entry of floors (a kernel value with its least), then linux/ephemeral-ports,
// linux/swap, linux/transparent-hugepages, linux/cpu-governor, linux/nofile
// and go/gomaxprocs. Each is one Register call, read on its own.
func init() {
	for _, f := range floors {
		Register(Rule{
			Name:  "linux/" + f.name,
			Needs: Selection{Components: {"linux"}, Goals: {f.goal}, Sources: {"config"}},
			Doc:   f.doc,
			Check: func(t *Target, _ Answers) []Finding {
				got, ok := t.hostInt(f.path)
				if !ok || got >= f.least {
					return nil
				}
				return []Finding{{
					Current:  fmt.Sprintf("%s = %d %s", f.path, got, f.unit),
					Proposed: fmt.Sprintf("%d %s at least (%s)", f.least, f.unit, f.fix),
					Why:      f.why,
				}}
			},
		})
	}

	Register(Rule{
		Name:  "linux/ephemeral-ports",
		Needs: Selection{Components: {"linux"}, Goals: {"reliability"}, Sources: {"config"}},
		Doc:   "https://docs.kernel.org/networking/ip-sysctl.html",
		Check: func(t *Target, _ Answers) []Finding {
			text, ok := t.hostFile("proc/sys/net/ipv4/ip_local_port_range")
			if !ok {
				return nil
			}
			fields := strings.Fields(text)
			if len(fields) != 2 {
				return nil
			}
			low, err1 := strconv.Atoi(fields[0])
			high, err2 := strconv.Atoi(fields[1])
			if err1 != nil || err2 != nil || high-low >= 16384 {
				return nil
			}
			return []Finding{{
				Current:  fmt.Sprintf("%d ports (%d to %d)", high-low+1, low, high),
				Proposed: "sysctl -w net.ipv4.ip_local_port_range=\"10240 65535\"",
				Why: "every outgoing connection takes one port per destination, and a closed one " +
					"holds it for the TIME_WAIT delay: a narrow window is a server that cannot " +
					"open a connection to its database under load, with an error naming the " +
					"address and not the cause",
			}}
		},
	})

	Register(Rule{
		Name:  "linux/swap",
		Needs: Selection{Components: {"linux"}, Goals: {"reliability"}, Sources: {"config"}},
		Doc:   "https://docs.kernel.org/admin-guide/sysctl/vm.html",
		Check: func(t *Target, _ Answers) []Finding {
			swaps, ok := t.hostFile("proc/swaps")
			if !ok {
				return nil
			}
			// The first line is the header; a line under it is a swap area.
			if len(strings.Split(strings.TrimSpace(swaps), "\n")) < 2 {
				return nil
			}
			swappiness, ok := t.hostInt("proc/sys/vm/swappiness")
			if !ok || swappiness <= 10 {
				return nil
			}
			return []Finding{{
				Current:  fmt.Sprintf("swap is on and vm.swappiness = %d", swappiness),
				Proposed: "vm.swappiness=1 on a latency-sensitive server, or no swap at all",
				Why: "a page of the heap swapped out costs a disk read the next time the garbage " +
					"collector walks it, which is a pause nothing in the program explains. The " +
					"measurement of a tail latency on a swapping machine measures the disk",
			}}
		},
	})

	Register(Rule{
		Name:  "linux/transparent-hugepages",
		Needs: Selection{Components: {"linux"}, Goals: {"reliability"}, Sources: {"config"}},
		Doc:   "https://docs.kernel.org/admin-guide/mm/transhuge.html",
		Check: func(t *Target, _ Answers) []Finding {
			text, ok := t.hostFile("sys/kernel/mm/transparent_hugepage/enabled")
			if !ok || !strings.Contains(text, "[always]") {
				return nil
			}
			return []Finding{{
				Current:  "transparent hugepages: " + text,
				Proposed: "madvise, so that only a program asking for them gets them",
				Why: "under always, the kernel compacts memory to hand out huge pages while the " +
					"program runs: the allocation that triggers it waits, and the wait lands on " +
					"one request in a thousand — a p99 nobody can attribute from inside",
			}}
		},
	})

	Register(Rule{
		Name:  "linux/cpu-governor",
		Needs: Selection{Components: {"linux"}, Goals: {"performance"}, Sources: {"config"}},
		Doc:   "https://docs.kernel.org/admin-guide/pm/cpufreq.html",
		Check: func(t *Target, answers Answers) []Finding {
			text, ok := t.hostFile("sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
			if !ok || text == "performance" {
				return nil
			}
			if answers.Get("role") == "workstation" {
				return nil // a laptop trades the clock for its battery on purpose
			}
			return []Finding{{
				Current:  "scaling_governor = " + text,
				Proposed: "performance on a server, or a measurement machine",
				Why: "the clock ramps when the load arrives, so the first requests of a burst are " +
					"served by a slower core than the rest: a benchmark reads it as variance it " +
					"cannot place, and a p99 carries it for good",
			}}
		},
	})

	Register(Rule{
		Name:  "linux/nofile",
		Needs: Selection{Components: {"linux"}, Goals: {"reliability"}, Sources: {"config"}},
		Doc:   "https://man7.org/linux/man-pages/man2/getrlimit.2.html",
		Check: func(t *Target, _ Answers) []Finding {
			if t.Root == "" || t.PID == 0 {
				return nil
			}
			text, ok := t.hostFile(filepath.Join("proc", strconv.Itoa(t.PID), "limits"))
			if !ok {
				return nil
			}
			soft, found := int64(0), false
			for line := range strings.SplitSeq(text, "\n") {
				if !strings.HasPrefix(line, "Max open files") {
					continue
				}
				fields := strings.Fields(strings.TrimPrefix(line, "Max open files"))
				if len(fields) == 0 {
					continue
				}
				if n, err := strconv.ParseInt(fields[0], 10, 64); err == nil {
					soft, found = n, true
				}
			}
			if !found || soft >= 65536 {
				return nil
			}
			return []Finding{{
				Current:  fmt.Sprintf("the process may open %d files", soft),
				Proposed: "LimitNOFILE=65536 in the unit file, or --ulimit nofile=65536 on the container",
				Why: "a connection is a file: this is the ceiling on connections the server can " +
					"hold, whatever its own bounds say. Reached, accept fails with EMFILE and the " +
					"server stops answering while staying up, which reads as a hang rather than a " +
					"limit. The default of 1024 is a desktop's",
			}}
		},
	})

	Register(Rule{
		Name:  "go/gomaxprocs",
		Needs: Selection{Components: {"go"}, Goals: {"performance"}, Sources: {"config"}},
		Doc:   "https://pkg.go.dev/runtime#GOMAXPROCS",
		Check: func(t *Target, _ Answers) []Finding {
			quota, ok := cgroupCPUQuota(t)
			if !ok {
				return nil
			}
			lang, ok := moduleLanguage(t)
			if !ok || lang >= 125 {
				// From language version 1.25 the runtime reads the cgroup
				// quota itself, and keeps reading it as it changes.
				return nil
			}
			return []Finding{{
				Current: fmt.Sprintf("the cgroup allows %.2f CPU and the module's language version is 1.%d",
					quota, lang-100),
				Proposed: "raise the go directive to 1.25 or later, or set GOMAXPROCS to the quota",
				Why: "below that language version the runtime defaults to every logical CPU of the " +
					"machine and ignores the quota: it starts that many threads for a share of a " +
					"core or two, and the scheduler throttles them in bursts — latency that looks " +
					"like the garbage collector and is not",
			}}
		},
	})
}

// cgroupCPUQuota reads the CPU bandwidth the process's cgroup allows, in
// cores. Only cgroup v2 is read: v1 is being retired, and a host on it
// reports nothing rather than something half right.
func cgroupCPUQuota(t *Target) (float64, bool) {
	text, ok := t.hostFile("sys/fs/cgroup/cpu.max")
	if !ok {
		return 0, false
	}
	fields := strings.Fields(text)
	if len(fields) != 2 || fields[0] == "max" {
		return 0, false // no quota at all
	}
	quota, err1 := strconv.ParseFloat(fields[0], 64)
	period, err2 := strconv.ParseFloat(fields[1], 64)
	if err1 != nil || err2 != nil || period == 0 {
		return 0, false
	}
	return quota / period, true
}

// moduleLanguage reads the go directive of the module as a number: 1.26.0
// is 126. It is the language version that decides the runtime's defaults,
// and it lives in go.mod — not in the toolchain that built the binary.
func moduleLanguage(t *Target) (int, bool) {
	if t.ModuleDir == "" {
		return 0, false
	}
	b, err := os.ReadFile(filepath.Join(t.ModuleDir, "go.mod")) //nolint:gosec // G304: the directory was opened by Open
	if err != nil {
		return 0, false
	}
	for line := range strings.SplitSeq(string(b), "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		line = strings.TrimSpace(line)
		rest, ok := strings.CutPrefix(line, "go ")
		if !ok {
			continue
		}
		parts := strings.Split(strings.TrimSpace(rest), ".")
		if len(parts) < 2 {
			continue
		}
		major, err1 := strconv.Atoi(parts[0])
		minor, err2 := strconv.Atoi(parts[1])
		if err1 != nil || err2 != nil {
			continue
		}
		return major*100 + minor, true
	}
	return 0, false
}
