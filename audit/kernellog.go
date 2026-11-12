package audit

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// What the kernel said about the machine while it ran. A setting read in
// /proc is a bound; a line in the kernel's log is that bound reached, and
// it is often the only trace: a dropped packet or a killed process never
// reaches the application's own logs, which then show timeouts and
// restarts with no cause.

// oomVictim names the process a line says the OOM killer took, in the form
// every kernel since 2.6 prints: "Killed process 1234 (name)".
var oomVictim = regexp.MustCompile(`Killed process \d+ \(([^)]*)\)`)

// kernelLines returns the lines of the kernel log that contain any of the
// markers.
func kernelLines(t *Target, markers ...string) []string {
	var out []string
	for line := range strings.SplitSeq(t.KernelLog, "\n") {
		if slices.ContainsFunc(markers, func(m string) bool { return strings.Contains(line, m) }) {
			out = append(out, line)
		}
	}
	return out
}

// init registers linux/conntrack-full and linux/oom-kill, the two lines
// whose cause is a setting another rule reads.
func init() {
	Register(Rule{
		Name:  "linux/conntrack-full",
		Needs: Selection{Components: {"linux"}, Goals: {"reliability"}, Sources: {"logs"}},
		Doc:   "https://docs.kernel.org/networking/nf_conntrack-sysctl.html",
		Check: func(t *Target, _ Answers) []Finding {
			lines := kernelLines(t, "nf_conntrack: table full")
			if len(lines) == 0 {
				return nil
			}
			return []Finding{{
				Current:  fmt.Sprintf("%d line(s) \"nf_conntrack: table full, dropping packet\" in %s", len(lines), t.KernelLogPath),
				Proposed: "net.netfilter.nf_conntrack_max raised (see linux/conntrack), or the server's traffic left untracked: --network host and no NAT on its port",
				Why: "each line is new connections dropped before they reached a socket: the " +
					"client sees a timeout and retries, the server sees nothing, and the " +
					"application's logs have no line for it. A bridge network or a NAT rule " +
					"tracks every connection, so the table fills with the load",
			}}
		},
	})

	Register(Rule{
		Name:  "linux/oom-kill",
		Needs: Selection{Components: {"linux"}, Goals: {"reliability"}, Sources: {"logs"}},
		Doc:   "https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-interface-files",
		Check: func(t *Target, _ Answers) []Finding {
			lines := kernelLines(t, "Killed process ")
			if len(lines) == 0 {
				return nil
			}
			var victims []string
			for _, l := range lines {
				if m := oomVictim.FindStringSubmatch(l); len(m) == 2 && !slices.Contains(victims, m[1]) {
					victims = append(victims, m[1])
				}
			}
			return []Finding{{
				Current: fmt.Sprintf("the OOM killer took %d process(es) in %s: %s",
					len(lines), t.KernelLogPath, strings.Join(victims, ", ")),
				Proposed: "a memory bound on each service above its peak (--memory, MemoryMax=), and GOMEMLIMIT under it for a Go server",
				Why: "a process killed for memory restarts with no error of its own: its " +
					"clients see a reset, a supervisor starts it again, and the only trace is " +
					"this line. Without a bound per service, the kernel picks the victim by " +
					"size, which need not be the one that grew",
			}}
		},
	})
}
