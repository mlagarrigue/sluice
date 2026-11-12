package audit

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// What the kernel counted during a load: two readings of its network
// counters, one before a step and one after, and the difference between
// them. A setting says what could go wrong; a counter that moved says what
// did. A rule here names the symptom, then the setting that explains it,
// and stays silent about the counters a load moves for other reasons —
// retransmissions and timeouts on a busy link are not a short queue.

// Counters is the difference between two readings of the kernel's
// counters, keyed as nstat names them: TcpExtListenOverflows,
// TcpRetransSegs.
type Counters struct {
	BeforePath, AfterPath string
	Delta                 map[string]int64
}

// parseCounters reads /proc/net/netstat or /proc/net/snmp — pairs of lines,
// "TcpExt: names…" then "TcpExt: values…" — or nstat -az's output, one
// counter a line after a "#kernel" header. Several files may be joined.
func parseCounters(text string) (map[string]int64, error) {
	out := map[string]int64{}
	lines := strings.Split(text, "\n")
	for i := 0; i < len(lines); i++ {
		f := strings.Fields(lines[i])
		switch {
		case len(f) == 0 || f[0] == "#kernel":
		case strings.HasSuffix(f[0], ":"):
			if i+1 >= len(lines) {
				return nil, fmt.Errorf("line %d: names with no values after them", i+1)
			}
			v := strings.Fields(lines[i+1])
			if len(v) != len(f) || v[0] != f[0] {
				return nil, fmt.Errorf("line %d: %d names and %d values", i+1, len(f)-1, len(v)-1)
			}
			prefix := strings.TrimSuffix(f[0], ":")
			for k := 1; k < len(f); k++ {
				n, err := strconv.ParseInt(v[k], 10, 64)
				if err != nil {
					return nil, fmt.Errorf("line %d: %s = %q", i+2, f[k], v[k])
				}
				out[prefix+f[k]] = n
			}
			i++
		case len(f) >= 2: // nstat: name, value, rate
			n, err := strconv.ParseInt(f[1], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("line %d: %s = %q", i+1, f[0], f[1])
			}
			out[f[0]] = n
		default:
			return nil, fmt.Errorf("line %d: %q is not a counter", i+1, lines[i])
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no counter: want /proc/net/netstat, /proc/net/snmp or nstat -az")
	}
	return out, nil
}

func openCounters(before, after string) (*Counters, error) {
	if before == "" || after == "" {
		return nil, errors.New("the counters are read twice, before and after a load: name both files")
	}
	read := func(path string) (map[string]int64, error) {
		b, err := os.ReadFile(path) //nolint:gosec // G304: the path is the caller's
		if err != nil {
			return nil, err
		}
		m, err := parseCounters(string(b))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		return m, nil
	}
	b, err := read(before)
	if err != nil {
		return nil, err
	}
	a, err := read(after)
	if err != nil {
		return nil, err
	}
	c := &Counters{BeforePath: before, AfterPath: after, Delta: map[string]int64{}}
	for k, v := range a {
		if w, ok := b[k]; ok {
			c.Delta[k] = v - w
		}
	}
	return c, nil
}

func init() {
	Register(Rule{
		Name:  "linux/listen-overflows",
		Needs: Selection{Components: {"linux"}, Goals: {"performance"}, Sources: {"logs"}},
		Doc:   "https://docs.kernel.org/networking/ip-sysctl.html",
		Check: func(t *Target, _ Answers) []Finding {
			c := t.Counters
			if c == nil {
				return nil
			}
			overflows, drops := c.Delta["TcpExtListenOverflows"], c.Delta["TcpExtListenDrops"]
			if overflows <= 0 && drops <= 0 {
				return nil
			}
			current := fmt.Sprintf("ListenOverflows +%d, ListenDrops +%d between %s and %s", overflows, drops, c.BeforePath, c.AfterPath)
			proposed := "net.core.somaxconn and net.ipv4.tcp_max_syn_backlog at 1024 at least (see linux/somaxconn), " +
				"and a program that accepts as fast as connections arrive"
			if n, ok := t.hostInt("proc/sys/net/core/somaxconn"); ok {
				current += fmt.Sprintf(", with somaxconn = %d", n)
				if n >= 1024 {
					proposed = "the queue is already 1024 or more: the program accepts too slowly for the burst — " +
						"more accepting goroutines or processes, or somaxconn raised further with the program's own backlog"
				}
			}
			return []Finding{{
				Current:  current,
				Proposed: proposed,
				Why: "each overflow is a connection the kernel dropped because the listening socket's accept " +
					"queue was full: the client waits a SYN timeout, one second and doubling, and retries. " +
					"The application never sees it; the latency of those clients is that second",
			}}
		},
	})
}
