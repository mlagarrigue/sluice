package audit

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// What the comparison's server counted during a load, from two answers of
// its metrics point (comparison/sluice/metrics.go: every number cumulative
// since the server started, read at the start and the end of a stage), and
// the sockets ss listed meanwhile. The difference is the stage's: the
// collector's share of the CPU, the gateway's batches, the connections
// waiting out TIME-WAIT.

// StudyMetrics is the difference between two metrics documents.
type StudyMetrics struct {
	BeforePath, AfterPath string
	CPUSeconds            float64 // the process's CPU, the runtime's estimate
	GCSeconds             float64 // the collector's part of it
	Batches, Keys         uint64  // the gateway's SQL batches and the keys in them
}

type studyDoc struct {
	CPUSeconds float64 `json:"cpu_seconds"`
	GC         *struct {
		CPUSeconds float64 `json:"cpu_seconds"`
	} `json:"gc"`
	Batches *struct {
		Count uint64 `json:"count"`
		Keys  uint64 `json:"keys"`
	} `json:"batches"`
}

func openStudyMetrics(before, after string) (*StudyMetrics, error) {
	if before == "" || after == "" {
		return nil, errors.New("the metrics are read twice, at the start and the end of a load: name both files")
	}
	var docs [2]studyDoc
	for i, path := range []string{before, after} {
		b, err := os.ReadFile(path) //nolint:gosec // G304: the path is the caller's
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(b, &docs[i]); err != nil {
			return nil, fmt.Errorf("%s: not the metrics point's JSON: %w", path, err)
		}
		if docs[i].GC == nil || docs[i].Batches == nil {
			return nil, fmt.Errorf("%s: no gc or batches: want GET /study/metrics", path)
		}
	}
	b, a := docs[0], docs[1]
	if a.CPUSeconds <= b.CPUSeconds || a.Batches.Count < b.Batches.Count {
		return nil, fmt.Errorf("%s was not read after %s: its counters are not larger", after, before)
	}
	return &StudyMetrics{
		BeforePath: before, AfterPath: after,
		CPUSeconds: a.CPUSeconds - b.CPUSeconds,
		GCSeconds:  a.GC.CPUSeconds - b.GC.CPUSeconds,
		Batches:    a.Batches.Count - b.Batches.Count,
		Keys:       a.Batches.Keys - b.Batches.Keys,
	}, nil
}

// Sockets is what ss -tan listed: the TIME-WAIT sockets by local port.
type Sockets struct {
	Path      string
	TimeWait  int
	LocalPort []int // of each TIME-WAIT socket
}

func openSockets(path string) (*Sockets, error) {
	b, err := os.ReadFile(path) //nolint:gosec // G304: the path is the caller's
	if err != nil {
		return nil, err
	}
	s := &Sockets{Path: path}
	header := false
	for line := range strings.SplitSeq(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		if f[0] == "State" {
			header = true
			continue
		}
		if f[0] != "TIME-WAIT" || len(f) < 5 {
			continue
		}
		s.TimeWait++
		s.LocalPort = append(s.LocalPort, hostPort(f[3]))
	}
	if !header {
		return nil, fmt.Errorf("%s: no State header: want what ss -tan printed", path)
	}
	return s, nil
}

// gcBound is the share of the CPU past which the collector is a cost the
// configuration can lower: the runtime aims at a quarter of the CPU only
// while marking, so a stage's average above a tenth is a heap that cycles
// often.
const gcBound = 0.10

// init registers the stage's rules, in this order: the collector's share,
// the gateway's batch size, then the TIME-WAIT sockets against the
// ephemeral ports.
func init() {
	Register(Rule{
		Name:  "app/gc-share",
		Needs: Selection{Components: {"app"}, Goals: {"performance"}, Sources: {"logs"}},
		Doc:   "https://go.dev/doc/gc-guide",
		Check: func(t *Target, _ Answers) []Finding {
			m := t.Study
			if m == nil || m.CPUSeconds <= 0 || m.GCSeconds/m.CPUSeconds < gcBound {
				return nil
			}
			current := fmt.Sprintf("the collector took %.0f%% of the CPU the server spent between %s and %s",
				100*m.GCSeconds/m.CPUSeconds, m.BeforePath, m.AfterPath)
			if t.App != nil {
				current += fmt.Sprintf(", GOGC %s, GOMEMLIMIT %s",
					cmp.Or(t.App.Env["GOGC"], "unset"), cmp.Or(t.App.Env["GOMEMLIMIT"], "unset"))
			}
			return []Finding{{
				Current:  current,
				Proposed: "GOMEMLIMIT near the memory the server may use, with GOGC raised (or off) under it: fewer cycles for the same heap",
				Why: "every cycle marks the live heap: a heap that fills quickly against a small GOGC is " +
					"collected often, and the CPU it takes is CPU the requests do not get — it shows " +
					"as throughput lost and as the p99 of the requests a cycle lands on",
			}}
		},
	})

	Register(Rule{
		Name:  "app/batch-size",
		Needs: Selection{Components: {"app"}, Goals: {"performance"}, Sources: {"logs"}},
		Doc:   connBudgetDoc,
		Check: func(t *Target, _ Answers) []Finding {
			m := t.Study
			if m == nil || m.Batches < 100 {
				return nil // too few batches to say what a batch is
			}
			mean := float64(m.Keys) / float64(m.Batches)
			if mean >= 2 {
				return nil
			}
			current := fmt.Sprintf("%d gateway batches of %.1f keys on average", m.Batches, mean)
			if t.App != nil {
				current += " for -gateway-size " + t.App.Flags["gateway-size"] + " and -gateway-within " + t.App.Flags["gateway-within"]
			}
			return []Finding{{
				Current: current,
				Proposed: "a load with more unit reads in flight at once, or a longer -gateway-within; " +
					"the value comes from the study's axis C, measured against the round trip to the database",
				Why: "a batch of one key pays the round trip a batch exists to share: the gateway then " +
					"adds its wait to every read and saves nothing, which a stage at this load cannot " +
					"tell apart from a server without one",
			}}
		},
	})

	Register(Rule{
		Name:  "linux/time-wait",
		Needs: Selection{Components: {"linux"}, Goals: {"reliability"}, Sources: {"logs"}},
		Doc:   "https://docs.kernel.org/networking/ip-sysctl.html",
		Check: func(t *Target, _ Answers) []Finding {
			s := t.TimeWait
			if s == nil {
				return nil
			}
			text, ok := t.hostFile("proc/sys/net/ipv4/ip_local_port_range")
			f := strings.Fields(text)
			if !ok || len(f) != 2 {
				return nil
			}
			low, err1 := strconv.Atoi(f[0])
			high, err2 := strconv.Atoi(f[1])
			if err1 != nil || err2 != nil || high < low {
				return nil
			}
			taken := 0
			for _, p := range s.LocalPort {
				if low <= p && p <= high {
					taken++
				}
			}
			ports := high - low + 1
			if taken*2 < ports {
				return nil
			}
			return []Finding{{
				Current: fmt.Sprintf("%d sockets in TIME-WAIT on ephemeral ports in %s, for %d ports (%d to %d)",
					taken, s.Path, ports, low, high),
				Proposed: "connections kept between requests (keep-alive towards the database and the application, " +
					"see linux/reconnections), and net.ipv4.ip_local_port_range wider",
				Why: "a connection this machine closes holds its local port for the TIME-WAIT delay: past the " +
					"range, a new outgoing connection fails with an address error, under the load " +
					"that opened them",
			}}
		},
	})
}
