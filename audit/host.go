package audit

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// The machine under the program: what the hypervisor takes from it, how
// its cores are shared, and what its provider says it is. The common
// checks read the host's own files under [Target.Root]; a provider adds
// what its metadata service reports, read from a saved file, and nothing
// else changes when one is added.

// A hostProvider is where the machine runs: a cloud whose metadata was
// saved, or a host recognised by its own files.
type hostProvider interface {
	// Name is the provider as the report names it: azure, wsl2.
	Name() string
	// Settings are the lines it adds to the system table.
	Settings() []Setting
}

// wsl2 is Windows' Linux virtual machine: no metadata service, the common
// checks alone.
type wsl2 struct{}

func (wsl2) Name() string        { return "wsl2" }
func (wsl2) Settings() []Setting { return nil }

// provider is the host's provider: the metadata saved when there is some,
// else what the kernel's release names, else none.
func (t *Target) provider() hostProvider {
	if t.Azure != nil {
		return t.Azure
	}
	if rel, ok := t.hostFile("proc/sys/kernel/osrelease"); ok && strings.Contains(strings.ToLower(rel), "microsoft") {
		return wsl2{}
	}
	return nil
}

// CPUTime is the difference between two readings of /proc/stat's first
// line: the time every CPU spent, by kind, in clock ticks.
type CPUTime struct {
	BeforePath, AfterPath string
	Total, Steal          int64
}

// cpuLine reads /proc/stat's "cpu" line: user nice system idle iowait irq
// softirq steal, then the guest times, which user already counts.
func cpuLine(text string) (total, steal int64, err error) {
	for line := range strings.SplitSeq(text, "\n") {
		f := strings.Fields(line)
		if len(f) == 0 || f[0] != "cpu" {
			continue
		}
		if len(f) < 9 {
			return 0, 0, fmt.Errorf("a cpu line of %d fields, the steal time is the eighth value", len(f)-1)
		}
		for i := 1; i <= 8; i++ {
			n, err := strconv.ParseInt(f[i], 10, 64)
			if err != nil {
				return 0, 0, fmt.Errorf("cpu field %d: %q", i, f[i])
			}
			total += n
		}
		steal, _ = strconv.ParseInt(f[8], 10, 64)
		return total, steal, nil
	}
	return 0, 0, errors.New("no cpu line: want a copy of /proc/stat")
}

func openCPUTime(before, after string) (*CPUTime, error) {
	if before == "" || after == "" {
		return nil, errors.New("/proc/stat is read twice, before and after a load: name both files")
	}
	var totals, steals [2]int64
	for i, path := range []string{before, after} {
		b, err := os.ReadFile(path) //nolint:gosec // G304: the path is the caller's
		if err != nil {
			return nil, err
		}
		if totals[i], steals[i], err = cpuLine(string(b)); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
	}
	if totals[1] <= totals[0] {
		return nil, fmt.Errorf("%s was not read after %s: the CPU time did not grow", after, before)
	}
	return &CPUTime{BeforePath: before, AfterPath: after, Total: totals[1] - totals[0], Steal: steals[1] - steals[0]}, nil
}

// topology counts the host's logical CPUs, its cores and its sockets from
// /sys/devices/system/cpu/cpu*/topology, false when they are not there.
func (t *Target) topology() (cpus, cores, sockets int, ok bool) {
	if t.Root == "" {
		return 0, 0, 0, false
	}
	dirs, err := filepath.Glob(filepath.Join(t.Root, "sys", "devices", "system", "cpu", "cpu[0-9]*"))
	if err != nil || len(dirs) == 0 {
		return 0, 0, 0, false
	}
	coreSet, socketSet := map[string]bool{}, map[string]bool{}
	for _, d := range dirs {
		rel, _ := filepath.Rel(t.Root, d)
		pkg, ok1 := t.hostFile(filepath.Join(rel, "topology", "physical_package_id"))
		core, ok2 := t.hostFile(filepath.Join(rel, "topology", "core_id"))
		if !ok1 || !ok2 {
			continue
		}
		cpus++
		coreSet[pkg+"/"+core], socketSet[pkg] = true, true
	}
	return cpus, len(coreSet), len(socketSet), cpus > 0
}

// stealBound is the share of CPU time taken by the hypervisor past which a
// measurement on this machine measures its neighbours too.
const stealBound = 2.0

// hostSystem returns the host's lines of the system table.
func hostSystem(t *Target) []Setting {
	var out []Setting
	if p := t.provider(); p != nil {
		out = append(out, Setting{"host provider", p.Name()})
		out = append(out, p.Settings()...)
	}
	if smt, ok := t.hostFile("sys/devices/system/cpu/smt/active"); ok {
		out = append(out, Setting{"host smt active", smt})
	}
	if cpus, cores, sockets, ok := t.topology(); ok {
		out = append(out, Setting{"host cpus", strconv.Itoa(cpus)},
			Setting{"host cores", strconv.Itoa(cores)}, Setting{"host sockets", strconv.Itoa(sockets)})
	}
	if c := t.CPUTime; c != nil {
		out = append(out, Setting{"host steal", fmt.Sprintf("%.1f%%", c.stealShare())})
	}
	return out
}

func (c *CPUTime) stealShare() float64 { return 100 * float64(c.Steal) / float64(c.Total) }

// init registers the host rules, in this order: the time the hypervisor
// took during a load, then the cores shared by two hardware threads.
func init() {
	Register(Rule{
		Name:  "host/steal",
		Needs: Selection{Components: {"host"}, Goals: {"performance"}, Sources: {"logs"}},
		Doc:   "https://man7.org/linux/man-pages/man5/proc_stat.5.html",
		Check: func(t *Target, _ Answers) []Finding {
			c := t.CPUTime
			if c == nil || c.stealShare() < stealBound {
				return nil
			}
			return []Finding{{
				Current:  fmt.Sprintf("%.1f%% of the CPU time stolen by the hypervisor between %s and %s", c.stealShare(), c.BeforePath, c.AfterPath),
				Proposed: fmt.Sprintf("under %.0f%%: a size with dedicated vCPUs, or the step run again at another time", stealBound),
				Why: "steal is time the program was ready to run and the hypervisor gave to another " +
					"machine: latencies grow and throughput drops for a cause outside the machine, " +
					"and a figure measured under it measures the neighbours too",
			}}
		},
	})

	Register(Rule{
		Name:  "host/smt",
		Needs: Selection{Components: {"host"}, Goals: {"performance"}, Sources: {"config"}, Profile: {"comparative"}},
		Doc:   "https://docs.kernel.org/admin-guide/hw-vuln/core-scheduling.html",
		Check: func(t *Target, _ Answers) []Finding {
			if smt, ok := t.hostFile("sys/devices/system/cpu/smt/active"); !ok || smt != "1" {
				return nil
			}
			current := "SMT active: two hardware threads share each core"
			if cpus, cores, _, ok := t.topology(); ok {
				current += fmt.Sprintf(" (%d CPUs on %d cores)", cpus, cores)
			}
			return []Finding{{
				Current:  current,
				Proposed: "the same SMT setting on every machine of the comparison, published; processes pinned to whole cores where a figure depends on it",
				Why: "two threads on one core share its execution units and caches: a server and a load " +
					"generator, or two workers, paired on a core run slower than on two, and which " +
					"pairs the scheduler makes changes from one run to the next",
			}}
		},
	})
}
