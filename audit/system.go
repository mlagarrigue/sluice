package audit

import (
	"fmt"
	"strconv"
	"strings"
)

// A Setting is one line of the table a comparison publishes for each of
// its machines: a value read, not a recommendation. Two machines of a
// comparison must show the same table, and a reader must be able to check
// that they did.
type Setting struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// hostSettings are the kernel values the comparison's protocol fixes and
// publishes, in its own order: queues, ports, connection tracking, UDP
// buffers, then memory.
var hostSettings = []struct{ name, path string }{
	{"kernel", "proc/sys/kernel/osrelease"},
	{"net.core.somaxconn", "proc/sys/net/core/somaxconn"},
	{"net.ipv4.tcp_max_syn_backlog", "proc/sys/net/ipv4/tcp_max_syn_backlog"},
	{"net.core.netdev_max_backlog", "proc/sys/net/core/netdev_max_backlog"},
	{"net.ipv4.tcp_abort_on_overflow", "proc/sys/net/ipv4/tcp_abort_on_overflow"},
	{"net.ipv4.ip_local_port_range", "proc/sys/net/ipv4/ip_local_port_range"},
	{"net.ipv4.tcp_tw_reuse", "proc/sys/net/ipv4/tcp_tw_reuse"},
	{"net.netfilter.nf_conntrack_max", "proc/sys/net/netfilter/nf_conntrack_max"},
	{"net.core.rmem_max", "proc/sys/net/core/rmem_max"},
	{"net.core.wmem_max", "proc/sys/net/core/wmem_max"},
	{"transparent_hugepage/enabled", "sys/kernel/mm/transparent_hugepage/enabled"},
	{"transparent_hugepage/defrag", "sys/kernel/mm/transparent_hugepage/defrag"},
}

// System is the table of what a target was set to, as a comparison
// publishes it: the host's kernel values under the root, the named
// process's descriptor limit, the PostgreSQL server's settings, the
// comparison server's flags, and the
// container's limits, network and log driver. A value the host does not have says "absent" — a kernel with no
// connection tracking is a fact to publish, not a gap — and a part nobody
// pointed at has no lines at all.
func System(t *Target) []Setting {
	var out []Setting
	if t.Root != "" {
		for _, s := range hostSettings {
			v, ok := t.hostFile(s.path)
			if !ok {
				v = "absent"
			}
			out = append(out, Setting{s.name, strings.Join(strings.Fields(v), " ")})
		}
		if t.PID != 0 {
			v := "unread"
			if text, ok := t.hostFile("proc/" + strconv.Itoa(t.PID) + "/limits"); ok {
				for line := range strings.SplitSeq(text, "\n") {
					if f := strings.Fields(strings.TrimPrefix(line, "Max open files")); strings.HasPrefix(line, "Max open files") && len(f) >= 2 {
						v = f[0] + ":" + f[1]
					}
				}
			}
			out = append(out, Setting{"process " + strconv.Itoa(t.PID) + " nofile", v})
		}
	}
	if pg := t.Postgres; pg != nil && pg.Settings != nil {
		for _, name := range postgresSettings {
			v := "absent"
			if s, ok := pg.setting(name); ok {
				v = strings.TrimSpace(s.Value + " " + s.Unit)
			}
			out = append(out, Setting{"postgres " + name, v})
		}
	}
	if t.App != nil {
		out = append(out, appSystem(t.App)...)
	}
	out = append(out, hostSystem(t)...)
	if c := t.Container; c != nil {
		hc := c.HostConfig
		nofile := "daemon default"
		if soft, hard, ok := c.ulimit("nofile"); ok {
			nofile = fmt.Sprintf("%d:%d", soft, hard)
		}
		orNone := func(v string) string {
			if v == "" {
				return "none"
			}
			return v
		}
		cpus, memory := "none", "none"
		if hc.NanoCpus != 0 {
			cpus = strconv.FormatFloat(float64(hc.NanoCpus)/1e9, 'f', -1, 64)
		}
		if hc.Memory != 0 {
			memory = strconv.FormatInt(hc.Memory, 10) + " bytes"
		}
		logDriver := []string{hc.LogConfig.Type}
		for _, k := range []string{"max-size", "max-file", "mode"} {
			if v := hc.LogConfig.Config[k]; v != "" {
				logDriver = append(logDriver, k+"="+v)
			}
		}
		out = append(out,
			Setting{"container image", c.Config.Image},
			Setting{"container nofile", nofile},
			Setting{"container cpuset", orNone(hc.CpusetCpus)},
			Setting{"container cpus", cpus},
			Setting{"container memory", memory},
			Setting{"container network", hc.NetworkMode},
			Setting{"container log driver", strings.Join(logDriver, " ")},
		)
	}
	return out
}
