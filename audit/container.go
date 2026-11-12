package audit

import (
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"slices"
	"strconv"
	"strings"
)

// What a running container was started with, as docker inspect reports it.
// The Dockerfile says what an image meant to be; the container is what was
// actually run, with the flags of a docker run nobody wrote down — a
// --privileged added to debug once, a socket mounted for a sidecar. Both
// are read, by different rules, because they disagree exactly where an
// audit is worth having.

// A Container is the part of docker inspect's record the rules read. The
// field names are Docker's, so that a reader can check one against the
// other.
type Container struct {
	Name            string `json:"Name"`
	AppArmorProfile string `json:"AppArmorProfile"`
	Config          struct {
		User  string   `json:"User"`
		Env   []string `json:"Env"`
		Image string   `json:"Image"`
	} `json:"Config"`
	HostConfig struct {
		Privileged     bool     `json:"Privileged"`
		CapAdd         []string `json:"CapAdd"`
		CapDrop        []string `json:"CapDrop"`
		SecurityOpt    []string `json:"SecurityOpt"`
		ReadonlyRootfs bool     `json:"ReadonlyRootfs"`
		NetworkMode    string   `json:"NetworkMode"`
		CpusetCpus     string   `json:"CpusetCpus"`
		NanoCpus       int64    `json:"NanoCpus"`
		Memory         int64    `json:"Memory"`
		Ulimits        []struct {
			Name string `json:"Name"`
			Soft int64  `json:"Soft"`
			Hard int64  `json:"Hard"`
		} `json:"Ulimits"`
		LogConfig struct {
			Type   string            `json:"Type"`
			Config map[string]string `json:"Config"`
		} `json:"LogConfig"`
	} `json:"HostConfig"`
	Mounts []struct {
		Type        string `json:"Type"`
		Source      string `json:"Source"`
		Destination string `json:"Destination"`
	} `json:"Mounts"`
}

// parseContainer reads what docker inspect printed: an array, which must
// hold exactly one container — a report that silently read the first of
// several would audit a container nobody chose.
func parseContainer(b []byte) (*Container, error) {
	var cs []Container
	if err := json.Unmarshal(b, &cs); err != nil {
		return nil, fmt.Errorf("not the output of docker inspect: %w", err)
	}
	if len(cs) != 1 {
		return nil, fmt.Errorf("holds %d containers, want one", len(cs))
	}
	return &cs[0], nil
}

// dockerInspect calls the docker client. A missing client is an error, not
// a silence: a container was named, and a report that did not read it must
// not look like one that found it well set.
func dockerInspect(name string) ([]byte, error) {
	bin, err := exec.LookPath("docker")
	if err != nil {
		return nil, fmt.Errorf("docker is not installed, or not on PATH; save docker inspect's output elsewhere and name the file instead: %w", err)
	}
	out, err := exec.Command(bin, "inspect", "--type", "container", "--", name).Output() //nolint:gosec // G204: the binary is looked up, the name is one argument
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return nil, fmt.Errorf("docker inspect %s: %w: %s", name, err, strings.TrimSpace(string(ee.Stderr)))
		}
		return nil, fmt.Errorf("docker inspect %s: %w", name, err)
	}
	return out, nil
}

// ulimit returns a container's own limit by name, and whether one was set:
// unset, the container inherits the daemon's default, which is not a
// value the record holds.
func (c *Container) ulimit(name string) (soft, hard int64, ok bool) {
	for _, u := range c.HostConfig.Ulimits {
		if u.Name == name {
			return u.Soft, u.Hard, true
		}
	}
	return 0, 0, false
}

// securityOpt reports whether a --security-opt names key with value. The
// daemon has accepted both key=value and the older key:value, and stores
// what it was given.
func (c *Container) securityOpt(key, value string) bool {
	for _, o := range c.HostConfig.SecurityOpt {
		k, v, ok := strings.Cut(o, "=")
		if !ok {
			k, v, ok = strings.Cut(o, ":")
		}
		if !ok {
			k, v = o, "true" // "no-new-privileges" alone means it is on
		}
		if strings.EqualFold(k, key) && strings.EqualFold(v, value) {
			return true
		}
	}
	return false
}

// isRoot reports whether a docker user field names uid 0: empty is the
// image's default, which is root unless the image said otherwise — and the
// record would then hold the image's USER.
func isRoot(user string) bool {
	name, _, _ := strings.Cut(user, ":")
	return name == "" || name == "root" || name == "0"
}

func hasFold(list []string, want string) bool {
	return slices.ContainsFunc(list, func(s string) bool { return strings.EqualFold(s, want) })
}

// init registers the container rules, in this order: the limits (nofile,
// cpuset, memory), the network, the log driver, then the confinement
// (capabilities, no-new-privileges, seccomp, AppArmor, read-only root,
// user, the Docker socket), and last go/godebug, read in the container's
// environment and the process's. Each is one call, read on its own.
func init() {
	needs := func(goal string, profiles ...string) Selection {
		s := Selection{Components: {"docker"}, Goals: {goal}, Sources: {"config"}}
		if len(profiles) > 0 {
			s[Profile] = profiles
		}
		return s
	}
	register := func(name, goal, doc string, check func(*Container, Answers) []Finding, profiles ...string) {
		Register(Rule{
			Name: "container/" + name, Needs: needs(goal, profiles...), Doc: doc,
			Check: func(t *Target, a Answers) []Finding {
				if t.Container == nil {
					return nil
				}
				return check(t.Container, a)
			},
		})
	}

	register("nofile", "reliability", "https://docs.docker.com/reference/cli/docker/container/run/#ulimit",
		func(c *Container, _ Answers) []Finding {
			soft, hard, ok := c.ulimit("nofile")
			if !ok {
				return []Finding{{
					Current:  "no --ulimit nofile: the daemon's default applies, whose soft limit may be 1024",
					Proposed: "--ulimit nofile=65536:65536",
					Why: "a connection is a file, so this is the ceiling on connections the server " +
						"can hold. Go raises its soft limit to the hard one by itself (since Go " +
						"1.19); most other runtimes do not, and accept then fails with EMFILE at " +
						"the 1025th connection while the server stays up. Unset, the value is the " +
						"daemon's and moves with its version, so two hosts may differ unseen",
				}}
			}
			if soft >= 65536 {
				return nil
			}
			return []Finding{{
				Current:  fmt.Sprintf("--ulimit nofile=%d:%d", soft, hard),
				Proposed: "--ulimit nofile=65536:65536",
				Why: "a connection is a file: past this many, accept fails with EMFILE and the " +
					"server stops answering while staying up, which reads as a hang, not a limit",
			}}
		})

	register("cpuset", "performance", "https://docs.docker.com/engine/containers/resource_constraints/#cpu",
		func(c *Container, _ Answers) []Finding {
			if c.HostConfig.CpusetCpus != "" {
				return nil
			}
			return []Finding{{
				Current:  "no --cpuset-cpus: the server runs on whichever cores the scheduler picks",
				Proposed: "--cpuset-cpus with the same cores for every implementation compared, apart from the database's and the load generator's",
				Why: "a comparison holds the cores fixed or it measures the neighbours: a load " +
					"generator or a database sharing a core with the server takes its time and " +
					"its caches, and the share changes from one run to the next. A CPU quota " +
					"(--cpus) bounds the time, not the cores, and throttles in bursts",
			}}
		}, "comparative")

	register("memory", "reliability", "https://docs.docker.com/engine/containers/resource_constraints/#memory",
		func(c *Container, _ Answers) []Finding {
			if c.HostConfig.Memory != 0 {
				return nil
			}
			return []Finding{{
				Current:  "no --memory: the container may take all the host's memory",
				Proposed: "--memory with a bound the server stays under at its peak, and for a Go server GOMEMLIMIT a little below it",
				Why: "unbounded, a leak or a burst grows until the kernel's OOM killer picks a " +
					"process on the host — maybe another one. Bounded, it is this container that " +
					"stops, and docker inspect says OOMKilled; with GOMEMLIMIT under the bound, " +
					"the garbage collector works harder before that happens",
			}}
		})

	register("network", "performance", "https://docs.docker.com/engine/network/drivers/host/",
		func(c *Container, _ Answers) []Finding {
			mode := c.HostConfig.NetworkMode
			if mode == "host" || mode == "none" || strings.HasPrefix(mode, "container:") {
				return nil // a shared namespace is its owner's to report
			}
			return []Finding{{
				Current:  "--network " + mode,
				Proposed: "--network host on every machine of the comparison",
				Why: "on a bridge every packet crosses a veth pair and the NAT, and every " +
					"connection takes a slot in the connection tracking table. That cost grows " +
					"with the load, so it is charged to whichever implementation is under the " +
					"most of it — the measured difference then includes the network's",
			}}
		}, "comparative")

	register("log-driver", "reliability", "https://docs.docker.com/engine/logging/drivers/json-file/",
		func(c *Container, _ Answers) []Finding {
			lc := c.HostConfig.LogConfig
			if lc.Type != "json-file" || lc.Config["max-size"] != "" {
				return nil
			}
			return []Finding{{
				Current:  "log driver json-file with no max-size",
				Proposed: "--log-driver local, or --log-opt max-size=10m --log-opt max-file=3",
				Why: "json-file keeps every line forever by default: a server logging each " +
					"request fills the disk under load, and then the database or the server " +
					"itself fails on a write that has nothing to do with logs. The local driver " +
					"rotates by default",
			}}
		})

	register("capabilities", "security", "https://docs.docker.com/engine/containers/run/#runtime-privilege-and-linux-capabilities",
		func(c *Container, _ Answers) []Finding {
			hc := c.HostConfig
			var out []Finding
			if hc.Privileged {
				out = append(out, Finding{
					Current:  "--privileged",
					Proposed: "no --privileged; --cap-add the one capability needed, if any",
					Why: "a privileged container has every capability, every device of the host and " +
						"no seccomp or AppArmor profile: it is root on the host by another name, " +
						"and one flaw in the program is one on the machine",
				})
			}
			if hasFold(hc.CapAdd, "ALL") || hasFold(hc.CapAdd, "CAP_ALL") {
				out = append(out, Finding{
					Current:  "--cap-add ALL",
					Proposed: "--cap-drop ALL, then --cap-add the capability the program needs (NET_BIND_SERVICE for a port under 1024)",
					Why: "every capability is a kernel check switched off — SYS_ADMIN alone mounts " +
						"filesystems and escapes most confinement. A server needs none of them to " +
						"serve, and NET_BIND_SERVICE at most",
				})
			}
			if !hc.Privileged && !hasFold(hc.CapDrop, "ALL") && !hasFold(hc.CapDrop, "CAP_ALL") {
				out = append(out, Finding{
					Current:  "the default capabilities are kept (no --cap-drop ALL)",
					Proposed: "--cap-drop ALL, and --cap-add only what the program needs",
					Why: "the default set includes CHOWN, DAC_OVERRIDE, SETUID, NET_RAW and others " +
						"a server never calls; each is a step an attacker inside the container " +
						"does not have to find",
				})
			}
			return out
		})

	register("no-new-privileges", "security", "https://docs.docker.com/reference/cli/docker/container/run/#security-opt",
		func(c *Container, _ Answers) []Finding {
			if c.securityOpt("no-new-privileges", "true") {
				return nil
			}
			return []Finding{{
				Current:  "no --security-opt no-new-privileges",
				Proposed: "--security-opt no-new-privileges",
				Why: "without it, a setuid binary inside the image raises its process to its " +
					"owner, root most often: the user the container runs as is then a starting " +
					"point, not a ceiling",
			}}
		})

	register("seccomp", "security", "https://docs.docker.com/engine/security/seccomp/",
		func(c *Container, _ Answers) []Finding {
			if !c.securityOpt("seccomp", "unconfined") {
				return nil
			}
			return []Finding{{
				Current:  "--security-opt seccomp=unconfined",
				Proposed: "the default seccomp profile (no seccomp option), or a profile of your own",
				Why: "the default profile refuses the system calls a container never needs — " +
					"keyctl, mount, the kernel module ones — which is where most container " +
					"escapes start. Unconfined, the whole kernel interface is reachable",
			}}
		})

	register("apparmor", "security", "https://docs.docker.com/engine/security/apparmor/",
		func(c *Container, _ Answers) []Finding {
			if c.AppArmorProfile != "unconfined" && !c.securityOpt("apparmor", "unconfined") {
				return nil // empty is a host without AppArmor, not a choice
			}
			return []Finding{{
				Current:  "AppArmor profile unconfined",
				Proposed: "the default docker-default profile (no apparmor option)",
				Why: "on a host that has AppArmor, the default profile keeps the container off " +
					"/proc and /sys writes and mounts; unconfined, those are one capability away",
			}}
		})

	register("read-only", "security", "https://docs.docker.com/reference/cli/docker/container/run/#read-only",
		func(c *Container, _ Answers) []Finding {
			if c.HostConfig.ReadonlyRootfs {
				return nil
			}
			return []Finding{{
				Current:  "the root filesystem is writable",
				Proposed: "--read-only, with --tmpfs /tmp if the program writes temporary files",
				Why: "a server writes nothing to its own image; a writable root lets whoever " +
					"gets in drop a binary and run it. Read-only also makes a write the program " +
					"was not meant to do fail loudly instead of filling a layer",
			}}
		})

	register("user", "security", "https://docs.docker.com/reference/cli/docker/container/run/#user",
		func(c *Container, _ Answers) []Finding {
			if !isRoot(c.Config.User) {
				return nil
			}
			current := "runs as root (no USER in the image, no --user)"
			if c.Config.User != "" {
				current = "runs as " + c.Config.User
			}
			return []Finding{{
				Current:  current,
				Proposed: "--user 65532:65532, or a USER in the image",
				Why: "root in the container is uid 0 on the host: an escape, a mounted volume or " +
					"a shared namespace then has the host's root. The Dockerfile rule reads what " +
					"the image meant; this one reads what was run, --user included",
			}}
		})

	register("docker-socket", "security", "https://docs.docker.com/engine/security/#docker-daemon-attack-surface",
		func(c *Container, _ Answers) []Finding {
			var out []Finding
			for _, m := range c.Mounts {
				if !strings.HasSuffix(m.Source, "/docker.sock") && !strings.HasSuffix(m.Destination, "/docker.sock") {
					continue
				}
				out = append(out, Finding{
					Current:  "the Docker socket is mounted at " + m.Destination,
					Proposed: "no socket in the container; a proxy that allows only the calls needed, if something must reach the daemon",
					Why: "the socket is the daemon's API: whoever reaches it starts a privileged " +
						"container with the host's root mounted, so the container's own " +
						"confinement no longer counts",
				})
			}
			return out
		})

	Register(Rule{
		Name:  "go/godebug",
		Needs: Selection{Components: {"go"}, Goals: {"security"}, Sources: {"config"}},
		Doc:   "https://go.dev/doc/godebug",
		Check: func(t *Target, _ Answers) []Finding {
			var out []Finding
			for _, env := range goDebugs(t) {
				out = append(out, Finding{
					Current:  env.where + " sets GODEBUG=" + env.value,
					Proposed: "no GODEBUG, or each key kept with the reason it is still needed and the Go release that removes it",
					Why: "each GODEBUG key restores a behaviour Go changed, and many of those " +
						"changes were for security — x509sha1, tlsrsakex, tls10server, " +
						"x509negativeserial bring back what the standard library refuses by " +
						"default. A key set once to get past an upgrade outlives its reason " +
						"unseen; the binary's go directive sets the defaults it should run with",
				})
			}
			return out
		},
	})
}

type goDebug struct{ where, value string }

// goDebugs returns the GODEBUG values the target sets: in the container's
// environment, and in the named process's under the root.
func goDebugs(t *Target) []goDebug {
	var out []goDebug
	if t.Container != nil {
		for _, e := range t.Container.Config.Env {
			if v, ok := strings.CutPrefix(e, "GODEBUG="); ok && v != "" {
				out = append(out, goDebug{"the container", v})
			}
		}
	}
	if t.Root != "" && t.PID != 0 {
		if text, ok := t.hostFile("proc/" + strconv.Itoa(t.PID) + "/environ"); ok {
			for e := range strings.SplitSeq(text, "\x00") {
				if v, ok := strings.CutPrefix(e, "GODEBUG="); ok && v != "" {
					out = append(out, goDebug{"process " + strconv.Itoa(t.PID), v})
				}
			}
		}
	}
	return out
}
