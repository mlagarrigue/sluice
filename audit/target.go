package audit

import (
	"debug/buildinfo"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
)

// A Target is what an audit was pointed at. One run may hold several: a
// binary, the module it was built from, the Dockerfile that packages it.
// A field left empty is a thing nobody pointed at, and the rules that
// read it stay silent — which is not the same as finding nothing, and the
// report says which.
type Target struct {
	// BinaryPath is the binary read, empty when none was given.
	BinaryPath string

	// Build is what the toolchain recorded in it, nil when no binary was
	// given. A file that is not a Go binary is an error at open time, not
	// a nil here: an audit that silently found nothing is a false green.
	Build *debug.BuildInfo

	// ModuleDir is a directory holding a go.mod, empty when none was
	// given.
	ModuleDir string

	// DockerfilePath is the Dockerfile read as text, empty when none was
	// given. The text is what a build was told to do; reading the image
	// that came out of it is another source and another rule.
	DockerfilePath string
	Dockerfile     string

	// Root is where the host's own files are read from, "/" on the machine
	// being audited. A test gives it a directory it seeded: a tool that
	// can only be tried against the machine running it cannot be tried at
	// all, and the one value that matters — a setting too low — is one
	// nobody wants to set on their own machine to find out.
	Root string

	// PID is the process whose own limits and environment are read, zero
	// when none was named. Its limits are not the host's: a server started
	// by a unit file with its own LimitNOFILE answers for itself.
	PID int

	// Container is a running container as docker inspect reports it, nil
	// when none was named. ContainerName is what the caller named it by,
	// or the file its record was read from.
	Container     *Container
	ContainerName string

	// KernelLog is the kernel's log as text — what dmesg printed, or a
	// kern.log — empty when none was given. It is read from a file the
	// caller saved rather than from the ring buffer: reading that takes a
	// privilege the audit must not need, and a file can be seeded.
	KernelLogPath string
	KernelLog     string

	// AppLog is the application's own log as text — what the server wrote
	// to its standard error or its log file during the load — empty when
	// none was given.
	AppLogPath string
	AppLog     string

	// Postgres is what was read of a PostgreSQL server, nil when nothing
	// of one was named.
	Postgres *Postgres

	// Firewall is the host's packet filter as saved, nil when none was
	// given.
	Firewall *Firewall

	// Proxy is the reverse proxy's configuration, nil when none was given.
	Proxy *Proxy

	// App is the comparison study's server as started, nil when its start
	// was not given.
	App *App

	// VulnDB is a local copy of the Go vulnerability database, absolute,
	// empty for govulncheck's default; VulnDBModified is the date its
	// index/db.json gives.
	VulnDB, VulnDBModified string

	// Counters is what the kernel's network counters moved by during a
	// load, nil when the two readings were not given.
	Counters *Counters

	// Study is what the comparison's server counted during a load, nil
	// when its two metrics documents were not given.
	Study *StudyMetrics

	// TimeWait is the sockets in TIME-WAIT that ss listed, nil when its
	// output was not given.
	TimeWait *Sockets

	// CPUTime is what the CPUs spent during a load, by kind, nil when the
	// two readings of /proc/stat were not given.
	CPUTime *CPUTime

	// Azure is the virtual machine as Azure's metadata service described
	// it, nil when its answer was not given.
	Azure *Azure
}

// A Spec names what to read. Every field is optional, and a Target with
// nothing in it is valid: the rules then have nothing to say, which is a
// result a caller may want to see.
type Spec struct {
	Binary     string
	Module     string
	Dockerfile string

	// Root is the host's filesystem root to read, "/" on the machine
	// itself. Empty means no host is read at all, so the rules that read
	// one stay silent.
	Root string

	// PID names a process whose limits and environment to read. Zero means
	// none.
	PID int

	// Container names a running container, read by calling docker inspect:
	// the client follows DOCKER_HOST, contexts, a rootless daemon and TLS,
	// which a second client written here would have to get right too. The
	// caller allows the tools source before naming one.
	Container string

	// ContainerJSON is a file holding what docker inspect printed for one
	// container: the same reading, with no tool and no daemon.
	ContainerJSON string

	// KernelLog is a file holding the kernel's log.
	KernelLog string

	// AppLog is a file holding the application's log.
	AppLog string

	// PGSettings is a file holding pg_settings as psql prints it in CSV,
	// with at least the name, setting and unit columns. PGHBA is the text
	// of pg_hba.conf; PGLog is the server's log; PGRoles is pg_authid or
	// pg_roles as psql prints it in CSV, with the rolname, rolsuper and
	// rolcanlogin columns.
	PGSettings, PGHBA, PGLog, PGRoles string

	// PGActivity is pg_stat_activity as psql prints it in CSV, read during
	// a load, with the state column at least and backend_type when the
	// server has it; several readings may follow one another, each with
	// its header.
	PGActivity string

	// Firewall is a file holding the packet filter's rules as nft list
	// ruleset or iptables-save prints them.
	Firewall string

	// ProxyConfig is a reverse proxy's configuration file, nginx.conf or
	// haproxy.cfg; ProxyVersion what nginx -v or haproxy -v printed, which
	// the advisories are judged on.
	ProxyConfig, ProxyVersion string

	// AppArgs is a file holding how the comparison study's server was
	// started: its environment as env prints it, NAME=value lines, and its
	// command line.
	AppArgs string

	// NetstatBefore and NetstatAfter are two readings of the kernel's
	// network counters, before and after a load: /proc/net/netstat (with
	// /proc/net/snmp appended or not), or nstat -az's output.
	NetstatBefore, NetstatAfter string

	// VulnDB is a directory holding a copy of the Go vulnerability
	// database (vuln.go.dev/vulndb.zip, unzipped): index/db.json and the
	// rest, read with no network.
	VulnDB string

	// MetricsBefore and MetricsAfter are two answers of the comparison
	// server's GET /study/metrics, at the start and the end of a load.
	MetricsBefore, MetricsAfter string

	// SS is a file holding what ss -tan printed during a load.
	SS string

	// StatBefore and StatAfter are two copies of /proc/stat, before and
	// after a load.
	StatBefore, StatAfter string

	// IMDS is a file holding what Azure's Instance Metadata Service
	// answered at /metadata/instance.
	IMDS string
}

// Open reads what a [Spec] names. Anything named must be readable: a path
// given and not read is an error, never a silence.
func Open(s Spec) (*Target, error) {
	t := &Target{}
	if s.Binary != "" {
		info, err := buildinfo.ReadFile(s.Binary)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", s.Binary, err)
		}
		t.BinaryPath, t.Build = s.Binary, info
	}
	if s.Module != "" {
		if _, err := os.Stat(filepath.Join(s.Module, "go.mod")); err != nil {
			return nil, fmt.Errorf("%s: no go.mod: %w", s.Module, err)
		}
		t.ModuleDir = s.Module
	}
	if s.Dockerfile != "" {
		b, err := os.ReadFile(s.Dockerfile) //nolint:gosec // G304: the path is the caller's
		if err != nil {
			return nil, err
		}
		t.DockerfilePath, t.Dockerfile = s.Dockerfile, string(b)
	}
	if s.Root != "" {
		info, err := os.Stat(s.Root)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("%s: not a directory", s.Root)
		}
		t.Root = s.Root
	}
	t.PID = s.PID
	switch {
	case s.Container != "" && s.ContainerJSON != "":
		return nil, errors.New("name a container or the file its record was saved to, not both")
	case s.Container != "":
		out, err := dockerInspect(s.Container)
		if err != nil {
			return nil, err
		}
		if t.Container, err = parseContainer(out); err != nil {
			return nil, fmt.Errorf("docker inspect %s: %w", s.Container, err)
		}
		t.ContainerName = s.Container
	case s.ContainerJSON != "":
		b, err := os.ReadFile(s.ContainerJSON) //nolint:gosec // G304: the path is the caller's
		if err != nil {
			return nil, err
		}
		if t.Container, err = parseContainer(b); err != nil {
			return nil, fmt.Errorf("%s: %w", s.ContainerJSON, err)
		}
		t.ContainerName = s.ContainerJSON
	}
	if s.KernelLog != "" {
		b, err := os.ReadFile(s.KernelLog) //nolint:gosec // G304: the path is the caller's
		if err != nil {
			return nil, err
		}
		t.KernelLogPath, t.KernelLog = s.KernelLog, string(b)
	}
	if s.AppLog != "" {
		b, err := os.ReadFile(s.AppLog) //nolint:gosec // G304: the path is the caller's
		if err != nil {
			return nil, err
		}
		t.AppLogPath, t.AppLog = s.AppLog, string(b)
	}
	if s.PGSettings != "" || s.PGHBA != "" || s.PGLog != "" || s.PGRoles != "" || s.PGActivity != "" {
		pg, err := openPostgres(s.PGSettings, s.PGHBA, s.PGLog, s.PGRoles, s.PGActivity)
		if err != nil {
			return nil, err
		}
		t.Postgres = pg
	}
	if s.Firewall != "" {
		b, err := os.ReadFile(s.Firewall) //nolint:gosec // G304: the path is the caller's
		if err != nil {
			return nil, err
		}
		if t.Firewall, err = parseFirewall(string(b)); err != nil {
			return nil, fmt.Errorf("%s: %w", s.Firewall, err)
		}
		t.Firewall.Path = s.Firewall
	}
	if s.ProxyConfig != "" {
		b, err := os.ReadFile(s.ProxyConfig) //nolint:gosec // G304: the path is the caller's
		if err != nil {
			return nil, err
		}
		if t.Proxy, err = parseProxy(string(b), s.ProxyVersion); err != nil {
			return nil, fmt.Errorf("%s: %w", s.ProxyConfig, err)
		}
		t.Proxy.Path = s.ProxyConfig
	}
	if s.AppArgs != "" {
		b, err := os.ReadFile(s.AppArgs) //nolint:gosec // G304: the path is the caller's
		if err != nil {
			return nil, err
		}
		if t.App, err = parseApp(string(b)); err != nil {
			return nil, fmt.Errorf("%s: %w", s.AppArgs, err)
		}
		t.App.Path = s.AppArgs
	}
	if s.NetstatBefore != "" || s.NetstatAfter != "" {
		c, err := openCounters(s.NetstatBefore, s.NetstatAfter)
		if err != nil {
			return nil, err
		}
		t.Counters = c
	}
	if s.VulnDB != "" {
		dir, modified, err := openVulnDB(s.VulnDB)
		if err != nil {
			return nil, err
		}
		t.VulnDB, t.VulnDBModified = dir, modified
	}
	if s.MetricsBefore != "" || s.MetricsAfter != "" {
		m, err := openStudyMetrics(s.MetricsBefore, s.MetricsAfter)
		if err != nil {
			return nil, err
		}
		t.Study = m
	}
	if s.SS != "" {
		ss, err := openSockets(s.SS)
		if err != nil {
			return nil, err
		}
		t.TimeWait = ss
	}
	if s.StatBefore != "" || s.StatAfter != "" {
		c, err := openCPUTime(s.StatBefore, s.StatAfter)
		if err != nil {
			return nil, err
		}
		t.CPUTime = c
	}
	if s.IMDS != "" {
		a, err := openAzure(s.IMDS)
		if err != nil {
			return nil, err
		}
		t.Azure = a
	}
	return t, nil
}

// hostFile reads a file of the host under [Target.Root], and says whether
// it was there. A file absent is not a setting at zero: a kernel without
// a module has no knob for it, and a rule must stay silent rather than
// report a bound nobody set.
func (t *Target) hostFile(rel string) (string, bool) {
	if t.Root == "" {
		return "", false
	}
	b, err := os.ReadFile(filepath.Join(t.Root, rel)) //nolint:gosec // G304: under the root the caller named
	if err != nil {
		return "", false
	}
	return strings.TrimSpace(string(b)), true
}

// hostInt reads a single number of the host.
func (t *Target) hostInt(rel string) (int64, bool) {
	text, ok := t.hostFile(rel)
	if !ok {
		return 0, false
	}
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return 0, false // a knob that exists and holds nothing says nothing
	}
	n, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// setting returns a build setting and whether the toolchain recorded it.
func (t *Target) setting(key string) (string, bool) {
	if t.Build == nil {
		return "", false
	}
	for _, s := range t.Build.Settings {
		if s.Key == key {
			return s.Value, true
		}
	}
	return "", false
}

// A Module is one line of the bill of materials: what the binary actually
// links, as the toolchain recorded it, rather than what a manifest meant
// to require.
type Module struct {
	Path    string `json:"path"`
	Version string `json:"version"`
	Sum     string `json:"sum,omitempty"`
	Main    bool   `json:"main,omitempty"`
}

// SBOM is the binary's bill of materials: the main module, the toolchain,
// and every dependency linked into it, replacements followed. It is a
// reading, not a recommendation, so it carries no finding and no verdict.
func SBOM(t *Target) []Module {
	if t.Build == nil {
		return nil
	}
	out := []Module{
		{Path: t.Build.Main.Path, Version: t.Build.Main.Version, Sum: t.Build.Main.Sum, Main: true},
		{Path: "stdlib", Version: t.Build.GoVersion},
	}
	for _, d := range t.Build.Deps {
		m := d
		if m.Replace != nil {
			m = m.Replace
		}
		out = append(out, Module{Path: m.Path, Version: m.Version, Sum: m.Sum})
	}
	return out
}
