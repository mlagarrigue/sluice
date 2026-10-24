package audit

import (
	"debug/buildinfo"
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
