package sluice_test

// ADR 0001 places each package by what it retains between two batches, and
// the layers that follow from it are a rule the compiler checks only in
// part: a root that imported join would be a cycle and fail to build, but a
// root, a join or a distinct importing diagnostics, or web importing
// httpstream, compiles fine and undoes the map. This test holds every
// package of the module to the imports of the module it is allowed, and
// names the package that strays.

import (
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
)

const modPath = "github.com/mlagarrigue/sluice"

// allowed lists, for each package of the module, the packages of the module
// its non-test files may import. A package absent from the table may import
// nothing of the module.
var allowed = map[string][]string{
	"":                         {"internal/batch"},
	"internal/batch":           nil,
	"diagnostics":              nil,
	"join":                     {""},
	"distinct":                 {""},
	"gateway":                  {""},
	"probe":                    {""},
	"pushdown":                 {""},
	"internal/handoff":         {"", "internal/batch"},
	"window":                   {"", "internal/batch", "internal/handoff"},
	"parallel":                 {"", "internal/batch", "internal/handoff"},
	"web":                      {"", "diagnostics", "web/internal/reqbody"},
	"web/internal/reqbody":     {"diagnostics"},
	"web/stream":               {"", "diagnostics", "net/httpstream", "web", "web/internal/reqbody"},
	"net/httpstream":           {"", "internal/ipkey", "internal/varint", "net/quic"},
	"net/quic":                 {"internal/ipkey", "internal/varint", "net/quic/udp"},
	"net/quic/udp":             nil,
	"internal/ipkey":           nil,
	"internal/varint":          nil,
	"internal/pgwire":          nil,
	"database/postgres":        {"", "database/postgres/pgtype", "internal/pgwire", "pushdown"},
	"database/postgres/pgtype": nil,
}

func TestImportLayers(t *testing.T) {
	out, err := exec.Command("go", "list", "-f", "{{.ImportPath}} {{join .Imports \" \"}}", "./...").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for line := range strings.Lines(string(out)) {
		f := strings.Fields(line)
		pkg := strings.TrimPrefix(strings.TrimPrefix(f[0], modPath), "/")
		if strings.HasPrefix(pkg, "example/") || strings.HasPrefix(pkg, "cmd/") ||
			strings.HasPrefix(pkg, "internal/bench") || strings.HasPrefix(pkg, "internal/streamtest") ||
			strings.HasPrefix(pkg, "internal/dgram") || strings.HasPrefix(pkg, "net/httpstream/internal/") ||
			strings.HasPrefix(pkg, "database/postgres/internal/") {
			continue
		}
		ok := allowed[pkg]
		for _, imp := range f[1:] {
			if imp != modPath && !strings.HasPrefix(imp, modPath+"/") {
				continue
			}
			rel := strings.TrimPrefix(strings.TrimPrefix(imp, modPath), "/")
			if !slices.Contains(ok, rel) {
				name := "package " + strconv.Quote(pkg)
				if pkg == "" {
					name = "the root package"
				}
				t.Errorf("%s imports %q: not in the layer table (ADR 0001)", name, rel)
			}
		}
	}
}
