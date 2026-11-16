package audit

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

// The licence of every module a binary links, read from the module cache
// where the go command unpacked it: the LICENSE (or LICENCE, COPYING) file
// at the module's root, identified by its text and named by its SPDX
// identifier. The identification looks for the phrases each licence's
// text is built on, not for a whole text: a licence file carries its
// copyright line and its formatting, and a match on the clauses that make
// it that licence holds through both. A text matching none is "unknown",
// never guessed.

// licenseFile is a file that carries a module's licence.
var licenseFile = regexp.MustCompile(`(?i)^(licen[cs]e|copying)([-._].*)?$`)

// licenseSigns are the phrases each licence's text carries, all of them,
// checked in this order: the GNU family before the permissive ones, whose
// phrases the GPL's own text does not contain, and BSD-3 before BSD-2,
// whose clauses it repeats.
var licenseSigns = []struct {
	id    string
	signs []string
}{
	{"AGPL-3.0", []string{"gnu affero general public license", "version 3"}},
	{"LGPL-3.0", []string{"gnu lesser general public license", "version 3"}},
	{"LGPL-2.1", []string{"gnu lesser general public license", "version 2.1"}},
	{"GPL-3.0", []string{"gnu general public license", "version 3"}},
	{"GPL-2.0", []string{"gnu general public license", "version 2"}},
	{"MPL-2.0", []string{"mozilla public license", "version 2.0"}},
	{"Apache-2.0", []string{"apache license", "version 2.0"}},
	{"MIT", []string{"permission is hereby granted, free of charge", `the software is provided "as is"`}},
	{"ISC", []string{"permission to use, copy, modify, and/or distribute this software for any purpose"}},
	{"BSD-3-Clause", []string{"redistribution and use in source and binary forms", "neither the name"}},
	{"BSD-2-Clause", []string{"redistribution and use in source and binary forms"}},
	{"Unlicense", []string{"this is free and unencumbered software released into the public domain"}},
}

// identifyLicense names a licence text by its SPDX identifier, "" when it
// matches none: case and line breaks are not the licence's.
func identifyLicense(text string) string {
	t := strings.Join(strings.Fields(strings.ToLower(text)), " ")
	for _, l := range licenseSigns {
		if !slices.ContainsFunc(l.signs, func(s string) bool { return !strings.Contains(t, s) }) {
			return l.id
		}
	}
	return ""
}

// escapeModulePath is the module cache's spelling of a path or a version:
// every capital letter as '!' and its lower case, so that the cache holds
// on a filesystem that ignores case.
func escapeModulePath(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= 'A' && r <= 'Z' {
			b.WriteByte('!')
			r += 'a' - 'A'
		}
		b.WriteRune(r)
	}
	return b.String()
}

// moduleLicenses reads the licences of a module version in the cache:
// one identifier a licence file, sorted, "unknown" for a file no
// identifier fits. Nil when the module is not in the cache or carries no
// licence file.
func moduleLicenses(cache, path, version string) []string {
	if cache == "" || version == "" {
		return nil
	}
	dir := filepath.Join(cache, filepath.FromSlash(escapeModulePath(path))+"@"+escapeModulePath(version))
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var ids []string
	for _, e := range entries {
		if e.IsDir() || !licenseFile.MatchString(e.Name()) {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name())) //nolint:gosec // G304: a file of the module cache the caller named
		if err != nil {
			continue
		}
		id := identifyLicense(string(b))
		if id == "" {
			id = "unknown"
		}
		if !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

// goLicense is the standard library's: Go's own LICENSE, a BSD
// three-clause text (https://go.dev/LICENSE).
const goLicense = "BSD-3-Clause"

// licensesOf fills the bill of materials' licences from the target's
// module cache.
func licensesOf(t *Target, modules []Module) {
	for i := range modules {
		m := &modules[i]
		switch {
		case m.Path == "stdlib":
			m.License = goLicense
		case !m.Main:
			m.License = strings.Join(moduleLicenses(t.ModCache, m.Path, m.Version), " AND ")
		}
	}
}

func init() {
	Register(Rule{
		Name:  "deps/license",
		Needs: Selection{Components: {"go"}, Goals: {"compliance"}, Sources: {"binary"}},
		Doc:   "https://spdx.org/licenses/",
		Check: func(t *Target, a Answers) []Finding {
			if t.Build == nil {
				return nil
			}
			if t.ModCache == "" {
				return []Finding{{
					Current:  "the licences of the modules " + t.BinaryPath + " links were not read: no module cache was given",
					Proposed: "-modcache $(go env GOMODCACHE), on a machine where go mod download ran for the binary's module",
					Why:      "the audit reads the licence each module ships in the cache; without it, the bill of materials names no licence at all",
				}}
			}
			var admitted []string
			for _, id := range strings.Split(a.Get("licenses"), ",") {
				admitted = append(admitted, strings.TrimSpace(id))
			}
			var outside, unread []string
			for _, m := range SBOM(t) {
				// A module replaced by a directory (replace => ../fork) is
				// sources beside the main module's, not a download: no
				// cache holds it, and its licence is that tree's.
				if m.Main || m.Path == "stdlib" || strings.HasPrefix(m.Path, ".") || filepath.IsAbs(m.Path) {
					continue
				}
				if m.License == "" {
					unread = append(unread, m.Path+"@"+m.Version)
					continue
				}
				ids := strings.Split(m.License, " AND ")
				if slices.ContainsFunc(ids, func(id string) bool { return !slices.Contains(admitted, id) }) {
					outside = append(outside, fmt.Sprintf("%s@%s (%s)", m.Path, m.Version, strings.Join(ids, " AND ")))
				}
			}
			var out []Finding
			if len(outside) > 0 {
				out = append(out, Finding{
					Current: "modules linked under a licence outside " + strings.Join(admitted, ", ") + ": " + strings.Join(outside, "; "),
					Proposed: "replace the module, or admit its licence once its terms are read: " +
						"-answer licenses=" + strings.Join(admitted, ",") + ",<identifier>",
					Why: "a binary that links a module distributes it under that module's terms: a copyleft " +
						"licence (GPL, AGPL) asks for the whole program's source, a weak one (MPL, LGPL) " +
						"for the module's own, and \"unknown\" is a text nobody has read",
				})
			}
			if len(unread) > 0 {
				out = append(out, Finding{
					Current:  "no licence file read in the module cache " + t.ModCache + " for: " + strings.Join(unread, ", "),
					Proposed: "go mod download, then audit again; a module without a licence file grants no right to use it",
					Why: "the audit reads the licence the module itself ships; a module absent from the cache, " +
						"or shipping none, is a licence nobody has seen",
				})
			}
			return out
		},
	})
}
