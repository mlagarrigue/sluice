package audit

import (
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"testing"
)

const (
	mitText = `MIT License

Copyright (c) 2013 TOML authors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software [...]

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND [...]`
	gplText    = "                    GNU GENERAL PUBLIC LICENSE\n                       Version 3, 29 June 2007\n"
	apacheText = "                                 Apache License\n                           Version 2.0, January 2004\n"
	bsd3Text   = "Redistribution and use in source and binary forms, with or without\nmodification [...]\n" +
		"   * Neither the name of Google Inc. nor the names of its\ncontributors may be used [...]"
	bsd2Text = "Redistribution and use in source and binary forms, with or without\nmodification, are permitted [...]"
)

func TestLicencesAreNamedByTheirClauses(t *testing.T) {
	for text, want := range map[string]string{
		mitText:    "MIT",
		gplText:    "GPL-3.0",
		apacheText: "Apache-2.0",
		bsd3Text:   "BSD-3-Clause",
		bsd2Text:   "BSD-2-Clause",
		"                  GNU LESSER GENERAL PUBLIC LICENSE\n                       Version 2.1, February 1999": "LGPL-2.1",
		"Mozilla Public License Version 2.0\n==================================":                                 "MPL-2.0",
		"All rights reserved. Ask us.": "",
	} {
		if got := identifyLicense(text); got != want {
			t.Errorf("%.40q: %q, want %q", text, got, want)
		}
	}
	if got := escapeModulePath("github.com/BurntSushi/toml"); got != "github.com/!burnt!sushi/toml" {
		t.Errorf("escaped %q", got)
	}
}

// seedModCache writes each module's licence file at its place in a
// module cache: path@version, capitals escaped.
func seedModCache(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for at, text := range files {
		modVersion, name, _ := strings.Cut(at, "|")
		p := filepath.Join(dir, filepath.FromSlash(escapeModulePath(modVersion)))
		if err := os.MkdirAll(p, 0o750); err != nil {
			t.Fatal(err)
		}
		if name != "" {
			if err := os.WriteFile(filepath.Join(p, name), []byte(text), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	return dir
}

// TestEveryLinkedModuleHasAnAdmittedLicence: a binary linking an MIT
// module (its path with capitals, COPYING), a GPL one, one replaced by an
// Apache fork, and one shipping no licence. The bill of materials names
// each licence; the GPL module is outside the default list, the module
// without a file is said unread, and admitting GPL-3.0 removes the first.
func TestEveryLinkedModuleHasAnAdmittedLicence(t *testing.T) {
	cache := seedModCache(t, map[string]string{
		"github.com/BurntSushi/toml@v1.4.0|COPYING": mitText,
		"example.org/gpl@v1.0.0|LICENSE":            gplText,
		"example.org/fork@v1.0.1|LICENSE.md":        apacheText,
		"example.org/none@v0.1.0|":                  "",
	})
	target := &Target{BinaryPath: "app", ModCache: cache, Build: &debug.BuildInfo{
		GoVersion: "go1.26.0",
		Main:      debug.Module{Path: "example.org/app", Version: "(devel)"},
		Deps: []*debug.Module{
			{Path: "github.com/BurntSushi/toml", Version: "v1.4.0"},
			{Path: "example.org/gpl", Version: "v1.0.0"},
			{Path: "example.org/orig", Version: "v1.0.0", Replace: &debug.Module{Path: "example.org/fork", Version: "v1.0.1"}},
			{Path: "example.org/none", Version: "v0.1.0"},
			{Path: "example.org/local", Version: "v0.0.0", Replace: &debug.Module{Path: "../local"}},
		},
	}}
	licences := map[string]string{}
	for _, m := range SBOM(target) {
		licences[m.Path] = m.License
	}
	for path, want := range map[string]string{
		"stdlib": "BSD-3-Clause", "github.com/BurntSushi/toml": "MIT", "example.org/gpl": "GPL-3.0",
		"example.org/fork": "Apache-2.0", "example.org/none": "", "example.org/app": "",
	} {
		if licences[path] != want {
			t.Errorf("%s: licence %q in the bill of materials, want %q", path, licences[path], want)
		}
	}

	run := func(answers Answers) []Finding {
		t.Helper()
		findings, err := Run(Selection{Components: {"go"}, Goals: {"compliance"}}, answers, target)
		if err != nil {
			t.Fatal(err)
		}
		return findings
	}
	got := run(nil)
	if len(got) != 2 || !strings.Contains(got[0].Current, "example.org/gpl@v1.0.0 (GPL-3.0)") ||
		strings.Contains(got[0].Current, "toml") || strings.Contains(got[0].Current, "fork") ||
		!strings.Contains(got[1].Current, "example.org/none@v0.1.0") || strings.Contains(got[1].Current, "local") {
		t.Errorf("default list: %+v", got)
	}
	got = run(Answers{"licenses": "MIT,Apache-2.0,BSD-3-Clause,GPL-3.0"})
	if slices.ContainsFunc(got, func(f Finding) bool { return strings.Contains(f.Current, "outside") }) {
		t.Errorf("GPL-3.0 admitted, still reported: %+v", got)
	}

	target.ModCache = ""
	if got := run(nil); len(got) != 1 || !strings.Contains(got[0].Current, "no module cache was given") {
		t.Errorf("no cache: %+v", got)
	}
}
