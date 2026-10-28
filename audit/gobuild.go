package audit

import (
	"go/version"
	"strings"
)

// What the build fixed, read in the binary rather than in the files that
// built it. A Dockerfile says what it meant to do; the binary says what
// was done, and the gap between the two is exactly what an audit exists
// to find. Everything here comes from debug/buildinfo, which is the
// toolchain's own record, written into every Go binary.

// minGoVersion is the oldest toolchain a binary of this repository should
// have been built by: the floor its go.mod states. Older ones are missing
// the fixes of every release since, and a binary is built once and run for
// months.
const minGoVersion = "go1.26.0"

// init registers the build rules, in this order: gobuild/trimpath,
// gobuild/cgo, gobuild/goamd64, gobuild/pgo, gobuild/toolchain, gobuild/vcs.
// Each is one Register call, read on its own.
func init() {
	needs := func(goal string) Selection {
		return Selection{
			Components: {"gobuild"},
			Goals:      {goal},
			Sources:    {"binary"},
		}
	}

	Register(Rule{
		Name:  "gobuild/trimpath",
		Needs: needs("security"),
		Doc:   "https://pkg.go.dev/cmd/go#hdr-Compile_packages_and_dependencies",
		Check: func(t *Target, _ Answers) []Finding {
			if t.Build == nil {
				return nil
			}
			if v, ok := t.setting("-trimpath"); ok && v == "true" {
				return nil
			}
			return []Finding{{
				Current:  "-trimpath absent",
				Proposed: "go build -trimpath",
				Why: "without it the binary carries the absolute paths of the tree it was built in, " +
					"which names the machine, the user and often the layout of a private repository; " +
					"it also makes two builds of the same source differ, so a release cannot be reproduced",
			}}
		},
	})

	Register(Rule{
		Name:  "gobuild/cgo",
		Needs: needs("security"),
		Doc:   "https://pkg.go.dev/cmd/cgo",
		Check: func(t *Target, _ Answers) []Finding {
			if t.Build == nil {
				return nil
			}
			if v, ok := t.setting("CGO_ENABLED"); !ok || v == "0" {
				return nil
			}
			return []Finding{{
				Current:  "CGO_ENABLED=1",
				Proposed: "CGO_ENABLED=0, unless a C dependency is required",
				Why: "the binary then needs a C library at run time, so it cannot run on a scratch or " +
					"distroless image, and the C code it links is outside the module graph: neither the " +
					"dependency rule nor a vulnerability database sees it",
			}}
		},
	})

	Register(Rule{
		Name:  "gobuild/goamd64",
		Needs: needs("performance"),
		Doc:   "https://pkg.go.dev/cmd/go#hdr-Build_constraints",
		Check: func(t *Target, _ Answers) []Finding {
			if t.Build == nil {
				return nil
			}
			if arch, _ := t.setting("GOARCH"); arch != "amd64" {
				return nil
			}
			level, ok := t.setting("GOAMD64")
			if ok && level != "v1" {
				return nil
			}
			current := "GOAMD64=v1 (the 2003 baseline)"
			if !ok {
				current = "GOAMD64 unset, so v1"
			}
			return []Finding{{
				Current:  current,
				Proposed: "GOAMD64=v3",
				Why: "v1 is the x86-64 of 2003: the compiler emits none of the instructions every server " +
					"and laptop sold in the last decade has. v3 is the level those machines all meet; a " +
					"binary built for it will not start on a CPU older than that, which is the trade",
			}}
		},
	})

	Register(Rule{
		Name:  "gobuild/pgo",
		Needs: needs("performance"),
		Doc:   "https://go.dev/doc/pgo",
		Check: func(t *Target, _ Answers) []Finding {
			if t.Build == nil {
				return nil
			}
			if v, ok := t.setting("-pgo"); ok && v != "" && v != "off" {
				return nil
			}
			return []Finding{{
				Current:  "no profile-guided optimisation",
				Proposed: "a default.pgo beside the main package, from a profile of the real load",
				Why: "the toolchain inlines and lays out code by what a recorded profile says is hot; its " +
					"own documentation reports between 2 and 14 % on its benchmark set. The profile has " +
					"to come from the load that matters, and a stale one is worth less than none",
			}}
		},
	})

	Register(Rule{
		Name:  "gobuild/toolchain",
		Needs: needs("reliability"),
		Doc:   "https://go.dev/doc/toolchain",
		Check: func(t *Target, _ Answers) []Finding {
			if t.Build == nil {
				return nil
			}
			got := t.Build.GoVersion
			if version.Compare(version.Lang(got), version.Lang(minGoVersion)) >= 0 &&
				version.Compare(got, minGoVersion) >= 0 {
				return nil
			}
			return []Finding{{
				Current:  "built by " + got,
				Proposed: "rebuild with " + minGoVersion + " or later",
				Why: "a binary is built once and run for months, so every fix released after its " +
					"toolchain is a fix it does not have; " + minGoVersion + " is the floor this " +
					"repository states, and the toolchain directive makes the version a property of " +
					"the module rather than of whoever built it",
			}}
		},
	})

	Register(Rule{
		Name:  "gobuild/vcs",
		Needs: needs("reliability"),
		Doc:   "https://pkg.go.dev/runtime/debug#ReadBuildInfo",
		Check: func(t *Target, _ Answers) []Finding {
			if t.Build == nil {
				return nil
			}
			rev, hasRev := t.setting("vcs.revision")
			modified, _ := t.setting("vcs.modified")
			switch {
			case !hasRev:
				return []Finding{{
					Current:  "no vcs.revision recorded",
					Proposed: "build from a clean checkout, inside the repository, so the toolchain stamps it",
					Why: "without the revision a binary in production cannot be tied to a commit: a " +
						"bug report names a version that no diff can be read against",
				}}
			case modified == "true":
				return []Finding{{
					Current:  "built from a modified tree at " + short(rev),
					Proposed: "build from a committed tree",
					Why: "the revision stamped is not what ran: the diff between them lives only on the " +
						"machine that built it, and is lost with it",
				}}
			}
			return nil
		},
	})
}

// short is a revision as a reader quotes it.
func short(rev string) string {
	if len(rev) > 12 {
		return rev[:12]
	}
	if rev == "" {
		return "an unnamed revision"
	}
	return strings.TrimSpace(rev)
}
