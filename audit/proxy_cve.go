package audit

import (
	"slices"
	"strconv"
	"strings"
)

// ngxAdvisory is an nginx security advisory as nginx.org lists it: the
// range found vulnerable, the releases that are not — "1.26.2+" covers
// 1.26.2 and its later patches, and the highest one every later release —
// and the directive without which the flaw is not reached.
type ngxAdvisory struct {
	ID         string
	Summary    string
	Vulnerable [][2]string
	Fixed      []string
	Directive  string
}

// ngxAdvisories are read from https://nginx.org/en/security_advisories.html,
// each one with the module whose directive reaches it: a release alone says
// a flaw is present, not that it is reachable.
var ngxAdvisories = []ngxAdvisory{{
	ID:         "CVE-2024-7347",
	Summary:    "a crafted mp4 file makes the worker over-read and may crash it, with ngx_http_mp4_module in use",
	Vulnerable: [][2]string{{"1.5.13", "1.27.0"}},
	Fixed:      []string{"1.27.1", "1.26.2"},
	Directive:  "mp4",
}}

// semver reads "1.26.2" as three numbers, false for anything else.
func semver(s string) ([3]int, bool) {
	var v [3]int
	f := strings.Split(s, ".")
	if len(f) != 3 {
		return v, false
	}
	for i := range f {
		n, err := strconv.Atoi(f[i])
		if err != nil {
			return v, false
		}
		v[i] = n
	}
	return v, true
}

func cmpVersion(a, b [3]int) int { return slices.Compare(a[:], b[:]) }

// affected says whether release v is within the advisory's vulnerable
// range and not covered by a fix.
func (adv ngxAdvisory) affected(v [3]int) bool {
	in := false
	for _, r := range adv.Vulnerable {
		lo, _ := semver(r[0])
		hi, _ := semver(r[1])
		in = in || (cmpVersion(v, lo) >= 0 && cmpVersion(v, hi) <= 0)
	}
	if !in {
		return false
	}
	var highest [3]int
	for _, f := range adv.Fixed {
		if x, _ := semver(f); cmpVersion(x, highest) > 0 {
			highest = x
		}
	}
	for _, f := range adv.Fixed {
		x, _ := semver(f)
		sameBranch := x[0] == v[0] && x[1] == v[1]
		if cmpVersion(v, x) >= 0 && (sameBranch || x == highest) {
			return false
		}
	}
	return true
}

func init() {
	Register(Rule{
		Name:  "proxy/cve",
		Needs: Selection{Components: {"proxy"}, Goals: {"security"}, Sources: {"config"}, CVEs: {"yes"}},
		Doc:   "https://nginx.org/en/security_advisories.html",
		Check: func(t *Target, _ Answers) []Finding {
			p := t.Proxy
			if p == nil || p.Kind != "nginx" {
				return nil
			}
			v, known := semver(p.Version)
			var out []Finding
			for _, adv := range ngxAdvisories {
				var lines []string
				for _, d := range p.Nginx.all(adv.Directive) {
					lines = append(lines, strconv.Itoa(d.Line))
				}
				f := Finding{
					Current:  adv.ID + " on nginx " + p.Version + ": " + adv.Summary,
					Proposed: "nginx " + strings.Join(adv.Fixed, " or ") + ", or a later release of either branch",
				}
				switch {
				case !known && len(lines) == 0:
					continue // nothing reaches it, whatever the release
				case !known:
					f.Current = adv.ID + ", nginx release not given: " + adv.Summary
					f.Verdict = VerdictUndetermined
					f.Why = "the configuration uses " + adv.Directive + " (line " + strings.Join(lines, ", ") +
						" of " + p.Path + "); whether the release is affected needs -proxy-version"
				case !adv.affected(v):
					continue
				case len(lines) == 0:
					f.Verdict = VerdictAbsent
					f.Why = "the release is affected, but no " + adv.Directive + " directive in " + p.Path +
						" reaches the flaw; one added later would, so the upgrade stays due"
				default:
					f.Verdict = VerdictApplies
					f.Why = "the release is affected and " + p.Path + " uses " + adv.Directive +
						", line " + strings.Join(lines, ", ")
				}
				out = append(out, f)
			}
			return out
		},
	})
}
