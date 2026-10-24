package main

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/mlagarrigue/sluice/comparison/loadgen/stats"
)

// The report of §5, in its nine points, in order, for every family of the
// campaign. A point with nothing to show says so: "none" is a result the
// study publishes (§5.7 to §5.9), and a silent omission is what this
// program exists to prevent.
//
// Two holes are left on purpose, each marked in the output: the three
// sentences of §5.4, which are the author's reading and not a
// computation, and the two columns of §5.3 that CMP-32 counts (lines of
// code, dependencies). Everything else is rendered from the campaign.

// files is what a report run writes: the document and its charts.
type files struct {
	markdown string
	charts   map[string]string // file name → SVG
}

// render builds the document and the charts of a campaign.
func render(in *inputs) files {
	out := files{charts: map[string]string{}}
	var b strings.Builder

	fmt.Fprintf(&b, "# The comparison — %s\n\n", in.matrix.Name)
	if in.summary.Trial || in.matrix.Trial || !in.summary.Publishable {
		fmt.Fprint(&b, "> **Not publishable.** This campaign is marked a trial: it plays fewer\n"+
			"> rounds than a verdict needs (§7.6). The figures below are the apparatus\n"+
			"> checking itself, and no verdict of theirs is a result.\n\n")
	}
	fmt.Fprintf(&b, "The keyspace holds %s rows. What each contender ran, image and arguments,\n"+
		"is in the campaign's `matrix.json`; the draws are in its `plan.jsonl`.\n\n", thousands(in.matrix.Rows))

	for _, family := range in.families() {
		scenario, proto, _ := strings.Cut(family, "/")
		fmt.Fprintf(&b, "## %s over %s\n\n", scenario, proto)

		// §5.1 — a diagram of what is measured.
		name := chartName(family, "apparatus")
		out.charts[name] = schema(in, family)
		fmt.Fprintf(&b, "### What is measured\n\n![The three roles and the latency between them](%s)\n\n", name)

		// §5.2 — two curves.
		lat, mem := curves(in, family)
		latName, memName := chartName(family, "latency"), chartName(family, "memory")
		out.charts[latName] = chart(family+" — latency against the rate", "TTLB, ms", lat)
		out.charts[memName] = chart(family+" — memory against the rate", "server RSS, MiB", mem)
		fmt.Fprintf(&b, "### Latency and memory against the rate\n\n"+
			"![p50 dashed, p99 solid, one colour per contender](%s)\n\n"+
			"![the server's resident memory at rest under each rate](%s)\n\n"+
			"Each point is the median of the repetitions at that rate; the rates are\n"+
			"the absolute grid (§3.2) and the scale is logarithmic, so one step of the\n"+
			"grid is one distance everywhere.\n\n", latName, memName)

		// §5.3 — the short table.
		fmt.Fprint(&b, "### The short table\n\n")
		fmt.Fprint(&b, shortTable(in, family))
		fmt.Fprint(&b, "\nLines of code and dependencies are counted by CMP-32 and are not in\n"+
			"this campaign's files; a dependency count compares inside an ecosystem\n"+
			"and barely across (npm, cargo and go do not cut a module the same way).\n\n")

		// §5.4 — three sentences, the author's.
		fmt.Fprint(&b, "### Why\n\n> **To write, three sentences:** the mechanism behind the gap; where\n"+
			"> Sluice.go loses; and, for a dynamic scenario, what is arithmetic rather\n"+
			"> than framework — a contender without grouping is bounded by its pool over\n"+
			"> the web↔db round trip (§4), which must not be read as \"faster framework\".\n\n")

		// §5.5 — the verdicts.
		fmt.Fprint(&b, "### Verdicts\n\n")
		fmt.Fprint(&b, verdictTable(in, family))

		// §5.6 to §5.9 — the counters, published even at zero.
		fmt.Fprint(&b, "\n### Batch size observed\n\n")
		fmt.Fprint(&b, batchTable(in, family))
		fmt.Fprint(&b, "\n### Wrong answers\n\n")
		fmt.Fprint(&b, wrongAnswers(in, family))
		fmt.Fprint(&b, "\n### Per stage: the server's closures, the accept queue, the errors\n\n")
		fmt.Fprint(&b, perStage(in, family))
		fmt.Fprint(&b, "\n### Crashes\n\n")
		fmt.Fprint(&b, crashes(in, family))
		fmt.Fprint(&b, "\n")
	}
	out.markdown = b.String()
	return out
}

// schema draws what is measured: the three roles, the round trip injected
// between them, and the scenario's own numbers.
func schema(in *inputs, family string) string {
	scenario, proto, _ := strings.Cut(family, "/")
	var rtt, slo, conns string
	for i := range in.stages {
		s := &in.stages[i]
		if s.family == family {
			rtt = fmt.Sprintf("%g ms", s.line.RTT)
			slo = fmt.Sprintf("%g ms", s.line.SLO)
			conns = strconv.Itoa(s.line.Conns)
			break
		}
	}
	box := func(x int, label, under string) string {
		return fmt.Sprintf(`<rect x="%d" y="70" width="150" height="64" rx="6" fill="#f4f6f8" stroke="#333333"/>`+
			`<text x="%d" y="98" text-anchor="middle" font-weight="bold">%s</text>`+
			`<text x="%d" y="118" text-anchor="middle" font-size="11">%s</text>`,
			x, x+75, escape(label), x+75, escape(under))
	}
	arrow := func(x1, x2 int, label string) string {
		return fmt.Sprintf(`<path d="M %d 102 L %d 102" stroke="#333333" marker-end="url(#a)"/>`+
			`<text x="%d" y="94" text-anchor="middle" font-size="11">%s</text>`, x1, x2, (x1+x2)/2, escape(label))
	}
	return fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" width="720" height="200" viewBox="0 0 720 200" font-family="sans-serif" font-size="12">`+
		`<defs><marker id="a" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">`+
		`<path d="M 0 0 L 10 5 L 0 10 z" fill="#333333"/></marker></defs>`+
		`<rect width="100%%" height="100%%" fill="#ffffff"/>`+
		`<text x="20" y="34" font-size="14" font-weight="bold">%s over %s</text>`+
		`<text x="20" y="52" font-size="11">scenario %s, SLO %s, %s client connections, one role per machine</text>`+
		`%s%s%s%s%s`+
		`</svg>`,
		escape(scenario), escape(proto), escape(scenario), slo, conns,
		box(20, "tester", "the generator"),
		arrow(170, 280, "client round trip "+rtt),
		box(285, "web", "the contender"),
		arrow(435, 545, "web ↔ db"),
		box(550, "db", "PostgreSQL"))
}

// curves builds the two charts' series: per contender, the median of the
// repetitions at each rate.
func curves(in *inputs, family string) (latency, memory []series) {
	for _, c := range in.contenders(family) {
		p50, p99, rss := map[int][]float64{}, map[int][]float64{}, map[int][]float64{}
		for i := range in.stages {
			s := &in.stages[i]
			if s.family != family || s.contender != c {
				continue
			}
			p50[s.line.Rate] = append(p50[s.line.Rate], s.line.TTLB.P50)
			p99[s.line.Rate] = append(p99[s.line.Rate], s.line.TTLB.P99)
			rss[s.line.Rate] = append(rss[s.line.Rate], s.line.Roles.Web.RSSMax/1024)
		}
		latency = append(latency,
			series{name: c + " p50", dashed: true, points: medianPoints(p50)},
			series{name: c + " p99", points: medianPoints(p99)})
		memory = append(memory, series{name: c, points: medianPoints(rss)})
	}
	return latency, memory
}

func medianPoints(byRate map[int][]float64) []point {
	var out []point
	for _, rate := range slices.Sorted(maps.Keys(byRate)) {
		med, _, _ := stats.MedianCI(byRate[rate])
		out = append(out, point{rate: rate, y: med})
	}
	return out
}

// shortTable is §5.3: the capacity with its interval, and what the stages
// say about the connections on either side.
func shortTable(in *inputs, family string) string {
	var b strings.Builder
	fmt.Fprint(&b, "| contender | lines of code | dependencies | capacity under SLO | db connections | SLO | client connections | held by the server |\n")
	fmt.Fprint(&b, "|---|---|---|---|---|---|---|---|\n")
	for _, c := range in.contenders(family) {
		capacity := "not measured"
		if st, ok := in.capacity(family, c); ok {
			capacity = fmt.Sprintf("%s req/s (%s – %s)", trim(st.Median), trim(st.Lo), trim(st.Hi))
			if st.Unstable {
				capacity += ", **unstable**"
			}
		}
		db, slo, conns, held := "—", "—", "—", "—"
		for i := range in.stages {
			s := &in.stages[i]
			if s.family != family || s.contender != c {
				continue
			}
			if s.line.Roles.DB.Connections != nil {
				db = strconv.Itoa(*s.line.Roles.DB.Connections)
			}
			slo = fmt.Sprintf("%g ms", s.line.SLO)
			conns = strconv.Itoa(s.line.Conns)
			if s.line.Roles.Web.EstablishedMax != nil {
				held = strconv.Itoa(*s.line.Roles.Web.EstablishedMax)
			}
			break
		}
		fmt.Fprintf(&b, "| %s | — | — | %s | %s | %s | %s | %s |\n", c, capacity, db, slo, conns, held)
	}
	return b.String()
}

// verdictTable is §5.5: the primary metrics only, and the scope of Holm
// written beside them.
func verdictTable(in *inputs, family string) string {
	var rows []verdict
	for i := range in.verdicts {
		v := in.verdicts[i]
		if v.Family == family {
			rows = append(rows, v)
		}
	}
	var b strings.Builder
	if len(rows) == 0 {
		fmt.Fprint(&b, "No comparison of this family received a verdict.\n")
	} else {
		fmt.Fprint(&b, "| comparison | metric | verdict | gap | interval | rounds | p | Holm threshold |\n")
		fmt.Fprint(&b, "|---|---|---|---|---|---|---|---|\n")
		for i := range rows {
			v := &rows[i]
			// The two metrics do not share a unit: the capacity's gap is a
			// whole number of grid steps (§7.4 — no float ratio decides it,
			// 1.05^k lands either side of a step on a bit), the p99's is a
			// ratio of the paired rounds. An interval printed without its
			// unit reads as the other one.
			interval := fmt.Sprintf("×%s – ×%s", trim(v.Lo), trim(v.Hi))
			if v.Metric == "capacity" {
				interval = fmt.Sprintf("%s – %s steps", trim(v.Lo), trim(v.Hi))
			}
			fmt.Fprintf(&b, "| %s | %s | **%s** | %s | %s | %d (%dW %dL) | %.4f | %.4f |\n",
				v.Name, v.Metric, v.Verdict, v.Percent, interval, v.Rounds, v.Wins, v.Losses, v.P, v.Holm)
		}
	}
	fmt.Fprint(&b, "\nA verdict is given on the two primary metrics only — capacity under the\n"+
		"SLO and the p99 TTLB at the designated point — each against its own floor\n"+
		"(§7.4). Everything else above is a median with its interval and carries no\n"+
		"verdict. Holm's correction holds over a family, a scenario × a protocol over\n"+
		"both metrics, not over the publication: a verdict here is one of a family's,\n"+
		"and reading several families together widens what chance explains.\n")
	return b.String()
}

// batchTable is §5.6: the batch size observed, for the variants that
// group. A contender whose counters say nothing is named all the same.
func batchTable(in *inputs, family string) string {
	var b strings.Builder
	fmt.Fprint(&b, "| contender | SQL batches | keys | keys per batch |\n|---|---|---|---|\n")
	for _, c := range in.contenders(family) {
		var batches, keys uint64
		for i := range in.stages {
			s := &in.stages[i]
			if s.family != family || s.contender != c {
				continue
			}
			start, end := s.line.Metrics.Start, s.line.Metrics.End
			if start == nil || end == nil || start.Batches == nil || end.Batches == nil {
				continue
			}
			batches += end.Batches.Count - start.Batches.Count
			keys += end.Batches.Keys - start.Batches.Keys
		}
		if batches == 0 {
			fmt.Fprintf(&b, "| %s | — | — | no grouping counter |\n", c)
			continue
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %s |\n", c, thousands(int(batches)), thousands(int(keys)), //nolint:gosec // G115: a campaign's counts
			trim(float64(keys)/float64(batches)))
	}
	return b.String()
}

// wrongAnswers is §5.7: the contract counter of every configuration, zero
// included — one wrong answer fails its stage, and zero is a result.
func wrongAnswers(in *inputs, family string) string {
	var b strings.Builder
	fmt.Fprint(&b, "| contender | wrong answers | answers checked |\n|---|---|---|\n")
	for _, c := range in.contenders(family) {
		var bad, ok int
		for i := range in.stages {
			s := &in.stages[i]
			if s.family == family && s.contender == c {
				bad += s.line.BadKey
				ok += s.line.OK
			}
		}
		fmt.Fprintf(&b, "| %s | %d | %s |\n", c, bad, thousands(ok))
	}
	return b.String()
}

// perStage is §5.8: by stage, the server's closures and the accept
// queue's overflows, zero included, and the errors by nature.
func perStage(in *inputs, family string) string {
	var b strings.Builder
	// Totals over the repetitions of a rate, with the stages they cover:
	// these counters are rare events, and a median of a rare event is zero
	// in every round but the one that mattered. The reader divides.
	fmt.Fprint(&b, "| contender | rate | stages | closed by the server | reconnects | accept overflows | accept drops | 4xx | 5xx | refused | reset | connect timeout | other | timed out |\n")
	fmt.Fprint(&b, "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, c := range in.contenders(family) {
		rows := map[int]stageLine{}
		covered := map[int]int{}
		for i := range in.stages {
			s := &in.stages[i]
			if s.family == family && s.contender == c {
				covered[s.line.Rate]++
				agg := rows[s.line.Rate]
				agg.Closed += s.line.Closed
				agg.Reconnects += s.line.Reconnects
				agg.Roles.Web.ListenOverflows += s.line.Roles.Web.ListenOverflows
				agg.Roles.Web.ListenDrops += s.line.Roles.Web.ListenDrops
				agg.Status4xx += s.line.Status4xx
				agg.Status5xx += s.line.Status5xx
				agg.Refused += s.line.Refused
				agg.Reset += s.line.Reset
				agg.ConnectTimeout += s.line.ConnectTimeout
				agg.Other += s.line.Other
				agg.TimedOut += s.line.TimedOut
				rows[s.line.Rate] = agg
			}
		}
		for _, rate := range slices.Sorted(maps.Keys(rows)) {
			r := rows[rate]
			fmt.Fprintf(&b, "| %s | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d |\n",
				c, rate, covered[rate], r.Closed, r.Reconnects, r.Roles.Web.ListenOverflows, r.Roles.Web.ListenDrops,
				r.Status4xx, r.Status5xx, r.Refused, r.Reset, r.ConnectTimeout, r.Other, r.TimedOut)
		}
	}
	return b.String()
}

// crashes is §5.9: every configuration's crashes with their cause, zero
// included.
func crashes(in *inputs, family string) string {
	var b strings.Builder
	fmt.Fprint(&b, "| contender | crashes | cause |\n|---|---|---|\n")
	for _, c := range in.contenders(family) {
		var n int
		var causes []string
		for i := range in.stages {
			s := &in.stages[i]
			if s.family != family || s.contender != c || s.line.Crash == "" || s.line.Crash == "no" {
				continue
			}
			n++
			if !slices.Contains(causes, s.line.Crash) {
				causes = append(causes, s.line.Crash)
			}
		}
		cause := "—"
		if len(causes) > 0 {
			cause = strings.Join(causes, "; ")
		}
		fmt.Fprintf(&b, "| %s | %d | %s |\n", c, n, cause)
	}
	return b.String()
}

// capacity is a contender's published capacity in a family.
func (in *inputs) capacity(family, contender string) (stat, bool) {
	for _, f := range in.summary.Families {
		if f.Family != family {
			continue
		}
		st, ok := f.Capacity.Contenders[contender]
		return st, ok
	}
	return stat{}, false
}

func chartName(family, what string) string {
	return strings.ReplaceAll(family, "/", "-") + "-" + what + ".svg"
}

// thousands groups a count the way the study's tables do.
func thousands(n int) string {
	s := strconv.Itoa(n)
	if n < 0 {
		return s
	}
	var parts []string
	for len(s) > 3 {
		parts = append([]string{s[len(s)-3:]}, parts...)
		s = s[:len(s)-3]
	}
	return strings.Join(append([]string{s}, parts...), ",")
}
