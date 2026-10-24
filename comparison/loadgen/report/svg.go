package main

import (
	"fmt"
	"math"
	"slices"
	"strings"
)

// The two curves of §5, written as SVG with the standard library alone:
// latency (p50 and p99) against the rate, and memory against the rate, on
// the absolute grid (§3.2). SVG rather than a plotting library because it
// is an open standard a browser, a document and a diff all read, and
// because a chart is axes, ticks and polylines — not a dependency.
//
// The rates are placed on a logarithmic scale: the grid is geometric
// (100 × 1.05^k), so equal steps are equal distances, which is what the
// reader of a capacity ladder wants to see.

// series is one contender's curve: a y per rate, holes allowed (a stage
// the contender never reached).
type series struct {
	name   string
	dashed bool // the p50 of a latency chart, so one colour carries one contender
	points []point
}

type point struct {
	rate int
	y    float64
}

// palette is read in order; a contender keeps its colour across the two
// charts of a family. Chosen for a white background and distinguishable in
// grey print.
var palette = []string{"#1f4e79", "#b4531f", "#2d6a2d", "#6a2d6a", "#8a6d1f", "#1f6a6a"}

// chart renders one chart, 720×360, with its axes, its ticks, its legend
// and one polyline per series.
func chart(title, yLabel string, all []series) string {
	const (
		w, h             = 720, 360
		left, right      = 64, 180
		top, bottom      = 40, 48
		plotW, plotH     = w - left - right, h - top - bottom
		tickLen, legendX = 5, w - right + 16
	)

	minRate, maxRate := math.Inf(1), math.Inf(-1)
	minY, maxY := math.Inf(1), math.Inf(-1)
	for _, s := range all {
		for _, p := range s.points {
			minRate = math.Min(minRate, float64(p.rate))
			maxRate = math.Max(maxRate, float64(p.rate))
			minY = math.Min(minY, p.y)
			maxY = math.Max(maxY, p.y)
		}
	}
	if math.IsInf(minRate, 1) { // nothing to draw: the axes alone say so
		minRate, maxRate, minY, maxY = 100, 1000, 0, 1
	}
	if maxRate <= minRate {
		maxRate = minRate * 1.05
	}
	minY = 0 // a latency or a memory chart that does not start at zero lies
	if maxY <= minY {
		maxY = 1
	}
	maxY *= 1.1 // headroom, so the highest point is not on the frame

	x := func(rate int) float64 {
		lo, hi := math.Log(minRate), math.Log(maxRate)
		return left + plotW*(math.Log(float64(rate))-lo)/(hi-lo)
	}
	y := func(v float64) float64 {
		return top + plotH*(1-(v-minY)/(maxY-minY))
	}

	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d" font-family="sans-serif" font-size="12">`, w, h, w, h)
	fmt.Fprint(&b, `<rect width="100%" height="100%" fill="#ffffff"/>`)
	fmt.Fprintf(&b, `<text x="%d" y="24" font-size="14" font-weight="bold">%s</text>`, left, escape(title))

	// Frame and ticks.
	fmt.Fprintf(&b, `<path d="M %g %g L %g %g L %g %g" fill="none" stroke="#333333"/>`,
		float64(left), float64(top), float64(left), float64(top+plotH), float64(left+plotW), float64(top+plotH))
	for _, t := range rateTicks(all) {
		fmt.Fprintf(&b, `<path d="M %g %g l 0 %d" stroke="#333333"/>`, x(t), float64(top+plotH), tickLen)
		fmt.Fprintf(&b, `<text x="%g" y="%d" text-anchor="middle">%d</text>`, x(t), top+plotH+20, t)
	}
	for i := range 5 {
		v := minY + (maxY-minY)*float64(i)/4
		fmt.Fprintf(&b, `<path d="M %d %g l -%d 0" stroke="#333333"/>`, left, y(v), tickLen)
		fmt.Fprintf(&b, `<text x="%d" y="%g" text-anchor="end">%s</text>`, left-tickLen-4, y(v)+4, trim(v))
	}
	fmt.Fprintf(&b, `<text x="%d" y="%d" text-anchor="middle">requests per second</text>`, left+plotW/2, h-12)
	fmt.Fprintf(&b, `<text x="16" y="%d" text-anchor="middle" transform="rotate(-90 16 %d)">%s</text>`,
		top+plotH/2, top+plotH/2, escape(yLabel))

	// One polyline per series, and its legend entry.
	for i, s := range all {
		colour := palette[i%len(palette)]
		if s.dashed { // p50 and p99 of one contender share its colour
			colour = palette[(i/2)%len(palette)]
		}
		pts := slices.Clone(s.points)
		slices.SortFunc(pts, func(a, b point) int { return a.rate - b.rate })
		var d strings.Builder
		for j, p := range pts {
			if j == 0 {
				fmt.Fprintf(&d, "M %g %g", x(p.rate), y(p.y))
				continue
			}
			fmt.Fprintf(&d, " L %g %g", x(p.rate), y(p.y))
		}
		dash := ""
		if s.dashed {
			dash = ` stroke-dasharray="5 4"`
		}
		if d.Len() > 0 {
			fmt.Fprintf(&b, `<path d="%s" fill="none" stroke="%s" stroke-width="2"%s/>`, d.String(), colour, dash)
		}
		for _, p := range pts {
			fmt.Fprintf(&b, `<circle cx="%g" cy="%g" r="2.5" fill="%s"/>`, x(p.rate), y(p.y), colour)
		}
		ly := top + 4 + i*18
		fmt.Fprintf(&b, `<path d="M %d %d l 22 0" stroke="%s" stroke-width="2"%s/>`, legendX, ly, colour, dash)
		fmt.Fprintf(&b, `<text x="%d" y="%d">%s</text>`, legendX+28, ly+4, escape(s.name))
	}
	fmt.Fprint(&b, `</svg>`)
	return b.String()
}

// rateTicks labels the ends and a few points in between, never every
// stage: a capacity ladder has thirty of them and the axis becomes a bar.
func rateTicks(all []series) []int {
	var rates []int
	for _, s := range all {
		for _, p := range s.points {
			if !slices.Contains(rates, p.rate) {
				rates = append(rates, p.rate)
			}
		}
	}
	slices.Sort(rates)
	if len(rates) <= 6 {
		return rates
	}
	out := []int{rates[0]}
	step := float64(len(rates)-1) / 5
	for i := 1; i <= 5; i++ {
		r := rates[int(math.Round(float64(i)*step))]
		if r != out[len(out)-1] {
			out = append(out, r)
		}
	}
	return out
}

// trim writes a number the way the axis wants it: no trailing zeros, and
// no exponent for the magnitudes a latency or an RSS takes.
func trim(v float64) string {
	switch {
	case v == 0:
		return "0"
	case v >= 100:
		return fmt.Sprintf("%.0f", v)
	case v >= 10:
		return fmt.Sprintf("%.1f", v)
	default:
		return fmt.Sprintf("%.2f", v)
	}
}

// escape makes a title safe inside an SVG text node. The names come from
// the campaign's matrix, so they are the author's own, but a report that
// cannot be parsed is a report that cannot be read.
func escape(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;")
	return r.Replace(s)
}
