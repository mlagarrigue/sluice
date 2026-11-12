package redis_test

import (
	"strconv"
	"testing"
)

const benchKeys = 1000

func benchConn(b *testing.B) ([][]string, func(cmds [][]string)) {
	b.Helper()
	c, p := dial(b)
	sets := make([][]string, benchKeys)
	gets := make([][]string, benchKeys)
	for i := range benchKeys {
		k := p + strconv.Itoa(i)
		sets[i] = []string{"SET", k, "value-" + strconv.Itoa(i), "EX", "60"}
		gets[i] = []string{"GET", k}
	}
	if _, err := c.Pipeline(b.Context(), sets); err != nil {
		b.Fatal(err)
	}
	return gets, func(cmds [][]string) {
		rs, err := c.Pipeline(b.Context(), cmds)
		if err != nil || len(rs) != len(cmds) {
			b.Fatal(err)
		}
	}
}

// BenchmarkGetPipelined is a thousand GETs in one Pipeline: one write, one
// round trip. ns/key is the op over the thousand keys.
func BenchmarkGetPipelined(b *testing.B) {
	gets, run := benchConn(b)
	b.ReportAllocs()
	for b.Loop() {
		run(gets)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*benchKeys), "ns/key")
}

// BenchmarkGetOneByOne is the same thousand GETs, one round trip each.
func BenchmarkGetOneByOne(b *testing.B) {
	gets, run := benchConn(b)
	b.ReportAllocs()
	for b.Loop() {
		for _, g := range gets {
			run([][]string{g})
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*benchKeys), "ns/key")
}
