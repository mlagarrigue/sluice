package main

import (
	"math"
	"math/bits"
	"runtime"
	"runtime/metrics"
	"sync/atomic"
)

// The metrics point of the contract (comparison/README.md, "The contract
// every contender honours"): GET /study/metrics, read at the start and the
// end of every stage, every number cumulative since the server started. It
// explains a p99, it is never a verdict (study §1.3, §1.2 batch size).
// Reading it costs one metrics.Read; serving costs nothing between reads.

// metricsDoc is the document, field for field the contract's.
type metricsDoc struct {
	Runtime    string      `json:"runtime"`
	CPUSeconds float64     `json:"cpu_seconds"`
	GC         *gcDoc      `json:"gc"`
	Batches    *batchesDoc `json:"batches"`
}

type gcDoc struct {
	CPUSeconds float64   `json:"cpu_seconds"`
	Pauses     pausesDoc `json:"pauses"`
}

type pausesDoc struct {
	Count   uint64   `json:"count"`
	Seconds float64  `json:"seconds"`
	Buckets []bucket `json:"buckets"`
}

type batchesDoc struct {
	Count uint64   `json:"count"`
	Keys  uint64   `json:"keys"`
	Sizes []bucket `json:"sizes"`
}

// bucket counts what is at most Le and above the bucket before it; only
// the buckets that counted something are written.
type bucket struct {
	Le    float64 `json:"le"`
	Count uint64  `json:"count"`
}

// The runtime/metrics names read. The CPU the process spent is the
// runtime's own estimate, total minus idle, so that the collector's share
// is a ratio of two numbers in one unit: the runtime documents its CPU
// classes as overestimates that compare only with each other.
const (
	metricCPUTotal  = "/cpu/classes/total:cpu-seconds"
	metricCPUIdle   = "/cpu/classes/idle:cpu-seconds"
	metricGCCPU     = "/cpu/classes/gc/total:cpu-seconds"
	metricGCPauseCP = "/cpu/classes/gc/pause:cpu-seconds"
	metricGCPauses  = "/sched/pauses/total/gc:seconds"
)

// readRuntime is the runtime's part of the document.
func readRuntime() (cpu float64, gc gcDoc) {
	samples := []metrics.Sample{
		{Name: metricCPUTotal},
		{Name: metricCPUIdle},
		{Name: metricGCCPU},
		{Name: metricGCPauseCP},
		{Name: metricGCPauses},
	}
	metrics.Read(samples)
	cpu = samples[0].Value.Float64() - samples[1].Value.Float64()
	gc.CPUSeconds = samples[2].Value.Float64()
	// The pause CPU is every pause times GOMAXPROCS when it happened (the
	// runtime's doc): over GOMAXPROCS it is the pauses' wall time, exact
	// as long as GOMAXPROCS did not change, which a server's does not.
	gc.Pauses.Seconds = samples[3].Value.Float64() / float64(runtime.GOMAXPROCS(0))
	h := samples[4].Value.Float64Histogram()
	for i, n := range h.Counts {
		if n == 0 {
			continue
		}
		gc.Pauses.Count += n
		// Counts[i] is [Buckets[i], Buckets[i+1]); the last bound may be +Inf.
		le := h.Buckets[i+1]
		if math.IsInf(le, 1) {
			le = math.MaxFloat64
		}
		gc.Pauses.Buckets = append(gc.Pauses.Buckets, bucket{Le: le, Count: n})
	}
	return cpu, gc
}

// batchStats counts the SQL batches of unit reads and the keys in them,
// by size in powers of two: 1, 2, 4… The gateway's pipeline records, from
// its one goroutine; the metrics point reads, from any.
type batchStats struct {
	count, keys atomic.Uint64
	sizes       [batchBuckets]atomic.Uint64
}

// batchBuckets reaches 2^20 keys a batch, far past any -gateway-size.
const batchBuckets = 21

func (b *batchStats) record(keys int) {
	if keys <= 0 {
		return
	}
	b.count.Add(1)
	b.keys.Add(uint64(keys))
	b.sizes[min(bits.Len(uint(keys-1)), batchBuckets-1)].Add(1)
}

func (b *batchStats) doc() *batchesDoc {
	d := &batchesDoc{Count: b.count.Load(), Keys: b.keys.Load()}
	for i := range b.sizes {
		if n := b.sizes[i].Load(); n > 0 {
			d.Sizes = append(d.Sizes, bucket{Le: float64(uint64(1) << i), Count: n})
		}
	}
	return d
}

// metrics is the document now.
func (a *app) metrics() metricsDoc {
	cpu, gc := readRuntime()
	return metricsDoc{Runtime: runtime.Version(), CPUSeconds: cpu, GC: &gc, Batches: a.batches.doc()}
}
