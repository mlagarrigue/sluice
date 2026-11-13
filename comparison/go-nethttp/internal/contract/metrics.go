package contract

import (
	"encoding/json"
	"math"
	"math/bits"
	"net/http"
	"runtime"
	"runtime/metrics"
	"sync/atomic"
)

// The metrics point of the contract, GET /study/metrics: every number
// cumulative since the server started, read at the start and the end of a
// stage. The runtime's part is read as the study's server reads it
// (comparison/sluice/metrics.go), so that the two Go processes explain
// their p99 in one unit. "batches" is null for a version that does not
// group; the DataLoader variant counts its fetches there, as R counts its
// gateway's.

// MetricsDoc is the document, field for field the contract's.
type MetricsDoc struct {
	Runtime    string      `json:"runtime"`
	CPUSeconds float64     `json:"cpu_seconds"`
	GC         *GCDoc      `json:"gc"`
	Batches    *BatchesDoc `json:"batches"`
}

// BatchesDoc counts the SQL batches of unit reads and the keys in them.
type BatchesDoc struct {
	Count uint64   `json:"count"`
	Keys  uint64   `json:"keys"`
	Sizes []Bucket `json:"sizes"`
}

// BatchStats counts batches by size in powers of two (1, 2, 4…), as the
// study's server does: the fetch function records, the metrics point reads,
// from any goroutine.
type BatchStats struct {
	count, keys atomic.Uint64
	sizes       [batchBuckets]atomic.Uint64
}

// batchBuckets reaches 2^20 keys a batch.
const batchBuckets = 21

// Record counts one batch of keys.
func (b *BatchStats) Record(keys int) {
	if keys <= 0 {
		return
	}
	b.count.Add(1)
	b.keys.Add(uint64(keys))
	b.sizes[min(bits.Len(uint(keys-1)), batchBuckets-1)].Add(1)
}

func (b *BatchStats) doc() *BatchesDoc {
	d := &BatchesDoc{Count: b.count.Load(), Keys: b.keys.Load()}
	for i := range b.sizes {
		if n := b.sizes[i].Load(); n > 0 {
			d.Sizes = append(d.Sizes, Bucket{Le: float64(uint64(1) << i), Count: n})
		}
	}
	return d
}

// GCDoc is the collector's part.
type GCDoc struct {
	CPUSeconds float64 `json:"cpu_seconds"`
	Pauses     struct {
		Count   uint64   `json:"count"`
		Seconds float64  `json:"seconds"`
		Buckets []Bucket `json:"buckets"`
	} `json:"pauses"`
}

// Bucket counts what is at most Le and above the bucket before it; only the
// buckets that counted something are written.
type Bucket struct {
	Le    float64 `json:"le"`
	Count uint64  `json:"count"`
}

// ReadMetrics is the document now.
func ReadMetrics() MetricsDoc {
	samples := []metrics.Sample{
		{Name: "/cpu/classes/total:cpu-seconds"},
		{Name: "/cpu/classes/idle:cpu-seconds"},
		{Name: "/cpu/classes/gc/total:cpu-seconds"},
		{Name: "/cpu/classes/gc/pause:cpu-seconds"},
		{Name: "/sched/pauses/total/gc:seconds"},
	}
	metrics.Read(samples)
	gc := &GCDoc{CPUSeconds: samples[2].Value.Float64()}
	// The pause CPU is every pause times GOMAXPROCS when it happened: over
	// GOMAXPROCS it is the pauses' wall time.
	gc.Pauses.Seconds = samples[3].Value.Float64() / float64(runtime.GOMAXPROCS(0))
	h := samples[4].Value.Float64Histogram()
	for i, n := range h.Counts {
		if n == 0 {
			continue
		}
		gc.Pauses.Count += n
		le := h.Buckets[i+1] // Counts[i] is [Buckets[i], Buckets[i+1])
		if math.IsInf(le, 1) {
			le = math.MaxFloat64
		}
		gc.Pauses.Buckets = append(gc.Pauses.Buckets, Bucket{Le: le, Count: n})
	}
	return MetricsDoc{
		Runtime:    runtime.Version(),
		CPUSeconds: samples[0].Value.Float64() - samples[1].Value.Float64(),
		GC:         gc,
	}
}

// Metrics answers GET /study/metrics; batches is nil for a contender that
// does not group, and its part of the document is then null.
func Metrics(batches *BatchStats) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		d := ReadMetrics()
		if batches != nil {
			d.Batches = batches.doc()
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(d)
	}
}
