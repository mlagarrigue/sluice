package contract

import (
	"encoding/json"
	"math"
	"net/http"
	"runtime"
	"runtime/metrics"
)

// The metrics point of the contract, GET /study/metrics: every number
// cumulative since the server started, read at the start and the end of a
// stage. The runtime's part is read as the study's server reads it
// (comparison/sluice/metrics.go), so that the two Go processes explain
// their p99 in one unit; "batches" is null, as a contender that does not
// group has none.

// MetricsDoc is the document, field for field the contract's.
type MetricsDoc struct {
	Runtime    string  `json:"runtime"`
	CPUSeconds float64 `json:"cpu_seconds"`
	GC         *GCDoc  `json:"gc"`
	Batches    *struct {
		Count uint64   `json:"count"`
		Keys  uint64   `json:"keys"`
		Sizes []Bucket `json:"sizes"`
	} `json:"batches"`
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

// ServeMetrics answers GET /study/metrics.
func ServeMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(ReadMetrics())
}
