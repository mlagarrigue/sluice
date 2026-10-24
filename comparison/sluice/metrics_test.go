package main

import (
	"encoding/json"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"testing"
)

// TestBatchStats: a batch counts once, its keys add up, and its size lands
// in the bucket of the next power of two.
func TestBatchStats(t *testing.T) {
	var b batchStats
	for _, n := range []int{1, 3, 64, 0, 65} {
		b.record(n)
	}
	d := b.doc()
	want := []bucket{{Le: 1, Count: 1}, {Le: 4, Count: 1}, {Le: 64, Count: 1}, {Le: 128, Count: 1}}
	if d.Count != 4 || d.Keys != 133 || !slices.Equal(d.Sizes, want) {
		t.Fatalf("%+v, want 4 batches, 133 keys, %v", d, want)
	}
}

// TestReadRuntime: after two collections, the pauses are counted, their
// buckets add up to the count in increasing order, and the collector's CPU
// is part of the process's.
func TestReadRuntime(t *testing.T) {
	runtime.GC()
	runtime.GC()
	cpu, gc := readRuntime()
	var sum uint64
	for i, b := range gc.Pauses.Buckets {
		sum += b.Count
		if i > 0 && b.Le <= gc.Pauses.Buckets[i-1].Le {
			t.Fatalf("buckets out of order: %v", gc.Pauses.Buckets)
		}
	}
	if gc.Pauses.Count < 2 || sum != gc.Pauses.Count || gc.Pauses.Seconds <= 0 {
		t.Fatalf("pauses %+v", gc.Pauses)
	}
	if cpu <= 0 || gc.CPUSeconds <= 0 || gc.CPUSeconds > cpu {
		t.Fatalf("cpu %v, gc cpu %v", cpu, gc.CPUSeconds)
	}
}

// TestMetricsPoint: after concurrent unit reads under a gateway that waits
// 20 ms for its batch, the point shows batches of more than one key, and
// the runtime's part of the document.
func TestMetricsPoint(t *testing.T) {
	s := start(t, "-gateway-within", "20ms")
	var wg sync.WaitGroup
	for k := range 64 {
		wg.Go(func() {
			if r, err := s.do("GET", fmt.Sprintf("/orders/%d", 1+k), nil); err != nil || r.status != 200 {
				t.Errorf("unit read %d: %v %d", k, err, r.status)
			}
		})
	}
	wg.Wait()
	r, err := s.do("GET", "/study/metrics", nil)
	if err != nil || r.status != 200 || r.ctype != "application/json" {
		t.Fatalf("%v %d %q", err, r.status, r.ctype)
	}
	var d metricsDoc
	if err := json.Unmarshal(r.body, &d); err != nil {
		t.Fatal(err)
	}
	t.Logf("batches %d keys %d sizes %v", d.Batches.Count, d.Batches.Keys, d.Batches.Sizes)
	if d.Batches == nil || d.Batches.Keys < 64 || d.Batches.Keys <= d.Batches.Count || d.GC == nil || d.CPUSeconds <= 0 || d.Runtime == "" {
		t.Fatalf("%+v", d)
	}
}
