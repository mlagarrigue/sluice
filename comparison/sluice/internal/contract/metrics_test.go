package contract

import (
	"runtime"
	"slices"
	"testing"
)

// TestBatchStats: a batch counts once, its keys add up, and its size lands
// in the bucket of the next power of two.
func TestBatchStats(t *testing.T) {
	var b BatchStats
	for _, n := range []int{1, 3, 64, 0, 65} {
		b.Record(n)
	}
	d := b.Doc()
	want := []Bucket{{Le: 1, Count: 1}, {Le: 4, Count: 1}, {Le: 64, Count: 1}, {Le: 128, Count: 1}}
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
	cpu, gc := ReadRuntime()
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
