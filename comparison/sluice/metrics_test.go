package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/mlagarrigue/sluice/comparison/sluice/internal/contract"
)

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
	var d contract.MetricsDoc
	if err := json.Unmarshal(r.body, &d); err != nil {
		t.Fatal(err)
	}
	t.Logf("batches %d keys %d sizes %v", d.Batches.Count, d.Batches.Keys, d.Batches.Sizes)
	if d.Batches == nil || d.Batches.Keys < 64 || d.Batches.Keys <= d.Batches.Count || d.GC == nil || d.CPUSeconds <= 0 || d.Runtime == "" {
		t.Fatalf("%+v", d)
	}
}
