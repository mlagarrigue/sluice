package gateway

import (
	"slices"
	"testing"
)

// The set of keys already asked is per batch: a key asked in two batches is
// fetched in both. Kept across batches, the second ask was answered missing
// without the backend being asked — the ordinary case of a service, the
// same key in two batches — and no test sent two.
func TestLookupAsksAgainInTheNextBatch(t *testing.T) {
	var asked [][]int
	pipeline := Lookup(ErrNoReply, func(keys []int, found map[int]string) error {
		asked = append(asked, slices.Clone(keys))
		for _, k := range keys {
			found[k] = "v"
		}
		return nil
	})
	first, second := lookupCalls(1), lookupCalls(1)
	pipeline(streamOf(first, second))
	if want := [][]int{{1}, {1}}; !slices.EqualFunc(asked, want, slices.Equal) {
		t.Errorf("fetch saw %v, want %v", asked, want)
	}
	if r := <-second[0].reply; r.err != nil || r.out != "v" {
		t.Errorf("the second batch answered %q, %v; want v", r.out, r.err)
	}
}
