package vertical

import (
	"context"
	"errors"
	"math"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/gateway"
)

// A batch mixing tenants where one group fails and the others succeed must
// answer every caller exactly once: the successful groups with their results,
// the failed group with its error. Partitions.Each keeps offering groups
// after a failure, so by the time it returns the successful groups have
// already replied — failing the whole batch at that point answers those
// calls a second time, which panics inside the gateway's run goroutine and
// takes the pipeline down for every caller that follows. This exercises
// dispatch through a real gateway, with no database behind it, which is
// exactly the seam serve uses.
func TestDispatchAnswersAMixedBatchExactlyOnce(t *testing.T) {
	boom := errors.New("backend refused")

	gw := gateway.New(gateway.Config{Size: 8, Within: 50 * time.Millisecond},
		func(calls sluice.Stream[*gateway.Call[request, Result]]) {
			var parts gateway.Partitions[*gateway.Call[request, Result], string]
			calls(func(batch sluice.Batch[*gateway.Call[request, Result]]) bool {
				dispatch(&parts, batch, func(tenant string, group []*gateway.Call[request, Result]) error {
					if tenant == "evil" {
						return boom
					}
					for _, c := range group {
						c.Reply(Result{Accepted: true})
					}
					return nil
				})
				return true
			})
		})
	defer func() { _ = gw.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// One submission, tenants interleaved, so the failing group rides the
	// same pipeline batch as the ones that succeed.
	outcomes := gw.DoBatch(ctx, []request{
		{tenant: "acme", want: Amendment{OrderID: 1, Index: 0, SaleQty: 1}},
		{tenant: "evil", want: Amendment{OrderID: 2, Index: 0, SaleQty: 1}},
		{tenant: "acme", want: Amendment{OrderID: 1, Index: 1, SaleQty: 2}},
		{tenant: "evil", want: Amendment{OrderID: 2, Index: 1, SaleQty: 2}},
	})

	for i, o := range outcomes {
		switch i % 2 {
		case 0: // acme
			if o.Err != nil {
				t.Errorf("call %d (acme): failed with %v; its group succeeded", i, o.Err)
			} else if !o.Out.Accepted {
				t.Errorf("call %d (acme): got %+v, want its group's reply", i, o.Out)
			}
		case 1: // evil
			if !errors.Is(o.Err, boom) {
				t.Errorf("call %d (evil): err = %v, want the group's own failure", i, o.Err)
			}
		}
	}

	// And the gateway survived the mixed batch: a later caller is served
	// rather than left on a dead pipeline.
	out, err := gw.Do(ctx, request{tenant: "acme", want: Amendment{OrderID: 3, SaleQty: 1}})
	if err != nil || !out.Accepted {
		t.Errorf("the gateway did not survive the mixed batch: out %+v, err %v", out, err)
	}
}

// line_index is int4: an index outside int32 names no row, so it must stay
// out of the query rather than wrap into one that names another line —
// 1<<32 used to reach the database as line 0.
func TestLineKeysLeavesOutIndexesNoInt4Holds(t *testing.T) {
	if strconv.IntSize == 32 {
		t.Skip("an int cannot exceed int32 here")
	}
	wide := int64(math.MaxInt32) + 1
	group := []*gateway.Call[request, Result]{
		{In: request{want: Amendment{OrderID: 1, Index: 2}}},
		{In: request{want: Amendment{OrderID: 1, Index: int(wide << 1)}}}, // 1<<32
		{In: request{want: Amendment{OrderID: 3, Index: math.MinInt32}}},
		{In: request{want: Amendment{OrderID: 4, Index: int(-wide - 1)}}},
	}
	orderIDs, indexes := lineKeys(group)
	if want := []int64{1, 3}; !slices.Equal(orderIDs, want) {
		t.Errorf("orderIDs = %v, want %v", orderIDs, want)
	}
	if want := []int32{2, math.MinInt32}; !slices.Equal(indexes, want) {
		t.Errorf("indexes = %v, want %v", indexes, want)
	}
}
