package gateway

import "github.com/mlagarrigue/sluice"

// Lookup is the pipeline of a gateway that fetches values by key — the shape
// most gateways have, written once rather than by every caller of [New].
//
// fetch receives the keys of a batch, each once, in the order they were first
// asked for, and fills found with the value of every key it finds: one
// `= ANY($1)` for the batch, one call to a remote service, one read of a
// cache. A call whose key fetch did not put in found then fails with missing
// — a nil missing fails it with [ErrNoReply], as [Call.Fail] does — and when
// fetch returns an error, every call of the batch fails with it, the values
// it found notwithstanding: an error part-way through a batch is not a
// statement about which keys are absent.
//
// keys and found are cleared and reused from one batch to the next, so the
// pipeline allocates nothing of its own per batch once their sizes have
// settled; neither may be kept past the call. A key asked for by several
// callers of one batch is fetched once and answers them all. fetch runs on the
// pipeline's goroutine, one batch at a time, and a panic in it is the
// pipeline's: every caller is released with [ErrPipelinePanicked].
//
//	gw := gateway.New(gateway.Config{Size: 64, Within: 2 * time.Millisecond},
//	    gateway.Lookup(errNotFound, func(ids []int64, found map[int64]Order) error {
//	        orders, err := loadOrders(ids) // one round trip for the batch
//	        for _, o := range orders {
//	            found[o.ID] = o
//	        }
//	        return err
//	    }))
func Lookup[K comparable, V any](missing error, fetch func(keys []K, found map[K]V) error) func(sluice.Stream[*Call[K, V]]) {
	if fetch == nil {
		panic("gateway: Lookup requires a fetch function")
	}
	return func(calls sluice.Stream[*Call[K, V]]) {
		var keys []K
		seen := make(map[K]struct{})
		found := make(map[K]V)
		calls(func(b sluice.Batch[*Call[K, V]]) bool {
			keys = keys[:0]
			clear(seen)
			clear(found)
			for _, c := range b.Items {
				if _, dup := seen[c.In]; !dup {
					seen[c.In] = struct{}{}
					keys = append(keys, c.In)
				}
			}
			err := fetch(keys, found)
			for _, c := range b.Items {
				v, ok := found[c.In]
				switch {
				case err != nil:
					c.Fail(err)
				case !ok:
					c.Fail(missing)
				default:
					c.Reply(v)
				}
			}
			return true
		})
	}
}
