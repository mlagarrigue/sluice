package contract

import (
	"strconv"
	"testing"
)

// STUDY_DB_POOL gets the same answer from both copies of the contract
// (this one and comparison/sluice's): a count from 1 to 4096. This copy
// took anything an int32 holds, so a pool of 5000 started here and was
// refused there.
func TestDBPoolIsBoundAsInTheOtherCopy(t *testing.T) {
	for _, tc := range []struct {
		pool string
		ok   bool
	}{{"1", true}, {"4096", true}, {"5000", false}, {"0", false}, {"-1", false}, {"x", false}} {
		c, err := FromEnv(func(k string) string {
			if k == "STUDY_DB_POOL" {
				return tc.pool
			}
			return ""
		})
		if (err == nil) != tc.ok {
			t.Errorf("STUDY_DB_POOL=%s: error %v, want accepted %v", tc.pool, err, tc.ok)
			continue
		}
		if tc.ok && strconv.Itoa(int(c.Pool)) != tc.pool {
			t.Errorf("STUDY_DB_POOL=%s: pool %d", tc.pool, c.Pool)
		}
	}
}
