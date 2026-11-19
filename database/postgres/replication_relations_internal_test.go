package postgres

import (
	"errors"
	"strings"
	"testing"
)

// A publication of partitioned tables describes one relation a partition,
// and can pass 16,384: ReplicationConfig.MaxRelations raises the bound, the
// byte budget with it, and zero keeps the default's refusal.
func TestReplicationMaxRelationsRaisesTheBound(t *testing.T) {
	describe := func(d *replDecoder, n int) error {
		for i := range n {
			if _, _, _, err := d.decode(relMsg(uint32(i), "s", "t", "a"), 0, nil, nil, nil); err != nil { //nolint:gosec // G115: i < n, small
				return err
			}
		}
		return nil
	}
	var byDefault replDecoder
	if err := describe(&byDefault, maxReplicationRelations+1); !errors.Is(err, ErrProtocol) {
		t.Errorf("zero MaxRelations, %d relations: %v, want ErrProtocol", maxReplicationRelations+1, err)
	}
	raised := replDecoder{maxRelations: maxReplicationRelations + 1}
	if err := describe(&raised, maxReplicationRelations+1); err != nil {
		t.Errorf("MaxRelations %d, as many relations: %v", maxReplicationRelations+1, err)
	}
	if err := describe(&raised, maxReplicationRelations+2); !errors.Is(err, ErrProtocol) {
		t.Errorf("one past MaxRelations: %v, want ErrProtocol", err)
	}
	if _, bytes := raised.relationLimits(); bytes <= maxReplicationRelationBytes {
		t.Errorf("a raised count keeps a %d-byte budget, the default's", bytes)
	}

	base := ReplicationConfig{Slot: "s", Publications: []string{"p"}}
	for _, n := range []int{-1, maxReplicationRelationsCeiling + 1} {
		rc := base
		rc.MaxRelations = n
		if _, err := startReplicationSQL(rc); err == nil || !strings.Contains(err.Error(), "MaxRelations") {
			t.Errorf("MaxRelations %d: %v, want a refusal naming it", n, err)
		}
	}
	rc := base
	rc.MaxRelations = maxReplicationRelationsCeiling
	if _, err := startReplicationSQL(rc); err != nil {
		t.Errorf("MaxRelations at its ceiling: %v", err)
	}
}
