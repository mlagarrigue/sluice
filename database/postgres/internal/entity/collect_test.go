package entity_test

import (
	"errors"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/database/postgres"
	"github.com/mlagarrigue/sluice/database/postgres/internal/entity"
	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
)

// The collection is the bind-once loop the guide used to spell out: bound on
// the first batch, every batch hydrated, in order, appended to what dst held.
func TestCollectAppendsEveryBatch(t *testing.T) {
	at := time.Date(2024, 5, 4, 10, 0, 0, 0, time.UTC)
	src := scriptedQuery(t, orderFields(), [][][]byte{
		{orderRow(1, 1, at, nil, true, "a"), orderRow(2, 2, at, nil, true, "b")},
		{orderRow(3, 3, at, nil, true, "c"), orderRow(4, 4, at, nil, true, "d")},
		{orderRow(5, 5, at, nil, true, "e")},
	}, false)
	prior := []entity.Order{{ID: 99}}
	got, err := entity.CollectOrder(prior, src)
	if err != nil {
		t.Fatalf("CollectOrder returned %v", err)
	}
	if len(got) != 6 || got[0].ID != 99 {
		t.Fatalf("%d orders, first %d; want the prior one then five", len(got), got[0].ID)
	}
	for i, o := range got[1:] {
		if o.ID != int64(i+1) || o.Label != string(rune('a'+i)) {
			t.Errorf("order %d = %d %q, out of order or misbound", i, o.ID, o.Label)
		}
	}
}

// What the result cannot fit wraps pgtype.ErrCodec, whether the binding or a
// value refused it; the server's own failure does not. On either, dst is
// returned as it stood.
func TestCollectErrors(t *testing.T) {
	at := time.Now()
	wrongType := orderFields()
	for i := range wrongType {
		if wrongType[i].Name == "id" {
			wrongType[i].TypeOID = pgtype.OIDInt4
		}
	}
	nullID := dataRow(pgtype.AppendTimestampTZ(nil, at), pgtype.AppendFloat8(nil, 1), nil, nil, pgtype.AppendBool(nil, true), []byte("x"))
	shortID := dataRow(pgtype.AppendTimestampTZ(nil, at), pgtype.AppendFloat8(nil, 1), []byte{1}, nil, pgtype.AppendBool(nil, true), []byte("x"))
	for _, tc := range []struct {
		name    string
		fields  []postgres.Field
		batches [][][]byte
		fail    bool
		codec   bool
	}{
		{"a column of another type", wrongType, [][][]byte{{orderRow(1, 1, at, nil, true, "a")}}, false, true},
		{"a NULL the field cannot hold", orderFields(), [][][]byte{{orderRow(1, 1, at, nil, true, "a")}, {nullID}}, false, true},
		{"a value that will not decode", orderFields(), [][][]byte{{shortID}}, false, true},
		{"the server's failure", orderFields(), [][][]byte{{orderRow(1, 1, at, nil, true, "a")}}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prior := make([]entity.Order, 1, 8)
			got, err := entity.CollectOrder(prior, scriptedQuery(t, tc.fields, tc.batches, tc.fail))
			if err == nil {
				t.Fatal("no error")
			}
			if errors.Is(err, pgtype.ErrCodec) != tc.codec {
				t.Errorf("errors.Is(%v, pgtype.ErrCodec) = %v, want %v", err, !tc.codec, tc.codec)
			}
			if len(got) != 1 || &got[0] != &prior[0] {
				t.Errorf("dst came back with %d elements, or another array; want it as it stood", len(got))
			}
		})
	}
}

// An empty result leaves dst as it is and binds nothing, since no batch
// describes the columns.
func TestCollectEmptyResult(t *testing.T) {
	got, err := entity.CollectOrder(nil, scriptedQuery(t, orderFields(), [][][]byte{{}}, false))
	if err != nil || len(got) != 0 {
		t.Errorf("%d orders, %v; want none and no error", len(got), err)
	}
}
