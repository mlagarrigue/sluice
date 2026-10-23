package entity

import (
	"time"

	"github.com/mlagarrigue/sluice/database/postgres/pgtype"
)

//go:generate go run github.com/mlagarrigue/sluice/cmd/sluicegen -type Product -in product.go

// Product exercises the mappings Order does not: the other text types, bytes,
// an exact decimal, a time.Time read from a date column, and the other
// spelling of nullable.
type Product struct {
	SKU       string               `db:"sku"`   // char(n)
	Owner     string               `db:"owner"` // name
	Image     []byte               `db:"image"`
	Thumbnail *[]byte              `db:"thumbnail"`
	Price     pgtype.Numeric       `db:"price"`
	Released  time.Time            `db:"released,date"`
	Weight    pgtype.Null[float64] `db:"weight"` // nullable without a pointer
}
