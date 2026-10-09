// Package entity holds the fixture the generator is exercised against.
//
// The generated file beside this one is committed on purpose: generated code
// that is not in the repository is code nobody reviews and nothing compiles
// until it is too late. A test regenerates it and compares, so drift between
// the generator and its output fails the build rather than surfacing on
// somebody's machine.
package entity

import "time"

//go:generate go run github.com/mlagarrigue/sluice/cmd/sluicegen -type Order -in order.go

// Order exercises every shape the generator has to handle: the plain types, a
// renamed column, a nullable one, and a field that is not a column at all.
type Order struct {
	ID        int64     `db:"id"`
	Total     float64   `db:"total_amount"`
	Label     string    // no tag: the column is label, by convention
	Active    bool      `db:"is_active"`
	CreatedAt time.Time `db:"created_at"`
	Note      *string   `db:"note"` // nullable: a pointer is how NULL is expressible
	//lint:ignore U1000 unexported, so not part of the row: the generator must skip it
	internal int
	Computed int64 `db:"-"` // opted out explicitly
}
