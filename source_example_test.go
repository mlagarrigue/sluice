package sluice_test

import (
	"errors"
	"fmt"

	"github.com/mlagarrigue/sluice"
)

func ExampleSource() {
	// A source that fails to open produces no element. Without Err, that is
	// indistinguishable from a file that was simply empty.
	src := sluice.NewSource(sluice.Empty[string](), func() error {
		return errors.New("open orders.csv: no such file")
	})

	for b := range src.Stream() {
		fmt.Println(b.Items)
	}
	if err := src.Err(); err != nil {
		fmt.Println("failed:", err)
	}
	// Output:
	// failed: open orders.csv: no such file
}

func ExampleSource_Collect() {
	src := sluice.NewSource(sluice.Of([]int{1, 2, 3}, 2), nil)

	// Collect consumes the stream and reports the error in one step, so
	// forgetting the check becomes a visible omission.
	values, err := src.Collect()
	fmt.Println(values, err)
	// Output:
	// [1 2 3] <nil>
}

func ExampleSource_Consume() {
	// A teardown failure — a Close that failed after the last element — arrives
	// once the data has already been read. The elements are still valid.
	var closeErr error
	stream := sluice.Stream[int](func(yield func(sluice.Batch[int]) bool) {
		defer func() { closeErr = errors.New("close: disk full") }()
		yield(sluice.Batch[int]{Items: []int{1, 2}})
	})

	src := sluice.NewSource(stream, func() error { return closeErr })
	err := src.Consume(func(b sluice.Batch[int]) bool {
		fmt.Println(b.Items)
		return true
	})
	fmt.Println("err:", err)
	// Output:
	// [1 2]
	// err: close: disk full
}
