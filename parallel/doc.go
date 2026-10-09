// Package parallel holds the operators that run goroutines: Ordered and
// Unordered spread batches over a pool of workers, WithState gives each
// worker a private state built once, and Async runs the upstream on its own
// goroutine so that reading overlaps what follows.
//
// Every operator here owns the goroutines it starts and stops them before it
// returns, on every path out including a panic, so no goroutine outlives the
// call. The core stays single-goroutine by construction; concurrency is
// something a pipeline opts into, here, one stage at a time.
package parallel
