// Package bench is the measurement harness: the performance ceiling of Go 1.26
// iteration primitives, and the per-operator figures every other claim in the
// repository is expressed against (see docs/benchmarks.md).
//
// The package holds only benchmarks and the tests that guard them; this file
// exists so the package has documentation outside its test files. The step-0
// questions it answers:
//  1. What is the ceiling? (native loop = denominator of every measurement)
//  2. How much does an operator stage cost, and is the cost linear?
//  3. How much does iter.Pull really cost per value?
//  4. Does all-batch keep its promise against tuple-at-a-time?
//
// Absolute nanoseconds do not transfer between machines or even between runs on
// a laptop, so comparisons go through the paired A/B scripts in scripts/
// (ab.sh, compare.sh, versus.sh, and gate.sh for CI), which take the ratio
// within each round and apply a sign test.
package bench
