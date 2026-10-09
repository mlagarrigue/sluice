// Package join holds the operators that materialise one side of their input:
// a hash join keeps its build side in a bounded table, a merge join and an
// interval join walk two ordered inputs with a bounded window of each, and
// ZipLongest pairs two streams by position.
//
// Each of them retains elements between batches, which is why they live here
// and not in the core: the core's operators hold nothing beyond the batch in
// flight. The cost of each is stated in its documentation, and the bound that
// keeps it finite — BuildLimit, or the input's ordering — is a required
// argument rather than an option.
package join
