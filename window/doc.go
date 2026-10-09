// Package window holds the operators that retain elements for a duration:
// Tumbling groups a time-ordered stream into consecutive panes, and
// CoalesceWithin regroups batches to a size without holding an element past a
// deadline.
//
// Both keep elements between batches and CoalesceWithin runs a goroutine for
// its clock, which is why they live outside the core. The core's Coalesce,
// which regroups by count alone and holds at most one batch, stays there.
package window
