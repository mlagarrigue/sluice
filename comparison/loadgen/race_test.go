//go:build race

package main

// raceSlack is what the race detector adds to the calibration's p50: it
// slows the client and the server of the test alike, about 0.25 ms on a
// developer's machine, and that is not the generator's lateness.
const raceSlack = 0.5
