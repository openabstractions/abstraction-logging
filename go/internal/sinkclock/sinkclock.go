// Package sinkclock is the clock a retained FileSink stamps its private row
// instants and ages its segments by. Production never sets it; this module's
// tests set it to place rows in time without waiting (LOG-P9). Only *_test.go
// files may call Set; sinkclock_test.go checks the module for that.
package sinkclock

import (
	"sync/atomic"
	"time"
)

var override atomic.Pointer[func() time.Time]

// Current is the current time, or the test clock while one is set.
func Current() time.Time {
	if f := override.Load(); f != nil {
		return (*f)()
	}
	return time.Now()
}

// Set installs a test clock and returns the function that restores the clock
// it replaced. The clock is process-wide: a test that calls Set must not run
// in parallel with other tests in its package.
func Set(now func() time.Time) (restore func()) {
	previous := override.Swap(&now)
	return func() { override.Store(previous) }
}
