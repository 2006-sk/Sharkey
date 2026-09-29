// Package clock provides the version generator used to order writes.
package clock

import (
	"sync/atomic"
	"time"
)

// Clock issues strictly increasing 64-bit versions derived from wall-clock
// nanoseconds. If the wall clock stalls or steps backwards, versions keep
// increasing by one, so a single Clock never issues a duplicate or a smaller
// version.
//
// Versions from *different* clocks (e.g. two coordinators) are only ordered
// as well as the machines' clocks are synchronised; this system therefore
// runs a single coordinator (see README, "Known limitations").
type Clock struct {
	last atomic.Uint64
	now  func() uint64
}

// New returns a clock backed by time.Now.
func New() *Clock {
	return &Clock{now: func() uint64 { return uint64(time.Now().UnixNano()) }}
}

// Next returns a version greater than every version previously returned.
func (c *Clock) Next() uint64 {
	for {
		last := c.last.Load()
		v := c.now()
		if v <= last {
			v = last + 1
		}
		if c.last.CompareAndSwap(last, v) {
			return v
		}
	}
}

// Observe advances the clock past v, so versions issued later sort after v.
func (c *Clock) Observe(v uint64) {
	for {
		last := c.last.Load()
		if v <= last || c.last.CompareAndSwap(last, v) {
			return
		}
	}
}
