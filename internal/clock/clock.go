// Package clock provides the version generator used to order writes.
//
// Last-writer-wins (see package storage) needs every write to carry a
// version such that "later write" means "larger version". Plain wall-clock
// time is close but not enough: two writes in the same nanosecond (or on a
// coarse clock) would tie, and NTP can step the clock backwards, making a
// later write look older and silently lose. This Clock is a hybrid: it uses
// wall-clock nanoseconds when they move forward, and a counter (+1) when
// they do not, so its output is strictly increasing no matter what the
// wall clock does, and still close to real time, which is handy when
// reading versions in logs.
//
//	wall clock:  1000  1000  1000   995 (stepped back)  1003
//	version:     1000  1001  1002  1003                1004
//
// (1003 at the last step: the wall clock says 1003, but that is not greater
// than the last version, 1003, so the counter continues.)
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
//
// Fields:
//
//   - last is the most recent version issued or observed. It is an atomic,
//     not a mutex-protected field, so Next is lock-free: many goroutines
//     can issue versions concurrently with a compare-and-swap loop.
//   - now is the time source, a field (not a direct time.Now call) so tests
//     can inject a frozen clock (see TestStalledClock).
//
// The zero value has a nil now and is not usable; construct with New.
type Clock struct {
	last atomic.Uint64
	now  func() uint64
}

// New returns a clock backed by time.Now.
//
// UnixNano is an int64 of nanoseconds since 1970; converting to uint64 is
// safe for any date after 1970. Go's time.Now also carries a monotonic
// reading, but UnixNano uses the wall clock, which can jump; Next is what
// papers over such jumps.
func New() *Clock {
	return &Clock{now: func() uint64 { return uint64(time.Now().UnixNano()) }}
}

// Next returns a version greater than every version previously returned.
//
// How the CAS (compare-and-swap) loop works, and why it is needed:
//
//  1. Read last: a snapshot of the shared state.
//  2. Compute the proposal v = max(now, last+1) from that snapshot.
//  3. CompareAndSwap(last, v): atomically "if c.last still equals the
//     snapshot, set it to v". On success nobody else issued a version in
//     between, so v is ours. On failure another goroutine moved c.last and
//     our proposal may be stale (it might equal the version they just
//     got), so go back to step 1.
//
// A plain "Load, compute, Store" would be racy: two goroutines could both
// load 1002, both compute 1003, and both return 1003, a duplicate version,
// which under last-writer-wins lets one of two different writes silently
// vanish. CAS makes step 1..3 behave like one atomic step. It is lock-free:
// a failed CAS means some other goroutine succeeded, so the system as a
// whole always makes progress, and no goroutine ever blocks holding a lock.
// Under light contention the loop almost always succeeds first time.
func (c *Clock) Next() uint64 {
	for {
		// Snapshot the latest issued/observed version.
		last := c.last.Load()
		// Prefer real time...
		v := c.now()
		// ...unless it has not advanced past last (same nanosecond, a coarse
		// clock, or the clock stepped back). Then fall back to a logical
		// increment, which guarantees strict increase.
		if v <= last {
			v = last + 1
		}
		// Publish v only if nobody changed last since our Load. If this
		// fails, retry from the top with a fresh snapshot.
		if c.last.CompareAndSwap(last, v) {
			return v
		}
	}
}

// Observe advances the clock past v, so versions issued later sort after v.
//
// A node calls this with the version of every write it applies. That
// matters for writes the node versions itself (a client talking to the node
// directly): without Observe, a node whose wall clock is behind the
// coordinator's could issue a version smaller than one it has already
// stored, and last-writer-wins would reject the newer write as stale. This
// is the "hybrid" part of a hybrid logical clock: remote versions pull the
// local clock forward, never backward.
//
// Note it sets last to v itself (not v+1): the next Next will return at
// least v+1 anyway, because it requires v > last.
func (c *Clock) Observe(v uint64) {
	for {
		last := c.last.Load()
		// Done if we are already at or past v (never move backwards), or if
		// we successfully raised last to v. If the CAS fails, another
		// goroutine changed last concurrently; reload and re-check, since
		// they may have moved it past v already.
		if v <= last || c.last.CompareAndSwap(last, v) {
			return
		}
	}
}
