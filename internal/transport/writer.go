package transport

import (
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// writer.go: write coalescing, the single biggest performance lever in this
// system.
//
// Why it exists (README, "Where the time goes"): the system is syscall-bound,
// not compute-bound. On the original sequential transport 75% of coordinator
// CPU was spent inside read/write syscalls, and on macOS loopback each
// syscall costs roughly 10-20 µs of CPU. Hashing, map lookups and the LRU did
// not even show up in the profile. So the way to go faster is to make FEWER
// syscalls, and the way to make fewer write syscalls is to put several
// frames into each one: "write coalescing" (a.k.a. batching).
//
// Picture a busy connection without coalescing, where 5 goroutines each have
// a response ready at about the same moment:
//
//	g1: write(f1)  g2: write(f2)  g3: write(f3)  g4: write(f4)  g5: write(f5)
//	=> 5 syscalls, 5 small TCP segments
//
// With coalescing:
//
//	g1: append f1, become flusher -> write(f1)
//	g2..g5: append f2..f5 while g1 is inside write, return immediately
//	g1: loop: buffer not empty -> write(f2 f3 f4 f5)
//	=> 2 syscalls
//
// The frames are independent (each carries its own request ID and length
// prefix), so the receiver's ReadFrame loop splits them apart again no matter
// how they were grouped. Grouping never changes meaning, only cost.
//

// Process-wide write counters: frames queued vs write syscalls issued. Their
// ratio is the coalescing factor, exposed in /stats.
//
// They are atomics because every connection's writer updates them
// concurrently without a shared lock; atomic.Int64 is a lock-free counter
// that is safe for concurrent Add/Load. A ratio of 1.00 means no batching at
// all (README: that is exactly what multiplexing alone produced); a ratio
// above 1 means frames are sharing syscalls. writeSyscalls counts calls to
// net.Conn.Write; Go's runtime may internally split a very large buffer
// into several write(2) calls, so for big frames it is a slight undercount.
var framesWritten, writeSyscalls atomic.Int64

// WriteStats reports how well frame writes are being coalesced.
//
// The json tags define the key names in the /stats document (reported as
// write_coalescing.frames_per_write, per README).
type WriteStats struct {
	// Frames: frames queued; WriteSyscalls: Write calls issued;
	// FramesPerWrite: Frames / WriteSyscalls (0 until the first write).
	Frames         int64   `json:"frames"`
	WriteSyscalls  int64   `json:"write_syscalls"`
	FramesPerWrite float64 `json:"frames_per_write"`
}

// ReadWriteStats returns the process-wide coalescing counters.
//
// The two Loads are not one atomic snapshot (a write may land between them),
// which is fine for a monitoring ratio.
func ReadWriteStats() WriteStats {
	s := WriteStats{Frames: framesWritten.Load(), WriteSyscalls: writeSyscalls.Load()}
	// Guard against division by zero before any write has happened.
	if s.WriteSyscalls > 0 {
		s.FramesPerWrite = float64(s.Frames) / float64(s.WriteSyscalls)
	}
	return s
}

// frameWriter lets many goroutines write frames to one connection while
// coalescing them into as few write syscalls as possible.
//
// There is no dedicated writer goroutine. A caller appends its frame to a
// shared buffer; if no flush is in progress, that caller becomes the flusher
// and writes the whole buffer — including frames other goroutines appended
// meanwhile — in one syscall, looping until the buffer is empty. Callers that
// arrive during a flush just append and return. Under light load every frame
// is written immediately by its own goroutine (no handoff); under heavy load
// dozens of frames share one syscall. On macOS loopback a write costs ~10-20µs,
// so this matters more than any user-space optimisation.
//
// Why not the obvious design, a dedicated writer goroutine fed by a channel?
// Every frame would then need a channel send plus a wake-up of the writer
// goroutine: a goroutine handoff on the latency-critical path, even for a
// lone request on an idle connection. Here, the common idle case costs one
// mutex lock and one Write by the goroutine that already has the frame.
//
// Why not just let every goroutine call nc.Write itself? Go's net.Conn is
// safe for concurrent use and serialises concurrent Writes internally, so
// frames would not get interleaved; but every frame would still cost its own
// syscall, which is exactly the cost we are trying to avoid. Merging frames
// requires a shared buffer, and a shared buffer requires a mutex.
//
// Double buffering: the socket Write happens WITHOUT holding mu, so other
// goroutines can keep appending while a syscall is in progress. To make that
// safe, the flusher takes ownership of the current buffer (`out`) and gives
// the writers a different array (`spare`) to append into. Nobody touches
// `out` while it is being written; afterwards it becomes the next spare.
//
//	              mu held                        mu NOT held
//	append ---> [ buf ] --swap--> out ---------> nc.Write(out)
//	            [spare] <--------------- recycle out[:0] afterwards
//
// Used by both sides: Server (responses) and MuxConn (requests). The plain
// sequential Conn does not use it: it writes directly to the socket, because
// it never has more than one frame to send at a time.
//
// Fields:
//   - nc: the connection frames are written to.
//   - timeout: write deadline applied before each Write (0 = none).
type frameWriter struct {
	nc      net.Conn
	timeout time.Duration

	// mu guards every field in this group (buf, spare, flushing, err). It is
	// never held across the socket Write, only for quick buffer bookkeeping:
	//   - buf: frames appended but not yet handed to Write.
	//   - spare: a previously written buffer, kept (emptied) for reuse so the
	//     steady state allocates no new buffers.
	//   - flushing: true while some goroutine is the designated flusher. This
	//     flag is what guarantees at most one Write is in progress per
	//     connection, so frames go out whole and in append order.
	//   - err: sticky (see below). Once set, every later write fails fast.
	mu       sync.Mutex
	buf      []byte // frames waiting to be written
	spare    []byte // recycled buffer
	flushing bool
	err      error // sticky: once a write fails the connection is dead

	// busy, if set, reports how many requests are outstanding on this
	// connection. When others are outstanding, more frames are about to be
	// written, so the flusher briefly yields to let them join the batch.
	//
	// It is a callback rather than a number so each side can supply its own
	// notion of "busy": the server reports how many handler goroutines are
	// running for this connection (len of its semaphore channel), the client
	// reports how many requests are awaiting responses (MuxConn.inflight).
	busy func() int32
	// linger is the longest the flusher waits for more frames (0 = never).
	// Both Server and MuxConn set it to lingerDur.
	linger time.Duration
}

// lingerMinInFlight: only linger when at least this many requests are
// outstanding on the connection. Below it, waiting costs latency without
// gathering enough frames to pay for itself (measured: see README).
//
// This is the "adaptive" part of the linger, similar in spirit to Nagle's
// algorithm (which delays small TCP sends hoping to combine them) but applied
// only when there is evidence more data is imminent. Nagle delays every
// small write on a connection with unacknowledged data; here a lightly
// loaded connection never waits at all. README: with the >= 8 threshold,
// node-side batching rose to ~2-9 frames per syscall.
const lingerMinInFlight = 8

// lingerDur is how long the flusher waits for more frames under load. Tuned
// by measurement on macOS loopback (README, "Transport optimisation"; see
// also "Where the time goes"): 100µs gave the best throughput at 50-250
// clients; it never applies to a lone request, so single-client latency is
// unaffected by it.
//
// The trade-off: each lingering flush adds up to 100µs to the latency of
// the frames already queued, in exchange for saving one or more ~10-20µs
// syscalls for every frame that joins the batch. Under load, where many
// frames join, that saves far more CPU than it costs in latency.
const lingerDur = 100 * time.Microsecond

// maxFlushRounds bounds how long one caller keeps flushing on behalf of
// others before handing off to a new goroutine, so a caller is never stuck
// writing indefinitely under sustained load.
//
// Without it, a goroutine that happened to become the flusher could be
// trapped in the loop forever while other goroutines keep refilling the
// buffer. That goroutine is usually a request handler (server) or a client
// caller (MuxConn.Do), so it would never get back to returning its own
// result. After maxFlushRounds rounds it hands the job to a fresh goroutine.
const maxFlushRounds = 8

// write appends a frame (via appendFrame) and ensures it gets flushed. It
// returns an error only if the connection is already known to be broken or
// this caller's own flush fails; frames flushed by another goroutine report
// failure through the connection being closed.
//
// appendFrame is a callback (e.g. AppendResponseFrame bound to a response)
// so the frame is encoded DIRECTLY into the shared buffer while mu is held:
// no intermediate per-frame buffer, no extra copy.
//
// Returning nil therefore means "your frame is queued and someone will write
// it", not "your frame has reached the peer". That is acceptable because
// every failure path closes the connection: the peer (or our own reader
// goroutine) notices, and pending requests fail there.
func (w *frameWriter) write(appendFrame func([]byte) []byte) error {
	w.mu.Lock()
	// Fail fast: after a failed write the stream may hold a partial frame, so
	// nothing more can be sent on this connection.
	if w.err != nil {
		err := w.err
		w.mu.Unlock()
		return err
	}
	// Encode the frame onto the end of the pending buffer (under mu, so frames
	// from different goroutines never interleave).
	w.buf = appendFrame(w.buf)
	framesWritten.Add(1)
	// Someone else is flushing and will pick this frame up on its next loop
	// iteration. We return immediately: no waiting, no syscall.
	if w.flushing {
		w.mu.Unlock()
		return nil
	}
	// Nobody is flushing: this goroutine becomes the flusher. Setting the flag
	// under mu makes the election race-free: exactly one goroutine wins.
	w.flushing = true
	w.mu.Unlock()
	// Flush outside the lock (flush re-acquires it as needed).
	return w.flush()
}

// flush writes out buf until it is empty. Only the goroutine that set
// flushing=true (or the goroutine it handed off to) runs it, so at most one
// Write is in progress on the connection at any time.
//
// Each loop iteration ("round"):
//  1. Under mu: if nothing is queued (or the connection is dead), clear
//     flushing and return.
//  2. Round 0 only, under load: linger briefly so more frames can join.
//  3. If this goroutine has done maxFlushRounds rounds, hand off.
//  4. Under mu: swap buf with spare, taking ownership of the frames.
//  5. Without mu: set the write deadline and Write everything in one call.
//  6. Under mu: recycle the written buffer; on error, record it and close.
//
// Checking the "anything left?" condition and clearing `flushing` in the
// same critical section is what prevents a lost frame: a writer that appends
// after we saw an empty buffer must see flushing=false and flush it itself;
// a writer that appended before sees flushing=true, but then we see its frame.
func (w *frameWriter) flush() error {
	for round := 0; ; round++ {
		// Re-check under the lock every round: other goroutines may have appended
		// while we were inside Write.
		w.mu.Lock()
		// Done (buffer drained) or dead (sticky error). Either way, give up the
		// flusher role so a later write can elect a new flusher.
		if len(w.buf) == 0 || w.err != nil {
			w.flushing = false
			err := w.err
			w.mu.Unlock()
			return err
		}
		// Linger only on the first round: later rounds already found frames that
		// arrived during the previous Write, so batching is happening naturally.
		if round == 0 && w.linger > 0 && w.busy != nil && w.busy() >= lingerMinInFlight {
			// Other requests are in flight on this connection, so more frames
			// are about to be written: wait briefly (yielding the CPU) so they
			// share this syscall.
			// Release mu while waiting, or nobody else could append.
			w.mu.Unlock()
			// Busy-wait with runtime.Gosched rather than time.Sleep: Gosched
			// lets other runnable goroutines (the very goroutines about to
			// produce frames) run on this processor, then returns quickly. A
			// sleep of ~100µs would go through the timer machinery, whose
			// granularity is typically coarser than that, and could oversleep
			// well past the intended linger. The cost is some CPU spent
			// spinning, which is why lingering only happens under load.
			for until := time.Now().Add(w.linger); time.Now().Before(until); {
				runtime.Gosched()
			}
			// Retake the lock; buf now holds whatever frames joined during
			// the linger.
			w.mu.Lock()
		}
		// This goroutine has flushed for long enough on others' behalf. Its own
		// frame went out in round 0, so it may return; the remaining work moves to
		// a new goroutine.
		if round == maxFlushRounds {
			// Keep flushing=true and let a fresh goroutine continue. Leaving
			// the flag set (instead of clearing it) means no other writer can
			// start a second, concurrent flush in the gap: ownership passes
			// directly to the new goroutine, which starts again at round 0.
			w.mu.Unlock()
			go w.flush()
			return nil
		}
		// Take ownership of everything queued so far...
		out := w.buf
		// ...and give appenders a different (recycled) array to write into. If
		// spare is nil, w.spare[:0] is also nil and append will allocate.
		w.buf = w.spare[:0]
		// spare is now "lent out" as buf; out will become the next spare.
		w.spare = nil
		// From here until we re-lock, other goroutines append into the new buf in
		// parallel with our syscall.
		w.mu.Unlock()

		// Write deadline: if the peer stops reading, the kernel's send buffer
		// fills and Write blocks. The deadline turns "blocked forever" into a
		// timeout error after w.timeout, which then kills the connection below.
		// It is refreshed per Write, so it bounds each batch, not the whole
		// connection lifetime.
		if w.timeout > 0 {
			_ = w.nc.SetWriteDeadline(time.Now().Add(w.timeout))
		}
		// The one syscall for the whole batch. net.Conn.Write writes everything
		// or returns an error (it loops internally on short writes).
		_, err := w.nc.Write(out)
		// Count it for the frames-per-write ratio in /stats.
		writeSyscalls.Add(1)

		// Back under the lock for bookkeeping.
		w.mu.Lock()
		// Recycle the written buffer as the next spare, so steady state allocates
		// nothing. But not if it has grown past 1 MiB (e.g. after a large value):
		// keeping it would permanently tie up that memory per connection, so we let
		// the GC reclaim it instead.
		if cap(out) <= 1<<20 { // don't pin huge buffers
			w.spare = out[:0]
		}
		// A write error is fatal for the connection. The Write may have sent part
		// of the batch, so the peer could be holding half a frame; nothing sent
		// afterwards could be framed correctly. Record the error (sticky), drop the
		// queued frames (they can never be delivered) and give up the flusher role.
		if err != nil {
			w.err = err
			w.buf = nil
			w.flushing = false
			w.mu.Unlock()
			// Closing makes the reader side fail too, so callers whose
			// frames were in this batch learn about it.
			//
			// Concretely: on the client, MuxConn.readLoop gets an error and
			// fails every pending request; on the server, the client sees the
			// connection drop. Close is called after unlocking, because it
			// may itself be a syscall and we never hold mu across a syscall.
			w.nc.Close()
			return err
		}
		// Success: loop to see whether more frames arrived during the Write.
		w.mu.Unlock()
	}
}
