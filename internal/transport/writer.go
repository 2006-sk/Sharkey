package transport

import (
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Process-wide write counters: frames queued vs write syscalls issued. Their
// ratio is the coalescing factor, exposed in /stats.
var framesWritten, writeSyscalls atomic.Int64

// WriteStats reports how well frame writes are being coalesced.
type WriteStats struct {
	Frames         int64   `json:"frames"`
	WriteSyscalls  int64   `json:"write_syscalls"`
	FramesPerWrite float64 `json:"frames_per_write"`
}

// ReadWriteStats returns the process-wide coalescing counters.
func ReadWriteStats() WriteStats {
	s := WriteStats{Frames: framesWritten.Load(), WriteSyscalls: writeSyscalls.Load()}
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
type frameWriter struct {
	nc      net.Conn
	timeout time.Duration

	mu       sync.Mutex
	buf      []byte // frames waiting to be written
	spare    []byte // recycled buffer
	flushing bool
	err      error // sticky: once a write fails the connection is dead

	// busy, if set, reports how many requests are outstanding on this
	// connection. When others are outstanding, more frames are about to be
	// written, so the flusher briefly yields to let them join the batch.
	busy func() int32
	// linger is the longest the flusher waits for more frames (0 = never).
	linger time.Duration
}

// lingerMinInFlight: only linger when at least this many requests are
// outstanding on the connection. Below it, waiting costs latency without
// gathering enough frames to pay for itself (measured: see README).
const lingerMinInFlight = 8

// lingerDur is how long the flusher waits for more frames under load. Tuned
// by measurement on macOS loopback (README, "Transport optimisation"): 100µs
// gave the best throughput at 50-250 clients; it never applies to a lone
// request, so single-client latency is unaffected by it.
const lingerDur = 100 * time.Microsecond

// maxFlushRounds bounds how long one caller keeps flushing on behalf of
// others before handing off to a new goroutine, so a caller is never stuck
// writing indefinitely under sustained load.
const maxFlushRounds = 8

// write appends a frame (via appendFrame) and ensures it gets flushed. It
// returns an error only if the connection is already known to be broken or
// this caller's own flush fails; frames flushed by another goroutine report
// failure through the connection being closed.
func (w *frameWriter) write(appendFrame func([]byte) []byte) error {
	w.mu.Lock()
	if w.err != nil {
		err := w.err
		w.mu.Unlock()
		return err
	}
	w.buf = appendFrame(w.buf)
	framesWritten.Add(1)
	if w.flushing {
		w.mu.Unlock()
		return nil
	}
	w.flushing = true
	w.mu.Unlock()
	return w.flush()
}

func (w *frameWriter) flush() error {
	for round := 0; ; round++ {
		w.mu.Lock()
		if len(w.buf) == 0 || w.err != nil {
			w.flushing = false
			err := w.err
			w.mu.Unlock()
			return err
		}
		if round == 0 && w.linger > 0 && w.busy != nil && w.busy() >= lingerMinInFlight {
			// Other requests are in flight on this connection, so more frames
			// are about to be written: wait briefly (yielding the CPU) so they
			// share this syscall.
			w.mu.Unlock()
			for until := time.Now().Add(w.linger); time.Now().Before(until); {
				runtime.Gosched()
			}
			w.mu.Lock()
		}
		if round == maxFlushRounds {
			// Keep flushing=true and let a fresh goroutine continue.
			w.mu.Unlock()
			go w.flush()
			return nil
		}
		out := w.buf
		w.buf = w.spare[:0]
		w.spare = nil
		w.mu.Unlock()

		if w.timeout > 0 {
			_ = w.nc.SetWriteDeadline(time.Now().Add(w.timeout))
		}
		_, err := w.nc.Write(out)
		writeSyscalls.Add(1)

		w.mu.Lock()
		if cap(out) <= 1<<20 { // don't pin huge buffers
			w.spare = out[:0]
		}
		if err != nil {
			w.err = err
			w.buf = nil
			w.flushing = false
			w.mu.Unlock()
			// Closing makes the reader side fail too, so callers whose
			// frames were in this batch learn about it.
			w.nc.Close()
			return err
		}
		w.mu.Unlock()
	}
}
