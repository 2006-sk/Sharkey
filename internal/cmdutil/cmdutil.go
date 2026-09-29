// Package cmdutil holds small helpers shared by the command binaries.
//
// The four binaries under cmd/ (node, coordinator, client, benchmark) each
// have their own main function, but some setup is identical in several of
// them: registering the protocol size-limit flags, turning Ctrl-C / SIGTERM
// into a cancelled context for graceful shutdown, and raising the process's
// open-file limit so that thousands of TCP connections can be open at once.
// Keeping that code here means the binaries cannot drift apart (for example,
// a node accepting 1 MiB values while the coordinator only accepts 64 KiB).
//
// Everything in this package is deliberately tiny and free of policy: it
// returns values and logs warnings, and each main decides what to do next.
package cmdutil

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"distkv/internal/protocol"
	"distkv/internal/transport"
)

// LimitFlags registers --max-key-size / --max-value-size / --max-frame-size.
//
// It starts from protocol.DefaultLimits() so the flag defaults and the
// library defaults are the same numbers (the README lists 1 KiB keys and
// 1 MiB values). The flag package writes parsed values straight into the
// fields of l through the pointers passed to IntVar, so the returned
// *protocol.Limits only holds the user's values AFTER the caller has run
// fs.Parse. Callers therefore keep the pointer and dereference it later
// (cfg.Server.Limits = *limits), never copying the struct before parsing.
//
// Why a FlagSet parameter instead of the global flag.CommandLine? Each
// binary builds its own FlagSet (with its own name for -h output), and the
// benchmark binary even has sub-commands with separate FlagSets. Passing the
// set in lets one helper serve all of them.
func LimitFlags(fs *flag.FlagSet) *protocol.Limits {
	// A local copy of the defaults; its address escapes to the heap and is
	// returned, which is fine in Go (escape analysis handles it).
	l := protocol.DefaultLimits()
	// Largest key accepted; oversized keys are rejected before any work.
	fs.IntVar(&l.MaxKeySize, "max-key-size", l.MaxKeySize, "maximum key size in bytes")
	// Largest value accepted in a PUT.
	fs.IntVar(&l.MaxValueSize, "max-value-size", l.MaxValueSize, "maximum value size in bytes")
	// Largest length-prefixed frame read off the wire. This protects the
	// reader from a bogus length prefix asking it to allocate gigabytes.
	fs.IntVar(&l.MaxFrameSize, "max-frame-size", l.MaxFrameSize, "maximum wire frame size in bytes")
	return &l
}

// SignalContext returns a context cancelled on SIGINT or SIGTERM.
//
// This is the standard Go graceful-shutdown pattern. SIGINT is what Ctrl-C
// sends in a terminal; SIGTERM is what `kill <pid>`, `docker stop` and most
// process managers send when they politely ask a program to exit. Without
// this, the Go runtime's default action for both signals is to terminate the
// process immediately, so in-flight requests would be cut off mid-response
// and clients would see connection resets.
//
// signal.NotifyContext installs a handler and returns a context whose Done
// channel closes when the first such signal arrives. main blocks on
// ctx.Done(), then calls Shutdown with its own timeout. The returned
// CancelFunc (usually named `stop`) un-registers the handler; once it has
// run, a SECOND Ctrl-C falls back to the default behaviour and kills the
// process at once, which is the escape hatch if shutdown ever hangs.
//
// SIGKILL (kill -9) and SIGSTOP cannot be caught by any program; the
// failure benchmarks use exactly those to simulate crashes and hangs that
// get no chance to clean up.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// RaiseFileLimit raises RLIMIT_NOFILE, logging the outcome.
//
// Background: on Unix every open TCP connection is a file descriptor, and
// each process has a soft limit on how many descriptors it may hold
// (RLIMIT_NOFILE, the value `ulimit -n` shows). macOS shells commonly
// default the soft limit to 256, far below what this system needs: the
// coordinator holds one connection per benchmark client (up to 250 in Test C)
// plus connections to every storage node, and the benchmark process holds
// one per simulated client. When the limit is hit, accept()/dial() fail with
// "too many open files" and the benchmark would report errors that are an
// artefact of the OS configuration, not of the system under test.
//
// A process may raise its own soft limit up to its hard limit without
// privileges. transport.RaiseFileLimit does that, capping the target at
// 65536 because macOS rejects values above kern.maxfilesperproc even when
// the hard limit reads as "unlimited". It returns the resulting soft limit.
//
// Failure here is only a warning, not fatal: a small dev cluster works fine
// with a low limit, so refusing to start would be unhelpful. The 4096
// threshold matches the default --max-conns of the node and coordinator.
func RaiseFileLimit(logger *log.Logger) {
	// Case 1: the syscall failed (e.g. a restricted container). Warn.
	// Case 2: it worked but the resulting limit is still small, which means
	// the hard limit itself is low; warn that heavy load may fail.
	// Otherwise stay silent: the common case needs no log line.
	if n, err := transport.RaiseFileLimit(); err != nil {
		logger.Printf("warning: could not raise open-file limit: %v", err)
	} else if n < 4096 {
		logger.Printf("warning: open-file limit is %d; many concurrent clients may fail", n)
	}
}
