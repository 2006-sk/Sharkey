// Package cmdutil holds small helpers shared by the command binaries.
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
func LimitFlags(fs *flag.FlagSet) *protocol.Limits {
	l := protocol.DefaultLimits()
	fs.IntVar(&l.MaxKeySize, "max-key-size", l.MaxKeySize, "maximum key size in bytes")
	fs.IntVar(&l.MaxValueSize, "max-value-size", l.MaxValueSize, "maximum value size in bytes")
	fs.IntVar(&l.MaxFrameSize, "max-frame-size", l.MaxFrameSize, "maximum wire frame size in bytes")
	return &l
}

// SignalContext returns a context cancelled on SIGINT or SIGTERM.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// RaiseFileLimit raises RLIMIT_NOFILE, logging the outcome.
func RaiseFileLimit(logger *log.Logger) {
	if n, err := transport.RaiseFileLimit(); err != nil {
		logger.Printf("warning: could not raise open-file limit: %v", err)
	} else if n < 4096 {
		logger.Printf("warning: open-file limit is %d; many concurrent clients may fail", n)
	}
}
