// Command client is a CLI for the key-value store.
//
//	client [--addr localhost:7100] put <key> <value>
//	client get <key>
//	client delete <key>
//	client stats | health | locate <key>
//
// Point --addr at a storage node (e.g. localhost:7101) to inspect that node
// directly, bypassing the coordinator.
//
// That works because the coordinator and the storage nodes speak the SAME
// binary protocol for GET/PUT/DELETE/STATS/PING. Reading a node directly is
// how the validation script proves replication: it asks the coordinator where
// a key should live, then checks each of those nodes on its own.
//
// Bulk subcommands used by the end-to-end validation script:
//
//	client bulk-put    --count 10000 [--prefix k]   PUT key_i = value(key_i)
//	client bulk-get    --count 10000 [--expect-deleted --every 2]  verify every value (or absence)
//	client bulk-delete --count 10000 [--every 2]    DELETE keys with index % every == 0
//	client verify-replication --count 10000 [--expect-deleted --every 2]  check each key on every replica node
//
// CLI design, in brief:
//   - Global flags (--addr, --timeout) come BEFORE the sub-command name.
//     Go's flag package stops parsing at the first non-flag argument, so
//     fs.Args() then starts with the sub-command and its own arguments.
//   - Sub-commands with options (the bulk ones) parse the remaining
//     arguments with their own FlagSet, the pattern `go` and `git` use.
//   - Exit status is the scripting contract: 0 success, 1 failure (including
//     "not found"), 2 usage error. scripts/validate.sh relies on it.
//   - Bulk values are a pure function of the key (expectedValue), so a later
//     invocation can verify data written by an earlier one without any state
//     file shared between them.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"distkv/internal/client"
	"distkv/internal/coordinator"
)

// usage prints the command summary to stderr (never stdout, so it cannot be
// mistaken for data by a script) and exits with status 2, the conventional
// Unix code for "command used incorrectly".
func usage() {
	fmt.Fprint(os.Stderr, `usage: client [--addr host:port] <command> [args]

commands:
  put <key> <value|->        store a value ("-" reads the value from stdin)
  get <key>                  print the value
  delete <key>               delete a key
  stats                      print the stats document (JSON)
  health                     print the health document (JSON)
  locate <key>               print the key's replica set (coordinator only)
  bulk-put | bulk-get | bulk-delete | verify-replication   (see --help of each)
`)
	os.Exit(2)
}

func main() {
	// Global flags. ExitOnError: a bad flag prints usage and exits 2.
	fs := flag.NewFlagSet("client", flag.ExitOnError)
	// Target server: the coordinator by default, or a storage node.
	addr := fs.String("addr", "localhost:7100", "coordinator (or storage node) address")
	// Per-request timeout, so a hung server cannot hang the CLI forever.
	timeout := fs.Duration("timeout", 5*time.Second, "request timeout")
	// Replace the auto-generated flag listing with our command summary.
	fs.Usage = usage
	fs.Parse(os.Args[1:])
	// Everything after the global flags: the sub-command and its arguments.
	args := fs.Args()
	if len(args) == 0 {
		usage()
	}
	opts := client.DefaultOptions()
	opts.RequestTimeout = *timeout
	// Four multiplexed connections: enough for the bulk commands' 32
	// concurrent requests to spread across sockets. Single commands only
	// use one. (opts is also reused for the per-node clients in
	// verify-replication.)
	opts.Conns = 4
	// The client dials lazily and pools connections; Close releases them.
	c := client.New(*addr, opts)
	defer c.Close()
	// No cancellation needed for a short-lived CLI: the per-request timeout
	// in opts bounds every call.
	ctx := context.Background()

	var err error
	// Go's switch with an init statement: cmd is the sub-command and rest its
	// arguments, both scoped to the switch.
	switch cmd, rest := args[0], args[1:]; cmd {
	case "put":
		need(rest, 2)
		val := []byte(rest[1])
		// "-" as the value means "read it from stdin", the Unix convention.
		// It allows binary values and values too large for a command line,
		// e.g. `client put img - < photo.png`.
		if rest[1] == "-" {
			if val, err = io.ReadAll(os.Stdin); err != nil {
				fail(err)
			}
		}
		err = c.Put(ctx, rest[0], val)
		if err == nil {
			fmt.Println("OK")
		}
	case "get":
		need(rest, 1)
		var v []byte
		if v, err = c.Get(ctx, rest[0]); err == nil {
			// Write the raw bytes (values may be binary; Println would add
			// formatting), then a newline for a tidy terminal.
			os.Stdout.Write(v)
			fmt.Println()
		}
	case "delete", "del":
		// "del" is a short alias.
		need(rest, 1)
		if err = c.Delete(ctx, rest[0]); err == nil {
			fmt.Println("OK")
		}
	case "stats":
		// Stats returns (json, error); Go lets a multi-value call be passed
		// directly as the arguments of printJSON.
		err = printJSON(c.Stats(ctx))
	case "health", "ping":
		err = printJSON(c.Ping(ctx))
	case "locate":
		// Only the coordinator knows the ring, so this fails against a node.
		need(rest, 1)
		err = printJSON(c.Locate(ctx, rest[0]))
	case "bulk-put", "bulk-get", "bulk-delete":
		err = bulk(ctx, c, cmd, rest)
	case "verify-replication":
		err = verifyReplication(ctx, c, rest, opts)
	default:
		// Unknown sub-command: usage error (exit 2).
		usage()
	}
	// A single error exit for every command keeps exit codes consistent.
	if err != nil {
		fail(err)
	}
}

// need exits with usage unless at least n positional arguments were given.
func need(args []string, n int) {
	if len(args) < n {
		usage()
	}
}

// fail reports err on stderr and exits 1. A missing key is printed as a
// friendly "(not found)" rather than a Go error string, but it still exits 1
// so a script can test for existence with `if client get k; then ...`.
func fail(err error) {
	// errors.Is also matches ErrNotFound when it is wrapped by another error.
	if errors.Is(err, client.ErrNotFound) {
		fmt.Fprintln(os.Stderr, "(not found)")
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

// printJSON pretty-prints a raw JSON document. The server sends compact JSON;
// decoding into `any` and re-encoding with indentation makes it readable
// without having to know its schema here.
func printJSON(raw json.RawMessage, err error) error {
	if err != nil {
		return err
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return err
	}
	// Re-encoding a value just decoded from JSON cannot fail, so the error
	// is ignored.
	out, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(out))
	return nil
}

// key builds the i-th bulk key. Zero-padding to 6 digits gives fixed-width
// names (key-000042) that sort in numeric order in listings and logs.
func key(prefix string, i int) string { return fmt.Sprintf("%s%06d", prefix, i) }

// expectedValue is deterministic so bulk-get can verify without state.
//
// Deriving the value from the key means that a separate process, possibly
// run minutes later after a node crash and restart, knows exactly what every
// key must contain. It also catches cross-key corruption: if key A ever
// returned key B's value, the comparison would fail.
func expectedValue(k string) string { return "value-of-" + k }

// bulkFlags holds the options shared by all bulk sub-commands.
//
//	count     keys 0..count-1 are processed
//	parallel  number of worker goroutines issuing requests concurrently
//	every     keys with i % every == 0 form the "deleted" subset
//	prefix    key name prefix, so several data sets can coexist
//	missing   set by --expect-deleted: the i%every==0 subset must be absent
type bulkFlags struct {
	count, parallel, every int
	prefix                 string
	missing                bool
}

// parseBulk parses a bulk sub-command's own flags with a dedicated FlagSet
// named after the sub-command, so `client bulk-get -h` shows only the flags
// relevant to it.
func parseBulk(name string, args []string) bulkFlags {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	var b bulkFlags
	fs.IntVar(&b.count, "count", 10000, "number of keys")
	fs.IntVar(&b.parallel, "parallel", 32, "concurrent requests")
	fs.IntVar(&b.every, "every", 1, "keys with index % every == 0 are deleted (bulk-delete) or expected missing (bulk-get --expect-deleted)")
	fs.StringVar(&b.prefix, "prefix", "key-", "key prefix")
	fs.BoolVar(&b.missing, "expect-deleted", false, "bulk-get: expect keys with i%every==0 to be missing")
	fs.Parse(args)
	return b
}

// forEach runs fn(i) for i in [0,count) with bounded parallelism and returns
// the number of failures (printing up to 10 of them).
//
// This is a worker pool without a channel: `parallel` goroutines share an
// atomic counter and each claims the next index with next.Add(1). Every index
// is handed out exactly once (the atomic increment cannot give two workers
// the same value), and fast workers naturally take more items than slow ones.
// Bounding the concurrency matters: 10,000 goroutines all firing at once
// would only queue up inside the client and could trip request timeouts.
// Printing at most 10 failures keeps a systematic problem (for example a
// node down) from producing 10,000 identical lines.
func forEach(count, parallel int, fn func(i int) error) int64 {
	var next, failures atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < parallel; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				// Add returns the NEW value, so subtract 1 to get the index
				// this worker just claimed (0, 1, 2, ...).
				i := int(next.Add(1) - 1)
				// All indexes handed out: this worker is done.
				if i >= count {
					return
				}
				if err := fn(i); err != nil {
					// Count every failure, but print only the first 10.
					if failures.Add(1) <= 10 {
						fmt.Fprintln(os.Stderr, "  FAIL:", err)
					}
				}
			}
		}()
	}
	// Wait for every worker to drain the index space.
	wg.Wait()
	return failures.Load()
}

// bulk implements bulk-put, bulk-delete and bulk-get. Each builds a per-key
// function fn(i) and hands it to forEach; the three differ only in fn. The
// validation script chains them: bulk-put N keys, bulk-get them, bulk-delete
// every 2nd, bulk-get --expect-deleted, kill a node, repeat the checks.
func bulk(ctx context.Context, c *client.Client, cmd string, args []string) error {
	b := parseBulk(cmd, args)
	// Wall-clock start, for the summary line.
	start := time.Now()
	var fn func(i int) error
	switch cmd {
	case "bulk-put":
		// Write key_i = expectedValue(key_i).
		fn = func(i int) error {
			k := key(b.prefix, i)
			return c.Put(ctx, k, []byte(expectedValue(k)))
		}
	case "bulk-delete":
		// Delete only the subset i % every == 0 (every=2: the even keys).
		// The other keys are skipped and count as successes.
		fn = func(i int) error {
			if i%b.every != 0 {
				return nil
			}
			return c.Delete(ctx, key(b.prefix, i))
		}
	case "bulk-get":
		fn = func(i int) error {
			k := key(b.prefix, i)
			v, err := c.Get(ctx, k)
			// With --expect-deleted, the deleted subset must come back as
			// NOT FOUND. Any value, or any other error (e.g. a timeout), is a
			// failure: a resurrected deleted key is exactly the bug this
			// check hunts for.
			if b.missing && i%b.every == 0 {
				if !errors.Is(err, client.ErrNotFound) {
					return fmt.Errorf("%s: expected deleted, got %q (err %v)", k, v, err)
				}
				return nil
			}
			// Every other key must be readable...
			if err != nil {
				return fmt.Errorf("%s: %w", k, err)
			}
			// ...and hold exactly the value its name implies.
			if string(v) != expectedValue(k) {
				return fmt.Errorf("%s: got %q want %q", k, v, expectedValue(k))
			}
			return nil
		}
	}
	failures := forEach(b.count, b.parallel, fn)
	// One summary line on stdout; scripts capture it into the saved logs.
	fmt.Printf("%s: %d keys, %d failures, %v\n", cmd, b.count, failures, time.Since(start).Round(time.Millisecond))
	// Any failure makes the command exit 1, which fails the calling script.
	if failures > 0 {
		return fmt.Errorf("%s: %d failures", cmd, failures)
	}
	return nil
}

// verifyReplication asks the coordinator where each key lives and reads it
// directly from every replica, checking each copy is present and correct.
//
// A normal GET through the coordinator cannot prove replication: it succeeds
// if ANY replica answers. This command goes around the coordinator. For each
// key it asks /locate-style "which N nodes own this key?" and then GETs the
// key from each of those nodes individually. It passes only if every owner
// holds the right value (or, for the deleted subset, every owner reports NOT
// FOUND, which proves the delete propagated as a tombstone to all replicas).
// After a crash and restart it also proves resync refilled the node.
//
// Caveat from the README: the addresses in the locate answer are the ones the
// coordinator uses. Under docker-compose those are container names (node1:7101)
// that the host cannot resolve, so this only works with the local cluster.
func verifyReplication(ctx context.Context, c *client.Client, args []string, opts client.Options) error {
	b := parseBulk("verify-replication", args)
	// One client per storage node address, created on first use and shared
	// by all workers. The mutex protects the map, because 32 workers may
	// ask for the same node at the same moment; Go maps are not safe for
	// concurrent writes.
	var mu sync.Mutex
	nodes := map[string]*client.Client{}
	nodeClient := func(addr string) *client.Client {
		mu.Lock()
		defer mu.Unlock()
		// Reuse the existing client (and its pooled connections).
		if nc, ok := nodes[addr]; ok {
			return nc
		}
		// First request to this node: create its client (connections are
		// dialled lazily by the pool).
		nc := client.New(addr, opts)
		nodes[addr] = nc
		return nc
	}
	// Total replica copies checked across all keys (keys x N when all pass).
	var copies atomic.Int64
	failures := forEach(b.count, b.parallel, func(i int) error {
		k := key(b.prefix, i)
		// Ask the coordinator for the key's replica set: address, role
		// (primary/replica) and current health state of each owner.
		raw, err := c.Locate(ctx, k)
		if err != nil {
			return err
		}
		var loc coordinator.Location
		if err := json.Unmarshal(raw, &loc); err != nil {
			return err
		}
		// Check every owner. The first bad copy fails this key (return), and
		// the error names the node, its role and state for debugging.
		for _, r := range loc.Replicas {
			// A direct GET to the storage node, bypassing the coordinator.
			v, err := nodeClient(r.Addr).Get(ctx, k)
			// Deleted subset: this node must say NOT FOUND.
			if b.missing && i%b.every == 0 {
				if !errors.Is(err, client.ErrNotFound) {
					return fmt.Errorf("%s on %s (%s, %s): expected deleted, got %q err %v", k, r.Addr, r.Role, r.State, v, err)
				}
				copies.Add(1)
				continue
			}
			// Live key: this node must hold the exact expected value.
			if err != nil || string(v) != expectedValue(k) {
				return fmt.Errorf("%s on %s (%s, %s): got %q err %v", k, r.Addr, r.Role, r.State, v, err)
			}
			copies.Add(1)
		}
		return nil
	})
	// forEach has returned, so no worker touches the map any more and it is
	// safe to iterate without the mutex.
	for _, nc := range nodes {
		nc.Close()
	}
	fmt.Printf("verify-replication: %d keys, %d replica copies verified, %d failures\n", b.count, copies.Load(), failures)
	if failures > 0 {
		return fmt.Errorf("verify-replication: %d failures", failures)
	}
	return nil
}
