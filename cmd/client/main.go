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
// Bulk subcommands used by the end-to-end validation script:
//
//	client bulk-put    --count 10000 [--prefix k]   PUT key_i = value(key_i)
//	client bulk-get    --count 10000 [--expect-deleted --every 2]  verify every value (or absence)
//	client bulk-delete --count 10000 [--every 2]    DELETE keys with index % every == 0
//	client verify-replication --count 10000 [--expect-deleted --every 2]  check each key on every replica node
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
	fs := flag.NewFlagSet("client", flag.ExitOnError)
	addr := fs.String("addr", "localhost:7100", "coordinator (or storage node) address")
	timeout := fs.Duration("timeout", 5*time.Second, "request timeout")
	fs.Usage = usage
	fs.Parse(os.Args[1:])
	args := fs.Args()
	if len(args) == 0 {
		usage()
	}
	opts := client.DefaultOptions()
	opts.RequestTimeout = *timeout
	opts.Conns = 4
	c := client.New(*addr, opts)
	defer c.Close()
	ctx := context.Background()

	var err error
	switch cmd, rest := args[0], args[1:]; cmd {
	case "put":
		need(rest, 2)
		val := []byte(rest[1])
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
			os.Stdout.Write(v)
			fmt.Println()
		}
	case "delete", "del":
		need(rest, 1)
		if err = c.Delete(ctx, rest[0]); err == nil {
			fmt.Println("OK")
		}
	case "stats":
		err = printJSON(c.Stats(ctx))
	case "health", "ping":
		err = printJSON(c.Ping(ctx))
	case "locate":
		need(rest, 1)
		err = printJSON(c.Locate(ctx, rest[0]))
	case "bulk-put", "bulk-get", "bulk-delete":
		err = bulk(ctx, c, cmd, rest)
	case "verify-replication":
		err = verifyReplication(ctx, c, rest, opts)
	default:
		usage()
	}
	if err != nil {
		fail(err)
	}
}

func need(args []string, n int) {
	if len(args) < n {
		usage()
	}
}

func fail(err error) {
	if errors.Is(err, client.ErrNotFound) {
		fmt.Fprintln(os.Stderr, "(not found)")
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func printJSON(raw json.RawMessage, err error) error {
	if err != nil {
		return err
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return err
	}
	out, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(out))
	return nil
}

func key(prefix string, i int) string { return fmt.Sprintf("%s%06d", prefix, i) }

// expectedValue is deterministic so bulk-get can verify without state.
func expectedValue(k string) string { return "value-of-" + k }

type bulkFlags struct {
	count, parallel, every int
	prefix                 string
	missing                bool
}

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
func forEach(count, parallel int, fn func(i int) error) int64 {
	var next, failures atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < parallel; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= count {
					return
				}
				if err := fn(i); err != nil {
					if failures.Add(1) <= 10 {
						fmt.Fprintln(os.Stderr, "  FAIL:", err)
					}
				}
			}
		}()
	}
	wg.Wait()
	return failures.Load()
}

func bulk(ctx context.Context, c *client.Client, cmd string, args []string) error {
	b := parseBulk(cmd, args)
	start := time.Now()
	var fn func(i int) error
	switch cmd {
	case "bulk-put":
		fn = func(i int) error {
			k := key(b.prefix, i)
			return c.Put(ctx, k, []byte(expectedValue(k)))
		}
	case "bulk-delete":
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
			if b.missing && i%b.every == 0 {
				if !errors.Is(err, client.ErrNotFound) {
					return fmt.Errorf("%s: expected deleted, got %q (err %v)", k, v, err)
				}
				return nil
			}
			if err != nil {
				return fmt.Errorf("%s: %w", k, err)
			}
			if string(v) != expectedValue(k) {
				return fmt.Errorf("%s: got %q want %q", k, v, expectedValue(k))
			}
			return nil
		}
	}
	failures := forEach(b.count, b.parallel, fn)
	fmt.Printf("%s: %d keys, %d failures, %v\n", cmd, b.count, failures, time.Since(start).Round(time.Millisecond))
	if failures > 0 {
		return fmt.Errorf("%s: %d failures", cmd, failures)
	}
	return nil
}

// verifyReplication asks the coordinator where each key lives and reads it
// directly from every replica, checking each copy is present and correct.
func verifyReplication(ctx context.Context, c *client.Client, args []string, opts client.Options) error {
	b := parseBulk("verify-replication", args)
	var mu sync.Mutex
	nodes := map[string]*client.Client{}
	nodeClient := func(addr string) *client.Client {
		mu.Lock()
		defer mu.Unlock()
		if nc, ok := nodes[addr]; ok {
			return nc
		}
		nc := client.New(addr, opts)
		nodes[addr] = nc
		return nc
	}
	var copies atomic.Int64
	failures := forEach(b.count, b.parallel, func(i int) error {
		k := key(b.prefix, i)
		raw, err := c.Locate(ctx, k)
		if err != nil {
			return err
		}
		var loc coordinator.Location
		if err := json.Unmarshal(raw, &loc); err != nil {
			return err
		}
		for _, r := range loc.Replicas {
			v, err := nodeClient(r.Addr).Get(ctx, k)
			if b.missing && i%b.every == 0 {
				if !errors.Is(err, client.ErrNotFound) {
					return fmt.Errorf("%s on %s (%s, %s): expected deleted, got %q err %v", k, r.Addr, r.Role, r.State, v, err)
				}
				copies.Add(1)
				continue
			}
			if err != nil || string(v) != expectedValue(k) {
				return fmt.Errorf("%s on %s (%s, %s): got %q err %v", k, r.Addr, r.Role, r.State, v, err)
			}
			copies.Add(1)
		}
		return nil
	})
	for _, nc := range nodes {
		nc.Close()
	}
	fmt.Printf("verify-replication: %d keys, %d replica copies verified, %d failures\n", b.count, copies.Load(), failures)
	if failures > 0 {
		return fmt.Errorf("verify-replication: %d failures", failures)
	}
	return nil
}
