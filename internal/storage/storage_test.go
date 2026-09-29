package storage

import (
	"fmt"
	"sync"
	"testing"
)

func TestApplyGet(t *testing.T) {
	m := NewMemory()
	if _, ok := m.Get("missing"); ok {
		t.Fatal("missing key found")
	}
	if ok, _ := m.Apply("k", Entry{Value: []byte("v1"), Version: 1}); !ok {
		t.Fatal("first write not applied")
	}
	e, ok := m.Get("k")
	if !ok || string(e.Value) != "v1" || e.Version != 1 {
		t.Fatalf("got %+v", e)
	}
	if st := m.Stats(); st.Keys != 1 || st.Tombstones != 0 {
		t.Fatalf("stats %+v", st)
	}
}

func TestLastWriterWins(t *testing.T) {
	m := NewMemory()
	m.Apply("k", Entry{Value: []byte("new"), Version: 10})
	// A delayed older write must not overwrite a newer one.
	applied, cur := m.Apply("k", Entry{Value: []byte("old"), Version: 5})
	if applied || string(cur.Value) != "new" {
		t.Fatalf("stale write applied: %v %+v", applied, cur)
	}
	// Same version is an idempotent replay, not a new write.
	if applied, _ := m.Apply("k", Entry{Value: []byte("new"), Version: 10}); applied {
		t.Fatal("replay applied twice")
	}
}

func TestTombstones(t *testing.T) {
	m := NewMemory()
	m.Apply("k", Entry{Value: []byte("v"), Version: 1})
	m.Apply("k", Entry{Tombstone: true, Version: 2})
	e, ok := m.Get("k")
	if !ok || !e.Tombstone || e.Value != nil {
		t.Fatalf("expected tombstone, got %+v", e)
	}
	// An older PUT arriving late must not resurrect the key.
	if applied, _ := m.Apply("k", Entry{Value: []byte("zombie"), Version: 1}); applied {
		t.Fatal("older put resurrected deleted key")
	}
	if st := m.Stats(); st.Keys != 0 || st.Tombstones != 1 {
		t.Fatalf("stats %+v", st)
	}
	// A newer PUT revives it.
	m.Apply("k", Entry{Value: []byte("back"), Version: 3})
	if st := m.Stats(); st.Keys != 1 || st.Tombstones != 0 {
		t.Fatalf("stats after revive %+v", st)
	}
	// Deleting a key never seen still records a tombstone.
	m.Apply("never", Entry{Tombstone: true, Version: 4})
	if st := m.Stats(); st.Tombstones != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestScanVisitsEverythingOnce(t *testing.T) {
	m := NewMemory()
	const n = 5000
	for i := 0; i < n; i++ {
		e := Entry{Value: []byte(fmt.Sprintf("value-%d", i)), Version: uint64(i + 1)}
		if i%10 == 0 {
			e = Entry{Tombstone: true, Version: uint64(i + 1)}
		}
		m.Apply(fmt.Sprintf("key-%d", i), e)
	}
	seen := map[string]bool{}
	var cursor []byte
	pages := 0
	for {
		kvs, next, err := m.Scan(cursor, 4096)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, kv := range kvs {
			if seen[kv.Key] {
				t.Fatalf("key %s returned twice", kv.Key)
			}
			seen[kv.Key] = true
		}
		if next == nil {
			break
		}
		cursor = next
	}
	if len(seen) != n {
		t.Fatalf("scan saw %d keys, want %d", len(seen), n)
	}
	if pages < 10 {
		t.Fatalf("expected pagination, got %d pages", pages)
	}
	if _, _, err := m.Scan([]byte{1}, 10); err == nil {
		t.Fatal("expected error for bad cursor")
	}
}

func TestConcurrentApplyGet(t *testing.T) {
	m := NewMemory()
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				k := fmt.Sprintf("k%d", i%500)
				ver := uint64(i*16 + g + 1)
				if i%4 == 0 {
					m.Apply(k, Entry{Tombstone: true, Version: ver})
				} else {
					m.Apply(k, Entry{Value: []byte("v"), Version: ver})
				}
				m.Get(k)
			}
		}(g)
	}
	wg.Wait()
	st := m.Stats()
	if st.Keys+st.Tombstones != 500 {
		t.Fatalf("counts drifted: %+v", st)
	}
}

// TestScanSingleEntryPages forces a page boundary at every entry, including
// every shard boundary (regression test for a cursor that skipped keys when
// a page filled up on the first key of a new shard).
func TestScanSingleEntryPages(t *testing.T) {
	m := NewMemory()
	for i := 0; i < 300; i++ {
		m.Apply(fmt.Sprintf("k%d", i), Entry{Value: []byte("v"), Version: 1})
	}
	var cursor []byte
	count := 0
	for {
		kvs, next, err := m.Scan(cursor, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(kvs) > 1 {
			t.Fatalf("page has %d entries, want 1", len(kvs))
		}
		count += len(kvs)
		if next == nil {
			break
		}
		cursor = next
	}
	if count != 300 {
		t.Fatalf("scanned %d entries, want 300", count)
	}
}
