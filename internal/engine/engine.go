package engine

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Mayanshh/Peregrine-KV/internal/memtable"
	"github.com/Mayanshh/Peregrine-KV/internal/sstable"
	"github.com/Mayanshh/Peregrine-KV/internal/wal"
)

// Engine is the central hub of the LSM tree database.
type Engine struct {
	mu       sync.RWMutex
	opts     Options
	memtable *memtable.SkipList
	wal      *wal.WAL

	// Background flushing concurrency channels
	flushCh chan struct{}
	compactCh chan struct{}
	wg      sync.WaitGroup
	closing bool
}

var tombstoneValue = []byte("__PEREGRINE_TOMBSTONE__")

func (e *Engine) DataDir() string {
	return e.opts.DataDir
}

// Open initializes the engine, mounts the WAL, and starts background workers.
func Open(opts Options) (*Engine, error) {
	if err := os.MkdirAll(opts.DataDir, 0755); err != nil {
		return nil, err
	}

	walPath := filepath.Join(opts.DataDir, "active.wal")
	w, err := wal.New(walPath, opts.WALSync)
	if err != nil {
		return nil, err
	}

	e := &Engine{
		opts:     opts,
		memtable: memtable.NewSkipList(),
		wal:      w,
		flushCh:  make(chan struct{}, 1),
		compactCh: make(chan struct{}, 1),
	}

	// Spin up the background I/O worker
	e.wg.Add(1)
	go e.flusher()
	e.wg.Add(1)
	go e.compactor()

	return e, nil
}

func IsTombstone(val []byte) bool {
	return bytes.Equal(val, tombstoneValue)
}

// Put writes a key-value pair to the database.
func (e *Engine) Put(key, val []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 1. Durability: Write to WAL
	if err := e.wal.Write(key, val); err != nil {
		return err
	}
	if e.opts.WALSync {
		e.wal.Sync()
	}

	// 2. In-Memory: Write to SkipList
	e.memtable.Put(key, val)

	// 3. Threshold Check: Non-blocking signal to flush
	if e.memtable.Size() >= e.opts.MemTableSize {
		select {
		case e.flushCh <- struct{}{}: // Signal flush worker
		default:
			// Flush already in progress, avoid blocking
		}
	}

	return nil
}

// Delete writes a tombstone so future compaction can remove deleted keys safely.
func (e *Engine) Delete(key []byte) error {
	return e.Put(key, tombstoneValue)
}

// Get retrieves a value by key.
func (e *Engine) Get(key []byte) ([]byte, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Currently checks active MemTable. 
	// Production read path will later check Immutable MemTables -> Level 0 SSTables -> Level N SSTables.
	return e.memtable.Get(key)
}

// flusher runs in the background, listening for size thresholds.
func (e *Engine) flusher() {
	defer e.wg.Done()
	for range e.flushCh {
		e.flushMemTable()
		select {
		case e.compactCh <- struct{}{}:
		default:
		}
	}
}

func (e *Engine) compactor() {
	defer e.wg.Done()
	ticker := time.NewTicker(time.Duration(e.opts.CompactionIntervalSec) * time.Second)
	defer ticker.Stop()

	for {
		e.mu.RLock()
		closing := e.closing
		e.mu.RUnlock()
		if closing {
			_ = e.compactLevel0ToLevel1()
			return
		}

		select {
		case <-e.compactCh:
			_ = e.compactLevel0ToLevel1()
		case <-ticker.C:
			_ = e.compactLevel0ToLevel1()
		}
	}
}

func (e *Engine) compactLevel0ToLevel1() error {
	files, err := os.ReadDir(e.opts.DataDir)
	if err != nil {
		return err
	}
	var l0 []string
	for _, f := range files {
		name := f.Name()
		if strings.HasPrefix(name, "level0_") && strings.HasSuffix(name, ".sst") {
			l0 = append(l0, filepath.Join(e.opts.DataDir, name))
		}
	}
	if len(l0) < 2 {
		return nil
	}
	sort.Strings(l0)

	merged := make(map[string][]byte)
	// older -> newer so newer values win
	for _, p := range l0 {
		r, err := sstable.NewReader(p)
		if err != nil {
			continue
		}
		entries, err := r.Entries()
		_ = r.Close()
		if err != nil {
			continue
		}
		for _, kv := range entries {
			k := string(kv[0])
			v := make([]byte, len(kv[1]))
			copy(v, kv[1])
			merged[k] = v
		}
	}
	if len(merged) == 0 {
		return nil
	}

	keys := make([]string, 0, len(merged))
	for k := range merged {
		if IsTombstone(merged[k]) {
			continue // drop deleted keys during compaction
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := filepath.Join(e.opts.DataDir, fmt.Sprintf("level1_%d.sst", time.Now().UnixNano()))
	b, err := sstable.NewBuilder(out)
	if err != nil {
		return err
	}
	for _, k := range keys {
		if err := b.Add([]byte(k), merged[k]); err != nil {
			return err
		}
	}
	if err := b.Finish(); err != nil {
		return err
	}

	for _, p := range l0 {
		_ = os.Remove(p)
	}
	return nil
}

// flushMemTable rotates the active MemTable and WAL, then persists data to an SSTable.
func (e *Engine) flushMemTable() {
	e.mu.Lock()
	if e.memtable.Size() == 0 {
		e.mu.Unlock()
		return
	}

	// 1. FAST ROTATION (Microsecond pause)
	oldMem := e.memtable
	oldWAL := e.wal

	e.memtable = memtable.NewSkipList()
	newWALPath := filepath.Join(e.opts.DataDir, fmt.Sprintf("active_%d.wal", time.Now().UnixNano()))
	
	var err error
	e.wal, err = wal.New(newWALPath, e.opts.WALSync)
	if err != nil {
		// In a true critical system, you might trigger a panic or graceful shutdown here
		fmt.Printf("CRITICAL: failed to create new WAL: %v\n", err) 
		e.mu.Unlock()
		return
	}
	e.mu.Unlock() // Let user writes continue seamlessly to the new MemTable

	// 2. SLOW I/O (Background processing)
	timestamp := time.Now().UnixNano()
	sstPath := filepath.Join(e.opts.DataDir, fmt.Sprintf("level0_%d.sst", timestamp))
	builder, err := sstable.NewBuilder(sstPath)
	if err != nil {
		fmt.Printf("ERROR: creating SSTable builder: %v\n", err)
		return
	}

	entries := oldMem.AllEntries()
	for _, entry := range entries {
		if err := builder.Add(entry[0], entry[1]); err != nil {
			fmt.Printf("ERROR: adding to SSTable: %v\n", err)
			return
		}
	}

	if err := builder.Finish(); err != nil {
		fmt.Printf("ERROR: finishing SSTable: %v\n", err)
		return
	}

	// 3. CLEANUP
	oldWAL.Purge()
}

// Close gracefully shuts down the engine, waiting for ongoing flushes to finish.
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closing {
		e.mu.Unlock()
		return nil
	}
	e.closing = true
	
	// Force a final flush if there's lingering data
	if e.memtable.Size() > 0 {
		e.flushCh <- struct{}{}
	}
	close(e.flushCh)
	e.mu.Unlock()

	e.wg.Wait() // Block until background flusher completes safely

	e.mu.Lock()
	defer e.mu.Unlock()
	return e.wal.Close()
}