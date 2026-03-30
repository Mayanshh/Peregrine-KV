package peregrinekv

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Mayanshh/Peregrine-KV/internal/distributed"
	"github.com/Mayanshh/Peregrine-KV/internal/engine"
	"github.com/Mayanshh/Peregrine-KV/internal/sstable"
)

// DB represents the public API for the Peregrine-KV store.
type DB struct {
	eng  *engine.Engine
	raft *distributed.Node
}

// Options configures Peregrine-KV.
type Options struct {
	// Directory where WAL and SSTables are stored.
	DataDir string

	// Approximate bytes before the MemTable is rotated and flushed.
	MemTableSize int

	// Sync WAL writes to disk immediately.
	WALSync bool

	// Number of cached SSTable value blocks (LRU entries).
	BlockCacheSize int

	// Background compaction interval in seconds.
	CompactionIntervalSec int

	// Distributed RAFT mode.
	EnableRaft bool
	NodeID     string
	RaftAddr   string
	Peers      []string // list of "id=addr" entries

	// Optional RAFT TLS configuration (server and client use the same config).
	TLSCertFile string
	TLSKeyFile  string
	TLSCAFile   string
}

// DefaultOptions returns a production-lean baseline.
func DefaultOptions() Options {
	e := engine.DefaultOptions()
	return Options{
		DataDir:                e.DataDir,
		MemTableSize:           e.MemTableSize,
		WALSync:                e.WALSync,
		BlockCacheSize:         e.BlockCacheSize,
		CompactionIntervalSec:  e.CompactionIntervalSec,
		EnableRaft:             e.EnableRaft,
		NodeID:                e.NodeID,
		RaftAddr:              e.RaftAddr,
		Peers:                 e.Peers,
		TLSCertFile:           e.TLSCertFile,
		TLSKeyFile:            e.TLSKeyFile,
		TLSCAFile:             e.TLSCAFile,
	}
}

// Open initializes the database, runs recovery, and prepares for read/writes.
func Open(opts Options) (*DB, error) {
	eopts := engine.Options{
		DataDir:                opts.DataDir,
		MemTableSize:          opts.MemTableSize,
		WALSync:               opts.WALSync,
		BlockCacheSize:        opts.BlockCacheSize,
		CompactionIntervalSec: opts.CompactionIntervalSec,
		EnableRaft:            opts.EnableRaft,
		NodeID:                opts.NodeID,
		RaftAddr:              opts.RaftAddr,
		Peers:                 opts.Peers,
		TLSCertFile:           opts.TLSCertFile,
		TLSKeyFile:            opts.TLSKeyFile,
		TLSCAFile:             opts.TLSCAFile,
	}

	eng, err := engine.Open(eopts)
	if err != nil {
		return nil, err
	}

	// Run Crash Recovery
	walPath := filepath.Join(eopts.DataDir, "active.wal")
	if err := eng.ReplayWAL(walPath); err != nil {
		return nil, fmt.Errorf("failed to recover from WAL: %w", err)
	}

	db := &DB{eng: eng}
	if eopts.EnableRaft {
		peers := parsePeers(eopts.Peers)
		node, err := distributed.NewNode(eopts.DataDir, eopts.NodeID, eopts.RaftAddr, peers, eopts.TLSCertFile, eopts.TLSKeyFile, eopts.TLSCAFile, func(op string, key, value []byte) error {
			if op == "delete" {
				return eng.Delete(key)
			}
			return eng.Put(key, value)
		})
		if err != nil {
			return nil, err
		}
		if err := node.Start(); err != nil {
			return nil, err
		}
		db.raft = node
	}

	return db, nil
}

// Put inserts or updates a key-value pair.
func (db *DB) Put(key, val []byte) error {
	if db.raft != nil {
		return db.raft.Propose("put", key, val)
	}
	return db.eng.Put(key, val)
}

// Delete marks a key as deleted using a tombstone record.
func (db *DB) Delete(key []byte) error {
	if db.raft != nil {
		return db.raft.Propose("delete", key, nil)
	}
	return db.eng.Delete(key)
}

// Get retrieves a value. It checks the MemTable first, then falls back to SSTables on disk.
func (db *DB) Get(key []byte) ([]byte, bool, error) {
	// 1. Check active MemTable (Fastest)
	if val, found := db.eng.Get(key); found {
		if engine.IsTombstone(val) {
			return nil, false, nil
		}
		return val, true, nil
	}

	// 2. Fallback to Disk: Check SSTables (Newest to Oldest)
	// In a complete system, we cache these readers. For this implementation, we open on demand.
	files, err := os.ReadDir(db.eng.DataDir())
	if err != nil {
		return nil, false, err
	}

	var sstFiles []string
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".sst" {
			sstFiles = append(sstFiles, f.Name())
		}
	}

	// Sort descending so we read the most recent SSTable first
	sort.Sort(sort.Reverse(sort.StringSlice(sstFiles)))

	for _, sstFile := range sstFiles {
		reader, err := sstable.NewReader(filepath.Join(db.eng.DataDir(), sstFile))
		if err != nil {
			continue // Skip corrupted files
		}
		
		val, found, err := reader.Get(key)
		reader.Close()

		if err != nil {
			return nil, false, err
		}
		if found {
			if engine.IsTombstone(val) {
				return nil, false, nil
			}
			return val, true, nil
		}
	}

	return nil, false, nil // Key not found anywhere
}

// Close gracefully shuts down the database.
func (db *DB) Close() error {
	if db.raft != nil {
		_ = db.raft.Close()
	}
	return db.eng.Close()
}

func (db *DB) IsLeader() bool {
	return db.raft != nil && db.raft.IsLeader()
}

func (db *DB) LeaderID() string {
	if db.raft == nil {
		return ""
	}
	return db.raft.LeaderID()
}

// RaftMetrics exposes basic RAFT/distribution counters (when RAFT mode is enabled).
func (db *DB) RaftMetrics() map[string]uint64 {
	if db.raft == nil {
		return map[string]uint64{}
	}
	m := db.raft.Metrics()
	return map[string]uint64{
		"leader_changes":       m.LeaderChanges,
		"elections_started":    m.ElectionsStarted,
		"append_rpcs":          m.AppendRPCs,
		"replications_failed": m.ReplicationsFailed,
	}
}

func parsePeers(peers []string) map[string]string {
	out := make(map[string]string, len(peers))
	for _, p := range peers {
		parts := strings.SplitN(p, "=", 2)
		if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
			out[parts[0]] = parts[1]
		}
	}
	return out
}