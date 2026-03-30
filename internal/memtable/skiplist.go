package memtable

import (
	"bytes"
	"sort"
	"sync"
)

// SkipList is a simple in-memory table abstraction.
// This project currently uses it as the active MemTable; it does not implement
// a probabilistic skip list yet (it behaves like a sorted map).
type SkipList struct {
	mu    sync.RWMutex
	data  map[string][]byte
	bytes int
}

func NewSkipList() *SkipList {
	return &SkipList{
		data: make(map[string][]byte),
	}
}

func (s *SkipList) Put(key, val []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := string(key)
	if old, ok := s.data[k]; ok {
		s.bytes -= len(old)
	} else {
		s.bytes += len(key)
	}

	// Copy to avoid caller mutation after write.
	v := make([]byte, len(val))
	copy(v, val)
	s.data[k] = v
	s.bytes += len(v)
}

func (s *SkipList) Get(key []byte) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v, ok := s.data[string(key)]
	if !ok {
		return nil, false
	}

	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

// Size returns an approximate memory footprint in bytes.
func (s *SkipList) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bytes
}

// AllEntries returns entries sorted by key ascending.
// Each entry is returned as [][]byte{key, value} to match the engine's usage.
func (s *SkipList) AllEntries() [][][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([][]byte, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, []byte(k))
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	entries := make([][][]byte, 0, len(keys))
	for _, k := range keys {
		v := s.data[string(k)]

		kc := make([]byte, len(k))
		copy(kc, k)
		vc := make([]byte, len(v))
		copy(vc, v)

		entries = append(entries, [][]byte{kc, vc})
	}
	return entries
}
