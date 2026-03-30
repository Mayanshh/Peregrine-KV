package sstable

import "sync"

// Manifest tracks SSTable files by level.
type Manifest struct {
	mu     sync.RWMutex
	levels map[int][]string
}

func NewManifest() *Manifest {
	return &Manifest{levels: make(map[int][]string)}
}

func (m *Manifest) SetLevelFiles(level int, files []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := make([]string, len(files))
	copy(c, files)
	m.levels[level] = c
}

func (m *Manifest) LevelFiles(level int) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	files := m.levels[level]
	c := make([]string, len(files))
	copy(c, files)
	return c
}
