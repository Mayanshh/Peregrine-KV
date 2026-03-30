package wal

import (
	"encoding/binary"
	"os"
	"sync"
)

// WAL is a simple append-only write-ahead log.
// Record format: [KeyLen(4)][ValLen(4)][Key][Val]
type WAL struct {
	mu   sync.Mutex
	f    *os.File
	sync bool
	path string
}

func New(path string, syncWrites bool) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{f: f, sync: syncWrites, path: path}, nil
}

func (w *WAL) Write(key, val []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	header := make([]byte, 8)
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(val)))

	if _, err := w.f.Write(header); err != nil {
		return err
	}
	if _, err := w.f.Write(key); err != nil {
		return err
	}
	if _, err := w.f.Write(val); err != nil {
		return err
	}

	if w.sync {
		return w.f.Sync()
	}
	return nil
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Sync()
}

// Purge deletes the current WAL file (after closing it).
func (w *WAL) Purge() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.f.Close(); err != nil {
		return err
	}
	return os.Remove(w.path)
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}
