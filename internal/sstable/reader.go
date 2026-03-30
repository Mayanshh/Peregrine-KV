package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/Mayanshh/Peregrine-KV/internal/cache"
)

const MagicNumber uint32 = 0x4C4F4355

// indexEntry maps a key to its byte offset in the SSTable file.
type indexEntry struct {
	key    []byte
	offset uint32
}

type Reader struct {
	file  *os.File
	path  string
	index []indexEntry
	bloom *BloomFilter
}

var defaultBlockCache = cache.NewBlockCache(4096)

// NewReader opens an SSTable, verifies its integrity, and loads the sparse index.
func NewReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := stat.Size()
	if fileSize < 8 {
		return nil, errors.New("file too short to be a valid SSTable")
	}

	// 1. Read Footer (Last 8 bytes)
	footer := make([]byte, 8)
	if _, err := f.ReadAt(footer, fileSize-8); err != nil {
		return nil, err
	}

	indexStart := binary.LittleEndian.Uint32(footer[0:4])
	magic := binary.LittleEndian.Uint32(footer[4:8])
	if magic != MagicNumber {
		return nil, errors.New("invalid magic number: corrupted SSTable")
	}

	// 2. Read Index Block
	indexSize := fileSize - 8 - int64(indexStart)
	indexData := make([]byte, indexSize)
	if _, err := f.ReadAt(indexData, int64(indexStart)); err != nil {
		return nil, err
	}

	// 3. Parse Index Block: [KeyLen(4)][Key][Offset(4)]
	var index []indexEntry
	buf := bytes.NewReader(indexData)
	for buf.Len() > 0 {
		var keyLen uint32
		binary.Read(buf, binary.LittleEndian, &keyLen)

		key := make([]byte, keyLen)
		io.ReadFull(buf, key)

		var offset uint32
		binary.Read(buf, binary.LittleEndian, &offset)

		index = append(index, indexEntry{key: key, offset: offset})
	}

	bloom := NewBloomFilter(uint32(len(index)*10), 5)
	for _, it := range index {
		bloom.Add(it.key)
	}

	return &Reader{file: f, path: path, index: index, bloom: bloom}, nil
}

// Get performs a binary search on the loaded index, then reads only the required bytes from disk.
func (r *Reader) Get(key []byte) ([]byte, bool, error) {
	if r.bloom != nil && !r.bloom.MightContain(key) {
		return nil, false, nil
	}

	// Binary search the index
	idx := sort.Search(len(r.index), func(i int) bool {
		return bytes.Compare(r.index[i].key, key) >= 0
	})

	if idx >= len(r.index) || !bytes.Equal(r.index[idx].key, key) {
		return nil, false, nil // Key not found in this SSTable
	}

	offset := r.index[idx].offset

	cacheKey := fmt.Sprintf("%s:%d", r.path, offset)
	if cached, ok := defaultBlockCache.Get(cacheKey); ok {
		return cached, true, nil
	}

	// Read Data Block: [KeyLen(4)][ValLen(4)][Key][Val]
	header := make([]byte, 8)
	if _, err := r.file.ReadAt(header, int64(offset)); err != nil {
		return nil, false, err
	}

	keyLen := binary.LittleEndian.Uint32(header[0:4])
	valLen := binary.LittleEndian.Uint32(header[4:8])

	data := make([]byte, keyLen+valLen)
	if _, err := r.file.ReadAt(data, int64(offset)+8); err != nil {
		return nil, false, err
	}

	// Double-check the key matches to avoid corruption collisions
	diskKey := data[:keyLen]
	if !bytes.Equal(diskKey, key) {
		return nil, false, errors.New("data corruption: key mismatch at offset")
	}

	val := data[keyLen:]
	defaultBlockCache.Put(cacheKey, val)
	return val, true, nil
}

// Entries returns all key/value entries in sorted key order.
func (r *Reader) Entries() ([][][]byte, error) {
	entries := make([][][]byte, 0, len(r.index))
	for _, ie := range r.index {
		v, ok, err := r.Get(ie.key)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		kc := make([]byte, len(ie.key))
		copy(kc, ie.key)
		vc := make([]byte, len(v))
		copy(vc, v)
		entries = append(entries, [][]byte{kc, vc})
	}
	return entries, nil
}

func (r *Reader) Close() error {
	return r.file.Close()
}