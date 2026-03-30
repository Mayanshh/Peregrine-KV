package sstable

import (
	"encoding/binary"
	"os"
)

// Builder writes sorted key-value pairs to an SSTable file.
// Format: [Data Blocks] -> [Index Block] -> [Footer (8 bytes)]
type Builder struct {
	file         *os.File
	offset       uint32
	indexKeys    [][]byte
	indexOffsets []uint32
}

// NewBuilder creates a new SSTable file at the given path.
func NewBuilder(path string) (*Builder, error) {
	// O_TRUNC ensures we start fresh if a partial file exists
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	return &Builder{
		file:         f,
		offset:       0,
		indexKeys:    make([][]byte, 0),
		indexOffsets: make([]uint32, 0),
	}, nil
}

// Add writes a key-value pair to the SSTable and records its offset for the index.
func (b *Builder) Add(key, val []byte) error {
	// Record the offset for the index block
	// In a heavily optimized production system, we'd record only 1 key per 4KB block (Sparse Index).
	b.indexKeys = append(b.indexKeys, key)
	b.indexOffsets = append(b.indexOffsets, b.offset)

	// Format: [KeyLen(4)][ValLen(4)][Key][Val]
	buf := make([]byte, 8+len(key)+len(val))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(val)))
	copy(buf[8:], key)
	copy(buf[8+len(key):], val)

	n, err := b.file.Write(buf)
	if err != nil {
		return err
	}
	b.offset += uint32(n)
	return nil
}

// Finish writes the index block and footer, making the file queryable, then closes it.
func (b *Builder) Finish() error {
	indexStart := b.offset

	// Write Index Block: [KeyLen(4)][Key][Offset(4)]
	for i, key := range b.indexKeys {
		buf := make([]byte, 8+len(key))
		binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key)))
		copy(buf[4:], key)
		binary.LittleEndian.PutUint32(buf[4+len(key):], b.indexOffsets[i])

		n, err := b.file.Write(buf)
		if err != nil {
			return err
		}
		b.offset += uint32(n)
	}

	// Write Footer: [IndexStartOffset(4)][MagicNumber(4)]
	// Magic number helps us verify this is a valid Locus SSTable upon opening.
	footer := make([]byte, 8)
	binary.LittleEndian.PutUint32(footer[0:4], indexStart)
	binary.LittleEndian.PutUint32(footer[4:8], 0x4C4F4355) // "LOCU"

	if _, err := b.file.Write(footer); err != nil {
		return err
	}

	return b.file.Close()
}