package sstable

import (
	"encoding/binary"
	"hash/fnv"
)

type BloomFilter struct {
	bits []byte
	k    uint32
	m    uint32
}

func NewBloomFilter(bitSize uint32, hashFunctions uint32) *BloomFilter {
	if bitSize < 1024 {
		bitSize = 1024
	}
	if hashFunctions == 0 {
		hashFunctions = 4
	}
	return &BloomFilter{
		bits: make([]byte, (bitSize+7)/8),
		k:    hashFunctions,
		m:    bitSize,
	}
}

func (bf *BloomFilter) Add(key []byte) {
	h1, h2 := hashPair(key)
	for i := uint32(0); i < bf.k; i++ {
		pos := (h1 + i*h2) % bf.m
		bf.bits[pos/8] |= 1 << (pos % 8)
	}
}

func (bf *BloomFilter) MightContain(key []byte) bool {
	h1, h2 := hashPair(key)
	for i := uint32(0); i < bf.k; i++ {
		pos := (h1 + i*h2) % bf.m
		if (bf.bits[pos/8] & (1 << (pos % 8))) == 0 {
			return false
		}
	}
	return true
}

func hashPair(key []byte) (uint32, uint32) {
	hA := fnv.New32a()
	_, _ = hA.Write(key)
	a := hA.Sum32()

	hB := fnv.New32()
	_, _ = hB.Write(key)
	b := hB.Sum32()
	if b == 0 {
		// Avoid zero-step for double hashing.
		b = binary.LittleEndian.Uint32([]byte{1, 0, 0, 0})
	}
	return a, b
}
