package sstable

import "testing"

func TestBloomFilterBasic(t *testing.T) {
	bf := NewBloomFilter(2048, 4)
	key := []byte("alpha")
	bf.Add(key)

	if !bf.MightContain(key) {
		t.Fatalf("expected inserted key to be present")
	}
	if bf.MightContain([]byte("definitely-not-present-xyz")) {
		// False positives are possible in bloom filters, but this specific
		// low-collision input should generally be absent with current settings.
		t.Fatalf("unexpected positive for absent key")
	}
}
