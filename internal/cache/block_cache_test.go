package cache

import "testing"

func TestBlockCacheEviction(t *testing.T) {
	c := NewBlockCache(2)
	c.Put("a", []byte("1"))
	c.Put("b", []byte("2"))
	c.Put("c", []byte("3"))

	if _, ok := c.Get("a"); ok {
		t.Fatalf("expected a to be evicted")
	}
	if v, ok := c.Get("c"); !ok || string(v) != "3" {
		t.Fatalf("expected c to be present")
	}
}
