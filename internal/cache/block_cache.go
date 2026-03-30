package cache

import (
	"container/list"
	"sync"
)

type entry struct {
	key   string
	value []byte
}

// BlockCache is a simple thread-safe LRU cache for SST blocks/records.
type BlockCache struct {
	mu       sync.Mutex
	capacity int
	ll       *list.List
	items    map[string]*list.Element
}

func NewBlockCache(capacity int) *BlockCache {
	if capacity <= 0 {
		capacity = 1024
	}
	return &BlockCache{
		capacity: capacity,
		ll:       list.New(),
		items:    make(map[string]*list.Element),
	}
}

func (c *BlockCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(elem)
	val := elem.Value.(*entry).value
	out := make([]byte, len(val))
	copy(out, val)
	return out, true
}

func (c *BlockCache) Put(key string, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.ll.MoveToFront(elem)
		v := make([]byte, len(value))
		copy(v, value)
		elem.Value.(*entry).value = v
		return
	}

	v := make([]byte, len(value))
	copy(v, value)
	elem := c.ll.PushFront(&entry{key: key, value: v})
	c.items[key] = elem

	if c.ll.Len() > c.capacity {
		last := c.ll.Back()
		if last != nil {
			c.ll.Remove(last)
			delete(c.items, last.Value.(*entry).key)
		}
	}
}
