package engine

// MemTable defines the behavior for our active memory store.
type MemTable interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, bool, error)
	Size() int64
	IsFull() bool
	Iterator() Iterator
}

// WAL defines the behavior for our durability layer.
type WAL interface {
	Append(key []byte, value []byte) error
	Sync() error
	Rotate() error // Closes current file, opens a new one
	Close() error
}

// Iterator allows us to sequentially read a MemTable or SSTable.
type Iterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Close()
}