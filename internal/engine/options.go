package engine

type Options struct {
	// The directory where WAL and SSTables are stored.
	DataDir string

	// Approximate threshold in bytes before a MemTable is rotated.
	MemTableSize int

	// Whether WAL writes are synced to disk.
	WALSync bool

	// Number of blocks to hold in memory.
	BlockCacheSize int

	// Background compaction interval in seconds.
	CompactionIntervalSec int

	// Distributed RAFT mode.
	EnableRaft bool
	NodeID     string
	RaftAddr   string
	Peers      []string

	// Optional TLS certs for RAFT gRPC.
	TLSCertFile string
	TLSKeyFile  string
	TLSCAFile   string

	// Chaos / fault injection knobs (for testing and resilience verification).
	FaultDropRate float64 // 0.0..1.0
	FaultDelayMs  int     // artificial latency added to RPC handlers
}

func DefaultOptions() Options {
	return Options{
		DataDir:       "./peregrine_data",
		MemTableSize:  64 * 1024 * 1024, // 64 MB
		WALSync:       false,
		BlockCacheSize: 4096,
		CompactionIntervalSec: 10,
		EnableRaft:            false,
		NodeID:                "",
		RaftAddr:              "",
		Peers:                 nil,
		TLSCertFile:           "",
		TLSKeyFile:            "",
		TLSCAFile:             "",
		FaultDropRate:        0,
		FaultDelayMs:         0,
	}
}