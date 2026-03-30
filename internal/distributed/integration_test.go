package distributed_test

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	peregrinekv "github.com/Mayanshh/Peregrine-KV"
)

func TestThreeNodeReplication(t *testing.T) {
	base := t.TempDir()
	nodes := []struct {
		id   string
		addr string
	}{}

	allocAddr := func() string {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("alloc port: %v", err)
		}
		defer l.Close()
		return l.Addr().String()
	}

	nodes = append(nodes, struct {
		id   string
		addr string
	}{"n1", allocAddr()})
	nodes = append(nodes, struct {
		id   string
		addr string
	}{"n2", allocAddr()})
	nodes = append(nodes, struct {
		id   string
		addr string
	}{"n3", allocAddr()})

	dbs := make([]*peregrinekv.DB, 0, len(nodes))
	for _, n := range nodes {
		peers := make([]string, 0, 2)
		for _, p := range nodes {
			if p.id == n.id {
				continue
			}
			peers = append(peers, p.id+"="+p.addr)
		}
		opts := peregrinekv.DefaultOptions()
		opts.DataDir = filepath.Join(base, n.id)
		opts.EnableRaft = true
		opts.NodeID = n.id
		opts.RaftAddr = n.addr
		opts.Peers = peers
		opts.MemTableSize = 1024 // flush quickly in test

		db, err := peregrinekv.Open(opts)
		if err != nil {
			t.Fatalf("open %s: %v", n.id, err)
		}
		dbs = append(dbs, db)
	}
	defer func() {
		for _, d := range dbs {
			_ = d.Close()
		}
	}()

	var leader *peregrinekv.DB
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, d := range dbs {
			if d.IsLeader() {
				leader = d
				break
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(120 * time.Millisecond)
	}
	if leader == nil {
		t.Fatalf("leader not elected")
	}

	if err := leader.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("leader put failed: %v", err)
	}

	deadline2 := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline2) {
		allOK := true
		for _, d := range dbs {
			v, ok, err := d.Get([]byte("k1"))
			if err != nil || !ok || string(v) != "v1" {
				allOK = false
				break
			}
		}
		if allOK {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}

	for i, d := range dbs {
		v, ok, err := d.Get([]byte("k1"))
		if err != nil {
			t.Fatalf("node %d get error: %v", i, err)
		}
		if !ok || string(v) != "v1" {
			t.Fatalf("node %d missing replication, got ok=%v val=%q", i, ok, string(v))
		}
	}
}
