package distributed_test

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	peregrinekv "github.com/Mayanshh/Peregrine-KV"
)

func TestThreeNodeReplicationChaos(t *testing.T) {
	base := t.TempDir()

	allocAddr := func() string {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("alloc port: %v", err)
		}
		defer l.Close()
		return l.Addr().String()
	}

	nodes := []struct {
		id   string
		addr string
	}{
		{"c1", allocAddr()},
		{"c2", allocAddr()},
		{"c3", allocAddr()},
	}

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
		opts.MemTableSize = 1024
		// Inject some faults and latency; RAFT should still converge.
		opts.FaultDropRate = 0.05
		opts.FaultDelayMs = 5

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
	deadline := time.Now().Add(25 * time.Second)
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
		time.Sleep(150 * time.Millisecond)
	}
	if leader == nil {
		t.Fatalf("leader not elected")
	}

	putDeadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(putDeadline) {
		if err := leader.Put([]byte("kChaos"), []byte("vChaos")); err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	checkDeadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(checkDeadline) {
		allOK := true
		for _, d := range dbs {
			v, ok, err := d.Get([]byte("kChaos"))
			if err != nil || !ok || string(v) != "vChaos" {
				allOK = false
				break
			}
		}
		if allOK {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("not all nodes replicated under chaos faults")
}

