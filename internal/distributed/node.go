package distributed

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Mayanshh/Peregrine-KV/internal/clusterpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type ApplyFn func(op string, key, value []byte) error

type logEntry struct {
	Term  uint64 `json:"term"`
	Op    string `json:"op"`
	Key   []byte `json:"key"`
	Value []byte `json:"value,omitempty"`
}

type Metrics struct {
	LeaderChanges      uint64
	ElectionsStarted   uint64
	AppendRPCs         uint64
	ReplicationsFailed uint64
}

type Node struct {
	clusterpb.UnimplementedRaftServer

	mu         sync.Mutex
	id         string
	addr       string
	peers      map[string]string
	raft       *RaftNode
	grpcServer *grpc.Server
	clients    map[string]clusterpb.RaftClient
	conns      map[string]*grpc.ClientConn

	logFile     *os.File
	logPath     string
	entries     []logEntry
	commitIndex uint64
	applied     uint64
	applyFn     ApplyFn

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	metrics Metrics

	snapshotEvery uint64

	// Fault injection fields (testing / resilience).
	faultDropRate float64
	faultDelayMs  int
}

func NewNode(dataDir, id, addr string, peers map[string]string, certFile, keyFile, caFile string, apply ApplyFn, faultDropRate float64, faultDelayMs int) (*Node, error) {
	if id == "" || addr == "" {
		return nil, errors.New("raft requires non-empty node id and raft address")
	}
	rand.Seed(time.Now().UnixNano())
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}
	path := filepath.Join(dataDir, fmt.Sprintf("raft_%s.log", id))
	lf, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		id:      id,
		addr:    addr,
		peers:   peers,
		raft:    NewRaftNode(id, mapKeys(peers)),
		clients: make(map[string]clusterpb.RaftClient),
		conns:   make(map[string]*grpc.ClientConn),
		logFile: lf,
		logPath: path,
		applyFn: apply,
		ctx:     ctx,
		cancel:  cancel,
		snapshotEvery: 100,
		faultDropRate: faultDropRate,
		faultDelayMs:  faultDelayMs,
	}
	if err := n.loadLog(); err != nil {
		return nil, err
	}

	serverOpts := []grpc.ServerOption{}
	creds, err := serverCreds(certFile, keyFile, caFile)
	if err != nil {
		return nil, err
	}
	if creds != nil {
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}
	n.grpcServer = grpc.NewServer(serverOpts...)
	clusterpb.RegisterRaftServer(n.grpcServer, n)
	return n, nil
}

func (n *Node) Start() error {
	lis, err := net.Listen("tcp", n.addr)
	if err != nil {
		return err
	}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		_ = n.grpcServer.Serve(lis)
	}()

	if err := n.connectPeers(); err != nil {
		return err
	}
	n.wg.Add(2)
	go n.electionLoop()
	go n.heartbeatLoop()
	return nil
}

func (n *Node) Close() error {
	n.cancel()
	n.grpcServer.GracefulStop()
	n.wg.Wait()
	for _, c := range n.conns {
		_ = c.Close()
	}
	return n.logFile.Close()
}

func (n *Node) IsLeader() bool { return n.raft.Role() == Leader }
func (n *Node) LeaderID() string { return n.raft.LeaderID() }
func (n *Node) CurrentTerm() uint64 { return n.raft.CurrentTerm() }
func (n *Node) Metrics() Metrics {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.metrics
}

func (n *Node) RequestVote(ctx context.Context, req *clusterpb.VoteRequest) (*clusterpb.VoteResponse, error) {
	if n.faultDelayMs > 0 {
		time.Sleep(time.Duration(n.faultDelayMs) * time.Millisecond)
	}
	if n.faultDropRate > 0 && rand.Float64() < n.faultDropRate {
		return nil, status.Error(codes.Unavailable, "injected raft RequestVote fault")
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	cur := n.raft.CurrentTerm()
	if req.Term < cur {
		return &clusterpb.VoteResponse{Term: cur, VoteGranted: false}, nil
	}
	if req.Term > cur {
		_ = n.raft.HandleHeartbeat(req.Term, "")
	}
	// Simple voting rule: grant if not voted in this term.
	if n.raft.votedFor == "" || n.raft.votedFor == req.CandidateId {
		n.raft.votedFor = req.CandidateId
		return &clusterpb.VoteResponse{Term: n.raft.CurrentTerm(), VoteGranted: true}, nil
	}
	return &clusterpb.VoteResponse{Term: n.raft.CurrentTerm(), VoteGranted: false}, nil
}

func (n *Node) AppendEntries(ctx context.Context, req *clusterpb.AppendEntriesRequest) (*clusterpb.AppendEntriesResponse, error) {
	if n.faultDelayMs > 0 {
		time.Sleep(time.Duration(n.faultDelayMs) * time.Millisecond)
	}
	if n.faultDropRate > 0 && rand.Float64() < n.faultDropRate {
		return nil, status.Error(codes.Unavailable, "injected raft AppendEntries fault")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.metrics.AppendRPCs++

	if req.Term < n.raft.CurrentTerm() {
		return &clusterpb.AppendEntriesResponse{Term: n.raft.CurrentTerm(), Success: false}, nil
	}
	_ = n.raft.HandleHeartbeat(req.Term, req.LeaderId)

	if req.PrevLogIndex > uint64(len(n.entries)) {
		return &clusterpb.AppendEntriesResponse{Term: n.raft.CurrentTerm(), Success: false}, nil
	}
	for _, pb := range req.Entries {
		var e logEntry
		if err := json.Unmarshal(pb.Command, &e); err != nil {
			return &clusterpb.AppendEntriesResponse{Term: n.raft.CurrentTerm(), Success: false}, nil
		}
		n.entries = append(n.entries, e)
		if err := n.appendLogLine(e); err != nil {
			return nil, err
		}
	}

	if req.LeaderCommit > n.commitIndex {
		max := req.LeaderCommit
		if max > uint64(len(n.entries)) {
			max = uint64(len(n.entries))
		}
		n.commitIndex = max
		if err := n.applyCommittedLocked(); err != nil {
			return nil, err
		}
	}
	return &clusterpb.AppendEntriesResponse{Term: n.raft.CurrentTerm(), Success: true}, nil
}

func (n *Node) Propose(op string, key, value []byte) error {
	if !n.IsLeader() {
		return fmt.Errorf("not leader: leader=%s", n.LeaderID())
	}
	n.mu.Lock()
	entry := logEntry{Term: n.raft.CurrentTerm(), Op: op, Key: key, Value: value}
	n.entries = append(n.entries, entry)
	idx := uint64(len(n.entries))
	if err := n.appendLogLine(entry); err != nil {
		n.mu.Unlock()
		return err
	}
	term := n.raft.CurrentTerm()
	n.mu.Unlock()

	acks := 1
	needed := (len(n.peers)+1)/2 + 1
	for peerID, cli := range n.clients {
		ok := n.replicateWithRetry(peerID, cli, idx, term, entry)
		if ok {
			acks++
		}
	}
	if acks < needed {
		n.mu.Lock()
		n.metrics.ReplicationsFailed++
		n.mu.Unlock()
		return fmt.Errorf("replication quorum not reached: got=%d need=%d", acks, needed)
	}

	n.mu.Lock()
	if idx > n.commitIndex {
		n.commitIndex = idx
		if err := n.applyCommittedLocked(); err != nil {
			n.mu.Unlock()
			return err
		}
	}
	n.mu.Unlock()
	return nil
}

func (n *Node) applyCommittedLocked() error {
	for n.applied < n.commitIndex {
		i := n.applied
		e := n.entries[i]
		if err := n.applyFn(e.Op, e.Key, e.Value); err != nil {
			return err
		}
		n.applied++
	}
	if n.commitIndex > 0 && n.commitIndex%n.snapshotEvery == 0 {
		if err := n.snapshotAndCompactLocked(); err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) electionLoop() {
	defer n.wg.Done()
	t := time.NewTicker(75 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
			if n.raft.TickElection() {
				n.mu.Lock()
				n.metrics.ElectionsStarted++
				term := n.raft.CurrentTerm()
				n.mu.Unlock()
				votes := 1
				for _, cli := range n.clients {
					ctx, cancel := context.WithTimeout(n.ctx, 300*time.Millisecond)
					resp, err := cli.RequestVote(ctx, &clusterpb.VoteRequest{
						CandidateId: n.id,
						Term:        term,
					})
					cancel()
					if err == nil && resp.VoteGranted {
						votes++
					}
				}
				if n.raft.TryBecomeLeader(votes) {
					n.mu.Lock()
					n.metrics.LeaderChanges++
					n.mu.Unlock()
				}
			}
		}
	}
}

func (n *Node) heartbeatLoop() {
	defer n.wg.Done()
	t := time.NewTicker(120 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
			if !n.IsLeader() {
				continue
			}
			term := n.CurrentTerm()
			for _, cli := range n.clients {
				ctx, cancel := context.WithTimeout(n.ctx, 300*time.Millisecond)
				_, _ = cli.AppendEntries(ctx, &clusterpb.AppendEntriesRequest{
					LeaderId:     n.id,
					Term:         term,
					PrevLogIndex: uint64(len(n.entries)),
					LeaderCommit: n.commitIndex,
				})
				cancel()
			}
		}
	}
}

func (n *Node) replicateWithRetry(_ string, cli clusterpb.RaftClient, idx, term uint64, entry logEntry) bool {
	cmd, _ := json.Marshal(entry)
	backoff := 100 * time.Millisecond
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(n.ctx, 700*time.Millisecond)
		resp, err := cli.AppendEntries(ctx, &clusterpb.AppendEntriesRequest{
			LeaderId:     n.id,
			Term:         term,
			PrevLogIndex: idx - 1,
			Entries: []*clusterpb.LogEntry{
				{Term: term, Command: cmd},
			},
			LeaderCommit: n.commitIndex,
		})
		cancel()
		if err == nil && resp.Success {
			return true
		}
		time.Sleep(backoff)
		backoff *= 2
	}
	return false
}

func (n *Node) connectPeers() error {
	creds, err := clientCreds("", "", "")
	if err != nil {
		return err
	}
	for pid, addr := range n.peers {
		var conn *grpc.ClientConn
		if creds != nil {
			conn, err = grpc.DialContext(n.ctx, addr, grpc.WithTransportCredentials(creds))
		} else {
			conn, err = grpc.DialContext(n.ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}
		if err != nil {
			return err
		}
		n.conns[pid] = conn
		n.clients[pid] = clusterpb.NewRaftClient(conn)
	}
	return nil
}

func (n *Node) appendLogLine(e logEntry) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err := n.logFile.Write(append(b, '\n')); err != nil {
		return err
	}
	return n.logFile.Sync()
}

func (n *Node) loadLog() error {
	if _, err := n.logFile.Seek(0, 0); err != nil {
		return err
	}
	dec := json.NewDecoder(n.logFile)
	for {
		var e logEntry
		if err := dec.Decode(&e); err != nil {
			break
		}
		n.entries = append(n.entries, e)
	}
	n.commitIndex = uint64(len(n.entries))
	n.applied = 0
	for i := range n.entries {
		e := n.entries[i]
		if err := n.applyFn(e.Op, e.Key, e.Value); err != nil {
			return err
		}
		n.applied++
	}
	_, _ = n.logFile.Seek(0, 2)
	return nil
}

func (n *Node) snapshotAndCompactLocked() error {
	snapPath := n.logPath + ".snapshot"
	// Important: until we implement log prefix compaction with an index base,
	// we must not truncate or clear the in-memory log; doing so would break
	// correctness for PrevLogIndex/LeaderCommit comparisons.
	// For now we only write snapshot metadata for operational visibility.
	content := fmt.Sprintf("last_commit=%d\nlast_term=%d\n", n.commitIndex, n.CurrentTerm())
	if err := os.WriteFile(snapPath, []byte(content), 0644); err != nil {
		return err
	}
	return nil
}

func (n *Node) AddPeer(id, addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peers[id] = addr
}

func (n *Node) RemovePeer(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.peers, id)
}

func mapKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func serverCreds(certFile, keyFile, caFile string) (credentials.TransportCredentials, error) {
	if certFile == "" || keyFile == "" || caFile == "" {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	caPem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(caPem)
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, ClientCAs: caPool, ClientAuth: tls.RequireAndVerifyClientCert}
	return credentials.NewTLS(cfg), nil
}

func clientCreds(certFile, keyFile, caFile string) (credentials.TransportCredentials, error) {
	if certFile == "" || keyFile == "" || caFile == "" {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	caPem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(caPem)
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: caPool}
	return credentials.NewTLS(cfg), nil
}
