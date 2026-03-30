package distributed

import (
	"math/rand"
	"sync"
	"time"
)

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

type RaftNode struct {
	mu          sync.RWMutex
	id          string
	peers       []string
	currentTerm uint64
	votedFor    string
	role        Role
	leaderID    string
	lastBeat    time.Time
}

func NewRaftNode(id string, peers []string) *RaftNode {
	return &RaftNode{
		id:       id,
		peers:    peers,
		role:     Follower,
		lastBeat: time.Now(),
	}
}

func (n *RaftNode) CurrentTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentTerm
}

func (n *RaftNode) Role() Role {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.role
}

func (n *RaftNode) LeaderID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.leaderID
}

func (n *RaftNode) TickElection() bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	if time.Since(n.lastBeat) < timeout || n.role == Leader {
		return false
	}
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.id
	return true
}

func (n *RaftNode) HandleHeartbeat(term uint64, leaderID string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if term < n.currentTerm {
		return false
	}
	n.currentTerm = term
	n.role = Follower
	n.leaderID = leaderID
	n.lastBeat = time.Now()
	n.votedFor = ""
	return true
}

func (n *RaftNode) TryBecomeLeader(votes int) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	majority := (len(n.peers)+1)/2 + 1
	if n.role == Candidate && votes >= majority {
		n.role = Leader
		n.leaderID = n.id
		n.lastBeat = time.Now()
		return true
	}
	return false
}
