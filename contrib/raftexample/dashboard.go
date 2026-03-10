package main

import (
	"embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

//go:embed dashboard/index.html
var dashboardFS embed.FS

// messageStats tracks inter-node message counts.
type messageStats struct {
	mu       sync.RWMutex
	sent     map[string]uint64
	received map[string]uint64
	// Per-peer sent counts: peerID -> msgType -> count
	peerSent map[uint64]map[string]uint64
	// Per-peer received counts
	peerRecv     map[uint64]map[string]uint64
	totalSent    uint64
	totalRecv    uint64
	proposalsSent uint64
	lastReset    time.Time
}

func newMessageStats() *messageStats {
	return &messageStats{
		sent:      make(map[string]uint64),
		received:  make(map[string]uint64),
		peerSent:  make(map[uint64]map[string]uint64),
		peerRecv:  make(map[uint64]map[string]uint64),
		lastReset: time.Now(),
	}
}

func msgTypeName(t raftpb.MessageType) string {
	return raftpb.MessageType_name[int32(t)]
}

func (ms *messageStats) recordSent(msgs []raftpb.Message) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	for _, m := range msgs {
		name := msgTypeName(m.Type)
		ms.sent[name]++
		ms.totalSent++
		if ms.peerSent[m.To] == nil {
			ms.peerSent[m.To] = make(map[string]uint64)
		}
		ms.peerSent[m.To][name]++
	}
}

func (ms *messageStats) recordReceived(m raftpb.Message) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	name := msgTypeName(m.Type)
	ms.received[name]++
	ms.totalRecv++
	if ms.peerRecv[m.From] == nil {
		ms.peerRecv[m.From] = make(map[string]uint64)
	}
	ms.peerRecv[m.From][name]++
}

func (ms *messageStats) recordProposal() {
	atomic.AddUint64(&ms.proposalsSent, 1)
}

func (ms *messageStats) snapshot() map[string]interface{} {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	sentCopy := make(map[string]uint64, len(ms.sent))
	for k, v := range ms.sent {
		sentCopy[k] = v
	}
	recvCopy := make(map[string]uint64, len(ms.received))
	for k, v := range ms.received {
		recvCopy[k] = v
	}
	peerSentCopy := make(map[string]map[string]uint64)
	for id, m := range ms.peerSent {
		key := fmt.Sprintf("%d", id)
		peerSentCopy[key] = make(map[string]uint64, len(m))
		for k, v := range m {
			peerSentCopy[key][k] = v
		}
	}
	peerRecvCopy := make(map[string]map[string]uint64)
	for id, m := range ms.peerRecv {
		key := fmt.Sprintf("%d", id)
		peerRecvCopy[key] = make(map[string]uint64, len(m))
		for k, v := range m {
			peerRecvCopy[key][k] = v
		}
	}

	return map[string]interface{}{
		"sent":           sentCopy,
		"received":       recvCopy,
		"peerSent":       peerSentCopy,
		"peerReceived":   peerRecvCopy,
		"totalSent":      ms.totalSent,
		"totalReceived":  ms.totalRecv,
		"proposalsSent":  atomic.LoadUint64(&ms.proposalsSent),
		"uptimeSeconds":  time.Since(ms.lastReset).Seconds(),
	}
}

// nodeStatsJSON is the JSON response for /api/stats.
type nodeStatsJSON struct {
	ID           uint64                 `json:"id"`
	Leader       uint64                 `json:"leader"`
	Term         uint64                 `json:"term"`
	State        string                 `json:"state"`
	Commit       uint64                 `json:"commit"`
	Applied      uint64                 `json:"applied"`
	Peers        []uint64               `json:"peers"`
	KVCount      int                    `json:"kvCount"`
	Messages     map[string]interface{} `json:"messages"`
	Progress     map[string]interface{} `json:"progress,omitempty"`
}

func (rc *raftNode) getStats(kvs *kvstore) nodeStatsJSON {
	status := rc.node.Status()

	peers := make([]uint64, 0, len(rc.peers))
	for i := range rc.peers {
		peers = append(peers, uint64(i+1))
	}

	var kvCount int
	if kvs != nil {
		kvs.kvStore.Range(func(_, _ interface{}) bool {
			kvCount++
			return true
		})
	}

	stats := nodeStatsJSON{
		ID:       status.ID,
		Leader:   status.Lead,
		Term:     status.Term,
		State:    status.RaftState.String(),
		Commit:   status.Commit,
		Applied:  rc.appliedIndex,
		Peers:    peers,
		KVCount:  kvCount,
		Messages: rc.msgStats.snapshot(),
	}

	// Include replication progress if this node is leader
	if len(status.Progress) > 0 {
		progress := make(map[string]interface{})
		for id, pr := range status.Progress {
			progress[fmt.Sprintf("%d", id)] = map[string]interface{}{
				"match":  pr.Match,
				"next":   pr.Next,
				"state":  pr.State.String(),
				"active": pr.RecentActive,
			}
		}
		stats.Progress = progress
	}

	return stats
}

func serveDashboard(rc *raftNode, kvs *kvstore, port int) {
	mux := http.NewServeMux()

	// Stats API endpoint
	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(rc.getStats(kvs))
	})

	// Dashboard HTML
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		data, err := dashboardFS.ReadFile("dashboard/index.html")
		if err != nil {
			http.Error(w, "dashboard not found", 500)
			return
		}
		w.Write(data)
	})

	addr := fmt.Sprintf(":%d", port)
	log.Printf("Dashboard available at http://localhost:%d", port)
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("dashboard server error: %v", err)
		}
	}()
}
