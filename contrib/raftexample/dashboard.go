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

// messageStats tracks inter-node message counts and byte volumes.
type messageStats struct {
	mu       sync.RWMutex
	sent     map[string]uint64
	received map[string]uint64
	// Per-peer sent counts: peerID -> msgType -> count
	peerSent map[uint64]map[string]uint64
	// Per-peer received counts
	peerRecv map[uint64]map[string]uint64
	// Byte volumes
	bytesSent     uint64
	bytesRecv     uint64
	peerBytesSent map[uint64]uint64
	peerBytesRecv map[uint64]uint64
	totalSent     uint64
	totalRecv     uint64
	proposalsSent uint64
	lastReset     time.Time
}

func newMessageStats() *messageStats {
	return &messageStats{
		sent:          make(map[string]uint64),
		received:      make(map[string]uint64),
		peerSent:      make(map[uint64]map[string]uint64),
		peerRecv:      make(map[uint64]map[string]uint64),
		peerBytesSent: make(map[uint64]uint64),
		peerBytesRecv: make(map[uint64]uint64),
		lastReset:     time.Now(),
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
		size := uint64(m.Size())
		ms.sent[name]++
		ms.totalSent++
		ms.bytesSent += size
		ms.peerBytesSent[m.To] += size
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
	size := uint64(m.Size())
	ms.received[name]++
	ms.totalRecv++
	ms.bytesRecv += size
	ms.peerBytesRecv[m.From] += size
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

	peerBytesSentCopy := make(map[string]uint64, len(ms.peerBytesSent))
	for id, v := range ms.peerBytesSent {
		peerBytesSentCopy[fmt.Sprintf("%d", id)] = v
	}
	peerBytesRecvCopy := make(map[string]uint64, len(ms.peerBytesRecv))
	for id, v := range ms.peerBytesRecv {
		peerBytesRecvCopy[fmt.Sprintf("%d", id)] = v
	}

	return map[string]interface{}{
		"sent":              sentCopy,
		"received":          recvCopy,
		"peerSent":          peerSentCopy,
		"peerReceived":      peerRecvCopy,
		"totalSent":         ms.totalSent,
		"totalReceived":     ms.totalRecv,
		"bytesSent":         ms.bytesSent,
		"bytesReceived":     ms.bytesRecv,
		"peerBytesSent":     peerBytesSentCopy,
		"peerBytesReceived": peerBytesRecvCopy,
		"proposalsSent":     atomic.LoadUint64(&ms.proposalsSent),
		"uptimeSeconds":     time.Since(ms.lastReset).Seconds(),
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
	Cache        map[string]interface{} `json:"cache"`
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

	cacheHits := rc.node.CacheHits()
	proposalsSent := atomic.LoadUint64(&rc.msgStats.proposalsSent)

	cacheStats := map[string]interface{}{
		"hits":      cacheHits,
		"proposals": proposalsSent,
	}
	if proposalsSent > 0 {
		cacheStats["hitRate"] = float64(cacheHits) / float64(proposalsSent)
	} else {
		cacheStats["hitRate"] = 0.0
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
		Cache:    cacheStats,
	}

	// Include replication progress if this node is leader
	if len(status.Progress) > 0 {
		progress := make(map[string]interface{})
		for id, pr := range status.Progress {
			progress[fmt.Sprintf("%d", id)] = map[string]interface{}{
				"match":       pr.Match,
				"next":        pr.Next,
				"state":       pr.State.String(),
				"active":      pr.RecentActive,
				"cacheIdx":    pr.CacheIdx,
				"nextCacheId": pr.NextCacheId,
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

	// PUT handler for proposals via dashboard
	mux.HandleFunc("/api/put", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			return
		}
		if r.Method != "POST" {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key == "" {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if err := kvs.Put(r.Context(), req.Key, req.Value); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
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
