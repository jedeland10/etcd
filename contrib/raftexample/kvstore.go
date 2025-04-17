// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/v3/contrib/raftexample/protostore"
	"go.etcd.io/raft/v3/raftpb"
)

///////////////////////////////////////////////////////////////////////////////
// Sharded Wait Registry Implementation (modeled after etcd's wait package)
///////////////////////////////////////////////////////////////////////////////

const defaultListElementLength = 64

// Wait provides the ability to wait and trigger events by ID.
type Wait interface {
	// Register returns a channel that will be triggered when Trigger is called with the same id.
	Register(id uint64) <-chan any
	// Trigger triggers the waiting channel(s) associated with the given id.
	Trigger(id uint64, x any)
	// IsRegistered returns true if an id is registered.
	IsRegistered(id uint64) bool
}

type list struct {
	e []listElement
}

type listElement struct {
	l sync.RWMutex
	m map[uint64]chan any
}

// New creates a new sharded wait registry.
func NewWait() Wait {
	res := list{
		e: make([]listElement, defaultListElementLength),
	}
	for i := 0; i < len(res.e); i++ {
		res.e[i].m = make(map[uint64]chan any)
	}
	return &res
}

func (w *list) Register(id uint64) <-chan any {
	idx := id % defaultListElementLength
	newCh := make(chan any, 1)
	w.e[idx].l.Lock()
	defer w.e[idx].l.Unlock()
	if _, ok := w.e[idx].m[id]; !ok {
		w.e[idx].m[id] = newCh
	} else {
		log.Panicf("duplicate registration for id: %x", id)
	}
	return newCh
}

func (w *list) Trigger(id uint64, x any) {
	idx := id % defaultListElementLength
	w.e[idx].l.Lock()
	ch := w.e[idx].m[id]
	delete(w.e[idx].m, id)
	w.e[idx].l.Unlock()
	if ch != nil {
		ch <- x
		close(ch)
	}
}

func (w *list) IsRegistered(id uint64) bool {
	idx := id % defaultListElementLength
	w.e[idx].l.RLock()
	defer w.e[idx].l.RUnlock()
	_, ok := w.e[idx].m[id]
	return ok
}

///////////////////////////////////////////////////////////////////////////////
// kvstore definition and methods (using the sharded wait registry)
///////////////////////////////////////////////////////////////////////////////

// kvstore is a key-value store backed by Raft, using a lock-free sync.Map.
type kvstore struct {
	// The key/value store (keys and values are of type string).
	kvStore     sync.Map
	snapshotter *snap.Snapshotter

	// Channel to propose a commit message to Raft.
	proposeC chan<- string

	// Wait registry to tie proposals with their commit completion.
	applyWait Wait

	// An atomic counter for generating unique proposal IDs.
	proposalSeq uint64
}

// proposalBufferPool avoids allocations when preparing proposals.
var proposalBufferPool = sync.Pool{
	New: func() interface{} { return &protostore.MyKV{} },
}

// newKVStore creates a new kvstore instance.
// It also starts the readProtoCommits goroutine to process Raft commits.
func newKVStore(snapshotter *snap.Snapshotter, proposeC chan string, commitC <-chan *commit, errorC <-chan error) *kvstore {
	s := &kvstore{
		snapshotter: snapshotter,
		proposeC:    proposeC,
		applyWait:   NewWait(), // Use the sharded wait registry here.
	}
	snapshot, err := s.loadSnapshot()
	if err != nil {
		log.Panic(err)
	}
	if snapshot != nil {
		log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
		if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
			log.Panic(err)
		}
	}
	// Read and apply committed entries from Raft.
	go s.readProtoCommits(commitC, errorC)
	return s
}

// Lookup returns the value for a given key.
func (s *kvstore) Lookup(key string) (string, bool) {
	value, ok := s.kvStore.Load(key)
	if ok {
		return value.(string), true
	}
	return "", false
}

// proposeToRaft constructs a proposal message and sends it to Raft.
func (s *kvstore) proposeToRaft(key string, value string) {
	kvMessage := proposalBufferPool.Get().(*protostore.MyKV)
	defer proposalBufferPool.Put(kvMessage)

	kvMessage.Key = []byte(key)
	kvMessage.Value = []byte(value)

	encodedData, err := kvMessage.Marshal()
	if err != nil {
		log.Fatalf("Failed to serialize MyKV: %v", err)
	}

	s.proposeC <- string(encodedData)
}

// Put issues a proposal to Raft and blocks until the proposal is committed.
// It returns an error if the proposal isn't applied within the context timeout.
func (s *kvstore) Put(ctx context.Context, key, value string) error {
	// Generate a unique proposal ID.
	proposalID := atomic.AddUint64(&s.proposalSeq, 1)
	// Register the proposal in the wait registry.
	waitCh := s.applyWait.Register(proposalID)

	// Prepare the proposal message.
	kvMessage := proposalBufferPool.Get().(*protostore.MyKV)
	defer proposalBufferPool.Put(kvMessage)
	kvMessage.Key = []byte(key)
	kvMessage.Value = []byte(value)
	kvMessage.ProposalID = proposalID // Assume MyKV has a ProposalID field.

	encodedData, err := kvMessage.Marshal()
	if err != nil {
		return err
	}
	commitMsg := string(encodedData)

	// Send the proposal to Raft.
	s.proposeC <- commitMsg

	// Block until the proposal is triggered (committed) or the context times out.
	select {
	case <-waitCh:
		// Proposal successfully applied.
		return nil
	case <-ctx.Done():
		// Clean up registration (optionally, Trigger with a nil/error value).
		s.applyWait.Trigger(proposalID, nil)
		return ctx.Err()
	}
}

// readProtoCommits processes proposals committed by Raft into our kvStore.
func (s *kvstore) readProtoCommits(commitC <-chan *commit, errorC <-chan error) {
	for commit := range commitC {
		if commit == nil {
			// Signal to load snapshot.
			snapshot, err := s.loadSnapshot()
			if err != nil {
				log.Panic(err)
			}
			if snapshot != nil {
				log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
				if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
					log.Panic(err)
				}
			}
			continue
		}

		// For each committed proposal, update the kvStore and trigger the wait.
		for _, data := range commit.data {
			var dataKv protostore.MyKV
			if err := dataKv.Unmarshal([]byte(data)); err != nil {
				log.Fatalf("raftexample: could not decode Protobuf message (%v)", err)
			}

			// Store the key/value pair.
			s.kvStore.Store(string(dataKv.Key), string(dataKv.Value))
			// Trigger the waiting Put (using the proposal ID embedded in the message).
			s.applyWait.Trigger(dataKv.ProposalID, nil)
		}
		// Signal that this commit batch is fully processed.
		close(commit.applyDoneC)
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

///////////////////////////////////////////////////////////////////////////////
// Snapshot methods (unchanged)
///////////////////////////////////////////////////////////////////////////////

var bufPool = sync.Pool{
	New: func() interface{} { return &bytes.Buffer{} },
}

// getSnapshot serializes the current kvStore to a snapshot.
func (s *kvstore) getSnapshot() ([]byte, error) {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	snapshot := &protostore.KVSnapshot{}

	// Use the Range method to iterate over sync.Map.
	s.kvStore.Range(func(key, value interface{}) bool {
		snapshot.Entries = append(snapshot.Entries, &protostore.MyKV{
			Key:   []byte(key.(string)),
			Value: []byte(value.(string)),
		})
		return true
	})

	data, err := proto.Marshal(snapshot)
	if err != nil {
		return nil, err
	}

	buf.Write(data)
	return buf.Bytes(), nil
}

// loadSnapshot loads a snapshot from the snapshotter.
func (s *kvstore) loadSnapshot() (*raftpb.Snapshot, error) {
	snapshot, err := s.snapshotter.Load()
	if err != nil {
		if errors.Is(err, snap.ErrNoSnapshot) {
			return nil, nil // No snapshot found is fine initially.
		}
		return nil, err
	}

	var kvSnap protostore.KVSnapshot
	if err := proto.Unmarshal(snapshot.Data, &kvSnap); err != nil {
		return nil, err
	}

	// Reconstruct kvStore by ranging over snapshot entries.
	s.kvStore = sync.Map{} // Reinitialize in-memory store.
	for _, kv := range kvSnap.Entries {
		s.kvStore.Store(string(kv.Key), string(kv.Value))
	}
	return snapshot, nil
}

// recoverFromSnapshot restores the state from a snapshot.
func (s *kvstore) recoverFromSnapshot(snapshot []byte) error {
	kvSnap := &protostore.KVSnapshot{}
	if err := proto.Unmarshal(snapshot, kvSnap); err != nil {
		return err
	}

	newKV := sync.Map{}
	for _, entry := range kvSnap.Entries {
		newKV.Store(string(entry.Key), string(entry.Value))
	}
	s.kvStore = newKV
	return nil
}
