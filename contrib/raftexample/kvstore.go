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
	"encoding/gob"
	"errors"
	"log"
	"strings"
	"sync"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/v3/contrib/raftexample/protostore"
	"go.etcd.io/raft/v3/raftpb"
)

// a key-value store backed by raft
type kvstore struct {
	proposeC    chan<- string // channel for proposing updates
	mu          sync.RWMutex
	kvStore     map[string]string // current committed key-value pairs
	snapshotter *snap.Snapshotter
}

type kv struct {
	Key string
	Val string
}

func newKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *commit, errorC <-chan error) *kvstore {
	s := &kvstore{proposeC: proposeC, kvStore: make(map[string]string), snapshotter: snapshotter}
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
	// read commits from raft into kvStore map until error
	// go s.readCommits(commitC, errorC)
	go s.readProtoCommits(commitC, errorC)
	return s
}

func (s *kvstore) Lookup(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.kvStore[key]
	return v, ok
}

var proposalBufferPool = sync.Pool{
	New: func() interface{} { return &protostore.MyKV{} },
}

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

func (s *kvstore) Propose(k string, v string) {
	var buf strings.Builder
	if err := gob.NewEncoder(&buf).Encode(kv{k, v}); err != nil {
		log.Fatal(err)
	}
	s.proposeC <- buf.String()
}

func (s *kvstore) readCommits(commitC <-chan *commit, errorC <-chan error) {
	for commit := range commitC {
		if commit == nil {
			// signaled to load snapshot
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

		for _, data := range commit.data {
			var dataKv kv
			dec := gob.NewDecoder(bytes.NewBufferString(data))
			if err := dec.Decode(&dataKv); err != nil {
				log.Fatalf("raftexample: could not decode message (%v)", err)
			}
			s.mu.Lock()
			s.kvStore[dataKv.Key] = dataKv.Val
			s.mu.Unlock()
		}
		close(commit.applyDoneC)
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *kvstore) readProtoCommits(commitC <-chan *commit, errorC <-chan error) {
	for commit := range commitC {
		if commit == nil {
			// signaled to load snapshot
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

		for _, data := range commit.data {
			var dataKv protostore.MyKV

			if err := dataKv.Unmarshal([]byte(data)); err != nil {
				log.Fatalf("❌ raftexample: could not decode Protobuf message (%v)", err)
			}

			s.mu.Lock()
			s.kvStore[string(dataKv.Key)] = string(dataKv.Value)
			s.mu.Unlock()
		}
		close(commit.applyDoneC)
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

var bufPool = sync.Pool{
	New: func() interface{} { return &bytes.Buffer{} },
}

func (s *kvstore) getSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	snapshot := &protostore.KVSnapshot{}
	for k, v := range s.kvStore {
		snapshot.Entries = append(snapshot.Entries, &protostore.MyKV{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}

	data, err := proto.Marshal(snapshot)
	if err != nil {
		return nil, err
	}

	buf.Write(data)
	return buf.Bytes(), nil
}

func (s *kvstore) loadSnapshot() (*raftpb.Snapshot, error) {
	snapshot, err := s.snapshotter.Load()
	if err != nil {
		if errors.Is(err, snap.ErrNoSnapshot) {
			return nil, nil // no snapshot found is fine initially
		}
		return nil, err
	}

	// Recover state explicitly from protobuf-encoded snapshot
	var kvSnap protostore.KVSnapshot
	if err := proto.Unmarshal(snapshot.Data, &protostore.KVSnapshot{}); err != nil {
		return nil, err
	}

	// Apply snapshot state explicitly
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, kv := range kvSnap.Entries {
		s.kvStore[string(kv.Key)] = string(kv.Value)
	}

	return snapshot, nil
}

func (s *kvstore) recoverFromSnapshot(snapshot []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap := &protostore.KVSnapshot{}
	if err := proto.Unmarshal(snapshot, snap); err != nil {
		return err
	}

	kv := make(map[string]string)
	for _, entry := range snap.Entries {
		kv[string(entry.Key)] = string(entry.Value)
	}

	s.kvStore = kv
	return nil
}
