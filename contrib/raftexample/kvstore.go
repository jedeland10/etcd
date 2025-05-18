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
	"log"
	"sync"
	"sync/atomic"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/v3/contrib/raftexample/protostore"
)

const defaultListElementLength = 64

type Wait interface {
	Register(id uint64) <-chan any
	Trigger(id uint64, x any)
	IsRegistered(id uint64) bool
}

type list struct {
	e []listElement
}

type listElement struct {
	l sync.RWMutex
	m map[uint64]chan any
}

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

type kvstore struct {
	kvStore     sync.Map
	proposeC    chan<- []byte
	applyWait   Wait
	proposalSeq uint64
}

var proposalBufferPool = sync.Pool{
	New: func() interface{} { return &protostore.MyKV{} },
}

func newKVStore(proposeC chan []byte, commitC <-chan *commit, errorC <-chan error) *kvstore {
	s := &kvstore{
		proposeC:  proposeC,
		applyWait: NewWait(),
	}
	go s.readProtoCommits(commitC, errorC)
	return s
}

func (s *kvstore) Lookup(key string) (string, bool) {
	value, ok := s.kvStore.Load(key)
	if ok {
		return value.(string), true
	}
	return "", false
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

	s.proposeC <- encodedData
}

func (s *kvstore) Put(ctx context.Context, key, value string) error {
	proposalID := atomic.AddUint64(&s.proposalSeq, 1)
	waitCh := s.applyWait.Register(proposalID)

	kvMessage := proposalBufferPool.Get().(*protostore.MyKV)
	defer proposalBufferPool.Put(kvMessage)
	kvMessage.Key = []byte(key)
	kvMessage.Value = []byte(value)
	kvMessage.ProposalID = proposalID

	encodedData, err := kvMessage.Marshal()
	if err != nil {
		return err
	}
	commitMsg := encodedData

	// Send the proposal to Raft.
	s.proposeC <- commitMsg

	select {
	case <-waitCh:
		return nil
	case <-ctx.Done():
		s.applyWait.Trigger(proposalID, nil)
		return ctx.Err()
	}
}

func (s *kvstore) readProtoCommits(commitC <-chan *commit, errorC <-chan error) {
	for commit := range commitC {
		for _, data := range commit.data {
			var dataKv protostore.MyKV
			if err := dataKv.Unmarshal([]byte(data)); err != nil {
				log.Fatalf("raftexample: could not decode Protobuf message (%v)", err)
			}
			s.applyWait.Trigger(dataKv.ProposalID, nil)
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
