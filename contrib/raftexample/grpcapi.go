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
	"context"
	"log"
	"net"
	"strconv"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"go.etcd.io/etcd/v3/contrib/raftexample/raftapi"
)

// grpcKVAPI implements raftapi.RaftKVServiceServer.
// In this simplified version the Put RPC call is optimistic (like the HTTP PUT)
// and does not wait for the value to be committed.
type grpcKVAPI struct {
	raftapi.UnimplementedRaftKVServiceServer
	store       *kvstore
	confChangeC chan<- raftpb.ConfChange
}

// Put submits a proposal to the underlying key-value store without waiting for a commit ack.
func (s *grpcKVAPI) Put(ctx context.Context, req *raftapi.PutRequest) (*raftapi.PutResponse, error) {
	// Call a new Put method on your kvstore that blocks until the proposal is committed.
	if err := s.store.Put(ctx, req.GetKey(), req.GetValue()); err != nil {
		return nil, err
	}

	// Only return once the proposal has been fully committed.
	return &raftapi.PutResponse{
		Key:   proto.String(req.GetKey()),
		Value: proto.String(req.GetValue()),
	}, nil
}

// Get checks the store for the specified key and returns its value if present.
func (s *grpcKVAPI) Get(ctx context.Context, req *raftapi.GetRequest) (*raftapi.GetResponse, error) {
	key := req.GetKey()
	if val, ok := s.store.Lookup(key); ok {
		return &raftapi.GetResponse{
			Value: proto.String(val),
			Found: proto.Bool(true),
		}, nil
	}
	return &raftapi.GetResponse{
		Found: proto.Bool(false),
	}, nil
}

// serveGRPCKVAPI starts the gRPC server and registers the RaftKV service.
func serveGRPCKVAPI(kv *kvstore, port int, confChangeC chan<- raftpb.ConfChange, errorC <-chan error) {
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	handler := &grpcKVAPI{
		store:       kv,
		confChangeC: confChangeC,
	}

	grpcServer := grpc.NewServer()
	raftapi.RegisterRaftKVServiceServer(grpcServer, handler)

	// Enable reflection for tools such as grpcurl.
	reflection.Register(grpcServer)

	go func() {
		log.Printf("gRPC server listening on %v", lis.Addr())
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// Block until an error is reported from the Raft layer.
	if err, ok := <-errorC; ok {
		log.Fatalf("raft error: %v", err)
	}
}
