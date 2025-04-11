package main

import (
	"context"
	"errors"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"go.etcd.io/etcd/v3/contrib/raftexample/raftapi"
	"sync/atomic"
)

// Global metric (if desired)
var committedCount uint64

// proposal wraps a PutRequest along with an ack channel to signal when the proposal is committed.
type proposal struct {
	req   *raftapi.PutRequest
	ackCh chan struct{}
}

type grpcKVAPI struct {
	raftapi.UnimplementedRaftKVServiceServer
	store       *kvstore
	confChangeC chan<- raftpb.ConfChange
	// A buffered channel to queue proposals.
	proposeCh chan *proposal
}

// Put receives a proposal, enqueues it, and blocks until the proposal is committed.
func (s *grpcKVAPI) Put(ctx context.Context, req *raftapi.PutRequest) (*raftapi.PutResponse, error) {
	// Create a dedicated ack channel for this proposal.
	ackCh := make(chan struct{})
	prop := &proposal{
		req:   req,
		ackCh: ackCh,
	}
	// Attempt to enqueue the proposal.
	select {
	case s.proposeCh <- prop:
		// Proposal enqueued; now wait for its commit ack.
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Wait for the proposal to be committed or timeout.
	select {
	case <-ackCh:
		return &raftapi.PutResponse{
			Key:   proto.String(req.GetKey()),
			Value: proto.String(req.GetValue()),
		}, nil
	case <-time.After(5 * time.Second):
		return nil, errors.New("timed out waiting for commit ack")
	}
}

// Get remains unchanged.
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

// serveGRPCKVAPI starts the gRPC server and launches the proposal processing goroutine.
func serveGRPCKVAPI(kv *kvstore, port int, confChangeC chan<- raftpb.ConfChange, errorC <-chan error) {
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Create our handler with a large proposal buffer.
	handler := &grpcKVAPI{
		store:       kv,
		confChangeC: confChangeC,
		proposeCh:   make(chan *proposal, 100000), // Adjust buffer size as needed.
	}

	grpcServer := grpc.NewServer()
	raftapi.RegisterRaftKVServiceServer(grpcServer, handler)

	// Enable reflection for debugging with grpcurl.
	reflection.Register(grpcServer)

	// Proposal processing goroutine.
	go func() {
		for prop := range handler.proposeCh {
			// Start processing each proposal concurrently.
			ackRaft := handler.store.proposeToRaft(prop.req.GetKey(), prop.req.GetValue())
			go func(k, v string, raftAck <-chan struct{}, ackCh chan struct{}) {
				select {
				case <-raftAck:
					atomic.AddUint64(&committedCount, 1)
				case <-time.After(5 * time.Second):
					log.Printf("❌ Timed out committing key: %s", k)
					atomic.AddUint64(&committedCount, 1)
				}
				// Signal that the proposal is complete.
				close(ackCh)
			}(prop.req.GetKey(), prop.req.GetValue(), ackRaft, prop.ackCh)
		}
	}()

	// Launch the gRPC server.
	go func() {
		log.Printf("gRPC server listening on %v", lis.Addr())
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// If raft reports an error, log it and exit.
	if err, ok := <-errorC; ok {
		log.Fatalf("raft error: %v", err)
	}
}
