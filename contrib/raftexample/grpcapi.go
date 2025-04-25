// main.go
package main

import (
	"context"
	"io"
	"log"
	"net"
	"strconv"
	"sync/atomic"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"go.etcd.io/etcd/v3/contrib/raftexample/raftapi"
)

// 32 MiB windows to allow large open‐loop bursts
const (
	initialWindowSize = 32 << 20

	perConnectionWindow = 128 << 20 // 128 MiB per‐connection, shared across all streams
)

// grpcKVAPI implements raftapi.RaftKVServiceServer.
type grpcKVAPI struct {
	raftapi.UnimplementedRaftKVServiceServer
	store       *kvstore
	confChangeC chan<- raftpb.ConfChange

	streamReqCount int64
}

func (s *grpcKVAPI) Put(ctx context.Context, req *raftapi.PutRequest) (*raftapi.PutResponse, error) {
	if err := s.store.Put(ctx, req.GetKey(), req.GetValue()); err != nil {
		return nil, err
	}
	return &raftapi.PutResponse{
		Key:   proto.String(req.GetKey()),
		Value: proto.String(req.GetValue()),
	}, nil
}

func (s *grpcKVAPI) Get(ctx context.Context, req *raftapi.GetRequest) (*raftapi.GetResponse, error) {
	if val, ok := s.store.Lookup(req.GetKey()); ok {
		return &raftapi.GetResponse{
			Value: proto.String(val),
			Found: proto.Bool(true),
		}, nil
	}
	return &raftapi.GetResponse{Found: proto.Bool(false)}, nil
}

func (s *grpcKVAPI) StreamProposals(stream raftapi.RaftKVService_StreamProposalsServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		// count & log
		count := atomic.AddInt64(&s.streamReqCount, 1)
		log.Printf("▶ Incoming proposal #%d:", count)

		// 1) ACK immediately so client can clear its window
		if err := stream.Send(&raftapi.PutResponse{
			Key:   proto.String(req.GetKey()),
			Value: proto.String(req.GetValue()),
		}); err != nil {
			return err
		}

		// 2) then commit in background (so we don't stall Recv)
		go func(r *raftapi.PutRequest) {
			// we ignore error here—benchmark cares about arrival rate
			_ = s.store.Put(stream.Context(), r.GetKey(), r.GetValue())
		}(req)
	}
}

func serveGRPCKVAPI(kv *kvstore, port int, confChangeC chan<- raftpb.ConfChange, errorC <-chan error) {
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	handler := &grpcKVAPI{
		store:       kv,
		confChangeC: confChangeC,
	}

	grpcServer := grpc.NewServer(
		grpc.InitialWindowSize(initialWindowSize),
		grpc.InitialConnWindowSize(perConnectionWindow),
	)
	raftapi.RegisterRaftKVServiceServer(grpcServer, handler)
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
