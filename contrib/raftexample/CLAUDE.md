# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

```sh
# Build
go build -o raftexample

# Run single node
./raftexample --id 1 --cluster http://127.0.0.1:12379 --port 12380

# Run local 5-node cluster (uses Procfile)
go install github.com/mattn/goreman@latest
goreman start

# Tests
go test ./...

# Regenerate protobuf (requires protoc 3.20.3, protoc-gen-gofast, go-grpc)
./scripts/genproto.sh
```

## Architecture

Three-component design around etcd's Raft library:

1. **Raft Node** (`raft.go`) — wraps `go.etcd.io/raft/v3`, manages WAL, snapshots, and inter-node HTTP transport (`rafthttp`). Runs two goroutines: `serveRaft()` for peer communication, `serveChannels()` as the main event loop that ticks the raft node and publishes committed entries via `commitC`.

2. **KV Store** (`kvstore.go`) — thread-safe `sync.Map` bridging raft consensus and the API layer. `Put()` proposes a protobuf-encoded entry to raft and blocks until committed. `readProtoCommits()` goroutine consumes from `commitC`, unmarshals entries, applies them, and signals waiters via proposal-ID-keyed channels.

3. **gRPC API** (`grpcapi.go`) — implements `RaftKVServiceServer` (Put, Get, GetCacheHits, ResetCacheHits). Legacy HTTP API in `httpapi.go` also supports cluster reconfiguration (POST/DELETE for add/remove nodes via `confChangeC`).

**Data flow:** Client → gRPC/HTTP → `kvstore.Put()` → `proposeC` → raft consensus → committed log → `kvstore.readProtoCommits()` → map updated.

## Proto

- `protostore/` — `MyKV` message (key-value pair)
- `raftapi/` — `RaftKVService` gRPC service definition
- Generated with gogo protobuf (`protoc-gen-gofast`)

## Cluster Config

Procfile defines a 5-node local cluster. Each node gets unique `--id`, separate peer port (x2379), and gRPC port (x2380). All nodes reference the full peer list via `--cluster`.
