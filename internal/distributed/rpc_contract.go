package distributed

//go:generate protoc --go_out=../ --go-grpc_out=../ ../../proto/cluster.proto

// This file anchors protobuf/gRPC generation for Raft RPCs.
// Run `go generate ./internal/distributed` after installing:
//   protoc, protoc-gen-go, protoc-gen-go-grpc
