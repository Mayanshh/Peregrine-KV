# Peregrine-KV (Lean Distributed KV)

Peregrine-KV is a key-value store built around an LSM-tree style storage engine (WAL + MemTable + SSTables) and a distributed replication layer based on RAFT consensus. It exposes a gRPC API for client operations (`Put`, `Get`, `Delete`).

Key storage/performance primitives implemented in this repo:
- **WAL durability** for crash recovery
- **MemTable** (in-memory sorted table)
- **SSTables** with an integrity-checked format
- **Bloom filters** to skip disk reads for obvious non-matches
- **Block caching (LRU)** to avoid re-reading hot values from disk
- **Tombstones** to safely represent deletes until compaction
- **Compaction** (Level-0 → Level-1) merging SSTables and removing tombstoned keys

Distributed layer:
- **RAFT leader election** (terms, votes, heartbeats)
- **Leader-only writes** with **majority quorum** replication and commit/apply
- **Snapshot metadata hooks** (log truncation is intentionally disabled until full index-base compaction is implemented)

## Architecture Overview

### Storage path (single node)
1. `Put/Delete`:
   - Write to **WAL**
   - Update **MemTable**
2. MemTable threshold triggers:
   - Flush MemTable into **Level-0 SSTables**
3. Background compaction:
   - Merge multiple `level0_*.sst` into a `level1_*.sst`
   - Drop keys whose latest value is a tombstone
4. Reads:
   - MemTable first
   - Then SSTables from newest to oldest, skipping readers via **Bloom filter**
   - **LRU block cache** stores recently read values to reduce disk I/O

### Distributed path (multi-node)
1. A client issues `Put/Delete` to the node it connects to.
2. If RAFT is enabled:
   - The node allows the operation only if it is the **RAFT leader**
   - The leader replicates the log entry to followers via gRPC RAFT RPCs
   - The leader commits once a **majority** acknowledges replication
   - The committed entry is applied to the local storage engine
3. Reads (`Get`) are served from the local engine state. Replication happens asynchronously, so very strict “immediate linearizable reads on followers” require additional read-index or leader-proxy logic (not enabled here).

## Build & Test

```bash
go test ./...
```

## Run (Standalone / Non-RAFT)

```bash
go run ./cmd/server -data ./peregrine_data -kv-addr 127.0.0.1:50051
```

KV gRPC endpoints are on `127.0.0.1:50051`.

Optional health/metrics:
- `http://127.0.0.1:8080/healthz`
- `http://127.0.0.1:8080/metrics` (empty when RAFT disabled)

## Run (3-Node RAFT Cluster with Docker Compose)

```bash
docker compose up --build -d
```

Compose starts:
- `n1` (raft 25051, kv 50051)
- `n2` (raft 25052, kv 50052)
- `n3` (raft 25053, kv 50053)

## gRPC API

Protocol buffers live in:
- `proto/kv.proto`

Client RPCs:
- `Put(PutRequest) -> PutResponse`
- `Get(GetRequest) -> GetResponse`
- `Delete(DeleteRequest) -> DeleteResponse`

## Example: Go gRPC Client

```bash
go run ./examples/grpc-client -addr 127.0.0.1:50051 -op put -key k1 -val v1
go run ./examples/grpc-client -addr 127.0.0.1:50051 -op get -key k1
go run ./examples/grpc-client -addr 127.0.0.1:50051 -op delete -key k1
```

## Use in Your Project (embedded library)

The public Go API is the `peregrinekv` package at repo root:

```go
opts := peregrinekv.DefaultOptions()
opts.DataDir = "./my_data"

// Optional: enable RAFT mode.
opts.EnableRaft = false

db, err := peregrinekv.Open(opts)
if err != nil { /* handle */ }
defer db.Close()

_ = db.Put([]byte("k"), []byte("v"))
val, found, _ := db.Get([]byte("k"))
_ = db.Delete([]byte("k"))
```

## Logs, Debugging, and Analytics

Runtime logs:
- gRPC server logs each KV RPC as `rpc=<method> err=<...> dur=<...>`
- RAFT layer persists its own log file per node: `raft_<node-id>.log`
- SSTable flush/compaction happens in the background and can be verified by inspecting the data directory

Operational counters:
- When RAFT is enabled, `/metrics` returns:
  - `leader_changes`
  - `elections_started`
  - `append_rpcs`
  - `replications_failed`

## Security & Production Notes (Free / No-cost)

This repo uses TLS/auth hooks for RAFT transport (optional), but Docker Compose and examples default to plaintext for simplicity.

For “free tier / no cost” hosting, you can use free plans such as:
- Docker-based deploys on free-tier containers (e.g., Fly.io/Railway/Render free plans) depending on availability.

You should add:
- TLS termination (or enable RAFT TLS)
- authentication/authorization on KV RPCs
- persistence and monitoring for raft + WAL + SSTables

## License

MIT

