# Replicated in-memory cache with Redis protocol and quorum read

Two applications built on gquorum:

- **`server/`**: a cache node speaking a subset of the Redis protocol. Three
  nodes host one `cache` load group; every write is replicated through Raft and
  every node can serve reads. The cache holds at most **512** keys and evicts
  the key written longest ago when a new key overflows it.
- **`loadgen/`**: a stress client. Several workers share a set of **1024** keys
  (twice the capacity, so eviction is constant). Each operation writes a new
  version of a key and immediately reads it back. The loader selects healthy
  nodes round-robin for every attempt and shares a bounded pool of reusable
  TCP connections.

The application cache is an in-memory map. Its Raft log and snapshots use the
regular disk-backed WAL store under `/data/raft`; `_meta` and authority state
live under `/data/system`. Snapshots are taken every 4096 entries. A restarted
process reconstructs its map from the snapshot and committed log, retaining the
same replica ID. No automatic reseating or empty-cache reset is involved.

The Compose demo sets `STORAGE_DISABLE_FSYNC=true` for speed. This skips fsync for
load-group WAL and applied-state writes while still flushing the WAL encoder to
the OS. Snapshot files, identity pins, directory operations and `_meta`/authority
storage retain their synchronization. A process restart with the OS still running
can recover these files, including after all three cache processes stop.
**Host/VM crashes and power loss can lose acknowledged writes or corrupt Raft
state.** This mode does not provide crash-durable Raft guarantees. Set
`STORAGE_DISABLE_FSYNC=false make run-cache-example` for normal synchronous writes.
The server flag is `--storage-disable-fsync`; it defaults to false outside Compose,
so fsync is enabled unless explicitly disabled.

## Run it

From the repository root, with Docker Compose available:

```sh
CACHE_WORKERS=32 make run-cache-example       # 1..256 workers; default 8
# Starts three cache nodes and the load generator.
docker compose -f examples/inmemory-cache/compose.yaml logs -f loadgen
redis-cli -p 16381 SET greeting hello        # any Redis client works
redis-cli -p 16383 GET greeting              # read it from another node
redis-cli -p 16382 DBSIZE                    # never more than 512
make stop-cache-example                      # stop all nodes; keeps Raft and topology volumes
make clean-cache-example                     # delete containers and volumes
```

`stop-cache-example` preserves data for the next start. `clean-cache-example`
deletes it. Existing volumes from the former memory-storage demo need a one-time
explicit clean before using this version: a group's storage class is immutable.

Nodes listen for Redis on **16381–16383**, bound to loopback. Override them
with `CACHE_NODE1_PORT`…`CACHE_NODE3_PORT`. `CACHE_WORKERS` controls parallelism
from **1 to 256**, default **8**; values outside that range are rejected.
`CACHE_DURATION` defaults to `0s` (run until stopped). Pass these environment
variables when running `make run-cache-example`.

The loader connects directly to `CACHE_NODES` and probes each node's HTTP
`GET /health` every **500ms**, with a **1s timeout** and an independent probe
loop per node. A slow node cannot delay another node's health updates; probes
for the same node never overlap. Health HTTP connections are reused.
`CACHE_HEALTH_PORT` defaults to **8080**, on the same hosts as the Redis
endpoints. Nodes return 200 only when `Node.Status().Ready` is true, the local
`Group.WaitReady` succeeds, and `Group.ReadBarrier` confirms a fresh cache-group
quorum and local application. The replica must still be running after the check.
Otherwise the endpoint returns 503. This uses the existing non-mutating APIs;
health probes do not append application commands or change cache keys. A node
enters rotation only after a successful probe and returns automatically after
recovery. `healthy=` in loader reports shows the
currently eligible endpoints.

Each command attempt selects the next healthy node round-robin. Failed
connections and retryable replies immediately remove that node from selection
until a successful health probe. When none are healthy, requests wait for a
health update within their existing timeout. Commands have an initial **2s
timeout** and at most **two retries sharing 500ms total**, with identical command
bytes preserved. Permanent command errors are not retried.

Workers and reports reuse Redis connections. Each node's pool has
`max(1, ceil(CACHE_WORKERS / node_count))` slots, so ten workers across three nodes
use at most **12 Redis connections**. A failed connection is discarded before
retrying, and the next attempt can select another healthy node. Reuse avoids
source-port exhaustion and the CPU cost of reconnecting for every command.

The load generator prints one line every five seconds. `rate`, `write_avg`
and `read_avg` cover only the last interval, so a slowdown shows up at once;
the `final` line covers the whole run:

```
progress elapsed=20s rate=1410/s write_avg=2.6ms read_avg=3.1ms total_writes=28150 total_reads=28150 hits=28150 evicted_misses=0 uncertain=0 stale=0 corrupt=0 write_errors=0 read_errors=0 connections=9 dials=9 healthy=node1:6379,node2:6379,node3:6379 dbsize=[node1:6379=512 node2:6379=512 node3:6379=512]
```

The sample output above illustrates the report format; it is not a benchmark
for the WAL configuration. Adjust `CACHE_WORKERS` to explore batching and
throughput on your machine.

| Field | Meaning |
| --- | --- |
| `hits` | The read returned the version just written, or a newer one. |
| `evicted_misses` | The key was already evicted by other workers' writes. Rare, because each read follows its write immediately; eviction itself is constant (`dbsize` stays at 512 with 1024 keys). |
| `uncertain` | An older version whose write had failed (it may still commit later). |
| `stale` | An older version than an acknowledged write: a consistency bug. Must be 0. |
| `write_errors`, `read_errors` | Requests that failed, for example while a node is down. Loader cancellation is excluded. |
| `connections`, `dials` | Open pooled TCP connections and cumulative connection attempts. Successful traffic reuses connections, so `dials` should remain bounded on a healthy cluster. |

It exits non-zero if any read was stale or corrupt.

## Try failures

```sh
docker compose -f examples/inmemory-cache/compose.yaml kill -s SIGKILL node3
# The loader removes node3 after a failed request or health check; load continues
docker compose -f examples/inmemory-cache/compose.yaml start node3
redis-cli -p 16383 INFO                      # the same replica_id after recovery
redis-cli -p 16383 DBSIZE                    # node3 caught up from the leader
```

The restarted node replays its local snapshot and log, then catches up from the
leader. Its health endpoint returns 503 until its replica can confirm reads,
then the loader returns it to the rotation. Killing two nodes removes
quorum and commands return `TRYAGAIN`; restarting them with their retained volumes
restores quorum. This tests process failure, not host/VM failure or erased disks.

## Supported commands

| Command | Behavior |
| --- | --- |
| `SET key value` | Replicated write (no options). Key ≤ 1 KiB, value ≤ 4 KiB. A rewrite makes the key the newest. |
| `GET key` | Quorum-confirmed read on the local replica: it reflects every acknowledged write. |
| `DEL key...`, `EXISTS key...`, `DBSIZE` | As in Redis. |
| `PING`, `ECHO`, `INFO`, `COMMAND`, `QUIT` | `INFO` shows the replica role, leader, term and applied index. |

Writes are retried with the same bytes after transient failures. `SET` and `DEL`
are idempotent, so a retry after an uncertain outcome is harmless; non-idempotent
commands such as `INCR` are deliberately not offered.

## Read the code

| Location | What to read there |
| --- | --- |
| [cache/machine.go](cache/machine.go) | The replicated state machine: bounded map, deterministic eviction, snapshot and restore. |
| [server/redis.go](server/redis.go) | Redis commands: writes through `load.ProposeRetry(group.Propose)`, reads through `Group.Read`. |
| [server/runtime.go](server/runtime.go), [server/main.go](server/main.go) | Configuration, `StaticPeers`, `Cluster.EnsureGroup` with WAL storage, a Factory with no `Retire` (no files outside the managed Raft store), shutdown order. |
| [server/health.go](server/health.go), [loadgen/balancer.go](loadgen/balancer.go) | Readiness and quorum checks, 500ms health polling and client-side round-robin selection. |
| [loadgen/](loadgen/) | Workers, bounded request retries, versioned values and read classification. |
| [resp/](resp/) | The minimal RESP2 encoder, parser and client shared by both apps. |

## Verify

```sh
make test                  # includes this module's unit tests
make test-cache-example    # isolated Compose: balancing, failover, restarts, sustained 10-worker reuse and 1/256-worker load
make test-function         # includes a single-replica Redis/recovery test that reopens WAL storage
```
