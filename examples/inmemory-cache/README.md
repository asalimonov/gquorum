# Replicated in-memory cache with Redis protocol and quorum read

Two applications built on gquorum:

- **`server/`**: a cache node speaking a subset of the Redis protocol. Three
  nodes host one `cache` load group; every write is replicated through Raft and
  every node can serve reads. The map grows as new keys are written. Keys remain until explicitly deleted
  with `DEL`; there is no automatic eviction or TTL.
- **`loadgen/`**: a stress client. Workers own disjoint portions of **615** generated
  keys by default, configurable through `CACHE_KEYS`. Each cycle sends one SET
  and four GETs, independently choosing a key for every operation: **80% reads,
  20% writes**. Reads include keys not yet written. The loader selects healthy
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
redis-cli -p 16382 DBSIZE                    # current number of keys
make stop-cache-example                      # stop all nodes; keeps Raft and topology volumes
make clean-cache-example                     # delete containers and volumes
```

To run only the three cache nodes:

```sh
CACHE_LOADGEN=0 make run-cache-example
# Also works directly with docker compose:
CACHE_LOADGEN=0 docker compose -f examples/inmemory-cache/compose.yaml up -d --build --wait
```

`CACHE_LOADGEN` controls the loader's container count: **0** disables it and
**1** (the default) enables it. Running `up` with `CACHE_LOADGEN=0` also removes
an existing loader container. Omit the variable or set it to `1` to enable it again.

`stop-cache-example` preserves data for the next start. `clean-cache-example`
deletes it. Existing volumes from the former memory-storage demo need a one-time
explicit clean before using this version: a group's storage class is immutable.

Nodes listen for Redis on **16381–16383**, bound to loopback. Override them
with `CACHE_NODE1_PORT`…`CACHE_NODE3_PORT`. `CACHE_WORKERS` controls parallelism
from **1 to 256**, default **8**; values outside that range are rejected.
`CACHE_DURATION` defaults to `0s` (run until stopped). Pass these environment
variables when running `make run-cache-example`. `CACHE_KEYS` overrides the
615-key default and must be at least the worker count. Each run uses a fresh key
namespace so old values cannot be mistaken for this run's versions. Reads can
miss before a key is first written; with a fixed key set, misses decrease as
the map fills. Missing keys are expected and never fail the run. The key count
limits the loader's working set, not server storage. Keys from earlier runs
remain until explicitly deleted or the example's data volumes are cleaned.

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

Workers reuse Redis connections. Each node's pool has
`max(1, ceil(CACHE_WORKERS / node_count))` slots, so ten workers across three nodes
use at most **12 Redis connections**. A failed connection is discarded before
retrying, and the next attempt can select another healthy node. Reuse avoids
source-port exhaustion and the CPU cost of reconnecting for every command.

The load generator prints one line every five seconds. `rate`, `write_rate`, `write_avg`
and `read_avg` cover only the last interval, so a slowdown shows up at once;
the `final` line covers the whole run:

```
progress elapsed=20s write_rate=600/s rate=3000/s write_avg=2.6ms read_avg=3.1ms total_writes=12000 total_reads=48000 hits=40000 misses=8000 uncertain=0 stale=0 corrupt=0 write_errors=0 read_errors=0 connections=9 dials=9 healthy=node1:6379,node2:6379,node3:6379
```

The sample output above illustrates the report format; it is not a benchmark
for the WAL configuration. Adjust `CACHE_WORKERS` to explore batching and
throughput on your machine.

| Field | Meaning |
| --- | --- |
| `hits` | The read returned the last acknowledged version of its key, or a newer one. |
| `misses` | A successful GET returned nil: the key has not been populated or was explicitly deleted. Expected, counted in throughput, and never an error. |
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
| [cache/machine.go](cache/machine.go) | The replicated state machine: growing key/value map, snapshot and restore. |
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

## CPU tuning and profiling

For a complete isolated capture with retained binaries, profiles, syscall traces
and an offline interactive report, run `make profile-cache`. It builds its own
three-node cluster and leaves an existing example running. See the
[profiling guide](../../docs/testing/performance-profiling.md).

Compose defaults `GOMAXPROCS` to **1 per process** for this small, single-group
workload. On the 14-vCPU Docker VM used for profiling, allowing every process to
schedule across all CPUs caused substantial thread-wakeup overhead. This setting
limits parallel Go execution, not the number of goroutines, loader workers or
Raft peers. Override it independently of worker count, for example:

```sh
CACHE_WORKERS=10 CACHE_GOMAXPROCS=2 make run-cache-example
```

Measure on your own machine when changing group count or workload. The library
and server binary do not change the application's Go scheduler settings.

`rate` counts successful writes plus completed reads per second, including
cache misses and excluding health probes.
`write_rate` counts just writes. Each worker sends one SET followed by four
independently selected, quorum-confirmed GETs. With one worker, total request
throughput is approximately `5 / (write_avg + 4 * read_avg)`. Increasing workers
allows independent requests to overlap; it does not weaken read consistency.

Profiling is disabled by default. Both binaries accept `CACHE_PPROF_LISTEN`
(or `--pprof-listen`). The optional Compose overlay exposes Go's CPU, heap,
goroutine and execution-trace endpoints on host loopback ports 6061–6063 for
nodes and 6064 for the loader:

```sh
CACHE_WORKERS=1 docker compose -f examples/inmemory-cache/compose.yaml \
  -f examples/inmemory-cache/compose.profile.yaml up -d --build
curl -fsS 'http://127.0.0.1:6061/debug/pprof/profile?seconds=30' -o node1.cpu
curl -fsS 'http://127.0.0.1:6064/debug/pprof/trace?seconds=2' -o loader.trace
go tool pprof -http=127.0.0.1:8090 node1.cpu
go tool trace loader.trace
```

CPU profiles measure time executing on CPU, including sampled system calls;
they do not measure time blocked waiting for a network response or quorum.
Use execution traces for those waits. Profiling shutdown cancels active profile
requests and joins its HTTP server. Normal `make run-cache-example` removes the
optional profiling listener and port mappings when recreating the services.
