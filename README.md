# gquorum

**gquorum is a Go library for building durable, distributed applications with
Raft consensus.** It manages cluster membership, replicated topology, application
replicas, recovery and coordinated shutdown. Your application supplies its state
machines, load transport and deployment settings.

One replicated `_meta` group coordinates the cluster. Independent application
Raft groups, called **load groups**, replicate your application commands and
state. Each group has its own membership, leader, log and snapshots.

## Purpose of the library

The library was created for network services which have the following requirements:

- Strict consistency between nodes due to high number of parallel operations with single
  entity like "budet" or "quota"
- Reduce issues with locks in OLTP database
- Minimze risks of loosing user's requests
- Horizonal scaling with keeping low latency for R/RW operations despite on
  its architecture as single rank application cluster
- Application should work with high SLI on a hardware without enterprise-grade
  lossless network
- Handle long lived transactions up to 30 minutes

Use cases where the library shines:

- Need to run a cluster without external coordinators and prevent split-brain
  issues
- You want to create/use durable queue with quorum commit w/o heavy dependencies
- You need to implement `Strong Read-After-Write Consistency` for a storage
- The application should guarantee QoS for resource heavy operations and support
  scaling via partitioning of sharding
- Need to create a distributed scheduler with at-least once gurantee of execution
  of tasks
- You have high volume of tansactions which tighly bind with each other and
  regular SQL database cannot handle due to frequent locks on rows.
- You need to implement automatic restoraion of nodes and automate update software,
  maintenance and decomission of nodes without loosing of user's requests
- You want to implement business transactions in native way with natural handling
  of inperfections of real-world hardnware, software and operation processes

Where the library can help with clustering but Raft is not suitale protocol
for payaload: low latency caches, fire-and-forget queues, cheap broadcasts,
RDMA-storages.

Where it is not recommended: [AP systems](https://www.researchgate.net/publication/392020831_Architectural_Tradeoffs_in_CP_vs_AP_Systems_for_Global-Scale_Applications),
pure in-memory cache, stateless applications and ephemeral disks.

Performance of a single thread worker with single Raft group on 3-node cluster:

- under 400us for a quorum write
- under 450us for a quorum read (from any node)
- above 2000 RPS

## Features

The features which the library propose in comparision with using coordination via
Raft and CRDT libraries, Zookeeper or PostgreSQL:

- **Cluster management:** the library already knows how to manage cluster via its
  topology, host states and healths.
- **Automatic cluster health:** the library already has mechanisms for testing
  reachability of each host (p2p), its statuses and can aggreagate all
  information for cluster leader and calculation of global cluster state.
- **Multiple Raft groups:** a shared topology quorum and independent application
  groups built on enterprise grade stable `etcd` Raft library. Allow to scale
  clusters on tens hosts.
- **Scheduling and automatic reapair:** the library already has built-in scheduler
  for placement of load groups and policies for automatic repair of nodes after
  incidents or intermittent issues with gating user's traffic from temporary 
  unavailable nodes.
- **Stable learners and voter configurations:** scaling cluster horizontally without
  loosing of time on large quorum. The scheduler and policies replace persistently
  unhealthy voters with qualified learners, with independent authorization and
  live-quorum checks.
- **Versioned topology administration:** compare-and-swap edits, previews,
  explicit approval, per-group convergence and operation progress, document
  history, watches and rollback planning. Explicit `RetryOperation` reuses an
  immutable failed plan; exact CAS replay recovers uncertain mutation replies.
  `Latest` is staged intent; `Target`
  is approved intent; `ReadProgress` reports group convergence independently.
- **Label-aware placement:** exact node selectors, reservation of nodes from new
  load replicas, learner-only nodes, and strict voter separation by a caller
  label such as `az`. Placement reports explain excluded hosts and unsatisfied
  groups before approval and in status.
- **Repair policies and lifecycle hooks:** customize supported repair decisions through
  `scheduling.RepairPolicy`, and startup readiness, retired-storage retention and
  removal behavior through `lifecycle.Hooks`.

### Other

- **Proposal receipts:** writes report whether they were applied, refused before
  submission, or have an unknown outcome. Supply a stable RequestID; applications
  own deduplication. Optional retries repeat only proven transient refusals.
- **Consistent reads:** ready voters, followers and learners confirm reads with
  ReadIndex and local apply waiting; optional application queries run serialized
  with Apply/Restore. Authoritative topology and lock reads have explicit APIs.
- **Resource-fenced administration:** immutable grants bind exact effects and
  identities. Supersession requests revocation; durable group/host fencing and
  receipts establish completion, with pending state when required resources
  cannot be reached or lack quorum.
- **In-memory groups:** create a load group with
  `GroupSpec{Storage: topology.StorageMemory}` to keep its Raft state in memory.
  Writes still commit on a quorum. Memory cache has persist on a disk for
  durability, but it easily achieves for a single Raft group 4000 RPS
  and under 1mks for operation on mediocre hardware.
- **Permanent majority loss:** recover one complete survivor into a fresh cluster,
  then start an ordinary Node on the same roots.
- **Large load snapshots:** bounded chunked transfers run alongside ordinary Raft on separate connections to the existing load port. Configure `Loads.SnapshotTransfer` at Node creation; the default encoded allowance is 256 MiB, and larger sizes are supported by configuration. Full-buffer application/storage APIs require memory for multiple snapshot-sized buffers. See [transport and resource contracts](transport/loadgrpc/README.md).
- **Durable recovery:** bbolt-backed metadata, an included etcd WAL backend for
  load groups, full state-machine snapshots, committed-tail replay, compaction
  and snapshot catch-up.
- **Dynamic membership and placement:** founding and joining, caller-defined
  node types, cluster-wide and per-group placement rules, learner-first growth,
  guarded promotion, leadership transfer, drain, activation and exclusion.
- **Identity-aware recovery and cleanup:** retained restarts, fresh-ID
  readmission and replica reseating, parking of missing or foreign storage,
  verified retirement, quarantine and configurable retention. Membership removal,
  joined host stop, retirement and physical collection have separate durable
  milestones; offline former hosts retain cleanup debt. Deleted group IDs
  are permanently retired.
- **Sessions and distributed locks:** activate and renew process sessions,
  acquire application locks and inspect their locally observed holders.
- **Bounded work and diagnostics:** proposal limits, bounded per-destination
  Raft queues, WAL-stall reporting, replica health, parked admissions and loads,
  scheduling status, local events and an optional persistent event journal.
- **Pluggable application integration:** typed load transport, custom storage,
  address resolution and health observations; included gRPC meta transport with
  TLS configuration and cluster credentials.

## Getting started

The module requires Go 1.26; the repository pins the Go 1.26.5 toolchain.

```sh
go get github.com/asalimonov/gquorum
```

To explore cluster administration, clone the repository and run the
[three-node example](examples/cluster/README.md) with Docker Compose:

```sh
make run-example
curl -fsS http://localhost:18081/api/cluster/status
make stop-example
```

The example demonstrates `_meta` bootstrap, status, topology approval, drain and
activation. Stopping preserves its durable volumes. For complete load integration,
read the [test application](tests/cluster/testapp/README.md), which implements a
key/value state machine, typed transport and application retirement using the
public `Node` and `Cluster` APIs.

The [replicated scheduler example](examples/scheduler/README.md) demonstrates a
complete application: a leader schedules tasks, followers pull their committed
assignments and execute them, and a SQLite/XORM state machine commits
assignments and results through consensus. It uses `Cluster.EnsureGroup`,
`StaticPeers`, managed application callbacks, leadership scopes with
`Leadership.Replicas`, applied-index watches, `load.ProposeRetry`, and the
reusable [load gRPC adapter](transport/loadgrpc/README.md); it has no
application RPC.

```sh
make run-scheduler-example
make test-scheduler-example
make stop-scheduler-example
```

The [replicated in-memory cache example](examples/inmemory-cache/README.md)
serves a subset of the Redis protocol from three nodes. Its state machine is an
in-memory map that grows without automatic eviction or TTL, and its group
uses disk-backed Raft logs and snapshots: a restarted process replays its state
under the same replica ID. The Compose demo skips load WAL/applied-state fsync
for speed; host crashes or power loss can lose acknowledged writes. A separate load generator
balances writes and reads across healthy nodes using reusable connections,
500ms HTTP health checks and 1–256 workers selected through `CACHE_WORKERS`.
The workload uses 80% reads and 20% writes across 615 keys by default; reads of
unpopulated keys are normal cache misses. Set `CACHE_LOADGEN=0` to run only
the three nodes.

```sh
make run-cache-example
make test-cache-example
docker compose -f examples/inmemory-cache/compose.yaml logs -f loadgen
make clean-cache-example
```

## Performance investigation

Run `make profile-cache` to build and profile an isolated cache cluster. It saves
raw pprof/syscall evidence, an interactive heatmap and flame graph, and CSV/JSON
statistics under `test-reports/performance/`. See the
[profiling guide](docs/testing/performance-profiling.md) for options and analysis
of existing applications or saved profiles.

## Contributing

Issues and pull requests are welcome. Describe the behavior you want to change
and include a reproducible example for bugs. For changes to public APIs,
membership, storage formats or recovery, explain the intended contract and
compatibility impact.

1. Read [AGENTS.md](AGENTS.md) and the architecture guides above for the package
   map, ownership rules and safety invariants.
2. Make a focused change with regression coverage appropriate to the behavior.
   Keep documentation and examples aligned with public API changes.
   Run `make install-hooks` once to enable lint checks for staged files before
   each commit. Use `make lint-staged` to run those checks manually, or
   `make lint` to check all pending staged, unstaged and untracked changes.
3. Run all tests before sending of a request:

   ```sh
   make test          # Compile all five application modules and run fast, uncached units
   make lint          # vet, revive style and unused-code checks across modules
   make test-race     # SLOW unit checks under the race detector
   make test-function # SLOW isolated non-Docker functional and storage tests
   make test-cluster  # VERY SLOW cluster tests, run only after large tasks or several large commits
   make test-sim      # SLOW deterministic three-node latency and partition simulations
   ```

The [testing guide](docs/testing/tests.md) describes test boundaries and commands.
Test targets require Python 3.9+ and save detailed results and Markdown Gantt
timelines under `test-reports/<suite>/latest/`. Each run keeps its own directory.
The [baseline validation record](docs/testing/validation.md) retains historical
pre-refactor results; run the current suites for new changes.

## License

Copyright 2026 Aleksandr Salimonov.

Licensed under the [Apache License, Version 2.0](LICENSE).
