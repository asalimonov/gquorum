# gquorum

**gquorum is a Go library for building durable, distributed applications with
Raft consensus.** It manages cluster membership, replicated topology, application
replicas, recovery and coordinated shutdown. Your application supplies its state
machines, load transport and deployment settings.

One replicated `_meta` group coordinates the cluster. Independent application
Raft groups, called **load groups**, replicate your application commands and
state. Each group has its own membership, leader, log and snapshots.

## Purpose of the library

The library was created for a service which has the following requirements:

- Strict consistency between nodes due to high number of parallel operations with
  single budget and overspending is forbidden
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

- **Multiple Raft groups:** a shared topology quorum and independent application
  groups built on `go.etcd.io/raft/v3`, with managed local replica lifetimes.
- **Replica-local proposal results:** ready load followers and learners forward
  ordinary writes to their leader and return results after local application,
  with bounded waits and application request IDs preserved across forwarding.
- **Consistent reads:** ready voters, followers and learners confirm reads with
  ReadIndex and local apply waiting; optional application queries run serialized
  with Apply/Restore. Authoritative topology and lock reads have explicit APIs.
- **Resource-fenced administration:** immutable grants bind exact effects and
  identities. Supersession requests revocation; durable group/host fencing and
  receipts establish completion, with pending state when required resources
  cannot be reached or lack quorum.
- **Durable recovery:** bbolt-backed metadata, an included etcd WAL backend for
  load groups, full state-machine snapshots, committed-tail replay, compaction
  and snapshot catch-up.
- **In-memory groups:** create a load group with
  `GroupSpec{Storage: topology.StorageMemory}` to keep its Raft state in memory.
  Writes still commit on a quorum. Memory cache has persist on a disk for
  durability, but it easily achieves for a single Raft group 4000 RPS
  and under 1mks for operation on mediocre hardware.
- **Versioned topology administration:** compare-and-swap edits, previews,
  explicit approval, observable operator progress, document history, watches and
  rollback planning. `Latest`, `Target` and `Active` distinguish staged,
  approved and converged topology.
- **Dynamic membership and placement:** founding and joining, caller-defined
  node types, cluster-wide and per-group placement rules, learner-first growth,
  guarded promotion, leadership transfer, drain, activation and exclusion.
- **Label-aware placement:** exact node selectors, reservation of nodes from new
  load replicas, learner-only nodes, and strict voter separation by a caller
  label such as `az`. Placement reports explain excluded hosts and unsatisfied
  groups before approval and in status.
- **Identity-aware recovery and cleanup:** retained restarts, fresh-ID
  readmission and replica reseating, parking of missing or foreign storage,
  verified retirement, quarantine and configurable retention. Deleted group IDs
  are permanently retired.
- **Stable learners and voter configurations:** new groups default to three voters;
  additional eligible hosts hold promotable learners. Per-group replicated
  configuration controls voter targets, failure/stability windows and switch cooldowns.
  The default scheduler replaces persistently unhealthy voters with qualified
  learners, with independent authorization and live-quorum checks.
- **Repair policies and lifecycle hooks:** customize supported repair decisions through
  `scheduling.RepairPolicy`, and startup readiness, retired-storage retention and
  removal behavior through `lifecycle.Hooks`.
- **Sessions and distributed locks:** activate and renew process sessions,
  acquire application locks and inspect their locally observed holders.
- **Bounded work and diagnostics:** proposal limits, bounded per-destination
  Raft queues, WAL-stall reporting, replica health, parked admissions and loads,
  scheduling status, local events and an optional persistent event journal.
- **Pluggable application integration:** typed load transport, custom storage,
  address resolution and health observations; included gRPC meta transport with
  TLS configuration and cluster credentials.

The default scheduler preserves healthy voter seats. Existing groups without a
voter configuration retain their legacy repair behavior until explicit
configuration adoption.
`topology.Limits` controls capacity and defaults to three nodes and five total
replicas per load group. Seven-node all-host deployments explicitly configure
both limits to at least seven. All admitted node types participate in `_meta`;
placement rules select load hosts, and voter configuration selects their voting roles.
Infrastructure provisioning and automatic data partitioning remain application
responsibilities. Resource measurement and resource-based placement decisions
also belong to the application. See [labels and placement](docs/architecture/placement.md)
for `GetLabels`/`SetLabels`, selector inheritance, safe relocation and the
staging/approval workflow.

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
in-memory map bounded to 512 keys with deterministic eviction, and its group
uses disk-backed Raft logs and snapshots: a restarted process replays its state
under the same replica ID. The Compose demo skips load WAL/applied-state fsync
for speed; host crashes or power loss can lose acknowledged writes. A separate load generator
writes through one random node and reads from another with several workers.

```sh
make run-cache-example
make test-cache-example
docker compose -f examples/inmemory-cache/compose.yaml logs -f loadgen
make clean-cache-example
```

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
   each commit. Use `make lint-staged` to run those checks manually.
3. Run all tests before sending of a request:

   ```sh
   make test          # Compile all four modules and run fast, uncached units
   make lint          # vet, revive style and unused-code checks across modules
   make test-race     # Fast unit checks under the race detector
   make test-function # Isolated non-Docker functional and storage tests
   make test-cluster  # Cluster tests
   ```

The [testing guide](docs/testing/tests.md) describes test boundaries and commands.
Test targets require Python 3.9+ and save detailed results and Markdown Gantt
timelines under `test-reports/<suite>/latest/`. Each run keeps its own directory.
The [API validation record](docs/testing/api-refactor-validation.md) records the
completed public API refactor checks. The [stable learner validation record](docs/testing/stable-learner-validation.md)
maps voter configurations, metrics, forwarding and recovery to their implementation and tests.
