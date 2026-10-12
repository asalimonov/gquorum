# gquorum

**gquorum is a Go library for building durable, distributed applications with
Raft consensus.** It manages cluster membership, replicated topology, application
replicas, recovery and coordinated shutdown. Your application supplies its state
machines, load transport and deployment settings.

One replicated `_meta` group coordinates the cluster. Independent application
Raft groups, called **load groups**, replicate your application commands and
state. Each group has its own membership, leader, log and snapshots.

## What you can build

- **Long-running transaction processing systems:** replicate workflow state and
  progress so processing can resume after a process restart or leader change.
- **Data storage clusters:** distribute application-defined stores across
  replicated groups with configurable placement and membership.
- **Data stream processing systems:** replicate processing state and checkpoints
  while your application supplies stream ingestion and processing logic.
- **Durable storage services:** recover application state from full snapshots
  and committed log entries using the included WAL backend or your own store.
- **Distributed schedulers and locks:** coordinate workers through replicated
  sessions and locks, and implement durable job state in application groups.
- **Clusters with autoscaling features:** build controllers around node
  admission, drain, removal and replica-placement APIs. Your application supplies
  capacity decisions and infrastructure provisioning.

Application transaction boundaries, external side effects, outboxes and request
deduplication remain application responsibilities. Consensus is per group;
cross-group transactions require application coordination.

## Features

- **Multiple Raft groups:** a shared topology quorum and independent application
  groups built on `go.etcd.io/raft/v3`, with managed local replica lifetimes.
- **Replica-local proposal results:** ready load followers and learners forward
  ordinary writes to their leader and return results after local application,
  with bounded waits and application request IDs preserved across forwarding.
- **Durable recovery:** bbolt-backed metadata, an included etcd WAL backend for
  load groups, full state-machine snapshots, committed-tail replay, compaction
  and snapshot catch-up.
- **Versioned topology administration:** compare-and-swap edits, previews,
  explicit approval, observable operator progress, document history, watches and
  rollback planning. `Latest`, `Target` and `Active` distinguish staged,
  approved and converged topology.
- **Dynamic membership and placement:** founding and joining, caller-defined
  node types, cluster-wide and per-group placement rules, learner-first growth,
  guarded promotion, leadership transfer, drain, activation and exclusion.
- **Identity-aware recovery and cleanup:** retained restarts, fresh-ID
  readmission and replica reseating, parking of missing or foreign storage,
  verified retirement, quarantine and configurable retention. Deleted group IDs
  are permanently retired.
- **Stable learners and voter policies:** new groups default to three voters;
  additional eligible hosts hold promotable learners. Per-group replicated
  policies control voter targets, failure/stability windows and switch cooldowns.
  The default scheduler replaces persistently unhealthy voters with qualified
  learners, with independent authorization and live-quorum checks.
- **Replaceable policies:** customize supported repair decisions through
  `policy.Scheduler`, and startup readiness, retired-storage retention and
  removal behavior through `policy.LifecyclePolicy`.
- **Sessions and distributed locks:** activate and renew process sessions,
  acquire application locks and inspect their locally observed holders.
- **Bounded work and diagnostics:** proposal limits, bounded per-destination
  Raft queues, WAL-stall reporting, replica health, parked admissions and loads,
  scheduling status, local events and an optional persistent event journal.
- **Pluggable application integration:** typed load transport, custom storage,
  address resolution and health observations; included gRPC meta transport with
  TLS configuration and cluster credentials.

The default scheduler preserves healthy voter seats. Existing groups without a
voter policy retain their legacy repair behavior until explicit policy adoption.
`topology.Limits` controls capacity and defaults to three nodes and five total
replicas per load group. Seven-node all-host deployments explicitly configure
both limits to at least seven. All admitted node types participate in `_meta`;
placement rules select load hosts, and voter policy selects their voting roles.
Infrastructure provisioning and automatic data partitioning remain application
responsibilities.

## Architecture and key interfaces

`gquorum.Node` owns one participant, its private topology service and its local
load replicas. `node.Cluster()` provides a stable administration view. Approved
topology plans drive replica startup, membership changes and retirement.

| API | Responsibility |
| --- | --- |
| `gquorum.Node` | Start, readiness, shutdown, local replicas, sessions and typed load-protocol receivers. |
| `gquorum.Cluster` | Read and edit topology, preview and approve plans, follow operators, manage membership and acquire locks. |
| `*load.Group` | Propose application commands, observe leadership and status, request snapshots and inspect proposal pressure for one local replica. |
| `gquorum.GroupFactory` / `load.Application` | Open application resources for an exact approved replica and transfer their release to Node. |
| `load.StateMachine` | Apply commands and implement complete `Snapshot` and `Restore` operations. |
| `load.Transport` | Implement `SendRaft`, `ReadReplica` and `ApplyAction` in the application's authenticated protocol. |
| `storage.Store` / `storage.RetirementStore` | Persist Raft state and collect verified retired storage; `storage/walstore` supplies the included implementation. |
| `policy.Scheduler` / `policy.LifecyclePolicy` | Supply repair and lifecycle decisions while the library enforces authorization and membership safety. |

`NewNode(NodeOptions)` validates settings without opening storage. `Start(ctx)`
acquires resources and begins admission and recovery; `WaitReady(ctx)` waits for
readiness. The context passed to `Start` owns the node lifetime. `Stop(ctx)` joins
shutdown, including managed replicas and application callbacks.

Applications provide identities, paths and listeners directly. For managed loads,
they also provide a factory, transport and storage selection. Node owns replica
orchestration; the application authenticates its protocol and implements its
business state machine. `load.NewStandalone` is available for independent
consensus with caller-owned lifecycle and authorization.

See the [Raft overview](docs/architecture/raft.md) for consensus domains,
elections, heartbeat protocols and durability. See the
[cluster-building guide](docs/architecture/library.md) for integration
contracts and the [ownership and recovery guide](docs/architecture/runtime.md)
for lifecycle details. Document and status reads are observations, not
linearizable read barriers. Forced recovery after permanent majority loss is
not currently implemented.

Nodes have readable string names (`Identity.NodeName`). Status reports the
leader's name as `meta_leader_node_name` and `groups[].leader_node_name`,
alongside the numeric Raft IDs `meta_leader_id` and `leader_id`.

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

## Contributing

Issues and pull requests are welcome. Describe the behavior you want to change
and include a reproducible example for bugs. For changes to public APIs,
membership, storage formats or recovery, explain the intended contract and
compatibility impact.

1. Read [AGENTS.md](AGENTS.md) and the architecture guides above for the package
   map, ownership rules and safety invariants.
2. Make a focused change with regression coverage appropriate to the behavior.
   Keep documentation and examples aligned with public API changes.
3. Run the relevant checks and include their results in your pull request:

   ```sh
   make test       # Compile all three modules and run fast, uncached units
   make lint       # go vet in all three modules
   make test-race  # Fast unit checks under the race detector
   ```

Use `make test-function` for isolated non-Docker functional and storage tests,
`make test-example` for the actual Compose demo and `make test-cluster` for Docker
process tests. `make test-unit` and `make test-unit-race` are aliases for the fast
checks. Functional tests stay beside their packages in `*_func_test.go` files
with the `functional` build tag. Unit files use `!functional`, so the commands
run separate suites. `make test-functional` remains an alias for `make test-function`.
Multi-node scenarios run through the bounded Docker pool under `make test-cluster`;
functional fixtures start at most one local participant. The process suite covers
three-node quorum, faults, recovery and multiple load groups, targeted
four-node admission and voter-swap regressions, five-node automatic learner
promotion with retained return and cooldown, and seven-host capacity validation.
Docker is required for the example and process suites. Protocol
changes use `make generate` and `make proto-check`, which require `protoc` and
install pinned Go generators.

The [testing guide](docs/testing/tests.md) describes test boundaries and commands.
Test targets require Python 3.9+ and save detailed results and Markdown Gantt
timelines under `test-reports/<suite>/latest/`. Each run keeps its own directory.
The [API validation record](docs/testing/api-refactor-validation.md) records the
completed public API refactor checks. The [stable learner validation record](docs/testing/stable-learner-validation.md)
maps voter policies, metrics, forwarding and recovery to their implementation and tests. Separate `examples/cluster` and
`tests/cluster` Go modules keep CLI and Docker dependencies outside the library
dependency graph. The example uses Kong for configuration and `net/http` for its
HTTP adapter; see its README for module-local build and run commands.
