# Working on gquorum

## Project and current API

gquorum is a Go library for durable clustered applications. It owns a replicated
`_meta` topology group and managed application Raft replicas (load groups).
Applications supply state machines, authenticated load transport, identities,
paths, listeners and deployment configuration. Use cases include transaction
processing, data and stream storage, durable jobs, distributed locks and
application-controlled scaling.

The module is `github.com/asalimonov/gquorum`. `go.mod` requires Go 1.26.0 and
pins toolchain Go 1.26.5. Core dependencies are etcd Raft, bbolt, etcd WAL, gRPC
and protobuf. Five application modules are the root, `examples/cluster`,
`examples/scheduler`, `examples/inmemory-cache` and `tests/cluster` (including the
caller fixture). Kong and Docker tooling stay outside the library module.

The public API refactor established these boundaries:

- `NewNode(NodeOptions)` constructs the participant; `Node` owns its lifecycle
  and managed replicas. `node.Cluster()` is an administration view with no
  independent lifetime.
- `*load.Group` represents one exact local replica and exposes application
  consensus. It has no independent start/stop or raw membership controls.
- `GroupFactory`, `load.StateMachine`, `load.Transport`, storage interfaces and
  `scheduling.RepairPolicy` and `lifecycle.Hooks` are application extension points.
- The former public `Runtime`, `OpenMeta` and `OpenLoad` APIs were removed.
  Keep orchestration in the library and applications on public APIs.
  `load.NewStandalone` is the explicit low-level alternative with a separate owner.

## Read before changing behavior

Start with [README.md](README.md), then the relevant contracts:

- [Raft overview](docs/architecture/raft.md): consensus scope, elections,
  heartbeat protocols, proposal ordering, persistence and group boundaries.
- [Library architecture](docs/architecture/library.md): public handles, topology
  workflow, integration interfaces and package boundaries.
- [Node ownership and recovery](docs/architecture/runtime.md): durable evidence,
  startup, shutdown, retirement, readmission and callback ownership.
- [Testing guide](docs/testing/tests.md): suite boundaries and commands.
- [API refactor validation](docs/testing/api-refactor-validation.md) and
  [audit](docs/testing/api-refactor-audit.md): completed coverage and known scope.
- [Physical compatibility](docs/testing/api-baseline-physical.md): fixture
  provenance and immutable hashes for retained storage and protocols.

Inspect implementation and nearby tests when changing a contract.
`docs/brainstorms` and `docs/plans` retain history; some describe superseded APIs.
Reconcile them with current code and architecture docs. Use
`git log -15 --oneline` for recent context. The refactor's final fixes emphasize
startup authorization before publication, scheduler revalidation, preserved
cleanup evidence and shutdown release ownership.

## Repository map

| Path | Responsibility |
| --- | --- |
| `node*.go`, `cluster*.go`, `doc.go` | Public API, validation, lifecycle composition and administration. |
| `topology/` | Documents, identities, placement, scoped operations, progress and validation. |
| `load/` | State-machine and transport contracts, replica handles and standalone owner. |
| `scheduling/`, `lifecycle/` and their test-helper packages | Repair policies, local lifecycle hooks and caller contract helpers. |
| `storage/`, `storage/walstore/`, `storage/memstore/`, `storage/storagetest/` | Persistence/retirement contracts, WAL backend, in-memory store and conformance helpers. |
| `events/`, `fault/`, `membership/` | Local events/journal, classified errors and membership safety. |
| `internal/managed/`, `internal/loadhandle/` | Exact replica ownership, recovery, approved effects and retirement. |
| `internal/metaservice/` | Topology service, admission, documents, sessions and protocol integration. |
| `internal/metaengine/`, `internal/metastore/` | Replicated metadata FSM and bbolt persistence, pins and ledgers. |
| `internal/metatransport/`, `internal/metapb/` | Meta networking, protobuf schema and generated code. |
| `internal/operator/`, `internal/scheduling/`, `internal/observe/` | Approved plan execution, scheduling and health observations. |
| `internal/raftengine/`, `internal/raftlog/` | Shared Raft loop, replay, persistence, bounded queues and logging. |
| `examples/cluster/` | Separate Go module: Kong CLI/configuration, cluster construction/readiness in `cluster.go`, HTTP adapter in `http.go` and process wiring in `main.go`. |
| `tests/cluster/testapp/` | Caller FSM, load protocol, retirement, unit/functional fixture tests and authenticated fault controls. |
| `tests/cluster/` | Separate module for Docker process, fault, scale, performance and Compose tests. |
| `scripts/test_report.py` | Shared test runner: Meridian-style stdout summaries, persistent package logs/JUnit and Markdown timelines. |

## Contracts to preserve

### Topology and authorization

- `Latest` may contain unapproved intent; `Target` is approved desired topology.
  Group/node progress retains independent proven declarations and source versions.
  `FullyConverged` is historical; there is no Active document or global execution
  cursor. Staging alone never authorizes effects.
- Mutations use expected-version compare-and-swap. Preview cannot combine with
  approval. If staging succeeds and approval fails, return the staged version
  so the caller can retry that version's approval.
- Execute immutable approved plans. Verify the complete
  operation/version/attempt/step/action/identity tuple, including after waits.
  A refusal or incomplete acknowledgement is never completion proof.
- Approve returns the retained first attempt; RetryOperation explicitly creates
  one immediate successor of a failed attempt with the same immutable plan.
  Replay uncertain mutations with their original CAS, actor/comment and exact
  predecessor IDs. Disjoint repair atomically rebases unapproved intent; inspect
  typed RebasedTo conflicts rather than approving whichever version is Latest.
- Per-group owners and state-based admission/evacuation barriers govern effects.
  Unrelated Target advancement, staged intent or offline cleanup cannot globally
  block healthy groups. Shared quorum and exact conflicting resources still gate.
- ReadProgress confirms one bounded metadata page, attaches only exact cached
  observations with original timestamps and never fans out. Its cursor is not a
  frozen snapshot; operation aggregate status remains complete on truncated pages.
- Preserve learner-first membership, guarded promotion, leadership transfer
  before leader removal/demotion and post-change live-quorum checks. Policies
  and serve overrides cannot bypass core safety checks.
- `topology.Limits` is the sole capacity setting: defaults are three nodes,
  five replicas per load group, four executing groups and two cleanup requests.
  Execution limits are immutable replicated settings; retained proof has no
  global count cap. Avoid fixed cluster-size assumptions. All admitted node
  types join `_meta`; type-based placement controls load hosting.
- Topology/status/lock-holder reads are observations, not linearizable barriers
  or proof of quorum.

### Identity, persistence and retirement

- Keep string node names (`Identity.NodeName`, `Node.Name`) distinct from
  numeric `_meta` `MemberID` and group `ReplicaID`. Status adds
  `MetaLeaderNodeName`/`LeaderNodeName` alongside existing numeric
  `MetaLeaderID`/`LeaderID`. Resolve names through approved/proven exact
  group/replica identity and retained meta owners, including cached status; preserve unknown names and observed IDs.
- Distinguish durable identity from process incarnation. Retained restarts
  preserve pins and supply a fresh `InstanceID`. Replacements use fresh monotonic
  replica IDs; deleted group IDs cannot be reused.
- Validate pins, approved topology, ledgers and actual storage before acquisition
  or recovery. Park missing, foreign, unreadable or contradictory state with its
  evidence intact. Never silently refound an established identity after log loss.
- Restore the complete application snapshot and committed tail regardless of
  applied hints. State-machine calls are serialized; `Apply` errors halt the
  replica, while deterministic business rejections belong in `Result.Code`.
- Proposal cancellation only ends a wait; it cannot undo a committed command.
  Preserve request IDs and deduplication across retries/replay. External
  transactions and outboxes remain application responsibilities.
- Retirement verifies exact identity and the entire approval or readmission
  proof, even after data is already absent. A bare `Verified=true` is insufficient.
  Preserve foreign/successor resources. Quarantine and collection recheck
  identity, ledger, assignments and opaque retirement tokens.
- Preserve approved cleanup/recovery evidence through history pruning until
  obligations are proven complete. Readmission fences and joins the old owner
  before installing the exact granted replacement identity.
- Separate native removal, joined stop, retirement and physical collection.
  Whole-group deletion requires exact stop/readmission proof for every former
  owner; detached collection does not delay group convergence. Missing data alone
  is not collection proof. Custom RetirementStore must implement ForgetCollection.
  Persist preparation before Collect, completion before report, and release the
  exact store marker before removing the collected journal row. Permanently lost
  hosts retain unconfirmed debt; no destroyed-host waiver is implemented.
- Metadata store/FSM/envelopes are v4 and authority journal is v2. Older changed
  formats refuse with evidence intact; no migration/mixed-version fallback.
  Preserve both legacy rejection fixtures and new frozen scoped fixtures.
- Applied, durably authorized self-removal fences participation before optional
  lifecycle decisions. Drain or an unapproved removal does not provide that proof.
- The approved alpha policy-naming refactor changes voter configuration and
  scheduling-status JSON names without migration or mixed-version support.
  Preserve unrelated persisted encodings across public API renames. Legacy names in
  events, metadata and wire values can be compatibility requirements. Never
  regenerate baseline files in `events/testdata`, `load/testdata/api-baseline`
  or `internal/metaengine/testdata/api-baseline` with current code to make tests pass.

### Concurrency and resource ownership

- `NewNode` validates/copies settings without I/O or application callbacks.
  `Start` acquires resources and returns before quorum readiness. Its context
  owns the lifetime; canceling readiness or a start waiter does not stop an
  accepted replica.
- One startup reservation owns each group. Concurrent starts for an exact
  replica share one result. Publish handles and observations only after replay
  readiness and startup authorization finish. Stopped handles never redirect
  to successors.
- Successful factories transfer `Application.Close` to Node, including late
  success after waiter cancellation. Failed factories release their own partial
  acquisitions. Join engine/replay work before application release.
- `StopLoadGroup` installs an exact local hold without deleting durable state.
  Recovery respects it; explicit start clears only the matching hold. Approved
  retirement remains possible while held.
- `RequestStop` begins teardown; `Stop(ctx)` joins it. Timeouts and failed Close
  retain ownership for serialized retries. Never overlap replacements with
  unresolved owners or close meta storage before managed work and callbacks join.
- Injected stores, transports, event buses and journals are borrowed. WAL storage
  selected through `LoadStorageOptions.WAL` and the in-memory store used by
  `topology.StorageMemory` groups are Node-owned. The meta listener
  transfers when startup acquires the service.
- Serialize scheduler calls per node; readiness, retention and removal share a
  separate serialized lifecycle runner. Inputs are owned copies. Overdue
  callbacks stay owned until actual return, without overlapping replacements.
  Callbacks may read status or request stop, but must not wait for teardown that
  joins themselves.
- Authenticate load senders before dispatching to Node. Preserve exact endpoints
  and delivery IDs so stale reports cannot affect successors. Keep queues bounded,
  destinations independent and send workers joined.

### Policy scope and application boundaries

The built-in repair policy converges replicated voter configuration, including
`_meta`, by selecting guarded promotions, demotions and stable-learner
replacements. Groups without voter configuration retain missing-load-voter
repair. It preserves pinned learners and healthy seats, never approves unrelated
staged user intent and does not balance healthy groups. The internal scheduler
rechecks topology, health, native membership and leadership after callbacks and
guards approval with replicated CAS. Local readiness, retention and removal
callbacks belong to `lifecycle.Hooks`. Repair implementation consistency remains
caller-owned; replicated implementation agreement is deferred.

Applications build autoscaling controllers with membership and placement APIs.
Infrastructure provisioning, workload scheduling and stream ingestion stay in
the application. Forced recovery after permanent majority loss is deferred.
Events are node-local and never replicated through `_meta`.

Keep `examples/cluster` small and meta-only. Full load integration and fault
controls belong in `tests/cluster/testapp`, using public APIs. Functional tests,
including private ledger and engine assertions, stay in their owning packages.
They may start one local participant or protocol doubles. Multi-node consensus,
admission and application scenarios belong to the Docker pool under
`tests/cluster`, even when their former fixtures ran inside a single Go process.
The library does not load application configuration or choose implicit storage roots.

## Development and verification

Use the root [Makefile](Makefile); root `go test ./...` alone does not cover the
separate example and test modules. Choose checks according to the change:

| Command | Coverage |
| --- | --- |
| `make test` / `make test-unit` | Compile all five application modules and run fast, uncached units with default build tags. |
| `make lint` | Vet, revive and merged unused analysis across all modules/build tags, including compile-only process tests. |
| `make test-race` / `make test-unit-race` | Fast unit checks under the race detector. |
| `make test-function` / `make test-function-race` | Non-Docker functional tests using `-tags=functional`, with optional race detection. |
| `make test-example` | Actual demo; requires Docker Compose. |
| `make test-cluster` | VERY SLOW Docker process scenarios with the `clustertest` tag. Run only at the end of a set of changes/commits, not after small or intermediate changes. |
| `make test-perf` / `make bench` | Process performance/resource gates or planner/snapshot benchmarks. |
| `make generate` / `make proto-check` | Regenerate protocols / require no generated diff; needs `protoc`. |

For Go changes, run focused regressions and relevant unit/vet checks. Use race
checks for concurrency changes and integration/process coverage for behavior
crossing replica or process boundaries. For documentation-only changes, verify
paths, API names, commands and `git diff --check`. Report what actually ran and
any unverified scope; historical validation records are not a new test run.

Test targets require Python 3.9+ and write a fresh run directory under
`test-reports/<suite>/`; `latest` links to the latest completed run. Read
`report.md`, `timeline.md` and package logs for failures. Summary counts include
parent tests and subtests, unlike the top-level source inventory. Preserve Go
failure exit codes and reports for build failures, timeouts and interruptions.
When changing the runner, run
`python3 -m unittest discover -s scripts -p '*_test.py'` and exercise the relevant
Make targets. `TEST_REPORTS_DIR` overrides the report root.

Keep `make test` lightweight. Units must not start network services, mocked
services, application replicas or subprocesses. Non-Docker functional tests stay
beside their owning packages in `*_func_test.go` files with
`//go:build functional`. Unit and benchmark files use `//go:build !functional`.
Keep shared helpers in untagged `*_helpers_test.go` files; exclusive helpers stay
with their suite. Separate each build constraint from the package clause with a
blank line. Select suites through build tags rather than short-mode skip guards.
`make test-functional` and `make test-functional-race` remain aliases for the
`test-function` targets. Tests against separately running applications belong in
`tests/cluster` and use the `clustertest` tag and Docker; Compose tests use `example`.
Keep isolated units parallel with temporary fixtures. Do not mutate
process environment, working directory or the upstream WAL segment-size global
in parallel tests.
The uncached unit target should remain below 30 seconds on the documented
machine; `UNIT_PACKAGES` defaults to four and `UNIT_PARALLEL` to eight.

Independent functional tests also call `t.Parallel()` before acquiring fixtures.
Use private temporary roots, ephemeral listeners, per-test transports and joined
cleanup. Parallelize table cases only when each case owns its fixture; keep
mutations of a shared component or store sequential within a parent test.
Do not add multi-node fixture helpers to functional packages; use the existing
Docker pool and authenticated fixture controls for those scenarios.
`FUNCTIONAL_PACKAGES` defaults to four and `FUNCTIONAL_PARALLEL` to eight for
ordinary and race runs, bounding concurrent I/O across functional packages.

Use three nodes for ordinary cluster, quorum, fault and performance coverage.
Do not repeat these scenarios at five or seven nodes. A larger cluster test must
name a specific regression that cannot be exercised at three nodes; currently
four-node admission/promotion, durable admission-pin restart and the strict
three-AZ voter replacement with a same-AZ standby need that capacity.

Process cases call `t.Parallel()` before acquiring isolated resources. Independent
admission-phase subtests also run in parallel; subtests sharing
a lease stay sequential. `CLUSTER_PARALLEL` defaults to eight, and the pool
capacity and concurrent startup limit follow Go's `-parallel` setting unless
overridden with `GQUORUM_TEST_POOL_SIZE` or `GQUORUM_TEST_COOK_PARALLEL`.
Dedicated `make test-perf` runs default to `PERF_PARALLEL=1` for comparable
latency measurements. Build with
`make test-cluster-build`, then focus with
`make test-cluster-run CLUSTER_ARGS='-v -run TestName'`. Rebuild after application
changes; do not replace a shared image tag during a run. Preserve verified
container, volume and worker cleanup. Prefer explicit barriers and observable
state for race regressions; retain durability, identity and resource-release
assertions when adapting tests.

Format Go changes with `gofmt`. Edit `.proto` sources and use the pinned
`make generate` generators instead of hand-editing `.pb.go`. Update architecture
docs when contracts change, and keep README claims and examples aligned with
implemented behavior.
