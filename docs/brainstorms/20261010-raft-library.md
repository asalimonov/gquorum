# Raft library brainstorm

Date: 2026-10-10. Status: research and API modelling in progress; this is not an implementation plan or a claim of working code.

## Initial request

Design and ultimately implement a reusable Go library by extracting the existing cluster framework and governance integration from the supplied source checkout. The new library must stand alone, without dependencies on, or product references to, that application.

Required scope:

- A reserved `_meta` Raft group owns cluster topology and preserves the current topology finite state machine, including approved, resumable effects.
- The topology document supports caller-defined node types.
- Applications can create additional load groups with application-defined state machines.
- Preserve bbolt persistence for `_meta` and etcd-format WAL persistence for load groups.
- Provide `_meta` networking and its protocol. Load-group protocols and networking belong to the application.
- Constructors receive settings through arguments. No application configuration reader, environment precedence, or implicit local storage paths.
- Expose lifecycle/quorum status, proposals and mutations, document reads/history/watches, cluster/quorum events, and integration operations for every approved effect.
- Require idempotent callbacks and verification of their exact authorized identities.
- Events are never stored in replicated `_meta` state.
- Supply a small Docker Compose example with `make run-example`, HTTP REST administration, and node-state/drain demonstrations.
- Port all source cluster tests that verify applicable Raft correctness, including their supporting unit tests and failure scenarios. Larger-cluster correctness, admission, planner cost, snapshot size and bounded shutdown are main scope; retain simple unit tests and isolated cluster tests as the testing approach.

The explicitly invoked brainstorm workflow is read-only for implementation code and permits writing this session log and, after decisions are resolved, a detailed target architecture. No source checkout changes are authorized or needed for this research.

## Repository and research baseline

The destination checkout contains only its initial Git commit, `117e11d`, and no Go module, README, architecture directory, or `project.txt`. The source path was supplied directly in the request, so no missing-file question is necessary. The source checkout inspected has HEAD `fc982c0`. No source files have been changed and no source tests have been run.

Inspected source areas, with paths relative to the supplied checkout:

| Area | Evidence | Extraction implication |
| --- | --- | --- |
| Topology model | `framework/cluster/topology/document.go`, `validate.go`, `README.md` | Retain stable replica identities, node administration states, declared placement, audit metadata, and forward-only versions. Generalize application shard terminology to load groups. |
| Documents | `framework/cluster/topology/store.go`, `metaraft/store.go` | Retain Latest/Target/Active, retained history, proposal CAS, and view-specific watches. |
| Topology commands | `framework/cluster/topology/metaraft/codec.go`, `sm.go`, `execution_state.go`, `admission.go`, `sessions.go` | Preserve execution, admission, session and lock semantics, rather than extracting only document replacement. |
| Approved effects | `framework/cluster/topology/plan.go`, `executor.go`, `convergence.go`, `cluster.go` | Exact approved steps and their durable progress form the caller-integration boundary. |
| Membership safety | `framework/cluster/membership/README.md`, policy/coordinator/safety implementation | Keep one change in flight per group, learner-first growth, catch-up checks, leader transfer before removal and post-change quorum checks. |
| Meta runtime | `framework/cluster/topology/metaraft/group.go`, `group_controller.go`, `bootstrap.go`, `activation.go`, `serve_override.go` | Extract the bbolt-backed RawNode runtime, bootstrap/join, sessions, identity fencing, status and networking handlers. |
| Local identity | `framework/cluster/topology/metaraft/bolt.go`, `replica_ledger.go` | Retain identity pinning and local load-replica records. Generalize their names and explicit path inputs. Local records do not enter snapshots. |
| Load runtime | `framework/cluster/raftbudget/raft/shard.go`, `types.go`, `raftgroup_methods.go`, `raftgroup_wal.go`, `snapshot.go`, `README.md` | Extract loop mechanics; replace concrete budget state machine/proposal/result/snapshot coupling with interfaces. |
| WAL storage | `framework/cluster/raftbudget/storage/storage.go`, `walstore/{walstore,snapshot,state,identity,refill}.go`, package READMEs | Preserve fsync, tail rewriting, snapshot markers, replay, quarantine and pinning. Replace split application-specific snapshot payloads with opaque complete snapshots. |
| Networking | `framework/cluster/raftbudget/transport/proto/transport.proto`, transport README | Split a dedicated meta-only protocol from the shared source protocol. Load message/snapshot/control RPCs must not enter the library. |
| Application adapters | `plugins/governance/group_controller.go`, `cluster_config.go` | Separate global status/effect control from budget stores, licensing, SQL projections, entity counters and service bootstrap. |
| Local event journal | `framework/cluster/events/journal.go`, `recorder.go`, `README.md` | Extract subscriptions plus a separate bbolt journal, using an explicit caller path. |
| Validation examples | topology/meta/store unit tests and `tests/cluster/{topology,identity,wal_store,admission}_test.go` | Port behavioral assertions rather than the whole application test harness. |

Dependency baseline from the source module: Go `1.26.5`, `go.etcd.io/raft/v3 v3.6.0`, `go.etcd.io/etcd/server/v3 v3.6.12`, `go.etcd.io/bbolt v1.4.3`. These are observed source pins, not a claim about latest released versions or installed tools. Extraction should start with matched pins; dependency upgrades are a separate decision.

Authoritative checks:

- [etcd Raft v3.6.0 usage contract](https://github.com/etcd-io/raft/blob/v3.6.0/README.md): persistence, message sending, committed application and configuration changes impose ordering responsibilities on a runtime owner.
- [etcd WAL API](https://pkg.go.dev/go.etcd.io/etcd/server/v3/storage/wal): reuse the existing WAL implementation rather than designing a new log format.
- [bbolt API](https://pkg.go.dev/go.etcd.io/bbolt): preserve the transactional durable-state implementation.

## Confirmed choices

| Question | User answer | Resolution |
| --- | --- | --- |
| Can some admitted node types stay outside `_meta`? | “Every admitted node joins `_meta` (Recommended)” | Every non-excluded admitted node has a `_meta` replica, independent of its type. Types constrain load-group eligibility. Draining a node preserves its meta participation. |
| Should events include a local journal? | “Event subscription plus local bbolt journal” | Provide both subscriptions and a node-local bbolt journal. Events never become meta proposals, topology fields or replicated snapshots. All journal paths are supplied by the caller. |
| Preserve fixed five-node/five-replica limits? | Initially: “Caller-supplied limits, defaults of five (Recommended)”; subsequently changed the default cluster size to three. | Accept the Limits struct through constructor parameters, with MaxNodes defaulting to three and MaxReplicas retaining its default of five. Limits.MaxNodes is the sole node-capacity setting. Persist the resolved Limits at founding and verify them on join/restart so deterministic validation cannot differ across replicas. Membership safety is fixed. |
| Who owns the load-group Raft loop? | “Library owns Raft loop and WAL recovery (Recommended)” | Provide a running load engine with pluggable state machine, transport and storage. The library owns Raft ordering, WAL recovery, snapshots and membership processing. |
| Reuse a deleted group ID? | “Permanently retire deleted group IDs (Recommended)” | Persist retired IDs in the meta FSM and its snapshots. Stage-only removal does not retire an ID; approval of a deletion does. Recreate the workload under a new ID. |
| Include manual force-new-cluster recovery initially? | “Defer force-new; include normal recovery paths (Recommended)” | Include restart, join, fresh-ID readmission and reseating. Defer force-new and its offline application-storage/fencing integration. |
| Approved-effect callback contract | “Require idempotent callbacks and verification of their exact authorized identities.” | This is a mandatory main-scope requirement for every approved effect, including retries after a successful effect whose completion record was lost. |

These choices do not yet constitute approval of every API signature below.

## Findings that determine the design

### The meta FSM has more responsibilities than document storage

Its current commands cover document CAS, execution approval, atomic repair-document plus execution commits, execution transitions, target activation, join admission, fresh-ID readmission, joiner readiness, session grants/revocations and session-owned locks. Keepalive refreshes are leader-local, not log entries. Session revocation releases locks. Replicated apply must not consult the local clock; audit timestamps and other nondeterministic inputs are carried in proposals.

State snapshots include Latest, Target, Active, retained history, executions, admissions, sessions, locks, activated-node history and applied index/term. Map-backed collections are serialized in deterministic order. A failed durable materialized-state write after an in-memory change is a fail-stop condition. Already-applied entries must not allocate new identities during replay.

The retained document history currently keeps 128 versions. Nonterminal executions and executions referenced by Target/Active remain pinned even when old history rotates. History is therefore a retained window, not an unlimited audit archive.

### Declaration, approval and convergence must remain distinct

Latest is the newest declared document. Target is the approved document under execution. Active is the last converged document. A staged change affects Latest only. Approval durably records the exact plan and Target before any runtime effect. Convergence advances through that plan. Active changes after successful final validation.

Keep explicit Stage/Plan/Apply operations in the public API. Convenience mutations may opt into apply, but their return value must distinguish document commitment from asynchronous execution approval and eventual convergence. A successful HTTP response must never imply convergence unless the endpoint explicitly waits for it.

### Drain is a load-plane operation

Source node states are `active`, `draining`, `drained`, `excluded`. Effective `drained` is derived once the Active document has no load replicas for the node; this completion does not require an additional consensus document version. Draining and drained nodes retain their `_meta` replicas. Exclusion removes cluster participation and must not reuse retired identities.

Observed health and leadership remain outside the document. Administrative states must not be automatically changed merely because a node becomes temporarily unreachable.

### Effects are retryable, exactly authorized operations

Require idempotent callbacks and verification of their exact authorized identities. This is part of the main implementation scope. Repeated execution of the same authorized operation must converge to the same intended result without duplicating the effect. An existing result counts as success only if its group, node and replica identities match the authorized operation; conflicting or superseded identities must be refused.

The source already carries execution ID, document version, attempt, step sequence, step kind, group ID, node name and replica ID in every authorized operation. Preserve all fields across caller transports. Receiving code validates the exact durable scope, not merely a trusted connection.

A leader can lose leadership after performing an effect but before recording completion. The next leader repeats the step against observed live state. Therefore the integration contract is idempotent, retryable effect execution; it cannot promise exactly-once external execution. Callback work must not run inside deterministic FSM Apply or on the Raft heartbeat loop.

If a caller needs stronger guarantees for application effects, it can atomically commit its change and deduplication record in one application transaction or use a recipient-supported idempotency key. An outbox durably records delivery intent but still requires recipient deduplication or transactional coordination. Those application-specific mechanisms remain caller-owned.

### The source load engine is application-specific

The current loop directly references budget proposals/results, reservation tracking, admission exemptions, compensation, redirect tombstones and a concrete state machine. Its durable snapshots split reservation data from state extensions and recover some domain state from SQL projections. Copying that engine with only import/name changes would retain these application dependencies.

The WAL implementation also initializes a placeholder `ConfState{Voters: []uint64{1}}` and writes that state into stored snapshot metadata/markers because its present storage contract does not carry membership. That is not a usable generic snapshot contract. The extracted store must persist each group's actual ConfState from the runtime; it must not retain the placeholder or recover membership from unspecified application tables.

Keep the loop, writer and concurrency invariants; replace domain seams. A generic load snapshot must contain the complete application snapshot and the live Raft ConfState. The library cannot reconstruct unspecified application state from a database projection.

The durable store's applied index alone is not enough to prove that a caller's application state was restored. Recovery must restore a full application snapshot and replay committed entries above it. Persistent caller state machines must support that restore/replay contract and avoid external non-idempotent effects in Apply.

### Meta networking needs a real protocol split

The source transport multiplexes meta and data traffic and also contains application request forwarding. Reusing its entire service would violate the requested boundary.

The extracted meta service may carry only meta Raft messages/snapshots, join/join-ready, session keepalive, identity probes, meta status/control, execution approval forwarding and parked-join observations. Meta message endpoints reject non-meta group IDs. Load messages, snapshots, proposals, membership commands and remote runtime management use caller-defined protocols.

The caller's load GroupController hides local versus remote status/effects. Its method arguments are library-owned types; its routing and wire schema are application-owned. Even when the meta leader hosts no load replica, it must be able to reach the correct load hosts through this interface.

Addresses are resolved at request time through an injected resolver and supplied bootstrap seeds. They do not become consensus-managed node identity. Do not extract the entire application gossip/KV subsystem merely to resolve addresses.

### Settings are arguments, not an application configuration system

Constructor option structs are required and allowed. Remove file loaders, environment readers, default directory resolution, application logging types, licensing and service startup. Meta receives an exact DB path; WAL receives an exact root path; events receive an exact DB path. Relative paths, if supported, refer only to the caller's working directory. Empty required paths fail validation.

Because independent stores may live in different caller-selected locations, readmission cannot rename an assumed common `raft/` parent. The caller must coordinate offline quarantine of its explicitly named stores before a superseded identity starts, including any application-owned state.

### Source optimizations need explicit library contracts

The load engine has an off-loop writer, one outstanding Ready, applied-index write coalescing, bounded admission queues and cancellable retries. Preserve the ordering mechanisms, but remove budget-specific exemption/compensation behavior. Persistence delays may shed new proposals without discarding a Ready or allowing state mutation past a pending durable write.

etcd WAL segment size is a process-wide variable. Multiple library instances cannot safely assign conflicting sizes. Resolve/validate one process-wide value, rejecting conflicts instead of changing a live global silently. Per-group sizes require a different upstream mechanism and are out of initial scope.

The event journal currently uses NoSync and best-effort observations. Moving its I/O behind a bounded event dispatcher is a reasonable extraction improvement: the heartbeat loop publishes without waiting for a journal transaction or subscriber. Dropped events must be visible through a counter/gap indication; durable execution progress is independent of these events.

## Proposed package boundaries

All names below are working proposals, not existing APIs.

```text
gquorum/                  facade and explicit lifecycle orchestration
topology/                 documents, validation, plans, execution contracts
meta/                     public meta handle, bootstrap/join/session APIs
load/                     public generic load runtime and machine interfaces
membership/               fixed shared membership safety and coordination
storage/                  durable Raft state contracts
storage/walstore/         etcd WAL, state.bolt and snapshot implementation
events/                   subscriptions and independent bbolt journal
internal/metaengine/      bbolt runtime and deterministic topology FSM
internal/metatransport/   meta-only gRPC service/client and bounded peer queues
internal/metapb/          generated meta-only protobuf code
examples/cluster/         caller application, custom load transport, Compose
tests/cluster/            focused example-cluster tests
```

Use ordinary library logging (`log/slog` or a narrow compatible interface) and explicit observers instead of importing application schema types. No automatic process-global metric registration by the facade. SQL stores/projections, budget domain logic, service licensing, gateway routing, application entities and configuration decoding are excluded.

## Working data model

```go
type NodeType string

type Limits struct {
    MaxNodes    int `json:"max_nodes"`
    MaxReplicas int `json:"max_replicas"`
}

type Node struct {
    Name      string            `json:"name"`
    Type      NodeType          `json:"type"`
    State     NodeState         `json:"state"`
    Region    string            `json:"region,omitempty"`
    Labels    map[string]string `json:"labels,omitempty"`
    CreatedAt time.Time         `json:"created_at"`
    UpdatedAt time.Time         `json:"updated_at"`
}

type PlacementRule struct {
    MinReplicas      int        `json:"min_replicas"`
    MaxReplicas      int        `json:"max_replicas"`
    AllowedNodeTypes []NodeType `json:"allowed_node_types,omitempty"`
}

type Group struct {
    ID            string         `json:"id"`
    Replicas      []Replica      `json:"replicas"`
    NextReplicaID uint64         `json:"next_replica_id"`
    Placement     *PlacementRule `json:"placement,omitempty"`
    CreatedAt     time.Time      `json:"created_at"`
    UpdatedAt     time.Time      `json:"updated_at"`
}
```

Node type values are caller-defined strings, separate from administrative state and replica voter/learner role. Empty AllowedNodeTypes means all node types. An explicit list restricts load placement; it never excludes an admitted node from `_meta`. Validate manual placement and repair placement identically. Do not call arbitrary caller policy code during deterministic replicated validation.

Keep the source Document audit/version fields and Latest/Target/Active semantics. Cluster-wide Limits are immutable consensus-pinned founding metadata, not fields a normal document proposal can change. Limits.MaxNodes is the only node-capacity setting, defaulting to three; introduce no separate node-limit constants, constructor/configuration fields or meta placement cap. `_meta` follows admitted membership and has no independently configurable placement rule. Per-load-group placement constraints should actually be enforced; the source per-group Placement field is currently mostly reserved/inert. Empty/unsupported reserved group states and witness modes must not be advertised as working features.

## Working public API model

```go
// Meta constructor parameters include explicit identity, DB path, network
// listener/resolver/security options, founding/join inputs and load controller.
func NewMeta(opts MetaOptions) (*Meta, error)
func (m *Meta) Start(ctx context.Context) error
func (m *Meta) WaitReady(ctx context.Context) error
func (m *Meta) Stop(ctx context.Context) error
func (m *Meta) Status(ctx context.Context) (ClusterStatus, error)

func (m *Meta) Latest(ctx context.Context) (Document, error)
func (m *Meta) Target(ctx context.Context) (Document, error)
func (m *Meta) Active(ctx context.Context) (Document, error)
func (m *Meta) Get(ctx context.Context, version uint64) (Document, error)
func (m *Meta) History(ctx context.Context, page HistoryPage) (HistoryResult, error)
func (m *Meta) Watch(ctx context.Context, view DocumentView) (<-chan DocumentUpdate, error)

func (m *Meta) Stage(ctx context.Context, doc Document, expectedVersion uint64) (Document, error)
func (m *Meta) Plan(ctx context.Context, version uint64) (Plan, error)
func (m *Meta) Apply(ctx context.Context, version uint64) (ExecutionID, error)
func (m *Meta) Execution(ctx context.Context, id ExecutionID) (ExecutionStatus, error)
func (m *Meta) WaitExecution(ctx context.Context, id ExecutionID) (ExecutionStatus, error)
func (m *Meta) Authorize(ctx context.Context, step AuthorizedStep) error
```

Typed convenience mutations should cover AddNode, DrainNode, ActivateNode, ExcludeNode, forced retirement with an audit reason, SetPlacement, CreateLoadGroup, DeleteLoadGroup, ChangeReplica, ReseatReplica and RollbackTo. Pass expected version, actor and apply intent explicitly in mutation arguments, rather than hiding those inputs in context values. Preserve session activation/deactivation, keepalive, session locks and own-session liveness APIs. Session-backed readiness is a consensus-connectivity signal, not a universal application serving policy.

```go
type GroupController interface {
    Status(context.Context, string) (GroupStatus, error)
    CreateGroup(context.Context, AuthorizedStep) error
    PrepareReplica(context.Context, AuthorizedStep) error
    ApplyMembership(context.Context, AuthorizedStep) error
    TransferLeader(context.Context, AuthorizedStep) error
    StopReplica(context.Context, AuthorizedStep) error
    CleanupReplicaState(context.Context, AuthorizedStep) error
    DeleteGroupState(context.Context, AuthorizedStep) error
}
```

All operations validate exact authorized identities. Create/delete are whole-group operations; prepare/stop/cleanup carry replica identity. WaitCaughtUp is evaluated by the library from current leader status. A caller controller must classify retryable unavailability separately from terminal invalid operations and must not claim global absence after only a local registry miss.

```go
type Command struct {
    Index uint64
    Term  uint64
    Data  []byte
}

type Result struct {
    Data []byte
    Code string // deterministic application outcome; rejection is not I/O failure
}

type Machine interface {
    Apply(context.Context, Command) (Result, error)
    Snapshot(context.Context) ([]byte, error)
    Restore(context.Context, []byte) error
}

type Transport interface {
    Send(context.Context, []raftpb.Message) error
}

func NewLoadGroup(opts LoadOptions) (*LoadGroup, error)
func (g *LoadGroup) Start(ctx context.Context) error
func (g *LoadGroup) Propose(ctx context.Context, data []byte) (Result, error)
func (g *LoadGroup) Step(ctx context.Context, msg raftpb.Message) error
func (g *LoadGroup) Status() GroupStatus
func (g *LoadGroup) ProposeConfChange(ctx context.Context, cc raftpb.ConfChangeV2) error
func (g *LoadGroup) TransferLeader(ctx context.Context, target uint64) error
func (g *LoadGroup) ReportUnreachable(replicaID uint64)
func (g *LoadGroup) ReportSnapshot(replicaID uint64, status raft.SnapshotStatus)
func (g *LoadGroup) Stop(ctx context.Context) error
```

Machine calls are serialized by the runtime. Applications synchronize reads and must include all replicated state in Snapshot. Restore plus committed replay reconstructs the machine without relying on the WAL applied hint. Fatal Apply/Restore/persistence errors halt the affected group; a business rejection is encoded in Result and still consumes the log entry. No arbitrary out-of-band effects occur in Apply.

The generic runtime uses opaque proposal framing/correlation and may expose a caller request ID for application deduplication. A canceled proposal may still commit; caller retry semantics must be documented. Transport Send only hands messages to the caller transport; snapshot success/failure is reported separately. Inbound routing is always scoped to the correct cluster/group/replica incarnation by caller code.

## Persistence model to specify in the target

No SQL schema or ORM is introduced. The target must enumerate bbolt buckets, exact key/value encodings, WAL metadata, snapshot payloads, format versions, constraints and transaction boundaries instead of fabricating SQL DDL.

| Store | Layout/content | Durability and ownership |
| --- | --- | --- |
| Meta bbolt | `hardstate/current`, BE-indexed `entries`, `snapshot/current`, `applied/{index,confstate}`, `state/current`, `identity`, local serve override and a generalized load replica ledger | Fsync transactions; applied membership/index are atomic. Replicated FSM snapshot excludes local identity, overrides, ledgers and events. |
| Load WAL | Caller root + validated group ID; `identity.json`, `wal/`, `snap/`, `state.bolt` | etcd WAL Save is durable before apply; snapshot file precedes its WAL marker; state metadata persists applied/floor/base/snapmark. Add complete ConfState recovery. |
| Local event bbolt | Caller file path; `events` with BE sequence keys and JSON values; `meta/schema_version` | Node-local, best-effort journal; bounded retention by age and live bucket bytes. Never part of Raft state. |
| Caller application store | Application-defined | Not opened, migrated or schema-managed by this library. Snapshot/restore and replay are caller interface obligations. |

Retain corruption versus lock-contention distinctions. A corrupt replica is parked/blocked for explicit repair; a missing voted store is not silently recreated under its old ID. Bind the identity before first durable writes. Ensure fresh storage and directory entries are durable before writing the corresponding local ledger record. Never reuse a retired replica ID, including during rollback; carry allocator high-water marks forward rather than restoring an older counter.

WAL Drop quarantines only the explicitly named group directory; it does not delete unrelated caller paths. Do not preserve source absolute defaults or old application directory names. Decide fresh format identifiers independently from legacy source store migration; direct in-place migration is not yet required by the request.

## Proposed algorithms and flows

### Mutation and execution

```go
func StageAndMaybeApply(ctx context.Context, edit Edit, opts MutationOptions) (MutationResult, error) {
    current := latestLocallyAppliedDocument()
    requireExpectedVersion(current.Version, opts.ExpectedVersion)
    desired := current.Clone()
    edit(&desired) // deterministic structure, proposer supplies timestamps
    validateDocumentAndPinnedLimits(desired)
    committed := proposeDocumentCAS(ctx, desired, opts.ExpectedVersion)
    if !opts.Apply { return MutationResult{Document: committed}, nil }
    executionID := applyOnMetaLeaderWithOneHopForwarding(ctx, committed.Version)
    return MutationResult{Document: committed, ExecutionID: executionID}, nil
}

func Converge(ctx context.Context) error {
    requireCurrentMetaLeadership()
    ex := readDurableNonterminalExecution()
    step := ex.CurrentExactStep()
    validateExactScope(ex, step)
    status := controllerFor(step.GroupID).Status(ctx, step.GroupID)
    if alreadySatisfied(status, step) { return commitCompletedStep(ctx, ex, step) }
    checkFixedMembershipSafety(status, step)
    // Load controller invokes application-owned local/remote protocols.
    if err := executeAuthorizedEffect(ctx, step); err != nil {
        return commitWaitingOrFailedTransition(ctx, ex, step, classify(err))
    }
    reobserveAndVerifyEffect(ctx, step)
    requireCurrentMetaLeadershipAndExecution()
    return commitCompletedStep(ctx, ex, step)
}
```

If leadership changes between the effect and its completion record, leave the step resumable. Final activation requires live membership and exact current execution validation. Never reverse completed external effects to emulate document rollback; rollback stages a new forward execution.

### Load Ready and recovery

```go
func RecoverLoad(ctx context.Context, store Store, machine Machine) error {
    recovered := store.Load(ctx, groupID)
    verifyIdentityAndReadableState(recovered)
    restoreCompleteSnapshot(machine, recovered.Snapshot)
    rebuildMemoryStorage(recovered.Snapshot, recovered.Entries, recovered.HardState)
    // Starting Applied at the snapshot boundary permits required committed replay.
    startRawNodeFromSnapshotBoundary()
    replayCommittedNormalEntriesAndMembershipBeforeReadyForServing()
    return nil
}

func ProcessReady(ctx context.Context, rd raft.Ready) error {
    holdSingleOutstandingReady()
    persistReceivedSnapshotAndRequiredReadyState(ctx, rd)
    waitForDurableWriterSuccessOrCancellation()
    updateMemoryStorage()
    restoreReceivedApplicationSnapshotIfAny()
    sendOwnedMessageCopiesViaCallerTransport(rd.Messages)
    for _, entry := range rd.CommittedEntries {
        applyNormalOrConfigurationEntry(entry)
    }
    publishAppliedIndexAndConfStateSafely()
    resolveAppliedProposalWaiters()
    rawNode.Advance(rd)
    return nil
}
```

The target must expand this sketch with exact source-preserving writer phases, snapshot/HardState transactions, waiters and shutdown behavior. It must not treat this simplified pseudocode as permission to change persistence ordering.

### Startup and shutdown

Startup validates supplied settings and paths; verifies pins; recovers meta; starts the meta service before announcing join-ready; obtains/renews activation sessions; registers load adapters before approving their effects; restores load snapshots and committed tails; resumes approved executions on the meta leader. Status remains explicit while startup is incomplete.

Shutdown stops new admission/proposals, cancels and joins convergence, performs bounded session deactivation where possible, cancels outbound streams before joining their send loops, drains/joins durable writers and runtime goroutines, closes owned Raft storage and then the event dispatcher/journal. Injected shared stores/listeners have explicit documented ownership. Never wait for a peer stream to finish before cancelling its connection.

## Example and verification model

Use three voting nodes, each in `_meta`, with a tiny replicated key/value or counter load machine. Use caller-defined node types in the document and a type-restricted load group. The example application supplies its own load-message/snapshot/control protocol and typed GroupController. It accepts flags/environment settings only in the example, not the library.

`make run-example` builds and starts Compose; `make stop-example` stops it while preserving data; `make clean-example` explicitly removes only example volumes. Paths and ports are chosen in example code/Compose and passed to constructors. Avoid authentication, gateway services, SQL, shared KV infrastructure and licensing requirements from the source application.

Proposed REST demonstrations: GET cluster status/documents/history/executions/events; POST node admission/drain/activation/exclusion; POST document plan/apply; load PUT/GET keys; optional leader transfer. Mutation requests use explicit expected versions and return document version plus execution ID. “reset apis” in the request is treated as REST APIs; no data-wipe route is assumed.

Focused checks to implement:

- Meta CAS under concurrent proposals, snapshot equivalence, deterministic serialization and replay guards.
- Exact execution authorization, supersession, leader failover, retry after side effects and no premature Active transition.
- Repeated approved callbacks after lost completion must avoid duplicate effects; conflicting group/node/replica identities and stale authorization must be refused.
- Stable replica IDs, fresh-ID readmission, typed eligibility and rollback allocator monotonicity.
- bbolt atomic snapshot/index/ConfState behavior and store lock/corruption classification.
- WAL conformance, tail replacement, complete snapshots, restart replay, identity pins and snapshot-marker ordering.
- Event isolation from replicated state, journal retention, subscription cancellation/backpressure and store path correctness.
- Unit runtime ordering with controlled storage failures; cancellation and bounded shutdown without leaked goroutines.
- Compose cluster startup, load replication, drain/activate, leader outage/recovery, persisted restart and resumed execution after leader replacement.

Port every applicable Raft correctness scenario identified in [the source test inventory](20261010-raft-library-test-port.md), replacing budget/gateway/SQL fixtures with the caller-defined test Machine, exact-effect controller and explicit storage paths. Run correctness scenarios at three, five and seven nodes, with single-node unit coverage, and retain the source fifteen-load-group scale case. Larger-cluster admission, planner cost, snapshot size and bounded shutdown are required validation work in this scope. Configure all capacity through Limits; the default node capacity remains three. Application-only assertions and the explicitly deferred force-new recovery are identified separately in the inventory so no applicable Raft test disappears silently.

## NOT CONSIDERED & TODO

- [potential-improvement] [porting:improvement-over-upstream] Exact public signatures, package split and constructor ownership are working models; expand every created file/method and failure flow in the target architecture.
- [potential-improvement] [porting:improvement-over-upstream] Group-ID reuse after deletion is resolved: approval of deletion permanently retires the ID. Add durable tombstones and preserve them through snapshots and rollback.
- [postponed] [porting:deffered] Migration of an existing application deployment's disk format, rolling wire compatibility and SQL-backed stores. Requirement is extraction into a new library/example, not an in-place cutover.
- [postponed] [porting:postponned-to-the-next-task] Offline force-new-cluster recovery is explicitly deferred by the user. It requires explicit storage scope and caller fencing/recovery hooks.
- [potential-improvement] [porting:improvement-over-upstream] Application serving readiness for subset/type-restricted load hosting must be caller-defined; copying the source gate's all-groups/global health rule would be incorrect for generic applications.
- [postponed] [porting:simplefication] Witness replicas, joint multi-change membership and enforced group pause/resume. Source fields are reserved or unsupported; do not silently expose them as implemented.
- [postponed] [porting:simplefication] Region-aware balancing, dynamic node-type changes and sophisticated placement algorithms. Initially use explicit eligibility with deterministic placement and safe failure when constraints cannot be met.
- [postponed] [porting:simplefication] Linearizable document/load reads beyond existing local materialized reads need explicit ReadIndex support. Document the consistency of every read rather than claiming all reads are linearizable.
- [potential-improvement] [porting:improvement-over-upstream] Event loss/gap semantics and journal-before-delivery versus delivery-before-journal ordering need exact target signatures and tests. Events must remain outside the consistency path.

## Session summary so far

Seven architectural choices are confirmed: all admitted nodes participate in `_meta`; events have subscriptions and a local bbolt journal; caller-provided Limits defaults MaxNodes to three and MaxReplicas to five, with the node-capacity setting defined only in Limits; the library owns the load Raft runtime and recovery; approved deleted group IDs are permanently retired; force-new recovery is deferred; idempotent callbacks and verification of their exact authorized identities are mandatory main-scope requirements. The extraction is feasible but requires a genuine generic runtime and meta protocol split. The source topology execution and membership machinery provides the strongest reusable foundation. Next deliverable is the full implementation specification and its model review.

The detailed target specification has now been written. Its model review was requested with the question: “Does the target specification match your intended library, including idempotent approved-effect callbacks, caller-owned load protocols/application storage, and complete snapshot-plus-WAL recovery?” Review response is pending. Implementation remains outside this explicitly invoked read-only brainstorm stage. Document checks cover balanced code fences, whitespace and absence of unwanted source-product references; no Go or Docker test results are claimed.

### Follow-up correction and explanation

User correction: “Change the default clsuter size from 5 to 3. We don't need to specify somwere else max node size except Limits struct.” Updated the target and current resolutions accordingly: MaxNodes defaults to three, is specified only in Limits, and has no separate meta placement cap. MaxReplicas remains five because the correction concerns cluster node count. The example uses the default three-node cluster and excludes an old node before admitting a replacement.

User requested an explanation of the exactly-once external effects TODO. Clarified the crash window between successful callback execution and committed completion, the resulting retry contract, safe idempotent create/stop/cleanup operations, and the distinction between an atomic application transaction, recipient idempotency and an outbox that still needs delivery deduplication.

User then required: “Need to add it to the main scope and declare \" Require idempotent callbacks and verification of their exact authorized identities.\"” Promoted that exact requirement to the main scope, confirmed decisions and effect contract in both documents. Verification now explicitly covers repeated callbacks after lost completion and refusal of conflicting identities/stale authorization.

User questioned why a universal cross-system transaction was listed as outside scope. That requirement had been introduced during the discussion rather than requested by the user; listing it as deferred work was misleading. Removed the item from both deferred/TODO lists. The main contract remains complete: durable approval, exact receiver authorization, idempotent callbacks, observed-result verification and resumable completion. Application-specific transaction/outbox details remain documented as caller integration mechanisms, without creating a deferred library feature.

User required larger-cluster testing in main scope and asked to find and adapt all source cluster tests that verify how Raft works. Removed the larger-cluster deferral from both documents. Enumerated all 42 Go test files under tests/cluster, containing 150 test functions excluding TestMain, plus supporting topology/meta, membership, load runtime, WAL, meta transport, local event and application-integration tests. The [test-port inventory](20261010-raft-library-test-port.md) records every cluster test's disposition and adaptation, supporting source symbols, target harness and validation obligations. The source already includes a five-node multi-group controller-shutdown test and a fifteen-group lifecycle test. Required target coverage adds seven-node variants, quorum-loss/recovery, capacity boundaries and measured planner/snapshot behavior; Limits.MaxNodes remains the sole capacity setting. No Go tests were created or executed during this design update.
