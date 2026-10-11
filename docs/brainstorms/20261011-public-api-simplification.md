# Brainstorm: simpler node, cluster and load-group APIs

Status: brainstorm complete. The [target architecture](20261011-public-api-simplification-target.md)
consolidates the accepted design and supersedes intermediate sketches below.
Implementation has not started as part of this brainstorm.

The clean API refactor, separate group/replica operations and a deliberate
public group wrapper are agreed. Managed local replica lifecycle through a caller
factory and transport, and stop holds lasting only for the current node lifetime,
are agreed. Startup-selectable defaults and caller-replaceable policies are user
requirements. The Scheduler/LifecyclePolicy split, policy.RestoreDegraded,
library-owned operator.Controller and Operator vocabulary are agreed. Scheduler
selection/configuration remains caller-supplied; cluster-wide agreement is
explicitly postponed. The refactor also includes HTTP naming and method review
in examples/cluster, its README and corresponding tests. Detailed contracts and
inventories are recorded in the target specification.
This is a discussion record, not an implementation specification.

## Initial request

The user finds the current structure difficult to understand and requests a
refactoring that hides complexity behind understandable names and a small set of
interfaces. Their proposals are:

1. Replace the root `Runtime` concept with something like a `RaftNode` interface,
   and expose cluster-specific operations through something like `RaftCluster`.
2. Hide `OpenMeta` in node initialization/recovery. Use `Start` for the node and
   consider `StartLoadGroup` for application groups instead of `OpenLoad`.
3. Propose additional names and semantic improvements, keeping the number of
   interfaces and relationships small.
4. Study NATS's Raft interface and the local TiKV source as references, accounting
   for this library's multiple Raft groups and replicated topology.

The explicitly invoked go-brainstorm workflow requests investigation and
discussion without implementation edits. Only this brainstorm record is being
created at this stage.

## Current implementation and sources examined

The starting implementation is commit `eb93a4d`. The root workspace has no
`project.txt` or `AGENTS.md`; the current checkout is the library source.

Local library sources:

- [Runtime](../../runtime.go), [load boot decisions](../../boot.go).
- [Meta options](../../meta/options.go), [meta construction](../../meta/group.go),
  [cluster mutations](../../meta/mutations.go), [document APIs](../../meta/documents.go).
- [Load API](../../load/load.go), [engine options and contracts](../../internal/raftengine/options.go).
- [Application controller contracts](../../topology/controller.go).
- [Library guide](../architecture/library.md), [ownership contracts](../architecture/runtime.md).
- [Test application lifecycle](../../tests/cluster/testapp/app.go) and
  [effect receiver](../../tests/cluster/testapp/controller.go).

External references:

- [NATS Raft source](https://raw.githubusercontent.com/nats-io/nats-server/refs/heads/main/server/raft.go),
  main branch as inspected on 2026-10-11. Its `RaftNode` is a member of one
  consensus group: it exposes group identity, proposals, membership, progress,
  leadership, snapshots and lifecycle. The interface also includes numerous
  server-specific controls. The useful pattern is an implementation hidden
  behind an operational interface; its full method list would not simplify our API.
- TiKV local checkout `/Users/speckzzz/reps/neria/tikv`, commit
  `137cbb2593decafebf99905b4d570869230565c2`.
  `src/server/raft_server.rs` defines `MultiRaftServer` as the owner of the
  multi-group system and store lifecycle. `components/raftstore/src/store/peer.rs`
  defines a local peer, while `components/raftstore/src/router.rs` routes by
  region/group and peer identity. This supports separating the hosting node,
  consensus group and local replica. TiKV's large trait hierarchy and batch
  scheduling are not needed to adopt that distinction.

These are architectural references, not proposals to copy either implementation.

## Where the current complexity comes from

### Construction exposes internal ordering

A caller creates `Runtime`, opens meta, starts meta, waits for meta readiness,
opens a load group, starts it, and waits for that group's readiness. Correct
ordering is a library responsibility, but the public API currently requires the
application to assemble it. The root options hold only logging/events, while
the actual node identity and cluster settings live in `meta.Options`.

### The meta handle mixes different scopes

`meta.Group` exposes local lifecycle, cluster topology changes, sessions,
observations, approved-effect authorization, local replica ledgers and protocol
entry points. This makes a user learn implementation boundaries to locate a
cluster operation. `OpenMeta` sounds like an optional resource operation even
though every admitted node participates in the topology quorum.

### Group means both a distributed group and a local running replica

`CreateLoadGroup` changes distributed desired topology; `OpenLoad` constructs
one local replica. Renaming both operations to Start/Create without identifying
their scope would preserve this ambiguity. The replica ID remains essential
when stopping, recovering or deleting local state.

### Application adapters repeat library knowledge

The test application's receiver translates approved membership steps into Raft
changes, coordinates the library membership helper, serializes local opens and
cleanup, and handles load boot ledgers. The transport and application state
machine are legitimate caller extension points. Several local orchestration
steps could become library operations without choosing a load wire protocol.

### Public aliases expose internal implementation shapes

`load.Group` and its options/contracts alias internal engine types. A public
wrapper can expose a deliberate API and prevent future engine helpers from
automatically becoming public methods. This need not introduce another interface.

## First recommendation: two main interfaces and a group handle

Use `gquorum.Node` and `gquorum.Cluster` as the two application-facing interfaces.
Use an ordinary concrete load-group handle for application consensus. Avoid
separate public managers for topology, documents, executions, membership,
recovery and lifecycle.

`Node` means this application's local participant: its identity, owned workers,
meta service and local load replicas. `Cluster` means the cluster administration
API accessed through that node. It is a stable view backed by the node, not a
second independent owner or another network client. `load.Group` represents the
local handle to one application consensus group.

`RaftNode` and `RaftCluster` are possible spellings. `Node` and `Cluster` are
recommended because the `gquorum` package supplies context, and Raft libraries
often use RaftNode for a single group's replica rather than a host of many groups.

### Candidate naming map

| Current name | Proposed name or placement | Reason |
| --- | --- | --- |
| `Runtime` | `Node` | Identifies the local participant and resource owner. |
| `New(Options)` plus `OpenMeta(meta.Options)` | `NewNode(NodeOptions)` followed by `node.Start(ctx)` | Node options carry the node's identity and topology-quorum dependencies. |
| `Runtime.Meta()` | `node.Cluster()` | Exposes cluster operations without requiring a meta handle. |
| `OpenLoad` then `Group.Start` | `node.StartLoadGroup(ctx, options)` | Performs construction/recovery and starts the local replica. |
| `Runtime.Load` / `LoadIDs` | `node.LoadGroup` / `LoadGroupIDs` | Makes the object being looked up explicit. |
| `Runtime.StopLoad` | `node.StopLoadGroup(ctx, replicaRef)` | Stops an exact local replica and preserves durable state. |
| `Runtime.Close` / `RequestClose` | `node.Stop` / `RequestStop` | Consistent lifecycle vocabulary, including safe callback-initiated stop. |
| `meta.CreateLoadGroup` | `cluster.CreateGroup` | Declares a distributed application group through topology approval. |
| `meta.DeleteLoadGroup` | `cluster.DeleteGroup` | Retires the distributed group through the approved execution. |
| `meta.Apply(version)` | `cluster.Approve(version)` | Makes authorization of a staged topology version explicit. |
| `load.Machine` | `load.StateMachine` | Describes the application extension point directly. |

Keep `Propose` for consensus commands, `Apply` for committed state-machine
application, and `Approve` for topology authorization. This removes three
different meanings from similar call sites.

Optional vocabulary refinements for later discussion: `Latest` → `Desired`,
`Target` → `Approved`, retaining `Active`; `Execution` → `Operation` for the
progress of an approved topology change. A view name must describe a durable
document even after it has converged. These names are candidates, not a reason
to change stored or wire field names.

### Illustrative API shape

The following is a scope sketch. Types and signatures require further design;
it is not the final public interface or a complete API inventory.

```go
type Node interface {
    Start(ctx context.Context) error
    WaitReady(ctx context.Context) error
    Stop(ctx context.Context) error
    RequestStop()

    Cluster() Cluster
    StartLoadGroup(ctx context.Context, opts LoadGroupOptions) (*load.Group, error)
    LoadGroup(id string) (*load.Group, bool)
    LoadGroupIDs() []string
    StopLoadGroup(ctx context.Context, replica ReplicaRef) error
}

type ReplicaRef struct {
    GroupID   string
    ReplicaID uint64
}

type Cluster interface {
    Status(ctx context.Context) (topology.ClusterStatus, error)
    // Existing document, history, watch and mutation capabilities remain
    // available through this interface; this sketch shows representative calls.
    DrainNode(ctx context.Context, name string, opts ChangeOptions) (ChangeResult, error)
    ActivateNode(ctx context.Context, name string, opts ChangeOptions) (ChangeResult, error)
    CreateGroup(ctx context.Context, spec GroupSpec, opts ChangeOptions) (ChangeResult, error)
    DeleteGroup(ctx context.Context, id string, opts ChangeOptions) (ChangeResult, error)
    Approve(ctx context.Context, version uint64) (OperationID, error)
    Operation(ctx context.Context, id OperationID) (OperationStatus, error)
    WaitOperation(ctx context.Context, id OperationID) (OperationStatus, error)
}

func NewNode(opts NodeOptions) (Node, error)
```

Node identity and shared settings should be supplied once. `LoadGroupOptions`
should carry the exact local group/replica identity plus application dependencies
(state machine, transport, store and tuning). It should not repeat node, cluster,
process, logger and event-bus fields that the owning node can supply consistently.
Founding peers and startup mode can be derived where approved topology and
durable evidence fully determine them; they must never be guessed from a missing
directory alone.

`NodeOptions` should use a small number of concrete settings structs for identity,
bootstrap/join, networking and storage. Grouping options does not require new
interfaces. Callers still supply paths and network dependencies. Exact layout
and ownership rules will be specified after the main API boundary is agreed.

## Lifecycle semantics to preserve and clarify

`NewNode` validates and assembles dependencies. `Start` owns meta initialization
or recovery, starts its protocol and workers, and begins admission/activation.
Recommendation: Start returns once startup has been launched; WaitReady is the
separate cluster-admission gate. Waiting for a quorum inside every Start would
make sequential multi-node startup awkward and delay availability of diagnostic
APIs. Application serving readiness remains distinct from one-time startup.

`StartLoadGroup` starts the local replica under the node's lifetime. Its call
context controls admission/startup work; canceling an incoming RPC after success
must not stop the replica. Whether the method also waits for replay completion
must be specified explicitly; it cannot promise elected leadership or quorum.

`cluster.CreateGroup` stages/optionally approves distributed intent. The
controller starts approved local replicas through `node.StartLoadGroup` on each
host. `cluster.DeleteGroup` retires topology and schedules cleanup, while
`node.StopLoadGroup` only stops an exact local handle. These distinct operations
must remain evident in names, docs and tests.

Stopping the node joins all owned work. A callback can request stop and return;
waiting inside a callback that shutdown must join remains invalid. Timeout must
retain ownership while teardown continues. Partial startup must remain visible
and cleanly stoppable. Restarting a stopped object versus constructing a new
object requires an explicit contract rather than accidental behavior.

## Additional simplifications worth designing

1. **Built-in local effect execution.** Consider a node operation that validates
   a typed approved action and performs local start/membership/stop/bookkeeping
   steps. Caller networking still delivers actions/status using its own protocol.
   This could remove duplicated coordinator and ledger code from applications.
   A resource factory or explicit resource arguments would be needed for startup;
   callback retry verification and caller-owned external transactions remain
   mandatory. This requires more design than renaming a controller.
2. **Keep customization boundaries explicit.** State machine, load transport and
   custom storage remain independent where callers need them. Do not add an
   interface for every settings struct, callback or helper. A convenience
   function type can cover a simple factory or resolver if needed.
3. **Preserve readable typed methods.** Do not replace all cluster operations
   with a string command or `Execute(any)` merely to reduce method count. That
   moves complexity into request switches and loses compile-time checking.
4. **Separate local status from cluster observations.** `node.Status` should
   describe local lifecycle; `cluster.Status` describes the cluster; group status
   describes one group's consensus. Avoid a field named Lifecycle whose scope
   changes according to which handle returned it.
5. **Keep storage mechanisms behind their existing boundary.** Ordinary callers
   should need node options and supplied stores, not direct boot-ledger access.
   The node can retain exact identity checks and parking internally.
6. **Keep the public surface intentional.** Public facade types belong in
   `gquorum`; topology values in `topology`; the application group API in `load`.
   A future move of meta implementation into `internal/` must account for current
   test/application imports and the chosen compatibility policy. Moving files
   without reducing caller obligations would not solve the problem.

## Alternatives and provisional recommendation

| Approach | Benefit | Cost |
| --- | --- | --- |
| Rename the existing types/methods only | Small migration | Retains split construction, leaked ledgers and broad meta handle. |
| Refactor to Node/Cluster with integrated startup | Clear ownership, fewer required steps, deliberate public contracts | Updates public callers and lifecycle tests. Recommended base. |
| Fully automatic local load hosting through factories | Removes most repeated replica orchestration | Requires resource/failure/cleanup contracts and a careful transport boundary. Additional decision. |

The selected base is a clean public API refactor with a small set of exported
entry points. The user reports no external users; the runnable example and the
repository's test callers will migrate together. Deprecated compatibility
wrappers are unnecessary.

## Decisions and questions

Confirmed by the user's request:

- Reduce visible complexity and use understandable names.
- Treat the local node as the entry point and separate cluster operations.
- Hide explicit meta opening in node lifecycle.
- Keep few interfaces; consult NATS and TiKV.

First question for the user:

> Should this be a clean public API refactor that updates all examples/tests to
> the new names, or must existing Runtime/OpenMeta/OpenLoad callers remain source
> compatible? Recommendation: a clean refactor, with a migration map in docs.

User answer:

> The project doesn't have any users, only example in ./examples/cluster.
> Need to do clean API refactor.

Additional user decisions:

- The distinction between declaring a distributed group and starting a local
  replica is accepted. Node-level replica manipulation is needed.
- A deliberate public group wrapper replacing internal aliases is accepted.
- Caller-written switches and manipulation logic must be minimized. Provide
  predeclared policies with defaults, selected and optionally configured at
  startup.

The specific spelling of candidate names and the full method inventory still
require the final target specification.

## Iteration 2: built-in mechanics and selectable policies

Historical note: this iteration's closed enum-only policy selection was revised
by the user's customization requirement in iteration 4. Its standard behavior
catalog remains useful; the public selection mechanism must support custom Go
implementations as well as those presets.

### What moves into the library

The current test application implements switches over approved step kinds in
`receiveEffect`, manages founding voters, creates membership requests and calls
the membership coordinator. `openLocal`, `stopLocalLocked`, `cleanupLocal` and
`readmission` additionally repeat local identity and storage lifecycle work.
Its self-removal handler verifies durable removal and schedules shutdown.

These are standard operations for the included engine and storage. Recommendation:
make the library execute them. Applications should not reimplement the mechanics
merely to use the same Raft engine with another state machine.

Policies select a behavior among supported alternatives; they do not require
the caller to supply the implementation of that behavior. The default path
should use managed local replicas, with a factory for application components and
a small transport adapter for caller-defined networking.

The distinction is important for the public API: Node/Cluster remain the main
interfaces. Do not introduce a public interface for every policy or a generic
policy registry, string expressions, middleware chain or callback interpreter.
Concrete typed options with predefined modes are sufficient.

### Candidate policy catalog

The defaults below are recommendations, not yet confirmed choices:

| Policy | Recommended default | Other supported choice / configuration | Scope |
| --- | --- | --- | --- |
| Startup readiness | Ready after local admission/activation and required meta recovery | On first bootstrap, additionally wait for every configured founder; optional deadline | Local startup gate, separate from application serving and load leadership |
| Replica repair | Restore required missing load voters | Operator-driven repair only; configure stable-node window, cooldown and observation timing | Cluster topology decision executed by its leader |
| Retired state | Quarantine exact approved retired Raft state, retain for seven days before expiry | Keep quarantined state indefinitely, or choose retention duration | Local Raft storage lifecycle |
| Approved node removal | Fence the old identity and stop the library node | Fence and retain local diagnostics until caller requests stop | Local lifecycle; never calls `os.Exit` |

Startup policy evaluates admission and recovery as well as its selected gate;
the word quorum must not suggest that counting live processes is sufficient.
Waiting for all founders is a first-bootstrap gate, not a requirement imposed on
every restart of an established cluster.

The existing repair algorithm only restores a required Active voter absent from
observed membership, using a fresh approved identity. It does not automatically
turn a dead peer into a different placement, recreate an established replica
whose disk disappeared, scale a healthy group, promote intentional learners,
approve operator drafts, or replace meta members. Preserve that precise meaning
in policy naming and documentation. An `AutomaticRepair` label alone is too vague.

Retirement is authorized by topology before a retention policy applies. Both
retention choices remove old state from the active namespace; retaining forever
must not leave the old replica occupying a replacement's live path. Never sweep
foreign directories, active identities or unregistered application data.
The seven-day value exists today as a fixed WAL quarantine retention; a policy
would make it explicit and configurable. Meta and application data require their
own exact-identity handling rather than blindly reusing a directory sweep.

An approved removal always fences the old identity. The diagnostic variant must
not continue operating its load replicas or retain participation under that
identity. The choice concerns how much local diagnostic state remains available
after fencing; it cannot preserve an active removed participant.

### Concrete configuration shape

Illustrative Go API, with only the repair policy expanded here:

```go
type Policies struct {
    Startup      StartupPolicy
    Repair       RepairPolicy
    RetiredState RetiredStatePolicy
    Removal      RemovalPolicy
}

type RepairMode uint8

const (
    RepairDefault RepairMode = iota
    RestoreMissingReplicas
    OperatorRepairOnly
)

type RepairPolicy struct {
    Mode         RepairMode
    StableFor    time.Duration
    Cooldown     time.Duration
    CheckEvery   time.Duration
    CheckTimeout time.Duration
}
```

`NodeOptions.Policies` is optional: zero values resolve to documented defaults.
For example, a caller can choose `OperatorRepairOnly` without implementing a
policy interface, or choose `RestoreMissingReplicas` with a longer `StableFor`.
An unspecified value must be distinguishable from an explicit disabled mode.

Resolve defaults, reject unknown modes or inconsistent durations, and copy
options at construction. Caller mutation of an options struct after startup
must not change running behavior. Configuration changes to behavior are explicit,
not side effects of sharing a mutable struct.

The small example should not need to configure policies at all. Its basic startup
uses defaults; richer policy choices and failure cases belong in the test fixture.

### Policies do not make safety optional

These remain mandatory library checks in every mode:

- exact cluster, process/member and replica identity validation;
- complete authorization of approved effects and verification on retries;
- learner-first growth, catch-up and majority checks;
- fixed handling of stale actions, successors and permanently retired group IDs;
- parking established replicas with missing, unreadable or foreign state;
- persistence ordering and replay correctness;
- cancellation ownership and joined shutdown.

`membership.SafetyPolicy` currently describes mandatory checks. The refactor
should avoid presenting that as a user-selectable behavior or accepting an
implementation that can bypass it. Policies control when supported work is
attempted or how approved retired resources are retained.

### Managed local replicas: proposed integration flow

The caller supplies its state machine, storage choice and load networking. A
factory function creates the components for an exact replica; it does not
implement topology steps. Multiple application state-machine types can be selected
by application metadata without forcing a switch over Raft lifecycle operations.

```text
cluster.CreateGroup / another topology change
    -> commit desired document and approve its execution
    -> library builds and orders exact authorized actions
    -> caller's transport delivers a typed action to its destination
    -> receiving Node validates authorization and receiver identity
    -> built-in executor prepares resources, starts/changes/stops the replica
    -> library verifies the effect and records progress
    -> completed execution activates the document
```

The caller's incoming handler can authenticate/decode and invoke a library
receiver such as `node.ApplyGroupAction(ctx, action)`. The library owns the switch
over action kinds, revalidation after waits, native membership coordination and
local ledger updates. Outbound action/status calls also use caller networking;
the proposal adds no built-in load-group network protocol.

`StartLoadGroup` remains available for explicit local manipulation and uses the
same executor and per-replica synchronization. Automatic and explicit starts must
not race into two machines, two store bindings or conflicting cleanup. A repeated
start can reuse only the exact already-running identity and compatible resources.
Stopping a replica locally must not accidentally be undone immediately by a
supervisor; explicit suspension versus terminal stop needs a defined contract.

The standard WAL store already implements identity-protected quarantine. The
node can coordinate it with stop and ledger retirement. A custom store still
implements its storage contract. Policies do not know arbitrary application
databases: application-specific resource release or retirement remains a small,
typed hook with idempotence and exact identity verification.

Factory resource ownership must be explicit: distinguish borrowed shared stores
and transports from per-replica resources the node must release, including partial
factory failure and startup cancellation. A cleanup hook for a retired replica
must work after restart even when no live machine exists. These contracts will
be specified before adding signatures to the final target.

### Policy consistency across nodes

Local readiness gates, local retention and removal behavior can differ per node.
A policy that chooses cluster topology changes has a different consequence:
conflicting repair modes on potential leaders would change behavior after an
election. The final design must make this explicit. The recommendation is a
cluster-agreed repair policy identity/settings, with local observation mechanics
separate where appropriate. How this is pinned and checked at startup belongs
in the storage/protocol specification; it is not an incidental Go rename.

The current reconciliation settings are local and are not persisted in cluster
rules. This is a discovered design difference, not a claim that policy agreement
already exists. A read-only effective-policy/status view should explain the
resolved choices and any reason a requested action is waiting or refused.

### Next question

> Should the default Node manage approved local replica creation, recovery,
> membership and retirement through built-in executors, with the caller supplying
> a component factory and its load transport? Recommendation: yes. The alternative
> is to retain explicit lifecycle orchestration in caller code and provide helper
> functions, which removes less integration work.

User answer: "I agree."

Resolution: Node manages approved local replica creation, recovery, membership
and retirement by default. Callers provide their application components and
load transport. Explicit local group operations remain available. The policy
catalog and exact signatures above remain draft recommendations.

## Iteration 3: managed hosting contracts

### One normal integration path

Use the same library executor for automatic topology effects, restart recovery
and explicit local group operations. Avoid independent public managed/manual
hosting implementations that could disagree about identity or storage ownership.

The caller supplies application state machines through one factory function,
sets storage and networking once on NodeOptions, and chooses policy values.
Node derives its replica's permitted startup mode, peer set and identity from
approved topology and retained evidence. The normal API should not ask the caller
to recreate those decisions in every factory invocation.

This refines the first iteration's `StartLoadGroup(ctx, options)` sketch: with
dependencies registered at node construction, the common start call only needs
an exact `ReplicaRef`. The factory receives a richer, immutable specification
including cluster, node, group, replica and approved group description. Start
never accepts arbitrary peers that differ from that declaration.

### Factory and ownership model

Candidate shape, shown independently of the final package placement:

```go
type GroupFactory func(context.Context, ReplicaSpec) (GroupApplication, error)

type GroupApplication struct {
    StateMachine load.StateMachine
    Close        func(context.Context) error // optional per-replica resource release
}
```

The factory creates application resources; it does not inspect action kinds or
implement learner promotion, leadership transfer, stop or Raft cleanup. A caller
with different machine types may dispatch by its own group metadata, which is
application logic rather than a switch over consensus lifecycle operations.

Ownership proposal:

| Resource | Ownership |
| --- | --- |
| Included meta database | Node opens/closes it at its explicit supplied path. |
| Included WAL backend selected through node storage settings | Node opens/closes it at its explicit supplied root. |
| Injected shared custom `storage.Store` | Borrowed; caller closes it after Node has stopped. |
| Injected load transport and caller listeners | Borrowed; caller manages the wire protocol and connection lifecycle. |
| Successful factory result | Node owns that replica's machine lifetime and invokes its optional Close after engine work has joined. |
| Failed factory call | Factory releases resources it acquired before returning its error; ownership was not transferred. |
| Injected event bus/journal | Borrowed; caller closes them after producers have stopped. |

The provided WAL selection and custom-store injection are mutually exclusive;
constructors reject ambiguous settings. A meta-only node supplies neither a load
factory nor load transport. Enabling load hosting requires a complete set of
factory, transport and storage dependencies.

Factory cancellation governs acquisition. A successful replica runs under Node's
lifetime; it cannot inherit a short-lived incoming effect request's context.
After successful transfer of ownership, Close must be available for failed startup
as well as ordinary shutdown. Calls are serialized with replica retirement.
If resource release fails or does not finish, the node retains a failed/stopping
record and must not create a competing successor instance blindly. Exact retry
and terminal-error behavior belongs in the final lifecycle specification.

Persistent application cleanup is separate from closing a live machine. A typed
retirement hook receives an exact, authorized retirement description, and must
work without constructing the retired machine after restart. The library
verifies authorization and Raft-state ownership; the hook verifies and disposes
only the application's resources for that same identity. External transactions
and idempotent retry verification remain application responsibilities.

### One load-network adapter with typed operations

Recommendation: combine load Raft delivery and the small set of remote control
operations into one load transport interface. The existing public GroupController
is then internal orchestration rather than an application implementation burden.
The caller supplies encoding, authentication, addressing and RPC/stream machinery.

Illustrative shape, with public value types to be specified in the target:

```go
type Transport interface {
    SendRaft(ctx context.Context, batch RaftBatch) error
    ReadReplica(ctx context.Context, request ReplicaQuery) (ReplicaStatus, error)
    ApplyAction(ctx context.Context, request ActionRequest) (ActionResult, error)
}
```

`Node` provides matching receiving methods, so an application can connect each
RPC handler to one method after authentication and decoding. Three typed
operations avoid a caller-written action switch and avoid an untyped command bus.
Batch routing keeps the existing cancellation and independent-destination
backpressure guarantees. Snapshot send completion/failure reporting must remain
available to the library even if the caller's transport is asynchronous.

Transport values must include explicit cluster affiliation and sender/receiver
identity, with no wildcard acceptance. The action's target replica is distinct
from the replica receiving a control request: adding a learner is sent to the
current leader, while the authorized action identifies the learner. Both tuples
must be validated. Library receivers revalidate after waits and serialize exact
local changes; transport authentication alone is not authorization.

Whole-group creation and retirement are expanded internally into exact local
work. Delete retries must protect successor state and account for previously
prepared replicas, even where the original topology action is group-wide.
Malformed replies or an absent confirmation cannot count as effect success.

This adapter is a Go contract. It does not install a load gRPC server, prescribe
a protobuf schema or carry load payloads over the built-in meta protocol. The
test application continues to own its load protocol, and the small example
continues to demonstrate meta only.

### Deliberate group handle and local ownership

A public group handle exposes proposals, identity, leadership/status, readiness
and explicit snapshot requests. Raw engine construction, startup, membership
translation, ledger manipulation and backend worker controls stay behind Node.

Any public stop operation on a group handle must delegate to Node's local stop
path. It must not call the internal engine's Stop in isolation, because doing so
would bypass stop intent and let the supervisor restart it. Exposing stop only
through `node.StopLoadGroup` is the simpler initial surface. The standalone load
use case needs an explicit equivalent owner in the final public API inventory.

Explicit membership changes are cluster operations that produce approved steps.
The normal managed group handle must not provide a second, unauthorised route
around the cluster planner by forwarding raw `ProposeConfChange` methods.

### Explicit stop versus automatic recovery

Observed current behavior: `tests/cluster/testapp/app.go:superviseLoads` scans
Active every 250 ms and opens missing local handles. Moving this loop into Node
without a stop-intent contract would make an explicit StopLoadGroup temporary
and surprising.

Recommendation for the default behavior:

1. `StopLoadGroup(ctx, ref)` records a local hold for that exact group/replica
   identity before canceling or joining its running work.
2. The supervisor and action receiver respect the hold. An approved action that
   needs this replica to start reports a typed waiting condition rather than
   silently reversing the stop.
3. `StartLoadGroup(ctx, ref)` explicitly releases the hold for that same identity
   and starts/resumes it using registered components and approved topology.
4. The hold does not change cluster membership or remove the node's assignment.
   Durable maintenance/placement changes use cluster.DrainNode or topology changes.
5. A hold on an old ID must not affect a different, newly approved replica ID.
   Approved removal/cleanup of the held old identity remains executable.
6. Internal stops for retirement, node shutdown or failed initialization do not
   create operator holds. They use the same synchronization with an explicit
   internal stop reason.

Two possible hold lifetimes:

| Choice | Semantics | Storage impact |
| --- | --- | --- |
| Current Node lifetime (recommended) | The replica stays stopped until explicit StartLoadGroup or a new Node starts and recovers approved assignments. | In-memory identity-keyed hold; no added durable bucket. |
| Across process restarts | The replica remains stopped after restart until an explicit start clears its hold. | Durable identity-keyed local hold, removal/readmission rules and recovery validation required. |

Current-lifetime holds keep restart recovery aligned with approved topology and
avoid a forgotten local flag preventing recovery indefinitely. Persistent holds
are appropriate if local maintenance is explicitly intended to survive process
restarts. This is a user-facing behavior choice, not just an implementation detail.

### Lifecycle pseudocode: shared start path

```text
startLocal(ctx, exactReplica, reason):
    reject if Node is stopping or its identity is fenced
    enter the local replica lifecycle gate
    validate exact approved identity and permitted founding/recovery scope
    if operator hold exists:
        if reason is explicit user start: release this exact hold
        else: return waiting_for_explicit_start
    if same identity is already running: return its public handle
    if another instance still owns resources: return stopping/not-ready
    probe retained storage and boot evidence
    if established state is missing or foreign: park and return typed error
    acquire application resources through factory
    open/recover engine and durably record identity
    register owned handle and start under Node lifetime
    on partial failure: retain ownership until engine/resources are joined
    return public handle; readiness can be awaited independently
```

The final specification must refine context timeouts, exact replay readiness,
resource release failures, idempotent retries and lock ordering. It must also
show that callbacks can access status/request stop without reentering a gate
held by their own invocation.

### Next question

> Should an explicit local StopLoadGroup hold survive a process restart?
> Recommendation: no. It lasts until explicit StartLoadGroup or Node restart;
> restart recovers the approved assignment. The alternative persists the hold
> until an explicit start, even across restarts.

User answer: "I agree with recommended."

Resolution: an explicit local stop hold lasts only for the current Node lifetime.
Explicit StartLoadGroup clears the exact hold. A new Node recovers approved
assignments normally; no persistent stop-hold bucket is introduced.

The user additionally requires default behavior to live in dedicated policy or
managed-behavior code with clear public contracts. Callers must be able to replace
included policy/behavior implementations in their own code without changing the
library. This supersedes a closed enum-only extension model.

## Iteration 4: replaceable policy implementations

### Extension requirement and structural recommendation

Default behavior must be implemented through the same public contract available
to application-defined behavior. Built-in settings are conveniences for creating
those implementations. No library source edit, import of an internal package,
registration through an init hook, reflection, plugin loader or upstream PR is
required to install a caller implementation.

Recommended package separation:

```text
gquorum/
    node.go, cluster.go, options.go       public entry points and composition
    policy/
        behavior.go                     public behavior contract and input/result values
        default.go                      complete default implementation
        startup.go, repair.go           configurable built-in decision logic
        retirement.go, removal.go       retention and removal decisions
        policytest/                     reusable contract checks for custom behavior
    internal/managed/
        host.go, replica.go             owned group lifecycle and supervision
        actions.go                      execution of exact approved actions
        policies.go                     invocation, validation and status reporting
        recovery.go, retirement.go      recovery and retirement coordination
```

Existing consensus, persistence, planner and execution machinery can remain in
their focused internal packages. The managed host composes them rather than
merging all implementations into a large node file. Final file names and public
package dependencies are still to be specified.

### One public behavior interface

Recommendation: expose one policy behavior interface with a small set of typed
methods, rather than an interface per knob or an untyped Evaluate(kind, any).

```go
package policy

type Behavior interface {
    Readiness(context.Context, ReadinessInput) (ReadinessDecision, error)
    Repair(context.Context, RepairInput) (RepairDecision, error)
    Retention(context.Context, RetentionInput) (RetentionDecision, error)
    Removal(context.Context, RemovalInput) (RemovalDecision, error)
}

func NewDefault(options Options) (*Default, error)
```

`Default` implements all four methods. Its Options have documented defaults and
built-in mode selections. A caller can embed that default and override one
method while inheriting the others:

```go
type siteBehavior struct {
    *policy.Default
}

func (p *siteBehavior) Repair(
    ctx context.Context,
    in policy.RepairInput,
) (policy.RepairDecision, error) {
    // Apply this application's candidate selection or maintenance window.
    // Return a typed decision; no membership or storage switch is needed.
    return p.Default.Repair(ctx, in)
}

var _ policy.Behavior = (*siteBehavior)(nil)
```

The delegation above illustrates the substitution mechanism; a real override
changes the decision before returning or chooses a different supported action.
An implementation may instead provide all methods without embedding Default.
Ordinary callers can omit the selection entirely and receive Default.

The concrete policy selection object can carry implementation identity and
configuration metadata without adding another policy interface. Exact field
names in NodeOptions will be set in the target. A custom behavior must not
silently inherit the default's identity for cluster-affecting decisions.

### Typed policy inputs and decisions

| Method | Input data | Output meaning |
| --- | --- | --- |
| Readiness | Owned startup/admission/recovery observations, founder progress, elapsed time and current node identity | Additional startup gate satisfied, wait with a reason, or fail startup. Mandatory admission/fencing checks still apply. |
| Repair | Owned versioned Desired/Approved/Active views, group observations, health/stability, holds and recent action information | No change, wait, or request a supported typed topology action for exact identities. |
| Retention | Verified retired replica identity, quarantine record, retirement time and current time | Keep quarantined state or allow collection after an explicit threshold. |
| Removal | Verified durable self-removal identity and completed fencing state | Stop the owned node or remain fenced with local diagnostics available. |

Input is a snapshot of observations, not a linearizable read. It carries enough
version and identity information to explain what the policy based its decision
on. The runtime clones mutable maps/slices before passing them to caller code.
Policies receive no store handles, raw Raft handles or internal locks through
these values.

Decisions are requests, not pre-authorized effects. An input's facts can become
stale while a policy runs. The managed host must recheck live ownership and
authorization and use version-checked proposals before making a topology change.
For retention it rechecks the exact quarantine identity and active ownership
immediately before collection. No policy output may manufacture durable proof.

This model permits custom candidate ordering, extra readiness conditions,
maintenance windows, retention calculations and removal response without a
caller action switch. Supported repair operations must be documented explicitly.
The present core accepts only a narrowly scoped replacement of a missing Active
voter; exposing a policy must not falsely promise arbitrary placement, scaling
or meta-membership rewrites through that same repair command. Broader decisions
can be supported only through existing validated topology operations or a
deliberately specified extension to those operations.

### Stable execution and mandatory validation

Recommended flow:

```text
owned observations
    -> selected built-in or caller Behavior method
    -> typed decision
    -> validate policy result and refresh authorization/version/identity
    -> normal planner and approved operation, or guarded local lifecycle action
    -> managed executor
    -> verify exact postconditions and publish progress
```

Swapping a policy changes decisions. The managed executor continues to enforce
the rules already agreed for this library: exact identities, approval, permanent
group-ID retirement, stop holds, majority/catch-up checks, durable ordering and
joined resource ownership. Existing application-defined state machines, stores,
transports and retirement hooks remain separate execution extension points.

The runtime must not hard-code a switch on concrete policy implementation types.
Both Default and a caller implementation are invoked through Behavior. Switching
over the bounded typed decision variants belongs inside the library, followed by
the same validation regardless of which implementation produced the decision.

### Callback execution contract

- Policies are installed at startup and remain fixed for a Node lifetime. A
  runtime policy hot-swap API is not implied by startup configuration.
- Callbacks receive a bounded context and must honor cancellation. Node does not
  abandon timed-out goroutines and start replacement callbacks concurrently.
  A Go context cannot forcibly terminate caller code; a callback still running
  after cancellation remains owned and visible to shutdown.
- Policy invocation must occur without holding replica, topology or storage
  locks. Reacquire locks and revalidate observations after return. A callback may
  query public status or request stop without joining its own shutdown.
- Recommend serializing invocations on each installed behavior instance owned
  by one Node, making default embedding and caller state straightforward. Callers
  sharing one instance between several Nodes must provide their own concurrency
  safety, or construct an instance per Node.
- Errors, invalid results and canceled evaluation cannot count as success or
  silently select a permissive fallback. Report a reason through status/events
  and retry according to the owning worker's bounded scheduling policy. Exact
  fatal versus retryable classification will be specified per policy method.
- A policy proposes decisions without performing topology, consensus or storage
  side effects itself. Application external effects belong in their explicit
  idempotent hooks, where postconditions and resource ownership can be verified.
- Custom configuration must be stable during the Node lifetime. The library can
  copy value options and observations; it cannot freeze the internals of a caller
  Go object. This immutability is part of the custom implementation contract.

### Cluster agreement and custom implementations

Historical proposal: iteration 8 explicitly postpones this agreement mechanism.
Do not implement its declaration, fingerprint, mismatch gate or metadata schema
in this refactor. Scheduler selection remains caller-supplied.

Cluster-affecting repair behavior needs explicit declared identity, version and
configuration identity in the startup policy selection. Nodes can validate that
declaration against the cluster's agreed policy metadata; local readiness,
retention and removal choices can remain node-specific.

An identity/configuration fingerprint is an agreement declaration, not proof
that arbitrary caller binaries or external observations are identical. The
library executes custom decision code outside the deterministic meta FSM. The
leader commits the concrete validated result; followers apply that result through
ordinary consensus without rerunning custom callbacks. This preserves replay
determinism and permits observations such as a maintenance window to inform the
leader's decision.

Built-in constructors can provide stable identity metadata automatically. Custom
implementations must declare their repair identity explicitly so an embedded
Default cannot mislabel changed logic. Failover and startup mismatch handling,
including the durable metadata schema, require the final target specification.

### Contract tests and regression scope

Provide reusable checks for inputs remaining owned, cancellation, valid decision
forms and predictable default delegation. Tests cannot prove arbitrary callback
code is deterministic, terminating or free of external side effects; document
those limits and exercise runtime guards separately.

Port the existing default-policy tests so built-in behavior retains its documented
decisions. Add an external-package test importing only public packages and
installing a custom Behavior. Process-cluster tests should verify that the
override affects decisions, that stale/invalid decisions remain refused, and that
leadership changes preserve policy agreement. The small meta example continues
to use defaults and contains no custom policy machinery.

### Remaining extension boundary question

> Should caller replacement cover policy decisions while the managed executor
> stays library-owned, or should callers also be able to replace that executor?
> Recommendation: replace decisions; use the existing state-machine, storage,
> transport and retirement hooks for application-specific execution.

At this iteration no answer was recorded. Iteration 7 closes this question:
the user accepts the library-owned Controller and explicit application hooks.

## Iteration 5: TiKV and PD terminology for replaceable decisions

The user asks whether TiKV has an equivalent of the proposed `Behavior`
interface, with the intention of unifying names, and points again to the local
checkout at `/Users/speckzzz/reps/neria/tikv`.

### Source findings

The local checkout remains at commit
`137cbb2593decafebf99905b4d570869230565c2`. There is no direct counterpart in the
inspected Raft extension points to an interface combining readiness, repair,
retention and removal decisions.

In `components/raftstore/src/coprocessor/mod.rs`:

- `Coprocessor` (line 64) supplies common start/stop hooks for injected behavior.
- `RoleObserver` (line 314) receives local Raft role changes.
- `RegionChangeObserver` (line 342) handles region changes and persistence hooks.
- `DestroyPeerObserver` (line 633) receives peer destruction notifications.
- `TransferLeaderObserver` (line 653) participates in leadership-transfer hooks.

`CoprocessorHost` in `components/raftstore/src/coprocessor/dispatcher.rs`
(line 539) holds the registry and invokes these extensions. Observer does not
necessarily mean passive notification: some hooks can reject, alter or defer
processing. Nevertheless, a destruction hook is not a retired-storage retention
policy, and these hooks do not collectively implement our proposed contract.

The local `components/raftstore/src/store/worker/pd.rs` sends heartbeats to PD
and handles the returned membership/leadership commands. Cluster scheduling
terminology therefore needs the separate PD source as well as this checkout.
Official PD sources inspected on 2026-10-11:

| PD name | Responsibility | Closest role here |
| --- | --- | --- |
| [Scheduler](https://raw.githubusercontent.com/tikv/pd/master/pkg/schedule/schedulers/scheduler.go) | Selects resource scheduling operations. | Cluster-level automatic decision extension. |
| [ReplicaChecker](https://raw.githubusercontent.com/tikv/pd/master/pkg/schedule/checker/replica_checker.go) | Checks replica count, health and placement; can return a corrective operator. It is a concrete type. | Current missing-voter repair decision, with a narrower supported action set here. |
| [Operator](https://raw.githubusercontent.com/tikv/pd/master/pkg/schedule/operator/operator.go) | Tracks ordered execution steps for a region change. | An approved per-group operation, conceptually. |
| [operator.Controller](https://raw.githubusercontent.com/tikv/pd/master/pkg/schedule/operator/operator_controller.go) | Dispatches steps and tracks operation progress. | Library-owned operation execution and verification. |

These are conceptual correspondences. A topology approval here can span several
groups; it is not automatically equivalent to one PD region operator. PD's
Scheduler interface also includes HTTP/configuration machinery that does not
belong in this library.

### Revised naming recommendation, pending discussion

Do not rename the entire four-method `Behavior` to `Scheduler`: three methods
concern local lifecycle rather than cluster scheduling. A clearer candidate is
two public extension interfaces in the dedicated `policy` package:

```go
package policy

type Scheduler interface {
    Schedule(context.Context, ScheduleInput) (ScheduleDecision, error)
}

type LifecyclePolicy interface {
    Readiness(context.Context, ReadinessInput) (ReadinessDecision, error)
    Retention(context.Context, RetentionInput) (RetentionDecision, error)
    Removal(context.Context, RemovalInput) (RemovalDecision, error)
}
```

`Scheduler` is adapted from PD terminology. `LifecyclePolicy` is our descriptive
name, not a TiKV interface name. `ScheduleInput`/`ScheduleDecision` would replace
the corresponding proposed Repair values, with their supported actions still
explicitly bounded. The rename does not add arbitrary balancing or placement.
`ReplicaChecker` is a candidate name for the built-in corrective decision
implementation; adapting it to our Scheduler interface would be our design,
not a claim that PD's ReplicaChecker implements PD's Scheduler interface.

This separates cluster-agreed decisions from node-local choices with two
extension contracts, without copying TiKV's observer hierarchy. Both can have
configurable defaults and direct startup injection of caller implementations.
A caller can override only scheduling or embed the default lifecycle policy to
override one local decision. Ordinary users need implement neither interface.

The typed-input ownership, bounded callback lifetime, idempotent effect hooks,
exact authorized-identity verification and revalidation rules from iteration 4
remain applicable. Cluster scheduling agreement metadata applies to Scheduler;
local lifecycle choices remain node-specific. The previous recommendation to
serialize invocation was scoped to one Behavior instance; if split, the target
must specify serialization per installed implementation and caller responsibility
when sharing state between them.

If keeping one aggregate interface is preferred, `policy.Policy` is an honest
name for it, but has no exact TiKV equivalent. `Observer` and `Coprocessor` would
misdescribe an aggregate decision contract. `Operator`/`Controller` are useful
execution vocabulary to evaluate in the final signature inventory, not approved
mechanical renames of our document/plan/execution types.

This iteration proposes a revision to iteration 4; the two-interface split and
exact type/package names are not yet user-approved. The earlier question about
whether callers may also replace the managed executor remains unanswered.
No implementation or persistence changes result from this naming investigation.

## Iteration 6: agreed policy split and consistent operator names

Historical naming note: iteration 8 replaces the proposed policy.ReplicaChecker
with policy.RestoreDegraded; Scheduler and operator.Controller are retained.

User response to iteration 5:

> I agree. Also let unify ReplicaChecker and operator.Controller if it is possible

Resolution: the two public decision interfaces, `policy.Scheduler` and
`policy.LifecyclePolicy`, replace the earlier aggregate Behavior proposal.
The user additionally authorizes aligning the repair and execution names with
PD where the responsibilities fit. This is terminology unification, not a
request to merge replica checking and operation execution into one component.

### Implementation mapping checked

The current [RestoreDegradedPolicy](../../internal/reconcile/policy.go) evaluates
observed Active replicas and requests a narrowly supported fresh-ID replacement
for a missing required voter. It does not execute the repair. This is a good
match for the name `ReplicaChecker`.

The current [Reconciler](../../internal/reconcile/engine.go) owns the observation
loop, leader and staging gates, stability/cooldown observations, validation and
submission of a repair. It also signals the execution engine when there is an
already approved execution to resume. Renaming the entire Reconciler to
ReplicaChecker would incorrectly put scheduling coordination into the decision
implementation. Its coordination work stays library-owned, invoking the public
Scheduler contract; the final file inventory will locate this work.

The current [ConvergenceEngine](../../internal/convergence/engine.go) reads a
durably approved execution, checks ownership and exact authorization, dispatches
its next step, verifies observed results, and records progress. It already fills
the role of an operator controller. Its implementation can move to
`internal/operator` with concrete type `Controller`, constructed and supervised
by the Node. The local replica host remains in `internal/managed`.

The current [ExecutionRecord](../../topology/execution.go) combines an ordered
Plan with identity, attempt, state, current step and audit metadata. It is the
closest data-model counterpart to an Operator. The public
[GroupController](../../topology/controller.go) is a different abstraction: an
effect adapter currently supplied by callers. Under managed hosting, its action
dispatch becomes library-owned and application dependencies are supplied through
the agreed factory, transport, storage and retirement hooks. It must not simply
be renamed to operator.Controller, which would preserve the integration burden.

### Recommended coherent vocabulary

| Current/provisional name | Refactored name | Responsibility |
| --- | --- | --- |
| `ReconcilePolicy` / proposed `Behavior.Repair` | `policy.Scheduler.Schedule` | Public replaceable cluster decision contract. |
| `RestoreDegradedPolicy` | `policy.ReplicaChecker` | Concrete default Scheduler; proposes supported missing-replica corrections. |
| `ConvergenceEngine` | `internal/operator.Controller` | Concrete library-owned driver of approved steps and verified progress. |
| `topology.Plan` | `topology.Plan` | Preview of ordered steps for a topology version, before authorization. |
| `topology.ExecutionRecord` | `topology.Operator` | Approved plan and execution progress for one version/attempt. |
| `topology.ExecutionID`, `ExecutionStatus`, `ExecutionState` | `topology.OperatorID`, `OperatorStatus`, `OperatorState` | Public identity and progress vocabulary. |
| Proposed `cluster.Operation` / `WaitOperation` | `cluster.Operator` / `WaitOperator` | Read or await an approved operator through the Cluster interface. |

The requested ReplicaChecker and Controller names can be adopted directly.
The related Operator value/API names above complete the recommendation and
supersede the provisional Operation spelling if adopted in the final target.
No additional public Controller or Checker interface is necessary. Keeping
public operator value types in topology avoids another package solely for data
types and allows internal/operator to depend on topology without cycles.

Candidate signatures, with the complete data structures still to be specified:

```go
// package policy
func NewReplicaChecker(options ReplicaCheckerOptions) (*ReplicaChecker, error)

func (*ReplicaChecker) Schedule(
    context.Context, ScheduleInput,
) (ScheduleDecision, error)

var _ Scheduler = (*ReplicaChecker)(nil)

// Relevant members of the public Cluster interface.
type Cluster interface {
    Plan(context.Context, uint64) (topology.Plan, error)
    Approve(context.Context, uint64) (topology.OperatorID, error)
    Operator(context.Context, topology.OperatorID) (topology.OperatorStatus, error)
    WaitOperator(context.Context, topology.OperatorID) (topology.OperatorStatus, error)
    // Other cluster methods omitted from this scope sketch.
}
```

ReplicaCheckerOptions contains decision-specific settings such as the stability
threshold. Driver timing, cooldown and enablement belong to the scheduling
options owned by the Node, rather than forcing every custom Scheduler to own a
worker. Zero values select defaults, a custom Scheduler can replace the default,
and users do not manually construct or start the operator Controller.

### Decision, approval and execution flow

```text
observations -> configured Scheduler (default: ReplicaChecker)
            -> typed ScheduleDecision
            -> mandatory identity/version/safety validation and plan building
            -> replicated approval and Operator record
            -> internal/operator.Controller
            -> managed local actions or caller-supplied load transport
            -> exact-result verification and durable progress

explicit cluster mutations -> Plan/Approve -> same Operator execution path
```

Scheduler output requests a change. It neither establishes authorization nor
supplies an executable arbitrary callback. The fixed core builds and validates
the supported plan and commits its approval before effects can run. An explicit
user approval and an automatically approved repair retain their existing audit
origins and admission rules while using the same execution machinery.

Controller owns ordered dispatch, retries, waits, leadership recovery and
progress. The managed host/receivers enforce exact effect authorization and
idempotent callbacks with verification of their exact authorized identities.
These checks remain required regardless of the selected Scheduler. The
recommendation remains to keep this Controller internal and library-owned;
the request to unify its name does not by itself authorize exposing a replaceable
execution implementation. Application execution remains customizable through
the already agreed explicit hooks.

Disabling automatic scheduling prevents new automatic decisions; it does not
cancel an approved Operator. Leader changes resume the recorded approved plan
instead of rerunning Scheduler to reconstruct an in-progress operation. A stale
decision must fail validation before approval, and stale work must fail the
exact-identity/attempt/step check before dispatch or progress recording.

### Granularity and persistence

PD operators are region-scoped. Our existing approved execution can include
several groups; retain that granularity when naming it Operator. Splitting it
into independent group operators would alter ordering, failure recovery and
approval semantics, so it is not implied by this naming refactor. Every Step
continues to identify its exact group and replica target.

The Go names do not require changing stored identity strings, bbolt bucket names,
JSON/protobuf field names or ledger keys. In particular, preserve the existing
version/attempt identity and exact authorization tuple during the refactor.
`Plan` is a preview; approval still validates the current version and observations
before durably creating or reusing its Operator. Candidate Operator values
returned to callers remain owned snapshots, not mutable controller handles.

The final target must inventory record/status/transition conversions, all public
query/history/watch methods, error and event names, and persisted encodings before
declaring the vocabulary migration complete. Existing scheduling, execution,
authorization, replay and cluster tests must follow the new API and retain their
behavioral assertions. The small example continues to use the defaults.

No implementation code was changed during this brainstorm iteration.

## Iteration 7: decision audit before the target specification

User response to iteration 6:

> I agree. Do we have other remaining questions/decisions?

The agreement covers ReplicaChecker, the internally owned operator.Controller,
Plan versus Operator, OperatorID and the Cluster operator query/wait methods.
It also resolves the earlier executor replacement question: the Node constructs
and manages the Controller; caller implementations replace decision policies
and application dependencies through the explicit hooks. Do not ask that
question again.

### One remaining substantive decision: scheduler agreement

The earlier recommendation to agree on cluster-affecting policy identity has
not yet been resolved explicitly. Current reconciliation settings are local;
persisting an agreed Scheduler identity/configuration therefore changes the
cluster contract, unlike a Go symbol rename.

Recommendation: store the active Scheduler identity, implementation version
and configuration fingerprint through `_meta`. Each node installs its Go
implementation at startup and declares the corresponding metadata. A node may
make new automatic topology decisions only when its declaration matches the
active cluster choice. This is a declared compatibility check, not proof that
the custom implementations contain identical code.

On a mismatch, keep Raft participation, approved Operator execution, local
replica recovery and explicit cluster administration available; report automatic
scheduling as blocked on that node. Do not halt the node or revoke already
approved work. Changing the active declaration requires an explicit replicated
cluster administration operation; a restart must not silently overwrite it.

The alternative is to keep Scheduler selection entirely local, allowing a
leader change to select a different automatic repair policy. Mandatory action
validation still applies under either model. The distinction is predictability
of automatic decisions and rolling configuration changes, not optional safety.

Question at this iteration: choose cluster-pinned Scheduler identity/configuration with
the mismatch behavior above, or permit node-local selection. After this choice,
the target can specify bootstrap/recovery defaults, an explicit update API,
rolling-change behavior and version checks at decision commit.

### Remaining work that does not need another user questionnaire

- Use the concise Node/Cluster names and complete their signatures, package
  boundaries, owned public values and the method migration inventory.
- Carry the discussed configurable defaults into the target: readiness after
  local admission/recovery, missing-required-voter repair, seven-day retired-Raft
  quarantine and fencing plus node stop after approved removal. These are the
  author's recommended defaults, not individually recorded user votes.
- Specify factory/resource ownership, the typed load transport and receiver
  methods, cancellation, partial startup and shutdown behavior using the rules
  already discussed. Keep Start separate from readiness and construction of a
  new Node separate from restarting a stopped instance.
- Preserve the existing advanced standalone load capability, documented in
  [the library guide](../architecture/library.md), behind a deliberate public
  wrapper. The source already exports load.New and supports load-only runtimes;
  removing that capability should not be inferred from simplifying the normal
  managed path. Its caller-owned membership/recovery authority must not leak
  into managed handles. Exact constructor and owner types are target-design work.
- Specify serialization per installed policy implementation and require caller
  synchronization for state shared between implementations or Nodes.
- Inventory every document, operator, history/watch, session, lock, admission,
  recovery and diagnostic capability. Complete persistence/protocol mapping and
  tests, including the new Scheduler agreement data if selected. Preserve the
  small meta example and the tests/cluster fixture boundary.

These are required design tasks, not postponed features. At this iteration the
target was pending the scheduler choice; iteration 8 resolves that choice by
explicitly postponing the cluster-wide agreement mechanism.

## Iteration 8: caller-configured scheduling, RestoreDegraded and demo HTTP

User decisions:

> Let postpone this for future, we assume that the user configres it in caller's
> code or configuration which passed for caller's code.
>
> RestoreDegradedPolicy need to rename policy.RestoreDegraded
>
> Also we will need to review and align http methods in ./example/cluster

The actual example directory is `examples/cluster`. These decisions supersede
the earlier Scheduler agreement proposal and ReplicaChecker naming; the rest of
the accepted Node/Cluster, Scheduler/LifecyclePolicy and Operator design remains.

### Scheduler selection is a startup dependency

Caller code selects/configures the Scheduler and passes it through NodeOptions.
The library parses no application configuration files. Built-in defaults remain
available, and a caller can supply its own Scheduler implementation.

No Scheduler declaration, implementation fingerprint, cluster-wide agreement
record, policy update proposal, startup mismatch refusal or scheduling mismatch
gate is introduced. Coordinating selections and rolling configuration changes
is the caller's responsibility. Leader changes can select different automatic
behavior when callers provide different implementations/options. Each concrete
decision is still validated and committed through the ordinary `_meta` protocol;
followers never rerun custom Scheduler code inside the FSM. Existing approved
Operators resume from their durable records regardless of local policy changes.
Mandatory topology/version/identity/membership checks are unaffected.

No new persisted schema is needed for Scheduler configuration. Local diagnostic
names can identify the installed policy without asserting agreement with other
nodes. Startup selection stays fixed for a Node lifetime; existing local
scheduling enable/disable controls do not become a policy hot-swap API.

### Revised built-in name

```go
package policy

type RestoreDegraded struct {
    // Private, validated decision options.
}

func NewRestoreDegraded(options RestoreDegradedOptions) (*RestoreDegraded, error)

func (*RestoreDegraded) Schedule(
    context.Context, ScheduleInput,
) (ScheduleDecision, error)

var _ Scheduler = (*RestoreDegraded)(nil)
```

The type moves from internal/reconcile.RestoreDegradedPolicy to the public
policy package. Do not add a ReplicaChecker alias or a second equivalent
implementation. RestoreDegradedOptions replaces the proposed
ReplicaCheckerOptions. Existing missing-required-voter semantics, stability
handling and bounded supported action set remain; the broader type name does
not imply general balancing, quorum reconstruction or repair of `_meta`.

The resulting terminology is:

```text
policy.Scheduler (default implementation: policy.RestoreDegraded)
    -> validated decision and approval
    -> topology.Operator
    -> internal/operator.Controller
    -> managed, idempotent and identity-verified effects
```

### HTTP review: preserve the small example

Inspected [http.go](../../examples/cluster/http.go),
[main.go](../../examples/cluster/main.go),
[README](../../examples/cluster/README.md),
[HTTP unit tests](../../examples/cluster/http_test.go) and
[Compose smoke test](../../tests/cluster/example_test.go).

The demo currently exposes local health, cluster status, three document views,
drain/activate, approval spelled apply, and execution progress. Keep exactly
those capabilities. Domain commands use POST and observations use GET. Drain
is a topology workflow, so retain the explicit drain/activate command routes
rather than introducing an arbitrary node-status setter.

Use these target routes, with the current Latest/Target/Active document names
retained. Desired/Approved were optional earlier naming suggestions, and need
not add another rename to this refactor.

| HTTP method and route | Library integration | Response |
| --- | --- | --- |
| `GET /health/live` | HTTP process liveness | 200 with local node name while the server is serving. |
| `GET /health/ready` | Local admission/session status plus Active node state | 200 when ready to serve the demo; 503 while starting, drained or fenced. |
| `GET /api/cluster/status` | `cluster.Status(ctx)` | 200 with cluster observations and operator progress. |
| `GET /api/topology/latest` | `cluster.Latest(ctx)` | 200 document and version ETag. |
| `GET /api/topology/target` | `cluster.Target(ctx)` | 200 approved document and version ETag. |
| `GET /api/topology/active` | `cluster.Active(ctx)` | 200 active document and version ETag. |
| `POST /api/nodes/{name}/drain` | `cluster.DrainNode(ctx, name, options)` | 200 staged/preview result; 202 when approved with an operator ID. |
| `POST /api/nodes/{name}/activate` | `cluster.ActivateNode(ctx, name, options)` | Same mutation contract. |
| `POST /api/topology/{version}/approve` | `cluster.Approve(ctx, version)` | 202 with `operator_id`. |
| `GET /api/operators/{id}` | `cluster.Operator(ctx, id)` | 200 progress, including state and waiting/failure detail. |

Register the two command routes explicitly rather than accepting every action
segment and switching over arbitrary strings. Existing Go ServeMux behavior
handles unsupported methods; no library HTTP server or protocol is introduced.

Corresponding vocabulary changes:

| Old HTTP spelling | New spelling |
| --- | --- |
| `/api/topology/{version}/apply` | `/api/topology/{version}/approve` |
| `/api/executions/{id}` | `/api/operators/{id}` |
| Mutation request `apply` | `approve` |
| Mutation/approval response `execution_id` | `operator_id` |
| Cluster-status execution progress field | `operator` |

The Go mutation option becomes Approve consistently. Keep Preview as a separate
existing capability, rejecting Preview+Approve together. No compatibility routes
or dual JSON fields are required under the accepted clean API refactor. Example
HTTP DTOs must project the intended public names even where old field names
remain in persisted records or the internal meta wire protocol.

Keep exact If-Match validation on drain/activate, body/header version agreement,
bounded strict JSON decoding, version ETags and stale-version 409 replies.
Retain X-Staged-Version when staging committed but approval failed, so clients
can inspect/approve that version without repeating the mutation. A 202 response
means approval was accepted, not that the workflow finished; clients poll the
Operator state. A locally lagging follower may temporarily return 404 for a new
operator, as the existing smoke test already permits.

The approval route's positive path version is its expected Latest version; it
needs no second required version header. If If-Match is supplied, require it to
match the path version. Validate before invoking Raft. Accept an empty body or
an empty JSON object and reject unknown options/query parameters, including
preview/dry_run/apply/approve flags; an approve command must not silently ignore
a purported preview. The current example ignores approval options, while the
test fixture already checks this distinction. Port that small validation
contract without importing the full fixture.

Preserve HTTP error classification: invalid requests 400, absent history 404,
version/identity/authorization conflicts 409, membership safety refusals 422,
unavailable/not-ready/stopped 503, and unexpected failures 500. All errors return
stable code/error fields and version conflict metadata where present. The final
signature inventory will align Go fault names while retaining intentional
persistence and internal protocol compatibility.

### Example and test changes required by the target

- `examples/cluster/main.go`: construct/start Node once, pass Node/Cluster to
  the HTTP adapter and stop through Node. Continue to keep configuration parsing
  and HTTP listener lifecycle in the application.
- `examples/cluster/http.go`: call the public interfaces and use the route/DTO
  vocabulary above. Keep Raft execution switches, WAL handling and load protocol
  implementation out of this adapter.
- `examples/cluster/README.md`: update all curl commands, response field names,
  operator polling and the complete route table together.
- `examples/cluster/http_test.go`: retain parallel fast decoder/version/error
  checks. Test strict approve input and verify rejected requests never invoke
  the mutation. Replace the current helper-test `/api/groups` dummy URL with a
  route that exists in this demo.
- `tests/cluster/example_test.go`: migrate the Compose smoke test to approve and
  operator names. Exercise both immediate approval and stage-then-approve, and
  retain node types, readiness, unchanged meta membership, stale-version refusal
  and absence of internal test-control routes.
- `tests/cluster/testapp/http.go` and its clients/tests: align shared endpoint
  vocabulary as library callers migrate. Keep the richer fixture-only routes and
  fault scenarios under tests/cluster; do not move them into the demo.

The refactor does not expand the Compose demo into a group administration API,
add load networking, or add a Scheduler configuration endpoint. The example
uses built-in policies and remains runnable with make run-example.

### Current resolution

The previously identified final policy decision is resolved. HTTP alignment is
now a concrete part of the refactor with the route contract above. Remaining
work is to consolidate the full target specification and its file/signature,
ownership, persistence and verification inventories. No implementation code was
changed in this discussion step.

## Final resolution — target architecture

The user confirmed the revised design and requested the target architecture
document if no questions remained. No further product decisions are required.
The [target specification](20261011-public-api-simplification-target.md) now covers:

- Public Node/Cluster/load.Group contracts, package boundaries, file inventory
  and the complete public API migration map (sections 2–5 and 11).
- Managed factory/resource ownership, typed inbound transport/action receivers,
  exact authorization verification, retirement and Node-lifetime stop holds
  (sections 3, 5, 7–8).
- Replaceable Scheduler/LifecyclePolicy contracts, policy.RestoreDegraded,
  startup settings and concrete library-owned operator.Controller (sections 6–7).
- Start/WaitReady/retry/cancellation, callback serialization, partial failures,
  stalled cleanup and joined shutdown (sections 3, 6–8).
- Complete relevant bbolt/WAL/event schemas, private compatibility DTOs, cache
  lifetimes and unchanged protocol encodings (sections 9–10 and appendix A).
- Standalone load ownership and the simple example/separate cluster-test fixture
  boundary (sections 5, 10 and 12).
- The reviewed approve/operator HTTP routes, strict parsing, error semantics,
  README/client migration and required test coverage (sections 10 and 12).
- Caller integration and key algorithm pseudocode (appendices B–C).

Document verification checks relative links, Markdown fences and Go declaration
syntax. An isolated Go prototype verifies that the defined public handle can wrap
the private representation without inheriting privileged methods. These checks
validate the specification's construction approach, not runtime behavior of an
implemented refactor. No implementation files were changed.

## NOT CONSIDERED & TODO

All target-design details previously listed here are resolved in the target
specification. There are no unanswered questions blocking implementation
planning. Required implementation and validation work is specified there.

Explicitly deferred by the user:

- [postponed] Cluster-agreed Scheduler identity/version/configuration, durable
  policy metadata, mismatch handling and cluster-wide policy update operations.
  Current behavior uses the Scheduler configured in caller code at Node startup.

The earlier force-new-cluster recovery deferral also remains unchanged; normal
recovery, readmission and reseating remain in scope. Larger-cluster correctness,
ported Raft tests, idempotent callbacks and exact authorized-identity verification
are required scope, not deferred items.
