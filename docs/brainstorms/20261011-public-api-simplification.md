# Brainstorm: simpler node, cluster and load-group APIs

Status: clean API refactor, separate group/replica operations and a deliberate
public group wrapper agreed. Startup-selectable built-in policies are a user
requirement. Managed local replica lifecycle through a caller factory and
transport is agreed. Detailed policy and lifecycle contracts are under discussion.
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

No answer recorded yet.

## NOT CONSIDERED & TODO

These are unresolved design details, not exclusions from the refactoring:

- [potential-improvement] Finalize the exact public package boundary under the
  agreed clean API refactor before specifying created/updated files.
- [potential-improvement] Finalize agreed managed hosting's factory/resource
  ownership, explicit stop-hold lifetime and inbound effect dispatch contracts.
- [potential-improvement] Finalize built-in policy names, defaults, validation
  and local versus cluster-agreed settings. Specify the durable agreement needed
  for topology-changing policies and the treatment of existing runtime controls.
- [potential-improvement] Specify Start/WaitReady/retry/partial-failure behavior,
  callback reentrancy and cancellation ownership.
- [potential-improvement] Map every existing document, watch, session, lock,
  reconciliation, admission and recovery API so simplification loses no required
  capability. Avoid exporting internal protocol entry points accidentally.
- [potential-improvement] Inventory bbolt buckets, WAL metadata, protocol fields
  and persisted identities; distinguish Go API renames from storage migrations.
  No persistence schema change is implied by the current proposal.
- [potential-improvement] Specify the treatment of standalone load engines and
  preserve the small meta demo / separate cluster fixture boundary.

The final target document will follow after these decisions. It must include the
full public signature map, ownership and startup/shutdown flows, failure handling,
storage/protocol compatibility inventory and implementation pseudocode.
