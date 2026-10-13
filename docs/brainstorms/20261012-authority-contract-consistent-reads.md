# Authority contract and consistent reads

Date: 2026-10-12. Status: brainstorm complete; implementation has not started.

## Initial request and scope

Design the implementation of `P0: authority contract` and `P1: consistent reads`
from `docs/architecture/design-constraints.md` and
`docs/architecture/comparision-with-tikv.md`. Study the reference TiKV checkout
at `/Users/speckzzz/reps/neria/tikv`. The user explicitly permits breaking
compatibility because the library is in development.

The explicitly invoked go-brainstorm workflow keeps implementation code unchanged
and records research, alternatives, decisions and remaining questions here. The
resolved implementation specification is in
[the target document](20261012-authority-contract-consistent-reads-target.md).
There is no `project.txt`; the supplied workspace is the library checkout.

## Source context

- Library HEAD: `498cb45` (Adding stable voters for raft groups). Initial working
  tree was clean. Read README first and inspected recent 15 commits.
- TiKV HEAD: `137cbb2593decafebf99905b4d570869230565c2`.
- Library dependency: `go.etcd.io/raft/v3 v3.6.0`.
- Architecture consulted: `raft.md`, `library.md`, `runtime.md`, the two requested
  design documents, and `docs/testing/tests.md`.
- The comparison document describes a cross-log guarantee needing definition;
  it does not claim a demonstrated exploitable race. This brainstorm preserves
  that distinction.

## Findings from current code

1. `internal/metaengine/authorization.go` validates exact operator, version,
   attempt, step, action and target fields against locally applied metadata.
   It rejects a receiver behind the required execution step, but cannot establish
   that its locally applied approval is still globally current.
2. `internal/metaengine/operator_state.go:applyBeginOperator` supersedes older
   operators and updates Target in a metadata command. No affected-group fence
   is committed by that transition. Retrying a failed plan creates another
   attempt; the authority design must cover attempts as well as versions.
3. `internal/managed/actions.go` checks authority before and after acquiring a
   local operation slot, after observations, and after effects. These are useful
   checks but remain local metadata checks. The final check cannot undo an
   effect that already happened.
4. The effect classes have different owners: configuration changes commit in
   the affected group; leadership transfer is advisory; prepare/start, stop and
   retirement operate on host-local resources. A load-group epoch alone cannot
   fence an isolated host's filesystem or a running application callback.
5. `internal/metaservice/effects.go` also executes metadata membership actions.
   P0 must cover this path and the direct transfer path, including the special
   `StepSeq == 0` authorization case, rather than only load actions.
6. `internal/metaengine/cleanup_evidence.go` already retains exact plans and
   history for uncertain effects across supersession. New fencing obligations
   must likewise survive pruning, restart and snapshots until discharged.
7. `load.Group` has proposals and observations, but no read barrier or query API.
   `load.StateMachine` has Apply, Snapshot and Restore only; calls are serialized.
8. `Cluster` document reads and LockHolder return local observations. Status
   mixes replicated metadata with health and remote observations. A meta barrier
   cannot make that whole mixed status result linearizable.
9. `internal/raftengine/loop.go` owns RawNode and applies entries serially after
   Ready persistence. It currently does not consume `Ready.ReadStates`.
   `group.go` enables PreVote and CheckQuorum. CheckQuorum alone is not a fresh
   read proof.
10. The existing startup, exact-handle and shutdown ownership rules remain
    requirements: no published half-restored state, no successor redirection,
    and no application release while owned callbacks remain active.

## Reference implementation findings

TiKV's `components/raftstore/src/store/fsm/peer.rs` validates store identity,
peer identity, request term and Region epoch in its proposal path (approximately
lines 5884, 5903, 6007 and 6012). This is validation against affected-resource
state, rather than proof of instantaneous cancellation in another Raft log.

TiKV v2's `operation/query/lease.rs` has a guarded lease optimization: it checks
the term of the storage commit index and lease validity before answering a
ReadIndex request without another heartbeat round. The rest of its query code
also has application progress, split and merge guards. These database-specific
guards are not a ready-made protocol for gquorum's management authorization.

For P1, propose the simpler quorum-confirmed path first. The pinned upstream
[RawNode API](https://github.com/etcd-io/raft/blob/v3.6.0/rawnode.go) supplies
ReadIndex and Ready.ReadStates; its
[read options](https://github.com/etcd-io/raft/blob/v3.6.0/read_only.go) distinguish
safe quorum reads from clock-dependent lease reads. Library integration still
has to wait for local application and protect state-machine access.

## Decision 1: revocation boundary

Question presented: which contract should P0 guarantee?

| Alternative | Tradeoff | Decision |
| --- | --- | --- |
| Resource fencing | Approval starts revocation; completion requires durable fences at affected groups/hosts. Old effects may complete before the boundary. An unavailable required resource leaves revocation pending. | Selected |
| Strict approval boundary | Replacement approval cannot complete until older effects have been fenced. Required unavailable participants may block approval. | Not selected |
| Durable grants | Previously granted exact effects remain permitted after supersession; successors must account for them. | Not selected |

User answer: **Resource fencing (recommended): explicit pending/completed
revocation, enforced by each affected group or host.**

Consequences of the selected boundary:

- Staging alone never grants authority.
- Metadata approval and fence completion are separate durable milestones.
- A metadata leadership change alone does not revoke an immutable approval.
- Supersession requests fencing; observing supersession locally is not proof
  that a remote effect can no longer happen.
- New conflicting work must not race unresolved older work on the same resource.
- Each affected group must order its fence with its administrative actions.
- Each affected host must persist a local fence and serialize/join older owned
  operations before claiming that its fence is complete.
- Completion cannot rely on an RPC timeout, process suspicion or a bare boolean
  acknowledgement. Proof must identify the exact authority and resource.
- An unreachable host may retain unresolved local obligations even after a
  consensus fence has completed. Do not label whole-operation revocation
  complete merely because the group has a quorum.
- Full per-group failure isolation and asynchronous physical cleanup are separate
  backlog tasks; this work must not claim to implement them implicitly.

## Decision 2: read API and serialization

User selected **Barrier + query API: library owns query serialization;
write-only state machines can omit Query.** This also includes explicit
authoritative topology and lock-holder reads while status stays diagnostic.

The public load API should expose a read barrier and a read operation. An
optional interface extends the application machine with Query. Query is invoked
only after the quorum-confirmed index is locally applied, and is serialized with
Apply, Snapshot and Restore. Unsupported queries return a classified error.
Query must not mutate replicated state or perform external side effects. Its
errors fail that read rather than halting the replica as Apply errors do.

Cancellation releases the caller but does not permit overlapping machine calls
or Close while Query still runs. The library must retain callback ownership
until the actual return. A barrier does not give a renewable leadership lease;
an application using a separate read must synchronize access to the same exact
machine and treat barrier-plus-state-access as one logical read operation.

## Decision 3: read placement

User selected **Any ready replica: leader-confirmed reads with local apply
waiting on followers and learners.** Learners participate in replication, not
in the quorum that confirms the read. A node without a local replica still
requires application routing. Each read remains bound to its exact engine run.

The pinned etcd Raft implementation forwards MsgReadIndex to the current leader.
The library must validate those messages and correlate responses with the
requesting run. Leadership confirmation does not remove the local apply wait.

## Decision 4: authoritative lock-holder semantics

User answered **“I agree”** to the recommendation of **committed ownership only**.
Session expiry/renewal is not being redesigned to promise a time-valid lease.

Current `internal/metaservice/session.go` refreshes leader-local deadlines;
session grants, lock acquisition and revocation are replicated. A safe read can
report the latter state. It cannot turn configured TTL into guaranteed remaining
time, or make an external resource obey the lock.

## Initial model (superseded by the completed target specification)

Candidate authority state separates a desired authority revision in metadata
from the revision durably installed at each resource. Revisions must increase
monotonically across attempts and leadership changes. Exact immutable action
identity remains necessary; an epoch number alone is not authorization.

```go
// Conceptual shapes; names, persistence layout and fields remain to be settled.
type EffectAuthority struct {
    Revision uint64
    Action   topology.AuthorizedStep
    // Exact resource scope and immutable grant binding are also required.
}

type FenceProgress struct {
    DesiredRevision uint64
    // Exact per-group commit/apply evidence and per-host durable completion
    // evidence, including unresolved previous owners, are tracked separately.
}
```

Conceptual flow:

```text
commit replacement approval in _meta
    -> record required resource fences as pending
    -> affected group: order a fence with administrative commands in its log
    -> affected host: persist fence, reject old arrivals, join accepted old work
    -> return exact evidence and commit progress in _meta
    -> enable conflicting successor work only after its required fences complete
```

A fresh `_meta` read is useful for obtaining current authority, but cannot close
the interval between that read and an effect in another log. P1 is therefore a
building block, not a substitute for the P0 protocol.

For P1 the candidate flow is: admit a bounded read request, issue safe ReadIndex,
match its request context, wait until this exact engine has applied the returned
index, then expose a barrier or execute a serialized application query. Context
cancellation ends the wait; it does not terminate the replica. Partitioned reads
must fail or time out instead of falling back to a local observation.

### Additional implementation constraints discovered

- `internal/managed/operations.go` already serializes local effects per group,
  but startup reservations outlive canceled waiters. Fence completion must join
  the owned startup/callback, not just acquire the operation gate after a waiter
  returns.
- `internal/managed/readmission_meta.go` can rename the metadata database when
  replacing a member. Putting host fences only in that old database or inside
  removable load storage would lose the fence at precisely the wrong boundary.
  The target needs a separately retained node-local authority store, or a fully
  specified transfer protocol that preserves it across metadata replacement.
- A per-approval epoch alone does not prevent an old step from running again
  after a later step changes native membership. Define monotonic action/step
  ordering and durable exact-effect receipts within the epoch, or advance the
  resource authority revision per effect. Do not use current membership alone
  as proof that the same old effect is still authorized.
- Raft configuration entries need integration with the existing outstanding
  configuration guard. Fence installation, configuration proposal and replay
  must use one ordered protocol; checks in the managed host before enqueueing
  are insufficient.
- Leadership transfer admission can be fenced, but an election already triggered
  by an admitted transfer cannot be undone. A fence cannot promise stable leader
  identity against ordinary Raft elections. Report initiation separately from
  observed completion and document this explicit effect boundary.
- RawNode does not offer read-request cancellation. Limiting API waiters alone
  would allow canceled read rounds to accumulate in upstream Raft while quorum
  is unavailable. Bound and coalesce outstanding protocol rounds as well as
  callers; retries and requests before current-term commitment need special care.
- A newly arriving read cannot join a quorum round dispatched before its arrival.
  Freeze each batch at dispatch and queue later arrivals for the next round.
- Both managed and standalone machine adapters need to preserve the optional
  query interface (`internal/managed/replica.go`, `load/standalone.go`).
- Meta service replacement during readmission must not let a barrier on an old
  engine be followed by a read of a different state machine. Capture exact
  engine/machine ownership together for authoritative reads.

### Candidate P1 API, naming subject to final review

```go
// load package
type QueryMachine interface {
    Query(context.Context, []byte) (Result, error)
}
func (g *Group) ReadBarrier(ctx context.Context) error
func (g *Group) Read(ctx context.Context, query []byte) (Result, error)

// Cluster additions. The combined topology view is captured under one SM lock.
ReadBarrier(context.Context) error
ReadTopology(context.Context) (topology.ReadView, error)
ReadLockHolder(context.Context, string) (topology.Session, bool, error)
```

ReadView would contain owned Latest, Target and Active documents from one
replicated-state snapshot. Diagnostics such as remote group health would not be
included. Query input and output byte slices must be owned copies. Reads do not
need durable request IDs or application deduplication because Query is read-only.

## Questions identified during research and their resolutions

1. Lock-holder reads return committed ownership, as confirmed by the user.
2. Allocate a monotonic revision for each exact effect; consumed receipts prevent
   earlier-step replay. Supersession creates separate fence-only grants.
3. Order group authority in the affected Raft log. Validate configuration context
   deterministically against that authority; never decide apply from local meta
   observations. Preserve pending administrative-entry and membership safety gates.
4. Retain host fences in an explicit Node-owned authority database that survives
   meta replacement and load retirement. Fence completion joins actual old work.
5. Retain exact receipts and unresolved obligations independently of terminal
   operators/history pruning. Consumed one-shot effects are settled; uncertain
   effects require reconciliation/fencing, including after successive approvals.
6. Any ready replica supports reads. Copy request/results, keep Query serialized,
   bound wrapper requests and unresolved Raft rounds separately, and fail safely
   on cancellation/partition/owner replacement without a cached-read fallback.
7. Direct transfers and readmission participate in fencing even where they have
   no ordinary operator ID. Admission/election semantics remain distinct.

Implementation signatures, complete new persistence records, data flows,
failure cases and method algorithms are specified in the target document.

## Proposed acceptance evidence

- Delayed action after supersession, with stale receiver metadata, both before
  and after the affected resource fence; verify the selected boundary exactly.
- Old controller and replacement leader concurrently delivering exact actions.
- Replay after group commitment but before metadata acknowledgement.
- Snapshot/restart preserves fences; reused addresses and successor identities
  never inherit old authorization.
- Host-local callback delayed across fencing and restart; completion cannot
  precede joined ownership or permit old cleanup to touch successor resources.
- Isolated former leader cannot answer a new authoritative read successfully.
- A read whose index is confirmed still waits for blocked local application.
- Cancellation, saturation, delayed replies and stopped/replaced handles do not
  leak requests, redirect reads or release application ownership prematurely.
- One-participant functional tests for engine and persistence details; three-node
  Docker scenarios for consensus, partitions and leadership changes, following
  the existing test-suite boundaries.

## NOT CONSIDERED & TODO

- [potential-improvement] [porting:deffered] Lease-read optimization and explicit
  clock assumptions; recommend quorum-confirmed ReadIndex for this scope.
- [postponed] [porting:postponned-to-the-next-task] Full failure isolation,
  independent group scheduling and asynchronous cleanup separation.
- [postponed] [porting:simplefication] TiKV key-range split/merge, MVCC snapshots,
  timestamp services and transactional read semantics: outside this library task.
- [postponed] External system fencing for application side effects: a library
  read or management fence cannot fence a third-party database automatically.

## Validation record

Source inspection and documentation checks only. No Go tests ran and no
implementation files changed. Proposed acceptance scenarios are not recorded
as passing tests. Checked Markdown code-fence balance, relative Markdown links,
whitespace, and existing paths named by the method inventory.

## Final resolution

The user selected resource fencing, barrier plus optional serialized Query,
reads on any ready replica, and committed lock ownership. The completed
[target specification](20261012-authority-contract-consistent-reads-target.md)
turns those choices into an implementation design, including snapshots, local
fence durability, controller recovery and required evidence. Backward compatibility
is deliberately not required; old baseline assets remain unmodified.

The next workflow is implementation planning/execution. This brainstorm does not
change code or claim that either backlog task is already implemented.

