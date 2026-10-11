# Node ownership and load recovery

`gquorum.Node` owns one participant, its private `_meta` service and all managed
load replicas. `NewNode` performs pure validation. `Start` acquires storage and
networking and begins recovery without waiting for quorum. `WaitReady` observes
mandatory admission/recovery/session requirements plus `policy.LifecyclePolicy`.
The default gate is local admission; `policy.AllFounders` can require every
configured founder on the first bootstrap. Retained recovery does not repeat
that initial gate. Node never loads application configuration.

`Cluster` is a stable administration view of Node. `load.Group` is a view of one
exact local replica. Neither is an independent lifecycle owner. The sole capacity
settings are in `topology.Limits`; the default node maximum is three.

## Startup evidence and local ownership

Meta opening validates the complete versioned identity pin and retained state
before writing. An absent pin requires a genuinely fresh database, including
absence of ledgers, overrides and membership markers. Partial pins, unsupported
schemas and orphan retained state are refused. A retained pin resumes its exact
member; a new process supplies a fresh InstanceID.

Managed startup derives identity, founding peers and mode from approved topology.
Staged Latest alone cannot authorize a replica. The local bbolt ledger and actual
WAL pin are inspected before application acquisition. Durable store creation or
restore precedes recording the identity. Repeated ledger writes preserve original
audit fields; reseating replaces only the exact prior ID transactionally.

| Local evidence | Decision |
| --- | --- |
| New approved identity, no prior store | Fresh founding or joining preparation. |
| Matching store, missing ledger | Adopt that exact durable store. |
| Matching store and ledger | Resume. |
| Approved fresh ID replaces an older ledger ID | Reseat with exact ledger replacement. |
| Previously opened identity loses its store | Park and preserve its ledger. |
| Active/completed membership proves establishment but log is empty | Park; never silently refound an established identity. |
| Valid prepared learner with empty log | Resume preparation. |
| Foreign or unreadable store | Park and preserve the evidence. |

One startup reservation owns each group. Concurrent requests for the same exact
replica wait for that startup and share its handle; other groups proceed
independently. Canceling a waiter does not cancel the owned startup. Factory
inputs are copies. A successful factory transfers `Application.Close` to Node,
including when cancellation occurs before its late return. Factory failure leaves
partial acquisitions with the factory. Node joins replay and engine work before
Close. Failed Close retains ownership and blocks replacement until a serial retry
succeeds. Publication occurs only after local replay readiness.

`StopLoadGroup(ctx, ref)` installs an exact volatile hold before joining the
replica. Recovery and ordinary approved starts respect the hold. An explicit
start clears only that matching hold; a fresh Node has no previous lifetime's
holds. Retirement can still join and remove a held identity. A stopped handle
never becomes a view of its successor. `ParkedLoads` and `Node.Status` return
owned observations. `LoadParkedError` matches `fault.ErrReplicaNotReady` while
preserving its cause. Successful startup clears the matching parked observation.

## Approved actions, transport and retirement

The internal operator controller dispatches an immutable approved plan. Typed
load requests distinguish sender, receiver and target replica. Node receivers
verify the entire operator/version/attempt/step/action/identity tuple and recheck
authority after waits. Unknown future approvals, stale attempts and mismatched
identities never authorize work. A remote refusal may be retried by a dispatcher
whose complete local approval is still valid; refusal is never completion proof.

The application supplies `load.Transport` and authenticates its protocol before
calling Node's typed receivers. Calls for different destinations are independent;
the Raft engine copies messages into bounded per-destination queues. Slow peers
cannot block healthy-majority delivery. Snapshot completion reports identify the
issued delivery and exact source/destination process and replica. Old reports
cannot reach replacement handles. Successful shutdown joins send workers.

`load.Group.ProposalStats` exposes pending limits, queued work and commit waiters;
these are local observations. Canceling a proposal wait cannot undo a committed
command. State machines must deduplicate application request IDs on uncertain
retries. `WALStall` and `Error` expose local persistence/engine diagnostics.

The library expands approved cleanup into exact identities, joins the owned
replica, probes retained storage and verifies its ledger. If application state
exists outside Raft, `LoadOptions.Retire` must independently verify its complete
identity and request proof, perform idempotent cleanup, and return the entire
matching `RetirementResult` with `Verified=true`. Absence on retry still requires
verification. A foreign or successor resource must be preserved. Only then may
the library quarantine the exact Raft state and forget its matching ledger.
Application transactions and outboxes remain caller-owned.

`storage.RetirementStore` enumerates exact retired identities and opaque tokens.
Lifecycle retention decides when collection is eligible; collection rechecks the
identity/token, live assignments and ledger. There is no implicit age-based
quarantine sweep. Retained approval evidence survives history pruning until its
cleanup obligations can be proven complete. Staged-only identities never enter
that retirement authority.

## Readmission and removal

`ReadmitNode` rotates a declared active node's meta/load identities only after
fresh health reports it dead. Live/unknown health or unconverged documents are
refused before staging. The ordinary CAS/optional approval pipeline preserves a
staged version on later approval failure. Retry `Approve` for that version.

A returning process joins with its caller-supplied retained paths. The library
validates the exact admission grant, fences and joins the former host, verifies
actual storage against the retained approved catalog, and invokes application
retirement with a readmission proof. It quarantines exact old load state and
moves the closed old meta database to a deterministic backup derived from its
member/admission identity. The backup retains its ledger; a verified retry can
resume after a prior move. Foreign pins, symlinks and replacement state are
refused. A new host starts only after the exact granted fresh meta pin is installed.

`LoadStoredIdentity(path)` reads a closed database's validated durable pin without
creating or mutating it and intentionally returns no process ID. Callers may use
it to choose retained restart versus joining. Configuration and any application
affiliation marker remain outside the library.

After applied, durably authorized self-removal, Node immediately fences meta and
load participation. `LifecyclePolicy.Removal` chooses stop or retained diagnostics
only after mandatory fencing/joining. A failed or delayed callback cannot undo
the fence. Removal proof includes exact process/member and approved version/index;
staged edits, drain, demotion, outgoing joint membership or another member's
removal do not trigger it. The durable membership-seen marker survives snapshots.

Join classification waits for native and durable FSM cursors to cover the
committed current-term state. Identity conflicts are terminal and wake WaitReady;
availability and pending/excluded admission can remain parked. A leader's volatile
exact-process name-conflict diagnostic never displaces the live holder's route or
allocates membership. `Cluster.ParkedJoins` exposes these observations.

## Shutdown, reads and policy callbacks

`RequestStop` schedules teardown; callbacks may invoke it and return. `Stop(ctx)`
joins the same owned teardown, retaining ownership on timeout. Node cancels its
lifetime, quiesces meta producers, joins admitted effects, policy callbacks and
managed replicas, closes owned load storage, then closes meta storage. Callback
or Close errors remain observable; unsuccessful release requires a serial retry.
No new replica can overlap an unresolved owner. A callback must not wait for its
own group's lifecycle or Node.Stop, which would wait for that callback itself.

Injected stores, transports, event buses and journals remain caller-owned. A WAL
store selected through `LoadStorageOptions.WAL` is Node-owned. Close borrowed
resources only after Node has joined. The factory's returned application resources
are the exception: ownership transfers on success through `Application.Close`.

Scheduler calls are serialized per node. The lifecycle runner serializes readiness,
retention and removal callbacks together. Deadlines bound accepted results; a
noncooperative callback stays owned until actual return, with no overlapping
replacement invocation. Policy decisions cannot bypass core authorization,
persistence, identity or membership checks.

`Node.Status()` reads local owned snapshots without probes. `Cluster.Status(ctx)`
collects observations; neither proves quorum or linearizability. Compare leader
and term when assessing continuity. Follower approvals make at most one control
hop to the observed leader. Domain errors preserve exact expected/current CAS
versions; empty success replies cannot identify an operator and are refused.

The small `examples/cluster` app demonstrates `_meta` topology, status, approval,
drain and activation. Full application FSM/protocol/retirement and fault controls
belong to `tests/cluster/testapp`; lifecycle orchestration belongs to the library.
See the [public integration guide](library.md) and
[ownership test mapping](../testing/runtime-ports.md).
