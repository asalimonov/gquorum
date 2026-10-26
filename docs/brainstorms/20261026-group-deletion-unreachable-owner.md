# Whole-group deletion with an unreachable former owner

Status: brainstorm complete; target architecture prepared for implementation planning.
Date: 2026-10-26.

## Initial input and scope

The user requested a go-brainstorm design for GitHub issue #5:
<https://github.com/asalimonov/gquorum/issues/5>.
The issue describes whole-group deletion waiting indefinitely for an unreachable
former owner's exact stop/readmission proof, with no destroyed-host waiver.

The user subsequently requested comparison with the local etcd and TiKV projects:

- `/Volumes/M4DATA/Users/speckzzz/reps/neria/etcd`
- `/Volumes/M4DATA/Users/speckzzz/reps/neria/tikv`, including Placement Driver behavior.

This session changes brainstorm documentation only. The task was supplied in chat;
there is no `project.txt` in the current checkout. The current gquorum worktree is
the project, and current contracts are in `docs/architecture`.

## Evidence provenance

| Project | Inspected revision | Qualification |
| --- | --- | --- |
| gquorum | `8e57798`, branch `feat/group-deletition-wo-node` | Clean worktree at research start; same commit as local main. |
| etcd | `00a7653ca9dc7d80475565a2279ab4cf741692eb` | Local checkout clean. |
| TiKV | `137cbb2593decafebf99905b4d570869230565c2` | Tracked sources clean; existing untracked `docs/` left untouched. |
| PD | `34ce7eca7eaf9b4fd06061984a1cc35884c49ee4` | Independent reference revision already cited by gquorum's TiKV comparison; not a TiKV dependency pin or evidence of the deployed PD image. |

The local TiKV checkout contains the PD client and Docker Compose configuration
using a PD image, rather than PD server source. The historical PD checkout named
in `docs/architecture/comparision-with-tikv.md` was unavailable. Relevant PD files
were downloaded from the official repository at the recorded revision into
`/tmp/gquorum-issue5-pd-34ce7eca`. Initial master browsing was exploratory; the
conclusions below use the recorded revision.

## Current gquorum behavior

The preceding issue analysis found that the issue remains unresolved. Five
focused planner/metadata tests passed in that earlier analysis; no new tests
were run during this upstream comparison.

- `topology/planner.go:89`: `planDeletedGroup` emits stops for every former
  replica whose progress is not `RetainedStopped`.
- `topology/plan_retirement.go:47`: `requiredRetainedStop` returns true whenever
  the desired group is absent. Thus previously detached host cleanup becomes a
  deletion prerequisite.
- `topology/plan.go:314`: deletion shape validation requires stops/readmission
  coverage for all retained owners.
- `internal/metaengine/scoped_deletion_completion.go:47`: completion accepts
  each owner's exact stop receipt or consumed readmission retirement batch.
- `internal/metaengine/scoped_deletion.go:21`: explicit unimplemented
  destroyed-host waiver TODO.
- `internal/metaengine/scoped_deleted_authority.go:9`: native obligations become
  vacuous only after completed deletion. Its reasoning currently depends on
  every possible former owner having joined its stop.
- `tests/cluster/delete_wait_test.go:18`: the Docker regression requires deletion
  to wait for the unreachable host, while allowing an unrelated group to progress.
- `docs/architecture/library.md:229`: the same requirement is documented.

Existing metadata already distinguishes removal from local shutdown and storage
cleanup. Relevant fields of `cleanupRecord` are:

```go
// Existing fields, abridged; this is not a proposed schema change.
Identity       storage.Identity
MemberID       uint64
OperationID    topology.OperationID
StopAction     *topology.AuthorizedStep
RetireAction   *topology.AuthorizedStep
ReadmissionKey *topology.AuthorityKey
Removed        *topology.EffectReceipt
Stopped        *topology.EffectReceipt
Retired        *topology.EffectReceipt
Collection     *authoritystore.CollectionReceipt
```

The planner's `RetainedCleanup` enum alone is not a sufficient new proof: it
means retirement debt exists and stop is unproven. Any relaxation must validate
the actual retained removal authority, exact identity and remaining effects.

## Comparison conditions

These cases must be distinguished when evaluating the requested fix:

| Case | Required distinction |
| --- | --- |
| One missing voter of a three-voter group | The affected group can normally commit using the remaining two. |
| Former peer already removed through committed membership change | Native removal is proven even though its process/storage may remain. |
| Missing peers leave the load group without quorum, while `_meta` has quorum | Management consensus cannot substitute for load consensus. |
| A whole old load quorum is partitioned from management but connected internally | A metadata tombstone alone does not establish that those processes stopped committing. |
| Host declared permanently destroyed | Administrative assertion is distinct from observed local shutdown or physical collection. |

etcd has one consensus group rather than gquorum's management/load separation.
TiKV Region merge removes a source group while preserving its data in a target;
it is the closest inspected whole-group lifecycle analogy, not an arbitrary
application-group destruction API.

## etcd findings

`server/etcdserver/server.go:1423` implements `RemoveMember` by proposing
`ConfChangeRemoveNode` after quorum checks. `mayRemoveMember` at line 1590
explicitly allows removal of an unavailable member where the health checks pass.
`configure` at line 1737 waits for the configuration change to apply and Raft to
advance on the responding server. It does not collect the removed host's stop
receipt or wait for that host's disk cleanup.

`server/storage/schema/membership.go:73` atomically deletes the active member row
and writes its ID to `MembersRemoved` inside the applying transaction.
`server/etcdserver/api/membership/cluster.go:429` retains the removed ID in memory.

Returning removed peers are rejected:

- `server/etcdserver/server.go:697`: Raft messages from removed IDs are refused.
- `server/etcdserver/api/rafthttp/http.go:405`: their streams receive HTTP 410.
- `server/etcdserver/api/rafthttp/stream.go:621`: HTTP 410 becomes a critical
  member-removed error.
- `server/etcdserver/server.go:842`: that critical error ends the server loop.

Tests inspected, not executed:

- `tests/integration/cluster_test.go:358`, `TestRejectUnhealthyRemove`: accepts
  removal of a stopped member while rejecting removal that breaks live quorum.
- `tests/integration/cluster_test.go:397`, `TestRestartRemoved`: retained restart
  of a removed member exits.

Limit: removal success is not evidence that every old process has physically
stopped. In particular, `server/etcdserver/v3_server.go:138` skips the quorum read
barrier for explicitly serializable reads. Do not generalize the membership
guarantee into a promise that no stale local read or old callback can occur.
The inspected membership API provides no equivalent of an external `_meta`
deleting the entire etcd consensus group without its participation.

## TiKV and PD findings

### Ordinary peer removal

[PD RemovePeer](https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/pkg/schedule/operator/step.go)
issues `ConfChangeType_RemoveNode`. `RemovePeer.IsFinish` checks that the store's
peer is absent from the reported Region. There is no removed-host stop receipt
in this completion predicate. The Region's Raft protocol commits membership;
PD observes the resulting Region state.

TiKV `components/raftstore-v2/src/operation/life.rs:12` documents the split:
quorum agrees to removal, the removed peer can be outside that quorum, and local
destruction happens later. Applying membership changes retains the exact old
peers in `removed_records`:
`components/raftstore-v2/src/operation/command/admin/conf_change.rs:310`.

The leader retries tombstone delivery in `life.rs:803`. Peers report destruction,
and `UpdateGcPeer` commits removal of the corresponding cleanup records. A local
durable tombstone and peer IDs/epochs reject stale recreation (`life.rs:218`).
v1 also asks PD to validate an old peer and requires compatible epoch evidence;
absence of the entire Region in PD is explicitly insufficient to destroy it
(`components/raftstore/src/store/worker/pd.rs:1765`).

The three-node regression
`tests/integrations/raftstore/test_conf_change.rs:869`,
`test_remove_node_on_partition`, isolates peer 3 and verifies that removing it
works while an unsafe removal of a live peer does not.

### Whole source Region disappears with an unreachable former peer

The strongest direct example is the raftstore-v2 test
`tests/integrations/raftstore/test_merge.rs:1879`,
`test_gc_source_removed_records_after_merge`:

1. Create left and right Regions on three stores.
2. Isolate store 3 and commit its peer's removal from the right Region.
3. Merge the right Region into the left while store 3 remains isolated.
4. Assert that the surviving Region retains the source's removed-peer record.
5. Restore connectivity, destroy the stale source peer, and clear the retained
   cleanup records.

`components/raftstore-v2/src/operation/command/admin/merge/commit.rs:544`
transfers source cleanup into `MergedRecord`, including source Region ID, epoch,
peers, removed peers, target identity and merge indices. The surviving target
owns cleanup after the source group disappears.

Important qualification: a missing **current** peer differs from a missing
already-removed peer. PD's normal merge checker excludes unhealthy/pending
Regions, and raftstore-v2's `start_check_trim_status`
(`operation/command/admin/merge/prepare.rs:360`) asks all current source/target
peers for availability. Some isolated-peer merge tests are explicitly v1-only.
Therefore this code does not establish that a v2 Region can always merge with
an unreachable current member. Removing that peer first is the demonstrated path.

### Permanent store retirement and cleanup waiver

TiKV has an additional path beyond waiting for a peer acknowledgement:
`life.rs:772`, `on_store_maybe_tombstone_gc_peer`, treats relevant peers on a
tombstoned store as confirmed for GC bookkeeping, and the GC tick proposes the
record update. `src/server/resolve.rs:103` obtains the tombstone signal from PD.
This retires outstanding cleanup bookkeeping without proving bytes were erased
on the unavailable host.

[PD store lifecycle](https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/server/cluster/cluster.go)
provides:

- `RemoveStore(storeID, physicallyDestroyed)`: mark Offline, optionally with a
  permanent physical-destruction assertion. This alone is not a completed Region
  removal or a physical deletion receipt.
- `UpStore`: refuses both tombstoned and physically destroyed stores.
- `BuryStore`: establish a store tombstone. Normal background burial follows
  evacuation; unsafe recovery can force burial under its own checks.

`pkg/unsaferecovery/unsafe_recovery_controller.go:226` excludes declared failed,
removed or physically destroyed stores from recovery reporting and drives a
separate recovery protocol on survivors. Ordinary peer removal does not gain
missing Raft votes from PD's own quorum.

Do not copy TiKV's GC confirmation representation into gquorum's `StoppedAt`,
`RetiredAt` or `CollectedAt`: those fields currently promise different physical
milestones. If adopting a waiver, represent that administrative decision
separately and preserve exact identity and audit provenance.

## Design implications and candidate directions

The research supports separating the proof that an old replica no longer has
cluster authority from the proof that its application and storage have been
released. It does not support declaring a live partitioned group stopped merely
because `_meta` approved deletion.

| Candidate | Benefit | Constraint/status |
| --- | --- | --- |
| Preserve detached debt for a peer already removed through Raft during later whole-group deletion | Direct counterpart of TiKV's demonstrated source-merge case; may reuse existing removal receipts | Recommended starting point; audit old grants, incomplete founding, native obligations and completion validators before declaring safe. |
| Remove an unreachable current peer through the load quorum before stopping the remaining owners | Extends the normal path to an unavailable minority without operator host-destruction assertions | Needs ordering that preserves quorum until all membership work is committed; never remove the final voter. Final design pending. |
| Commit an irreversible terminal record in the load Raft log, then detach host cleanup | Potential direct whole-group protocol that avoids sequential membership shrinking | Alternative, not selected; introduces durable engine/replay semantics and requires load quorum. |
| Explicit permanent-host fencing/destruction assertion | Covers unprovable host shutdown where an operator can establish permanent loss/fencing | Separate audited semantic decision; never manufacture physical cleanup evidence. Scope pending. |
| Treat `_meta` tombstone or failure timeout alone as completion | Small implementation but weak completion semantics | Not supported by the inspected normal upstream protocols; an isolated old load quorum may continue. |

Illustrative decision logic, not an approved algorithm or implementation:

```go
for _, owner := range exactFormerOwners {
    switch {
    case hasExactStopOrReadmissionProof(owner):
        accountForStoppedOwner(owner)
    case hasValidatedCommittedRemovalProof(owner):
        retainDetachedCleanup(owner) // Do not create stop/collection timestamps.
    default:
        requireAdditionalSafetyProof(owner)
        // Native removal before shutdown, a terminal group record, or an
        // explicitly designed permanent-host assertion; still undecided.
    }
}
// Complete only after all still-authorized owners/effects are handled.
// Preserve the permanent group tombstone and outstanding exact cleanup in _meta.
```

This would change the current all-former-owners-joined deletion contract in
AGENTS.md and architecture docs. Upstream removal semantics do not guarantee
that unreachable application processes cannot finish previously committed work.
The desired public completion guarantee remains to be agreed.

## Discussion ledger

1. User requested design of issue #5. Accepted scope: brainstorm, no source edits.
2. User requested etcd/TiKV/PD comparison under equivalent failure conditions.
   Research completed above; no implementation option chosen by the user yet.
3. Previous scope question: should the fix cover both a functioning load quorum
   and permanent loss of that quorum, or only the normal quorum-available case?
   Recommended direction to discuss: automatic normal deletion with detached
   cleanup, plus a distinct explicit permanent-host path if quorum-loss deletion
   is required.
4. User clarified the desired direction: only `_meta` quorum should be required
   to remove a group; removal creates the next document version; missing hosts
   receive the change later and automatically remove their local groups. The
   user asked why the load group's quorum would be necessary. This supersedes
   the earlier recommendation to require native removal before whole deletion
   as a universal prerequisite. No source implementation is authorized here.

## Clarification: metadata deletion versus immediate cessation of service

The requested logical deletion does not inherently require load-group quorum.
`_meta` is already the authority for approved group existence and permanent group
ID retirement. Its quorum can commit an approved deletion and exact retained
cleanup obligations independently of the affected load group's availability.
Staging the next document alone must still not authorize effects; existing
approval semantics remain relevant.

The earlier recommendation mixed this logical deletion with a stronger guarantee:
that an isolated old group cannot continue accepting operations after deletion
is reported complete. Native membership removal or a committed terminal record
is one way of obtaining such a guarantee, rather than a universal requirement
for deleting the group from authoritative topology.

Concrete partition example, with all five nodes voting in `_meta` and load group
G having voters C, D and E:

| Partition | `_meta` votes | G votes |
| --- | --- | --- |
| A, B, C | 3/5: can approve deletion | 1/3 |
| D, E | 2/5: cannot receive/commit the new metadata state | 2/3: can still commit load operations |

Stopping C after it applies deletion does not stop D and E. gquorum's existing
load operations use their own Raft group; they do not acquire fresh `_meta`
approval for every operation (`load/group.go:34`, `docs/architecture/raft.md`).
An offline process is different: it executes nothing while offline, and its
startup must confirm current authorization before publishing a retained replica.

Candidate contract matching the user's direction:

1. Approved deletion commits through `_meta` with a new document version and a
   permanent group tombstone; no load quorum is required for this decision.
2. Nodes that apply the approved deletion reject new work and reconcile exact
   local replicas through stop, retirement and collection.
3. Unreachable owners remain represented by durable detached cleanup debt.
4. Returning/restarting hosts obtain authoritative metadata before publishing
   old replicas and reconcile tombstoned groups automatically.
5. Current snapshots must retain deletion/cleanup evidence even when the original
   document version has been pruned. The protocol cannot require replaying every
   historical topology version.
6. Successful logical deletion does not by itself prove immediate shutdown of an
   already-running isolated load quorum, joined application callbacks or disk
   erasure.

Unresolved question: may a running partition continue serving until it learns of
the deletion, or is a bound on that period required? A bounded guarantee would
need an additional serving-authorization/fencing protocol, with explicit handling
of expiry and in-flight operations. Such a protocol is not implied by merely
having a permanent metadata tombstone or observing session health.

## Follow-up: every host runs metadata, and connectivity failures correlate

The user reaffirmed that every cluster host must run `_meta`, and observed that
broken metadata connectivity usually also prevents load replicas from reaching
their leaders. They asked whether that makes the deletion approach safe without
direct enforcement of the dependency.

Every host running `_meta` remains a mandatory assumption. The five-node example
above already satisfies it. D and E both have metadata replicas, but together
cannot commit metadata; their two load votes can still elect a load leader and
commit G operations. Connectivity correlation does not establish quorum
equivalence. The separate metadata/load ports can also fail independently.

Further source inspection confirmed that the current implementation deliberately
allows established load owners to continue without metadata quorum:

- `internal/managed/authority.go:241`: accepted local owners bypass fresh
  metadata authority queries during reconciliation; the comment explicitly
  preserves independence from metadata quorum.
- `load/group.go:34` and `internal/raftengine/group.go:314`: proposals use the
  load engine, with no per-operation metadata-session gate.
- `internal/raftengine/read_requests.go:31`: safe reads confirm the load group's
  quorum rather than metadata quorum.
- `node_lifecycle.go:296` and `lifecycle/default_hooks.go:62`: session liveness
  contributes to startup readiness. That does not enforce continuing load
  authorization after publication.
- `internal/metaservice/session.go` and
  `docs/architecture/comparision-with-tikv.md:113`: session renewal refreshes
  leader-local deadlines; it is not a reusable fresh-quorum fencing proof.

Design assessment:

- Metadata-only logical deletion plus automatic cleanup after catch-up remains
  valid under the user's stated eventual-cleanup model. No load quorum is needed
  for that topology decision.
- The statement that an isolated deleted group cannot continue serving needs
  explicit enforcement. Merely detecting likely loss of connectivity, or a
  cached leader/session observation, is insufficient.
- A possible stronger design would require expiring quorum-backed serving
  authorization, local refusal after expiry, and authoritative catch-up before
  resuming. A deletion-completion guarantee based on that design must account
  for outstanding authorizations, leader changes, and in-flight work. Existing
  session TTL cannot simply be relabeled as that authorization.
- Such enforcement also stops otherwise healthy load groups when metadata is
  unavailable. That availability trade-off is a separate contract decision and
  has not been chosen by the user.

Pending focused question: should losing metadata authority automatically suspend
all local load-group serving, even when those load groups still have quorum?

## Decision: metadata-only deletion with independent load availability

The user explicitly rejected suspending load serving when `_meta` authority is
unavailable: replicas should continue serving while their own group has quorum.
The user also clarified that an operator deletes a group after its real workload
has ceased. A metadata quorum must be able to accept the new document and enforce
it later on temporarily unavailable hosts without waiting for permanently lost
hosts.

These are the accepted design inputs:

| Decision | Chosen behavior |
| --- | --- |
| Authority to delete a load group | Approved document committed through `_meta` quorum. |
| Load quorum prerequisite | None for whole-group deletion. |
| Metadata availability and existing load serving | Existing load serving remains independent; no new serving lease or automatic suspension. |
| Workload quiescence | Operator/application responsibility before requesting deletion; no new traffic-idleness heuristic or drain handshake. |
| Missing owner | Does not block logical deletion; retain exact deferred shutdown/retirement/collection work. |
| Host returns | Apply authoritative metadata and automatically reconcile deleted identities; preserve startup authorization before publication. |
| Permanently lost host | Logical deletion can finish; unconfirmed physical cleanup remains truthful. |
| Group identity | Permanent retirement remains; the deleted group ID cannot be reused. |

The protocol must not depend on the inference that metadata-quorum loss implies
load-quorum loss. It must work when the unavailable hosts are powered off and
when they form a still-running partition. The selected semantics do not require
proof that every such host has ceased application activity at logical completion.
This resolves the earlier serving-lease question; no lease or explicit
destroyed-host waiver is needed merely to unblock logical group deletion.

### Concrete completion model to carry into the target design

Preserve the public staging/approval distinction. `DeleteGroup` without approval
only stages the next version; staging alone never authorizes shutdown. Approval
should atomically install the desired omission, permanent tombstone, exact
cleanup obligations, and logical completion of the deletion's group block.
Other blocks in the same operation retain their own completion requirements.

`WaitOperation` for a deletion-only operation can therefore succeed before all
former hosts have stopped. `ReadProgress` continues exposing actual cleanup
milestones. Never fill `StoppedAt`, `RetiredAt` or `CollectedAt` merely because
the approved document no longer contains the group.

Proposed algorithm, still subject to detailed proof/retention review:

```go
func approveWholeGroupDeletion(state State, approved Document, plan Plan) State {
    validateCASAndExactApprovedScope(state, approved, plan)
    owners := collectAllPossibleOwners(state, plan.GroupID)

    next := state.Clone()
    next.Target = approved
    next.RetireGroupID(plan.GroupID, approved.Version)
    for _, owner := range owners {
        // Keep exact historical member/replica, storage class, actions and proof.
        // Existing debt keeps its origin; additional owners receive exact debt.
        next.PreserveOrCreateDetachedDeletionDebt(owner)
    }
    next.CompleteLogicalDeletionBlock(plan.GroupID, approved.Version)
    validateCandidateGraph(next)
    return next // Installed atomically by the replicated metadata apply.
}

func reconcileDeletedReplica(owner ExactOwner, deletion ApprovedDeletion) error {
    verifyExactDeletionAuthority(owner, deletion)
    rejectFurtherLocalUse(owner)
    joinOwnedReplicaAndCallbacks(owner)
    recordActualStopReceipt(owner)
    retireExactStorageAndApplication(owner)
    // Existing retention/collection protocol then runs independently.
    return nil
}
```

### Implementation seams identified after this decision

- `topology/plan_partition.go` and `topology/plan_retirement.go`: deletion stops
  belong in detached cleanup. Same-host replacement stops must remain required.
- `internal/metaengine/scoped_approval.go`: approval already prepares a complete
  candidate graph before installation, including owners, cleanup, deletion and
  completion; use this atomicity boundary.
- `internal/metaengine/scoped_deletion.go` and
  `scoped_deletion_completion.go`: complete based on retained exact obligations,
  rather than requiring all host receipts at that moment. The current code only
  searches required steps for new stop actions and must support detached ones.
- `internal/metaengine/scoped_cleanup_handover.go` and
  `scoped_cleanup_progress.go`: current validation explicitly forbids detached
  deletion stops; revise it without weakening identity or receipt checks.
- `internal/operator/dispatch_cleanup.go`: existing bounded dispatch processes
  each identity independently, including grants covering multiple hosts. Reuse
  this ownership model for post-deletion stop work.
- `internal/metaengine/scoped_deleted_authority.go`: its existing justification
  assumes all former hosts have stopped. Rewrite the administrative-terminal
  semantics and audit revocation queries; logical deletion must not manufacture
  a physical fence receipt or settle an unresolved host floor.
- Retained readmission batches, incomplete creation, superseded native grants,
  delayed responses and historical cleanup need detailed analysis under the
  new semantics. The confirmed direction does not waive these checks.

## NOT CONSIDERED & TODO

- **[postponed]** Detailed handling of in-flight Apply/Query/Close and external
  effects. Logical deletion does not promise remote callback completion; local
  teardown still must join ownership before release.
- **[postponed]** Historical/in-flight membership changes, delayed grants,
  prepared-only replicas, retry and supersession: necessary safety audit for any
  relaxed completion predicate.
- **[potential-improvement]** A separate permanent-host declaration for retiring
  physical-cleanup bookkeeping. It is not needed for the selected logical
  deletion contract; retain unconfirmed debt under existing rules for now.
- **[postponed]** Full persisted schemas, public API/signatures, migration decision,
  and detailed test plan; write the target specification after design choices.
- **[potential-improvement]** TiKV table DROP/SQL GC and range-destruction workflows
  beyond the inspected `unsafe_destroy_range` entry point; these delete data
  ranges and are not an arbitrary Raft-group deletion contract.
- **[postponed]** Running upstream integration suites. This comparison is source
  and test inspection, not fresh empirical validation.

## Session outcome so far

etcd demonstrates quorum-based member removal without host-stop acknowledgement.
TiKV demonstrates removal of a whole source Region while an already-removed
former peer is still unreachable, transferring cleanup responsibility to a
surviving owner. TiKV additionally has store-tombstone-based GC bookkeeping
retirement. gquorum can draw on these mechanisms, but must explicitly select its
whole-group completion guarantee and its behavior when the load quorum is lost.

## Final target architecture — 2026-10-26

The user requested the target architecture with maximum reuse of existing logic
and without breaking the public gquorum API.

Created [the detailed target](20261026-group-deletion-unreachable-owner-target.md).
It supersedes the earlier candidate protocol table in this historical log.

Final design:

- Existing CAS staging and approval commit the logical deletion through _meta.
- New deletion plans place exact host stops and retirement in the existing
  cleanup partition; no load quorum or missing-host acknowledgement is required.
- Existing tombstones, cleanup rows, bounded dispatcher, host authority journal,
  storage retirement and collection are reused.
- Pending readmission receives durable ordinary debt without inventing receipts;
  its existing complete batch can settle that debt.
- Native revocation settlement still requires actual stop proof for all retained
  owners. Logical deletion alone does not manufacture a physical fence.
- Public method signatures, options, JSON record fields, metadata schema 4,
  authority journal schema 2, transport actions and ports remain unchanged.
- Existing approved immutable plans retain their original required-step/retry
  semantics. This scope adds no plan migration or rolling-version compatibility.
- The target inventories internal methods, exact persisted schema excerpts,
  algorithms, all relevant flows, failure cases, API semantics, documentation
  changes and owning-package/process verification gates.
- No implementation source was edited during the brainstorm.

Verification performed for the target:

- Checked referenced source paths and existing method declarations.
- Compared all 12 included record declarations exactly with current source.
- Checked document links, balanced code fences, template expansion and whitespace.
- Checked the focused functional-test Make invocation with make -n; it preserves
  the quoted test-name expression.
- Ran git diff --check. The brainstorm directory is intentionally gitignored,
  so the document-specific checks above also examined both saved files directly.
- No implementation or upstream integration suite was run for this documentation
  task; the prior five focused tests remain historical analysis evidence only.

BRAINSTORM DONE


## Planning verification correction — historical metadata log replay

During go-plan verification, source inspection found a compatibility gap in the
initial target. Restore can apply a pre-change committed begin_operation command
after its snapshot. The originally specified new-approval shape rejection would
then refuse an already-committed historical required-stop plan. Old zero-step
readmission-covered deletion plans also make inference from step shape ambiguous.

The target was corrected in sections 3.3, 5.8, 9.15a–9.15c and 13.24:

- Add one optional internal BeginOperationProposal.DeletionSemantics field.
- Zero/absent preserves historical approval semantics; new service approvals
  write 1 to select detached logical deletion. Unknown values refuse.
- Pass that committed value through approval-only helpers; use ephemeral helper
  fields, without adding a mode to public Plan/Operation or stored snapshots.
- Preserve old command bytes when the field is absent and old commands' actual
  replay behavior, not only their decoded shape.
- Retry of a mixed operation preserves the already-successful new deletion block.
- Public APIs, metadata snapshot schema 4 and authority journal schema 2 remain
  unchanged. The only new wire field is in the internal approval-command body.

This is a compatibility correction needed to satisfy the requested API/reuse
constraints, not migration of old plans or a new deletion policy choice.
It was communicated during planning. No source implementation was edited.

The execution plan is
[20261026-group-deletion-unreachable-owner.md](../plans/20261026-group-deletion-unreachable-owner.md).
It sequences seven tasks, including an atomic activation checkpoint and final
architectural-delta/wiring verification.
