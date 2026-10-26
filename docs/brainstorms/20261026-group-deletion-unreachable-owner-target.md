# Target architecture: metadata-authorized group deletion with deferred cleanup

Status: design specification for implementation planning; not implemented.
Revision: includes the 2026-10-26 planning correction for deterministic historical command replay.
Date: 2026-10-26.
Source revision reviewed: 8e57798.
Issue: <https://github.com/asalimonov/gquorum/issues/5>.
Discussion: [research and decisions](20261026-group-deletion-unreachable-owner.md).

## 1. Request and agreed contract

Fix whole-group deletion waiting indefinitely for an unreachable former owner's
exact stop/readmission receipt. Reuse existing gquorum machinery, preserve public
Go interfaces and public JSON shapes, and avoid a new deletion subsystem.

The user selected these semantics:

- Every admitted cluster host continues to run a metadata replica.
- Only `_meta` quorum is required to approve deletion of a load group.
- Approval installs the next topology version, permanently retires the group ID,
  and retains exact obligations for eventual shutdown and cleanup.
- Missing hosts do not delay logical group deletion. Returning hosts automatically
  reconcile their retained replicas against authoritative metadata.
- Existing load groups continue serving with their own quorum when metadata is
  unavailable. Do not introduce metadata serving leases or suspend healthy
  load groups because their metadata connectivity fails.
- The application/operator stops using a group before requesting its deletion.
  The library does not infer idleness from health, traffic counters or timeouts.

Successful deletion means the group has been removed from approved cluster
topology and its cleanup obligations have durable ownership. It does not mean
every former process has stopped, every application callback has returned, or
every disk has been erased. A running partition can temporarily continue using
its older view. No stronger real-time service cutoff is promised.

This contract is independent of any correlation between metadata and load
connectivity. It remains meaningful if a load quorum is isolated from the
metadata quorum.

### 1.1 Success conditions

A newly approved deletion block succeeds in the metadata apply that installs:

1. The approved document without the group.
2. Its permanent `retiredGroup` record.
3. Complete exact cleanup coverage for every potentially established former
   owner, including issued but unacknowledged creation.
4. The nil group checkpoint at the approved version.
5. The operation block's successful status.

These changes are one candidate-state installation. Corrupt identities or missing
authorization still refuse approval atomically. An unreachable host is not such
a refusal.

A mixed operation succeeds only when all of its required blocks succeed.
Deletion does not manufacture completion for another group or metadata admission.

### 1.2 Preserved boundaries

- `Latest` is staged intent; `Target` is approved intent. Staging deletion does
  not stop a replica, retire an ID, or authorize cleanup.
- Group IDs cannot be reused, including by rollback.
- Ordinary replica membership changes retain their existing native Raft quorum,
  leadership-transfer, voter-transition and receipt requirements.
- Same-host replacement still requires exact predecessor stop and retirement.
- Readmission still fences/joins its former owner and consumes its whole batch
  before publishing its replacement.
- Missing storage, an excluded node or a newer member ID does not prove stop,
  retirement or collection.
- `StopLoadGroup` remains a local hold, not deletion.
- `_meta` itself cannot be deleted through `DeleteGroup`.
- Physical fencing/revocation observations remain distinct from logical deletion.

## 2. Decisions and alternatives

| Topic | Selected design | Reason |
| --- | --- | --- |
| Deletion authority | Existing approved document and scoped operation | Already replicated, versioned and CAS-protected. |
| Required execution | No required host steps for a new deletion block | Removes the missing-host dependency. |
| Local stop/retirement | Existing steps in `PlanBlock.Cleanup` | Reuses bounded dispatch and receipt protocols. |
| Persistent terminal identity | Existing `RetiredGroups` plus nil group checkpoint | No new tombstone representation. |
| Deferred work | Existing `cleanupRecord` with exact historical member identity | Survives restart, snapshot and pruning. |
| Pending readmission | Preserve ordinary deletion debt; let the existing whole readmission batch settle it | Approval does not wait for a returning process. |
| Grant conflicts | Existing resource floors, revocations and host fences | Logical success must not authorize deleting successor resources. |
| Native-resource settlement | Require all retained host stops before using the existing deleted-resource shortcut | Preserves the stronger physical meaning of revocation completion. |
| Metadata loss during serving | Existing independent load behavior | Explicit user decision. |
| Public surface | No new methods, fields, options, states or errors | Requested API stability. |
| Data formats | Existing snapshot schema 4 and authority journal schema 2; one optional internal approval-command semantics marker | Distinguishes new approval behavior from historical log replay without changing public DTOs. |
| Permanent missing hosts | Retain unconfirmed cleanup indefinitely | No invented destruction proof or host waiver. |
| Execution model | Existing controller workers and Node ownership | No new watcher, worker pool, RPC, listener or callback. |

Rejected for this change: native peer removal as a deletion prerequisite,
load-log terminal commands, metadata serving leases, automatic idleness detection,
force-delete options, and a separate destroyed-host declaration.

The TiKV analogy is transfer of outstanding cleanup after logical group removal.
gquorum keeps that responsibility in `_meta` rather than in a surviving Region.
See the discussion log for pinned upstream evidence and the limits of that analogy.

## 3. Public API and compatibility

### 3.1 Signatures and representations

The following public signatures remain unchanged:

~~~go
// Methods of gquorum.Cluster:
DeleteGroup(context.Context, string, MutationOptions) (MutationResult, error)
Approve(context.Context, uint64) (topology.OperationID, error)
WaitOperation(context.Context, topology.OperationID) (topology.OperationStatus, error)
RetryOperation(context.Context, topology.OperationID) (topology.OperationID, error)
ReadProgress(context.Context, topology.ProgressQuery) (topology.ProgressPage, error)
~~~

Keep `MutationOptions`, `MutationResult`, `Plan`, `PlanBlock`, `OperationStatus`,
`GroupProgress`, `CleanupProgress` and all error codes unchanged. No additions to
`NodeOptions`, `LoadOptions`, `topology.Limits`, storage interfaces or lifecycle
hooks. Root package files require documentation changes only.

The intentional behavior change is that a new deletion operation can succeed
before its local stop actions finish. Existing clients compile without changes.
Clients that need physical completion already have `ReadProgress` cleanup
milestones; document that `WaitOperation` is not a physical-cleanup barrier.

Expected state immediately after approval:

| Value | Expected result |
| --- | --- |
| `Target.Group(id)` | nil |
| Group checkpoint | `Converged == nil`, `ConvergedVersion == deletionVersion` |
| Group owner | Empty after logical completion |
| `GroupProgress.Deleting` | false after logical completion; do not repurpose it to mean cleanup pending |
| Deletion block | `OperationSucceeded`, `TotalSteps == 0`, `CurrentStep == 0` |
| Operation aggregate | Existing aggregate of all block states |
| Cleanup | Exact rows, possibly with every physical milestone still absent |
| `FullyConverged` | May advance according to existing independent-group rules |

Do not add a synthetic native-removal receipt for whole-group deletion.
`CleanupProgress.RemovedAt` can legitimately remain nil.

### 3.2 CAS, replay, cancellation and preview

Reuse the existing mutation pipeline and exact replay digest. If staging succeeds
but approval fails, return the staged document version as today. Preview computes
the new plan but authorizes nothing. Repeating approval returns its retained first
attempt. A canceled waiter does not undo committed deletion or cancel cleanup.

A succeeded deletion operation does not become failed merely because cleanup
later encounters an unavailable host or callback error. Do not use
`RetryOperation` to retry cleanup; the detached worker already owns retries.
Calling `DeleteGroup` again with a different mutation is not a new idempotency
mechanism: preserve existing unknown-group/CAS behavior and exact original replay.

### 3.3 Retained historical plans and rollout

Preserve old immutable plans and frozen fixtures. The new planner emits detached
deletion stops. Marker-1 approval validation requires that shape. Restoration still
accepts valid previously issued required-stop deletion plans and existing
readmission-covered plans.

**No migration of already-approved legacy deletion operations is included.**
Their required steps and original retry semantics remain unchanged. A deletion
approved by the new implementation receives the new completion semantics.
An already-pending legacy deletion may therefore still wait for its old host
proof. Do not silently rewrite its plan, relabel existing execution grants, or
claim that an upgrade converts that operation. This deliberate scope keeps the
change compatible with immutable approvals and avoids a migration API.

Keep snapshot/journal shapes and version constants unchanged. Add only the
optional internal `BeginOperationProposal.DeletionSemantics` field specified in
section 5.8. This command marker is necessary: a retained restart can replay a
previously committed approval after its snapshot, so changing the interpretation
of the old unmarked command would reject or alter historical committed work.
The marker is not a runtime feature flag and is not exposed by public mutation
options, Operation, Plan, status or any new RPC.

Unmarked/zero commands retain the original approval/debt/completion semantics.
New public approvals write marker 1 and use the new behavior. The value is
committed with the approval command, never inferred from current time, process
version, connectivity or plan shape. Shape alone is ambiguous for old zero-step
readmission-covered deletions.

New snapshots contain only the existing completed block/checkpoint and cleanup
representation. They need no persistent operation mode: every marker-1 deletion
block completes during its approval, and retry of a mixed operation only reclaims
failed sibling blocks. Unknown marker values refuse without applying state.

Mixed-binary operation and downgrade after new approvals remain unsupported.
Older decoders may reject the new optional internal field. This does not change
public source compatibility or authorize modifying frozen fixtures.

Frozen legacy-rejection, scoped-v4, scoped-v2 and native restart artifacts remain
immutable. New regression fixtures are separate generated test state, not
replacement baseline files.

## 4. Component architecture and ownership

~~~mermaid
flowchart TD
    Caller["Cluster.DeleteGroup / Approve"] --> Stage["Existing CAS document mutation"]
    Stage --> Meta["Existing _meta approval Apply"]
    Meta --> Tombstone["Target omission + permanent group ID + nil checkpoint"]
    Meta --> Debt["Exact retained cleanup rows"]
    Tombstone --> Success["Deletion block succeeds"]
    Debt --> Queue["Existing ReadyCleanup / ReadyFences"]
    Queue --> Host["Existing granted Stop and retirement on exact host"]
    Host --> Receipts["Actual stop / retirement / collection receipts"]
    Receipts --> Debt
    Debt --> Retention["Existing evidence retention and snapshot"]
    Retention --> Return["Returning host obtains current authority"]
    Return --> Host
~~~

**Topology planner.** Continue emitting exact stop and deletion-state steps in
the flat immutable plan. Classify deletion stops as cleanup and avoid reissuing
actions already owned by detached debt. Keep the flat stop-before-retire order.

**Metadata FSM.** Collect possible owners from approved/proven declarations,
issued grants and retained debt. Construct exact cleanup coverage, then complete
the nil checkpoint without waiting for host receipts. Keep all fallible work on
an unpublished candidate and perform full graph validation before installation.

**Authority catalog.** Continue to close conflicting grants and create host
fences. Logical deletion does not prove those fences have reached a resource.
Keep native obligations until actual host stops make the old native group
quiescent; host floors always retain their existing evidence requirements.

**Cleanup controller.** Existing per-identity dispatch, shared cleanup/fence
capacity, rotating selection, timeouts and backoff remain the execution engine.
A terminal deletion operation does not remove its cleanup work.

**Managed host.** Existing exact granted stop joins engine/replay and callback
ownership. Existing retirement verifies identity and proof, and existing
collection records preparation and completion. No callback runs inside metadata
Apply and no new application callback is introduced.

**Readmission and startup.** Existing startup authorization rejects retired group
IDs before handle publication. Pending readmission retains its host-resource
priority; its complete batch may settle deletion debt. A changed member ID alone
cannot do so.

## 5. Complete persistence contract for affected state

### 5.1 Storage technology and physical key layout

The implementation uses bbolt and JSON/protobuf records. There is no SQL database,
DDL migration, SQL index or ORM model to introduce. The complete affected record
schemas are below. Snapshot/journal tags below are existing serialization tags; section 5.8 specifies
the single new internal command tag.

The metadata database retains exactly these buckets:

| Bucket | Keys/value representation | Change |
| --- | --- | --- |
| `format` | `schema_version` -> ASCII `4` | None |
| `hardstate` | `current` -> Raft HardState protobuf | None |
| `entries` | Existing encoded Raft indexes -> entries | None |
| `snapshot` | `current` -> existing Raft snapshot representation | None |
| `applied` | `index` -> existing uint64 encoding; `confstate` -> protobuf | None |
| `state` | `current` -> canonical `scopedState` JSON | New legal deletion state, same fields |
| `identity` | `node_name`, `cluster_id`, `member_id`, `member_seen` | None |
| `serve_override` | `current` -> existing override representation | None |
| `load_replicas` | Group ID -> exact `ReplicaRecord` JSON | Existing receipt-authorized removal only |

`Store.SaveApplication(index, confState, data)` already writes state, applied
index and applied membership in one bbolt transaction, rejecting application
beyond durable Raft commit. Reuse it unchanged.

The independent authority database remains schema 2 with buckets `format`,
`identity`, `fences`, `effects` and `collection`. Its record keys and serialized
values remain unchanged. WAL directories, snapshot envelopes, identity pins,
retirement tokens and collection markers remain unchanged.

### 5.2 Replicated state envelope

The full existing top-level envelope remains:

~~~go
type scopedState struct {
	// Derived only; never trusted from a snapshot or independently persisted.
	indexes *scopedIndexes
	// Ephemeral engine-supplied context for one committed application entry.
	// Convergence persists the resulting proof; snapshots cannot provide this.
	nativeConf       *raftpb.ConfState
	importPlacement  *recoveryplacement.Context  // owned atomic-import analysis only; never serialized/published
	SchemaVersion    uint32                      `json:"schema_version"`
	Rules            topology.Rules              `json:"rules"`
	Latest           topology.Document           `json:"latest"`
	Target           topology.Document           `json:"target"`
	History          []topology.Document         `json:"history"`
	Operations       []topology.Operation        `json:"operations"`
	Groups           []groupConvergence          `json:"group_convergence"`
	Nodes            []nodeConvergence           `json:"node_convergence"`
	Cleanup          []cleanupRecord             `json:"cleanup"`
	Intents          []Intent                    `json:"intents"`
	FullyConverged   topology.FullyConverged     `json:"fully_converged"`
	AuthorityRecords []scopedAuthorityRecord     `json:"authority_records,omitempty"`
	AuthorityFloors  []resourceFloor             `json:"authority_floors,omitempty"`
	Revocations      []scopedRevocationRecord    `json:"revocations,omitempty"`
	RetiredGroups    []retiredGroup              `json:"retired_groups"`
	Allocations      []allocationRecord          `json:"allocations"`
	Admissions       []AdmissionRecord           `json:"admissions,omitempty"`
	Sessions         []sessionState              `json:"sessions"`
	Locks            []lockEntry                 `json:"locks"`
	Activated        []string                    `json:"activated,omitempty"`
	VoterSwitches    []topology.VoterSwitchState `json:"voter_switches,omitempty"`
	AppliedIndex     uint64                      `json:"applied_index"`
	AppliedTerm      uint64                      `json:"applied_term"`
	Recovery         *model.ImportState          `json:"recovery,omitempty"`
}
~~~

No top-level snapshot collection is added or removed. Besides the deletion-related
contents of `Operations`, `Groups`, `Cleanup` and `RetiredGroups`, normal approval
can update `History`, `AuthorityRecords`, `AuthorityFloors`, `Revocations`,
`FullyConverged` and the applied position through the existing code.

All other collections retain their existing schema and behavior: `Nodes`,
`Intents`, `Allocations`, `Admissions`, `Sessions`, `Locks`, `Activated`,
`VoterSwitches` and optional `Recovery`. Ordinary combined operations may still
update their own affected collections.

### 5.3 Permanent retirement and group progress

~~~go
type retiredGroup struct {
	ID      string `json:"id"`
	Version uint64 `json:"version"`
}

type groupConvergence struct {
	GroupID       string               `json:"group_id"`
	Declaration   *topology.Group      `json:"declaration,omitempty"`
	SourceVersion uint64               `json:"source_version"`
	Owner         topology.OperationID `json:"owner,omitempty"`
	Deleting      bool                 `json:"deleting"`
	CompletedAt   time.Time            `json:"completed_at"`
}
~~~

Constraints:

- Retired IDs are unique, permanent, non-meta group IDs.
- Retirement version is nonzero and no newer than approved `Target`.
- The group must be absent from approved desired topology.
- Logical completion stores nil `Declaration`, nonzero `SourceVersion`,
  empty `Owner`, false `Deleting` and nonzero `CompletedAt`.
- `CompletedAt` is the metadata logical-completion timestamp, not a host stop time.
- A completed tombstone with unfinished exact cleanup is now valid.
- A completed tombstone that loses known former-owner coverage is still invalid.
- Historical `Deleting=true` rows with required-stop plans remain restorable.

### 5.4 Immutable operation and partition schemas

~~~go
type Plan struct {
	Version      uint64           `json:"version"`
	Steps        []Step           `json:"steps"`
	VoterBases   []PlanVoterBase  `json:"voter_bases,omitempty"`
	Blocks       []PlanBlock      `json:"blocks"`
	Dependencies []StepDependency `json:"dependencies,omitempty"`
}

type PlanBlock struct {
	GroupID       string `json:"group_id"`
	SourceVersion uint64 `json:"source_version"`
	Steps         []int  `json:"steps"`
	Cleanup       []int  `json:"cleanup,omitempty"`
}

type Operation struct {
	Status    OperationStatus `json:"status"`
	Plan      Plan            `json:"plan"`
	Previous  OperationID     `json:"previous,omitempty"`
	Actor     string          `json:"actor"`
	Origin    Origin          `json:"origin"`
	StartedAt time.Time       `json:"started_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

type OperationStatus struct {
	ID              OperationID    `json:"id"`
	Version         uint64         `json:"version"`
	Attempt         uint32         `json:"attempt"`
	State           OperationState `json:"state"`
	Blocks          []BlockStatus  `json:"blocks"`
	TotalBlocks     int            `json:"total_blocks"`
	BlocksTruncated bool           `json:"blocks_truncated"`
	ErrorCode       string         `json:"error_code,omitempty"`
	Error           string         `json:"error,omitempty"`
}

type BlockStatus struct {
	GroupID     string         `json:"group_id"`
	State       OperationState `json:"state"`
	CurrentStep int            `json:"current_step"`
	TotalSteps  int            `json:"total_steps"`
	Blocked     *BlockedReason `json:"blocked,omitempty"`
	ErrorCode   string         `json:"error_code,omitempty"`
	Error       string         `json:"error,omitempty"`
}
~~~

New deletion example, with no inherited obligations:

~~~go
Plan{
    Version: 12,
    Steps: []Step{
        {Seq: 1, GroupID: "orders", Kind: StepStopReplica, NodeName: "a", ReplicaID: 1},
        {Seq: 2, GroupID: "orders", Kind: StepStopReplica, NodeName: "b", ReplicaID: 2},
        {Seq: 3, GroupID: "orders", Kind: StepStopReplica, NodeName: "c", ReplicaID: 3},
        {Seq: 4, GroupID: "orders", Kind: StepDeleteGroupState},
    },
    Blocks: []PlanBlock{{
        GroupID: "orders", SourceVersion: 7,
        Steps: nil, Cleanup: []int{1, 2, 3, 4},
    }},
}
~~~

Every flat step still belongs to exactly one partition. No new step kinds.
Empty required steps do not mean empty cleanup coverage. A deletion with only
inherited detached debt can have no newly emitted steps at all.

### 5.5 Exact cleanup schema

~~~go
type cleanupRecord struct {
	Identity       storage.Identity                  `json:"identity"`
	Storage        topology.StorageClass             `json:"storage"`
	MemberID       uint64                            `json:"member_id"`
	OperationID    topology.OperationID              `json:"operation_id"`
	StopAction     *topology.AuthorizedStep          `json:"stop_action,omitempty"`
	RetireAction   *topology.AuthorizedStep          `json:"retire_action,omitempty"`
	ReadmissionKey *topology.AuthorityKey            `json:"readmission_key,omitempty"`
	Removed        *topology.EffectReceipt           `json:"removed,omitempty"`
	Stopped        *topology.EffectReceipt           `json:"stopped,omitempty"`
	Retired        *topology.EffectReceipt           `json:"retired,omitempty"`
	Collection     *authoritystore.CollectionReceipt `json:"collection,omitempty"`
	RemovedAt      *time.Time                        `json:"removed_at,omitempty"`
	StoppedAt      *time.Time                        `json:"stopped_at,omitempty"`
	RetiredAt      *time.Time                        `json:"retired_at,omitempty"`
	NotBefore      *time.Time                        `json:"not_before,omitempty"`
	Blocked        *topology.BlockedReason           `json:"blocked,omitempty"`
	CreatedAt      time.Time                         `json:"created_at"`
	UpdatedAt      time.Time                         `json:"updated_at"`
}
~~~

Unique logical key is the complete `storage.Identity`:
`(ClusterID, GroupID, NodeName, ReplicaID)`. Historical `MemberID` is separately
validated; it is never replaced by the current member using the same name.

Constraints preserved:

- Origin operation, storage class and original retirement action are immutable.
- Stop action can be rebound only by the existing approved handover/retry rules.
- Stop/retire actions must match retained immutable operation tuples exactly.
- Receipt and timestamp are both absent or both present.
- Retirement requires actual stop or valid complete readmission proof.
- Collection requires actual retirement and an exact collection receipt.
- Whole-group debt can have no native removal receipt.
- An actionless debt is valid only with already-consumed readmission proof.
- Pending readmission does not create a fake `ReadmissionKey` or timestamp:
  pending deletion uses an ordinary action-bearing cleanup row.
- Memory retirement finishes its obligation according to current memory-storage
  rules, without manufacturing a disk-collection receipt.
- Completed/collected rows are not recreated if their evidence was legitimately
  pruned together with all references.

### 5.6 Local records and public cleanup projection

~~~go
type ReplicaRecord struct {
	NodeName  string    `json:"node_name"`
	ClusterID string    `json:"cluster_id"`
	ReplicaID uint64    `json:"replica_id"`
	Origin    string    `json:"origin"`
	CreatedAt time.Time `json:"created_at"`
}

type CollectionReceipt struct {
	Identity      storage.Identity      `json:"identity"`
	MemberID      uint64                `json:"member_id"`
	RetirementKey topology.AuthorityKey `json:"retirement_key"`
	Token         string                `json:"token"`
	LocalSequence uint64                `json:"local_sequence"`
	CollectedAt   time.Time             `json:"collected_at"`
}

type CleanupProgress struct {
	Identity    storage.Identity `json:"identity"`
	Storage     StorageClass     `json:"storage"`
	MemberID    uint64           `json:"member_id"`
	OperationID OperationID      `json:"operation_id"`
	RemovedAt   *time.Time       `json:"removed_at,omitempty"`
	StoppedAt   *time.Time       `json:"stopped_at,omitempty"`
	RetiredAt   *time.Time       `json:"retired_at,omitempty"`
	CollectedAt *time.Time       `json:"collected_at,omitempty"`
	NotBefore   *time.Time       `json:"not_before,omitempty"`
	Blocked     *BlockedReason   `json:"blocked,omitempty"`
	UpdatedAt   time.Time        `json:"updated_at"`
}
~~~

These types remain unchanged. A public cleanup row can show a successfully
deleted group with nil `StoppedAt`, `RetiredAt` and `CollectedAt`. That is an
honest pending physical obligation, not an operation failure.

### 5.7 Indexes, retention and caches

Reuse `scopedIndexes`:

| Key | Value | Lifetime |
| --- | --- | --- |
| Group ID | `*groupConvergence` | Rebuilt from each candidate/restore |
| Operation ID | `*Operation` | Rebuilt; retained operations only |
| Exact storage identity | `*cleanupRecord` | Rebuilt; one row per identity |
| Authority key | `*scopedAuthorityRecord` | Rebuilt from durable authority |
| Authority resource | Pending authority keys | Rebuilt from actual settlement |
| Sorted cleanup identities | Cleanup row pointers | Existing bounded cursor selection |

No external cache, TTL or durable cache key is introduced. Controller cursors and
backoff are volatile scheduling hints; restarting them cannot lose work.
Existing observation caches remain observations, never deletion authority.

Incomplete cleanup already roots its origin, action-binding operations,
declaration, grants and receipts through `scopedRetention`. Reuse those edges
after the group row ceases to be `Deleting`. Do not retain every historical
document forever merely because the group is logically deleted. Keep permanent
retired IDs and allocation floors as today.

### 5.8 Internal approval command semantics (planning correction)

The sole persisted wire-shape addition is to the internal command body.
This type is in `internal/metaengine`, not the public gquorum API.

~~~go
type DeletionSemantics uint8

const (
    DeletionSemanticsLegacy DeletionSemantics = iota // 0; field omitted by old writers
    DeletionSemanticsDetached                       // 1; new approval interpretation
)

type BeginOperationProposal struct {
    Version           uint64             `json:"version"`
    Plan              topology.Plan      `json:"plan"`
    Scopes            []ScopeExpectation `json:"scopes"`
    Actor             string             `json:"actor"`
    Origin            topology.Origin    `json:"origin"`
    At                time.Time          `json:"at"`
    ProposalID        string             `json:"proposal_id"`
    DeletionSemantics DeletionSemantics  `json:"deletion_semantics,omitempty"`
}
~~~

The command kind remains `begin_operation` and the envelope schema remains 4.
Omitting the zero field preserves existing old-command bytes and canonical
fixture encodings. The custom UnmarshalJSON accepts an absent field as zero and
rejects values greater than 1. No new public option or command endpoint selects
this value; `metaservice.operationProposal` writes 1 for new approvals.

`prepareScopedApproval` reads the marker and passes it through the affected
approval-only helpers. Add the ephemeral `semantics DeletionSemantics` field to
`deletionPreparation` and `cleanupHandover`; these helpers are never serialized.
Use keyed literals when updating their construction sites, including tests and
legacy completion callers (which use the zero value).

~~~go
type deletionPreparation struct {
    source, next *scopedState
    collector planningCollector
    semantics DeletionSemantics
}

type cleanupHandover struct {
    source    *scopedState
    desired   topology.Document
    collector planningCollector
    at        time.Time
    required  map[storage.Identity]topology.AuthorizedStep
    detached  map[storage.Identity]topology.AuthorizedStep
    semantics DeletionSemantics
}
~~~

Retain the complete original decision branches for mode 0 in retirement
validation, debt preparation, cleanup handover and deletion completion. Do not
limit compatibility to snapshot decoding. In particular, a historical committed
zero-step readmission deletion must still wait for the whole batch, and a
historical committed required-stop deletion must not be rejected by the new
shape rule. Helpers used after approval continue deriving their obligations
from the retained immutable plan and actual proof, as today.

The existing automatic repair/recovery command paths that internally construct
a BeginOperationProposal retain zero semantics unless their own committed
envelope explicitly carries a marker in a future change. They do not implement
user whole-group deletion; do not infer marker 1 merely from the current binary.

Required regression: replay pre-change serialized `begin_operation` commands
over a pre-deletion snapshot through retained commit, including required-stop,
inherited-debt and zero-step/readmission cases. Compare resulting state and
subsequent legacy completion/retry behavior with pre-change expectations.
Test marker-1 replay and unknown-marker refusal separately.

## 6. Planner changes

### 6.1 Required versus detached steps

Change `requiredRetainedStop` to require a stop only for an existing desired group
that reuses the host with another replica ID:

~~~go
return desired != nil && slices.ContainsFunc(desired.Replicas, func(r Replica) bool {
    return r.NodeName == key.NodeName && r.ReplicaID != key.ReplicaID
})
~~~

Keep `detachedPlanStep` and `bindPlanBlocks` as the shared classification path.
Their existing calls then place deletion stops in `Cleanup`. Remove the explicit
whole-group-deletion rejection in `validateBlockSafety`. Keep its same-host
replacement prohibition and all founding/admission barrier checks.

### 6.2 Avoid duplicate retirement ownership

`planDeletedGroup` must emit:

| Retained progress | New plan action |
| --- | --- |
| `RetainedHosted` | Exact stop, plus one shared `StepDeleteGroupState` for newly owned retirement |
| `RetainedCleanup` | No duplicate stop/retirement; existing debt keeps ownership |
| `RetainedStopped` | No new host action; retain its exact proven debt/evidence |

`bindDetachedStops` remains responsible for rebinding a formerly required
same-host stop that becomes detached because the desired group disappears.
Keep the existing deletion branch of `validateStopOrder`: it already accepts
exact stops without adjacent native removal when the desired group is absent.
No change to `planShapeValidator.detachingStop` is needed. Preserve sequence
identities and the flat stop-before-delete ordering.

Shape/native-simulation validation accepts retained cleanup as coverage for
already-owned retirement. It must still reject an omitted stop for a possibly
hosted owner that has no debt or complete readmission proof. A public
`RetainedProgress` value is input to planning, not sufficient proof for the FSM.

### 6.3 Readmission

For a group being deleted, `bindReadmissionRetirement` must not erase newly needed
host stop/delete steps merely because `ReadmissionCovers` says a replacement
admission is planned. Planning coverage is not completion proof.

Modify the existing builder in two places:

1. `appendStep` preserves load stop steps when the desired group is absent.
   Existing omission for readmission of groups that remain present is unchanged.
2. `requiresDeletion` keeps `StepDeleteGroupState` when any potentially hosted
   deletion owner still needs an ordinary retirement obligation, including owners
   whose node is being readmitted.

If complete readmission proof already marks an owner stopped, no duplicate work
is emitted. No load-native read or load quorum is introduced for deletion.

## 7. Metadata approval and complete coverage

### 7.1 Preparation ordering

Preserve the existing `applyBeginOperation` pipeline. First select the deterministic
approval interpretation from the committed section 5.8 marker. The sequence
below is the mode-1 path; mode 0 executes the original decisions.

1. Exact approval replay handling.
2. Scope/CAS, plan, recovery and retirement validation.
3. Clone candidate ownership and supersede conflicting operation blocks.
4. Existing admission and cleanup handovers.
5. Prepare deletion debts and permanent retired IDs.
6. Validate complete detached coverage and complete new deletion blocks.
7. Existing history, node progress and `FullyConverged` processing.
8. Existing authority closure/fence preparation.
9. Rebuild and validate the complete graph.
10. Install the candidate atomically.

Do not complete a group based solely on the approved omission or the prospective
nil checkpoint. Coverage must be derived from the source's approved evidence and
the exact candidate debt rows.

### 7.2 Debt preparation

For every possible former owner collected from proven declarations, issued
creation/preparation grants, native-removal evidence and existing cleanup:

- Existing debt: preserve its member ID, class, origin, original retirement
  action and all receipts. Apply only the existing approved stop handover if
  required. Do not wait for its stop/readmission receipt.
- Newly hosted identity: create ordinary deletion debt bound to the new stop
  and deletion-state actions from `block.Cleanup`.
- Complete readmission with no materialized debt: reuse `readmissionDebt` to
  materialize its already-proven actionless row.
- Never-issued/non-hosted declaration: no physical debt is necessary, but retire
  the group ID permanently.
- Contradictory identity/storage/action: fail before publishing any candidate
  debt, checkpoint or retired-ID changes.

The current helper appends only rows with `StopAction != nil`. Update that
sentinel to append any nonzero returned identity so materialized actionless
completed-readmission rows are retained.

`deletionDebt` may retain existing ordinary debt even when its host is offline,
its replacement admission is pending, or its old readmission grant was superseded.
The valid ordinary action-bearing debt remains the obligation. This does not
authorize using an invalid or closed readmission grant.

### 7.3 New private coverage helper

Add one internal helper in
`internal/metaengine/scoped_deletion_detached.go`:

~~~go
func (p deletionPreparation) validateDetachedCoverage(blocks []topology.PlanBlock) error
~~~

It validates the following without network I/O, callback execution or candidate
mutation:

- Each selected deletion block names a load group absent from the new approved document.
- New deletion blocks have no required steps.
- Every possible source owner has exact candidate debt or materialized complete
  readmission proof.
- A pending ordinary debt has both actions, a valid immutable origin and a stop
  binding classified as cleanup.
- The source identity/member/storage match the candidate row.
- The permanent retired-ID record is present.
- A row with nil stop proof is not treated as physically stopped.

Accept all operation blocks in one call, skip surviving groups, and build
maps of deletion scopes, debts and operations once. Traverse sorted source owners
once, selecting only those scopes. Do not rescan all debts per owner or all
owners per deleted group. Full graph validation subsequently verifies all nested
receipt/floor provenance.

### 7.4 Logical completion

`prepareDeletionCompletions` uses that helper for newly approved deletion blocks,
then reuses `completeApprovalGroup` to publish the nil checkpoint and marks only
those blocks successful. It does not call the old receipt-waiting completion
algorithm for the new shape.

Retain the older `deletionPreparation.complete` / readmission-dependent path where
needed to execute already-approved legacy operations. Do not change the meaning
of their immutable required partition or fabricate consumed step receipts.

The old `protectReadmission` guard must not block a newly approved deletion whose
every owner now has independent action-bearing cleanup coverage. Keep protection
for legacy omissions that still depend on a particular pending batch. A missing
receipt is no longer a new deletion-approval dependency; malformed retained proof
remains an error.

### 7.5 Restore validation

Update `validateScopedDeletion` so a logically completed tombstone may have
unstopped debt when that exact debt is bound to detached cleanup. Continue to
reject:

- Missing debt for a known potentially hosted owner.
- Wrong member/replica, foreign storage class or nonexistent action origin.
- Required-stop debt falsely presented as a new detached-completion proof.
- Bare readmission intent presented as a receipt.
- Partial/closed/readmission grants used to invent stopped or retired milestones.

Restore reuses validateDetachedCoverage for completed tombstones and retains
the original pending-readmission checks for legacy Deleting rows. Recover former
proven owners from retained plan SourceVersion declarations as well as current
checkpoints: replacing a declaration with nil must not let a missing cleanup row
erase its own proof obligation.

Preserve valid legacy pending rows and completed physical-deletion rows.
Restoration never completes an operation or rewrites its immutable plan as a side
effect of decoding.

## 8. Cleanup, readmission and physical authority

### 8.1 Reuse cleanup dispatch

`ReadyCleanup` already picks stop before retirement. `BeginCleanupEffect`
already issues an exact one-host grant, and the controller dispatches each host
independently. Reuse the complete path:

`ReadyCleanup -> BeginCleanupEffect -> resource fence -> DispatchGranted -> RecordEffects -> Stop/Retire milestone`.

The metadata operation being terminal is not a reason to reject its retained
cleanup action. Existing tuple validation and history pinning remain mandatory.

`scopedCleanupValidation.stopAction` must accept a stop in the bound operation's
cleanup partition when its retirement action is `StepDeleteGroupState`. Validate
the group is permanently retired and absent from approved topology. Do not allow
arbitrary cleanup classification to bypass same-host replacement barriers.

### 8.2 Readmission priority without new persistent state

A newly recorded deletion debt can refer to a historical member while a newer
member with the same node name is approved. Preserve its old member identity;
do not redirect its ordinary Stop RPC to the replacement process.

Add a small private predicate, colocated with cleanup authorization:

~~~go
func (s *scopedState) cleanupWaitsForReadmission(debt *cleanupRecord) bool
~~~

For a tombstoned load group, return true when `Target.MemberID(node)` is greater
than the debt's historical member ID and physical retirement is still pending.
`cleanupDebtForAction` returns `ErrAuthorityPending` for such ordinary dispatch.
The existing ready selector skips it and continues other identities. Do not
gate approval, logical completion or unrelated groups.

The existing readmission protocol obtains the historical identities from retained
documents, grants and cleanup rows. Its whole-batch completion updates existing
debt through `settleReadmissionCleanup` / `retireReadmissionTarget`. Partial
receipts remain authority evidence only.

Keep active, still-bound readmission grants under the current
`readmissionStillBound` rule. Do not add a new persisted batch reference, promise
pending readmission has stopped anything, or force replacement admission to wait
for an old-process cleanup RPC that can no longer be routed.

If replacement intent is canceled, existing revocation/fencing governs the
in-flight batch. The ordinary exact debt survives. Absence of any returning
authorized owner may leave physical cleanup unconfirmed indefinitely.

### 8.3 Preserve physical fencing semantics

Do not let the earlier nil checkpoint make the existing `deletedResource`
shortcut assert that an unfinished native resource is physically quiescent.

Keep the private function name `completedDeletions` for a small diff, but tighten
its implementation and comment: it returns groups whose logical tombstone exists
**and whose every retained cleanup owner has exact stop proof**.

Compute candidates from the existing permanent retired IDs and group rows, then
remove any candidate with an unstopped cleanup row. This pass must use the owned
state slices, not call `authorityCatalog` or depend on partially built indexes:
it is invoked while rebuilding indexes. Full deletion-coverage validation is a
prerequisite to installing a candidate, so no missing row can make the result
silently authoritative.

Consequences:

- `WaitOperation` can succeed while `WaitRevoked` still waits for physical proof.
- Native fence work may continue failing for an unavailable deleted group; it is
  detached and cannot hold logical deletion.
- Once all host stops are proven, native obligations become vacuous by the
  existing permanent-ID reasoning, without a fabricated fence receipt.
- Host resources are never settled merely because their group ID is retired.
- Retirement and collection retain their own later milestones.

Reuse existing rotating/backoff dispatch so failing native fences cannot starve
host stops. A regression must verify progress under occupied cleanup capacity.

### 8.4 Startup, runtime and storage

No new Node watcher is required. Existing metadata approval makes the permanent
tombstone visible; existing cleanup dispatch stops live replicas asynchronously.
`queryScopedLoadScope` already returns `ErrGroupRetired` for a retired ID.
Retained startup obtains current authority floors and rechecks scope before
publication.

Receiving the document is not a synchronous application-shutdown callback.
A directly held replica handle may remain alive until its granted stop runs.
This is consistent with the chosen eventual-stop contract and avoids adding a
metadata check to every load proposal/read.

Keep all existing stop/factory/Close ownership rules. Cleanup waits locally for
owned work; logical deletion does not. Stop never itself deletes durable storage.
Retirement and collection verify exact authority even when data is already absent.
Foreign/successor data remains intact.

## 9. File and method inventory

The following is the complete intended production change surface. Public
signatures and constructor/DI signatures are preserved. Approval-only internal
helpers receive the explicit section 5.8 semantics parameter. New exported names
inside internal/metaengine are internal-package contracts, not public library API.

### 9.1 UPDATED: topology/planner.go

~~~go
func (b *topologyPlanBuilder) planDeletedGroup(groupID string)
func (v *plannedStepValidator) applyDeleteGroupState(step Step, state *validationGroup) error
~~~

Emit only new obligations; accept already-retained cleanup in deletion simulation.
Keep native-change simulations for surviving groups unchanged.

### 9.2 UPDATED: topology/plan_retirement.go

~~~go
func requiredRetainedStop(desired *Group, key ReplicaKey) bool
~~~

Narrow required stops to same-host replacement. Reuse
`validateDeletionRetirement` unchanged for plan-shape compatibility; the new FSM
coverage check independently validates pending readmission cases. Public
RetainedProgress enums remain unchanged.

### 9.3 UPDATED: topology/plan.go

~~~go
func (v *planShapeValidator) validateDeletionOrder(current Step) error
~~~

Recognize existing debt as coverage without weakening validation of newly
hosted owners. Existing stop-order validation already permits exact deletion
stops; preserve it.

### 9.4 UPDATED: topology/plan_block_safety.go

~~~go
func validateBlockSafety(desired Document, plan Plan) error
~~~

Remove the all-deletion-stops-required restriction. Preserve replacement and
founding safety. `topology/plan_partition.go` retains its shared implementation;
update comments/tests if needed, without an alternative partition builder.

### 9.5 UPDATED: topology/readmission_plan.go

~~~go
func (p *readmissionPlanBuilder) appendStep(step Step)
func (p *readmissionPlanBuilder) requiresDeletion(step Step) bool
~~~

Keep deletion obligations for pending readmission hosts. Ordinary replacement
plans keep their existing omission and preparation rules.

### 9.6 UPDATED: internal/metaengine/scoped_approval_retirement.go

~~~go
func validateNewRetirement(c planningCollector, input topology.PlanInput,
    desired topology.Document, plan topology.Plan, semantics DeletionSemantics) error
func duplicateRetirement(owner planningReplica, desired *topology.Group,
    step topology.Step, semantics DeletionSemantics) bool
~~~

Require the new detached deletion shape for marker-1 approval. Count hosted deletion
owners even when readmission is planned. Reject duplicate issuance for existing
detached debt; retain the exact required-to-detached stop-handover exception.

### 9.7 UPDATED: internal/metaengine/scoped_deletion.go

~~~go
func prepareDeletionDebts(source, next *scopedState, collector planningCollector,
    semantics DeletionSemantics) error
func (p deletionPreparation) ordinaryDebt(id storage.Identity,
    block topology.PlanBlock) (cleanupRecord, error)
func validateScopedDeletion(state *scopedState, index *scopedIndexes) error
~~~

Read new stop actions from cleanup partitions, preserve existing records,
materialize consumed readmission when necessary and validate pending debt under
completed tombstones. Keep `formerReplicaStorage` and `deletionScopes` as the
existing sources of storage provenance and permanent IDs.

### 9.8 UPDATED: internal/metaengine/scoped_deletion_proof.go

~~~go
func (p deletionPreparation) deletionDebt(id storage.Identity,
    block topology.PlanBlock, existing *cleanupRecord) (cleanupRecord, error)
~~~

Return existing debt without a host-reachability/readmission-consumption
prerequisite. Zero returned identity still means no additional row.
Keep `requiredPlanStop` and `deletionStop` for legacy plan validation; do not
pretend they recognize a physically consumed detached stop.

### 9.9 CREATED: internal/metaengine/scoped_deletion_detached.go

~~~go
func (p deletionPreparation) validateDetachedCoverage(blocks []topology.PlanBlock) error
~~~

A focused pure validation helper. Reuse `deletionPreparation`; do not introduce
a second persisted deletion state machine or a public proof type.

### 9.10 UPDATED: internal/metaengine/scoped_deletion_completion.go

~~~go
func prepareDeletionCompletions(source, next *scopedState, collector planningCollector,
    semantics DeletionSemantics) error
func (p deletionPreparation) protectReadmission() error
func (p deletionPreparation) dependsOnReadmission(key planningReplicaKey,
    target topology.AuthorityTarget) bool
~~~

New deletion approval completes from coverage. Protect only true legacy omitted
retirement dependencies. Keep existing `readmissionDebt` and `complete` for
materializing actual evidence / legacy execution.

### 9.11 UPDATED: internal/metaengine/scoped_cleanup_handover.go

~~~go
func prepareCleanupHandovers(source, next *scopedState, collector planningCollector,
    semantics DeletionSemantics) error
func (h cleanupHandover) prepare(debt cleanupRecord) (cleanupRecord, error)
func detachedCleanupAllowed(desired topology.Document, id storage.Identity) bool
~~~

Allow detachment when the desired group is absent. Rebinding an exact old-member
stop into deletion cleanup may coexist with a newer approved member; it does not
authorize dispatch to that newer member. Preserve timestamps, original retirement
action, and every consumed milestone.

### 9.12 UPDATED: internal/metaengine/scoped_cleanup_progress.go

~~~go
func (check scopedCleanupValidation) stopAction(debt cleanupRecord) bool
~~~

Validate detached deletion stops under their exact retained cleanup action and
permanent tombstone. Keep `pendingStop` for old required-stop plans.

### 9.13 UPDATED: internal/metaengine/scoped_cleanup_authorization.go

~~~go
func (s *scopedState) cleanupDebtForAction(id storage.Identity,
    action topology.AuthorizedStep) (*cleanupRecord, error)

// NEW private helper:
func (s *scopedState) cleanupWaitsForReadmission(debt *cleanupRecord) bool
~~~

Preserve all existing tuple/class/assignment checks, adding the historical-member
readmission dispatch guard. `authorityClass` remains partition-derived.

### 9.14 UPDATED: internal/metaengine/scoped_deleted_authority.go

~~~go
func (s *scopedState) completedDeletions() map[string]bool
~~~

Require physical host-stop coverage before native settlement, even though logical
convergence is already complete. Preserve `deletedResource` as group-resource-only.

### 9.15 UPDATED: internal/operator/remaining_plan.go

~~~go
func validateRetainedReplica(view RemainingPlanView, ex Record,
    group string, replica topology.RetainedReplica) error
~~~

Recognize deletion cleanup already owned by retained debt when validating the new
plan. Keep exact required-stop checks for surviving same-host replacement and
legacy required partitions. `remaining_scope.go` needs its deletion comment
updated; no new native read is required.

### 9.15a UPDATED: internal/metaengine/operation_proposals.go

~~~go
func (p *BeginOperationProposal) UnmarshalJSON(data []byte) error
~~~

Add the optional section 5.8 command field; declare its enum in the small
deletion_semantics.go companion to respect the per-file public-declaration limit.
Validate known values;
retain absent-field behavior, required fields and exact plan-scope validation.

### 9.15b UPDATED: internal/metaservice/operation_approval.go

~~~go
func (g *Group) operationProposal(read metaengine.PlanRead,
    request applyVersionRequest) (any, error)
~~~

New approve proposals select `DeletionSemanticsDetached`. Retry proposals keep
their existing shape and reconstruct only the exact failed attempt.

### 9.15c UPDATED: internal/metaengine/scoped_approval.go

~~~go
func prepareScopedApproval(s *scopedState, desired topology.Document,
    p BeginOperationProposal, kind approvalKind) (*scopedState, error)
~~~

Validate and propagate the committed semantics marker to the approval-only
helpers. Mode 0 preserves the current branches; mode 1 selects sections 6–8.
`applyBeginOperation`, proposal kind, decoder dispatch and final atomic state
installation are reused. No replay-specific live-process flag is introduced.

### 9.15d CREATED: internal/metaengine/deletion_semantics.go

Declares the exact section 5.8 DeletionSemantics enum and constants. This is an
internal companion file, not a public API addition.

### 9.16 Reused without production behavior changes

| Files/components | Existing functions or responsibilities |
| --- | --- |
| `cluster_mutations.go`, `internal/metaservice/mutations.go` | `DeleteGroup` / `DeleteLoadGroup` CAS staging |
| `internal/metaservice/operation_planning.go` | `capturePlan` already skips native reads for deleted scopes |
| `internal/metaengine/scoped_begin.go` | `applyBeginOperation` atomic preparation/revocation/installation |
| `internal/metaengine/scoped_approval_completion.go` | `completeApprovalGroup` and aggregate/marker processing |
| `internal/metaengine/ready_cleanup.go` | `ReadyCleanup`, `queryReadyCleanup`, `pendingCleanupAction` |
| `internal/metaengine/scoped_cleanup_issue.go` | `applyBeginCleanupEffect` exact grants and resource checks |
| `internal/metaengine/scoped_cleanup_effect.go` | Actual stop/retirement receipt recording |
| `internal/metaengine/scoped_cleanup_settle.go` | Complete readmission batch settles existing debt |
| `internal/metaengine/scoped_retention*.go` | Root and transitively retain unfinished cleanup evidence |
| `internal/operator/dispatch_cleanup.go`, `dispatch_fences.go` | Bounded independent cleanup/fence scheduling |
| `internal/managed/dispatch*.go`, `retirement.go`, `replica.go` | Host effects, joined ownership and retirement |
| `internal/metaengine/scoped_load_scope.go` | Reject retired-group startup |
| `internal/metastore`, `internal/authoritystore` | Existing durable state and host journal |
| `storage/*`, `load/*`, `internal/raftengine` | Existing data plane and physical collection semantics |

Verification failures in these reused paths must be traced to a concrete missing
contract before broadening the production change surface.

### 9.17 Routes, construction and configuration

No library HTTP routes exist to add. The existing test application route
`DELETE /api/groups/{id}` continues calling `Cluster.DeleteGroup` with the same
request/response DTOs. The existing internal approval command gains the section 5.8 marker; no new
gRPC method, protobuf field, transport action kind,
permission mechanism or port is required.

`NewNode`, managed Host construction in `node_load.go`, operation-coordinator
wiring, retirement stores and lifecycle hook registration remain unchanged.
The existing `MaxConcurrentCleanup` budget remains the only cleanup capacity
control. Do not add a deletion timeout or force flag.

## 10. Data flows and failure behavior

### 10.1 Ordinary approved deletion

1. Caller supplies group ID and expected version.
2. Existing mutation stages the omission.
3. Approval captures metadata evidence; no native read is made for the deleted group.
4. Planner emits exact deferred actions.
5. FSM atomically installs tombstone, debt and logical success.
6. Caller observes success through the existing operation APIs.
7. Controller dispatches stops to reachable exact owners.
8. Stop receipts precede retirement grants and retirement receipts.
9. Host retention/collection runs under the existing protocol.
10. Retained evidence is pruned only after existing completion/reference rules allow it.

### 10.2 Host unavailable during approval

Steps 1–6 are identical. The host's debt remains unstopped and dispatch backs off.
Other host cleanup and unrelated group operations continue. Nothing marks its
data collected. Later connectivity lets the same worker retry the retained action.

### 10.3 Returning host: metadata log catch-up

The host rejoins with its retained identity and fresh process InstanceID.
Existing current-authority checks run before publishing retained load replicas.
Once its metadata includes the tombstone, scope lookup refuses the deleted group.
Exact cleanup grants then stop any remaining owner and retire its state.
Do not erase state merely because a local document is missing the group.

### 10.4 Returning host: metadata snapshot catch-up

When old log entries are compacted, install and validate the current metadata
snapshot. It carries permanent group retirement and retained exact cleanup
evidence. Rebuild indexes, reject the old assignment and process cleanup as above.
The protocol does not depend on replaying the original deletion document event.

### 10.5 Host never returns

Logical success remains permanent. Incomplete physical debt and required evidence
remain retained. No TTL, inferred stop, fake timestamp or global operation slot
is held. A future explicit evidence-retirement feature is out of scope.

### 10.6 Readmission races with deletion

A pending replacement admission does not block the new deletion block. The old
exact owner gets or retains ordinary deletion debt. Ordinary cleanup dispatch
waits behind approved replacement ownership; the existing readmission batch
fences and retires old resources, then records actual whole-batch evidence into
that debt. Later collection follows historical identity through journal rebind.
A partial batch cannot produce stop/retirement timestamps.

### 10.7 Unfinished creation or replacement is superseded

The collector includes issued targets even if publication or effect reporting
never finished. Approval installs their debt before logical completion.
Supersession closes grants and prepares host fences through the existing path.
An in-flight factory or Close remains owned until it actually returns.
The cleanup lane joins it and handles late success under the exact identity.
Unissued possible declarations need no physical cleanup grant.

### 10.8 Metadata leader changes or process crashes

All authoritative state is in the replicated candidate. Before commit, no
deletion authority exists. After commit, a new leader rebuilds cleanup queues
from debt and authority, regardless of whether the original client got a reply.
Volatile worker/cursor loss does not lose debt. Local effect journal replay
prevents repeating completed physical actions without their original evidence.

### 10.9 Partitioned live load group

An isolated group may continue serving through its own quorum before it observes
deletion. Do not use successful metadata deletion as proof otherwise. When it
reconnects, existing authorization and exact cleanup apply. Operator quiescence
is the operational precondition; no metadata serving lease is added.

### 10.10 Cache behavior

There is no deletion cache hit/miss protocol. Public observations may lag.
`ReadProgress` still performs one bounded metadata read and attaches only exact
cached observations with their original timestamps. A stale observation cannot
override an approved tombstone or authorize new startup. Controller backoff cache
loss only changes retry timing.

### 10.11 Node shutdown during cleanup

Existing Stop ownership joins dispatcher work and host callbacks before releasing
owned stores. Waiter timeout leaves exact ownership retained for retry. A late
factory success transfers Close responsibility as before. Borrowed resources
remain borrowed. Metadata store closure still waits for managed teardown.

### 10.12 Failure-mode matrix

| Failure/condition | Logical deletion | Deferred work/evidence |
| --- | --- | --- |
| No metadata quorum | Cannot approve; existing uncertain-outcome rules apply | No new local effect from staging |
| No load quorum, metadata available | Can approve and succeed | Stops do not need load consensus |
| All load hosts unreachable | Can approve and succeed | Exact debt persists |
| Wrong CAS | Refuse normally | Original state unchanged |
| Malformed owner/action/member/storage | Refuse atomically | Preserve original evidence |
| Client reply lost | Replay original mutation/approval | Never allocate a different deletion implicitly |
| Stop callback delayed/fails | Remains logically succeeded | Retain ownership and retry |
| Store retirement fails | Remains logically succeeded | Stopped may be proven; retired stays absent |
| Partial readmission | Can succeed with ordinary debt | No synthetic whole-batch proof |
| Replacement member approved | Can succeed | Do not target replacement with old-member Stop |
| Native fence cannot reach dead load quorum | Does not block deletion | Physical revocation remains pending |
| Cleanup receipt late after supersession | Existing exact validation decides acceptance | No changed tuple or successor timestamp |
| Host state missing | Not collection proof | Existing exact retirement/absence validation |
| Foreign/successor storage at path | No destructive access | Park/refuse with evidence intact |
| History pruning/snapshot | Tombstone survives | Unfinished debt pins its proof graph |
| Reuse of deleted group ID | Refuse | Permanent allocation/retirement evidence retained |

## 11. Verification specification

This document is design work. The following are implementation gates, not tests
claimed as executed by this design session.

### 11.1 Planner units

Update the existing deletion/partition/readmission tests; retain their exact
identity and ordering assertions while changing the completion partition.

- Three former hosts produce no required steps and exact cleanup stops.
- Existing detached debt is inherited once, with no duplicate retirement.
- Former required same-host stop is rebound once into cleanup on group deletion.
- Missing stop/debt for a hosted identity is rejected.
- Pending readmission retains deletion obligations; completed readmission does
  not repeat physical work.
- Same-host replacement of a surviving group still requires stop/retire.
- Deletion planning succeeds without a load-native observation.
- Legacy plan shapes remain shape-valid and immutable.

Relevant existing files: `topology/plan_partition_test.go`,
`plan_blocks_test.go`, `plan_retirement_test.go`,
`plan_shape_scoped_test.go`, `planner_scoped_test.go`,
`readmission_scoped_test.go` and `readmission_pending_test.go`.

### 11.2 Metadata units

Use the owning-package scoped fixtures; no live replicas in unit tests.

- Approval produces a nil checkpoint and successful deletion block with all
  stop/retire/collection receipts absent.
- `ReadyCleanup` returns exact stop work after the operation succeeds.
- Retirement cannot issue before actual stop.
- Missing/foreign debt or wrong action provenance refuses the whole candidate.
- Supersession preserves every possible issued owner.
- Mixed operation deletion block succeeds while another block remains pending.
- Missing/partial/fenced readmission does not block deletion with ordinary debt;
  complete readmission settles only its exact identities.
- Cleanup dispatch never addresses a replacement using the old member's action.
- New logical tombstone does not prematurely settle native or host revocation.
- All actual stops allow native-resource settlement; host retirement/collection
  remain separately observable.
- Delayed/lost receipts retain original timestamps and ownership.
- Snapshot round-trip and pruning preserve pending debt after logical success.
- No authoritative side effects occur during decoding.
- Historical unmarked approval commands replay over an older snapshot with
  their original required-stop and zero-step/readmission behavior.
- Marker-1 commands replay to the new logically complete state; unknown marker
  values refuse before state changes; old zero-command encodings stay identical.
- Mixed-operation retry retains succeeded deletion blocks without requiring a
  persistent marker on public Operation or Plan records.
- Frozen compatibility artifacts remain unchanged.
- Marker-1 approval rejects required-stop deletion shape; legacy restored operations
  retain their original execution/retry behavior.

Update relevant `scoped_deletion*`, `scoped_cleanup*`, `ready_cleanup_test.go`,
`scoped_group_completion_test.go`, `scoped_retention_test.go` and
`ready_fences_test.go`. Keep tests that require physical proof for readmission,
same-host replacement and collection; only the logical deletion gate changes.

### 11.3 Functional tests

Use protocol doubles or a single local participant, owning temporary roots and
joined cleanup:

- Exact deleted replica refuses retained startup/handle publication.
- Cleanup after terminal operation still reaches existing managed stop and
  retirement code.
- Failed Close, late factory success and cancellation retain ownership.
- Pending native fence work does not starve reachable host stop work under the
  existing cleanup limit.
- Historical-member readmission reports settle the intended deletion debt.
- Store reopen and local journal replay preserve actual milestones.

Extend owning `*_func_test.go` files. Reuse
`TestStartupRecaptureIgnoresIndependentVersionAndRejectsAssignmentChange` and
`TestFencedCleanupUsesRemoteGrantAndPreservesSuccessorEvidence` where appropriate.
Run relevant functional race coverage because dispatch/teardown ownership is part
of the regression boundary.

### 11.4 Three-node process scenarios

Use the existing Docker pool and authenticated fixture controls.

1. Rewrite `TestFailureIsolation_DeletionStopProof` as
   `TestFailureIsolation_DeletionWithoutFormerHost`:
   kill a non-meta-leader former host, approve deletion, require operation success
   while that process remains killed, verify its physical milestones are absent,
   restart it, and require eventual exact cleanup and rejection of the retired ID.
2. Preserve its independent-group serving assertion and retained-volume checks.
3. Add `TestFailureIsolation_DeletionWithoutLoadQuorum`:
   keep metadata connectivity healthy on three hosts while disabling only the
   selected load group's transport sufficiently to lose its quorum. Approve its
   deletion; require logical success and continued unrelated-group progress.
   Do not kill two metadata voters to manufacture this condition.
4. Add `TestFailureIsolation_DeletionSnapshotCatchup`:
   keep one host offline across deletion and enough metadata progress to require
   snapshot catch-up; restart from retained storage and verify no deleted replica
   is published, exact cleanup occurs, and permanent ID reuse still fails.
5. Exercise delete/readmission overlap with existing authenticated delays and
   exact barriers. A partial readmission must not block logical deletion or
   falsely report physical retirement.

Use three nodes. New fixture controls, if required, belong in
`tests/cluster/testapp` and use public APIs. Do not add a multi-node functional
fixture to the root module.

### 11.5 Commands and checkpoints

Focused development examples:

~~~sh
go test ./topology ./internal/metaengine ./internal/operator -run 'Deletion|DeletionWithout|Cleanup|Readmission|Retained' -count=1
make test-unit
make lint
make test-function FUNCTIONAL_ARGS="-run 'Deletion|Cleanup|Readmission|StartupRecapture'"
make test-function-race FUNCTIONAL_ARGS="-run 'Deletion|Cleanup|Readmission|StartupRecapture'"
~~~

Before final process validation, run `make lint-full` once, then:

~~~sh
make test-cluster-build
make test-cluster-run CLUSTER_ARGS='-v -run TestFailureIsolation_Deletion'
~~~

Complete the repository-required final feature gates, including unit race and
broader process coverage where changed authority/readmission paths warrant them.
Do not repeatedly run expensive suites during intermediate edits. Read fresh
reports and package logs; historical validation is not a new test run.

No protobuf generation is expected: the sole wire addition is the optional
internal JSON approval-command marker; protobuf shapes remain unchanged.
Any need to edit generated protocols or baseline files is a reason to
revisit the implementation against this design.

## 12. Documentation and scope limits

During implementation update:

- `AGENTS.md`: replace the all-former-host-stop deletion invariant with logical
  completion plus retained exact cleanup; preserve physical proof requirements.
- `README.md`: deletion through metadata quorum and eventual host cleanup.
- `docs/architecture/library.md`: `WaitOperation` / progress meaning and caller
  quiescence responsibility.
- `docs/architecture/runtime.md`: offline return, pending readmission, local
  callback ownership and physical revocation.
- `docs/architecture/project-map.md`: logical tombstone versus native settlement,
  plan partition and cleanup mapping.
- `docs/architecture/comparision-with-tikv.md`: update the implemented status only
  after the code and tests land; retain the distinction from Region merge.
- `docs/architecture/raft.md`: no load quorum for destructive logical deletion;
  ordinary native membership still needs its own quorum.
- Public comments on `DeleteGroup`, `WaitOperation` and progress fields as needed.
- Testing documents only with newly executed validation evidence.

This target does not modify the current architecture documents to claim the
feature already exists.

### NOT CONSIDERED & TODO

- **[postponed]** Migration of previously approved legacy required-stop deletion
  plans. Preserve their original semantics in this change; no migration API or
  silent plan rewriting.
- **[potential-improvement]** Explicit permanent-host attestation to retire
  unconfirmed physical debt. Default remains durable unconfirmed debt; no stub
  API is introduced.
- **[potential-improvement]** Strong global cessation of serving before deletion
  success. Explicitly excluded by the agreed eventual-stop contract; no serving
  lease or per-request metadata check.
- **[postponed]** External application side effects and remote infrastructure
  disposal. Existing application ownership remains; no new callbacks or waivers.
- **[postponed]** Mixed-version/downgrade support. Existing unsupported policy
  remains; no feature-negotiation stub.

## 13. Implementation pseudocode

The code below is Go-flavoured pseudocode. Existing helper names identify reuse;
local lookup operations describe bounded map/slice work, not additional public
APIs. Each block specifies a changed/new decision point.

The changed approval algorithms below describe **marker 1**. Their mode-0
branches execute the original pre-change logic unchanged; that is required for
historical command replay, not just snapshot restore. Shared helpers receive
`semantics` explicitly or use the ephemeral `p.semantics` / `h.semantics` field.
The mode is not inferred from step counts, since zero-step plans are ambiguous.


### 13.1 planDeletedGroup

~~~go
func (b *topologyPlanBuilder) planDeletedGroup(groupID string) {
    needsNewRetirement := false
    for _, owner := range b.input.formerReplicas(groupID) {
        switch owner.Progress {
        case RetainedHosted:
            b.emit(groupID, StepStopReplica, owner.NodeName, owner.ReplicaID)
            needsNewRetirement = true
        case RetainedCleanup, RetainedStopped:
            // Existing exact debt/proof owns this identity.
        default:
            // Input validation rejects unknown progress before planning.
        }
    }
    if needsNewRetirement {
        b.emit(groupID, StepDeleteGroupState, "", 0)
    }
    // bindDetachedStops handles required->detached inherited stop handover.
    // bindPlanBlocks partitions these actions using the shared predicate.
}
~~~

### 13.2 requiredRetainedStop

~~~go
func requiredRetainedStop(desired *Group, key ReplicaKey) bool {
    return desired != nil && slices.ContainsFunc(desired.Replicas, func(r Replica) bool {
        return r.NodeName == key.NodeName && r.ReplicaID != key.ReplicaID
    })
}
~~~

### 13.3 validateBlockSafety

~~~go
func validateBlockSafety(desired Document, plan Plan) error {
    for _, block := range plan.Blocks {
        for _, seq := range block.Cleanup {
            step := plan.Steps[seq-1] // Header validates sequence bounds first.
            if step.Kind == StepStopReplica &&
                requiredRetainedStop(desired.Group(block.GroupID), exactKey(step)) {
                return invalid("same-host replacement requires its predecessor stop")
            }
        }
    }
    return validateFoundingBarriers(desired, plan)
}
~~~

### 13.4 Existing stop-order validation (reused)

No change is needed to `validateStopOrder` or `detachingStop`. When the desired
group is absent, the existing `validateStopOrder` does not require adjacent
native removal. The changed partition predicate classifies the exact stop as
cleanup; FSM coverage verifies the retained action binding.

### 13.5 validateDeletionOrder

~~~go
func (v *planShapeValidator) validateDeletionOrder(step Step) error {
    for _, owner := range v.input.formerReplicas(step.GroupID) {
        if owner.Progress == RetainedStopped || owner.Progress == RetainedCleanup {
            continue // FSM must independently validate debt/proof.
        }
        if v.stopped[step.GroupID][owner.ReplicaID] {
            continue // Exact planned stop; may be in Cleanup.
        }
        if ReadmissionCovers(v.input, v.desired, v.plan, owner.NodeName) {
            continue // Preserve old plan shape validity; not new approval proof.
        }
        return errorMissingExactStop(step.GroupID, owner.ReplicaID)
    }
    return nil
}
~~~

### 13.6 applyDeleteGroupState

~~~go
func (v *plannedStepValidator) applyDeleteGroupState(step Step, state *validationGroup) error {
    for _, owner := range v.input.formerReplicas(step.GroupID) {
        if owner.Progress == RetainedStopped || owner.Progress == RetainedCleanup {
            continue
        }
        if state.stopped[owner.ReplicaID] ||
            ReadmissionCovers(v.input, v.desired, v.plan, owner.NodeName) {
            continue
        }
        return planUnsafef("delete group lacks exact owner coverage")
    }
    return nil
}
~~~

### 13.7 validateDeletionRetirement (reused unchanged)

~~~go
func (v *planShapeValidator) validateDeletionRetirement(group string) error {
    for _, owner := range v.input.formerReplicas(group) {
        if owner.Progress != RetainedHosted {
            continue
        }
        // Keep historical readmission-only plans shape-valid.
        // New approval coverage separately requires ordinary debt unless actual proof exists.
        if ReadmissionCovers(v.input, v.desired, v.plan, owner.NodeName) {
            continue
        }
        if !planContainsDeleteState(v.plan, group) {
            return planUnsafef("missing retained group retirement")
        }
    }
    return nil
}
~~~

### 13.8 readmissionPlanBuilder.appendStep

~~~go
func (p *readmissionPlanBuilder) appendStep(step Step) {
    // Preserve existing metadata preparation/emission logic.
    applyExistingMetadataEmissionRules(step)

    replacement := p.replacement(step.NodeName)
    deletingLoad := step.GroupID != MetaGroupID && p.desired.Group(step.GroupID) == nil
    if replacement != nil && !deletingLoad &&
        (step.Kind == StepStopReplica || step.Kind == StepCleanupReplicaState) &&
        exactPlanReplica(p.desired, step) == nil {
        return
    }
    if step.Kind == StepDeleteGroupState && !p.requiresDeletion(step) {
        return
    }
    p.steps = append(p.steps, step)
}
~~~

### 13.9 readmissionPlanBuilder.requiresDeletion

~~~go
func (p *readmissionPlanBuilder) requiresDeletion(step Step) bool {
    owners := p.input.formerReplicas(step.GroupID)
    if p.desired.Group(step.GroupID) == nil {
        return len(owners) == 0 || any(owners, progressIsHosted)
    }
    return existingNonDeletionPredicate(owners, p)
}
~~~

### 13.10 validateNewRetirement and duplicateRetirement

~~~go
func validateNewRetirement(c planningCollector, input PlanInput, desired Document, plan Plan, semantics DeletionSemantics) error {
    if semantics == DeletionSemanticsLegacy {
        // Execute the original validation branch, including old duplicate rules.
        return originalRetirementValidation(c, input, desired, plan)
    }
    c.markConvergedOwners(input.Groups)
    for _, block := range plan.Blocks {
        if desired.Group(block.GroupID) == nil && len(block.Steps) != 0 {
            return fault.ErrOperationAuthorization // New approvals only.
        }
    }
    hosted := collectHostedGroups(c, func(group string, owner planningReplica) bool {
        return desired.Group(group) == nil ||
            !ReadmissionCovers(input, desired, plan, owner.target.NodeName)
    })
    for _, step := range plan.Steps {
        if step.Kind == StepDeleteGroupState && !hosted[step.GroupID] {
            return fault.ErrOperationAuthorization
        }
        owner := c.owners[exactPlanningKey(step)]
        if owner != nil && duplicateRetirement(*owner, desired.Group(step.GroupID), step, semantics) &&
            !(owner.progress == RetainedCleanup && c.detachingRequiredStop(step)) {
            return fault.ErrOperationAuthorization
        }
    }
    return nil
}
~~~

~~~go
func duplicateRetirement(owner planningReplica, desired *Group, step Step,
    semantics DeletionSemantics) bool {
    if semantics == DeletionSemanticsLegacy {
        return originalDuplicateRetirementPredicate(owner, desired, step)
    }
    switch step.Kind {
    case StepRemoveReplica:
        return owner.removed != 0
    case StepCleanupReplicaState:
        return owner.progress == RetainedCleanup || owner.progress == RetainedStopped
    case StepStopReplica:
        if owner.progress == RetainedStopped { return true }
        if owner.progress != RetainedCleanup { return false }
        // New deleted-group stops must not duplicate existing detached ownership.
        return desired == nil || !hostReusedByDifferentReplica(desired, owner)
    }
    return false
}
~~~

### 13.11 prepareDeletionDebts

~~~go
func prepareDeletionDebts(source, next *scopedState, collector planningCollector,
    semantics DeletionSemantics) error {
    op := newestOperation(next)
    blocks, retired := next.deletionScopes(op)
    debts := cloneCleanupRows(next.Cleanup)
    existing := indexCleanupRows(debts)
    p := deletionPreparation{source: source, next: next, collector: collector, semantics: semantics}

    for _, key := range collector.sortedOwners() {
        owner := collector.owners[key]
        block, deleting := blocks[key.group]
        if !deleting || owner.progress == "" { continue }
        id := exactStorageIdentity(next.Target.ClusterID, key, owner)
        row, err := p.deletionDebt(id, block, existing[id])
        if err != nil { return err } // No source or partial candidate publication.
        if row.Identity != (storage.Identity{}) {
            debts = append(debts, row)
            existing[id] = ownedLookup(row)
        }
    }
    sortCleanupAndRetiredIDs(debts, retired)
    next.Cleanup, next.RetiredGroups = debts, retired
    return nil
}
~~~

### 13.12 ordinaryDebt

~~~go
func (p deletionPreparation) ordinaryDebt(id storage.Identity, block PlanBlock) (cleanupRecord, error) {
    owner := p.collector.owners[planningReplicaKey{id.GroupID, id.ReplicaID}]
    op := newestOperation(p.next)
    row := exactNewDebt(id, owner.target.MemberID, op.Status.ID, op.UpdatedAt)
    class, err := p.source.formerReplicaStorage(id, row.MemberID)
    if err != nil { return cleanupRecord{}, err }
    row.Storage = class

    if owner.progress == RetainedStopped {
        // Only actual complete readmission may materialize actionless debt.
        return p.readmissionDebt(row, op.UpdatedAt)
    }
    for _, seq := range block.Cleanup {
        step := op.Plan.Steps[seq-1]
        if exactStop(step, id) { row.StopAction = boundAction(op, step) }
        if step.Kind == StepDeleteGroupState { row.RetireAction = boundAction(op, step) }
    }
    if row.StopAction == nil || row.RetireAction == nil {
        return cleanupRecord{}, fault.ErrOperationAuthorization
    }
    return row, nil
}
~~~

### 13.13 deletionDebt

~~~go
func (p deletionPreparation) deletionDebt(id storage.Identity,
    block PlanBlock, existing *cleanupRecord) (cleanupRecord, error) {
    if existing == nil { return p.ordinaryDebt(id, block) }
    owner := p.collector.owners[planningReplicaKey{id.GroupID, id.ReplicaID}]
    if existing.Identity != id || existing.MemberID != owner.target.MemberID {
        return cleanupRecord{}, fault.ErrIdentityMismatch
    }
    class, err := p.source.formerReplicaStorage(id, existing.MemberID)
    if err != nil { return cleanupRecord{}, err }
    if existing.Storage != class { return cleanupRecord{}, fault.ErrIdentityMismatch }
    // Existing handover has already rebound a required stop if needed.
    // Full candidate coverage/action validation follows. No receipt wait here.
    return cleanupRecord{}, nil
}
~~~

### 13.14 validateDetachedCoverage

~~~go
func (p deletionPreparation) validateDetachedCoverage(blocks []PlanBlock) error {
    deleting := map[string]bool{}
    for _, block := range blocks {
        if p.next.Target.Group(block.GroupID) != nil { continue }
        if block.GroupID == MetaGroupID || len(block.Steps) != 0 ||
            !hasPermanentRetiredID(p.next, block.GroupID) {
            return fault.ErrOperationAuthorization
        }
        deleting[block.GroupID] = true
    }
    debts, operations := preparedLookupMaps(p.next) // Built once for this batch.
    for _, key := range p.collector.sortedOwners() {
        owner := p.collector.owners[key]
        if !deleting[key.group] || owner.progress == "" { continue }
        id := exactStorageIdentity(p.next.Target.ClusterID, key, owner)
        row := debts[id]
        if row == nil || row.MemberID != owner.target.MemberID {
            return fault.ErrOperationAuthorization
        }
        if !sourceStorageMatches(p.source, *row) {
            return fault.ErrIdentityMismatch
        }
        if row.Stopped != nil {
            // Existing whole graph validates actual stop/readmission provenance.
            continue
        }
        if row.StopAction == nil || row.RetireAction == nil ||
            !exactRetainedCleanupAction(operations, *row.StopAction, id) {
            return fault.ErrOperationAuthorization
        }
    }
    return nil
}
~~~

### 13.15 prepareDeletionCompletions

~~~go
func prepareDeletionCompletions(source, next *scopedState, c planningCollector,
    semantics DeletionSemantics) error {
    if semantics == DeletionSemanticsLegacy {
        return originalDeletionCompletion(source, next, c)
    }
    p := deletionPreparation{source: source, next: next, collector: c, semantics: semantics}
    op := newestOperation(next)
    // Validate every new deletion before changing any completion row.
    if err := p.validateDetachedCoverage(op.Plan.Blocks); err != nil { return err }
    if err := p.protectReadmission(); err != nil { return err }

    for i, block := range op.Plan.Blocks {
        if next.Target.Group(block.GroupID) != nil { continue }
        next.completeApprovalGroup(block.GroupID, op)
        op.Status.Blocks[i].State = OperationSucceeded
        op.Status.Blocks[i].CurrentStep = 0
        op.Status.Blocks[i].Blocked = nil
    }
    op.Status.State = aggregateBlockStates(op.Status.Blocks)
    return nil
}
~~~

### 13.16 protectReadmission / dependsOnReadmission

~~~go
func (p deletionPreparation) protectReadmission() error {
    for _, key := range p.collector.sortedOwners() {
        owner := p.collector.owners[key]
        if currentNewDeletionHasIndependentDetachedCoverage(p, key) {
            continue // A particular pending batch is no longer the only obligation.
        }
        // Keep existing exact legacy dependency validation, including refusal
        // of cancellation/rebinding that abandons its sole retirement source.
        if err := validateExistingReadmissionDependency(p, key, owner); err != nil {
            return err
        }
    }
    return nil
}
~~~

~~~go
func (p deletionPreparation) dependsOnReadmission(key planningReplicaKey,
    target AuthorityTarget) bool {
    if currentNewDeletionHasIndependentDetachedCoverage(p, key) {
        return false
    }
    return existingLegacyDependencyPredicate(p, key, target)
}
~~~

### 13.17 cleanup handover

~~~go
func detachedCleanupAllowed(desired Document, id storage.Identity) bool {
    group := desired.Group(id.GroupID)
    return group == nil || !hostReusedByOtherReplica(group, id)
}
~~~

~~~go
func (h cleanupHandover) prepare(debt cleanupRecord) (cleanupRecord, error) {
    // Existing binding lookup and required/detached selection stay unchanged.
    action, found, wasRequired := selectExistingHandover(h, debt)
    if found {
        if h.at.Before(debt.UpdatedAt) { return cleanupRecord{}, fault.ErrOperationAuthorization }
        if h.desired.MemberID(debt.Identity.NodeName) > debt.MemberID &&
            h.desired.Group(debt.Identity.GroupID) != nil {
            return cleanupRecord{}, fault.ErrOperationAuthorization
        }
        // Deleted-group branch requires exact detached action and source owner;
        // it does not authorize delivery to the newer member.
        validateSelectedHandoverOrReturnError(h, debt, action, wasRequired)
        debt.StopAction, debt.UpdatedAt = &action, h.at
        return debt, nil
    }
    if !wasRequired { return debt, nil }
    return existingCompleteReadmissionHandover(h, debt)
}
~~~

### 13.18 stopAction validation

~~~go
func (check scopedCleanupValidation) stopAction(debt cleanupRecord) bool {
    origin, bound := lookupAndValidateExactStopBinding(check, debt)
    if origin == nil || bound == nil { return false }
    if requiredCleanupStop(bound, *debt.StopAction) {
        return debt.Stopped != nil || debt.ReadmissionKey != nil || check.pendingStop(debt, bound)
    }
    if !matchingBlockContainsCleanup(bound.Plan, debt.Identity.GroupID, debt.StopAction.StepSeq) {
        return false
    }
    if debt.RetireAction.Kind == StepDeleteGroupState {
        return hasPermanentRetiredID(check.state, debt.Identity.GroupID) &&
            check.state.Target.Group(debt.Identity.GroupID) == nil
    }
    return true
}
~~~

### 13.19 cleanupWaitsForReadmission

~~~go
func (s *scopedState) cleanupWaitsForReadmission(debt *cleanupRecord) bool {
    return debt.Identity.GroupID != MetaGroupID &&
        s.Target.Group(debt.Identity.GroupID) == nil &&
        hasPermanentRetiredID(s, debt.Identity.GroupID) &&
        debt.Retired == nil &&
        s.Target.MemberID(debt.Identity.NodeName) > debt.MemberID
}
~~~

### 13.20 cleanupDebtForAction

~~~go
func (s *scopedState) cleanupDebtForAction(id storage.Identity,
    action AuthorizedStep) (*cleanupRecord, error) {
    debt, err := existingExactCleanupBindingAndPartitionChecks(s, id, action)
    if err != nil { return nil, err }
    if s.cleanupWaitsForReadmission(debt) {
        return nil, fault.ErrAuthorityPending
    }
    if action.Kind != StepStopReplica && debt.Stopped == nil {
        return nil, fault.ErrAuthorityPending
    }
    return debt, nil
}
~~~

### 13.21 validateScopedDeletion

~~~go
func validateScopedDeletion(state *scopedState, index *scopedIndexes) error {
    collector, err := collectExistingDeletionOwners(state, index)
    if err != nil { return invalidScopedState("contradictory deletion ownership") }
    for _, key := range collector.sortedOwners() {
        owner := collector.owners[key]
        row := index.groups[key.group]
        if owner.progress == "" || !deletionRow(row) { continue }
        id := exactStorageIdentity(state.Target.ClusterID, key, owner)
        debt := index.cleanup[id]
        if debt != nil && debt.MemberID == owner.target.MemberID {
            if debt.Stopped != nil { continue }
            if exactDetachedCleanupCoverage(state, index, debt) { continue }
            if row.Deleting && state.deletionStop(id) { continue } // Legacy.
        }
        if row.Deleting && validExistingLegacyReadmissionCoverage(collector, key, owner, state.Target) {
            continue
        }
        return invalidScopedState("deletion lost exact former-owner coverage")
    }
    return nil
}
~~~

### 13.22 completedDeletions

~~~go
func (s *scopedState) completedDeletions() map[string]bool {
    candidates := existingPermanentNilCheckpointCandidates(s)
    // Physical settlement requires actual Stop, not nil approved declaration.
    for _, debt := range s.Cleanup {
        if candidates[debt.Identity.GroupID] && debt.Stopped == nil {
            delete(candidates, debt.Identity.GroupID)
        }
    }
    return candidates
    // No catalog/index recursion here. Full graph validates coverage before install.
}
~~~

### 13.23 validateRetainedReplica

~~~go
func validateRetainedReplica(view RemainingPlanView, ex Record,
    group string, owner RetainedReplica) error {
    if retainedOwnerStillDesired(view, group, owner) { return nil }
    if owner.Progress == RetainedStopped ||
        ReadmissionCovers(view.Input, view.Target, effectivePlan(view, ex), owner.NodeName) {
        return nil
    }
    desired := view.Target.Group(group)
    if owner.Progress == RetainedCleanup &&
        (desired == nil || !desiredUsesHost(desired, owner.NodeName)) {
        return nil // Existing debt owns exact deferred work.
    }
    if !remainingPlanHas(ex, StepStopReplica, group, owner.ReplicaKey) {
        return remainingPlanError("plan omits exact retained stop")
    }
    return validateRetainedRetirement(view, ex, group, owner)
}
~~~

### 13.24 Internal approval marker decoding and selection

~~~go
func (p *BeginOperationProposal) UnmarshalJSON(data []byte) error {
    // Decode the existing payload and validate existing required fields/scopes.
    decoded := decodeExistingBeginOperationPayload(data)
    if decoded.DeletionSemantics > DeletionSemanticsDetached {
        return fault.ErrUnsupported
    }
    *p = decoded
    return nil
}
~~~

~~~go
func (g *Group) operationProposal(read metaengine.PlanRead,
    request applyVersionRequest) (any, error) {
    if request.Kind == "retry" {
        return existingExactRetryProposal(read, request)
    }
    plan, err := g.planOperation(read)
    if err != nil { return nil, err }
    proposal := existingBeginOperationPayload(read, request, plan)
    proposal.DeletionSemantics = metaengine.DeletionSemanticsDetached
    return proposal, nil
}
~~~

~~~go
func prepareScopedApproval(s *scopedState, desired topology.Document,
    proposal BeginOperationProposal, kind approvalKind) (*scopedState, error) {
    if proposal.DeletionSemantics > DeletionSemanticsDetached {
        return nil, fault.ErrUnsupported
    }
    semantics := proposal.DeletionSemantics
    // Preserve the current atomic candidate preparation sequence.
    // Pass semantics to validateNewRetirement, prepareCleanupHandovers,
    // prepareDeletionDebts and prepareDeletionCompletions.
    // Their zero branches run the original decisions; mode 1 uses this target.
    return prepareExistingCandidateWithExplicitSemantics(s, desired, proposal, kind, semantics)
}
~~~
