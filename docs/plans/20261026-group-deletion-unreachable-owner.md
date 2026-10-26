# Implementation plan: Group deletion with unreachable former owners

**Plan:** `docs/plans/20261026-group-deletion-unreachable-owner.md`
**Date:** 2026-10-26
**Project source:** `../..` relative to this plan; repository root `.`
**Branch:** `feat/group-deletition-wo-node`
**Source revision inspected:** `8e57798`
**Target architecture:** [20261026-group-deletion-unreachable-owner-target.md](../brainstorms/20261026-group-deletion-unreachable-owner-target.md)
**Target SHA-256:** `cd9d1d8c4b47d4532eb92a5a43297449837a1afa01f2f2772fa57fe690f56370`
**Full brainstorm log:** [20261026-group-deletion-unreachable-owner.md](../brainstorms/20261026-group-deletion-unreachable-owner.md)
**Issue:** <https://github.com/asalimonov/gquorum/issues/5>
**Status:** implementation in progress; task checkboxes record verified completion.

## User execution instruction — 2026-10-26

Run cluster tests only in Task 6, the penultimate task. Task 6 now includes full
lint before image build, focused and full Docker verification, and their fixes.
Task 7 performs the final architectural audit and non-cluster checks. If its
audit finds a code gap requiring cluster revalidation, reopen Task 6, complete
those runs there, then return to Task 7; do not run cluster tests in the final
task. This instruction supersedes the original command placement below.

## User documentation scope — 2026-10-26

The user removed feature documentation updates from Tasks 6 and 7. Keep a brief
changelog as the explicit exception. Preserve validation, final implementation
and wiring audit, plus accurate plan/progress records. This overrides earlier
requirements to update README, AGENTS, architecture documents or public comments.

## Execution contract

The linked target is the design authority. Section references below refer to that
file. Implement its agreed metadata-only logical deletion and deferred cleanup;
do not introduce a different deletion protocol.

The operator/application stops using the group before deletion. New approved
deletions require only metadata quorum and complete durable ownership of cleanup.
Load groups retain their independent serving behavior when metadata is
unavailable. Logical deletion does not claim all remote applications have stopped.

Preserve public method signatures, public JSON fields, options, step kinds, storage
interfaces, two network ports, metadata snapshot schema 4 and authority journal
schema 2. Target section 5.8 adds one optional INTERNAL approval-command marker
to preserve deterministic replay of old committed logs.
No force-delete API, serving lease, synchronous all-host barrier, extra controller,
new persistence backend or automatic traffic-idleness detector is planned.

Existing approved immutable plans retain their original required-step/retry
semantics. The new behavior applies to new approvals. Existing legacy pending
deletions are not silently migrated. Frozen compatibility fixtures remain
immutable; mixed-binary operation and downgrade after new approvals remain
unsupported as specified in target §3.3.

Create this plan during the planning session. The target and discussion receive
the documented replay-compatibility correction described below; no proposal file
or implementation code is produced. Implementation, validation
reports and the public changelog belong to subsequent execution. Update the
progress log after each completed task and commit its coherent changes. Record
unplanned changes and unresolved gaps in the sections at the end.

## Current-state reconciliation

Research confirmed these implementation facts:

| Current code/contract | Planned consequence |
| --- | --- |
| `requiredRetainedStop` returns true for absent desired groups | Task 3 changes deletion partitioning; same-host replacement remains required. |
| `validateBlockSafety` rejects deletion stops in cleanup | Task 1 prepares the validator before generation changes. |
| `planDeletedGroup` emits stops for every unstopped owner | Task 3 avoids duplicate work for already-retained debt. |
| `ordinaryDebt` finds stop actions only in required steps | Task 3 reads new deletion stops from cleanup. |
| `prepareDeletionCompletions` waits for stop/readmission receipts | Task 3 completes from exact durable cleanup coverage. |
| `ReadyCleanup` is independent of terminal operation status | Reuse its bounded queue and retry protocol. |
| `completedDeletions` currently assumes nil checkpoint means every host stopped | Task 2 preserves physical revocation by checking actual host-stop evidence. |
| `capturePlan` already skips native reads for deleted scopes | Preserve it; deletion needs no load quorum probe. |
| Retired IDs already refuse startup in `queryScopedLoadScope` | Verify actual startup/return wiring rather than add another watcher. |
| Metadata session liveness is not a continuous load-serving gate | Preserve independent load availability. |
| Current architecture/AGENTS still require every deletion host to stop | The user-approved target replaces that rule for new deletion plans; Task 6 aligns current docs after implementation. |

This requested contract change is already resolved in the brainstorm. It does
not waive identity, readmission, callback ownership or physical collection proof.

The worktree is the source repository. There is no `project.txt` or nested/symlinked
documentation Git repository at the inspected paths. Use the current feature
branch rather than initializing a separate repository. The documentation
directories are ignored by Git, so explicitly add only this feature's plan and
its target/discussion documents for the skill-requested documentation commit.

At planning start, an unrelated tracked deletion exists:
`docs/brainstorms/20261025-large-load-snapshot-transfer-target.md`.
Do not restore or stage it as part of this feature.

## Planning correction: historical committed approval replay

The initial target preserved old snapshots and pending plans but required every
new call to `validateNewRetirement` to reject required-stop deletion shapes.
Source review found that `applyBeginOperation` invokes that same validation when
an old committed `begin_operation` command is replayed after its snapshot.
Without a discriminator, the new code could refuse a command the old cluster
already committed. Zero-step readmission plans also make plan-shape inference
ambiguous.

Target §3.3, §5.8, §9.15a–§9.15c and §13.24 now specify the smallest explicit
correction: an optional internal `DeletionSemantics` command field, with zero
preserving old behavior and one selecting deferred deletion. Snapshot/journal
schemas and public APIs remain unchanged. This preserves old log interpretation;
it is not migration of old plans or a user-visible compatibility flag.

Task 2 adds decoding/zero-value compatibility and a temporary refusal of mode 1
until Task 3 wires the complete new path. Task 3 selects 1 in the real service
proposal constructor, passes it through approval helpers, preserves the original
zero branches and removes that temporary refusal. Task 4 tests actual old-log
replay, including the ambiguous readmission case, rather than only restore.

## Buildable checkpoint strategy

The planner and approval path have one atomic activation boundary in **Task 3**.
Changing the shared stop predicate before metadata support is ready would cause
new plans to fail approval. Rejecting old plan shapes before the planner changes
would likewise break current callers.

- Task 1 accepts the new plan shape in validation without changing generated plans.
- Task 2 adds candidate-state/cleanup validation and physical settlement support;
  existing public mutation paths still generate the old shape.
- Task 3 switches plan generation, exact debt preparation and logical approval
  completion together.
- Task 4 verifies persistence, compatibility, readmission and local ownership.
- Task 5 adds the three-node process regressions and necessary test-only controls.
- Task 6 aligns current documentation and runs penultimate cluster validation.
- Task 7 closes all gaps, runs final gates, creates the public changelog, and ends
  with a production wiring audit.

Task 2's new pure coverage helper is exercised by owning-package tests and is
wired into approval in Task 3. It must not introduce an interim public feature
flag, return fabricated success, or survive the final audit as test-only code.

Each task should remain within approximately 1,500 non-generated changed
production/test lines. If a measured task exceeds that, split supporting work
into additional green checkpoints without separating Task 3's activation
boundary or omitting coverage. Keep architectural delta verification last.

### Per-task verification

For each code-changing task:

- Format changed Go files, run `make test` across all five application modules,
  focused owning-package vet and `make lint` for pending changes.
- Run the task's focused regressions. Relevant functional tests may run; unrelated
  functional tests are reserved for the final checkpoint.
- All executed checks must pass, including failures outside the edited files.
  Investigate and fix necessary failures, record unrelated work explicitly, and
  preserve unrelated user changes. An unresolved failure prevents task completion.
- Do not run full lint, full unit race, full Docker, performance or simulation
  suites at every task. Full final gates belong to Task 7.
- Units must not start services, replicas or subprocesses. Functional tests use
  one local participant or protocol doubles, `t.Parallel()`, private roots,
  ephemeral listeners and joined cleanup. Multi-node tests use the Docker pool.

Documentation-only tasks validate references, API names, command quoting and
whitespace; they do not rerun unrelated runtime tests.

## Target representations reused by the tasks

### Public API: unchanged

~~~go
// Existing methods of gquorum.Cluster:
DeleteGroup(context.Context, string, MutationOptions) (MutationResult, error)
Approve(context.Context, uint64) (topology.OperationID, error)
WaitOperation(context.Context, topology.OperationID) (topology.OperationStatus, error)
RetryOperation(context.Context, topology.OperationID) (topology.OperationID, error)
ReadProgress(context.Context, topology.ProgressQuery) (topology.ProgressPage, error)
~~~

### New deletion partition: existing types

~~~go
Plan{
    Version: deletionVersion,
    Steps: []Step{
        {Seq: 1, GroupID: "orders", Kind: StepStopReplica, NodeName: "a", ReplicaID: 1},
        {Seq: 2, GroupID: "orders", Kind: StepStopReplica, NodeName: "b", ReplicaID: 2},
        {Seq: 3, GroupID: "orders", Kind: StepStopReplica, NodeName: "c", ReplicaID: 3},
        {Seq: 4, GroupID: "orders", Kind: StepDeleteGroupState},
    },
    Blocks: []PlanBlock{{
        GroupID: "orders", SourceVersion: previousConvergedVersion,
        Steps: nil, Cleanup: []int{1, 2, 3, 4},
    }},
}
~~~

Target §5.1–§5.7 contains the unchanged snapshot/journal schemas and JSON tags.
Target §5.8 adds the optional internal approval-command marker:

~~~go
type DeletionSemantics uint8
const (
    DeletionSemanticsLegacy DeletionSemantics = iota
    DeletionSemanticsDetached
)
// Added to internal/metaengine.BeginOperationProposal:
DeletionSemantics DeletionSemantics `json:"deletion_semantics,omitempty"`
~~~

Zero/absent retains old log semantics; new public approval writes 1. This field
is not added to public Plan, Operation, MutationOptions or snapshots. Unknown
values refuse. No new RPC/proposal kind or protobuf field is needed.

Reuse
`scopedState`, `retiredGroup`, `groupConvergence`, `Operation`, `PlanBlock`,
`cleanupRecord`, `ReplicaRecord`, `CollectionReceipt` and `CleanupProgress`.
There is no SQL DDL, ORM, schema migration or cache TTL to add.

The logical checkpoint has nil declaration, nonzero deletion source version,
empty owner and `Deleting=false`. Its operation block has zero required steps
and succeeds even when cleanup physical timestamps are nil.

### New private methods

~~~go
// internal/metaengine/scoped_deletion_detached.go
func (p deletionPreparation) validateDetachedCoverage(blocks []topology.PlanBlock) error

// internal/metaengine/scoped_cleanup_authorization.go
func (s *scopedState) cleanupWaitsForReadmission(debt *cleanupRecord) bool
~~~

Public signatures are preserved. Target §5.8/§9 additionally threads the internal
semantics parameter through approval-only helpers; target §9 is the complete method/file
inventory. Pseudocode-local lookup names in target §13 do not authorize adding a
new public abstraction or redundant subsystem.

## Task index

| Task | Goal | Depends on | Approximate non-generated LOC |
| --- | --- | --- | --- |
| 1 | Prepare plan validation for deferred deletion | — | 200–450 |
| 2 | Prepare metadata coverage, cleanup and physical settlement | 1 | 650–1,100 |
| 3 | Activate metadata-only deletion atomically | 1–2 | 900–1,500 |
| 4 | Verify durability, readmission and managed lifecycle | 3 | 600–1,100 |
| 5 | Add three-node offline/quorum/catch-up regressions | 3–4 | 650–1,200 |
| 6 | Align public contracts and architecture docs | 3–5 | Primarily documentation |
| 7 | Final validation and architectural delta verification | All preceding tasks | Verification and bounded gap fixes |

## Task list

### Task 1: Prepare plan validation for deferred deletion

**Goal:** make exact deferred deletion plans shape-valid without changing the
planner's selected output or the current approval completion path.

**Files:** update `topology/plan_block_safety.go`, `topology/plan.go`,
`topology/planner.go` (the validation method only), relevant
`topology/*_test.go`, and `internal/operator/remaining_plan.go` /
`remaining_retained_test.go`.

**References:** target §3.1 public API, §4 component diagram, §5.4 plan/block
schemas, §6.1–§6.2 validation and inherited debt, §9.1/§9.3/§9.4/§9.15 method
inventory, §13.3/§13.5/§13.6/§13.23 algorithms; target §11.1 tests.

- [x] Remove only the whole-deletion prohibition on cleanup stop steps from
  `validateBlockSafety`. Because the old shared predicate still treats nil desired
  groups as required until Task 3, guard the replacement check with a nonnil
  desired group so it does not reintroduce the deletion rejection.
- [x] Retain the same-host replacement and founding/admission dependency checks;
  reject cleanup stops that would bypass an actual surviving replacement.
- [x] Update `validateDeletionOrder` and `applyDeleteGroupState` to recognize
  retained cleanup as existing obligation coverage. Continue to require exact
  stop/retirement planning for a possibly hosted owner without retained proof.
- [x] Preserve readmission-covered historical plan shape validation. Shape
  acceptance alone must not be treated as proof of consumed readmission.
- [x] Update `validateRetainedReplica` to accept a deleted group's already-owned
  detached debt; keep required checks for surviving same-host replacements.
- [x] Leave `requiredRetainedStop`, `planDeletedGroup` generation,
  `readmissionPlanBuilder` and the metadata approval path unchanged in this task.
  Do not add a second partition builder or temporary public flag.
- [x] Reuse `validateStopOrder`, `detachingStop` and
  `validateDeletionRetirement` as specified in target §6.2/§13.4/§13.7; do not
  rewrite them simply to match pseudocode formatting.
- [x] Add parallel units using hand-built exact cleanup partitions: accept valid
  deletion cleanup and inherited debt, reject missing hosted-owner coverage,
  reject replacement-stop bypass, preserve flat stop-before-retire order, and
  accept valid historical required-stop plans.
- [x] Keep generator expectation tests asserting the old generated partition
  until Task 3. Split validator tests from generator tests where they currently
  conflate those behaviors.

#### Verification

- [x] `go test ./topology ./internal/operator -run 'Plan|Retained|Deletion|Remaining' -count=1`.
- [x] `make test`, `go vet ./topology ./internal/operator` and `make lint` pass.
- [x] Inspect representative generated deletion plans and confirm they still
  have the old required-stop partition; only validation support was prepared.
- [x] Record actual results and commit this coherent checkpoint.

### Task 2: Prepare metadata coverage, cleanup validation and physical settlement

**Goal:** support valid terminal tombstones with unfinished detached cleanup,
without enabling new deletion approval semantics yet.

**Files:** create `internal/metaengine/scoped_deletion_detached.go` and its unit
tests; update `scoped_deletion.go` (restore validation only),
`scoped_cleanup_progress.go`, `scoped_cleanup_authorization.go`,
`scoped_deleted_authority.go`, `operation_proposals.go` and the existing
approval entry check for the temporary mode guard, plus corresponding tests.

**References:** target §4 FSM/authority components and diagram; §5.2–§5.8 exact
state, debt and index schemas; §7.3/§7.5 coverage and restore; §8.1–§8.3 cleanup,
readmission priority and physical authority; §9.7/§9.9/§9.12–§9.15c signatures;
§13.14/§13.18–§13.24 algorithms; §11.2 acceptance.

- [x] Add the target §5.8 internal enum and optional `BeginOperationProposal`
  field. Extend its custom decoder to accept absence/0 as legacy, accept 1 and
  reject unknown values without changing required-field/plan-scope validation.
  Existing zero-command encoding must remain byte-identical.
- [x] Keep service-created proposals at zero until Task 3. Temporarily refuse
  marker-1 Apply with an explicit unsupported result before candidate mutation;
  do not decode and silently execute it with legacy semantics. Record removal
  of this internal preparation guard in Task 3.
- [x] Add codec tests for absent/zero/one/unknown markers and old serialized
  approvals. Do not add a marker to public DTOs or alter the proposal kind.
- [x] Add `validateDetachedCoverage(blocks []topology.PlanBlock) error` exactly as
  specified. Reuse `deletionPreparation`, the source collector, and candidate
  operation/debt maps; inspect source owners once per approval batch.
- [x] Validate exact group, member, replica, storage class, operation/action tuple,
  cleanup partition, permanent ID and complete owner coverage. Do not call
  network, application callbacks, storage acquisition or recursive catalog
  reconstruction from this helper.
- [x] Require no required steps for the new deletion shape. Treat missing debt,
  foreign identity and malformed source provenance as hard errors; a nil physical
  receipt by itself is valid pending work.
- [x] Update `validateScopedDeletion` to allow a logically completed tombstone
  with exact detached debt, while keeping missing/foreign/required-stop-only
  coverage invalid for that new terminal state.
- [x] Keep historical pending deletion/readmission rows valid on restore.
  Decoding must not complete an operation, manufacture a milestone, or modify
  its immutable plan.
- [x] Update `scopedCleanupValidation.stopAction` to validate a detached Stop
  paired with `StepDeleteGroupState` under permanent approved retirement.
  Preserve bound-operation provenance, handover ordering and exact receipts.
- [x] Add `cleanupWaitsForReadmission(debt *cleanupRecord) bool` and invoke it
  after existing exact binding/class/assignment checks in `cleanupDebtForAction`.
  It defers ordinary dispatch for a deleted group's historical member when a
  newer member is approved; it must not affect approval or unrelated identities.
- [x] Tighten `completedDeletions` to filter out candidates with any retained
  unstopped owner. Use state slices while indexes are being built. Do not call
  `authorityCatalog` recursively or settle host resources from a tombstone.
- [x] Exercise actual `ReadyCleanup`, `BeginCleanupEffect` and restore/query
  entry points on manually constructed valid candidate states: terminal operation
  still exposes Stop; retirement waits for Stop; readmission priority yields;
  incorrect action tuples refuse; native revocation stays pending until actual
  host stops.
- [x] Add negative tests for removing one known debt row, partial readmission
  proof, inconsistent timestamps and false physical settlement. Compare source
  bytes before/after refusals to prove no partial mutation.
- [x] Keep the new coverage helper unselected by production approval until Task
  3; it is test-exercised supporting logic, not an alternate execution path.

#### Verification

- [x] `go test ./internal/metaengine -run 'Deletion|Cleanup|ReadyFence|Authority|Frozen' -count=1`.
- [x] `make test`, `go vet ./internal/metaengine` and `make lint` pass.
- [x] Existing generated/approved deletion behavior remains unchanged. Existing
  snapshots and required-stop plans still restore without rewrite.
- [x] Check snapshot/journal struct fields, tags and version constants have no
  delta, and only the documented optional internal command field was added; record results and commit.

### Task 3: Activate metadata-only deletion in planner and approval together

**Goal:** enable the requested behavior in one coherent production checkpoint,
with complete exact cleanup coverage and no load-quorum dependency.

**Files:** update `topology/planner.go`, `plan_retirement.go`,
`readmission_plan.go`; `internal/metaengine/scoped_approval_retirement.go`,
`scoped_deletion.go`, `scoped_deletion_proof.go`,
`scoped_deletion_completion.go`, `scoped_cleanup_handover.go`,
`scoped_approval.go` and `internal/metaservice/operation_approval.go`; existing fixture
helpers and affected unit/functional expectation tests. Preserve the existing
atomic installation sequence while threading the internal command semantics.

**References:** target §1.1 completion contract, §3.1–§3.3 public behavior and
legacy plans, §4 diagram, §5.3–§5.5 unchanged schemas, §5.8 replay marker, §6 planner, §7 approval,
§8.2 readmission, §9.1/§9.2/§9.5–§9.11/§9.15a–§9.15c inventory, §13.1/§13.2/§13.8–§13.17/§13.24
algorithms; §10.1/§10.2/§10.6/§10.7 flows and §11.1–§11.2 tests.

- [x] Set `DeletionSemanticsDetached` in the real
  `metaservice.operationProposal` approve constructor. Preserve the Retry
  command shape and existing recovery/repair emitters' semantics.
- [x] Validate and propagate the committed mode through `prepareScopedApproval`,
  `validateNewRetirement` / `duplicateRetirement`,
  `prepareCleanupHandovers`, `prepareDeletionDebts` and
  `prepareDeletionCompletions`. Add the ephemeral semantics field to the existing
  deletion/handover helpers and use keyed construction literals.
- [x] Preserve the complete original mode-0 decision branches, including old
  duplicate-retirement rules, required-step action lookup and zero-step
  readmission waits. Remove Task 2's temporary refusal of mode 1. Do not infer
  mode from plan shape, runtime version, health, time or a replay flag.
- [x] Change `requiredRetainedStop` to require a stop only when a nonnil desired
  group reuses the host for another replica ID. Reuse `detachedPlanStep` and
  `bindPlanBlocks` so deletion stops enter `Cleanup`.
- [x] Change `planDeletedGroup` to emit exact stops only for `RetainedHosted`
  owners, and one shared deletion-state step when newly owned retirement exists.
  Existing `RetainedCleanup` and `RetainedStopped` owners keep their original
  debt/proof; `bindDetachedStops` owns required-to-detached handover.
- [x] Preserve deletion Stop/DeleteState steps for pending readmission hosts in
  `readmissionPlanBuilder.appendStep` and `requiresDeletion`. Do not alter
  ordinary surviving-group readmission omissions or metadata admission sequences.
- [x] In marker-1 approval retirement validation, require empty required partitions
  for deletion blocks, count hosted deletion owners independently of planned
  readmission, reject duplicate retirement issuance, preserve exact
  required-to-detached handover, and retain the original binding for conservative
  already-detached Stop entries (target §9.6/§9.15e).
- [x] Update debt preparation to find new Stop and DeleteState actions in
  `block.Cleanup`. Preserve exact historical member/storage/origin and every
  existing receipt. A returned zero identity means no appended row; a nonzero
  actionless row backed by consumed readmission must be retained.
- [x] Reuse `readmissionDebt` only to materialize already-proven complete
  readmission. Missing or partial batch proof cannot become a stop timestamp.
  New pending readmission instead receives ordinary action-bearing debt.
- [x] Let `deletionDebt` retain existing valid debt without waiting for the host
  or consumption of a pending batch. Keep hard identity/storage/provenance errors
  and deterministic traversal/refusal.
- [x] Allow required-to-detached handover for desired group absence, including
  exact old-member debt with a newer approved member. Preserve the old retirement
  action and rely on Task 2's dispatch guard; never deliver the old Stop to a
  replacement identity.
- [x] Wire `validateDetachedCoverage` into `prepareDeletionCompletions` for the
  whole new operation before changing any completion row. Skip surviving groups.
  After validation, reuse `completeApprovalGroup` and aggregate only the existing
  block states.
- [x] Bypass the old receipt-waiting completion path for new approvals; retain it
  where necessary for restored legacy operations. New deletion block success has
  `CurrentStep=0`, `TotalSteps=0`, no blocker and a nil versioned checkpoint.
- [x] Narrow `protectReadmission` / `dependsOnReadmission` only where new ordinary
  detached coverage already owns every relevant identity. Preserve protection
  for genuine legacy omitted-action dependencies.
- [x] Preserve `applyBeginOperation` candidate preparation, full graph validation
  and atomic installation; limit mode-1 deletion resource conflicts to actual
  cleanup bindings as specified in target §9.15e. A failing sibling
  validation must publish neither partial tombstone nor partial cleanup.
- [x] Update generator/approval expectations only now. Separate current-plan
  fixture builders from historical fixture builders; construct legacy retained
  states directly or from immutable artifacts rather than asking marker-1 approval
  to accept an old plan. Explicit unmarked mode-0 command fixtures must still
  replay to their original result.
- [x] Cover a pure deletion with all hosts unavailable; inherited debt only;
  zero-issued creation; issued but unacknowledged founding/preparation; mixed
  operation with one pending sibling; pending readmission; hard refusal despite
  other pending owners; exact approval replay/CAS and canceled waiter semantics.
- [x] Preserve physical assertions: operation success must not populate
  Removed/Stopped/Retired/Collected receipts, settle unresolved host/native
  revocation, permit group ID reuse or authorize a successor's deletion.
- [x] Add concise `TODO(not planned)` comments at the owning boundaries for
  excluded legacy-plan conversion and permanent-host debt waiver, with current
  default behavior as specified in the exclusions table below. Do not add a
  runtime warning or public stub for expected offline cleanup.
- [x] Confirm the helper introduced in Task 2 is now reached from real approval.
  No temporary activation flag, fallback or second planner remains.

#### Verification

- [x] `go test ./topology ./internal/metaengine ./internal/operator -run 'Deletion|Cleanup|Readmission|Retained|Retry|Approval' -count=1`.
- [x] `make test`, `go vet ./topology ./internal/metaengine ./internal/operator`
  and `make lint` pass across the newly activated behavior.
- [x] Run focused functional planning/ledger regressions:
  `make test-function FUNCTIONAL_ARGS="-run 'PlanningDeletion|ScopedPlanningDeletion|DeletesInterruptedFounding|GrantedLoadLedgerCleanup|RollbackApprovedGroupDeletion'"`.
- [x] Inspect actual `BuildPlan -> Approve -> ReadProgress/WaitOperation` results:
  no load-native read, successful deletion with exact unresolved debt, and
  unchanged behavior for staged-only deletion.
- [x] Repair every test affected by the intentional semantics without weakening
  its durable identity/resource ownership assertions; record results and commit.

### Task 4: Verify retained evidence, compatibility and managed lifecycle

**Goal:** prove the existing persistence, readmission and cleanup runtime remains
correct when the operation is already logically complete.

**Files:** owning tests in `internal/metaengine/scoped_deletion*_test.go`,
`scoped_cleanup*_test.go`, `scoped_retention_test.go`,
`scoped_group_completion_test.go`, `ready_fences_test.go`;
`internal/metaservice/*_func_test.go`, `internal/operator/*_func_test.go` and
`internal/managed/*_func_test.go`. Production corrections stay within target §9
unless a concrete gap requires a recorded minimal expansion.

**References:** target §3.3 rollout, §5.1–§5.7 complete persistence/index schemas,
§7.5 restore, §8.1–§8.4 authority and lifecycle, §9.16 existing wiring,
§10.3–§10.12 recovery/failures, §11.2–§11.3 verification, and §13.18–§13.23.
Current [physical fixture provenance](../testing/api-baseline-physical.md).

- [x] Replay pre-change serialized unmarked `begin_operation` commands after a
  pre-deletion snapshot, through retained committed indexes. Cover required-stop
  deletion, inherited detached debt and zero-step pending readmission. Compare
  original expected state, status and subsequent legacy completion/retry; a
  decoder-only test is insufficient.
- [x] Separately replay marker-1 commands to the new logically completed state;
  prove unknown values refuse without state changes and old zero encodings
  remain exact. Mixed-operation retry must preserve successful deletion blocks
  while retrying only failed siblings, without a snapshot/public operation mode.
- [x] Add metadata snapshot/restore and history-pruning regressions after logical
  completion with an unreachable owner. Verify the permanent tombstone,
  historical member, bound actions, source documents and actual timestamps survive.
- [x] Prove incomplete debt roots its evidence even with `GroupProgress.Deleting == false`. Preserve bounded progress pages and complete aggregate operation
  status. Do not pin every historical group document indefinitely.
- [x] Exercise repeated/lost/late effect receipts after completion and
  supersession. Preserve first timestamps and exact tuple/attempt attribution;
  conflicting receipts must refuse without partial mutation.
- [x] Exercise missing, partial, complete and superseded readmission batches.
  Logical deletion remains successful with ordinary debt; only whole-batch proof
  updates stop/retirement, only its exact historical identities, and no ordinary
  RPC reaches the replacement under the predecessor's member ID.
- [x] Exercise a current deleted replica, prepared-only owner and held local
  replica through the actual cleanup ingress. A local hold does not prevent
  approved retirement; foreign/successor storage remains untouched.
- [x] Verify retained startup and startup recapture reject the retired ID before
  handle publication. Reuse `TestStartupRecaptureIgnoresIndependentVersionAndRejectsAssignmentChange`
  and the existing exact authority-floor checks.
- [x] Verify a delayed factory, Apply/replay, failed Close and canceled waiter
  retain ownership for serialized retries. Logical completion cannot release
  borrowed or Node-owned resources before their owners join.
- [x] Verify terminal deletion cleanup through the real controller path:
  `ReadyCleanup -> BeginCleanupEffect -> exact dispatch -> RecordEffects`,
  not just pure predicate tests.
- [x] Hold unreachable native fence work under the normal cleanup budget and
  prove reachable host stops eventually progress using existing rotation/backoff.
  Preserve the distinction between `WaitOperation` and `WaitRevoked`.
- [x] Verify all-host stop evidence activates native settlement but leaves
  outstanding host retirement/collection separate. Reject premature physical
  settlement when any known host remains unproven.
- [x] Exercise the existing collection transaction order: preparation before
  deletion, durable completion before report, exact store marker release before
  journal row removal. Test callback/store failure and retry with original proof.
- [x] Validate both WAL and memory debt completion semantics; do not fabricate
  `CollectedAt` for memory storage.
- [x] Run existing frozen metadata, authority and native restart fixtures as
  read-only originals with private writable copies. Preserve hashes and canonical
  encodings; never regenerate baseline artifacts to satisfy changed expectations.
- [x] Keep historical required-stop operations restorable and executable with
  original retry semantics. New approvals must reject that old shape without
  changing decoding behavior.
- [x] Record any necessary implementation corrections in NOT PLANNED CHANGES and
  rerun affected tests; do not add a new scheduler, watcher, store schema or
  public API as a test convenience.

#### Verification

- [x] `make test` and `make lint`; focused vet for every production package changed
  by a correction.
- [x] `make test-function FUNCTIONAL_ARGS="-run 'Deletion|Cleanup|Collection|Readmission|StartupRecapture|FencedHost|FullStop|Frozen|Restart'"`.
- [x] Check owning functional tests call `t.Parallel()` before acquiring fixtures,
  use joined cleanup and do not introduce multi-node in-process consensus.
- [x] Record case names and fresh report paths. Race execution is reserved for
  Task 7 unless a new concrete concurrency defect requires a focused run now.
- [x] Commit the verified checkpoint.

### Task 5: Add three-node offline, missing-load-quorum and catch-up regressions

**Goal:** verify the selected public behavior across real processes while
preserving independent group availability and exact physical cleanup evidence.

**Files:** update `tests/cluster/delete_wait_test.go`; create or extend
`tests/cluster/delete_recovery_test.go` for additional cases; reuse
`tests/cluster/controls.go`, `authority_quorum_test.go` patterns and
`testapp/hooks_*`. Any new fixture-only controls belong in `tests/cluster/testapp`
with owning unit/functional tests.

**References:** target §1 contract, §3 public semantics, §4 diagram,
§5.6 public cleanup schema, §8.4 runtime, §9.17 routes/configuration,
§10.1–§10.11 flows, §11.4 process scenarios. These tests use public APIs and do
not change library constructors or configuration schemas.

- [x] Rename/rewrite `TestFailureIsolation_DeletionStopProof` as
  `TestFailureIsolation_DeletionWithoutFormerHost`. Kill a non-meta-leader former
  host; require approved deletion and operation success while it remains killed.
- [x] Preserve exact old identity, retained-volume and missing-stop/retirement/
  collection assertions for the unreachable host. Create/use an independent
  group while cleanup is pending, then restart the host and require eventual
  exact cleanup and retired-ID rejection.
- [x] Add `TestFailureIsolation_DeletionWithoutLoadQuorum` with three metadata
  participants alive. Use existing authenticated `SetReadFaults` /
  `ReadFaults{DropRaft:true}` for the selected group and exact process/replica.
  Confirm traffic-drop counters and loss of that group's progress; do not kill
  two metadata voters or break unrelated load groups.
- [x] Approve deletion through surviving metadata quorum without restoring the
  selected load quorum. Verify logical success and independent-group progress,
  then release faults and observe exact cleanup.
- [x] Add `TestFailureIsolation_DeletionSnapshotCatchup`: hold one host offline
  across deletion and actual metadata compaction, restart from retained storage,
  prove snapshot catch-up occurred and the retired replica was never published,
  and require eventual physical cleanup.
- [x] Do not confuse fixture `Spec.SnapshotThreshold` / `-snapshot-threshold`
  with metadata compaction: those currently configure load snapshots only.
  For deterministic small tests, add the test-only `MetaSnapshotThreshold uint64`
  field to `cluster.Spec` and testapp configuration, an optional
  `-meta-snapshot-threshold` flag defaulting to zero, and pass it to the existing
  public `NodeOptions.Meta.SnapshotThreshold`. Zero retains library defaults;
  no public gquorum setting or storage root is added.
- [x] Reuse authenticated metadata-proxy observations or the existing public
  event/metrics surfaces to establish snapshot catch-up and exact process
  incarnation. If an observation is missing, add only the necessary bounded
  counter to the existing fixture proxy/control response, validate its binding,
  and test it. A sent snapshot or a polling gap alone cannot prove installed
  catch-up or absence of transient publication.
- [x] Add `TestFailureIsolation_DeletionDuringReadmission` using existing
  readmission/effect barriers: hold a partial batch, approve deletion and require
  logical completion with no fabricated host proof, then release and verify
  exact retirement. Preserve stable identity and cleanup assertions.
- [x] Ensure every independent case uses `t.Parallel()` and its own pool lease,
  roots, fault controls and cleanup. Use explicit observable barriers rather
  than sleeps as evidence; use three hosts.
- [x] Keep controls authenticated, exact to process/replica and registered only
  in the caller fixture. Reuse existing load/meta listeners; no test-only
  mechanism enters the library or the small example.
- [x] Compile the process suite and run fixture units/local functional checks.
  Defer Docker image build/run to Task 6 after final full lint, avoiding
  intermediate expensive process runs and shared-image replacement.

#### Verification

- [x] `make test` and `make lint`; vet changed fixture packages through
  `go vet ./testapp` from `tests/cluster` if production fixture files changed.
- [x] From `tests/cluster`:
  `go test -tags=clustertest -run '^$' -exec=true ./...` compiles process tests without
  executing their binaries. The exec wrapper is required because TestMain starts
  the Docker pool even when no test names match.
- [x] Run relevant local fixture functional cases if new controls were added:
  `go test -tags=functional -run 'ReadFault|Meta|Deletion|Snapshot' -count=1 ./testapp`
  from `tests/cluster`; confirm selected tests exist and report what actually ran.
- [x] Check all four public process-case names and controls are ready for Task
  6; no Docker success is claimed from compile-only checks. Commit the checkpoint.

### Task 6: Complete penultimate cluster validation

**User scope correction (2026-10-26):** omit feature documentation updates to
README, AGENTS, architecture documents and public comments. The behavior is
obvious to external users. Keep a brief changelog as the explicit exception in
Task 7. Execution records continue to record actual work and verification.

**Goal:** complete full lint and real process verification before the final audit.

**Files:** executable corrections only if verification reveals a concrete defect;
this plan and its progress record. No feature documentation updates.

**References:** target §3–§11 contracts, runtime and process verification.

- [x] Run `make lint-full` before building the current fixture image.
- [x] Run `make test-cluster-build`, record image identity, then run focused
  deletion cluster cases and the complete process suite in this task only.
- [x] Fix every failure, run focused owning checks and rerun affected cluster
  coverage here before the final audit. Rebuild only after an active run finishes.
- [x] Record reports and confirm container, volume and worker cleanup.

#### Verification

- [x] Full lint, image build, focused deletion process cases and full process
  suite pass with fresh reports; no compile-only claim substitutes for execution.
- [x] Run `git diff --check`; executable corrections receive appropriate unit,
  functional and vet checks before committing.
- [x] Commit this verification checkpoint with the execution records.

### Task 7: Final validation and architectural delta verification

**Goal:** compare the complete implementation with every applicable target
component, close all gaps, validate real operation, and confirm production wiring.

**Files:** full source delta from the implementation baseline, all plan task
artifacts and tests, current architecture documents for read-only comparison, this plan; create
`docs/changelogs/20261026-group-deletion-unreachable-owner.md` only after the
implemented behavior has been verified.

**References:** target ALL sections, especially §4 architecture diagram, §5
complete persistence contract, §9 every method/component, §10 every flow/failure,
§11 gates and §12 exclusions. Use §13 as decision-logic cross-check, not a reason
to duplicate existing equivalent helpers.

- [ ] Verify public API and configuration delta against target §3/§9.17:
  signatures, JSON field names, options, transport methods, ports, constructors,
  storage interfaces and error codes are unchanged; existing callers compile
  across all five modules.
- [ ] Verify planner delta against target §5.4/§6/§9.1–§9.5:
  new exact cleanup partitions, inherited-debt deduplication, readmission
  preservation, no load-native read for deletion and unchanged same-host
  replacement barriers.
- [ ] Verify approval/FSM delta against target §5.2–§5.5/§7:
  every possible issued owner is covered before atomic nil-checkpoint success;
  mixed blocks aggregate correctly; CAS/replay/preview/refusal/cancellation
  retain their existing meanings.
- [ ] Verify readmission and authority delta against target §8.2–§8.3:
  ordinary old-member dispatch defers correctly, complete batches settle only
  exact identities, partial proof remains incomplete, and native/host
  revocation is not inferred from logical deletion.
- [ ] Verify cleanup/runtime delta against target §8.1/§8.4/§9.16:
  actual controller queues reach host stop and retirement after operation
  success, callbacks remain owned and joined, retries remain bounded and
  independent, and foreign/successor resources are preserved.
- [ ] Verify database/schema/index/cache delta against target §5:
  bbolt buckets and all included record fields/tags match, metadata snapshot schema 4 and
  journal schema 2 remain, the sole internal command marker matches target §5.8,
  no SQL/ORM/cache/migration is introduced, incomplete
  debt pins complete proof through restart/snapshot/pruning, and no
  index/catalog recursion or cross-product owner/debt scan was added.
- [ ] Verify collection/local storage delta against target §5.6/§8.4/§10.11:
  preparation, actual deletion, durable receipt, report, marker release and
  journal removal keep their established order; WAL/memory behavior stays exact.
- [ ] Verify legacy/compatibility delta against target §3.3:
  old committed commands replay after snapshots with original semantics, old
  immutable plans and their retries retain semantics, and marker-1 approvals use
  the new shape; immutable fixtures/hashes remain unchanged; no silent
  migration, downgrade or mixed-version claim.
- [ ] Verify caller fixture/process delta against target §9.17/§11.4:
  all three-node scenarios exist and use authenticated exact controls, real
  metadata snapshot evidence and observed cleanup; no multi-node root
  functional helper or unregistered control was introduced.
- [ ] Verify all target §10 data flows and failure cases against code and test
  evidence, including missing load quorum, all missing owners, late factory
  success, readmission overlap, independent sibling work and permanently
  unconfirmed physical cleanup.
- [ ] Verify excluded scenarios against the target/exclusions table. Per user
  correction, leave feature documentation/public comments unchanged; write only
  the brief changelog. Implement no excluded feature merely to resolve a TODO.
- [ ] Enter every found mismatch in NOT IMPLEMENTED GAPS with target section,
  affected paths and evidence. Fix it under the target/plan, or record an
  explicit user-approved deferral. Mark any unresolved item `[_]` and do not
  report the plan complete while required gaps remain.
- [ ] After corrections, run the final commands below in the stated order.
  Record non-cluster suite reports and reuse the verified Task 6 cluster evidence.
  Reopen Task 6 if an audit fix requires cluster reruns; do not run them in Task 7.
  Retain pass/fail status, source revision, case names, image and cleanup evidence. Do not count
  historical or compile-only runs as fresh runtime validation.
- [ ] Create the user-facing changelog describing only visible improvements:
  deleting an unused group no longer waits for unavailable former hosts;
  returning hosts clean up automatically; cleanup status remains observable.
  Mention the new-approval scope where relevant. Do not include private method
  names, implementation history, unexecuted claims or task numbers.
- [ ] Finalize the progress log and resolve every NOT IMPLEMENTED GAPS item;
  preserve accurate NOT PLANNED CHANGES and validation records. Record final
  architecture verification and commit the complete task.
- [ ] **Verify every component is wired for actual use as the final subtask:**
  trace public DeleteGroup through staging, Approve, capturePlan/BuildPlan,
  applyBeginOperation and validateDetachedCoverage to the logical checkpoint;
  trace the real operation controller through ReadyCleanup/ReadyFences,
  BeginCleanupEffect, exact host actions and receipts to retirement/collection; trace retained Node startup/readmission through current authority,
  tombstone rejection and existing journals; confirm the existing HTTP fixture
  route and all new test controls are registered, constructors/coordinators are
  the real production instances, and no helper is reachable only from tests.

#### Verification

The final task requires the final validation commands, the component-by-component
audit above and a green build. Perform all gap fixes and required reruns before
checking the final wiring item. Remaining unresolved required gaps or failed
checks mean incomplete work, regardless of elapsed time or passing unit tests.

## Validation Commands

Commands run from the repository root unless another directory is stated.
Preserve quoted expressions when passing through Make.

### During each code task

~~~sh
make test
make lint
git diff --check
~~~

Run focused package tests/vet listed in each task; `make test` compiles all five
application modules and runs uncached units. No full Docker/full lint/full race
loop is required between intermediate tasks.

### Final non-cluster checkpoint: Tasks 6–7

First close architectural gaps, then run:

~~~sh
make test
make test-unit-race
make test-function
make test-function-race FUNCTIONAL_ARGS="-run 'Deletion|Cleanup|Collection|Readmission|StartupRecapture|FencedHost|FullStop|Frozen|Restart'"
make lint-full
~~~

In Task 6, run full lint once on the finished code before Docker. If a later concrete fix
changes code, rerun its affected checks and required gates; do not repeat broad
suites after documentation-only edits.

In Task 6 only, build the current fixture image once, then run focused and full process coverage
without replacing that image during either run:

~~~sh
make test-cluster-build
make test-cluster-run CLUSTER_ARGS='-v -run TestFailureIsolation_Deletion'
make test-cluster-run
~~~

The full process suite is justified by changes to shared approval, readmission,
retention and physical-authority rules. It runs once at the end, not per task.
Record image identity and verified container/volume/worker cleanup. If this suite
reveals a fixture change, rebuild after the current run finishes before retesting.

Do not run performance/benchmarks, Gosim or separate chaos campaigns for this
feature. The internal JSON command marker requires codec/replay checks, not protobuf
generation; no protobuf schema changes or generated-file edits are planned. Compose demo execution is not required because its meta-only integration
and interfaces are unchanged; all module compile/unit checks still run.

Read the new `test-reports/<suite>/.../report.md`, `timeline.md` and failing
package logs. Preserve failure exit codes. Record unavailable environment
prerequisites as unverified scope; do not silently drop a gate.

### Documentation/plan validation

~~~sh
git diff --check
make -n test-function FUNCTIONAL_ARGS="-run 'Deletion|Cleanup|Readmission|StartupRecapture'"
~~~

Also check relative plan/target/log links, numeric target section references,
existing source paths and method names, contiguous tasks, unchecked implementation
boxes, per-task verification blocks, the final wiring subtask and changelog path.
Since documentation directories are ignored, inspect their file contents directly
as well as staged diffs.

## Target coverage map

| Target contract | Implementation tasks | Final audit |
| --- | --- | --- |
| §1–§3 deletion semantics, API, legacy scope | 1, 3, 4, 6 | Task 7 public/FSM/compatibility |
| §4 architecture and ownership diagram | 2–5 | Task 7 runtime and final wiring |
| §5.1–§5.8 schemas, command replay, indexes and persistence | 2–4 | Task 7 database/collection |
| §6 planner and readmission plan rewrite | 1, 3 | Task 7 planner |
| §7 approval, exact coverage and restore | 2–4 | Task 7 FSM/durability |
| §8.1 cleanup dispatch | 2–5 | Task 7 runtime |
| §8.2 readmission priority | 2–5 | Task 7 readmission |
| §8.3 physical fencing | 2–5 | Task 7 authority |
| §8.4 startup and storage | 4–5 | Task 7 runtime/collection |
| §9.1–§9.15c changed/new methods | 1–3 | Task 7 signatures and final wiring |
| §9.16–§9.17 reused components/routes/config | 3–5 | Task 7 public/fixture/final wiring |
| §10.1–§10.12 all flows and failures | 3–5 | Task 7 flow matrix |
| §11 all required verification categories | 1–5, 7 | Task 7 fresh validation evidence |
| §12 docs and exclusions | 3, 6 | Task 7 documentation/scope |
| §13.1–§13.24 algorithms | 1–4 | Task 7 decision-logic review |

## NOT CONSIDERED & TODO

These exclusions are carried forward from the user-selected target, not new
requests for additional scope. Prefer a concise comment at the owning boundary;
do not add public stubs or noisy WARN logs for deliberate default behavior.

| Item | Tag | Required default/comment |
| --- | --- | --- |
| Convert already-approved legacy deletion plans | [postponed] | Preserve immutable plans/retry semantics. At new-approval versus legacy-completion boundary: `// TODO(not planned): migrate previously approved required-stop deletion plans.` No migration endpoint or automatic rewrite. |
| Permanently waive unconfirmed host cleanup | [potential-improvement] | Retain debt indefinitely. Replace the old deletion-blocking TODO with `// TODO(not planned): retire unconfirmed cleanup through an explicit permanent-host declaration.` |
| Global immediate cessation of serving | [potential-improvement] | Keep independent load serving and eventual stop. Document at deletion completion: `// TODO(not planned): provide a global serving-cutoff barrier for logical group deletion.` No lease, new barrier option or metadata check on ordinary requests. |
| External side effects and infrastructure disposal | [postponed] | Existing application responsibility. Add one relevant retirement-boundary comment if needed: `// TODO(not planned): coordinate external transactions or infrastructure disposal.` No new callbacks. |
| Mixed-version operation/downgrade | [postponed] | Existing unsupported deployment policy. At schema/compatibility documentation boundary: `// TODO(not planned): negotiate mixed-version deletion semantics or downgrade new tombstones.` Do not change version numbers or add a negotiation stub. |

The task does not port TiKV's merge protocol, raft-rs, PD APIs or store-GC
acknowledgement representations. Those sources supplied rationale; existing
gquorum identity and receipt contracts remain the implementation foundation.

## NOT IMPLEMENTED GAPS

No implementation has been attempted by this planning session.

During execution record each gap as:

- `[ ] <target section; affected files; missing behavior; evidence; owning task>`
- `[x] <resolution and fresh verification evidence>` when fixed.
- `[_] <unresolved gap and reason>` when it remains unfixed; this prevents a
  completion claim unless the user explicitly approves deferral.

Do not use an out-of-scope label to hide a required target feature.

## NOT PLANNED CHANGES

Task 6 implementation adjustments (reviewable in its commit):
- scoped_revocation_scope.go and scoped_approval_retirement.go: index exact
  cleanup/action bindings once instead of scanning all debt per new deletion
  action. The same authorization predicates and historical replay results hold.
- Deletion process fixtures now explicitly enable immediate retention when they
  assert CollectedAt; default retention intentionally leaves retired storage.
- failure_replay_test.go and authority_host_test.go now use required same-host
  replacement stops for failed-attempt retry/revocation coverage. Whole deletion
  cannot supply a failed required stop under the new semantics. Original CAS,
  immutable retry, exact fence and successor-preservation assertions remain.
- execution_test.go now checks successful logical deletion with refused physical
  cleanup, exact data retention, automatic retry after process return, and an
  unchanged state-file digest in quarantine. No new operation is needed to retry
  cleanup. resources_test.go waits for physical retirement after engine release.
- authority_quorum_test.go waits for a successful exact read after restoring
  connectivity; a confirmed fence alone does not prove follower read readiness.
  Initial full-suite evidence exposed a transient unavailable response.

Task 5 implementation adjustments (reviewable in its commit):
- Added fixture-only hooks_readmission_report.go because existing action/fence
  barriers cannot hold a locally produced readmission receipt batch across a
  process restart. The authenticated existing metadata proxy forwards the exact
  load receipts while withholding the metadata receipt, bound to admission
  node/member/operation and the first authenticated incarnation. Release forwards
  the original full batch. No library API, schema or listener was added.
- Added bounded accepted-snapshot boundary observations to the existing proxy.
  Process verification also requires the same incarnation's applied index to
  reach that boundary and zero load factories since restart; a send alone is
  never counted as installed catch-up.
- User removed Task 6/7 feature documentation updates, keeping only a brief
  changelog exception. The validation and final wiring audit remain required.

Task 4 implementation adjustments (reviewable in its commit):
- cleanup_debt_bench_func_test.go: its functional query regression builds current
  plans; select mode 1 and record a real detached Stop before scaling retirement
  debt. The initial focused functional run exposed the obsolete mode-0 pairing.
- New testdata/deletion-replay captures came from the unchanged 8e57798 worktree;
  no existing frozen fixture was regenerated or edited. Complete resulting
  snapshots match byte-for-byte, then legacy failure/retry retains original plans.

Task 3 implementation adjustments (reviewable in its commit):
- scoped_approval_retirement.go: use a planningCollector receiver to meet the
  four-argument lint rule; validation behavior and internal semantics stay explicit.
- scoped_cleanup_handover.go and scoped_revocation_scope.go: a pending readmission
  regression exposed conservative redundant planner stops. Keep their original
  detached debt/action binding and derive mode-1 deletion conflicts only from
  actually bound actions, preserving readmission floors. Thread the marker through
  scoped_begin/revocation; repair/retry keep legacy selection. Target §9.15e records
  this necessary authority correction; old command interpretation is unchanged.
- replicas_func_test.go: replace old required-stop fixture execution with actual
  deferred Stop grants/receipts before ledger retirement assertions.

Task 2 implementation adjustments (reviewable in its commit):
- internal/metaengine/deletion_semantics.go: enum declaration moved to a companion
  file because revive limits public declarations per file. Optional command field
  remains in operation_proposals.go; no semantic/API change.
- scoped_deletion.go and scoped_deletion_detached.go: restore reuses exact coverage
  validation and recovers known former owners from retained plan-source
  declarations. A corruption regression proved that a nil checkpoint plus a
  deleted debt row could otherwise hide a previously proven owner. This also
  gives the pure helper a real restore callsite before approval activation.
- Validation helpers were factored to meet complexity/line-length lint limits;
  no new state, transport, or worker was introduced.


Planning found and corrected the historical command-replay gap described above:
target §5.8 adds one internal semantics marker and mode propagation. This is the
only design correction; public API and snapshot/journal shapes remain unchanged.

Record necessary execution deviations with the
file, reason, target impact and relevant diff/commit. A design change requires
reconciliation with the target rather than an undocumented alternate algorithm.

## Progress log

| Date | Task | Work/result | Validation |
| --- | --- | --- | --- |
| 2026-10-26 | Task 3 | Activated detached deletion with exact retained debt and inherited readmission authority. | Unit 2349/2349; focused functional 10/10; focused regressions/vet/lint PASS. |
| 2026-10-26 | Task 2 | Internal command marker, exact coverage/restore validation and physical settlement support. | Focused tests/vet/lint PASS; unit report 20261026T132448.610357Z-il0ggigv: 2342/2342. |
| 2026-10-26 | Task 1 | Plan validators accept exact deferred deletion and inherited debt; generator selection remains unchanged. | Focused tests/vet/lint PASS; unit report 20261026T130516.077623Z-9xhac48t: 2323/2323. |
| 2026-10-26 | Planning | Read target, current contracts, planner/FSM and fixture seams; defined seven green checkpoints and the Task 3 activation boundary. | Documentation/source-reference checks only; no implementation suite executed. |

## Planning verification notes

- The target has one documented compatibility correction: deterministic replay
  uses the internal approval-command semantics marker. This plan otherwise
  supplies execution order and fixture realization within its stated contracts.
- Task 1 needs a nonnil-group guard while the old stop predicate remains selected;
  otherwise merely removing the explicit deletion rejection still rejects the
  new shape as a replacement. Task 3 then installs the final shared predicate.
- Task 2 separates logical tombstones from physical settlement before approval
  can complete early; this prevents accidental `WaitRevoked` success.
- Legacy tests must not submit historical shapes with marker 1 once Task 3
  selects new semantics. Unmarked historical commands must still replay. Preserve immutable originals and separate fixture
  construction paths.
- Existing test snapshot threshold configures load groups, not metadata.
  Task 5 uses the existing public metadata option through fixture-only wiring
  and requires real snapshot/catch-up evidence.
- Compile-only process validation uses -exec=true: TestMain starts Docker even
  when -run selects no tests. No compile-only success is counted as runtime proof.
- Optional fixture observations are bounded and authenticated. No private
  library API or additional listener is permitted.
- Task 7 is last, audits each subsystem, fixes recorded gaps, creates the public
  changelog, and ends with real production/route/DI wiring verification.
