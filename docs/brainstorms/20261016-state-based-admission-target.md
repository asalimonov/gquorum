# Target amendment: state-based admission barriers

Status: **accepted peer consensus; implementation authorized by the user on 2026-10-16**.
Date: 2026-10-16. Code inspected at acceptance: `d2b5a67`. Current implementation progress is recorded in the plan and progress log.

Amends the [failure-isolation target](20261015-failure-isolation-and-operational-api-target.md)
after [peer thread 10](../review/feat_failure-isolation-and-api.md#thread-10-discussion-carry-admission-dependent-scopes-into-the-existing-planner).
The user authorized adopting consensus without further approval and completing
the goal. This amendment is normative with the main target. Task 26 is the first
unimplemented task at acceptance. This document specifies the replacement contract,
source-level qualifications and implementation impact.

Search tags: `FI_ADMISSION_STATE`, `FI_ADMISSION_ORIGIN`, `FI_ADMISSION_RETENTION`,
`FI_SCOPE_COMPLETE`.

## Decision and observable behavior

An unchanged load block keeps its operation owner when a metadata admission
operation is superseded. It waits for durable admission proof for its exact host
identity. A later metadata operation can supply that proof. Changing the host's
member identity, placement eligibility or load assignment still affects the load
scope and follows normal supersession, fencing and readmission rules.

| Case | Existing target | Proposed target |
| --- | --- | --- |
| Metadata preparation consumed; metadata owner changes | Keep load owner and consumed prerequisite | Keep load owner and exact admission proof |
| Preparation unconsumed; metadata owner changes | Supersede/replan dependent load blocks | Keep load owner; wait for preparation by the successor |
| New load-only approval uses a node being admitted by an older operation | Same-plan step edges cannot express this dependency | Approve the load block; wait for that node/member's proof |
| Member replaced or removed | Affected load ownership changes | Same exact-identity safeguards |

Example: v8 admits n4/member 4 and owns B. v9 changes only metadata intent while
keeping n4/4 assigned to B. B remains owned by v8; its preparation becomes runnable
when either v8's retained proof or v9's preparation proves n4/4 ready. A failed
metadata operation can still require intervention. The guarantee is that changing
the operation ID alone cannot strand a valid dependency.

The alternative is `PlanInput.DependentGroups` plus FSM-derived invalidation
closure. It carries more evidence through planning and still needs an additional
rule for dependencies across operations. We recommend the state predicate:
ordinary APIs and `PlanInput` keep their current shapes. One additional field is
confined to the internal admission record.

## Admission predicate (`FI_ADMISSION_STATE`)

The FSM derives this requirement for every load `PrepareReplica` target and every
founding voter host in `CreateFoundingGroup`, whether or not the immutable plan
contains an explicit admission dependency. Founding learners have their own
preparation steps. Evacuation, stop and cleanup do not require the removed host to
remain admitted.

For each affected host:

1. Resolve cluster/node/member and exact load replica from the operation's pinned
   approved document and grant target. Require its current approved assignment,
   host eligibility and member binding to remain valid. Validate the current load
   owner and complete action tuple as usual.
2. Accept either validated `_meta` convergence containing that exact node/member,
   including validated founding seed convergence, or an approved retained `_meta`
   `PrepareReplica` grant whose exact required host receipts are all consumed.
3. Preserve process authentication, complete readmission retirement, resource
   floors and joined-owner requirements. Historical consumption proves a fact;
   it does not reopen the historical grant for execution.

`AdmissionReady`, grant issuance, cached health and native membership alone are
insufficient. Readiness can precede the receipt, and founding may have no
preparation receipt. Convergence is a durable proof checkpoint, not a health test.

Go-shaped algorithm; helper names below specify private behavior, not new APIs:

```go
func loadAdmissionSatisfiedLocked(op, action) bool {
    for _, target := range preparationTargets(op.PinnedDocument, action) {
        if !matchesCurrentApprovedBinding(op, action, target) { return false }
        if exactMetaConvergenceContains(target) { continue }
        if !exactApprovedMetaPreparationConsumed(target) { return false }
    }
    return true
}
```

Recheck through the same predicate at dispatch, grant issuance and execution
authorization, including after waits. An unmet prerequisite returns the existing
`ErrUnavailable` and leaves the load block waiting. A stale owner, foreign
identity or invalid action remains an authorization failure. No public error type
or caller-selected dependency mechanism is added.

**Source qualification:** `authorityTargetLocked` currently searches retained
documents and grants. Its result alone does not prove a *current* approved
binding. The new predicate must explicitly compare against Target and the exact
approved load assignment; history fallback remains useful for retirement.

## Scope and plan rules (`FI_SCOPE_COMPLETE`)

The required block set equals semantic ownership scopes, including `_meta` for
node changes. Remove admission-dependent invalidation closure. Unassigned repair
candidate reads remain separate independence evidence, as agreed in Correction 11.
New-plan native collection covers affected groups; proof lookup for admission is
an FSM state read and does not add an unchanged load group to that set.

Within-plan admission edges may remain deterministic ordering/explanation data.
They are neither the sole source of the admission gate nor an extra requirement
that the original prerequisite block succeed after it has been superseded.
Removal continues to use the existing state-based evacuation predicate. Preserve
the restricted edge shapes, immutable step identities and retry plan equality.

The FSM independently derives scopes and rejects omitted, duplicate or unjustified
extra blocks. Required deletion/replacement stops and resource conflicts remain
mandatory. Scoped helpers stay isolated from legacy `Active = Target` completion
until complete activation; no authority-mode flag is introduced.

## Admission handover and origin (`FI_ADMISSION_ORIGIN`)

Admission records are keyed by `(NodeName, InstanceID)`. Creating a second record
for the same process is invalid. Retry and same-member `_meta` supersession must
therefore rebind the existing live record atomically with ownership installation.

Add one FSM-assigned field to internal `AdmissionRecord` and its persistence/join
representation:

```go
ReadmissionOperatorID topology.OperationID `json:"readmission_operator_id,omitempty"`
```

The field follows the current internal `OperatorID`/`ReadmissionInstanceID`
vocabulary; `OperationID` is the target identifier type. It is not a Node/Cluster
API parameter. Empty means the record's current operator is also its origin.
On first handover, save the previous operator before assigning the successor;
later handovers preserve that saved origin. The existing instance-origin field
independently preserves the original process when an allowed pending-process
replacement occurs.

Keep the existing length-prefixed `ReadmissionID` encoding, using the origin
operator and origin instance:

```text
originOperator = ReadmissionOperatorID, falling back to OperatorID
originInstance = ReadmissionInstanceID, falling back to InstanceID
ReadmissionID = existing encoding(originOperator, MemberID, NodeName, originInstance)
```

Handover must:

- Validate the exact current owner, same node/member/process and successor's
  pinned approved admission work before mutating the row. Fresh admission retains
  learner-first validation. A resumed admission validates its consumed prefix
  and remaining approved work together.
- Preserve original grant/action/receipt attribution and the complete retirement
  batch. Current operation ownership changes; historical evidence does not.
- Recompute admission milestones from exact retained evidence. Retirement alone
  cannot imply AddLearner or promotion. A consumed preparation does not imply
  native membership. Readiness remains tied to the authenticated current process.
- Install handover, scoped revocations, new owners and required-stop rebinding in
  one validated FSM transition. Relevant fences still precede new authority.
- Keep ongoing retirement for the same granted replacement usable under its
  exact retained proof. Do not cancel it merely because its operation ID changed,
  revive a fenced grant or allocate another replacement identity. Late historical
  receipts may settle their own batch without authorizing the historical owner.

**Already-native case:** a successor may have only a promotion or zero membership
steps: `ensureLearner` skips Prepare/Add when the member is already native. Requiring
a complete fresh learner-first sequence in every successor would strand this
case. Validate the approved origin/consumed prefix plus the successor's suffix;
use exact retained effects or convergence. Do not fabricate a new receipt or
force repeated membership changes. `Previous` still means an identical-plan
retry; never assign it across different approval versions to bypass validation.

Restore and join validation must preserve both origin dimensions, reject a
caller-selected or unsupported origin, and validate the retained approved
identity/batch rather than accepting the new field as proof. Compare resolved
origin operators when checking an earlier process record. Keep one record per
process. Handle queries and late receipts through the recorded origin, while
current readiness/issuance requires current ownership.

Member changes and process replacement continue to follow their existing rules;
handover grants no permission to reuse an established member after data loss.

## Retention (`FI_ADMISSION_RETENTION`)

Keep the consumed preparation, approved identity and readmission provenance while
the current approved member lacks equivalent `_meta` convergence. This includes
members with no current load dependent: a later load-only approval may need the
proof. Keep origin operations/documents and complete batches required by live
admissions and retained effects, using the existing proof-closure machinery.

Equivalent validated convergence may replace this admission-barrier proof root.
Other restore, retry, cleanup or ownership roots still prevent pruning. Without
convergence, release the barrier root only after that member is no longer approved
and no retained/approved load assignment or immutable plan references it; also
respect every other independent root. No barrier ledger, durable ready boolean
or new history-cap admission restriction is introduced.

## Implementation impact

| Existing target / task | Required amendment |
| --- | --- |
| Target §2, §3.3, §5.1–5.2, §12 | Replace admission invalidation closure with the exact state predicate; fix approval and barrier snippets. |
| Target §5.4, §6.1, §12 retry | Specify same-record admission handover, stable origin and resumed prefixes. |
| Tasks 26–27 | Plan/collect semantic ownership scopes; remove dependent-closure input; retain scope CAS and separate repair reads. |
| Tasks 28–34 | Keep restricted barriers; block set equals affected scopes; atomic same-member admission handover accompanies metadata takeover. |
| Tasks 37–38, 43 | Preserve exact retirement authority through handover; share the waiting predicate and handover path with retries. |
| Tasks 49–51 | Founding proof, non-founding binding, prefix validation, origin persistence/restore/join and retention roots. |
| Controller/verification tasks, final audit | Waiting classification, after-wait checks, end-to-end dependency and handover cases; remove obsolete closure expectations. |

Split admission work into bounded tasks before coding if the 550-line ceiling
requires it. Introduce the complete shared handover validation before any new
approval/retry caller becomes active. Update active plan references as implementation proceeds;
no task checkbox is completed by this design amendment.

## Required regressions and inspection evidence

| Regression | Required result |
| --- | --- |
| Metadata owner changes before/after preparation consumption | B keeps its owner; waits/runs according to exact proof. |
| Load-only approval depends on older admission | No additional `_meta` ownership; preparation waits safely. |
| Founding and established member after eligible proof compaction | Exact convergence supplies admission proof. |
| Ready-only, issued-only, wrong node/member/cluster, stale convergence after reseat/exclusion | Refuse admission of the load effect. |
| Retry/supersession before Prepare, after Prepare and after AddLearner | One process record; stable ReadmissionID; no repeated proven membership effect. |
| Pending-process replacement interleaved with owner handover; late response | Stable origin dimensions; only current owner/process gains readiness. |
| Origin/batch corrupted or omitted on restore | Atomic refusal, with no partially installed graph. |
| No current dependent; prune; later load-only approval | Preparation proof survives until equivalent convergence or valid release. |
| Abandoned admission with retained load reference | Proof stays pinned until all applicable roots release it. |
| Evacuation/stop/cleanup after member removal | No admission gate prevents required retirement. |

Inspected sources: [dependency validation](../../topology/plan_blocks.go),
[native learner planning](../../topology/voter_moves.go),
[admission records/binding](../../internal/metaengine/admission.go),
[ReadmissionID and receipt handling](../../internal/metaengine/authority_readmission.go),
[origin restore checks](../../internal/metaengine/metadata.go),
[scoped milestone validation](../../internal/metaengine/scoped_admission.go),
[record uniqueness](../../internal/metaengine/scoped_state_sort.go),
[authority target lookup](../../internal/metaengine/authority_issue.go),
[metadata AddLearner dispatch](../../internal/metaservice/effects.go),
[join validation](../../internal/metaservice/join_checks.go) and
[current ownership retention](../../internal/metaengine/cleanup_evidence.go).
These are proposed regressions, not results from a new test run.

## At-capacity metadata replacement (`FI_META_SURGE`)

Peer thread 11 identified a cycle between admission and evacuation when metadata
removed an outgoing host before preparing its replacement. Plan preparation and
AddLearner for incoming metadata hosts before any decommission removal. Existing
paired catch-up waits, leadership transfer, promotions and live-quorum/placement
checks remain mandatory. Desired learners do not need a standalone promotion wait.

Final documents and metadata voter counts remain bounded by MaxNodes. Native
membership may temporarily contain MaxNodes plus exact still-native identities
whose node has no desired metadata member and whose exact removal is in the plan.
Recompute the allowance from each simulated membership state. It covers learners while
load replicas evacuate; it contracts when those outgoing identities leave.
Construction and remaining-plan validation apply the same bound. A same-node
readmission has a desired replacement member and receives no decommission
allowance. Unknown identities never supply capacity. No public capacity setting
or authority mode is added.

For a load-only successor during an in-progress readmission, the approved exact
replacement binding selects readmission ownership of predecessor retirement.
It is not consumption proof. Load preparation still waits for exact admission;
deletion and zero-step completion independently require consumed readmission
retirement for every omitted former identity. Required-stop handover must not
convert that obligation into a call to the retired predecessor process.

A replace-and-move approval waits for the incoming host to prepare and join as a
learner before decommission removal. If that host never joins, the old member
stays configured; removal-only approvals remain possible and unaffected. An
operator wanting removal first can approve removal and addition separately.
At the voter cap, the existing guarded remove-then-promote pair remains. If the
outgoing voter is dead, metadata has no spare voter fault tolerance while load
evacuation catches up. Demote/promote before final evacuation-gated removal is a
separate availability improvement, deferred rather than silently changing the
approved demotion/adjacency contract in this correction.

For origin validation, Pending handover without any issued fact requires only
structural origin eligibility and the FSM-assigned write-once field. When issued
facts exist, original readmission AdmissionID must match the resolved origin;
the origin cannot follow the earliest retained prefix fact. Prefix operations
must be ordered between origin and current owner with the same approved member
and valid admission work. Facts issued by the current owner are suffix evidence.
Do not invent a receipt for a pending handover or add another provenance ledger.

## Admission role history and completion (`FI_ADMISSION_ROLE`)

Peer thread 12 clarifies that historical promotion and current native role are
separate evidence. Ordered exact-member Prepare/Add/Promote/Demote issuers must
not precede the origin. Validate each version's suffix against its pre-plan role;
immutable retries use that same role. A native voter targeting learner needs
exactly one Demote; targeting voter allows only leadership transfer. A learner
needs the existing admission/promotion grammar with only proven prefixes omitted.

Initialize role once from an exact validated metadata checkpoint. Older consumed
facts retain historical milestones but cannot overwrite its role; only newer
consumed effects change role. A checkpoint without this member supplies neither
role nor a version filter. Covered terminal plans still need retained same-member
bindings and ordered provenance, while convergence replaces their prefix proof.
A live admission cannot have convergence at or after its origin version: metadata
completion, including zero-step approval, completes its admissions atomically.
Completed admissions still require a succeeded metadata block. Approval handover
uses source checkpoints, never the candidate's prospective convergence.

## Deletion and pending readmission (`FI_DELETION_BATCH`)

Thread 13 consensus: retire a deleted group ID at approval, including before its
first convergence. Pending deletion preserves its existing checkpoint: source
zero with nil declaration means unknown, while nonzero source with nil declaration
and no Deleting flag means a completed tombstone. Approved intent alone creates
no host obligation; issued founding/preparation, native membership, proven
checkpoints and existing debt supply exact former identities. Retain their
approved storage/member assignments. Fence late founding/preparation before Stop.

Ordinary deletion debt is created at approval with the deletion operation's exact
required Stop and deferred DeleteGroupState, original member/storage, and no
invented native removal. Existing debt remains original except required-stop
handover. A new deletion may omit a readmission-owned host only once an issued
batch targets that exact former node/member/replica and is usable or consumed.
Every resource must retain its open current-floor grant or be consumed. Missing
issuance returns Unavailable naming the node/former member and “readmission
authority not yet issued”; retry approval of the same staged version. Closed or
displaced unconsumed authority returns AuthorityFenced. Foreign proof refuses.
Preview may describe the plan before issuance. Restore rejects missing coverage.

Pending consumption keeps deletion pending, including a zero-step block;
execution reports an awaiting-retirement reason and rechecks on receipts. An
approval may not cancel or fence its depended-on unconsumed batch. Existing
required-stop handover still needs consumed proof before dropping its binding.
Completion uses the whole-batch predicate from restore, matched to the former
identity even when a later replacement member supplied the proof. Atomically
install the tombstone and missing actionless debts with the consuming batch's
original admission operation, ReadmissionKey, and exact Stopped/Retired receipt.
Reuse existing exact debts; never manufacture completion from partial receipts.

Scoped readmission/admission issuance checks current exact approved member and
metadata ownership/handover, not equality with the latest whole Target version.
An unrelated approval must not prevent a batch from being issued. This preserves
the approval-time batch prerequisite without adding a provenance entity or API.
