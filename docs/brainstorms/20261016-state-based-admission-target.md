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
| Tasks 38–39, 44 | Preserve exact retirement authority through handover; share the waiting predicate and handover path with retries. |
| Tasks 50–52 | Founding proof, non-founding binding, prefix validation, origin persistence/restore/join and retention roots. |
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

## Stop rebinding and complete deletion coverage (`FI_STOP_BINDING`)

Thread 14 consensus supersedes the earlier blanket statement that detached stops
always remain at their original approval. Original debt OperationID, member,
storage, Removed and RetireAction remain immutable; an unconsumed StopAction may
move to an exact successor action when its execution requirement changes.

| Current Stop binding | Successor Stop | Result |
| --- | --- | --- |
| Required | Required | Rebind to exact successor |
| Required | Cleanup, with no same-host reuse or deletion | Rebind as detached |
| Required | Cleanup while reuse/deletion still requires it | Refuse |
| Required | Omitted | Refuse unless exact readmission consumption covers it |
| Detached | Required | Rebind as required |
| Detached | Cleanup | Refuse duplicate; retain existing detached binding |

For planning, RetainedCleanup plus requiredRetainedStop(Approved.Group, identity)
and the successor's desired placement identifies the required→detached case.
There is no new public input field. The FSM independently checks the debt's actual
binding and the successor's exact partition. Native exclusion/consumed removal
must already hold; otherwise ordinary guarded removal precedes Stop.

Restore validates the current Stop action in its bound operation, including a
later approval's Cleanup partition. The original retirement action and Removed
receipt still validate against the origin. All unconsumed Stop rebindings settle
or close the old conflicting grant before successor issuance. Thread 15 refines
consumption: the first exact Stop receipt, including a late historical binding,
supplies the identity-level Stopped milestone with its original tuple/time. Later
receipts cannot rewrite it. Cleanup authorization still resolves current StopAction.
Completion and retry-prefix selection use this same exact identity/member proof.
A newly issued Stop for that identical retired target cannot block completion;
a live executor still settles it before later same-host preparation. No second
proof representation or already-satisfied API flag is added.

An existing unstopped debt never excuses an omitted deletion stop. If the current
deletion block (or ownerless failed tip) has no exact required Stop, the former
identity needs an issued usable/consumed readmission batch at approval and restore,
and that batch remains protected against cancellation until consumption. This
applies equally to debts with detached and required bindings. Pending completion
cannot hide a hard error on another identity: replicated preparation scans in a
deterministic order and hard refusal outranks Unavailable. Completed-debt pruning
must also preserve restore when another root still pins former-host declarations.


## Receipt installation and former-owner derivation (thread 15)

RecordEffect/RecordReadmissionReceipts carry private At with omitzero encoding.
The proposing service stamps its clock; the FSM clamps receipt facts against
retained debt/operation times, never reads a clock, and preserves first milestone
timestamps on replay. The last batch receipt atomically completes an eligible
waiting zero-step deletion. Controllers use max(now, observed UpdatedAt) for
transitions and re-observe after a concurrent clock refusal; a far-future proposer
clock can delay progress until that bound is observed.

Readmission derives the former owner from converged bindings, issued Prepare/Create
grants and unretired debt, excluding members whose complete retirement batch is
consumed. An approved but never prepared intermediate member cannot hide the actual
old owner. The existing single-former-member protocol remains: one candidate must
match PreviousMemberID; multiple unretired owners refuse as Unavailable. The
admission-before-preparation invariant prevents that contradictory state in normal
execution. If no possible owner remains, the latest former approved member supplies
exact pin/absence reconciliation. Retained batch retries use their immutable original
targets. Keep these proof roots while an admission depends on them.

Approve replay compares the complete immutable Plan (nil/empty-equivalent value
equality); differing plans return VersionConflict with the first OperationID.
Captured Scopes are first-approval CAS preconditions. Executor transitions cannot
invent Superseded. Closed native grants without receipts leave zero-step membership
explicitly blocked; a fence never proves nonexecution. Abandoned host preparation
is ignored only in membership reconstruction when its identity is omitted. The
existing planner still emits a guarded no-op Remove before creating cleanup debt.

### Node and receipt isolation precision (peer thread16)

`FI_NODE_DEPENDENCY`: node convergence depends on exact member changes and the
node's effective placement/eligibility change for groups naming it in checkpoint
or Target. Derive this from state, not the latest metadata operation's plan;
metadata supersession cannot discard an older drain's pending load prerequisite.
An unrelated descriptive node edit does not wait for unrelated load work.

Zero-step declarations complete at approval only with exact membership/admission
proof; otherwise they remain waiting (`membership_unproven`, or the applicable
admission blocker). Zero-step deletion can wait for `readmission_retirement`.
Host-only preparation omitted from desired membership is not native uncertainty.
The planner's former load checkpoint boundary and issuance's possibly-hosted
former-member derivation are distinct checks; neither substitutes a newer meta
checkpoint for unresolved old load ownership.

Validated receipts trigger opportunistic deletion completion only for touched
debts or groups of their exact readmission batch. Failure of that completion
leaves a scoped diagnostic and preserves the receipt facts. Full candidate/restore
graph validation remains mandatory; this rule does not admit contradictory proof.

### Mutation provenance precision (Task48)

Typed intent records use ExpectedVersion as the acceptance CAS parent and Version
as its immediate successor. RollbackTo preserves the existing document
ParentVersion historical-source lineage; its requested source version also stays
in the fingerprint. Restore must not mistake that lineage for the CAS parent.
Original approval lookup never substitutes a retained retry when the first
attempt's evidence is missing. Pruning removes complete attempt chains together.

Explicit reseat records the same provenance inside its atomic repair proposal.
Its document ParentVersion is the approved repair source, while ExpectedVersion
is the captured Latest acceptance CAS. Retained replay returns R and its first
operation, without allocating another R/S pair or approving staged S.


### Founding metadata proof (peer thread 17, Task 50)

`FI_FOUNDING_SEED`: initial v1 approval has empty approved/proven state and no
retained effect/admission/cleanup evidence. The metadata engine's owned native
configuration at the application entry must equal the replicated founding
voters/learners, including any prior configuration entry in the same Ready.
Divergent local seeds halt with an Apply error; they cannot select different
refusal or completion results. Only the zero-step `_meta` block completes from
that assertion. Load blocks retain their own effect requirements. Admission
uses that retained successful block and the exact current approved member,
independently of newer Target versions, aggregate failure and load-dependent
node progress. Native/current-term service checks remain. Snapshot restore uses
the persisted checkpoint, not a new seed assertion or an AfterApply cache.

### First session after a retained restart (peer thread 18, Task 51)

`FI_SESSION_ADMISSION`: completed admission belongs to the exact durable
(node name, member ID), while session exclusivity belongs to the process lock.
A retained restart before its first session uses the old process's completed
admission after checking approved/proven metadata membership. It does not bind
the seed again. Local pin/startup and authenticated join checks still prohibit
claiming an established member after disk loss. A late unbound founder may bind
the retained v1 metadata proof after unrelated approvals; service and FSM must
apply the same predicate, including during the schema-3 transition.

### Proof retention precision (peer thread 19, Task 52)

`CLEANUP_PROOF`: select current documents, bounded recent history, live work and
unresolved obligations before traversing dependencies. Retained versions keep
whole retry chains and all required consumption, including successful siblings.
Completed cleanup is retained only when this independently selected graph names
its identity; repeat dependency closure to a fixed point. Pending deletion keeps
its full former inventory. Required rebase ancestry may exceed the soft cap.

Authority resource floors are permanent high-water keys, not grant authority.
Schema 4 permits a missing settled holder while validating the key/resource shape,
cluster, group and applied revision boundary. Retained grants remain below their
floors and retain any fence evidence needed for settlement. Schema 3 stays strict
until the complete cutover. Never treat the floor alone as a receipt.

A retained readmission batch keeps every process row with its exact ReadmissionID,
its full batch and required origin operations. Live handovers also keep intermediate
issued prefix operations. Completed current-member admission remains available
before first activation; an abandoned terminal member need not wait for activation
once exact retirement/removal and all remaining dependencies permit pruning.
Founding rows remain whenever the founding operation remains, preserving one seed
binding. An earlier consumed batch used by a retained admission stays even when
no other former-owner source remains.

Checkpoint source compaction requires an approved identical declaration with the
same durable node/member bindings. Preserve the source if retained witnesses
would be ambiguous. Archived consumed metadata preparation uses its exact grant
and receipt identity, independently of an obsolete whole source document.

### Collection courier precision (peer thread 20, Task 53)

`COLLECTION_LINEAGE`: a consumed metadata readmission batch proves whole-journal
rebinding. Resolve its AdmissionID through the original AdmissionRecord, starting
from the cleanup debt's historical MemberID. Only the unique monotonic chain's
endpoint may report collection. Retained process restarts and excluded hosts
remain eligible; Target membership and InstanceID equality are not gates. Older
couriers are fenced; ambiguous branches or a courier beyond the consumed chain
return retryable Unavailable. ReadmissionKey, when present, checks only the final
batch and never selects authority.

Exact retained retirement independently proves the old replica. Later metadata
links need not target that replica: collection rows preserve their old member,
token, sequence and host timestamp through journal rebind. First acceptance binds
those immutable fields; duplicates revalidate the courier and echo the original
receipt. Metadata UpdatedAt uses proposal time clamped against the retained row;
it does not order the independent host clock or collection bucket sequence.

Every independently retained physical retired cleanup row keeps all reachable
consumed journal branches as closure dependencies, including after collection.
This preserves lost-reply retries after later readmissions and prevents pruning
from converting ambiguity into permission. Complete debt and its chain are not
independent roots; once all other references disappear, both may prune and the
read-only absence query can settle the host record.

### Host collection proof precision (peer thread 22, Task 57)

`COLLECTION_REVISION`: the first retirement milestone records delivery order,
not which valid revision the host must select. Collection and deferred status may
cite any retained cleanup revision of the debt's exact RetireAction, the original
milestone grant, or its consumed readmission grant. Require the cited grant's
exact recorded host/member/replica receipt. Pending effect delivery returns
Unavailable; restore refuses absent proof. Snapshot validation uses the same
binding. Debt retains its operation and all grant revisions, so this adds no
independent retention root. The host selects its latest complete exact retirement;
Stop receipts and mere preparation never suffice.

`CLEANUP_ASSIGNMENT`: completed exact local retirement and its full retained
grant prove permanent assignment exclusion. Approval removes that identity from
intent before issuing retirement, and allocation floors/retired-group tombstones
prohibit reintroducing it. A fresh Target, Scope, Assignments or grant RPC adds
no safety and can strand excluded hosts during metadata loss. Before preparation,
validate local proof, binding/owner, ledger and enumerated storage identity/token,
then serialized retention. Persist prepared before deletion; recheck local
resources under the shared group gate. Prepared retries use saved proof without
rerunning policy. Collected rows never delete again. Memory cleanup creates no
durable collection receipt or timestamp. Reporting/release remain ordered after
local completion and are implemented in Tasks 58–59.

### Restricted collection reporting precision (peer thread 23, Task 59)

`COLLECTION_COURIER`: typed MetaControl reports always carry ReporterMemberID
from the local authority journal pin. Use the existing AuthorityRead hint rule:
zero selects the header member; a nonzero hint must be at least the header member.
Cluster, node and process come from the authenticated request header; the resolved
member must be nonzero. This handles zero or lagging participation headers without
mutating Raft/admission identity or adding a transport override. Both header and
hint are caller assertions under the cluster credential; retained retirement and
the consumed metadata-readmission chain establish collection authority.

The leader performs collection_check through its existing confirmed read, then
Machine.ValidateCleanupCourier using the same FSM chain walk, then RecordCollection
with full Apply-time validation. Only HistoryUnavailable from debt/proof lookup
or proposal permits another confirmed absence check. Identity/authorization,
conflicting receipt and fenced courier errors never become settlement or routing
fallbacks. Settled echoes the receipt and safe-read index, appends nothing and
claims no historical acceptance. Schema3 adapters remain Unsupported until78.

Deferred status keeps its existing proposal shape: courier preflight authenticates
the report; Apply validates exact retirement binding. A concurrent courier change
can leave an old diagnostic but cannot authorize deletion or alter milestones.
ReportCleanupStatus uses the existing managed.CleanupStatus callback value for
internal composition; no new public application type is introduced.


### Collection proof and progress transition (peer threads24–25, Task61)

A consumed readmission batch may retire an already-retired cleanup identity.
Collection accepts either retained exact completed retirement revision, including
the later batch grant. The cleanup row preserves its first milestone and need not
have ReadmissionKey equal to the receipt key. Validate the explicit receipt key
against the exact target, retained admission/origin and whole-batch consumption;
existing collection-lineage closure retains this proof through pruning/restore.

The final ReadTopology pair/marker and bounded local diagnosis are prepared with
scoped fixtures; schema3 cannot safely switch until Task78. Keep ReadView.Active,
Views and the legacy topology query until that cutover. Legacy status now projects
GroupProgress from Latest/Target/Active with Active.Version only for existing
groups; never infer owner, Deleting or Blocked. No new compatibility entity.
ReadProgress uses the existing safe read before cache-only decoration. Missing
source/reporter provenance (including source zero) means nil Observation. Exact
source/assignment and current cluster/node/member/process matching preserve
original timestamps. Later local metadata may conservatively suppress an older
valid sample. Complete observation producers are Task70; live scoped reads are78.


### Controller transition and ready selection (peer thread26, Task62)

One Controller accepts exactly one internal coordinator port. Until78 the live
composition uses LegacyCoordinator; scoped configuration refuses legacy Store
and whole-document validators. Delete the bridge at complete cutover. WorkItem
is owned Operation + GroupID, with the retained Approved declaration and current
Consumed proof observation added by Task65; ScopedValidator receives the exact scope.
ReadyBlocks is bounded selection, never authorization or a durable reservation.
Use exact current owners and state-based barriers; Requires does not revive a
superseded predecessor cursor. Reuse current grants, and include consumed-step
advancement and zero-step finalization. Derive dependency/revocation diagnostics
on read rather than writing per poll. Group-key continuation permits fair rotation;
workers still validate the full tuple after waits at issuance/transition boundaries.


### Exact dispatch accounting (peer thread27, Task63)

GroupEffects.DispatchEffect(ctx, grant, resource, target) returns one EffectReceipt.
It is internal: keep the full immutable grant and require exact resource/target
membership. Native calls use an explicit nonzero observed leader candidate, with
recipient leadership verification and no hidden forwarding. Native inflight keys
are (AuthorityKey, group resource); host calls additionally key the exact target.
Both share the actual (host, group) gate across ordinary and cleanup budgets.
The managed courier revalidates the grant after fence recording; metadata native
execution refuses lost leadership instead of forwarding behind this accounting.

Each controller run seals admission under its dispatch mutex before joining its
workers. Allocate maps only for admitted work, never worker-count-sized pools.
A bounded request releases its RPC slot on return/timeout; durable actionWorks
and journals retain remote callback ownership. Record valid late receipts before
reporting lost leadership; only a separately validated FSM transition advances.
Backoff is bounded by the larger execution budget and expires idle hosts. Group
selection rotates through ReadyBlocks; no waiting group owns the whole loop.
Task64 adds cleanup selection and canonical fair target rotation;65 connects the
exact effect worker and requires recorded founding host receipts before native
creation. Legacy whole-grant wrappers remain until78 removes their consumers.


### Cleanup/fence dispatch and completed deletion (peer thread28, Task64)

ReadyCleanup selects exact identity/member/action from the ordered retained debt
index, excluding required stops and completed retirement. BeginCleanupEffect
reuses the existing immutable grant; each target dispatches and records separately.
Cleanup and resource fences alternate within MaxConcurrentCleanup and share the
ordinary host/group registry. Reserve bounded work before issuance or recipient
resolution; bind the grant key and actual native host once known. One last request
in the existing bounded backoff row rotates same-host identities after a timeout;
fence/native requests do not overwrite that cleanup target history.

ReadyFences paginates resources and selects only their current floor when it is a
fence. Existing fenceReceiver resolves the exact current journal member or observed
native leader inside the worker budget; DispatchFence does not reroute. Retryable
refusals trigger another current-state selection; one failed fence never suppresses
independent required work. No separate queue owner or application API is added.

A permanent retired-group marker plus completed group tombstone (nil declaration,
no owner, !Deleting, nonzero completion and source at least the retirement version)
settles AuthorityGroup resources only. Completion already requires joined Stop or
consumed whole-batch readmission for every possible former owner, including issued
never-converged founding targets, and the group ID cannot be reused. Physical
retirement stays detached. No native FenceEvidence is fabricated. Derive the same
predicate in obligation indexes, authority queries, settlement and restore; pruning
removes obsolete native revocation references while retaining the permanent row.
Host resources remain protected by their exact fences/effects and durable floors.


### Required execution and retry reuse (Task65)

ReadyBlocks copies the operation-version declaration and derives current-step
consumption through retained original authority. These fields are observations;
the FSM independently checks exact owner, sequence and proof on every transition.
The scoped worker advances only through PlanBlock.Steps, preserving global numbers
and skipping detached Cleanup. Consumed retry prefixes never issue a new grant.
Unconsumed work always obtains its exact grant, even when local/native state looks
satisfied. Before dispatch and after waits, revalidate the selected scope; valid
late receipts remain recorded under their original action tuple.

Each worker sends one unchanged grant to one resource/target. Founding requires
all recorded host receipts before native confirmation; the existing bounded retry
row remembers the last selected host after RPC release, including at capacity one.
Recheck preparatory catch-up before destruction using block order, verify observed
native predicates and final ReadNative membership. Completed deletion requires FSM
joined-stop/readmission proof, never a leader after the voters have stopped.
Existing actionWorks, acceptGuardedStart, beginLocalEffect and stopRetiringIdentity
reuse exact ready/joined state and persist fresh proof under a new fenced grant;
missing retained storage parks and unresolved startup/Close stays owned. No new
public API, persistent cursor or lifecycle owner is introduced. Concrete service
validators connect66; complete scoped persistence/composition remains78.


### Scoped service capture and retry validation (peer thread30, Task66)

`FI_PLAN_CAPTURE`: reuse the safe-read PlanRead envelope. Capture desired intent,
exact group source/owner and retained identities under one FSM lock; read native
membership only for those scopes; recapture with that evidence. Recheck the scope
set, affected approved declarations and selected operation cursor. Unrelated group
progress and the selected block's own Pending-to-Running transition do not revoke
work. A missing deletion quorum is not a prerequisite. Original approval and retry
edges resolve before any native I/O, including a lost response after later progress.

`FI_RETRY_PROOF`: validate original grant/receipt tuples against contiguous Previous
ancestry and immutable full-plan equality; select required global sequences without
reindexing. Metadata derives consumed steps, including exact readmission-discharge
proof for required stops. Native receipts can prove that the current step's voter
result is already present for simulation, but never manufacture metadata consumption.
An unconsumed step still runs under its new exact grant and persists fresh proof.
Detached cleanup and successful sibling blocks do not enter the selected suffix.

Drain validation consumes PlanInput, approved node state, actual native voters and
health, with exact ownership from proven/approved groups, node checkpoints and
retained `_meta` bindings. Unknown/conflicting owners refuse; old node-state
checkpoints cannot override approved draining. Forced-retirement audit scope remains
exact. Evaluate drain safety only for the selected metadata scope.

ApplyVersion discriminates approve(version) and retry(previous); retry carries no
plan and never silently selects newer intent. Operation observes the exact original
attempt; WaitOperation waits for its complete aggregate, with cancellation ending
only the wait. Service queries remain Unsupported until complete schema4 activation
at78. Its cutover removes BuildLegacyPlan, old PlanFor/Apply/WaitOperator adapters,
legacy PlanRead.Operator and activateCompleted's bootstrap/controller callers, then
selects OperationControllerConfig and the already-prepared scoped validators.


### Convenience convergence and direct transfers (peer threads31–32, Task67)

`FI_ENSURE_CAS`: check desired/approved requested-scope compatibility before
returning matching nonzero convergence. Pending removal conflicts even if an old
checkpoint matches. A local hold never invalidates that proof or gets cleared.
Pin one creation CAS/typed intent for the call; replay uncertain replies with that
CAS. FSM replay proves original acceptance and derives RebasedTo; inspect only the
successor head against its actual parent, requiring an exact one-group creation
with the same actor/comment. Do not walk all rebase ancestry client-side or guess
a fresh expected version. Approve only that head. An approval conflict naming its
rebased successor permits inspecting that successor; an unqualified conflict is
returned. A failed creation returns its exact diagnostic, never RetryOperation.

`FI_TRANSFER_SCOPE`: capture an unowned/proven group source and exact bindings,
validate the active declared target against health and fresh native membership,
then recapture after waits. Use fresh observed leadership and receiver leadership
checks; no new NativeState field. Unrelated Target version changes alone do not
invalidate capture. GrantTransferProposal is also the existing atomic join of an
unfinished exact action after a lost reply/canceled caller. Always propose/join,
then send the same grant to the observed leader, chosen from immutable grant
Targets. Recomputing member binding from Target or node progress is unsafe because
both can differ from the retained recipient proof. A changed leader is reobserved
under that same grant. Even an already-leading destination needs durable intent
settlement; raftengine skips the native self-transfer. This supersedes the initial
thread31 no-dispatch shortcut, which could strand unconsumed authority.

Prepare exact readmission/reseat predecessor and owner checks with existing
PlanInput. Named legacyEnsureProgress/legacyEnsureFailure/legacyTransferPlan and
schema3 replacement guards remain only until78. That cutover switches to confirmed
scoped reads/full exact operation diagnostics and scoped replacement commit paths;
no synthetic Active document or runtime mode flag is permitted.
