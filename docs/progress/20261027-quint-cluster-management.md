# Quint cluster-management execution progress

Plan: `docs/plans/20261027-quint-cluster-management.md`
Target: `docs/brainstorms/20261027-quint-cluster-management-target.md`
Branch: `feat/quint-spec`

## Task 1 — Executable typed Choreo foundation
Date: 2026-10-27

Implemented the shared state vocabulary, exact identity/proof distinctions, one application-owned Choreo GlobalContext, explicit request queue/delivery/handling and a concrete one-node foundation. Vendored unchanged Choreo/basicSpells and the Apache license from Quint commit bd327e32df01a578a0f7270ed7e80d0e4b0b0c6e, with hashes. Coverage enumerates 82 public API entries and marks unimplemented scope explicitly.

Verified:
- Quint 0.33.0 typecheck of instances/foundation.qnt.
- `quint test ... --main foundation --seed 1 --max-samples 10`: four passing tests (one assumption test and ten executions each of observation, invalid-refusal and request-bound scenarios).
- `quint run ... --invariant safety --witnesses deliveredRequest --seed 1 --max-samples 50 --max-steps 20`: no sampled violation, reply witnessed in all 50 traces.
- Vendor SHA-256/license/import provenance and document links checked; staged whitespace validation before commit.

Implementation detail: Quint's `action` keyword cannot name a record field, so the schema uses `kind`. Explicit re-export of cluster actions is required through the test wrapper. We use Choreo's pure cue/context/effect functions; its unused native state/display variables are not part of the model. No behavior beyond the documented foundation is claimed complete. No Go tests or formal verification were run for this model-only checkpoint.

## Task 2 — Profile runner and honest manifests
Date: 2026-10-27 (Task 1 commit: cdc21f8)

Implemented a Python 3.9-compatible profile runner, pinned foundation JSON, explicit instance-bound checks, test-name coverage checks, bounded child process groups, raw exit/log/trace preservation, source/vendor hashes, and separate sampled/bounded/exhaustive/incomplete classifications. Verification is gated on backend qualification; its first real execution remains Task 17. Missing pinned dependencies refuse without installation. Default profile uses the bundled TypeScript evaluator.

Verified:
- 17 runner unit/contract tests pass, including real Quint detection of manifest-bound drift, wrong CLI version, zero/missing tests, malformed profiles, absent executable/backend, subprocess nonzero exit, timeout, signal and latest-report publication rules.
- `python3 -m py_compile scripts/quint/run.py` and whitespace validation pass.
- Real foundation test profile: all four deterministic tests pass; real simulation profile: no sampled safety violation, 50 samples/depth 20/seed 1. Reports: test-reports/quint/20261027T115342384324Z-2c7c87e7 and 20261027T115346605296Z-b32bcea1.

Evidence changed the bounds-check implementation: importing/re-exporting an already concretely instantiated module exposed unbound consts in Quint. A zero-step invariant run against the original concrete instance verifies modelBounds without a duplicate instantiation. The failed prototype is removed. Quint's JSON run format omits witness counts; reports explicitly preserve that limitation rather than fabricate coverage. All handlers/protocol scope beyond Task 1 remain pending.

## Task 3 — Independent consensus, network and local views
Date: 2026-10-27 (Task 2 commit: 27fabe9)

Added an established three-node/two-load fixture, exact native commit certificates, independent directed meta/load connectivity, commitment separate from local metadata application, safe abstract election/catch-up, crash/retained restart with fresh incarnation, stale message consumption, explicit loss and duplicate delivery. Host metadata image/index and owner engineRunning fields refine the target's durable/volatile distinctions; they do not change any Go format. Restart returns Starting and does not republish loads; Task 7 owns completion of the startup protocol.

Verified:
- Smoke3 typecheck; 16 deterministic tests passed (four shared + twelve membership/environment tests).
- 50 seeded Rust samples, depth 60: no safety violation; independent-load-progress witness in 15/50, local-apply lag in 33/50, retained restart in 50/50 traces.
- 17 runner tests passed after extending the runtime bounds assertion.
- Real runner foundation tests, smoke3 tests and smoke3 simulation passed. Reports: test-reports/quint/20261027T120907345573Z-ae10a21a, 20261027T120913042812Z-195f7833, 20261027T120920437014Z-e6b46d34.

Test semantics: Quint fail() does not carry a successor state for subsequent then/expect clauses. Negative transition checks now end their traces, after all state assertions. No production fallback/stutter action was added to accommodate tests.

A concurrent edit to docs/project.md appeared during this task, adding technical-debt documentation guidance. It was read and preserved, and is not staged in this task's commit. The active goal and remaining plan are unchanged.

## Task 4 — Document edits, placement and immutable plans
Date: 2026-10-27 (Task 3 commit: 6050704)

Implemented pure topology validation, request guards, document mutation, placement/hosting/voter selection, preview and typed plan construction. Required steps and detached cleanup retain exact identities, source versions, voter guards, sequence/dependency ordering, already-proven stops and allocation floors. API dispatch remains intentionally pending Task 5; pure variants do not fabricate successful executed calls.

The target sketches were refined against implementation: min/max voters versus hosted-replica capacity; explicit surge/lag/stability/cooldown controls; raw hosting-list duplicate checks; group allocation cursors; forced audit; typed PlanResult refusal; separate required/cleanup steps; and retained owner milestones. ReseatReplica's atomic repair/approval differs from ordinary Stage/Approve. Helpers placement.qnt/planning.qnt stay pure behind topology.qnt. The final target SHA-256 for this checkpoint is 348e833b952a28ae4dedd8bf9fc6f03f68ccfc8ed6d618584efc9e6d99116f11.

Verified:
- 40 focused topology/placement/planning tests pass through instances/topologyChecks.qnt, including rollback ancestry, retained staged identity restoration, same-host replacement ordering, transfer before removal, two-to-one refusal and deletion without load-native observation.
- Accumulated smoke3 test profile passes all 16 existing scenario tests plus all 40 topology cases. Report: test-reports/quint/20261027T125655851067Z-51d85f51.
- Smoke3 seeded simulation passes: 50 samples, depth 60, seed 1. Report: test-reports/quint/20261027T125713174404Z-c9fb9269.
- Foundation profile and 17 runner tests passed during this task; their code paths were unchanged by the subsequent pure topology/rollback additions.
- Current model/profile source hashes were compared against the passing full test report; all match. Document links, fences and whitespace checked. No Go tests or exhaustive model checking claimed.

User requested: “Pause after finishing the current task.” Task 4 is finished and committed with this entry. Pause before Task 5; Tasks 5–22 remain unchecked. The concurrent docs/project.md edit is preserved outside this commit. The full goal remains unfinished.

## Scope revision while paused — 2026-10-27

User requested a grouped review of coverage.md to remove redundant model work and defer labels/other features. User approved “Lean management scope; defer offline recovery,” then required metrics always good with no lag/stability issues this iteration.

Documentation now classifies all 82 methods: 17 Core controls, 15 shared Primitives, 19 Observe projections, 8 Omit wrappers/diagnostics and 23 Deferred feature methods. Labels are excluded from every active input path. Live policy tuning, EnsureGroup/RollbackTo, manual sessions/application locks, serving override, storage-loss/memory/custom-store and offline Recovery are outside v1. Minimum internal sessions, actual availability/native quorum, ownership/fencing, returning-node readmission and cleanup remain core.

Performance metric inputs are fixed favorable: lag/backlog within limits, no no-progress failure, freshness/current collector valid, stability satisfied and cooldowns clear. Availability-driven repair has no elapsed detection-window timing. Successful native catch-up milestones and revalidation after actual topology/identity/leadership changes remain; management deadlock/livelock is still checked.

The plan is revised from 22 to 18 tasks: Tasks 1–4 remain historical completed work; Tasks 5–18 remain pending. Full sessions/read-API work is folded into existing primitives; old Tasks 13–15 (storage/offline recovery) are deferred. Old Tasks 16–22 become 12–18. Substantive profiles decrease from nine to six, plus foundation. Resume at revised Task 5, beginning with scope assertions/request generation and separation of extension-only tests.

This was documentation-only analysis. Executable .qnt/Python/profile files were not changed, implementation was not resumed, and no model test/verification claim was rerun. Existing broad Task 4 tests remain on disk until Task 5 normalizes scope. The concurrent user edit in docs/project.md remains untouched.

## Task 5 — in-progress checkpoint, 2026-10-27

Implementation resumed after the approved scope reduction. Added executable scope assumptions/invocation guards and coreChecks selection; restored retained missing-native/foreign-native safety tests using role changes rather than label edits. Shared confirmed reads and ordinary staging now have separately interleavable invocation, transport, quorum, apply and reply boundaries. Cancellation stops the waiter without undoing submitted effects. Pure approval/retry rules cover scoped owners, replay, immutable plans and exact unissued deletion obligations. Approval/retry routing and atomic reseat remain incomplete; no Task 5 completion/commit is claimed.

Evidence collected before the current checkpoint: 31 smoke3 scenarios and 51 coreChecks tests passed in direct runs. The profile runner passed 31 scenarios plus the earlier 41-core-test selection in `test-reports/quint/20261027T141944881680Z-a2db7ebe`; its subsequent 50-sample/depth-60 run passed in `20261027T142019441430Z-4276b636`. These hashes precede the later pure approval/retry additions and step grouping. A direct 500-sample/depth-120 fault-heavy run reached staging once, but no completed mutation/read replies; deterministic scenarios prove those paths, and a healthy managementStep subset was introduced for targeted reachability. Its last output was not retained after interruption, so no pass is claimed for that run.

Source findings: raw Stage submits a literal expected version even when caller application lags; convenience zero CAS captures local Latest once. Historical semantic replay runs before revalidation. Approval retains cleanup actions before grants are issued, so CleanupDebt now separates exact planned actions, optional stop grant and actual receipts. RebasedTo follows the complete successor chain.

User requested less frequent Quint test execution: batch approximately 4–6 times more work between suite runs, keeping cheap typechecks during editing and complete validation at task checkpoints. No background Quint process remained at the interruption check.

## Task 5 — scope, metadata transactions and exact operation retries
Date: 2026-10-27

Implemented executable lean-scope guards/invariants and explicit core test imports; shared read tickets with process/request/proposal/round correlation; separately interleavable stage preparation, proposal transport, quorum commitment, local application and reply; preview, original-CAS/audit replay and observable cancellation/indeterminate outcomes. Leader-side approval work captures metadata, reads native membership and availability separately, confirms metadata again, then rechecks scoped immutable proposals at commitment. Added scoped owners/supersession, first-attempt replay, one failed-attempt successor, exact unissued deletion obligations, and atomic reseat/approval with independent staged-intent rebase.

Validation:
- Foundation 7 tests passed: `test-reports/quint/20261027T154905846454Z-d5708b3f`.
- smoke3 43 scenarios and coreChecks 60 tests passed: `test-reports/quint/20261027T154929137587Z-91c5bd39`.
- Full smoke3 fault relation, seed 1, 50 samples, depth 60: sampled safety pass in `test-reports/quint/20261027T155031955190Z-bb318403`.
- Healthy single-request exploration: `quint run scripts/quint/instances/smoke3.qnt --main smoke3 --init approvalReachabilityInit --step protocolStep --backend rust --invariant safety --witnesses stagedMutation repliedMutation approvedOperation --seed 1 --max-samples 50 --max-steps 160 --verbosity 1`; all three witnesses reached in 50/50 traces. Log: `test-reports/quint/task5-approval-reachability-fixed.log`.
- Python runner suite: 17 tests passed. Active instances typechecked; executable bound checks passed. Current smoke3 bounds: three nodes, two initial loads, three calls/process, position 8, incarnation 2, delivery IDs through 40.

Corrections found during implementation: explicit imports fixed a collision between private test helpers; the retained failed-operation fixture needed an explicit map construction rather than nested transformValues; reseat validation now preserves independent staged allocation floors; native reads require quorum and published exact replicas; repeated metadata queries use distinct read rounds; healthy exploration no longer floods retries (the full fault relation retains them). These were model/harness corrections, not confirmed library defects. No source-faithful failing library scenario has yet been confirmed; reporting rules and the ongoing report are in `docs/reports/20261027-quint-cluster-management.md`.

The task grew beyond its initial line estimate to keep the source's read/recapture and transport boundaries explicit. Pure helpers remain under scripts/quint and use one composed world; vendor libraries and production Go are unchanged. Retry routing uses a retained failed-before-effect fixture; native execution, receipt-prefix reconciliation, grant revocation/fencing and runtime failure generation remain in Task 6. This checkpoint does not claim full cluster-management coverage or qualified exhaustive/progress checking. The concurrent user edit in docs/architecture/library.md is preserved outside this commit; source/document contract differences remain an architectural-audit concern under the agreed implemented-code baseline.

Final checkpoint audit: all 37 recorded model/configuration hashes match the current files; documentation links resolve and `git diff --check` passes. Target SHA-256: `0b4997c5fa252b5ad298de7d63f4cab9a02d7b137b79b7b499dc3aff3bb64ea8`.

## Task 6 — in progress

Task 5 committed as `5152a75`. Began source-grounded authority catalog rules in grants.qnt and extended records with immutable planned-step/request bindings, optional whole-group receipt targets, durable receipt/fence positions/terms, metadata resource floors, closure state and explicit revocation fence keys. Pure helpers distinguish immutable grant validation from issuance eligibility, exact receipt/fence replay, boundary ordering, command consumption and resumable transfer settlement. They are not yet wired to dispatch or consumption; no Task 6 completion is claimed. Typechecks passed for grants.qnt and smoke3. Full tests are deferred until the next substantial implementation batch, per the user’s requested cadence.

Source findings to preserve: ValidateGrant intentionally accepts a closed but exact retained grant; physical resource fencing limits delayed execution. A transfer-start receipt consumes a command but cannot alone complete transfer or settle resumable intent. Fence/receipt order is checked by native applied index/term or host-local sequence. Whole-group founding may use a targetless group receipt; host receipts always name an exact target.

### Task 6 — executable authority checkpoint, 2026-10-27

Wired authority control commands into the existing metadata proposal/commit/apply path, using metadata log positions for grant revisions. Added immutable step authorization, manual-transfer authority, grant replay, scoped closure/fence obligations, receipt/fence ordering and settlement. Added receiver delivery/wait/revalidation/consumption, durable native transfer-start receipts, separate reporting and transfer observation/checkpoint actions. Native/host fencing and ordinary/readmission receipt validation remain distinct. Transfers and fences exercise the physical path now; lifecycle, membership and cleanup effect bodies belong to Tasks 7–10 and are not simulated as automatic successes.

Added remaining-plan retry validation against captured native membership, exact consumed current-step/prefix receipts, predecessor/result voter sets and transfer prerequisites. Existing immutable successor routing and completed sibling preservation remain unchanged. Runtime failure fixtures still need a final audit; no Task 6 completion or commit is claimed.

Validation at this checkpoint:
- `quint typecheck scripts/quint/instances/authority3.qnt`: passed.
- `python3 scripts/quint/run.py --profile smoke3 --mode test --seed 1`: 43 smoke scenarios and 75 core checks passed; report `test-reports/quint/20261027T173550168309Z-b3dff5b1/report.md`.
- `quint test scripts/quint/instances/authority3.qnt --main authority3 --seed 1 --max-samples 1`: 48 scenarios passed, including five new transfer/durable-report/duplicate/stale-incarnation/post-wait-fence cases. This instance has position bound 16 and delivery bound 80; other bounds match smoke3. Its omitted-fence-check variant violates physical receipt/fence ordering as intended.
- `git diff --check`: passed before this log append.
- No new sampled exploration, exhaustive verification or Go regression run is claimed. No confirmed source-faithful library failure was found. A new scenario initially used Quint `then` for a read-only assertion; corrected to `expect` before validation. This was a model test-construction error.

Remaining before Task 6 completion: strengthen offline-host/native-quorum revocation and omitted-identity-check scenarios; audit receiver/proof/checkpoint wiring and action barriers against the subsequent lifecycle/membership tasks; integrate the authority instance into runner coverage; run the final scoped exploration/checkpoint. The current task remains unchecked. User feedback identified excessive analysis as the primary delay; test execution alone did not account for it.

## Task 6 — effect authority, fences, receipts and immutable retry suffixes
Date: 2026-10-27

Implemented authority control proposals in the existing metadata commit/apply path; exact approved-step and manual-transfer grants; immutable historical catalogue validation versus issuance eligibility; resource-sensitive closure/revocation; native/host durable floors and evidence; confirmed courier identity proofs; receiver revalidation after waits; durable transfer consumption, separate report and observed-leader checkpoint; and remaining-plan retry validation against consumed receipt prefixes, native predecessor/result membership, transfer prerequisites and capacities. Retrying a failed block preserves completed siblings and revokes only the failed/conflicting scope. Added a bounded consumption-identity monitor so later restart cannot rewrite the witness.

The complete approved-transfer scenario enters through a public topology mutation and approval, then executes grant/dispatch/proof/consume/report/leader-observation/checkpoint. Added native-quorum and offline-host revocation cases, stale courier binding and receiver identity cases, host report ordering, exact replay, partial host batches and deliberate omitted-floor/identity controls. The authority3 profile starts simulation from an issued/authenticated manual transfer, with 3 nodes, 2 loads, position bound 16, delivery bound 80, request bound 3 and incarnation bound 2.

Validation on the final executable files:
- authority3: 57 asynchronous scenarios and 80 core checks passed, seed 1, 10 samples/test: `test-reports/quint/20261027T175518147536Z-68290e5b/report.md`.
- authority3 full fault relation: sampled safety passed, seed 1, 50 samples, depth 100: `test-reports/quint/20261027T175524093311Z-27f2ba93/report.md`.
- foundation: 7 tests passed: `test-reports/quint/20261027T175524093311Z-d6c634ca/report.md`.
- Python runner tests: 17 passed. Typecheck and bound preflights passed; diff whitespace check passed. No exhaustive/progress or Go conformance claim is made.

Model corrections during the task: native grants must use approved history rather than every earlier staged document; manual transfers preserve their captured source version; courier authentication requires a fresh metadata proof; retries must not close completed siblings' resources. One offline-host test used `fail()` around a multi-step helper; it was corrected to assert the disabled delivery action directly. These are model/test corrections, not confirmed library defects. No source-faithful failing library scenario was found; the existing findings report remains applicable.

Task boundaries: physical transfer and fencing exercise the shared authority pipeline now. Lifecycle, native membership and cleanup action bodies (including their runtime failure generation) remain Tasks 7–10; unsupported bodies do not emit fake successful receipts. Task 7 extends host-floor installation/join/completion when callbacks or Close are outstanding. Minimum session activation will update the replicated courier binding; current initial bindings represent the established fixture. Full public TransferLeader waiter coverage belongs to Task 12. No production Go or vendor source changed.

## Task 7 — in-progress ownership foundation

Task 6 committed as `4059cfd`. Added `lifecycle.qnt` pure ownership transitions and `lifecycle_test.qnt` scenarios for one exact reservation, shared waiters, waiter cancellation, late successful factory acquisition, failed factory ownership, replay/publication boundaries, engine join, serialized Close retry and exact local holds. Owner records now retain their process, waiters, callback/replay status, captured declaration and authority revision. Process-local holds survive slot changes but are cleared by process crash; an old identity's stop cannot stop its successor.

Typechecks passed for the composed authority3 instance after record expansion and for the new lifecycle test module. These new lifecycle tests have not been executed yet, consistent with the requested batched cadence. They are not yet in the production step relation/coreChecks. Remaining Task 7 work: actor/network wiring and startup scope/floor recapture; interrupted host-fence installation versus join/completion; minimum session activation/renewal/expiry/recheck/revocation and readiness; retained restart recovery; scenario validation. No Task 7 completion is claimed.

Grounding: managed.reserveStart/reconcileStartupAuthority distinguish accepted-owner replay from new-owner floor import; runReplicaStartup/acquireApplication transfer ownership on successful late factory return and publish only after final authorization; failed factories keep their partial acquisitions. Node StartLoadGroup/StopLoadGroup delegate to these shared ownership phases. Before wiring session readiness, inspect the internal activation/lease path; public manual session and application lock APIs remain excluded.

## Task 7 — local ownership, retained lifetime and internal sessions
Date: 2026-10-27

Implemented and wired shared reservation/start/replay/authorization/publication and stop/join/Close/release transitions. Concurrent exact starts share one owner; canceling a waiter leaves accepted work owned. Successful late factory return transfers application release ownership, failed factories retain responsibility for their partial acquisition, and failed/timed-out Close cannot release a slot. Exact holds affect only their matching replica. Node shutdown leaves metadata participation running until managed work releases.

Host fencing now persists its floor before completing proof, joins older startup/Close work, survives interruption with the same durable position and reports completion separately. Retained restart clears volatile work/holds, preserves valid WAL and authority evidence, uses a fresh incarnation, reauthorizes local handles and establishes its new internal session.

Internal session grant and reserved node binding are separate metadata commands; no application lock API is modeled. Renewal is leader-local. Expiry observation, exact-process probe, serialized revoke submission and durable revocation remain separate. Late replies bind the exact process/member/session/round and respect request expiry. Source-grounding clarified that load scope is locally applied Target, accepted owners bypass new floor-query quorum work, and load-handle publication is distinct from observed session-based node readiness. Target §§5.4/8.6/10.1 and the model README were clarified accordingly. Established fixture authority is explicitly represented by exact retained seed identities at floor 1; new identity/effect authority still requires grants.

Validation:
- lifetime3: 76 runtime scenarios and 91 core checks passed, seed 1, 10 samples/test: `test-reports/quint/20261027T184618336599Z-6d59ee80/report.md`.
- lifetime3 full fault relation: sampled safety passed, seed 1, 50 samples, depth 120: `test-reports/quint/20261027T184745295488Z-daa25d54/report.md`.
- foundation: 7 tests passed: `test-reports/quint/20261027T184745295788Z-7b998eaf/report.md`.
- Python runner checks: 17 passed.
- All 53 recorded model/configuration hashes matched the final executable files. Typecheck/bound preflights and diff whitespace checks passed. No exhaustive, temporal-progress or Go conformance claim is made.

The first batch passed 69/70 runtime cases; restart activation exposed a model leader-discovery guard that required an existing observation after crash had cleared it. The guard now permits first discovery. This was a model defect, not a confirmed library failure. No source-faithful failing library scenario was found. The test/report changes preserve all previous authority and management cases.

User requested stopping after committing the current task. Task 8 has not been started; pause the active goal after the Task 7 commit. No production Go or vendor source changed.


## Task 8 — resumed, native membership checkpoint (in progress)

The persisted goal was resumed after the refactoring finished (f43d20e). Task 8 is active; its checklist remains incomplete. Added native guard rules and a phased shared-world path: durable authority install → precheck → rechecked proposal → current-quorum commitment/configuration/receipt → separate report/checkpoint. Uncommitted work may be lost after its leader fails; exact receipt replay never changes configuration again. Dispatch now distinguishes these phased native effects from the older immediate transfer/fence consume path. No Go code changed.

Source grounding: membership/safety.go and coordinator.go; internal/raftengine/authority_membership.go. Safety/health is rechecked before proposal, not fabricated at commit. A failure after a valid proposal can leave the resulting configuration unavailable; that is represented as expected unavailability, not mislabeled a violation. Approval/voter-result/identity checks and current-quorum commitment remain separate.

Validation of this intermediate source snapshot: lifetime3 runner completed with 85 scenarios + 105 explicit core checks (`test-reports/quint/20261027T210314553586Z-52784f42`); 11 new pure membership checks passed (the direct command also ran 23 basicSpells checks); 8 new native scenarios plus the inherited native-quorum regression passed. Direct Rust simulation of admission4's demotionWaiting fixture, full step/safety, seed 1, 50 samples ×40 steps returned status ok. Local logs, JSON and source hashes: `test-reports/quint/task8-native-checkpoint/`. This is not completed admission4 coverage: fresh-node binding/preparation and scale5 are still pending.

Construction fixes: added the native dispatch eligibility missing from the prior transfer/fence-only gate, renamed a fixture conflicting with basicSpells.last, and moved assertions before fail() in two scenario scripts. These were model/test-construction errors, not faithful failing library scenarios, so no library-defect report was added. Next: source-grounded fresh admission handshake, host preparation and catch-up checkpointing; then full scaling and capacity scenarios.

### Task 8 — fresh admission and 3→4 checkpoint (still in progress)

Added process-bound admission records and immutable join-response proof; fresh requests, confirmed leader reads, binding commit, reply delivery, exact durable pin and local metadata startup remain separate. Preparation requires a bound approved admission and produces an exact host receipt; it is not native membership. Ready acknowledgement is a separate replicated command. Native catch-up checkpoints and promotion use the same approved operation. Preserved the source distinction that engine WaitReady is local replay readiness, not a voter/quorum certificate (`internal/raftengine/loop.go`, `metaservice/authority_host.go`). Already-satisfied native effects retain configuration revision.

Source grounding: `internal/metaservice/join.go`, `join_admission.go`, `join_client.go`, `join_checks.go`; `internal/metaengine/scoped_joining_admission.go`, `scoped_admission_prefix.go`, `scoped_barriers.go`. Added model modules `metaengine/admission.qnt` and `metaservice/admission_steps.qnt`, extending the existing staged suite chain through native_test and admission_test. `instances/admission4.qnt` is a working four-node instance, not a completed admission4 profile yet.

Latest checkpoint validation: admission4 typechecks; 19 selected scenarios pass at seed 1/max-samples 1 (18 new native/admission/composition cases plus the inherited native-quorum regression). This includes real public AddNode→approval→join→prepare→learner→catch-up→promotion→metadata completion for 3→4, stale join replies, unprepared rebinding and premature catch-up refusal. `test-reports/quint/task8-admission-checkpoint/` retains local-only scenario log and source hashes. Full lifetime3 regression completed: 85 scenarios + 105 core checks (`test-reports/quint/20261027T212332826502Z-c98977f9`); whitespace check passes. No library defect established. Task 8 has not been committed or marked complete.

Remaining Task 8 work: parameterized growth/shrink trace and scale5 profile; four-node replacement/capacity/pin cases; node progress/evacuation proof; durable self-removal fencing; explicit native refusal/failure routing where the library returns a terminal effect error; retained restart during partially completed admission; final profile inventories/evidence. Specifically, the earlier `authority.zeroStepProven` blanket refusal after any historical grant is a model checkpoint limitation: source `scoped_approval_completion.go:zeroStepMembership` reuses prior proven membership and exact subsequent consumed effects. Reconcile that source rule before draining/excluding already-admitted nodes, rather than bypassing it in a fixture. Node progress follows `scoped_node_progress.go` (converged current meta plus relevant load dependencies), and self-removal follows `metaservice/removal.go` (applied native removal AND approved exclusion/replacement, with a sticky retained membership-seen boundary). These are pending model work, not confirmed library failures. Parent Tasks 9–18 remain unstarted.


### Task 8 — source corrections, tooling and reported progress finding

Implemented node progress from current metadata proof plus relevant load dependencies, replay of retained membership proof for empty plans, and durable applied self-removal fencing. Native removal now creates cleanup debt on receipt recording, matching `scoped_cleanup_effect.go`; whole-group deletion keeps approval-time debt. Exact granted load preparation and joined load-stop receipts support two-load evacuation, with metadata removal gated on both native exclusion and host-stop proof (`scoped_barriers.go`). Retirement/collection remain deferred to their planned tasks.

The complete 3→4→5→4→3 script, fourth-host replacement, activation, two-load evacuation and fenced restart execute through the shared facade. New nodes complete internal session activation in the scaling script. Added scoped failure/wait reporting, exact cached effect-result delivery, and metadata completion from returned receipts after native quorum loss. Host preparation resumes its exact grant after a failed factory releases ownership.

Tooling: long Rust test trace serialization exceeded Node's string limit; the TypeScript fallback and an ordinary admission batch hit its default heap. Those were incomplete runs. Explicit long-witness REPL entries avoid full trace export without changing the test sequence; exact Boolean output is required because REPL can exit zero after errors. Ordinary suites retain their samples and exact inventories. Large profiles record a 16384 MiB frontend heap. The runner's 19 unit checks pass. Details: `docs/reports/20261028-quint-task8-tooling.md`.

Further source checks corrected `_meta` AddLearner's Ready-admission gate, live-admission courier resolution and same-member operation handover. The pre-Ready prepared restart no longer falsely succeeds. QCM-001 is reproduced by `admissionRestartExpected.qnt`; its desired continuation fails at the native precheck. A positive witness verifies both binding refusal and missing admission readiness. The acknowledged restart and superseding-operation witnesses pass. Go reproduction remains pending; the source map, command, bounded assumptions and manual regression recipe are in `docs/reports/20261028-quint-admission-prepared-restart.md`. No Go/vendor changes were made.

Final validation is being recorded below before Task 8 is committed. Earlier development-run manifests retain their original source hashes and must not be presented as final-snapshot evidence.


### Task 8 completed — final evidence

All Task 8 requirements are implemented and wired through the shared world: exact admission and same-member handover, fresh/retained identity, native role and quorum guards, scoped failure/wait handling, load preparation, load-stop evacuation gates, node progress and persistent self-removal fencing. Four-node capacity/replacement/pin cases and the full 3→4→5→4→3 sequence execute; the load-evacuation witness moves both loads before exclusion. Source-derived permission checks distinguish a live admission courier from session-based fallback. No global topology-operation gate was introduced.

Final source-matched validation (Quint 0.33.0, Rust v0.7.0, seed 1):

| Check | Result | Report directory under test-reports/quint |
| --- | --- | --- |
| admission4 | 109 ordinary scenarios, 115 core checks, 5 named long witnesses; passed | 20261028T012751654586Z-7edbd141 |
| scale5 | 111 ordinary scenarios, 115 core checks, 6 named long witnesses; passed | 20261028T013239014267Z-2f207252 |
| smoke3 simulation | 50 samples, depth 60; sampled-pass | 20261028T013240216789Z-91588f12 |
| admission4 simulation | 50 samples, depth 60; sampled-pass | 20261028T013540065756Z-4ce7e584 |
| scale5 simulation | 50 samples, depth 80; sampled-pass | 20261028T013540065756Z-5c16411b |

Ordinary profiles use 10 maximum samples/test; each named long REPL witness executes once, with exact-result validation and no full trace export. Counts overlap across profiles. All hashes in these five final manifests match the executable tree. The earlier smoke3 test (50 +115) and foundation test (10) passed at the preceding source checkpoint (`20261027T232324338219Z-dea7d4dc`, `20261027T231035723930Z-f5b1cd75`); they are historical, not reruns on the final admission-handover revision. The final admission/scale suites retain their inherited scenarios and explicit core inventory. Runner unit checks: 19 passed. Import paths, acyclicity, sole world ownership, documentation links and `git diff --check` passed.

QCM-001 is not a passing recovery claim: the separate `retainedPreparationRestartShouldContinueTest` returns exit 1/QNT513 at the required learner precheck. The refused-binding/disabled-add witness passes and preserves safety. Local partial ITF trace, command output and source hashes are in `test-reports/quint/task8-admission-gap/`; the report and source reproduction remain in the repository. Go reproduction has not been run. This finding was exposed by correcting an omitted source Ready gate, not by weakening that gate or changing production behavior.

No Go, vendor, public API, storage format or default Make target changed. Full retirement/collection/readmission, scheduler repair, API/visibility closure and progress/backend qualification remain Tasks 9–18. Scenario/sampled results do not establish exhaustive equivalence, temporal progress or Go conformance. The goal remains active; continue with Task 9 after this commit.


## Task 9 — group creation/deletion in progress

Started from clean Task 8 commit 5da0c3a. Added approved founding-host startup through the existing reservation/factory/replay/publication path, plus separate native confirmation of the exact immutable voter set. Initial native configuration is seeded when the first authorized engine starts; it has no leader, commit or completion receipt. Later hosts cannot reset existing consensus. All required host receipts precede native confirmation, matching operator/scoped_effects.go and managed/granted_actions.go. Metadata/load channels and native elections remain separate.

Added healthy creation, explicit healthy-host placement with one offline node, required-offline-founder wait, late founder startup after native progress, staged-create refusal, deletion without load quorum, offline debt, ID-reuse refusal, incomplete-founding cancellation and unrelated-group progress cases. A verification-only deletion-obligation monitor captures exact former-owner obligations before approval mutates metadata; the missing-debt negative control checks the monitor without granting actors new knowledge.

Source correction during implementation: retained startup may use a current complete resource floor; creation-grant restrictions belong to fresh storage/configuration acquisition (managed/authority.go:startAuthority and boot.go:checkBootCreation). Kept retained recovery behavior unchanged rather than imposing a new grant-kind restriction. A draft revocation test assumed manual transfer closure on detached deletion; source successor-resource selection does not make that assertion. Replaced its setup with a genuinely superseded executing transfer, retaining the intended native-revocation and independent-group checks.

Initial focused creation tests: 5 passed. Focused deletion batch: 6 passed. First full deletion3 run passed 97/98 scenarios and stopped before core checks (`test-reports/quint/20261028T024343744165Z-c1977133`); its failed test was the setup assumption above, not a library counterexample. The same source's simulation passed 50 samples at depth 100 (`20261028T024344921525Z-3389e48c`). Final validation remains pending. No Go or vendor code changed; QCM-001 remains separately documented.


## Task 9 — validated checkpoint

Completed healthy/degraded founding and detached deletion; retirement/collection/readmission remain Task 10. The target's creation flow and checkpoint status, README and API evidence ledger now describe the implementation. The target update makes initial configuration versus native completion explicit; it does not change the agreed abstraction or deferred scope.

Final validation on the current executable hashes (Quint 0.33.0 / Rust v0.7.0, seed 1):

| Check | Result | Report directory under test-reports/quint |
| --- | --- | --- |
| deletion3 tests | 98 scenarios + 118 core checks; passed | 20261028T025232467610Z-b77fdc0b |
| admission4 tests | 109 ordinary scenarios + 118 core + 5 long witnesses; passed | 20261028T025629620337Z-cf369b1d |
| scale5 tests | 111 ordinary scenarios + 118 core + 6 long witnesses; passed | 20261028T025629620340Z-22562ada |
| foundation tests | 10 passed | 20261028T025954366302Z-648b8a3d |
| smoke3 simulation | 50 samples, depth 60; sampled-pass | 20261028T025232467621Z-dfef6f5e |

Ordinary tests retain 10 maximum samples/test; each long REPL witness executes once with exact-result checks. Inventories overlap. All final report hashes match the executable tree. Authority3 also typechecks. Deletion3 includes every lifetime3 scenario and core check; native/scaling suites retain the earlier chain. Import audit: 85 nonvendor modules, resolving/acyclic imports and one world declaration in cluster.qnt. Documentation links and git diff --check pass.

The separate lifetime3 run timed out at its configured 60-second test-command limit (exit -15), recorded as incomplete in 20261028T025232467634Z-7654d06d. It is not a passing run. Its entire 85-scenario/118-core inventory passed in deletion3, so no redundant rerun or timeout-setting change was made. The earlier deletion3 simulation (50 samples, depth 100) predates only the corrected revocation test setup and is historical evidence, not a final-hash run.

The first deletion3 failure was a test setup error about manual-transfer revocation, corrected by using a genuinely superseded executing operation. No new source-faithful library failure was found; QCM-001 remains separately documented with Go reproduction pending. No Go/vendor/dependency/default Make changes. This checkpoint supplies scenario and sampled evidence, not exhaustive progress, verification or implementation conformance. The active goal continues with Task 10.
