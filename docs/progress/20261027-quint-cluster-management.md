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
