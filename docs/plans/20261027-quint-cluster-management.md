# Implementation plan: initial Quint cluster-management model

**Plan:** `docs/plans/20261027-quint-cluster-management.md`
**Date / scope revision:** 2026-10-27
**Project source:** `../..` relative to this plan; repository root `.`
**Branch:** `feat/quint-spec`
**Source baseline:** `e8e85dbd7879b1c858252cd2a6126a36920f8a8e`
**Target architecture:** [target](../brainstorms/20261027-quint-cluster-management-target.md)
**Target SHA-256 at scope revision:** `ef805050edbda81419326482776bf27bf06a1046f85de84c8e590749c3847873`
**Target SHA-256 at Task 5 checkpoint:** `0b4997c5fa252b5ad298de7d63f4cab9a02d7b137b79b7b499dc3aff3bb64ea8`
**Full brainstorm log:** [discussion](../brainstorms/20261027-quint-cluster-management.md)
**Execution progress:** `docs/progress/20261027-quint-cluster-management.md`
**Library scenario findings:** `docs/reports/20261027-quint-cluster-management.md`
**Status:** Tasks 1–8 completed; next is Task 9. The resumed goal remains active. QCM-001 is a retained model reproduction with Go regression pending; validation and limitations are in the execution log.

Module organization was refactored separately on 2026-10-28; see the [refactoring plan](20261028-quint-module-refactoring.md). Pending file paths below are relative to `scripts/quint/` unless stated otherwise. Completed task records retain their original paths as history. The prior protocol pause ended when the goal was resumed.

## Revised objective and execution contract

The user approved **lean management scope; defer offline recovery**, then specified **metrics always good; no lag/stability issues this iteration**. Implement core topology transactions, scaling, group/replica operations, scheduler repair, identity/fencing, retained restart, ownership and cleanup. Reuse shared quorum/lifecycle/transport primitives and derive operator visibility. Do not implement one state machine per public API.

The [coverage ledger](../../scripts/quint/coverage.md) classifies all 82 entries: 17 Core, 15 Primitive, 19 Observe, 8 Omit, 23 Deferred. Initial acceptance covers the retained controls/mechanisms, not all 82 methods. Labels, selectors, AZ/domain/reserved-label/type-placement logic are entirely excluded from v1, including generic Stage and creation paths. Application locks/manual session workflows, EnsureGroup/RollbackTo, live policy tuning, serving override, disk-loss/memory-storage suites and offline Recovery are deferred.

Keep minimum exact-process sessions because admission/readiness depends on them. Keep numeric voter/capacity rules, exact host assignments, learner/promotion/removal/transfer safety, partial effects, late replies, immutable retries, owner holds and exact cleanup debt. Do not turn a deferred feature into a simulated product Unsupported result or a successful stub.

All model code/dependencies remain under `scripts/quint/`. Production Go/API/storage/schema/module/port changes are outside this plan. Root Make targets remain opt-in. Simulation, bounded verification and completed finite exploration remain distinct. Do not remove negative controls or progress checks to make the smaller scope look complete.

Execute tasks in order, validate and commit each separately, and append to the existing progress file. Target approximately 300–1,400 authored lines per implementation task; avoid creating another model architecture. Scope normalization is required integration work in Task 5. Existing extension helpers/tests may remain archived or standalone, but cannot widen active initial profiles or force deferred implementation work.

## Scope change and task migration

Completed Tasks 1–4 below are historical records of the broader checkpoint. Their original references/evidence are preserved in commit `fa2e8ad` and the progress log; they are not retroactively rewritten as lean implementation. The next task reconciles executable profiles with the new scope.

| Previous task | Revised initial treatment |
| --- | --- |
| 1–4 | Completed; preserve history. |
| 5–6 | Same core workflow/authority; add scope normalization/shared fresh reads in 5. |
| 7 | Retain ownership/lifetime; fold in minimum internal session gates, omit wrapper/DI/custom-store machinery. |
| 8 | Retain numeric membership/scaling; no label/domain scenarios. |
| 9 | Create/delete; EnsureGroup and historical deletion-compatibility profiles removed. |
| 10 | Keep cleanup/readmission and durable collection ordering. |
| 11 | Fixed-policy scheduler; no labels, live tuning or custom workload/resource-balancing policy model. |
| 12 (full sessions/locks/read APIs) | Removed as a task. Necessary session/read primitives move to 5/7; visibility moves to revised 12. |
| 13 (storage-loss), 14–15 (offline recovery) | Deferred entirely; not unchecked tasks or empty profiles. |
| 16–19 | Renumbered 12–15: retained coverage, progress, qualification, tooling. |
| 20–22 | Renumbered 16–18: architectural delta, visible external review, documentation/gap closure. |

This reduces remaining tasks from **18 to 14** and substantive profiles from **9 to 6**. Task count alone understates the reduction: getter wrappers, labels, locks and recovery state dimensions are removed from remaining tasks as well.

## Common validation

Use cheap typechecks during editing. Per the user’s 2026-10-27 instruction, batch roughly 4–6 times more implementation work between `quint test` runs than the previous small-patch cadence; run deterministic suites after significant changes and at task validation, not after each helper. At the task checkpoint, typecheck all active initial instances, run all retained deterministic tests and seeded smoke3 safety simulation. Preserve existing completed checks during the transition; Task 5 explicitly separates extension-only tests rather than silently skipping retained failures. Every profile checks scope assumptions and actual executable bounds.

For runner changes, run its complete unit suite, syntax check and real profile smoke. Fix failures in the suites run and log unrelated fixes. Go/race/Docker/Gosim/performance suites are not blanket checks for model-only changes. If an authorized deviation touches Go/shared tooling, apply the repository's relevant checks and report it.

Per the user’s reporting instruction, failures in implemented library scenarios that are fully represented within the agreed abstraction must be recorded in `docs/reports/20261027-quint-cluster-management.md`. Confirm source-to-model correspondence first; include the violated property, exact scenario/trace, Go paths, revisions/hashes, bounds, command/seed, assumptions and a manual Go regression recipe. Preserve confirmed counterexamples and do not weaken properties to make checks pass. Distinguish source-faithful counterexamples from model/fixture defects, artificial bound exhaustion and expected availability blockers. Report actual failed verification outcomes; a reproduced library finding is not a passing safety check.

## Task overview

| Task | Deliverable | State |
| --- | --- | --- |
| 1–4 | Foundation, runner, consensus/network, pure topology/planning | Completed historical checkpoint |
| 5 | Enforce lean domains and wire CAS/approval/retry | Completed |
| 6 | Exact effects/fences/receipts/revocation | Completed |
| 7 | Local ownership/lifecycle and minimum internal sessions | Completed |
| 8 | Native membership and 3–5-node scaling | Pending |
| 9 | Healthy/degraded create and detached delete | Pending |
| 10 | Cleanup/collection and returning-owner readmission | Pending |
| 11 | Supported fixed-policy scheduler repairs | Pending |
| 12 | Core API coverage and derived operator visibility | Pending |
| 13 | Explicit stuck/fair-progress and backend calibration | Pending |
| 14 | Core negative controls and six-profile qualification | Pending |
| 15 | Opt-in Make/reporting integration | Pending |
| 16 | Architectural delta verification | Pending |
| 17 | Visible external review and confirmed fixes | Pending |
| 18 | Final gap closure/documentation/changelog | Pending |

## Completed task records

### Task 1: Establish an executable typed Choreo foundation

**Files:** `scripts/quint/{types,handlers,cluster,properties,cluster_test}.qnt`, `instances/foundation.qnt`, `README.md`, `coverage.md`, `vendor/*` (new).
**References:** target §§1–6, §§10.4/12; §4 composition diagram; schemas §§5.1–5.8. Sources: `cluster.go`, `node.go`, `node_recovery.go`, `topology/authority*.go`, `topology/storage_class.go`, `topology/hosting.go`; local Quint examples at checkout `bd327e32`.

- [x] Pin Quint 0.33.0 and vendor the exact Choreo/basicSpells files with license, upstream revision, file hashes and documented local adapter delta. No absolute checkout imports or npm dependency inside a Go module.
- [x] Define target identities and grouped state records incrementally: `ReplicaKey`, `ProcessKey`, `AttemptKey`, `EffectTarget`, `EffectKey`, `RequestKey`, volatile/durable node records, `NodeStorageConfig`, topology/native/metadata/network/recovery records. Resolve referenced enum/input/update types from actual source branches; record each mapping instead of inventing missing behavior.
- [x] Preserve corrected forms: `GroupDecl.storage: StorageDefault | StorageMemory`, `hosting: Option[HostingRule]`, action/admission receipt shape plus separate `FenceEvidence`. Keep request IDs, process incarnation and durable member/replica identities distinct.
- [x] Instantiate one node actor and exact finite constants. Build one real `listen_*`/`act_*` pair through `main_listener`, an explicit enqueue→delivery boundary, and `init/step`. Use a harmless local observation or invalid-request refusal; do not pretend to commit a topology change before consensus exists.
- [x] Add one named invariant, one delivered-request witness, and configuration-assumption `run` assertions. Extend type definitions only after the previous executable slice runs. Add a coverage ledger with planned/implemented/excluded status and exact public API names.
- [x] Document every future-handler placeholder and its owning task. Import each stateful cluster exactly once; frame all actual variables including any derived Choreo display state.

#### Verification

- [x] `quint typecheck scripts/quint/instances/foundation.qnt`, deterministic foundation tests and a short seeded foundation simulation all pass; no uninitialized const or undefined map/Option/list access.
- [x] The witness exercises actual handler wiring and delayed delivery, not a test-only state mutation. All foundational invariants execute with `step`.
- [x] Vendor license/hash/path checks and `git diff --check` pass; record commands/output in progress before committing.

### Task 2: Add the profile runner and honest run manifests

**Files:** `scripts/quint/run.py`, `run_test.py`, `profiles/foundation.json`, runner fixtures and README (new/updated).
**References:** target §§6/6.3/11/13; reporting requirements and CLI contract. Existing style: `scripts/test_report.py` and its tests (read-only).

- [x] Implement the target CLI `--profile NAME --mode {test,simulate,verify} [--seed INTEGER] [--reports-dir PATH]` with Python 3.9-compatible standard library code. Initial profile schema: version, pinned CLI version, instance/main/init/step, bounds, properties, witnesses, backend, samples/steps, timeout and optional TLC config; add explicit test entry points so zero selected tests cannot pass.
- [x] Define small typed runner records/functions: `load_profile(path) -> Profile`, `validate_profile(profile, root) -> None`, `build_commands(profile, mode, seed) -> list[list[str]]`, `run_command(argv, timeout, artifacts) -> CommandResult`, `write_report(manifest, results) -> None`, `main(argv) -> int`. Use subprocess argument lists; never shell-concatenate profile data.
- [x] Validate names, existing paths, required const/instance bounds and supported mode/backend combinations. Separate simulation/test backend selection from verification backend selection. Keep report bounds consistent with concrete instance constants through executable assumption checks; do not trust JSON declarations alone.
- [x] Before execution, record CLI/backend availability and version; fail clearly for missing pinned dependencies without downloading/installing them. Supply explicit samples even with a seed and explicit init/step/main/property selection.
- [x] Persist exact commands, revision/hashes, bounds/seed, stdout/stderr, ITF artifacts, child exit/timeout/signal and a result category: sampled-pass, bounded-pass, exhaustive-complete, counterexample, infrastructure-error or incomplete. Retain failures/interruption evidence; publish latest only after completed reporting.
- [x] Use a real foundation profile for integration tests and controlled fake child processes for timeout/exit/report tests. No Go execution or replay adapter.

#### Verification

- [x] `python3 -m unittest discover -s scripts/quint -p '*_test.py'` and `python3 -m py_compile scripts/quint/run.py` pass.
- [x] Foundation `--mode test` and `--mode simulate --seed 1` run the real CLI and retain manifests. Missing CLI, wrong version, malformed bounds, zero tests and timed-out children fail with accurate artifacts.
- [x] Common model checks remain green; the runner never reports a timeout or failed tool as verification success.

### Task 3: Model independent consensus, network faults and applied views

**Files:** `membership.qnt`, `membership_test.qnt`, `instances/smoke3.qnt`, `profiles/smoke3.json` (new); shared model/types/handlers/properties (updated).
**References:** target §§4/5.5/5.8/6.1/7/8.7/10.1/11; §14 `quorumRead`; `docs/architecture/raft.md`, `internal/raftengine`, `internal/metaservice` read paths.

- [x] Implement `hasCommitPath(consensus, nodes, links): bool`, abstract leader authority/election, commit/apply positions and catch-up classes. Use exact native voter identities and valid durable evidence; observations/session health cannot create a commit certificate.
- [x] Add directed independent meta/load link sets and `deliver/drop/duplicate/setLink` actions. Intercept Choreo sends as explicit queued management effects; partitioned messages cannot teleport to mailboxes. Model duplicate delivery without changing request/effect identity.
- [x] Add `crash/restart`, fresh incarnations and retained valid disk for this task. Keep metadata commitment separate from per-node local apply; capture local read views, stale observations and old-process messages.
- [x] Create `smoke3` with three nodes and two exact preexisting load groups as a validated initializer. Add an abstract load-commit token so load progress can be witnessed without modeling payloads. Creation of new groups remains Task 9.
- [x] Add a non-omniscience check: handler inputs expose applied/cached values and exact supplied proofs; current shared metadata/native state is accessed only by the specified consensus/environment transitions.

#### Verification

- [x] Common checks pass. Witness delayed local apply, asymmetric isolation, leader replacement, fresh restart incarnation and independent load commits while `_meta` cannot commit.
- [x] With two of three voters unavailable, new commits are refused/blocked; a learner cannot substitute for a vote. Meta and load partition tests have distinguishable outcomes.
- [x] Seeded `smoke3` exercises crash/partition/recovery actions without artificial identity reuse or undefined accesses.

### Task 4: Implement document edits, placement and pure plan construction

**Files:** `topology.qnt`, `placement.qnt`, `planning.qnt` and their test modules, `instances/topologyChecks.qnt` (new); types, shared tests, profiles, target clarifications and coverage (updated).
**References:** target §§5.2–5.3/6/6.2/8.1–8.3; topology schemas and `makePlan` interfaces; source `topology/{document,hosting,placement,voter_config,plan}*.go` and `cluster_mutations.go`.

- [x] Implement `validateDocument`, `mutationGuard`, `mutateDocument`, `affectedScopes`, `eligibleHosts`, `makePlan` and `preview` with the target signatures and narrower typed input records. Guard and update functions remain separate.
- [x] Define named request variants for every document mutation, including full Stage, placement/hosting/voter config, pins, roles, reseat, group/node edits, forced retirement and RollbackTo. Use exact fields from target §6.2; full-document labels remain Stage edits.
- [x] Model bounded representative node types, selectors, reserved/learner-only hosts, explicit/all-eligible/legacy hosting, voter domains and pinned learners. Capacity limits are replicated inputs distinct from search bounds.
- [x] Plan only declared affected scopes with ordered step/guard dependencies; preserve immutable source identities, allocation floors and retired IDs. Keep legacy group grammar distinct where it changes behavior.
- [x] Make preview pure, reject preview+approve, validate malformed ID/role/options and immutable storage class. Full shape validation catches sorted/unique input requirements before abstracting lists to sets.

#### Verification

- [x] Table tests cover eligible/ineligible placement, absent hosting versus explicit hosting, strict-domain refusal, last-voter constraints delegated to membership checks and tombstone refusal.
- [x] Plans retain exact identities and stable structure; unrelated labels/scopes do not create fictitious effects. Preview changes no durable or authority state.
- [x] Common checks pass with no newly enabled API falsely claiming execution before later task wiring.

## Remaining implementation tasks

### Task 5: Enforce initial scope and wire CAS, approval and exact retries

**Files:** active types/handlers/cluster/topology/properties, profile/test entry points, README/coverage (updated); authority/reads/transactions/operations/repair/scope modules and tests (new).
**References:** target §§1–6, §§8.1/8.7/10/12; §14 mutateAndMaybeApprove, approvePlan, retryOperation, quorumRead.

- [x] Add executable v1 scope assertions and request/configuration-domain checks: empty labels/selectors, no domain policy, one node type, fixed voter presets, always-good performance/stability/metric-freshness evidence, retained owned WAL, no lock/offline-recovery/manual-session/override/convenience-policy actions. Restrict generic Stage and creation inputs too; out-of-scope scenarios are harness exclusions, not simulated library refusals.
- [x] Separate Task 4 extension-only label/policy/pin-setter/rollback tests from required initial test entry points. Retain their history/code if useful without creating new extension implementation work. Update expected test lists and stale task references; do not suppress any retained-core failure.
- [x] Wire capture→submit→commit→local apply→reply/cancel/retry and the shared confirmed-metadata-read primitive. Capture convenience CAS once; preserve Stage's explicit CAS and original audit/predecessors across uncertain replay.
- [x] Implement guarded approve/retry with immutable plans, exact group scopes, first-attempt replay, one immediate failed-attempt successor and affected-block supersession. Stage success followed by approval failure preserves its staged version.
- [x] Wire ReseatReplica's distinct atomic repair/approval and independent-intent handling; never route it through an invented ordinary two-commit workflow. Leave physical revocation obligations for Task 6.

#### Verification

- [x] Scope tests show deferred data cannot enter active init/step through direct setters, Stage, AddNode or CreateGroup. Initial profiles select only retained-core evidence.
- [x] Common checks plus stale CAS, partial stage success, lost replies, exact approval/retry and disjoint-owner cases pass. A confirmed read waits for local apply; an observation does not grant authority.
- [x] Current profile bounds and source hashes match reports; no second cluster instantiation or fabricated success stub appears.

### Task 6: Enforce effect authority, resource fences and receipts

**Files:** authority/handlers/cluster/types/properties/tests (updated); negative-control cases (new).
**References:** target §§5.2/5.6/6.1/9/10; §14 executeAuthorizedEffect.

- [x] Implement grant→dispatch→consume→durable receipt→report→checkpoint with exact operation/version/attempt/step/action/identity/revision checks, including after waits.
- [x] Preserve group-wide versus host resources, effect/readmission receipts versus fence evidence, and manual transfer grants versus approved-step authority. Reuse primitives without erasing these semantic differences.
- [x] Keep revocation requested versus proven complete separate. Retain every required resource obligation across logical operation completion and unreachable hosts.
- [x] Replay exact durable receipts idempotently; refusal, partial acknowledgement or transfer-start cannot prove completion. Exercise stale sender/incarnation/delivery/receipt paths.
- [x] Extend failed-before-effect retry validation to the immutable remaining native plan and consumed receipt prefix; retain completed siblings and revoke outstanding predecessor effects before successor consumption.

#### Verification

- [x] Common checks pass; stale work cannot affect successors and duplicate delivery cannot duplicate effects.
- [x] Offline/native-quorum revocation remains pending with exact evidence retained.
- [x] Deliberately omitted revalidation/identity checks produce the intended counterexamples.

### Task 7: Model local ownership, retained lifetime and minimum sessions

**Files:** lifecycle.qnt/tests (new); shared types/handlers/cluster/properties (updated).
**References:** target §§5.4/6/8.6/8.8/9; §14 startAndPublishReplica, stopAndRetire.

- [x] Implement shared reserve/acquire/replay/authorize/publish and request-stop/join/release phases. Lifetime APIs reuse those phases; no separate constructor, accessor or DI state machine. Extend host-fence floor installation versus completion around older startup/action/Close ownership; Task 6 fixtures have no in-flight host callbacks.
- [x] Preserve exact local holds, explicit matching clearance, concurrent exact-start reservation, late successful factory ownership and failed/timeout Close ownership. Model callbacks as bounded outcomes, not Go threads or application implementations.
- [x] Add minimum internal session activation/renewal/expiry/exact-process recheck/revocation bound to durable identity and incarnation. Readiness requires admitted/recovered/current live session; neither health nor session state substitutes for quorum.
- [x] Retained restart restores valid storage with a fresh incarnation and must re-establish publication/authority. Do not introduce store-loss, memory/custom-store or offline-recovery paths.

#### Verification

- [x] Common checks and start/stop/cancellation/late-return/hold/failed-release scenarios pass without successor overlap.
- [x] Old-incarnation renewal or readiness acknowledgements cannot authorize the new process; expiry observation differs from committed revocation.
- [x] Early publication/release negative controls fail. No application lock or public manual-session workflow is activated.

### Task 8: Implement guarded membership and dynamic scaling

**Files:** `consensus/`, `topology/`, `metaengine/authority.qnt`, `metaservice/handlers.qnt`, relevant subsystem steps, `cluster.qnt`, `tests/` (updated); admission4 and scale5 instances/profiles (new).
**References:** target §§5.3/5.5/8.2/11; §14 membershipChange.

- [x] Implement guarded propose/commit/transfer with learner-first admission, catch-up, promotion, leader transfer before removal/demotion, last-voter and live post-change quorum checks. Keep precheck/proposal/commit/observation distinct.
- [x] Wire node add/drain/activate/exclude/force-retire and replica role changes through approved plans; every admitted node retains `_meta`. Distinguish explicit hosting from fixed voter targets and total host capacity.
- [x] Allocate fresh monotonic identities and exact predecessor captures. Retain intrinsic/role-derived learner pins without standalone pin API or label-based eligibility.
- [x] Add four-node learner/replacement and five-node scaling cases. Configure capacity five; use no AZ/label/type-placement scenarios. Initial metadata-only scale witness is extended to two loads in Task 12.

#### Verification

- [x] Common checks and named 3→4→5→4→3 cases pass; premature promotion, unsupported joint changes and unsafe leader removal refuse.
- [x] Quorum loss after precheck cannot fabricate commitment, and existing/role-derived pinned learners remain protected.
- [x] Manual transfer uses the shared mechanism with its proper authorization purpose.

### Task 9: Wire healthy/degraded creation and detached deletion

**Files:** `topology/`, `metaengine/authority.qnt`, `managed/`, `metaservice/handlers.qnt`, relevant subsystem steps, `cluster.qnt`, `tests/` (updated); deletion3 instance/profile (new).
**References:** target §§5.7/8.3–8.4/9/11; §14 approvePlan.

- [ ] Wire CreateGroup with fixed supported hosting/storage/voter presets, immutable founding authorization, exact local startup and convergence. Cover successful degraded placement and a required offline host that must wait.
- [ ] Commit current detached deletion through `_meta`, retaining complete exact former-owner debt and retiring the group ID atomically with logical completion.
- [ ] Keep native/host cleanup and revocation outstanding independently of logical deletion, including absent load quorum. Preserve unrelated group progress and disallow group-ID reuse through core Stage/create paths.
- [ ] Exclude EnsureGroup and historical required-stop deletion/replay-compatibility profiles from v1; do not create stubs or claim their correctness.

#### Verification

- [ ] Common checks and healthy/degraded create/delete cases pass through actual composed actions.
- [ ] Deletion can finish without load quorum while exact cleanup debt remains; no global instant stop is asserted.
- [ ] Missing former-owner debt is caught; existing independent groups remain capable of progress.

### Task 10: Complete cleanup, collection and returning-owner handover

**Files:** `managed/`, `metaengine/authority.qnt`, `metaservice/handlers.qnt`, `cluster.qnt`, `harness/properties.qnt`, `tests/` (updated).
**References:** target §§5.6–5.8/8.4/8.8/9; §14 stopAndRetire, collectRetiredReplica, readmitReturningNode.

- [ ] Keep native removal, actual joined stop, retirement and physical collection distinct; verify exact grant/identity/ledger/token/assignment even when data is absent.
- [ ] Preserve collection preparation before effect, durable completion before report and exact marker release before journal removal. Use abstract retention eligibility rather than retention-policy tuning.
- [ ] Wire ReadmitNode's successor grant, predecessor fence/join, complete exact retirement batch, meta catch-up and current assignments before readiness.
- [ ] Settle matching debt without reopening terminal deletion or resurrecting a group. Protect required history/proof and foreign/successor resources; offline hosts retain unconfirmed debt.

#### Verification

- [ ] Common checks plus offline return, delete/readmission overlap, partial batch, proof retention and process-crash-at-collection-boundary cases pass.
- [ ] Missing files, fresh IDs or bare verified flags cannot substitute for receipts; successor resources survive old cleanup.
- [ ] No dependency on deferred offline Recovery or disk-loss profiles is introduced.

### Task 11: Implement supported scheduler repairs with fixed policy

**Files:** `metaservice/scheduler.qnt`, scheduler tests under `tests/`, scoped3 profile (new); `cluster.qnt`, `types.qnt`, `harness/properties.qnt` (updated).
**References:** target §§5.3/5.4/8.5/10/11; §14 schedulerRepair.

- [ ] Implement capture, supported target-count/unavailability decisions, fresh revalidation and replicated repair CAS with exact group/source/owner/native identity. Fix lag/backlog within limits, progress healthy, metrics/current collector fresh, standby stability satisfied and cooldowns clear. Do not implement metric-fault generators, elapsed-window counters, no-progress repair or hysteresis scenarios. Actual unavailability remains explicit, with detection eligibility abstracted without timing.
- [ ] Retain RestoreMissingReplica and ReconfigureVoters, healthy-seat/pin preservation, scheduler enable/disable and per-group observation independence.
- [ ] Protect unrelated staged intent through the implemented rebase/typed-conflict path. Recheck actual leadership, availability, native membership and authority after delayed decisions; good metrics do not make an outdated authorization valid.
- [ ] Model stale/invalid supported-decision proposals to test core rejection; do not implement custom policy code, custom workload/resource-balancing policy, labels/domains or runtime tuning.

#### Verification

- [ ] Common checks and concurrent user-operation/repair, disable/re-enable, leader change and outdated-decision cases pass; metric/stability inputs remain favorable in every active state.
- [ ] Removed revalidation is detected by a negative control; callbacks cannot approve foreign intent or bypass membership safety.
- [ ] One unavailable group does not globally block unrelated healthy work under the stated resources/fairness.

### Task 12: Close retained API coverage and derived operator visibility

**Files:** coverage.md/regressions.md, retained test entry points, profiles and display/projection helpers (updated/new).
**References:** target §§6.2/8.7/10/11–13; all 82 ledger dispositions.

- [ ] Audit all public entries against the live interfaces, but require executable coverage only for Core controls and retained Primitive semantics. Observe entries map to derived views; Omit/Deferred entries need reasons, not fake implementations.
- [ ] Complete integrated healthy/degraded group/node cases, two-node unavailability, independent channels/groups, 3→5→3 with two groups, manual/scheduler races and returning owners.
- [ ] Add accepted/refused/waiting/uncertain outcomes for retained mutation families. Check version/predecessor/partial-commit results and ensure action witnesses use composed entry points.
- [ ] Expose concise derived trace summaries: Latest/Target, operation/block/step, node/member/replica/incarnation, native membership, held/parked owners, fences, cleanup debt and blockers. Do not implement metrics, watch streams, query pagination or polling loops.
- [ ] Write manual Go regression recipes using existing owning-package/Docker/Gosim boundaries. No automated replay, dashboard or new application fixture is required.

#### Verification

- [ ] All 82 entries have accurate dispositions/current evidence; every retained action/mechanism has a witness or appropriate refusal case.
- [ ] Common checks and integrated cases pass; derived visibility cannot leak global truth into actor decisions.
- [ ] No claim for labels, locks, EnsureGroup/RollbackTo, policy tuning, memory/lost storage or offline Recovery is inferred from old helpers/tests.

### Task 13: Qualify stuck, fair-progress and core recovery checks

**Files:** `harness/properties.qnt`, `tests/`, progress_small profile/backend controls (new/updated).
**References:** target §§7/10.2–10.3/11–13.

- [ ] Implement pending obligations, expected blockers, enabled useful actions and unexpected-stuck checks. Distinguish terminal/bound-exhausted states from internal deadlock; irrelevant getters/rejects/fault toggles cannot conceal a stuck obligation.
- [ ] Stabilize only promised retained-storage resources/connectivity/callbacks and stop new disruptive work. Do not restore lost data or invoke deferred offline Recovery.
- [ ] Apply per-obligation fairness and check eventual core completion/convergence; global-step fairness is insufficient. Preserve expected quorum/hold/retention/callback blockers.
- [ ] Qualify the pinned backend with known-pass and known-fail safety, stuck and temporal cases; the TLC adapter disables built-in deadlock reporting.
- [ ] Check supported core API recovery sequences from selected reachable stabilization states. Label witnesses versus recovery under a particular policy; failure does not prove every alternative path impossible.

#### Verification

- [ ] Known stuck/livelock/temporal-failure controls fail for the intended reason, while passing controls and common tests pass.
- [ ] A tractable progress_small instance completes its required finite/qualified temporal checks; incomplete or unsupported is not verified.
- [ ] Permanent absent quorum is reported as an expected blocker/out-of-scope disaster assessment, not successful rebuild or false internal deadlock.

### Task 14: Qualify core negative controls and six substantive profiles

**Files:** `harness/properties.qnt`, `tests/`, `instances/`, `profiles/`, coverage/reports (updated).
**References:** target §§10–13; six substantive profiles plus foundation in §11.

- [ ] Finish faulty variants for stale authority, lost debt, successor receipt confusion, early publication/release, false transfer completion, foreign-intent approval and false collection proof.
- [ ] Calibrate/freeze finite identity/version/attempt/message/history bounds and only the minimum retained lifecycle/session phases so each named in-scope scenario fits. Keep search bounds separate from replicated capacities; never wrap identities or discard debt to fit.
- [ ] Execute retained deterministic tests, seeded samples, designated bounded safety checks and the tractable finite profile. Do not build storage3/recovery3/sessions3 or label/domain profiles.
- [ ] Record per-profile scope closure and guarantee/completeness. Expected negative controls must fail their intended property; parser/runtime/tool failures do not count as detection.

#### Verification

- [ ] Required normal checks pass and negative controls fail correctly; actual constants, hashes and manifests match.
- [ ] No deferred path is active via generic Stage, defaults or initialization; current scope assertions run in each profile.
- [ ] Reports separate sampled, bounded, exhaustive-complete, counterexample and incomplete outcomes; no exhaustive five-node promise.

### Task 15: Integrate opt-in Make targets and failure-preserving reports

**Files:** Makefile, runner/tests/profiles/README (updated).
**References:** target §§6.3/11/13.

- [ ] Add test-quint, test-quint-simulate and test-quint-verify with explicit profile/seed/report-root forwarding; keep existing default Go test dependencies unchanged.
- [ ] Default test-quint runs retained deterministic suites, runner tests and smoke simulation only. Extension-only tests and deferred profile names are not hidden default requirements.
- [ ] Qualify timeout/signal/child-process cleanup, raw exit/log/trace preservation, zero/missing tests, unavailable backend, report failure and latest publication. No automatic installation or download.
- [ ] Exercise real Make commands and report-root overrides; preserve precise source/profile/hash provenance and derived trace summaries.

#### Verification

- [ ] Runner and existing shared Python tests pass; real default/simulation/designated-small-verification targets work.
- [ ] `make -n test` confirms unchanged Go test dependencies; `git diff --check` passes.
- [ ] Interrupted, unsupported or incomplete checks cannot be relabeled successful or replace evidence dishonestly.

### Task 16: Verify the architectural delta and wiring

**Files:** implementation audit/fixes, plan/progress (updated).
**References:** revised target all sections and coverage ledger. Third-to-last task.

- [ ] Audit scope/types/domains: labels/policy extensions, locks, storage loss and offline Recovery are excluded from active initial profiles, not merely absent from the method list.
- [ ] Audit transaction/planner/API semantics: exact CAS, partial outcomes, immutable retries, scoped ownership, required/cleanup plans and retained controls match the revised target.
- [ ] Audit consensus/network/lifecycle/minimum sessions: local versus committed views, native quorum, ownership/publication/holds/join and exact process fencing remain present.
- [ ] Audit authority/cleanup/readmission: exact grants/fences/receipts, independent debt and collection ordering are not weakened by wrapper removal.
- [ ] Audit scheduler, shared reads and derived visibility: fixed-policy revalidation/independence and non-omniscient actor inputs; no redundant API machines.
- [ ] Audit properties/profiles/tooling/evidence: retained witnesses, negative controls, qualified progress, accurate reports and manual recipes; no production DB/cache/API/dependency changes.
- [ ] Log every real initial-scope gap in NOT IMPLEMENTED GAPS, fix and validate it or explicitly mark unresolved. Deliberate approved exclusions are not gaps to implement.
- [ ] **Verify actual wiring last:** Make/runner → scoped concrete instance → one init/step → real handlers/pure mechanisms → exact effects/completion; selected scope/safety/progress properties and retained tests execute. Definitions and test-only helpers are not sufficient wiring.

#### Verification

- [ ] All retained suites pass; rerun affected sampled/bounded/finite checks after fixes and preserve completeness labels.
- [ ] Delta report distinguishes actual missing core requirements from approved deferred features.

### Task 17: Run visible external review and fix confirmed findings

**Files:** implementation fixes, revmux artifacts, review report, plan/progress (updated).
**References:** revised target/ledger/plan, project rules, library Design constraints and .revmux/profile.md.

- [ ] Read current revmux/Paseo skills and preflight/profile configuration. Scope the review to this feature against the recorded baseline, including material uncommitted fixes.
- [ ] Provide the original goal, approved lean scope, historical completed work, revised target, ledger and validation evidence. Explain scoped-concurrency versus the older global gate; do not treat deferred labels/locks/Recovery/wrappers as missing initial features.
- [ ] Create a new visible Paseo terminal in this worktree and run revmux there with separate report/progress output. Retain task/run/terminal/report IDs. Use the documented fallback only if revmux is unavailable.
- [ ] Review abstraction soundness, scope leakage, false completeness, vacuous properties, wrong atomic boundaries and omitted core identity/cleanup/liveness behavior. Confirm findings, fix major/high issues and re-review material fixes.

#### Verification

- [ ] A real visible review completes with explicit degradation/completeness; no missing/partial review is called clean.
- [ ] Confirmed major/high findings are fixed and affected validation passes; minor/unresolved findings remain visible. Link the report instead of copying it into chat.

### Task 18: Close gaps and document the initial deliverable

**Files:** model README/coverage/regressions, relevant project/test docs, changelog, plan/progress (updated/new).
**References:** revised target §§1–3/6/10–15; project documentation rules.

- [ ] Close every genuine initial-scope gap, validate fixes and preserve explicit unresolved markers where necessary. Do not turn exclusions into implementation tasks or tick incomplete core work.
- [ ] Document exact retained scope, constants/presets, commands/profiles/bounds, result interpretation, operator trace output and the deliberate expansion procedure for deferred features.
- [ ] Update project-map/testing pointers and concise user-facing changelog. Correct stale recovery-availability documentation without claiming Recovery was modeled. Keep the known global-operation design discrepancy explicit.
- [ ] Remove stale references to the old 22-task/full-API requirement from active guidance; retain historical evidence accurately. Verify paths, API names, test selection and unchanged default Make behavior.
- [ ] Run final applicable checks and record source/hash-qualified evidence and limitations. Commit the completed documentation/fixes; no push/publish without instruction.

#### Verification

- [ ] Whitespace/links/commands, retained deterministic suites, runner tests and default model smoke pass.
- [ ] Required core components are wired, scope closure holds, and no unresolved core gap is hidden behind deferred-feature labels.
- [ ] Final report states which properties were checked at which bounds and expressly excludes deferred features and Go-conformance proof.

## Validation Commands

These are future execution gates, not results of this documentation-only scope review.

```sh
python3 -m unittest discover -s scripts/quint -p '*_test.py'
python3 scripts/quint/run.py --profile foundation --mode test
python3 scripts/quint/run.py --profile smoke3 --mode test
python3 scripts/quint/run.py --profile smoke3 --mode simulate --seed 1
```

Run focused retained profiles as introduced: admission4/scale5 in 8, deletion3 in 9, scoped3 in 11, progress_small in 13. Task 5 first normalizes existing wider test selections. Do not add deferred profile placeholders or count extension-only tests as initial coverage.

After backend qualification in 13–14:

```sh
python3 scripts/quint/run.py --profile progress_small --mode verify
python3 scripts/quint/run.py --profile scoped3 --mode verify
python3 scripts/quint/run.py --profile deletion3 --mode verify
```

After integration in 15:

```sh
python3 -m unittest discover -s scripts -p '*_test.py'
make test-quint
make test-quint-simulate QUINT_PROFILE=scoped3 QUINT_SEED=1
make test-quint-verify QUINT_PROFILE=progress_small
make -n test
git diff --check
```

Each verification result records initial scope, bound/initialization/step assumptions, properties, backend/version, depth where applicable, completion and source hashes. No empty/unfinished/unsupported run is success. No blanket Go/race/Docker/performance requirement arises from this model-only plan; apply repository checks if an explicitly logged deviation changes that code/tooling.

## NOT CONSIDERED & TODOs

| Tag | Feature | Treatment |
| --- | --- | --- |
| [postponed] | Labels/selectors/domains/reserved/learner-only labels and heterogeneous type placement | Completely exclude active inputs and profiles, including indirect Stage/create paths. |
| [postponed] | Live placement/voter tuning, standalone pin setter | Fixed presets; retain explicit hosting, roles and internal pin safety. |
| [postponed] | Metric lag/backlog, no-progress, freshness/gaps and stability/cooldown timing | Always-good inputs; no metric-fault state dimensions or timing/hysteresis claims. |
| [postponed] | EnsureGroup/RollbackTo | Core stage/create/approve/retry only. |
| [postponed] | Manual sessions, locks, serving override | Minimum internal session/readiness behavior only. |
| [postponed] | Storage-loss/corruption/memory/custom-store and offline Recovery | Retained valid WAL, current lineage; no recovery proof or fake API refusal. |
| [postponed] | Historical required-stop deletion/replay compatibility profiles | Current detached semantics; preserve Go compatibility fixtures unchanged. |
| [omitted] | Metrics/history/watch/getter/constructor plumbing | Derived observations where useful; no feature task. |
| [postponed] | Automated Go replay | Manual regression recipes. |
| [potential-improvement] | Strict global operation gate | Implemented scoped baseline remains. |
| [too-complex] | Concrete Raft/payload/byte-level/OS/crypto internals | Approved protocol abstractions only. |

Deferred cases need scope comments/documentation, not new production NotImplemented/WARN behavior. Known defects must not be reclassified as exclusions to obtain a green model.

## NOT PLANNED CHANGES

Task 1 used `kind` because Quint reserves `action` and used Choreo pure helpers over one application-owned world. Tasks 3–4 refined source-grounded state/placement/plan schemas; see the execution log and target history. This scope revision is explicitly user-approved, not an unplanned implementation change. No executable model or production files changed in the scope-review step. Task 5 subsequently added pure sequencing/repair helpers and the read/proposal correlation fields required by the source. It also corrected reseat validation of independent staged allocation floors and kept hosted-node normalization in core tests using a fixed policy fixture. Production Go remains unchanged.

Task 8 brought forward the minimal granted load-stop/debt path needed by metadata evacuation barriers; retirement, collection and full readmission remain Tasks 9–10. It also added explicit long-witness REPL routing and frontend heap settings after reproducing Quint trace-export limits; root Make integration remains Task 15. QCM-001 is an explicitly retained source-grounded progress finding, not an omitted model mechanism or a passing recovery claim.

## NOT IMPLEMENTED GAPS

- [x] Scope normalization completed in Task 5: executable feature guards/safety assumptions, explicit retained-core test imports, and separate extension-only tests. Later task machinery must retain these assumptions.

Implementation remains incomplete. Tasks 9–18 retain group workflows, retirement/collection/readmission, scheduling, coverage and progress/backend qualification. Task 8 checks and its explicit known-finding record are complete. QCM-001 remains a reported model reproduction with Go regression pending. Deferred/Omit features are not hidden unfinished tasks.

## Scope-review validation

All 82 public entries are retained exactly once with explicit disposition. Completed Tasks 1–4 are preserved; active tasks are 5–18. The last three are architectural delta (16), external review (17), documentation/gap closure (18). This review validates documentation/scope consistency only; no model test or verification result was rerun or upgraded.
