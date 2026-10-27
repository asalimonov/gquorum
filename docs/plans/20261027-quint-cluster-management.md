# Implementation plan: Quint cluster-management specification

**Plan:** `docs/plans/20261027-quint-cluster-management.md`
**Date:** 2026-10-27
**Project source:** `../..` relative to this plan; repository root `.`
**Branch:** `feat/quint-spec`
**Source revision inspected:** `e8e85dbd7879b1c858252cd2a6126a36920f8a8e`
**Target architecture:** [20261027-quint-cluster-management-target.md](../brainstorms/20261027-quint-cluster-management-target.md)
**Target SHA-256 at planning:** `64b1a24b8068997529445ce6e92c44c12a8ebb455a5d728cb34ba0533c64ff9b`
**Full brainstorm log:** [20261027-quint-cluster-management.md](../brainstorms/20261027-quint-cluster-management.md)
**Execution progress:** `docs/progress/20261027-quint-cluster-management.md` (create during execution).
**Status:** executing; Tasks 1–2 complete, Tasks 3–22 pending. The user approved this sequence during planning.

## Objective and execution contract

Implement the accepted executable specification under `scripts/quint/`: cluster management, all public administration families, automatic repair, healthy/degraded creation/deletion, dynamic 3–5-node membership, storage loss and offline recovery. Abstract Raft consensus and application payloads. Keep management messages, authority, local application, callback ownership and durable evidence explicit. Produce manual Go regression recipes, not an automated implementation replay adapter.

The target is the design authority. References below identify its numbered sections and named algorithms in §14. The source correspondence matrix connects model actions to current implementation; a model invariant is not proof that Go conforms. Never repair a discrepancy by silently granting the model stronger synchronization, authority or observations than the implementation has.

Production Go packages, module dependencies, persisted encodings, application APIs, frozen fixtures and transport ports are **UNCHANGED by this plan**. Adding the model requires no DDL, ORM, bbolt schema, cache or migration work. Model types live in Quint. Root Makefile integration remains opt-in and does not add work to `make test`.

Complete tasks in order, validate and commit each separately, and append results to the progress file. Target roughly 300–1,400 newly authored model/test/runner lines per implementation task; vendor copying is exempt. Split supporting work before starting a task if measured scope exceeds that bound, retaining runnable checkpoints and the final three-task order. Record deviations and concrete gaps below. Do not mark unexecuted tests or unresolved obligations complete.

## Planning reconciliation

| Finding / existing decision | Plan consequence |
| --- | --- |
| User selected implemented scoped concurrency despite library.md and review profile's global gate | Preserve independent operation ownership; review brief explicitly records this accepted baseline. A stricter global gate is deferred. |
| User approved abstract Raft and Choreo | Use explicit management messages and an abstract quorum service; do not implement election/log internals. |
| User approved storage-loss/recovery profiles and declined automated Go replay | Tasks 13–15 model recovery; Task 16 writes manual recipes only. |
| Target §15 postpones enum detail and bound calibration to implementation | Those are required implementation tasks here, not excluded features. Only genuine scope exclusions remain in the final TODO table. |
| `topology/storage_class.go` permits only default/memory | Target §§5.3–5.4 corrected: group storage is default/memory; `NodeStorageConfig.defaultBackend` is OwnedWAL/BorrowedCustom. Custom donor recovery refusal is separate. |
| Legacy groups can omit Hosting | Target §5.3 corrected to `hosting: Option[HostingRule]`; preserve absent versus explicit/all-eligible cases. |
| Group-wide actions and readmission/fence proof differ from replica effects | Target §§5.2/5.6 corrected with `EffectTarget` variants, optional action/admission fields and separate `FenceEvidence`; resource proof cannot be a bare boolean/set membership. |
| Local Quint 0.33.0 supports Apalache/TLC; its TLC adapter disables built-in deadlock reporting | Require explicit stuck predicates and passing/failing backend calibration; never rely on built-in deadlock defaults. |
| CLI seed can change the default sample count to one | Every simulation command supplies both seed and sample count; manifests record both. |
| Root tests do not cover a future nested Python/Quint directory | Add explicit runner unit discovery and Quint targets; no claim that root Go tests exercise the model. |

Only the existing target's schema clarifications were edited during planning; no new proposal document is created. The skill-requested planning commit includes this plan and its exact target/log dependencies, explicitly adding those three ignored files without changing `.gitignore` or staging unrelated files.

## Runnable checkpoints and common verification

Task 1 is a minimal executable vertical slice: exact types, one node actor, one delivered management request, one observation/refusal, one test and one invariant. Later tasks extend the same composed model. There must be no multi-task period where the model only typechecks or uses uninitialized constants. Types/helpers for future tasks may be declared, but future handlers stay absent/disabled with their implementing task recorded; never return fabricated success to make a scenario run.

The shared entry point is `cluster.init/step`. Concrete instances bind constants exactly once. `properties.qnt` defines reusable predicates over typed records; a concrete wrapper owns the named state-reading invariants/temporal properties and imports the one instantiated cluster. Avoid two independent cluster instances or circular property/protocol imports. Main modules match file stems unless an explicitly documented test wrapper names another main.

For every model-changing task: typecheck changed modules and all currently runnable instances; run all existing deterministic Quint tests; run focused new witnesses/refusals and the seeded `smoke3` safety simulation. During Task 1 use the foundation instance; Task 3 introduces `smoke3`. Newly added tests must run through public model actions; adversarial fixture injection is isolated and labeled. `quint run` passing does not establish exhaustive coverage.

For Python runner changes: run its full unit suite, syntax checks, and a real registered-profile smoke. Fix failures in the suites run, including unrelated failures, before proceeding; record unrelated fixes. Do not mask failures as skips or introduce broad production changes to satisfy a model defect. Go suites/race/Docker/performance are not blanket per-task commands for a model-only change. If a justified deviation touches Go/shared runners, run the repository checks required for that deviation and log the scope.

## Task overview

| Task | Deliverable | Approximate authored LOC |
| --- | --- | --- |
| 1 | Executable types/Choreo foundation | 500–1,200 |
| 2 | Versioned profile runner and reports | 500–1,100 |
| 3 | Independent consensus, network faults and local views | 700–1,400 |
| 4 | Topology edits, placement and pure planning | 700–1,400 |
| 5 | CAS, approval, supersession and exact retries | 700–1,400 |
| 6 | Effect authority, fences and receipts | 700–1,400 |
| 7 | Local startup, holds and joined teardown | 600–1,300 |
| 8 | Membership, node admission and scale transitions | 700–1,400 |
| 9 | Group creation/ensure and detached deletion | 600–1,300 |
| 10 | Cleanup, collection and returning-owner handover | 700–1,400 |
| 11 | Scheduler capture, decisions and revalidation | 600–1,300 |
| 12 | Sessions, locks, reads and local switches | 600–1,300 |
| 13 | Storage loss, restart and fresh-identity repair | 500–1,100 |
| 14 | Recovery inspection and exact acceptance | 500–1,100 |
| 15 | Recovery execution, cancellation and startup consumption | 700–1,400 |
| 16 | Complete API coverage and manual regression recipes | 400–1,000 |
| 17 | Explicit deadlock/liveness and recoverability checks | 600–1,300 |
| 18 | Negative controls and full verification matrix | 500–1,200 |
| 19 | Make integration and final reporting qualification | 300–900 |
| 20 | Architectural delta verification | audit + focused corrections |
| 21 | Visible external review and confirmed fixes | review + focused corrections |
| 22 | Final gap closure, documentation and changelog | documentation + focused corrections |

## Task List

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

- [ ] Implement `hasCommitPath(consensus, nodes, links): bool`, abstract leader authority/election, commit/apply positions and catch-up classes. Use exact native voter identities and valid durable evidence; observations/session health cannot create a commit certificate.
- [ ] Add directed independent meta/load link sets and `deliver/drop/duplicate/setLink` actions. Intercept Choreo sends as explicit queued management effects; partitioned messages cannot teleport to mailboxes. Model duplicate delivery without changing request/effect identity.
- [ ] Add `crash/restart`, fresh incarnations and retained valid disk for this task. Keep metadata commitment separate from per-node local apply; capture local read views, stale observations and old-process messages.
- [ ] Create `smoke3` with three nodes and two exact preexisting load groups as a validated initializer. Add an abstract load-commit token so load progress can be witnessed without modeling payloads. Creation of new groups remains Task 9.
- [ ] Add a non-omniscience check: handler inputs expose applied/cached values and exact supplied proofs; current shared metadata/native state is accessed only by the specified consensus/environment transitions.

#### Verification

- [ ] Common checks pass. Witness delayed local apply, asymmetric isolation, leader replacement, fresh restart incarnation and independent load commits while `_meta` cannot commit.
- [ ] With two of three voters unavailable, new commits are refused/blocked; a learner cannot substitute for a vote. Meta and load partition tests have distinguishable outcomes.
- [ ] Seeded `smoke3` exercises crash/partition/recovery actions without artificial identity reuse or undefined accesses.

### Task 4: Implement document edits, placement and pure plan construction

**Files:** `topology.qnt`, `topology_test.qnt` (new); types, shared tests and coverage (updated).
**References:** target §§5.2–5.3/6/6.2/8.1–8.3; topology schemas and `makePlan` interfaces; source `topology/{document,hosting,placement,voter_config,plan}*.go` and `cluster_mutations.go`.

- [ ] Implement `validateDocument`, `mutationGuard`, `mutateDocument`, `affectedScopes`, `eligibleHosts`, `makePlan` and `preview` with the target signatures and narrower typed input records. Guard and update functions remain separate.
- [ ] Define named request variants for every document mutation, including full Stage, placement/hosting/voter config, pins, roles, reseat, group/node edits, forced retirement and RollbackTo. Use exact fields from target §6.2; full-document labels remain Stage edits.
- [ ] Model bounded representative node types, selectors, reserved/learner-only hosts, explicit/all-eligible/legacy hosting, voter domains and pinned learners. Capacity limits are replicated inputs distinct from search bounds.
- [ ] Plan only declared affected scopes with ordered step/guard dependencies; preserve immutable source identities, allocation floors and retired IDs. Keep legacy group grammar distinct where it changes behavior.
- [ ] Make preview pure, reject preview+approve, validate malformed ID/role/options and immutable storage class. Full shape validation catches sorted/unique input requirements before abstracting lists to sets.

#### Verification

- [ ] Table tests cover eligible/ineligible placement, absent hosting versus explicit hosting, strict-domain refusal, last-voter constraints delegated to membership checks and tombstone refusal.
- [ ] Plans retain exact identities and stable structure; unrelated labels/scopes do not create fictitious effects. Preview changes no durable or authority state.
- [ ] Common checks pass with no newly enabled API falsely claiming execution before later task wiring.

### Task 5: Wire staged CAS, scoped approval, supersession and retries

**Files:** `authority.qnt`, `authority_test.qnt` (new); cluster/handlers/types/properties/tests (updated).
**References:** target §§5.6/6/6.1/8.1/9/10.1; §14 `mutateAndMaybeApprove`, `approvePlan`, `retryOperation`; `internal/metaengine/scoped_{documents,approval,begin,retry}*.go`, `cluster_documents.go`, `cluster_mutations.go`.

- [ ] Implement `canApprove/approve`, `canRetry/retry` and capture→submit→commit→apply→reply actions. Zero expected version captures local Latest once; preserve original audit/CAS across uncertain replies.
- [ ] Separate stage and approval commitments. Return the staged version when approval fails; cancellation/lost reply does not undo commitment. Repeated Approve returns the retained first attempt; exact Retry creates at most one immediate successor of an eligible failed attempt.
- [ ] Maintain Latest/Target, per-group proven declarations/source versions, independent owners and operation block state. Supersede only affected blocks; prepare exact revocation obligations for Task 6 rather than assuming supersession instantly stops resources.
- [ ] Preserve the complete immutable plan/version/attempt/predecessor chain and all-or-nothing candidate-state validation. Keep FullyConverged historical, not a global controller cursor.
- [ ] Add explicit refusal/result states for conflict/invalid/unavailable/authorization cases and a caller-only uncertainty outcome; domain effects are judged by actual commit state, not the response.

#### Verification

- [ ] Common checks and scenarios for stale CAS, stage-success/approval-failure, lost reply, replayed approval, concurrent exact retry and rollback-to-new-intent pass.
- [ ] Two disjoint owned operations can coexist; a changed plan or predecessor cannot be replayed as an exact retry. A stale caller cannot approve whichever version is currently Latest.
- [ ] Source-equivalent scope supersession retains pending physical revocation instead of forging its completion.

### Task 6: Enforce exact effect authority, fences and durable receipts

**Files:** authority/handlers/cluster/types/properties and authority tests (updated); initial `fault_detection_test.qnt` (new).
**References:** target §§5.2/5.6/6.1/9/10.1/10.4; §14 `executeAuthorizedEffect`; `topology/authority*.go`, `internal/{operator,authoritystore,metaengine,managed}` authority/effect paths.

- [ ] Implement `validateEffect`, `installFence`, `acceptReceipt`, `revocationComplete` and separate grant→dispatch→receive→consume→persist→report→checkpoint actions.
- [ ] Match complete operation/version/attempt/step/action/identity/grant-revision tuples at the real consumption/checkpoint boundaries, including revalidation after waits. Preserve group-wide target sets and exact host/member/replica targets.
- [ ] Model action receipts, readmission receipts and fence evidence with their respective source shape validations; keep every required resource's installed proof, including stale or missing evidence refusals.
- [ ] Return durable prior receipts for exact retries without duplicate effects; incomplete/refused/transfer-started acknowledgements cannot prove finished work. Failed native/host operations preserve their outstanding obligations.
- [ ] Add stale grant, delayed predecessor receipt, successor identity and offline revocation scenarios. Introduce an isolated negative control omitting identity/grant revalidation; never put that bypass in normal profiles.

#### Verification

- [ ] Common checks pass; exact duplicate work is idempotent and stale deliveries cannot mutate successors.
- [ ] Revocation remains pending without its required host/native evidence, including when the originating operation is logically terminal.
- [ ] The negative control produces the intended invariant counterexample; ordinary profiles reject the same stale message.

### Task 7: Model local ownership, startup publication, holds and teardown

**Files:** `lifecycle.qnt`, `lifecycle_test.qnt` (new); shared composition/properties/types (updated).
**References:** target §§5.4/6/8.6/9; §14 `startAndPublishReplica`, `stopAndRetire`; `node_lifecycle.go`, `node_load.go`, `internal/managed` startup/stop paths and lifecycle hooks.

- [ ] Implement `canReserve/reserve`, `canPublish/publish`, `requestStop`, `canJoin/join` against exact OwnerState and storage configuration/evidence. Concurrent exact starts share one reservation/result; another identity waits for release.
- [ ] Separate factory return, replay, authorization, publication, stop request, engine/callback join and application close. Failed factories clean their own partial acquisitions; successful late factories transfer ownership even if their waiter canceled.
- [ ] Preserve local exact StopLoadGroup holds, explicit matching-start clearance and retirement eligibility while held. Never redirect a stopped handle to a successor.
- [ ] Model Node Start/WaitReady/RequestStop/Stop independently, startup context lifetime, failed/timeout Close retaining ownership and serialized retries. Hooks may delay/fail; readiness/retention/removal share their actual runner constraints.
- [ ] Include owned/borrowed resource markers at the abstraction needed to prevent premature release; application callbacks observe copies, and reentrant RequestStop cannot synchronously join itself.

#### Verification

- [ ] Common checks plus late factory success, canceled start waiter, concurrent exact starts, stop-during-replay, failed Close retry and local hold scenarios pass.
- [ ] Deliberately early publication/ownership release is caught by negative controls. A refused startup preserves foreign/missing evidence for Task 13.
- [ ] Every publication has replay and exact authorization evidence; replacement cannot overlap unresolved ownership.

### Task 8: Implement native membership and dynamic node scaling

**Files:** membership/topology/authority/handlers/cluster tests (updated); `instances/{admission4,scale5}.qnt`, matching profiles (new).
**References:** target §§5.3/5.5/6/6.2/8.2/11; §14 `membershipChange`; `membership/safety.go`, `topology/voter*.go`, metadata admission and current cluster membership tests.

- [ ] Implement `canProposeChange/proposeChange`, `canCommitChange/commitChange`, `canTransfer/transfer`: learner-first addition, caught-up guarded promotion, transfer before leader demotion/removal, last-voter and live post-change quorum rules.
- [ ] Keep safety precheck, proposal, native commitment and observed completion separate. Partitions between them may stall safely; do not assume an invariant that quorum always survives subsequent arbitrary faults.
- [ ] Wire AddNode, DrainNode, ActivateNode, ExcludeNode and ForceRetireNode through approved plans and local effects; retain every admitted node's `_meta` assignment independent of load hosting eligibility.
- [ ] Allocate monotonic fresh identities, model admission reservations and exact predecessor captures. AddNode/basic admission runs now; returning-owner full receipt batches are completed in Task 10.
- [ ] Build named four-node learner/replacement and five-node 3→4→5→4→3 witnesses, explicitly configuring capacity five and distinguishing number of hosts from number of voters. Use a metadata-only scaling witness until group cleanup is implemented; Task 16 must extend scale5 to the full two-load-group sequence. Mark that intermediate limitation in the profile.

#### Verification

- [ ] Common checks plus ordinary three-node membership cases pass; named admission4/scale5 deterministic traces run through the shared action entry points.
- [ ] Invalid joint/multi-change, premature promotion, pinned learner promotion and leader removal without transfer are refused.
- [ ] Losing quorum after precheck cannot commit/acknowledge a fictional native change; growing a cluster never creates a metadata-free admitted node.

### Task 9: Implement group creation, EnsureGroup and detached deletion

**Files:** topology/authority/lifecycle/handlers/cluster tests (updated); `instances/deletion3.qnt`, profile (new).
**References:** target §§6.2/8.3–8.4/9/11; §14 `approvePlan`; `cluster_ensure*.go`, `internal/metaengine/scoped_deletion*.go`, group planning and `tests/cluster/delete_{wait,recovery}_test.go`.

- [ ] Wire CreateGroup founding through placement, immutable plan, approved startup and convergence; model both successful degraded placement and waiting for a specifically required offline founder.
- [ ] Implement EnsureGroup's composed read/create/stage/approve/wait workflow: exact spec/actor/comment recognition, original CAS retention, conflict for foreign staged edits or pending deletion; historical convergence alone does not mean a local replica is running.
- [ ] Model new detached deletion as one metadata commitment publishing a retired ID, full former-owner debt and logical completion. Keep physical/native stop and revocation outstanding, even without load quorum.
- [ ] Preserve retained legacy required-stop deletion approvals as a bounded legacy-plan initializer/semantics case when they remain executable in the inspected code; do not silently migrate them or treat old plan steps as new detached behavior.
- [ ] Prove former-owner coverage from source obligations, not the candidate debt's own count. Prevent reuse of retired group IDs, including RollbackTo and EnsureGroup paths.

#### Verification

- [ ] Common checks and healthy/degraded create/ensure/delete scenarios pass; an independent group commits during another group's logical deletion.
- [ ] Deletion succeeds through metadata with load quorum absent, while its old owners still have exact pending obligations; no instantaneous global serving cutoff is asserted.
- [ ] Omitted former-owner debt triggers a negative invariant; old required-stop plans remain waiting until their actual required proof arrives.

### Task 10: Complete retirement, collection and readmission handover

**Files:** lifecycle/authority/handlers/cluster/properties/tests (updated).
**References:** target §§5.6–5.7/8.4/9; §14 `stopAndRetire`, `collectRetiredReplica`, `readmitReturningNode`; `internal/managed/{collection*,readmission*}`, `internal/authoritystore/{collection*,rebind*}`, metadata cleanup/readmission paths.

- [ ] Implement `canRetire/retire`, `canCollect/collect` with full exact grant/identity/storage/assignment/ledger/token evidence; already absent data does not bypass authorization.
- [ ] Split removed, actual joined stop, retired and physically collected milestones. Keep preparation durable before collection, completion durable before report, and exact marker release before journal-row removal.
- [ ] Wire ReadmitNode through successor grant, non-serving predecessor fencing/join, all required retirement receipts, complete batch consumption, new meta catch-up and current load assignments.
- [ ] Settle matching deleted-group debt from complete readmission evidence without reopening a terminal deletion or resurrecting assignments. Preserve foreign/successor resources and retained proof through bounded history pruning.
- [ ] Treat offline host/permanently lost resources as unconfirmed debt, not a waiver. Add crash/resume actions at every durable collection/handover boundary and retention defer/release events.

#### Verification

- [ ] Common checks and deletion3 traces for offline return, delete-during-readmission, partial batch refusal, retained proof pruning and collection crash/resume pass.
- [ ] Bare verified flags, absent files, wrong member incarnation and incomplete receipt batches cannot settle debt.
- [ ] Marker release precedes debt deletion; retained successor storage survives predecessor cleanup retries.

### Task 11: Implement scheduler decisions and post-callback revalidation

**Files:** `scheduler.qnt`, `scheduler_test.qnt`, `instances/scoped3.qnt`, profile (new); composition/types/properties (updated).
**References:** target §§5.6/6/8.5/10.1/11; §14 `schedulerRepair`; `scheduling/{default_repair,voters,scheduler}.go`, `internal/scheduling/scoped_*.go`, `internal/metaengine/scoped_repair_test.go`.

- [ ] Implement `capture`, `chooseRepair`, `canRevalidate`, `makeRepair` and `repairIndependent` using exact captured group/source/owner/native identity and bounded stability/cooldown observations.
- [ ] Split observe, callback decision, fresh scope/health/native/leadership revalidation and replicated repair CAS. Model no action, RestoreMissingReplica and ReconfigureVoters; invalid custom-policy output has an explicit refusal path.
- [ ] Preserve pinned learners, healthy seats, configured `_meta`/load voter roles, placement domains and held identities. Missing one group's observation is not a global scheduler stop.
- [ ] Model independent staged-intent rebase and typed RebasedTo outcome without approving the user's staged changes. Retain exact captures after callback waits and reject stale decisions rather than selecting the newest Latest.
- [ ] Add independent user-operation/repair witnesses grounded in `TestScopedRepairLeavesIndependentUserOperationOwned`, plus disable/re-enable, leader loss, callback timeout/nonreturn and fresh replacement races.

#### Verification

- [ ] Common checks and scoped3 runs pass; unrelated staged intent survives repair and unrelated owned work retains its operation identity.
- [ ] Stale callbacks cannot change a successor, promote a pinned learner or bypass native safety; removed revalidation is detected by a negative control.
- [ ] A healthy group remains capable of progress while another group lacks observations/quorum or waits for an owner.

### Task 12: Model sessions, locks, quorum reads and local switches

**Files:** `sessions.qnt`, `sessions_test.qnt`, `instances/sessions3.qnt`, profile (new); handlers/types/cluster/properties/tests (updated).
**References:** target §§5.8/6/6.2/8.7/9; §14 `quorumRead`; `node_session.go`, `topology/sessions.go`, `internal/metaservice/session*.go`, metadata reads/progress.

- [ ] Implement `canActivate/activate`, `canRenew/renew`, `canRevoke/revoke`, `canAcquire/acquire`; bind sessions to exact process incarnation and locks to committed session ownership.
- [ ] Keep lease expiry observations separate from exact-process checks and committed revocation; model renewal/revocation races and old-incarnation callbacks with bounded logical time.
- [ ] Wire ReadBarrier/ReadTopology/ReadProgress/ReadAuthority/ReadLockHolder through safe barrier and local application, while local getters return only their observed view. WaitOperation/WaitRevoked cancellation affects the wait only.
- [ ] Model ReadProgress exact cached identity/age and missing attachments; its cursor is not a frozen snapshot, and aggregate operation state remains complete on a truncated page. Watch/update delivery may lag/coalesce.
- [ ] Wire local scheduling and serving switches through validated values; neither creates native quorum or effect authority. Complete observation projections for lifecycle, parked work, recovery and metrics; no new metrics protocol state.

#### Verification

- [ ] Common checks and sessions3 tests cover renewal/expiry/revocation, stale holder observations, canceled waits, read barrier local-apply delay, stale attachments and partial progress pages.
- [ ] No session/lock observation is used as a quorum certificate. Serve override and policy enablement cannot bypass core safety.
- [ ] Every getter in coverage.md has a documented freshness/side-effect classification; no topology write is attributed to an observational method.

### Task 13: Add storage-loss and exact-identity restart profiles

**Files:** lifecycle/types/cluster/properties/tests (updated); `instances/storage3.qnt`, profile (new).
**References:** target §§5.3–5.5/7/8.8/9/11; §14 startup/retirement algorithms; `internal/managed` recovery paths, `topology/storage_class.go`, memory-storage and lost-WAL regression sources.

- [ ] Add `loseStore(node, replica, evidence)` for missing, unreadable, foreign and contradictory states; retain evidence rather than erasing/reinitializing it. Distinguish simulated loss from a valid retained restart.
- [ ] Apply Memory loss on process exit/restart; retain cluster/member/replica pins according to actual durable ownership. Default storage's OwnedWAL/BorrowedCustom backend remains node configuration.
- [ ] Wire parked storage to safe fresh-identity reseating/readmission only with existing authorization, native quorum and predecessor ownership rules. Memory-majority loss is not repaired by silently reusing voted identities.
- [ ] Check restored complete snapshot plus committed-tail position using bounded abstract data-version evidence; applied hints cannot skip reconstruction or exceed retained commit. No snapshot bytes or application payload semantics are modeled.
- [ ] Add restarted store with successor/foreign identity, partial loss, majority memory loss and repeated restart under fresh incarnation witnesses.

#### Verification

- [ ] Common checks and storage3 pass; retained valid restart keeps durable IDs, whereas a lost store cannot resume its old voted identity.
- [ ] No quorum or grant is invented for repair after majority loss. Evidence remains available for later recovery inspection.
- [ ] Snapshot/tail/hint mismatch is parked/refused; tests distinguish loss of a memory image from loss of node authority evidence.

### Task 14: Model offline inspection and exact recovery acceptance

**Files:** `recovery.qnt`, `recovery_test.qnt` (new); types/handlers/cluster/properties (updated); recovery profile scaffold (new).
**References:** target §§5.7/6/6.2/8.8/9; §14 `recoverStoppedSurvivor`; `node_recovery.go`, `recovery/types.go`, `internal/recovery/{inspect,model,disk}`.

- [ ] Implement `inspect`, `canAccept/accept` and complete RecoveryRequest/RecoveryPlan/Acceptance input records. Inspect reads evidence without mutation; Approve binds the exact inspected plan/digest and actor/comment.
- [ ] Require stopped exclusive source ownership for mutating/inspecting maintenance as implemented, complete retained required groups, valid source lineage and supported Node-owned WAL donor. Reject custom/memory donors and unsupported multi-survivor combinations explicitly.
- [ ] Model fresh-lineage allocation, archived old operations/sessions/locks/debt inventories, retired IDs and no silent omission of required groups. `AcknowledgeDataLoss` and `OldDeploymentFenced` are separate explicit acceptance premises.
- [ ] Expose Status/List as durable observations, including diagnostics without granting mutation authority. Keep runtime Node.Recovery/NewRecovery handles non-owning until a call acquires source resources.
- [ ] Declare later execution/cancellation phases but do not return completed recovery before Task 15 exists.

#### Verification

- [ ] Common checks and recovery unit scenarios pass for complete survivor inspection, changed-plan refusal, missing loss/fencing acceptance, running-source refusal and unsupported donor rejection.
- [ ] Inspect leaves source and authority unchanged; acceptance never erases old evidence or automatically starts a replacement cluster.
- [ ] Recovery states and refusal reasons map to current public APIs, not the obsolete “unimplemented recovery” documentation sentence.

### Task 15: Implement recovery execution, cancellation and startup consumption

**Files:** recovery/lifecycle/handlers/cluster/types/properties/tests (updated); complete `instances/recovery3.qnt`, profile.
**References:** target §§5.7/6/8.8/9/11; §14 `recoverStoppedSurvivor`; `internal/recovery/{exec,runtime,disk}`, `RecoveryStartOptions`, Files Backup/Undo contracts.

- [ ] Implement `canAdvance/advance`, `canCancel/cancel`, `canConsumeOnStart/consumeOnStart`; separate approval, Files callback, archival, awaiting start, import, validation and completion durable phases.
- [ ] Add crash/resume before/after each durable boundary, Files failure/late return, supported cancellation/undo and retained source ownership. Reserved RollingBack/RolledBack values have no executable disaster-rollback action.
- [ ] Consume exact recovery authorization through ordinary modeled Node startup, reconstruct complete survivor snapshots/tails and verify every required group before completion. Do not restore obsolete sessions/locks/operation owners as fresh-lineage authority.
- [ ] Implement ResetForJoin's explicit archival/reset path and old-node rejoin refusal until it completes. Keep old and new lineage evidence distinguishable even when a node/group name repeats.
- [ ] Require exact persisted plan/phase on retries; loss acceptance permits the documented acknowledged-write-loss possibility, not arbitrary data fabrication. External deployment fencing remains an attested environment premise.

#### Verification

- [ ] Common checks and recovery3 end-to-end Rebuild/ResetForJoin witnesses pass, including failures between Files, archive, import and validation.
- [ ] Cancel after an unsupported phase refuses without undoing committed archival; old-lineage messages and identities cannot participate in the rebuilt lineage.
- [ ] Incomplete donor and missing accepted evidence cannot become successful recovery after restart or repeated Run.

### Task 16: Close the public API matrix and integrated scenario coverage

**Files:** `coverage.md`, `regressions.md`, family/cluster tests, instance/profile declarations (updated/new).
**References:** target §§6.1–6.2/8–9/11–13; full brainstorm API/scenario tables; public `Cluster`, `Node`, `Recovery` interfaces and helper constructors.

- [ ] Audit every public method against the live interfaces: map it to a named action sequence, explicit observation projection, composed workflow or justified abstraction. Add missing methods/variants rather than counting broad family labels as complete coverage.
- [ ] Complete all healthy/degraded group and node sequences through the real composed `step`, including two-node unavailability, per-channel faults, independent groups, 3→5→3 with two load groups and varied voter counts, manual/scheduler races and returning owners.
- [ ] For each mutation family, include accepted, invalid/stale-refused, waiting and uncertain-result examples where the actual API permits them. Verify returned staged versions, predecessors and unchanged versus partially committed state.
- [ ] Bind action/variant coverage to witnesses so a defined but unreachable method is visible. Isolate arbitrary bad-message injection to negative tests; ordinary scenarios use valid public setup actions.
- [ ] Write manual Go regression recipes with source test locations, public calls, exact fault/barrier sequence, identity assertions and expected outcome. Use owning-package tests for local behavior and existing Docker/Gosim boundaries for multi-node cases; do not add automated Go replay or alter fixtures in this task.

#### Verification

- [ ] Every public interface method has a non-placeholder matrix row, and every modeled action family has an executable witness/refusal case.
- [ ] Common checks plus all profile deterministic tests pass; integrated traces exercise the composed modules instead of isolated idealized helpers.
- [ ] Recipes distinguish observed behavior from model-only claims and do not claim that existing metadata-focused Gosim fixtures already cover load/recovery replay.

### Task 17: Implement explicit stuck, fair-progress and recoverability checks

**Files:** properties, family/cluster tests (updated); `instances/progress_small.qnt`, backend calibration fixtures and profile (new).
**References:** target §§10.2–10.3/11–13; meaningful-progress/stabilization actions in §§6.1/7. Local Quint TLC adapter and temporal operator implementation are tool sources, not product contracts.

- [ ] Implement `pendingObligations`, `expectedBlockers`, `enabledUsefulActions`, `unexpectedStuck`, with separate normal terminal state, modeled limit exhaustion and genuine controller stuckness. A pending obligation's preparatory/delivery/fence/report actions count; unrelated rejects/fault toggles do not.
- [ ] Add stabilization profiles stopping new disruptive work/faults and promising only explicitly selected resources. Never restore destroyed evidence as part of “heal.” Keep callback nonreturn, local holds, disabled scheduling, retention and absent quorum as named premises.
- [ ] Define per-obligation fairness and temporal eventual completion/convergence. Frame every mutable model variable; do not use fairness of global `step` as evidence that an individual group/message is served.
- [ ] Qualify backend semantics with tiny known-pass/known-fail safety, stuck and temporal examples. Explicit stuck predicates are mandatory because the local adapter supplies TLC `-deadlock`. Record backend/runtime dependency versions and unsupported temporal combinations.
- [ ] For public-API recovery, use the target's temporal formulation alternative: from any reachable selected stabilization state, a finite explicit API recovery controller must reach the profile's desired outcome under its stated fairness/resources. Its success establishes recovery under that policy; failure does not prove that no other API sequence exists. Keep existential witnesses, policy convergence and universal fairness claims labeled separately.
- [ ] Add livelock/starvation controls and a case where legitimate permanent loss is classified as unsupported recovery, not success or an algorithmic deadlock. Do not weaken premises/properties merely because verification is expensive.

#### Verification

- [ ] Common checks and focused simulation witnesses pass; all intentionally stuck/livelocked/incorrect temporal fixtures are detected by the named checker.
- [ ] One tractable finite `progress_small` configuration completes its required TLC exploration and supported temporal checks. Incomplete/unsupported backend results are recorded as gaps, never marked verified.
- [ ] Per-obligation progress survives irrelevant invalid calls in fault prefixes, and tests expose starvation hidden by global-step fairness.

### Task 18: Qualify negative controls and the full profile matrix

**Files:** fault_detection/family tests, all target instances/profiles, properties, coverage and reports (updated).
**References:** target §§10–13; all nine named profiles in §11 and every safety predicate in §10.1.

- [ ] Finish isolated faulty variants for omitted post-wait authorization, dropped deletion debt, predecessor receipt acceptance, early publication/release, premature transfer completion, foreign staged intent approval and false collection proof. Normal configs cannot select these bypasses accidentally.
- [ ] Calibrate concrete finite bounds for NODE_POOL/load IDs, request/attempt/version/generation counts, message counts, abstract positions/time and history. Demonstrate each named scenario fits; include boundary/refusal checks without identity wrap, fake truncation or discarded debt.
- [ ] Freeze reproducible initial profile settings with actual measured runtime/state-space evidence; use fixed recorded seeds and explicit sample/depth counts. Keep search capacity distinct from replicated library limits.
- [ ] Execute all named deterministic tests, selected sampled safety runs and designated Apalache bounded checks. Complete the small finite exhaustive profile; do not require an unmeasured exhaustive five-node claim.
- [ ] Match each negative control's counterexample to its intended property and expected stage. Treat a parser/runtime/tool error or missing witness as failure of the control, not successful bug detection.
- [ ] Produce the per-profile guarantees/limitations table and retain ITF traces/manual regression recipes for genuine new findings. Any current-code counterexample is classified and reported; changing production Go to fix it requires a separately scoped change, not silently rewriting this model.

#### Verification

- [ ] All required ordinary profile suites pass their configured checks, negative controls fail in the expected way, and all named safety properties are actually selected by at least one applicable run.
- [ ] Bounds/assumptions and manifests agree with executed constants; no hidden unbounded diagnostic history defeats finite exploration.
- [ ] Reports distinguish sampled, bounded, exhaustive-complete, counterexample and incomplete results, with reproducible command/seed/backend details.

### Task 19: Integrate opt-in Make targets and qualify runner failures

**Files:** Makefile (updated); `scripts/quint/run.py`, runner tests/profiles/README (updated).
**References:** target §§6.3/11/13; CLI/profile/report interface. Existing Makefile and `scripts/test_report.py` reporting conventions.

- [ ] Add `test-quint`, `test-quint-simulate`, `test-quint-verify` to help/PHONY with `QUINT_PROFILE ?= smoke3`, `QUINT_SEED ?= 1`, and explicit `TEST_REPORTS_DIR` forwarding. `test-quint` runs complete registered deterministic checks plus a bounded smoke simulation; simulation/verify targets run their selected profile.
- [ ] Preserve `.DEFAULT_GOAL`, existing Go test dependencies, Docker behavior and root module dependencies. Validate all profile selection/seed/report overrides with safely quoted arguments; the Python runner still uses argument arrays internally.
- [ ] Finish report qualification for timeout/interruption, expected negative controls versus unexpected failure, bounded pass versus exhaustive completion, no tests, unknown profile, missing backend and report write failures. Retain raw child exit status even when the wrapper uses a normalized failure code.
- [ ] Bound and join launched verification children, retain partial artifacts on interruption and avoid declaring stale previous reports current. No auto-install/update/download path is introduced.
- [ ] Test runner report roots both via direct CLI and Make. Add documented dependency/setup commands as instructions only; fixture subprocesses remain independent of Docker/Go services.

#### Verification

- [ ] Runner unit suite and all existing Python script tests pass. If `scripts/test_report.py` remains untouched, it still needs no new Quint suite; this plan uses its independent runner.
- [ ] Exercise `make test-quint`, `make test-quint-simulate QUINT_PROFILE=scoped3 QUINT_SEED=1`, and the designated small `make test-quint-verify` profile with real tools.
- [ ] `make -n test` shows the unchanged Go test command/dependencies, and changed report-root tests create fresh reports in the requested root. Common checks and `git diff --check` pass.

### Task 20: Verify the architectural delta and all model wiring

**Files:** all implementation files (audit/fixes), this plan and progress log (updated).
**References:** target **all sections**, especially §§4–6 (diagram/schema/interfaces), §§8–10 (flows/properties), §§11–13 (profiles/reporting), §14 (algorithms). This is the third-to-last task.

- [ ] Audit state/types: every target record/variant, corrected storage/hosting/evidence distinction, finite-domain premise, local-versus-committed view and volatile-versus-durable distinction exists and matches source semantics.
- [ ] Audit topology/planner/API: every method mapping, pure signature, CAS/preview/rollback/ensure rule, placement branch, source-version and immutable-plan retry path exists.
- [ ] Audit consensus/network/handlers: exact voter quorums, independent channels, delayed local apply, explicit message delivery and no omniscient handler shortcut; all stateful imports are singular and framed.
- [ ] Audit authority/lifecycle/cleanup: grants, fences, receipts, startup reservation/publication, callbacks/holds/join, predecessor handover, retained proof and collection ordering all match target flows.
- [ ] Audit scheduler/sessions/reads: capture/decision/revalidation/CAS, independent intent preservation, sessions/locks, observations, barriers, pagination and local switches are represented and tested.
- [ ] Audit storage/recovery: donor restrictions, evidence preservation, full replay abstraction, inspection/acceptance, Files/archival/cancel/resume, lineage and ResetForJoin behavior match target.
- [ ] Audit properties/profiles/tooling: all named properties, action witnesses, faulty controls, finite-bound checks, temporal calibration, run categories, Make targets and manual regression recipes are present. Confirm zero production DB/cache/API/Go dependency changes.
- [ ] Enter every missing/mismatched item in **NOT IMPLEMENTED GAPS**, fix within the agreed scope and rerun affected checks, or leave `[_]` with an explicit unresolved reason. Do not silently convert planned acceptance criteria into exclusions.
- [ ] **Verify actual wiring last:** starting from each Make/runner profile, follow concrete instance → one cluster.init/step → handlers/pure modules → consensus/local effects and exact completion paths; confirm profile-selected properties and scenario tests are reachable and executed. Definitions or unit-only references are insufficient.

#### Verification

- [ ] Full deterministic/model and runner suites pass; rerun the focused sampled/bounded/exhaustive checks affected by audit fixes, preserving their completeness labels.
- [ ] Record a component-by-component delta checklist and exact exercised commands in the progress log. Unresolved gaps remain visible and prevent a complete implementation claim.

### Task 21: Run a visible external review and fix confirmed findings

**Files:** reviewed implementation (fixes if needed), local `.revmux` task/run artifacts, `docs/review/20261027-quint-cluster-management.md`, plan/progress (updated).
**References:** target all sections; project AGENTS.md, `docs/project.md`, `docs/architecture/library.md` Design constraints and `.revmux/profile.md`. Revmux and Paseo binaries were available at planning time.

- [ ] Read the current revmux and Paseo skills, rerun availability/preflight, inspect `.revmux/profile.md`, and resolve a valid configured review profile. Scope the review to this feature's implementation against the captured baseline `e8e85dbd7879b1c858252cd2a6126a36920f8a8e` (or its recorded rebased equivalent), including uncommitted fixes as appropriate.
- [ ] Prepare revmux goal/scope/context with original request, approved target, this plan, coverage, validation evidence and known exclusions. Explicitly explain the user's implemented-scoped-concurrency decision versus the profile's older global gate; do not substitute a stricter desired protocol into baseline findings.
- [ ] Create a **new visible Paseo terminal in this worktree**, create the revmux task/run and launch the actual review there. Persist stdout JSON separately from stderr/progress; retain terminal/task/report identifiers. Use the skills' current command/tool syntax instead of assuming a nonexistent `arch-review` revmux profile.
- [ ] Review soundness of abstraction/atomic boundaries, omitted behaviors, vacuous invariants, missing action witnesses, guard/property duplication, negative controls, lifetime/identity evidence, fake exhaustive claims and runner failure handling. Reviewers do not edit source or run destructive workloads.
- [ ] Inspect degraded/partial review status before interpreting results. Confirm findings against source and traces, fix all confirmed major/high issues within scope, record minor findings, and rerun affected validations plus a focused re-review of material fixes. Do not copy the report into chat; leave the visible terminal and linked report.
- [ ] If revmux becomes unavailable, follow the skill's documented Paseo arch-review/advisor fallback and record the change. If no review can run, mark it unresolved; never mark a missing/partial review as a clean completed review.

#### Verification

- [ ] A real review ran in a new visible terminal; report/status identifies completeness and confirmed dispositions. Revmux findings exit status is not confused with tool failure.
- [ ] Confirmed major/high findings are fixed and affected tests pass; material fixes have been re-reviewed. Any remaining issue is explicit in **NOT IMPLEMENTED GAPS** or the minor-findings report.

### Task 22: Close remaining gaps and publish implementation documentation

**Files:** `scripts/quint/{README,coverage,regressions}.md`, `docs/{architecture/project-map,architecture/library,architecture/raft,testing/tests}.md`, root README if needed (updated); `docs/changelogs/20261027-quint-cluster-management.md` (new); plan/progress and any gap fixes (updated).
**References:** target §§1–3/6.3/11–15; project `Documentation rules`; entire verified delta and review dispositions. This is the final task.

- [ ] Revisit every **NOT IMPLEMENTED GAPS** item, fix it in accordance with the target or record `[_]` with its unresolved scope/reason. Never use a skipped marker to satisfy required runnable/verification/review deliverables. Run focused validation/re-review if a final fix changes behavior.
- [ ] Write the model README with exact setup/pinned dependencies, commands/profiles/bounds, supported APIs/faults, interpretation of reports and the update-before-feature-change workflow. Clearly separate model verification from Go conformance and expected recovery limitations.
- [ ] Complete coverage and manual regression recipes using actual names/results, not planned aspirations. Document all true exclusions with `TODO(not planned)` at relevant model/tool boundaries; remove all task-number stubs whose implementation is now due.
- [ ] Add concise project-map/testing/README pointers and opt-in commands. Reconcile recovery's stale unsupported claim. For concurrency, document observed scoped behavior and explicitly retain the global-serialization design discrepancy as a candidate future restriction; do not quietly repeal a design constraint or claim that a stricter gate was implemented.
- [ ] Create the user-facing changelog describing the new executable cluster-management specification and developer verification commands, without claiming runtime fixes/performance changes or universal proof of cluster safety. Use the actual completion date if execution occurs on another day and record the filename adjustment.
- [ ] Recheck all cross-document paths, API/command names, dependencies, report claims and default Make behavior. Confirm final implementation files match the accepted target and review fixes remain wired.
- [ ] Run final applicable checks, record commands/results/remaining limits and complete the plan/progress entries. Commit documentation and any fully validated final corrections; do not publish or push without an instruction to do so.

#### Verification

- [ ] `git diff --check`, documentation links/commands, complete deterministic tests, runner units and the default Make smoke pass; profile evidence refers to actual current source/model hashes.
- [ ] Rerun only verification affected by final implementation corrections; unchanged prior results retain exact revision/hash provenance rather than being relabeled as fresh runs.
- [ ] No unfinished temporary stub, unwired component or unresolved required gap is hidden behind checked boxes; final summary identifies what was actually verified and at which bounds.

## Validation Commands

Commands below are implementation gates, **not results from the planning session**. Run from repository root with the pinned CLI and explicitly provisioned verification dependencies. The runner must verify supported flags/backend behavior against the installed version. Use registered concrete mains; never execute the parameterized core directly with unbound constants.

### Bootstrap — Task 1

The foundation wrapper exposes imported scenario tests and its state-reading `safety` predicate through the one concrete cluster instance.

```sh
quint --version
quint typecheck scripts/quint/instances/foundation.qnt
quint test scripts/quint/instances/foundation.qnt --main foundation --seed 1 --max-samples 10
quint run scripts/quint/instances/foundation.qnt --main foundation --invariant safety --seed 1 --max-samples 50 --max-steps 20
git diff --check
```

### Per-task model and runner gates — Task 2 onward

```sh
python3 -m unittest discover -s scripts/quint -p '*_test.py'
python3 scripts/quint/run.py --profile foundation --mode test
python3 scripts/quint/run.py --profile foundation --mode simulate --seed 1
```

From Task 3 replace the foundation simulation gate with `smoke3`, retaining foundation tests. Every registered profile owns concrete test entry points; the task's focused profile is additional to the accumulated deterministic suite, not a replacement for it.

```sh
python3 scripts/quint/run.py --profile smoke3 --mode test
python3 scripts/quint/run.py --profile smoke3 --mode simulate --seed 1
python3 scripts/quint/run.py --profile scoped3 --mode test
python3 scripts/quint/run.py --profile scoped3 --mode simulate --seed 1
```

Run scoped3 only from Task 11. Likewise introduce admission4/scale5 in Task 8, deletion3 in Task 9, sessions3 in Task 12, storage3 in Task 13, recovery3 in Task 15, and progress_small in Task 17. A partially introduced profile must explicitly list completed scope; it cannot claim the final profile's properties until its completing task.

### Verification — Tasks 17–18 and relevant later fixes

```sh
python3 scripts/quint/run.py --profile progress_small --mode verify
python3 scripts/quint/run.py --profile scoped3 --mode verify
python3 scripts/quint/run.py --profile deletion3 --mode verify
```

Configure `progress_small` for TLC finite exploration/qualified temporal checking and scoped3/deletion3 for Apalache bounded safety. Other profiles' checked backend/depth are recorded in their JSON and final coverage table. Run all nine deterministic profiles and all required sampled checks; full verification of every five-node interleaving is not a promised acceptance gate. Negative controls run under their explicitly declared expected-counterexample harness and retain raw failing traces.

Each reported verification guarantee must include properties, constants, initialization, fault assumptions, step relation, backend/version, depth where applicable, completion status and source/model hashes. The runner must not manufacture a property result by merely finding an empty/dead-ended trace, receiving a zero exit without completion evidence, or recognizing an expected nonzero code from an unrelated tool error.

### Integration — Task 19 onward

```sh
python3 -m unittest discover -s scripts -p '*_test.py'
python3 -m unittest discover -s scripts/quint -p '*_test.py'
make test-quint
make test-quint-simulate QUINT_PROFILE=scoped3 QUINT_SEED=1
make test-quint-verify QUINT_PROFILE=progress_small
make -n test
git diff --check
```

Check report-root override through both `TEST_REPORTS_DIR` and direct `--reports-dir` in temporary fixtures. `test-quint` runs all registered completed deterministic profiles, runner tests and one smoke simulation; avoid hidden verify calls in this fast target. `test-quint-verify` is explicit opt-in. Existing shared Python tests are a final/integration checkpoint, not repeated after every `.qnt` edit.

### Scope-dependent repository checks

No `make lint-full`, race, Docker, Gosim or performance run is required just because the modeled system is concurrent. If execution adds a Go regression or modifies Go/shared tooling outside scope, log it under NOT PLANNED CHANGES and run `gofmt`, `make lint`, `make test` plus the relevant functional/race/process checks prescribed by AGENTS.md. If the shared runner changes, run its Python suite and exercise affected Make targets. Do not claim historical tests are new results.

Documentation-only final edits require link/API/command checks and whitespace validation. Re-run affected model checks when a documentation correction exposes an actual model/configuration change; otherwise retain exact prior verification provenance.

## Completeness matrix

| Target area | Implementing tasks | Required acceptance evidence |
| --- | --- | --- |
| §4 composition, §§5.1–5.2 domains/identity, §6 modules | 1–3, 20 | Concrete executed instance, one stateful composition, exact IDs and framed variables. |
| §5.3 topology/placement; §§8.1–8.3 | 4–5, 8–9 | Valid/refused API traces, source-equivalent plan and CAS behavior. |
| §5.4 ownership; §8.6 | 7, 10, 13 | Reservation, publication, cancellation, join/Close/hold evidence. |
| §5.5 consensus; independent load behavior | 3, 8 | Native membership/catch-up/quorum witnesses and stale-view separation. |
| §5.6 authority/operations; §9 failures | 5–6, 9–11 | Exact grants/receipts/fences, immutable retries and pending revocation. |
| §5.7 cleanup/recovery; §§8.4/8.8 | 9–10, 13–15 | Complete debt, collection order, fresh-lineage/rejoin and failure resumes. |
| §5.8 sessions/network/calls; §8.7 reads | 3, 5, 12 | Explicit asynchronous delivery, uncertainty, session and observation semantics. |
| §8.5 scheduler | 11 | Capture/decide/revalidate/CAS, independent repair and foreign intent preservation. |
| §6.2 full public surface | 4–16 | Exact method-by-method matrix and reachable action/refusal witnesses. |
| §§10.1/10.4 safety and negative controls | every behavior task, 18 | Selected predicates plus intended failing variants. |
| §§10.2–10.3 progress/tool semantics | 17–18 | Known stuck/livelock controls and completed qualified small verification. |
| §11 nine profiles; §12 incremental execution | 1–19 | Every profile bound/configuration tested; no fabricated completeness. |
| §§6.3/13 commands/reports/manual recipes | 2, 16, 19, 22 | Real Make entry points, accurate manifests and reproducible manual recipes. |
| §14 transition pseudocode | 5–15 | Every listed durable/wait/message boundary represented and exercised. |
| §15 exclusions | 1, 16–22 | Explicit TODO markers and documented unsupported claims. |

## NOT CONSIDERED & TODOs

The exclusions below were agreed in the brainstorm/target. No new product API stub, WARN log or runtime failure is added to gquorum for a model exclusion; place the marker in the relevant model/tool boundary and document it in coverage. Required type elaboration and bound calibration are Tasks 1/4/18, not exclusions.

| Tag | Excluded scenario | Marker / default behavior |
| --- | --- | --- |
| [postponed] | Automated replay of Quint traces through Go APIs | `regressions.md` and `run.py`: `TODO(not planned): automatic Go trace replay`; preserve traces/manual recipes; reject unknown replay modes rather than silently ignoring them. |
| [potential-improvement] | Changing baseline to global topology serialization | authority/scheduler model boundary: `TODO(not planned): strict global operation gate`; preserve scoped baseline and record candidate future policy. |
| [too-complex] | Full Raft elections/log-replication proof | membership model: `TODO(not planned): concrete Raft replication/elections`; use the approved safe-consensus abstraction with observable commit/apply boundaries. |
| [too-complex] | Arbitrary application state machines and external transactions/outboxes | abstract load commit boundary: `TODO(not planned): application business semantics`; token witnesses only. |
| [too-complex] | Byte-level storage/wire corruption, OS scheduling/resource proof, cryptographic authentication verification | network/storage boundaries: `TODO(not planned): byte-level and OS/cryptographic modeling`; inject evidence/failure classes and assume authenticated ingress without bypassing identity checks. |
| [postponed] | Guarantee of completed exhaustive five-node exploration | runner/docs: `TODO(not planned): universal five-node exhaustive guarantee`; run measured finite/bounded profiles and report timeouts incomplete. |
| [too-complex] | Unsupported multi-survivor/custom/in-memory donor recovery or disaster rollback | recovery model: `TODO(not planned): unsupported recovery modes`; include explicit source-equivalent refusal witnesses, no successful action. |

## NOT PLANNED CHANGES

Task 1: the target record field `action` is implemented as `kind` because Quint reserves `action`; the mapping is documented in the model README/coverage. Choreo source is unchanged: its pure helpers operate on one application-owned GlobalContext instead of its own unused state variables. Planning clarified existing target sketches for storage backend versus class, optional legacy hosting, group-wide effects and distinct fence/readmission evidence; these are documented in Planning reconciliation, not production behavior changes. During execution record unexpected file changes, rationale and relevant diff here.

## NOT IMPLEMENTED GAPS

None discovered in implementation yet; implementation has not begun. Record entries as `- [ ] component: missing requirement, target reference, evidence and next action`. Close only after validation; use `[_]` for explicitly unresolved items with reasons. Empty here does not mean pending tasks are implemented.

## Planning verification record

- Inspected the current branch/clean tracked worktree, recent history, current project rules, target/log, review profile, Make/test boundaries and relevant public/type definitions.
- Confirmed installed Quint 0.33.0 and available revmux/Paseo; inspected CLI test/run help for concrete command options. No model execution, Go tests, external review or implementation work ran during planning.
- User accepted the 22-task sequence; all implementation/verification boxes intentionally remain unchecked.
- Preserve the final three tasks in order: architectural delta verification (20), external review (21), documentation and gap closure (22).
- Planning validation checked all 22 task numbers, per-task file/reference/verification fields, final audit wiring order, target hash, document links, code-fence balance and whitespace. This validates the plan structure, not the future model.
