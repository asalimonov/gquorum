# Implementation plan: failure isolation and operational APIs

**Plan:** `./docs/plans/20261015-failure-isolation-and-operational-api.md`

**Project source:** `../..` relative to this file; repository root `.`

**Date:** 2026-10-15

**Branch:** `feat/failure-isolation-and-api`

**Baseline:** `f17208f` (map), Go implementation `926a18b`

**Target:** [20261015-failure-isolation-and-operational-api-target.md][T]

**Brainstorm:** [full discussion][B]; [completed peer review][R]

**Status:** implementation in progress; completion and validation are recorded per task below.

**Accepted design amendment (2026-10-16):** [state-based admission](../brainstorms/20261016-state-based-admission-target.md), from peer thread 10. The user authorized adopting peer consensus without further approval and finishing the goal. The amendment is normative alongside [T]; resume from the first unchecked task below.

[T]: ../brainstorms/20261015-failure-isolation-and-operational-api-target.md
[B]: ../brainstorms/20261015-failure-isolation-and-operational-api.md
[R]: ../review/feat_failure-isolation-and-api.md

## Outcome and boundaries

Implement the reviewed target as one library change: independently converging approved group blocks; detached, durable cleanup; and a small operational API on the existing `Cluster` handle. Keep `Node`, `Cluster`, `load.Group`, existing authority grants, managed ownership, storage extensions and policy/lifecycle callbacks. No public network service, additional lifecycle owner, generic jobs framework or request registry.

The final contract has Latest and Target, durable group/node convergence, and a historical FullyConverged marker. It has no Active document or composite replacement. Operation replaces the public Operator vocabulary. ReadProgress provides bounded inspection; RetryOperation retries an exact failed attempt. Replacement mutations require predecessor identities. The stricter RetirementStore contract, including ForgetCollection, is included. API and changed-format breaks are intentional; backward compatibility is not a deliverable.

[T] is the design authority. Its §2 flow diagram, §8 end-to-end flows/failure table and §12 algorithms are normative for these tasks. The contract excerpts at the end of this plan reproduce its target types; they do not introduce another design. Preserve exact JSON tags from [T] even where a task describes a shape in prose. Current architecture documents describe the old implementation until the documentation tasks update them.

## Repository findings and execution rules

- Actual modules are root, `examples/cluster`, `examples/scheduler`, `examples/inmemory-cache`, and `tests/cluster`; `scripts/revive` is a tooling module. `make test` covers all five application modules. Preserve the existing scheduler/cache demos while adapting their API use; keep `examples/cluster` small and meta-only.
- `make lint` runs the repository's vet, revive, staticcheck and golangci configuration; it is not merely `go vet`. Use the current Makefile and [test guide](../testing/tests.md).
- There is no `project.txt`, `architecture/`, or nested documentation Git repository here. Work in the existing root branch. `docs/plans/` is ignored; stage this plan explicitly without staging unrelated `docs/review/` files.
- User steering (2026-10-16): stop adding/splitting tasks and renumbering cross-references. Extend existing tasks and use larger cohesive implementation commits. The former 550-line ceiling and per-task budget estimates are superseded; task numbers remain stable. Fold review fixes into the next relevant existing task. Preserve meaningful verification and review after every five implementation/correction commits.
- After each five implementation commits, inspect newly obsolete code with references and unused-code lint, delete safe dead code, and request a paseo-peer review of that batch. Evaluate findings against the target and actual code, fold justified corrections into the next relevant existing task, and record accepted/rejected findings with evidence before the next five-commit checkpoint. Independent implementation and task commits may continue while the peer reviews the immutable committed range; defer work that needs an unresolved review decision. Retained transition scaffolding needs a named removal task. Count all implementation/correction commits toward subsequent five-commit review boundaries; documentation/planning commits do not count.
- Tasks execute in numbered order. A dependency list names additional semantic prerequisites, not permission to skip earlier tasks. At every checkpoint all modules compile and `make test` passes. Run focused tests named in each task, and focused functional/race checks when ownership or persistence changes. Fix failures encountered, including unrelated failures, and record unrelated fixes under NOT PLANNED CHANGES.
- First introduce contracts and pure helpers, then state-machine behavior, stores and execution, then public composition. Temporary compile scaffolding is allowed only for an intermediate commit: record its exact symbol/file and removal task in the progress log. Do not introduce public V2 APIs or promise compatibility. Existing old symbols may remain until their consumers are converted; new paths must fail closed until their real implementation is connected. Remove all scaffolding by Task 78. Never synthesize an authoritative composite Document to make old code compile.
- A signature change and all callers needed for that commit to compile belong together. Partition broad naming changes across the component tasks below; if a bridge is unavoidable, keep it private wherever possible and remove it before final cutover. No production flag chooses between old/new authority models. Change a reachable behavior only when its safety prerequisites are implemented; test new helpers directly before wiring them. A temporary stub must return a classified unsupported error, never fabricated progress or success.
- Keep schema constants at the current version until their complete encoder/decoder and invariants are ready. Do not write a partially implemented v4 state or v2 journal. Activate each complete format in its owning task; old changed formats then reject explicitly. No migration, fallback to empty state or current-code regeneration of immutable baseline fixtures. Before Task 78, exercise new FSM handlers/codecs through isolated owning-package fixtures; do not route live production proposals into scoped state that the active persistence path cannot save. Service/Node composition edits before that point prepare the final wiring; the complete activation and real-entry-point restart check happen together in Task 78. Move an inseparable wiring/format change into an earlier bounded cutover task if needed, recording that ordering change here rather than briefly enabling unsafe persistence.
- Tests follow AGENTS.md: pure units use `!functional`; services, stores and single-participant fixtures use `functional`; multi-node scenarios use the Docker pool. Use explicit barriers, exact identities and joined cleanup, not timing-only assertions. Test names introduced below are planned names, not claims that they already exist.
- Consult TiKV/PD source paths and revisions in [T] §9 at the relevant lifecycle/dispatch tasks. Adapt removed-peer records, tombstone-before-delete and group ownership inside existing packages. Do not port swallowed deletion errors, volatile PD operation storage, priorities or a second controller architecture. Record any actual code port and required license attribution.

## Target coverage and ordering

| Target contract | Primary tasks |
| --- | --- |
| Public API, Operation/Plan/progress types, exact conflicts | 1–7, 47–49, 60–61, 66–67, 74–78 |
| v4 metadata, indexes, convergence and proof retention | 8–15, 34–46, 50–53, 78 |
| Scoped planning, admission/removal and atomic repair rebase | 22–41, 45–51, 66–70 |
| Detached cleanup, journal v2 and exact physical collection | 42–43, 52–59, 64–65, 71–72 |
| Bounded dispatch, policy isolation, observation provenance | 5, 62–70 |
| Former Active consumers and actual ownership wiring | 50–51, 61, 66–78 |
| Failure/recovery evidence, performance measurement, documentation | 79–87 |
| Complete design-to-code and entry-point audit | 88 |


## Task list

### Task 1: Establish Operation domain vocabulary

**Files:** `topology/operator*.go` → `operation*.go`, `topology/authority*.go`, `fault/fault.go`. **Target:** [T] §3.3–3.4, §7.2; contract C3. **Budget:** ≤550 Go lines.

- [x] Add the exact OperationID, OperationState, OperationStatus, BlockStatus, Operation and OperationTransition definitions and state helpers from C3, including the BlockedReason dependency from C2 in this task. Preserve execution ID strings and existing state values; implement deep copies of owned fields.
- [x] Inventory all Operator naming consumers and assign their migration to Tasks 7–12, 24–25, 50, 58–63, 67–73. Keep Step comparable; do not put dependency slices in AuthorizedStep. Rename public authorization error vocabulary without changing unrelated Origin/event values.
- [x] Add pure tests for ID construction, state validity, owned copies and preservation of full aggregate state in partial projections. Record temporary compile scaffolding explicitly. Aggregate calculation/tests belong to Task 40 alongside the FSM consumer, avoiding an unused helper or an extra public aggregation API.

**Verification**

- [x] Run focused topology/fault units and `make test`; check that no new public compatibility API is documented.

### Task 2: Extend immutable plans with blocks and dependencies

**Files:** `topology/operator_plan.go`, `topology/plan.go`, nearby tests. **Target:** [T] §3.3, §12 “Retry derives state”; contract C3. **Depends on:** 1. **Budget:** ≤550.

- [x] Implement Plan{Version, Steps, VoterBases, Blocks, Dependencies}, PlanBlock{GroupID, SourceVersion, Steps, Cleanup} and StepDependency{StepSeq, Requires}; Clone/Equal include every field, with immutable global Step.Seq values.
- [x] Validate unique deterministic block keys, one owner for every step, disjoint required/cleanup membership, valid references and acyclic edges. Allow only admission/removal barrier shapes; do not create a general dependency API.
- [x] Test zero-step declarations, malformed partitions, duplicate/missing steps, forbidden edge shapes, cycles and equality changes in each immutable component. Preserve the existing voter-proof checks.

**Verification**

- [x] Run focused plan/authority units and `make test`.

### Task 3: Add bounded progress and freshness value types

**Files:** new `topology/progress.go`, `topology/read.go`, `status.go`, `views.go`. **Target:** [T] §3.2, §8.3; contract C2. **Depends on:** 1–2. **Budget:** ≤550.

- [x] Add the full C2 progress structs, BlockedReason codes, FullyConverged, CleanupSummary, Validate and Clone methods. Preserve nil versus present timestamps/declarations and all JSON names.
- [x] Prepare final ReadView{AppliedIndex, Latest, Target, FullyConverged}; GroupStatus adds Reporter, ReporterReplicaID, ReceivedAt and SourceVersion only. ClusterStatus prepares bounded Operations with explicit truncation; its Groups conversion to GroupProgress and per-group convergence/owner projection belongs to Task 61. Remove old fields only after consumers are converted in Tasks 61 and 69–73.
- [x] Test copy isolation, query limit normalization (100 default, 1000 maximum), invalid negative/oversized limits and exact freshness identity fields. Paging algorithm belongs to Task 60.

**Verification**

- [x] Run topology value/validation units and `make test`.

### Task 4: Define public mutation results and structured conflicts

**Files:** `cluster_mutations.go`, `fault/fault.go`, private error encoding/decoding in `internal/metaservice`. **Target:** [T] §3.1, §3.4, §8.2; contracts C1/C4. **Depends on:** 1. **Budget:** ≤550.

- [x] Establish MutationResult{Document, OperationID, RebasedTo, Placement}; preserve MutationOptions and its Preview/Approve exclusion. Add ExpectedVersion, CurrentVersion and RebasedTo to the existing fault.Error.
- [x] Carry structured conflict fields through ApplyResult/private leader forwarding and public errors without flattening them. Preserve an accepted staged Document when optional approval fails.
- [x] Test conflict round trips, errors.Is/Unwrap behavior, partial staging results and preview remaining write-free. Defer semantic replay to Tasks 47–48; do not silently refresh caller CAS.

**Verification**

- [x] Run focused fault/codec units and applicable mutation functional regressions; run `make test`.

### Task 5: Define execution capacity and scoped policy inputs

**Files:** `topology/document_rules.go`, `scheduling/scheduler.go`, `metrics/observation.go`. **Target:** [T] §5.4, §6.4; contracts C7/C9. **Depends on:** 3. **Budget:** ≤550.

- [x] Add final Limits fields MaxNodes, MaxReplicas, MaxConcurrentGroups and MaxConcurrentCleanup; normalize to 3/5/4/2, reject negative worker counts. Remove the old pending-authority cap only with its consumers in Tasks 38–39 and 74.
- [x] Establish ScheduleInput's exact C7 shape and owned Clone; Groups carry approved/proven state, never Desired into a policy callback. Prepare metrics TargetVersion and per-group SourceVersion fields.
- [x] Test normalization, JSON fields and callback-copy isolation. Do not make a worker count a limit on retained durable evidence. Policy behavior and collector invalidation are implemented in Tasks 68–70.

**Verification**

- [x] Run limits/scheduling/metrics units and `make test`.

### Correction 1: Close the first five-commit review

**Files:** plan/block validation, legacy FSM entry guards, scheduling copies/boundaries, status values and matching tests. **Target:** §3.2–3.3, §5.4, §6.4; review thread 2. **Budget:** ≤550 Go lines.

- [x] Reject detached required deletion/replacement stops and unrelated founding prerequisites; source versions precede their plan version.
- [x] Refuse scoped plans in all legacy FSM entry/restore paths until complete Task 78 activation; retain full classified errors.
- [x] Keep ScheduleInput.Clone faithful, strip Desired at callback/conformance boundaries, and use explicit no-effect checks at old IsEmpty callers.
- [x] Keep raw observations free of convergence ownership; add explicit reporter replica identity, document immutable Limits and record remaining producer/removal tasks. Reuse the existing placement clone.
- [x] Run focused regressions, staged lint and make test; record accepted/rejected review arguments and dead-code evidence.

### Task 6: Define specialized collection contracts

**Files:** `storage/retirement.go`, new `internal/authoritystore/collection.go`, `internal/managed` configuration. **Target:** [T] §4.2, §7.2; contracts C6/C11. **Budget:** ≤550.

- [x] Define CollectionRecord, CollectionReceipt, CollectionResult and concrete managed CollectionCallbacks exactly as C6/C11. Keep receipts internal and reuse storage.Identity/RetiredReplica; avoid import cycles.
- [x] Prepare the final RetirementStore method set Retired/Collect/ForgetCollection and document exact deletion proof versus proof release. Coordinate interface activation with the complete walstore implementation in Task 56; no successful placeholder ForgetCollection.
- [x] Specify prepared/collected validation and key encoding helpers: length-prefixed group/node plus big-endian replica ID, cluster checked against the journal pin. Add pure shape/key tests, including malformed tokens/receipt state.

**Verification**

- [x] Compile contract users, run focused authoritystore units and `make test`.

### Task 7: Define replicated operation and private query payloads

**Files:** `internal/metaengine/operator_proposals.go`, `repair_proposals.go`, `codec.go`, `reads.go`, `proposal_dispatch.go`. **Target:** [T] §7.1–7.3; contract C10. **Depends on:** 1–6. **Budget:** ≤550.

- [x] Add exact BeginOperation, RetryOperation (no Plan), TransitionOperation, CommitRepairOperation and RecordCollection payloads; add ExpectedTarget/Scopes to voter-change proposals and optional Intent to document proposals. Define the C5 intentRecord dependency here; Task 8 reuses it rather than creating another type.
- [x] Add BeginCleanupEffect/RecordCleanupStatus and progress/collection_check query shapes. The latter returns only CollectionCheck{Untracked, AppliedIndex}; authenticated Courier and optional ReadmissionKey travel with retained receipt acceptance.
- [x] Test strict payload decoding, required fields, correlation IDs, structured errors and rejection of client-selected authority classes. Keep new mutation dispatch unavailable until its handler is ready; remove ActivateTarget in Task 78, not by mapping it to fake completion.

**Verification**

- [x] Run codec/proposal units and `make test`; confirm no protobuf change is needed for existing private JSON envelopes.

### Task 8: Implement v4 metadata models and deterministic serialization

**Files:** `internal/metaengine/sm.go`, `disk.go` (custom JSON codecs), snapshot helpers, new `progress.go`/`cleanup.go`. **Target:** [T] §4.1, §6.2; contracts C5/C8. **Depends on:** 1–7. **Budget:** ≤550.

- [x] Implement exact smState v4 fields and group/node convergence, intent, cleanup, authority Class and scoped revocation records. Document gains RebasedFrom; no Active field or CollectionAcks in the final format.
- [x] Serialize deterministic sorted unique arrays and preserve existing unrelated state/bucket encodings. Define operation records around the immutable Plan and separate block statuses; preserve all current session/lock/admission/allocation data.
- [x] Test deterministic round trips and nil tombstone versus never-converged records. Keep this codec isolated from persistent writes until restore validation and state installation are ready; record the activation point in Task 78.

**Verification**

- [x] Run pure codec/snapshot units and `make test`; inspect the complete v4 shape against C5.

### Task 9: Rebuild scoped lookup and obligation indexes

**Files:** `internal/metaengine/scoped_state.go`, new scoped index helpers/tests. **Target:** [T] §4.1–4.3, §8.4. **Depends on:** 8. **Budget:** ≤550.

- [x] Rebuild group/node records and owners, operation lookup, first/latest attempt, Previous→retry, exact cleanup, parent/kind/digest intent, rebase-successor and resource-obligation indexes from durable rows. No independently persisted indexes or new public API.
- [x] Reject duplicate lookup keys and conflicting retry/rebase edges atomically. Use one common resource index for unfinished execution/cleanup grants and missing revocation evidence; bound work by actual retained evidence, not worker counts or the removed debt cap.
- [x] Test equivalent indexed results before/after JSON restoration, exact successor identity separation and atomic rejection preserving prior decoded state. Keep full graph/proof validation explicitly assigned to Tasks 10–15 and live installation disabled until Task 78.

**Verification**

- [x] Run index/codec units and `make test`; run staged lint and inspect the changed-Go-line budget.

### Correction 2: Preserve scoped settlement and exact cleanup identity

**Files:** scoped indexes/obligations, collection payloads/callbacks, permanent staging guard and regressions. **Target:** §4.1–4.3, §6.2–6.3, §7.1–7.3; review thread 3. **Budget:** ≤550 Go lines.

- [x] Reuse current-floor issuance and revocation settlement predicates, including late consumption, newer fences and resumable leadership transfer. Keep older non-floor records as proof, not independent issuance gates.
- [x] Reject raw caller RebasedFrom (superseded by Correction 6 normalization); carry the entire expected cleanup Action and historical member/retirement proof through internal callbacks. Correct the contradictory target signatures.
- [x] Store immutable document index values and identify original approval only by attempt 1 with no Previous. Reject mismatched lookup identities and never substitute a retained retry for a missing original.
- [x] Run focused regressions, exact staged lint and `make test`; record all review dispositions and downstream proof/rename obligations.

### Task 10: Validate scoped documents and immutable operation attempts

**Files:** scoped restore helpers/tests, existing topology plan validators. **Target:** [T] §4.1–4.3, §8.4, §12 retry algorithm. **Depends on:** 9. **Budget:** ≤550.

- [x] Validate rules, document/rebase/applied-index boundaries and retained operation source documents without a hard history cap. Preserve the distinction between historical FullyConverged and current group progress.
- [x] Validate complete operation/block status, immutable plans and restricted dependency graphs. Reject Previous cycles, skipped/forked retries, altered plans and lost terminal sibling proof. Reuse existing document-independent guard validation; exact approval/scope and authority checks remain in their owning tasks.
- [x] Test failed attempts with successful siblings, missing sources, malformed plans/attempts, historical markers and validation without candidate mutation. Keep these pure helpers isolated from live installation until Task 78.

**Verification**

- [x] Run focused scoped graph/restore and topology units, staged lint and `make test`; check the Go diff budget.

### Task 11: Validate convergence, identity floors and retained provenance

**Files:** scoped convergence/identity restore helpers/tests. **Target:** [T] §4.1–4.3, §8.4. **Depends on:** 10. **Budget:** ≤550.

- [x] Validate group/node declarations, independent source versions, exact owners and tombstones. Reject dangling or duplicate nonterminal owners; do not synthesize a composite Active document. Retain self-contained checkpoints when obsolete whole documents can safely be pruned.
- [x] Validate intent/rebase provenance, monotonic allocations/retired IDs and unchanged session/lock records using existing checks where valid. Admission/readmission and voter-switch proof checks belong to Tasks 14–15 after authority and cleanup validation.
- [x] Test tombstones, conflicting declarations, stale/missing owners, decreasing floors and rejection without replacing prior state. Do not impose hard history/debt caps or omit original restore obligations.

**Verification**

- [x] Run focused convergence/identity units and `make test`; run relevant persistence checks only if this task acquires a store.

### Task 12: Validate scoped authority catalog, actions and revocations

**Files:** authority restore helpers, scoped catalog validation and tests. **Target:** [T] §4.1–4.3, §6.2, §8.4. **Depends on:** 11. **Budget:** ≤550.

- [x] Validate exact grant/resource/floor/action/receipt tuples and scoped revocations. Reuse unchanged grant, receipt, boundary and readmission-grant validators; do not apply the removed global debt cap or synthesize Active.
- [x] Derive Class from retained required/cleanup membership, with exact independently retained debt proof when original operations can be released. Validate sorted unique revocation obligations and retained operation scope; settlement and issuance share the existing predicates.
- [x] Test forged actions/classes, missing/decreasing floors, contradictory evidence and scoped revocations, including valid debt above the removed cap. Wire this stage into isolated candidate decoding without enabling live v4 installation.

**Verification**

- [x] Run focused catalog/restore units, staged lint and `make test`; preserve the 550-line ceiling.

### Task 13: Validate exact cleanup milestones and collection proof

**Files:** scoped cleanup validation and restore tests. **Target:** [T] §4.1–4.3, §6.2–6.3, §8.4. **Depends on:** 12. **Budget:** ≤550.

- [x] Validate cleanup identity/storage against retained approved/proven declarations, exact stop/retirement/collection tuples, milestone ordering and evidence equality. Refuse collection without retirement proof, collection for memory storage and contradictory identity floors; preserve all unresolved evidence.
- [x] Require the applicable approved-action or complete retained readmission proof path. Whole-group deletion retains exact required-stop proof without inventing native removal receipts. Failed/unconsumed cleanup remains restorable.
- [x] Wire cleanup proof checks into isolated candidate decoding; test atomic refusal and equivalent successful lookups. Keep live v4 installation refused until Task 78.

**Verification**

- [x] Run cleanup/restore units, staged lint and `make test`; maintain the 550-line ceiling.

### Correction 3: Require approved provenance for rebases and compacted checkpoints

**Files:** scoped document/convergence validators and fixtures. **Target:** [T] §4.1–4.3, §5.3; review thread 4 findings 2/5. **Budget:** estimated 250–400 changed Go lines, ceiling 550.

- [x] Prove the atomic L/R/S ancestry and that rebased L has no approval/convergence/action authority. Use only independently approved context for compacted checkpoints.
- [x] Correct fixture provenance and cover each contradictory witness, missing repair ancestry and staged-only declaration.
- [x] Run focused regressions, make test and staged lint before committing.

### Correction 4: Preserve exact cleanup progress and retry lineage

**Files:** scoped cleanup/authority validation and fixtures. **Target:** [T] §4.1–4.3, §6.1–6.2; review thread 4 findings 1/3/7. **Budget:** estimated 400–550 changed Go lines, ceiling 550.

- [x] Require two-way receipt/milestone agreement, the stop-before-retirement boundary and honest unfinished deletion ownership. Pin the originating operation and its source context until debt pruning.
- [x] Resolve exact successor-attempt actions without relabeling consumed predecessor receipts; test retry and unrelated-progress restoration. Remove obsolete archived-cleanup branches and duplicate validation. Apply the final peer ancestry-walk correction and test an approved ancestor behind multiple staged edits.
- [x] Run focused regressions, make test and staged lint before committing.

### Correction 5: Preserve cleanup origin through complete readmission

**Files:** scoped cleanup/readmission validators and regressions. **Target:** [T] §4.1–4.3, §6.2; review thread 4 round 3. **Budget:** estimated 250–400 changed Go lines, ceiling 550. Split from Correction 4 before coding because exact readmission/milestone preservation expands the necessary fixtures and checks.

- [x] Separate readmission-created debt from removal debt later covered by readmission. Preserve original operation/actions and consumed stop/native receipts; independently validate exact admission and complete batch coverage.
- [x] Keep older action grants bound until settled/fenced; require readmission receipt only for milestones it actually supplies. Correct the fixture that deleted a retained cleanup grant.
- [x] Run readmission/cleanup regressions, make test and staged lint.

### Correction 6: Normalize generated rebase provenance on ordinary edits

**Files:** FSM document installation, mutation derivation, regressions and retention notes. **Target:** [T] §3.4, §4.3, §5.3; review thread 4 findings 4/6. **Budget:** estimated 100–200 changed Go lines, ceiling 550.

- [x] Clear caller/inherited RebasedFrom on non-rebase installs and typed previews; test continued staging after a rebase. Replace the earlier rejection contract with normalization.
- [x] Document atomic retry-chain pruning and distinguish incomplete index-input tests from valid restored state. Clarify zero member IDs and collection clock/sequence scope.
- [x] Run focused regressions, make test and staged lint before committing.

### Task 14: Validate admission lineage and completed required effects

**Files:** scoped admission/completion validation and restore tests. **Target:** [T] §4.1–4.3, §8.4, §12 retry/admission algorithms. **Depends on:** 13. **Budget:** ≤550.

- [x] Validate retained admission/readmission identity and lineage, with original approved sequence and appropriate step evidence. Preserve failed admission evidence without granting it current execution authority; unrelated Target versions cannot invalidate an owned admission.
- [x] Validate completed required steps per block, including successful siblings and consumed prefixes inherited through Previous. Reuse exact grant/resource-consumption predicates; do not substitute fencing or legacy whole-document/global-cursor completion.
- [x] Wire validation into isolated owned candidate decoding; test atomic refusal and equivalent successful restore. Live activation remains Task 78; voter history proof follows in Task 15.

**Verification**

- [x] Run admission/completion/restore units, staged lint and `make test`; preserve live format rejection and the 550-line ceiling.

### Correction 7: Preserve consumed retirement across admission successors

**Files:** `internal/metaengine/authority_readmission.go`, scoped admission validation and regressions; target/plan clarification. **Target:** [T] §4.3, §5.2, §6.2, §12 retry/admission; review thread 5 round 1. **Budget:** estimated 150–250 changed Go lines, ceiling 550.

- [x] Share exact node/member consumed-readmission evidence between live preparation and scoped admission restore; preserve the original batch tuple across attempts/processes. Remove the redundant batch forwarder.
- [x] Test retry readiness and decoder atomic refusal for incomplete or mismatched prior retirement; retain learner/promotion effect checks.
- [x] Record newer-approval required-stop takeover, retry admission rebinding and full pinned-operation proof costs in their implementation tasks. Keep future scoped approval disabled until takeover validation and transitions are implemented together.
- [x] Run focused regressions, make test and staged lint before committing.

### Task 15: Validate retained voter history before restore installation

**Files:** scoped voter validation and restore tests. **Target:** [T] §4.1–4.3, §8.4. **Depends on:** 14. **Budget:** estimated 350–500 changed handwritten Go lines, ceiling 550.

- [x] Validate voter-switch identities, allocation/index bounds, cooldown retention and completed effects using scoped block/Previous proof. Reuse unchanged shape checks without global completion or synthetic Active.
- [x] Complete owned state/index/proof preparation before candidate replacement. Test malformed retained voter proof and atomic refusal, equivalent successful restored lookup and valid history after operation pruning. Live Machine/format installation remains Task 78, where handlers and persistence activate together.

**Verification**

- [x] Run voter/proof/restore units, focused persistence checks if a store is acquired, staged lint and `make test`; verify live format rejection remains intact.

### Task 16: Move voter planning proof to explicit approved and proven inputs

**Files:** `topology/plan_input.go` (new), `voter_moves.go`, `voter_transition.go`, `voter_guards.go`, `plan_voter_proof.go`, immediate private callers/tests. **Target:** [T] §5.1, §7.2; contract C12. **Depends on:** 2–3. **Budget:** estimated 300–450 Go lines, ceiling 550.

- [x] Introduce exact PlanInput value fields. Convert private voter proof/state helpers to per-group/per-node proven lookups, keeping Approved independently available for placement checks; never build a synthetic composite Document.
- [x] Preserve frozen old/new domain interpretations, learner-first promotion/demotion, exact identity and live-quorum checks. Test approved/proven disagreement, unknown/tombstoned convergence and input immutability through the new private path.
- [x] Keep existing public planner signatures until Task 22 converts all callers atomically. The private document-to-input adapter represents only the existing whole-document callers; remove its production planner use in Task 22 and any remaining old API adapters at cutover Task 78. Do not expose a second planner API.

**Verification**

- [x] Run voter/planner/readmission units, staged lint and `make test`; record temporary adapter references and the measured Go diff.

### Task 17: Move plan shape and readmission proof to scoped inputs

**Files:** `topology/plan.go`, `planner.go`, `readmission_plan.go`, nearby tests. **Target:** [T] §5.1, §6.2, §7.2; contract C12. **Depends on:** 16. **Budget:** estimated 350–500 Go lines, ceiling 550.

- [x] Convert private shape, simulated membership, founding/deletion and readmission helpers to PlanInput, distinguishing approved intent from exact converged group/node declarations and member IDs.
- [x] Preserve stop-before-retirement, whole-group former-owner coverage, guarded role ordering and exact replacement preparation. Retain exact former-owner lookup from supplied proven declarations; no reconstructed cluster document. Task 22 must additionally carry intermediate identities from retained effects when they are absent from both current approval and convergence.
- [x] Test unknown initial convergence versus tombstone, retained former identities and approval/proof disagreement; keep old public entry adapters until Task 22. Required-stop takeover planning is completed with block/dependency derivation in Task 28.

**Verification**

- [x] Run shape/destructive-boundary/readmission/voter units, staged lint and `make test`.

### Correction 8: Preserve load-owner readmission proof after metadata convergence

**Files:** `topology/plan_input.go`, `readmission_plan.go`, scoped readmission tests. **Target:** [T] §5.1–5.2, §6.2; review thread 6. **Budget:** estimated 150–300 changed Go lines, ceiling 550.

- [x] Prefer the known historical node member for former load ownership; fall back to the converged meta member only when that node member is unknown or zero. Keep meta admission identity independent of delayed load convergence; remove the max-member helper.
- [x] Preserve an immutable plan's exact readmission boundary after meta convergence. Settle and implement load-only replanning without repeating a completed metadata admission, retaining exact readmission authorization at apply time.
- [x] Test meta-first convergence, unchanged/fresh/unknown identities, load stop elision and incomplete-boundary refusal. Keep the existing remaining-plan case with no node declaration passing.
- [x] Run focused units, `make test`, staged lint and the commit-25 full lint/dead-code checkpoint; record the correction. This is commit 25 and triggers the regular peer review before the next implementation commit.

### Correction 9: Close the commit-25 review and clarify stale member intent

**Files:** readmission planner/error assertions, legacy input comments, metadata rollback regression, removal-task comments. **Target:** [T] §5.2 FI_READMISSION_BOUNDARY; review thread 7. **Budget:** estimated 100–150 changed Go lines, ceiling 550.

- [x] Return VersionConflict with fresh-intent guidance when desired membership precedes proven membership; keep the refusal and exact identity guards unchanged.
- [x] Retain the service/FSM regression proving rollback and raw staging cannot reuse an approved retired member during unfinished readmission. Record both reviewers' before/after evidence and withdrawal of the unreachable-regression claim.
- [x] Document legacy DeletionBase/OperatorBase composite inputs and their dependence on the allocation guard; update named adapter removal references for the Tasks 18–22 split and cutover 66.
- [x] Run focused topology/operator units and rollback functional checks, staged lint and make test; close review thread 7 with no remaining decision.

### Task 18: Carry retained identity and founding evidence in planner inputs

**Files:** `topology/plan_input.go`, new retained value/validation helpers, voter identity/proof helpers and nearby unit tests. **Target:** [T] §5.1, §7.2 FI_PLAN_EVIDENCE; contract C12. **Depends on:** 16–17, Correction 8. **Budget:** estimated 350–500 changed Go lines, ceiling 550.

- [x] Add the exact RetainedGroup/RetainedReplica/RetainedProgress values and PlanInput.Retained. Separate possible founding, hosted state, cleanup ownership and proven stop from declaration/role/convergence/authority.
- [x] Validate exact nonzero unique replica IDs, host bindings, progress values, Desired disjointness and available identity floors; reject contradictions without mutating inputs. Reuse ReplicaKey for retained lookup; route voter identity/proof lookups through the retained input without inventing replica roles. The FSM collector checks the full historical allocation floor.
- [x] A retained founding grant or possible retained host prevents refounding, even when all founding identities remain Desired or no converged declaration exists. Preserve unknown-versus-tombstone semantics and independent Approved placement rules.
- [x] Test twice-superseded native identities, conflicting bindings, retained/fresh/tombstoned founding, input immutability and unknown native refusal. Preserve guarded old/new domains and pinned-learner safety. Preserve the named removal comments updated in Correction 9: public planner conversion 22, cutover 66.

**Verification**

- [x] Run focused retained/voter/founding/shape units, staged lint and `make test`. No new public planner overload or synthetic Document is permitted.

### Task 19: Plan and validate retirement from exact retained progress

**Files:** topology planner, voter moves, shape/simulation, deletion and readmission helpers; retained retirement tests. **Target:** [T] §6.2, §7.2 FI_PLAN_EVIDENCE, §12 deletion algorithms. **Depends on:** 18. **Budget:** estimated 400–550 changed Go lines, ceiling 550.

- [x] Enumerate former owners from Converged plus Retained. Native hosted identities receive guarded removal; non-native hosted identities receive exact no-op removal and host retirement. Native readmission-stopped identities receive removal only. Cleanup-owned identities never receive duplicate removal/retirement actions.
- [x] Carry only required stops for cleanup-owned non-native identities: same-host incoming assignments and every whole-deletion owner. Preserve detached debt ownership; partition/action rebinding follows in Tasks 28–34. Reject unproven stop elision and malformed adjacency without blanket live/native requirements on host-only deletion.
- [x] Update role/shape/native simulation and whole-group deletion checks together. Never infer roles from retained identity alone. Keep readmission exact-member boundaries and ordinary promotion behavior; retain old immutable-plan identities even after successful host cleanup when validating an existing plan.
- [x] Test native/prepared former owners missing from all current documents, stopped versus cleanup-owned debt, required same-host stops, full/partial deletion and no second retirement action. A possibly-founded deleted group remains attributable without a fabricated converged declaration.

**Verification**

- [x] Run focused planner/readmission/retirement/voter units, staged lint and `make test`.

### Task 20: Build retained planning evidence from metadata records

**Files:** `internal/metaengine/load_catalog.go`, cleanup/authority evidence helpers, internal Machine planning read and unit tests. **Target:** [T] §4.3, §5.2, §7.2 FI_PLAN_EVIDENCE; contract C12. **Depends on:** 18–19. **Budget:** estimated 400–550 changed Go lines, ceiling 550.

- [x] Reuse existing retained operation/document/grant identity sources to produce PlanInput metadata with owned values. Issued Prepare and CreateFoundingGroup grants count with any outcome; approved intent alone does not create host state. Keep FoundingIssued before subtracting Desired.
- [x] Derive Hosted/Cleanup/Stopped from exact cleanup/readmission evidence, historical MemberID and allocation bounds. Preserve the original actionless readmission debt and removal-origin receipts. Retained native contradictions require applied-index comparison: stale reads are retryable unavailability; sufficiently recent contradictions refuse.
- [x] Share one pure retained-evidence collector between read-side production and later scoped apply revalidation. Give current schema-3 boundaries actual Active declarations and exact separately retained evidence; never decompose the synthetic DeletionBase into alleged convergence. Scoped-state adapters and authoritative approval wiring follow in Tasks 37–38 and 64/74.
- [x] Expose only the needed internal Machine planning read, with a real implementation. Keep legacy DeletionBase/OperatorBase only while old nonplanner consumers still need them; record their exact removal task. No public Node/Cluster surface or exported test-only workaround.
- [x] Test consumed/refused/unknown preparation/founding, successful cleanup, same-name readmission, lagging native evidence, identity conflicts, source ownership and multiple supersessions. Pinning writer changes remain Task 52; do not assume pruning can erase founding evidence meanwhile.

**Verification**

- [x] Run focused metadata catalog/cleanup/readmission units, staged lint and `make test`; use functional tests only if acquiring storage or a replica.

### Task 21: Convert public plan validation and its immediate callers

**Files:** `topology/plan.go`, related validator/readmission entries, `internal/metaengine` approval/admission/repair validation and tests. **Target:** [T] §5.1, §7.2; contract C12. **Depends on:** 18–20. **Budget:** estimated 300–500 changed Go lines, ceiling 550.

- [x] Change ValidatePlanShape to (PlanInput, Document, Plan) and all immediate callers atomically. Reuse the internal planning collector for metadata validation. Keep ReadmissionCovers/ValidateVoterSetTransition with the RemainingPlanView/PlanRead native-context conversion in Task 22; those are not immediate shape callers. Their later conversion must preserve exact retained proof and update target signature notes. No public old-signature aliases.
- [x] Keep existing immutable-plan validation separate from new-plan omission of settled identities: retain exact old steps/voter bases and readmission provenance through the operation's pinned records. Do not fabricate declarations to satisfy shape checks.
- [x] Adapt pure fixture helpers to the actual owned base and explicit retained evidence; preserve behavioral assertions. Keep the existing BuildPlan/BuildPlanPreferred entry signatures until Task 24, so the remaining caller/native conversion is a bounded atomic commit.
- [x] Test independent proven groups, newer Approved versus older sources, intermediate identity shape, retry after prior cleanup, and stale/member/domain rejection. Remove any private adapters made obsolete by this conversion; remaining legacy adapters have Task 24/76 removal obligations.

**Verification**

- [x] Run shape/voter/readmission/metadata units and relevant focused planning functional tests, staged lint and `make test`.

### Correction 10: Align legacy planning with issued host evidence

**Files:** `internal/metaengine/load_catalog.go`, metadata/service regressions, plan/comment references. **Target:** §7.2 FI_PLAN_EVIDENCE; review thread 8 finding 1. **Budget:** estimated 180–350 changed Go lines, ceiling 550.

- [x] Reproduce both failed unfounded-group deletion and omitted unprepared replacement through the real FSM; retain established retirement work.
- [x] Filter the existing legacy new-planning catalog using actual converged/issued retained evidence. Never treat approval alone as host work or add a composite-to-convergence validation adapter; leave immutable OperatorBase attribution and authorization catalogs intact until their planned removal.
- [x] Verify issued but unacknowledged effects, completed-cleanup immutable retries, focused functional regressions, staged lint and make test. Record thread-8 disposition and update removal references after the Task 22–24 split.

### Task 22: Convert immutable remaining-plan validation to explicit evidence

**Files:** `topology/readmission_plan.go`, `voter_transition.go`, `internal/operator/remaining_plan.go`, `internal/metaengine/reads.go`, `internal/metaservice/plan_validation.go`, matching tests. **Target:** [T] §5.1–5.2, §7.2 FI_PLAN_EVIDENCE. **Depends on:** 18–21. **Budget:** estimated 400–530 changed Go lines, ceiling 550.

- [x] Change ReadmissionCovers and ValidateVoterSetTransition to accept PlanInput, converting every caller atomically. Preserve exact retained identities, frozen voter bases, historical member ownership and proven placement rules.
- [x] Replace RemainingPlanView/PlanRead's composite Base with actual planning evidence from the metadata collector. Preserve immutable original steps and retained grant/receipt provenance after cleanup; never reconstruct convergence or replica roles from an operation's steps.
- [x] Use proven/retained/desired identities as native-read candidates; keep exact founding retry authorization and current applied/native guards. Remaining retirement checks distinguish Hosted/Cleanup/Stopped without duplicating a debt's actions or weakening required stops.
- [x] Convert owning tests, add explicit retained/context and completed-cleanup retry coverage, and remove obsolete validation adapters. The remaining construction adapter is removed in Task 24.

**Verification**

- [x] Run remaining-plan/readmission/voter/metadata units, focused immutable-plan functional regressions, staged lint and `make test`.

### Task 23: Collect actual native state for new-plan callers

**Files:** `internal/metaservice/planning.go`, `bootstrap.go`, planning helpers and functional tests. **Target:** [T] §5.1, §7.2 FI_PLAN_EVIDENCE. **Depends on:** 20–22. **Budget:** estimated 350–500 changed Go lines, ceiling 550.

- [x] Collect owned PlanInput through metadata PlanningInput and real controller ReadNative calls, including retained observation candidates. Recollect metadata against returned native evidence; preserve exact reporter/group/applied identity and stale/removal rejection.
- [x] Keep GroupProgress observations for liveness/leadership only. While the old BuildPlan signature remains until Task 24, its membership observations must derive from actual NativeState, never manufacture NativeState from cached GroupStatus. Document the exact temporary call adaptation and remove it in Task 24; no new composite document or authority mode.
- [x] Preserve actual founding metadata bootstrap, unknown/possibly-founded refusal and host-only deletion without a live quorum requirement. Scope-only collection remains Task 25; schema-3 actual convergence changes to scoped collection in Tasks 66/76.
- [x] Test cached/native disagreement, missing or foreign native evidence, retained candidates, actual founding and quorum safety through existing single-participant/protocol-double fixtures.

**Verification**

- [x] Run planning/bootstrap functional regressions, relevant units, staged lint and `make test` across all five application modules.

### Task 24: Convert public construction signatures and remove old adapters

**Files:** `topology/planner.go`, native/observation helpers, all immediate production/test planner callers. **Target:** [T] §5.1, §7.2; contract C12. **Depends on:** 18–23. **Budget:** estimated 400–550 changed Go lines, ceiling 550; re-estimate before editing.

- [x] Use BuildPlan(PlanInput, Document, Rules) and BuildPlanPreferred(PlanInput, Document, PlanOptions); update every immediate caller in the same commit. Consume the real inputs already collected in Task 23 and remove the temporary old-call adaptation.
- [x] Bind membership exclusively from actual NativeState, checking exact group/reporter/identity and applied evidence; use GroupProgress observations only for separate liveness/leadership. Keep exact founding seed handling and host-only deletion behavior.
- [x] Delete obsolete document-to-input production adapters without a parallel public planner or synthetic Active. Tests may decompose genuine fixture documents and explicit membership; production may not label composite documents as convergence.
- [x] Test approved/proven/native disagreement, independent convergence, unknown/missing native refusal, live quorum and pinned-learner safety. Keep all original conversion obligations; semantic scopes follow in Task 25.

**Verification**

- [x] Run planner/readmission/voter units, focused planning/bootstrap functional regressions, staged lint and `make test` across all five application modules.

### Task 25: Derive semantic affected groups

**Files:** `topology/placement.go`, new topology scope helper/tests. **Target:** [T] §5.1–5.3, §12 “Scoped authorization and completion”. **Depends on:** 24. **Budget:** estimated 250–400 changed Go lines, ceiling 550.

- [x] Reuse existing placement evaluation to derive affected group declarations, effective placement/eligibility, membership, exact identity allocation and whole deletion scopes. Node-only edits require a `_meta` declaration scope.
- [x] Keep group-specific dependencies on relevant node eligibility/domain fields and exact participant identities; unrelated descriptive node fields must not become load repair dependencies. Refuse independence conservatively when dependency evaluation fails.
- [x] Test disjoint group edits, inherited versus overridden placement rules, node label/type changes, zero-runtime declarations, exact allocation/member changes, deterministic order and input ownership. No new ordinary Node/Cluster API or general dependency registry.

**Verification**

- [x] Run scope/placement units, staged lint and `make test`; record the commit-35 dead-code audit and request peer review.

### Correction 11: Separate operation ownership from repair candidate reads

**Files:** topology scope helper/tests; target §5.1 clarification. **Target:** [T] §5.1–5.3; review thread 9. **Budget:** estimated 200–350 changed Go lines, ceiling 550.

- [x] Keep AffectedGroups as write/ownership scope: changed declarations/effective rules, assigned-host eligibility/state/member changes and `_meta` node declarations. Adding/removing an unassigned node must not seize unchanged load groups or require their native quorum.
- [x] Preserve genuinely materialized load changes and assigned predecessor/readmission boundaries. Keep candidate eligibility/member read dependencies separate in Task 27; ignore unassigned candidates ineligible in both compared versions. No new public read-set type or API.
- [x] Reproduce eligible/ineligible node addition and removal, assigned/unassigned identity and placement changes, and actual new load assignments. Keep conservative invalid-input handling, deterministic ordering and existing scope/copy regressions.
- [x] Run focused topology units, exact staged lint and make test; record peer disposition before the next implementation commit.

### Task 26: Restrict planning work to affected scopes

**Files:** `topology/planner.go`, shape/deletion helpers, `internal/metaservice/planning_input.go`, focused tests. **Target:** [T] §5.1–5.2, §7.2. **Depends on:** 25. **Budget:** estimated 400–550 changed Go lines, ceiling 550.

- [x] Restrict native requirements, observations and service collection to semantic ownership scopes; unchanged unhealthy A must not gate B. Admission uses the mandatory state predicate and never adds unchanged load scopes. Preserve exact guards for every affected group and actual founding seed handling.
- [x] Keep unrelated retained deletion debt outside B's whole-deletion checks without dropping required stops for a deleting/taken-over group. Preserve immutable original-plan validation separately from new-plan scope selection.
- [x] Test B planning with A unavailable, affected A refusal, declaration-only work, independent deletion debt and required exact host coverage. Stage these scoped helpers until their block/approval and persistence consumers are ready: never feed a partial scoped plan into legacy whole-document completion. Final activation remains Task 78; no runtime authority-mode flag or compatibility API.

**Verification**

- [x] Run planner/shape/retirement units and focused service native-collection functional checks; staged lint and `make test`.

### Task 27: Validate semantic scopes and independence in the FSM

**Files:** `internal/metaengine` scope/approval helpers and owning-package tests. **Target:** [T] §5.1–5.3, §12 “validateApprovalScopes” and repair rebase. **Depends on:** 25–26. **Budget:** estimated 300–500 changed Go lines, ceiling 550.

- [x] Derive semantic read/write conflicts from trusted approved/candidate declarations and exact allocation evidence. AffectedGroups supplies ownership scopes only; separately compare repair candidate eligibility/domain/member reads with existing placement evaluation. Ignore unassigned candidates that are ineligible in both versions; do not merge candidate reads into operation ownership or native collection. Service scope lists are checked evidence, never trusted scope selection. Keep the common derivation used by planning and replicated validation.
- [x] Check captured group source versions/current owners and required affected group sets under the FSM lock. No admission invalidation closure is needed; the final exact ownership set and atomic installation remain Tasks 37–38. Unknown dependencies refuse independence.
- [x] Test omitted/extra/duplicate scopes, stale source/owner expectations, disjoint edits versus shared eligibility/rules, node-only edits and allocation conflicts. Repair must not read every unrelated node field; same-group edits and exact host/member changes remain conflicts.

**Verification**

- [x] Run scope/approval/repair units, staged lint and `make test`. New scoped apply paths remain isolated until complete activation.

### Task 28: Build required blocks and the two barrier shapes

**Files:** `topology/planner.go`, `plan.go`, readmission/voter plan helpers. **Target:** [T] §3.3, §5.2, §6.2. **Depends on:** 22–27. **Budget:** ≤550.

- [x] Partition steps by exact group and source version; classify only safe detached stop/retirement actions as Cleanup. Whole-group deletion stops and same-host replacement prerequisites stay required.
- [x] Carry unfinished required stops for exact removed-but-unstopped identities when a newer approval takes over a failed or superseded group; derive them from retained debt, even when convergence or current native membership omits that identity. Keep the old origin/retirement action and consumed milestones.
- [x] Emit restricted admission/removal edges for local ordering; both barriers use mandatory state predicates. Admission proof can come from another operation, without adding its scope. Retain immutable global sequence numbers and deterministic within-block ordering. Zero-step declaration blocks still have provenance.
- [x] Test all-step coverage, no cleanup prerequisite masquerading as independent work, readmission coverage, former-owner deletion catalog and restricted edge forms. Adapt current adjacency validators to execution/cleanup separation without weakening their safety predicates.

**Verification**

- [x] Run plan shape/destructive boundary/voter tests and `make test`.

### Task 29: Preserve admission origin across operation handover

**Files:** `internal/metaengine/admission.go`, `authority_readmission.go`, disk/restore/cleanup helpers, `internal/metaservice/join_checks.go`, owning tests. **Target:** accepted admission amendment FI_ADMISSION_ORIGIN; [T] §5.2, §7.1. **Depends on:** 15, 25–28. **Budget:** estimated 250–450 changed Go lines, ceiling 550.

- [x] Add the internal FSM-assigned ReadmissionOperatorID field with its exact JSON name and stable ReadmissionID fallback. Preserve independent operator/instance origins and existing grant encoding; pending process replacement copies both dimensions.
- [x] Reject caller-selected origins; validate origin operation ordering, exact approved member and original admission sequence on scoped restore. Validate process-origin references by resolved original operator; reject unsupported origin use in legacy restore until scoped activation. Preserve disk/join fields and original cleanup attribution without relabeling grants or receipts.
- [x] Test identity stability across retries/version changes and process replacement, exact proof lookup, malformed/missing/foreign origins, atomic restore refusal and join/bind refusals. Live handover and resumed-prefix validation are Task 33; this task enables no new owner transition.

**Verification**

- [x] Run admission/readmission/restore units, relevant join functional regressions, staged lint and `make test`.

### Correction 12: Isolate scoped construction and admit before decommission

**Files:** topology planner/metadata replacement helpers, remaining-plan native capacity, join validation tests and design amendment. **Target:** peer thread 11 findings 1–2; FI_META_SURGE. **Budget:** ≤550 Go lines.

- [x] Keep legacy construction entirely flat, including converged-version previews; partition only scoped construction. Remove the temporary flat construction option at Task 78.
- [x] Prepare/add incoming metadata hosts before decommissioning members. Bound temporary native seats by MaxNodes plus exact still-native decommissioned identities; preserve document/voter limits, learner-first waits, live-quorum and placement guards. No decommission allowance for same-node readmission.
- [x] Apply the same exact contracting allowance in remaining-plan validation. Recompute the allowance from each simulated state and require the exact planned Remove. Verify join grants accept exact retained membership hints without treating them as final document capacity.
- [x] Add live/scoped three-node replacement, four-node learner/reduction edge cases, converged previews and resumed native-surge regressions before/after removal. Test same-node order, refused unknown identities and missing planned removals. Missing replacement readiness leaves the old member configured; removal-only approvals remain available. Remove the redundant deletion-kind test. Run focused units/functionals, staged lint and make test.
- [x] Assign readmission-owned load-only retirement and exact completion proof to Tasks 34–37, and explicitly preserve shared approval validation in Task 49. Origin provenance test boundaries are resolved in thread 11 round 3.

### Task 30: Prepare scoped approval validation and ownership changes

**Files:** `internal/metaengine` approval/scope/operation helpers and tests. **Target:** [T] §5.2, §7.1, §12 “Approve”/“validateApprovalScopes”. **Depends on:** 15, 25–29. **Budget:** estimated 350–500 changed Go lines, ceiling 550.

- [x] Prepare applyBeginOperation validation with exact Latest/plan/source/owner expectations and independent FSM scope derivation. Required blocks equal semantic ownership scopes; unchanged admission-dependent load blocks keep their owner. Reuse one FSM-side check under the apply lock for new approvals; repair/voter/readmission handlers adopt it in their owning tasks.
- [x] Prepare scoped supersession, owner changes and operation state on owned copies. Explicit user approval supersedes affected user scopes only; repair cannot seize them. Preserve unrelated blocks and original attribution. Do not install partial state before Tasks 33–38 supply admission/debt/revocation preparations.
- [x] Reject duplicate cleanup-owned retirement at new approval: consumed Remove, Cleanup for Cleanup/Stopped, Stop for Stopped, DeleteGroupState without an uncovered Hosted owner. Allow exact required same-host/deletion debt stops without another Remove. Keep immutable approved-plan validation valid after consumption; no public validation mode.
- [x] Test forged omitted/extra/duplicate blocks, stale captures/CAS, unavailable siblings, consumed retirement refusal versus immutable retry acceptance, and unchanged state on refusal. Any provisional handler returns classified Unsupported until atomic preparation is complete.

**Verification**

- [x] Run scope/approval/retirement/atomicity units, staged lint and `make test`.

### Task 31: Keep load-only retirement owned by pending readmission

**Files:** `topology/readmission_plan.go`, scoped planner/retirement tests. **Target:** accepted admission amendment, thread 11 finding 3; [T] §5.2, §12. **Depends on:** 29–30. **Budget:** estimated 150–300 changed Go lines, ceiling 550.

- [x] For load-only successors during in-progress readmission, use the exact approved replacement member binding to select predecessor retirement ownership. Do not synthesize an unowned metadata block or reintroduce unroutable old-host Stop/Cleanup.
- [x] Preserve required native removal and other hosts' stops/deletion. An all-readmission deletion may have a zero-step block; Task 37 must require exact consumed retirement for every omitted former identity before completing it. Approved binding selects ownership, never proves consumption.
- [x] Test pending versus completed predecessor retirement without metadata convergence; receipt-level preparation facts remain covered by Tasks 32/34–35, exact predecessor/member binding, same-member/nonreplacement refusal, mixed-host and all-readmission deletion. Preserve immutable caller inputs and ordinary metadata promotion order.

**Verification**

- [x] Run focused scoped readmission/retirement/planner units, staged lint and `make test`.

### Task 32: Validate resumed admission prefixes and origin facts

**Files:** `internal/metaengine` admission/restore helpers and tests. **Target:** accepted admission amendment FI_ADMISSION_ORIGIN; thread 11 finding 5; [T] §5.2, §12. **Depends on:** 29–31. **Budget:** estimated 350–500 changed Go lines, ceiling 550.

- [x] Validate original approved/consumed admission prefix plus the successor's approved suffix. Already-native successors may omit Prepare/Add; exact receipts or metadata convergence must justify that omission. Recompute admission milestones without inventing native membership or relabeling old grant/action attribution.
- [x] Extend scoped restore to the same prefix/suffix contract. Keep Previous exclusively for immutable-plan retry. Share proof validation with the handover preparation in Task 33 and later retry/non-founding admission paths.
- [x] Validate an eligible origin against issued facts: exact original readmission AdmissionID, origin no later than the earliest retained prefix fact, ordered same-member prefix operations between origin/current owner. Current-owner suffix facts are not origin evidence. Pending without issued facts needs structural eligibility only; no fabricated receipt or new provenance ledger.
- [x] Test before/after Prepare/Add, zero-step and promotion-only successors, retained original retirement, missing/contradictory prefix evidence, eligible-but-wrong origin contradicting a batch/prefix, and eligible no-fact Pending acceptance. Restore refusal must not mutate the input graph.

**Verification**

- [x] Run scoped admission/readmission/restore units, staged lint and `make test`.

### Task 33: Prepare same-record admission handover

**Files:** `internal/metaengine` admission/approval helpers and tests. **Target:** accepted admission amendment FI_ADMISSION_ORIGIN; [T] §5.2, §12. **Depends on:** 29–32. **Budget:** estimated 250–450 changed Go lines, ceiling 550.

- [x] Prepare handover of the one live (node, instance) row to an exact same-member metadata successor. Save the original operator once, retain process origin, and preserve consumed facts and the complete retirement batch. Reject changed members, canceled/revoked proof and wrong owners; grant no replacement identity implicitly.
- [x] Use Task 32's original-prefix/successor-suffix proof validation and milestone derivation. Share preparation with later retry and non-founding admission paths; atomic candidate preparation is Task 37 and installation with scoped revocation protection is Task 38.
- [x] Test same-version retry and different-version supersession before/after preparation and learner membership, interleaved process replacement, stable original retirement identity, rejected stale/changed members or canceled authority, and unchanged source state on any refusal.

**Verification**

- [x] Run admission handover/restore units, staged lint and `make test`.

### Correction 13: Preserve flat admission validation and scoped native suffixes

**Files:** `topology/readmission_plan.go`, planner wiring and focused tests. **Target:** state-based admission amendment; peer thread 12 findings 2–3. **Budget:** ≤550 Go lines.

- [x] Limit approved pending readmission ownership to partitioned plans. Give binding the existing construction scope instead of an ambiguous empty plan; flat validation continues to require complete admission or exact convergence.
- [x] Preserve the existing planner suffix for an already-native scoped replacement. Never repeat its consumed Prepare/Add; keep legacy flat construction until activation.
- [x] Verify forged flat omission refusal, honest flat construction, scoped native learner/voter successors, focused units, staged lint and make test.

### Correction 14: Validate admission role history and checkpoint ordering

**Files:** scoped admission prefix validation, scoped demotion shape, dispatch comment and focused tests. **Target:** state-based admission amendment; peer thread 12 finding 1 and round 3. **Budget:** ≤550 Go lines.

- [x] Track current native role separately from historical promotion. Include exact Demote issuers in ordered prefix provenance; reject facts before the origin. Require the exact desired role-change suffix. Permit scoped demotion of an exact approved unconverged member while retaining its voter guards.
- [x] Initialize role once from an exact checkpoint; replay only newer consumed role changes, preserving promotion history. Refuse live admissions covered by metadata convergence; completed records still need succeeded blocks. Preserve immutable retry grammar.
- [x] Test demotion/transfer/zero-step refusal, multiple owners and re-promotion, checkpoint chronology/compaction/absent members, earlier Demote issuance, actual approval handover and retry. Run focused units, staged lint and make test.

### Task 34: Prepare exact required-stop handover

**Files:** `internal/metaengine` approval/cleanup/restore helpers and tests. **Target:** [T] §5.2, §6.2, §12 “Approve”; accepted admission amendment. **Depends on:** 29–33. **Budget:** estimated 350–500 changed Go lines, ceiling 550.

- [x] Prepare owned cleanup rows and rebind every unconsumed required StopAction in an affected scope to the successor's exact required step, including across approval versions. Preserve original OperationID, Storage, detached retirement, removal and consumed milestones. Missing/wrong-host/wrong-replica required stops refuse the whole preparation.
- [x] Extend restore in the same task: inspect the bound action's own required partition independently of the original StepSeq; accept its current owner or failed ownerless chain tip. Preserve deletion rows and the whole-group stop boundary. Keep old issued actions/receipts as historical evidence; both operation chains are proof roots for Task 52 retention.
- [x] For readmission-owned omitted required stops, require complete exact retirement for the former identity before superseding its old binding, and preserve the original readmission proof instead of rebinding to an unroutable predecessor. Existing actionless readmission debt stays actionless. Zero-step/new deletion completion proof is handled by Task 37.
- [x] Test cross-version and retry bindings with different step sequences, late predecessor receipts, current/failed-tip restore, omitted/foreign stops, consumed stop preservation, exact readmission omission and atomic preparation/refusal.

**Verification**

- [x] Run approval/debt/restore units, staged lint and `make test`.

### Task 35: Prepare exact whole-group deletion debt

**Files:** `internal/metaengine` approval/deletion/cleanup helpers and tests. **Target:** [T] §5.2, §6.2, §12 “Group completion and deletion”; accepted admission amendment. **Depends on:** 29–34. **Budget:** estimated 350–500 changed Go lines, ceiling 550.

- [x] Prepare first deletion debt from exact former hosted identities and immutable storage/member evidence. Required stops and deferred DeleteGroupState retain their approved tuples; never invent a native-removal receipt. Preserve inherited/actionless/consumed debt and permanently retire deleted group IDs at approval.
- [x] For omitted former readmission hosts, require an issued exact former-target usable or consumed batch. Missing issuance returns Unavailable naming node/former member; foreign proof refuses and closed/displaced unconsumed proof returns AuthorityFenced. Pending whole-batch consumption permits pending approval, with no actionless debt until completion. Restore requires the same retained coverage. Desired binding alone is insufficient; Task 37 creates original-attributed actionless debt only at completion.
- [x] Retain pending deletion boundaries even for an issued group that never converged. Its convergence remains unknown; immutable former assignments and exact debt retain ownership evidence. Completion still requires every former host's exact stop/readmission proof.
- [x] Test new/inherited deletion debt, mixed and all-readmission ownership, partial usable batch acceptance, missing/foreign/fenced proof refusal, exact storage/member attribution, never-converged issued ownership and source immutability. Wire this preparation into approval; complete graph/node/zero-step preparation follows in Task 37.

**Verification**

- [x] Run approval/deletion/debt units, staged lint and `make test`.

### Task 36: Complete deletion proof and protect readmission dependencies

**Files:** scoped deletion/approval/cleanup helpers and tests. **Target:** [T] §6.2, §12 “Group completion and deletion”; accepted amendment FI_DELETION_BATCH and peer thread 13. **Depends on:** 35. **Budget:** estimated 350–500 changed Go lines, ceiling 550.

- [x] Prepare deletion completion only after every exact former host is stopped or its entire readmission batch is consumed. Match the former identity even across later replacement members; prefer complete proof over older pending batches. Create missing original-admission-attributed actionless debts, preserve existing exact debts and install the tombstone with block success atomically on the owned candidate.
- [x] Zero-step deletion with pending batch consumption remains pending, with no completion milestone or actionless debt. An unissued never-converged group may complete immediately with no host debt. Reuse this completion preparation in later scoped transitions.
- [x] Refuse approval that cancels the usable unconsumed batch needed by retained deletion or a required-stop omission. Preserve same-member handover and consumed proof. Scoped revocation enforces the same protection in Task 38.
- [x] Test partial/full batch completion, missing/foreign proof, original storage/member/operation attribution, exact existing-debt reuse, later replacement proof, cancellation refusal and candidate/source immutability. Compose these preparations through scoped approval; full node/marker/history assembly remains Task 37.

**Verification**

- [x] Run focused deletion/readmission/approval/restore units, staged lint and `make test`.

### Correction 15: Make deletion refusal deterministic and close the live-admission invariant

**Source:** peer thread 14 findings 1 and 4, verified against c12eab9. **Budget:** estimated 180–350 changed Go lines including tests, ceiling 550. Complete before Task 37; remaining review findings receive separate bounded corrections.

- [x] Sort exact owner keys wherever deletion preparation can refuse. A hard error outranks pending Unavailable across all owners; no partial debt or milestone is published. Cancellation protection keeps its no-issued-batch semantics with deterministic selection.
- [x] Reject a live admission covered by a metadata checkpoint at/after its origin even if that checkpoint no longer contains the member; retain exact-member-only role initialization.
- [x] Test mixed pending/hard errors across repeated preparation runs, immutable source/candidate on refusal, stable transient selection, and missing-member live admissions.

**Verification**

- [x] Run focused deletion/admission units, staged lint, `make test`, and diff checks; record the result and commit separately.

### Task 37: Prepare atomic approvals and zero-step convergence

**Files:** `internal/metaengine` approval/deletion/convergence helpers and tests. **Target:** [T] §5.2, §6.2, §7.1, §12 “Approve”; accepted admission amendment. **Depends on:** 29–36 and Correction 15. **Batch:** complete approval preparation plus the remaining thread-14 corrections; no further task split.

- [x] Finish the approval candidate with prepared Target/history, operations/owners, zero-step group/node/marker completion and same-record admission/required-stop handover. Complete live admissions atomically with their metadata block, including zero-step completion; a covering checkpoint cannot coexist with a live admission. Prepare one non-fallible installation boundary after every validation; unchanged load blocks remain owned. Share approval validation with repair/voter/readmission as those handlers are implemented.
- [x] Use Tasks 35–36's prepared deletion debt, completion and permanent retirement IDs. Zero-step completion requires whole-batch consumption for every omitted exact former identity, including a later replacement member's batch. Create missing original-admission-attributed actionless debts atomically with the tombstone; reuse existing exact debts. Pending prerequisites keep the block pending. Approval binding alone cannot prove completion or create its own admission checkpoint. Refuse cancellation of an unconsumed batch on which retained deletion/required-stop omission depends.
- [x] Integrate scoped revocation preparation in Task 38 before enabling isolated FSM installation; until then the dispatcher returns Unsupported after complete candidate preparation. Never call global legacy revocation or publish partially prepared ownership. Live production activation remains Task 78 with complete persistence.
- [x] Test omitted/extra/duplicate scopes, unchanged admission-dependent owners, new/inherited deletion boundaries, zero-step deletion before/after exact preparation/retirement receipts, late CAS refusal and atomic candidate/refusal. Task 38 completes actual isolated install/refusal checks with revocation present.

- [x] Close thread-14 existing-debt omission gap: approval, cancellation protection and restore inspect the exact deletion block (including failed tip). An unstopped existing debt without a required Stop needs an issued usable exact readmission batch.
- [x] Implement thread-14 Stop rebinding table: required→required, required→detached when reuse/deletion ends, and detached→required. Use existing Approved/desired placement in planning; validate actual binding in the FSM. Preserve original debt/retirement/Removed attribution and consumed receipts; refuse duplicate detached stops and omitted required stops. Restore later-bound Cleanup actions by their exact bound operation.
- [x] Add composed planner/approval/restore regressions for the rebinding chain, pending/fenced/missing readmission with existing debt, and cancellation. General conflicting-grant settlement remains Task 38; bound-action cleanup authorization remains Task 43.

**Verification**

- [x] Run approval/placement/deletion/convergence/atomicity units, staged lint and `make test`.

### Task 38: Scope revocation and remove durable-debt capacity admission

**Files:** `internal/metaengine/authority_revocation.go`, `authority_catalog.go`, `cleanup_evidence.go`. **Target:** [T] §6.2, §6.4; contract C8. **Depends on:** 8–15, 34–37. **Budget:** ≤550.

- [x] Implement revokeAuthorityLocked(index, []blockRef, successor); key revocations by OperationID+GroupID. Close/inherit only affected resources and preserve unrelated and cleanup-class grants unless an actual resource conflict requires fencing. Preserve usable exact readmission retirement for the same granted replacement during admission handover; never revive fenced grants. Refuse cancellation/fencing of a batch still required by pending deletion. Fence issued founding/preparation resources before a deletion Stop so late founding cannot recreate data afterward.
- [x] Every unconsumed StopAction rebind (required→required, required→detached or detached→required) settles/closes the old conflicting host grant before successor issuance; late old receipts stay historical.
- [x] Remove pendingCost admission and global inherited-obligation/close-all behavior. Prepare the complete catalog update before installation; preserve exact floor/evidence validation and aggregate ReadAuthority/WaitRevoked semantics.
- [x] Finish isolated applyBeginOperation by composing Task 37's complete candidate with scoped revocations before one non-fallible installation. Test atomic install/refusal, exact approval replay and late CAS; live production activation remains Task 78.
- [x] Test A's pending revocation does not affect B, same-resource obligations cannot be skipped, cleanup grants survive unrelated approval and “not requested” differs from requested-and-complete.

**Verification**

- [x] Run authority revocation/restore units and `make test`.

### Task 39: Scope effect issuance, authorization and completion prerequisites

**Files:** `internal/metaengine/authority_issue.go`, `authorization.go`, `authority_receipts.go`, transfer/readmission authority helpers. **Target:** [T] §2, §6.4, §12 “authorizeLocked”. **Depends on:** 34–38. **Budget:** ≤550.

- [x] Authorize exact operation/attempt/version/step/action/group/host/replica and current block cursor/owner; use the operation's pinned document. Remove global Target.Version equality and all-revocations-complete requirements.
- [x] Check only required native/host floors and relevant unfinished prerequisites, including transfer/readmission paths. Remove remaining pending-record admission gates while retaining exact grant reuse and receiver identity checks. Mandatory load admission checks current approved bindings plus exact metadata convergence or consumed preparation; missing proof waits with Unavailable, including when no dependency edge exists.
- [x] Test old B operation remains authorized after A advances Target, stale tuple/floor fails, apply lag remains retryable and B can issue effects with A's pending fences. Catalogue all five target-listed gates as removed/scoped, not just the obvious issuance loop.

**Verification**

- [x] Run authorization/effect/transfer units and `make test`.

### Task 40: Validate block transitions and prepare group convergence

**Files:** `internal/metaengine/operator_state.go`, `progress.go`. **Target:** [T] §6.1, §7.2, §12 “completeGroupLocked”. **Depends on:** 34–39. **Budget:** ≤550.

- [x] Implement noncompletion applyTransitionOperation/validateScopedTransition and the validation/preparation portion of completeGroupLocked. Require exact required receipts and relevant barriers; aggregate full operation state independently of projected pages. Add inherited retry lookup when Task 44 installs Previous-chain behavior.
- [x] Record role-change cooldown under the exact consumed grant/action attempt, once; inherited retry consumption must not relabel the effect or reset an existing cooldown. Completion may first be recorded by a later attempt in the validated Previous chain. Preserve the original role tuple and use a completion application index after grant issuance.
- [x] Prepare the group's declaration from that operation's pinned approved version. Defer successful completion installation until Task 42 supplies the node/marker/debt preparations; do not publish a partially completed group in this intermediate task.
- [x] Test independent block cursors/failure, original declaration provenance after newer Target and stale tuple/final-validation refusal. Add the aggregate-precedence calculation/tests moved from Task 1: any runnable/waiting sibling keeps the operation nonterminal; once all terminate, failed dominates superseded, then succeeded. Task 42 adds successful atomic completion and duplicate-completion regressions once every prepared component exists.

- [x] A zero-step deletion awaiting whole-batch readmission consumption stays blocked with an exact reason; prepare a receipt-driven recheck; Task 42 installs its complete group/debt/node result atomically.

- [x] Use Task 37's rebound required StopAction for ordinary deletion completion, including formerly detached debt. Its exact receipt supplies Stopped; do not add a second parallel proof source or relabel an old receipt.

**Verification**

- [x] Run scoped transition/completion units and `make test`.

### Task 41: Advance node proof and historical convergence markers

**Files:** `internal/metaengine/progress.go`, deletion/admission helpers. **Target:** [T] §2, §3.3, §6.1–6.2, §12 “Group completion and deletion”. **Depends on:** 40. **Budget:** ≤550.

- [x] Implement advanceNodeProgressLocked and updateFullyConvergedLocked using current approved declarations, exact dependent proof and required deletion stops. Pin the marker's immutable document; no current-health or physical-collection claim.
- [x] Whole-group deletion produces a tombstone only after every former owner is stopped or fully covered by exact readmission proof. Keep retired group IDs permanent; no majority, timeout, newer ID or ForceRetire waiver.
- [x] Test deferred cleanup permits advancement, unreachable required stop prevents only affected completion, initial nil convergence is unknown and a superseded version may never get a marker.

**Verification**

- [x] Run node/deletion/marker units and `make test`.

### Task 42: Persist exact monotonic cleanup debt

**Files:** `internal/metaengine/cleanup.go`, `cleanup_evidence.go`, effect consumption. **Target:** [T] §4.1, §6.2, §12 “upsertCleanupLocked”; contract C5. **Depends on:** 40–41. **Budget:** ≤550.

- [x] Implement upsertCleanupLocked/detachCleanupLocked and bounded CleanupPage. Native removal creates debt in the same Apply before progress publication; whole deletion uses its former assignments without fabricating RemovedAt. Finish completeGroupLocked by atomically installing validated group/debt/node/marker drafts from Tasks 40–41.
- [x] Keep readmission-created debt actionless and unchanged when its old native identity is subsequently removed; retain that exact removal receipt as execution evidence. Verify exact readmission target coverage for every omitted host effect. Preserve historical MemberID and original removal-origin receipts.
- [x] Preserve immutable original Storage and compatible stop/retire/readmission proof, monotonic milestones and exact historical MemberID. Repeated removal cannot overwrite collected debt or resurrect it. StorageMemory completes at Retired with nil CollectedAt.
- [x] Test conflicting proof/storage, repeat consumed effects, deletion attribution and complete application-retirement/quarantine/ledger prerequisites. Add A-failed/B-completed, duplicate completion and no partial mutation regressions; keep opaque tokens out of ordinary progress.

**Verification**

- [x] Run cleanup/memory/readmission units and `make test`.

### Task 43: Authorize detached cleanup and changed deferral status

**Files:** `internal/metaengine/cleanup.go`, authority issue/resource helpers, `internal/metaservice/cleanup.go`. **Target:** [T] §6.2–6.3, §7.1–7.2. **Depends on:** 39, 42. **Budget:** ≤550.

- [x] Implement BeginCleanupEffect and RecordCleanupStatus handlers. Derive execution/cleanup Class from exact retained debt; permit only the approved deferred stop/retirement action after operation termination.
- [x] When permitting multiple cleanup grant revisions for one action, replace authorityCatalog's current duplicate-action rejection and adjust reverse milestone checks together: one retained exact receipt supplies a milestone; compatible late receipts from other fenced/reconciled revisions remain historical evidence. Test late old/new revision receipt order without rewriting consumed milestones.
- [x] Reuse a matching grant; reconcile a newer same-resource floor before reissuing a revision. Never revive native/preparation authority or affect successor ownership. Authenticate original host/readmission proof for NotBefore/blocker changes.
- [x] Test terminal-operation cleanup, forged cleanup classification, same-host conflict, repeated polling causing no durable writes and preserved milestones on deferral/failure updates.

- [x] Match BeginCleanupEffect.Action against the complete stored action tuple; a reused StepSeq from another operation is not the expected action. Preserve CleanupStatus historical MemberID/RetirementKey through callbacks and authenticated routing.

- [x] Resolve cleanup Stop authorization from the debt's current StopAction and its exact bound approval, which may differ from debt.OperationID after required→detached handover. Retirement and Removed keep original attribution.

**Verification**

- [x] Run cleanup authority/status units and `make test`.

### Task 44: Implement exact failed-attempt retry

**Files:** `internal/metaengine/operator_state.go`, retry/receipt lookup helpers. **Target:** [T] §3.4, §6.1, §12 “Retry derives state”. **Depends on:** 38–43. **Budget:** ≤550.

- [x] Derive the new attempt from Previous inside the FSM; preserve the entire immutable Plan byte-for-byte. Reopen only failed blocks at the first unconsumed required step; terminal siblings retain their state/proof and superseded scopes are never reclaimed.
- [x] Require terminal failed aggregate, unchanged approved failed scopes and executable suffix. Prepare scoped predecessor revocations before assigning retry owners; retain original receipt tuples through Previous rather than relabeling them.
- [x] Rebind live admission progress to the exact retried `_meta` attempt, preserving the origin operator/instance and batch evidence needed by ReadmissionID in the same process-keyed record. Reject caller-selected origins and validate persistence/join/restore provenance. Derive readiness from complete same-node/member retirement; learner/promoted progress still requires exact native effects through Previous. Rebind only unconsumed required StopAction in the same atomic transition. Test retry before/after preparation, AddLearner and promotion, including lost responses and process replacement.
- [x] Test duplicate retry returns the same immediate successor even after its failure, concurrent retry deduplication, lost-response replay, missing history, unsafe suffix and failed-block-with-running-sibling refusal.

**Verification**

- [x] Run plan retry/authority consumption units and `make test`.

### Task 45: Implement deterministic semantic rebase preparation

**Files:** new `internal/metaengine/rebase.go`, existing repair document helpers. **Target:** [T] §5.3, §12 “Replicated repair and rebase”. **Depends on:** 25–34. **Budget:** ≤550.

- [x] Implement rebaseStaged(base, staged, repair) with semantic read/write conflict checks. Preserve staged caller attribution/audit, exact requested identities and all intended edits while applying a disjoint repair.
- [x] Prepare R at Latest+1 with parent old Target; use the atomic installer to retain S.RebasedFrom (ordinary commitLatestLocked clears it); when staged delta exists prepare S at Latest+2, parent R and RebasedFrom old Latest. Validate both before installation, preserve monotonic floors and deterministic supplied timestamps.
- [x] After an actual rebase, verify both a typed mutation and raw read-modify-Stage succeed with RebasedFrom cleared; retain original S provenance unchanged.
- [x] Test overlapping placement/eligibility changes, identity conflicts, multi-edit staging, no-staging case, version exhaustion and no accidental approval of the preserved edit.

**Verification**

- [x] Run rebase/repair document units and `make test`.

### Task 46: Commit repair and voter changes with atomic rebase

**Files:** `internal/metaengine/repair_proposals.go`, `voter_change.go`, `rebase.go`. **Target:** [T] §5.2–5.3, §7.1, §8.2. **Depends on:** 34–45. **Budget:** ≤550.

- [x] Implement applyCommitRepairOperation and route CommitVoterChange through the same atomic preparation/install path after existing voter guards. Check captured Latest/Target/source/owner/native inputs in the FSM.
- [x] Install Target=R, Latest=S (or R without staged edits), immutable operation, histories, provenance/indexes and owners as one change. Reject user-owned scope preemption, invalidated-scope omissions and overlap without partial writes.
- [x] Test R/S visibility at one applied index, duplicate/conflicting proposals, retained staged attribution, failover snapshot/replay and watcher delivery only after the complete transaction.

- [x] A rebased successor does not duplicate the original typed intent row. Replay follows RebasedFrom to the one original (expected-parent, kind, digest) acceptance record.

**Verification**

- [x] Run repair/voter/CAS units and `make test`.

### Task 47: Normalize typed intent without allocating on replay

**Files:** root mutation helpers, `internal/metaservice/mutations.go`, document mutation helpers. **Target:** [T] §3.4, §12 “Stage / typed mutation replay”. **Depends on:** 4, 7. **Budget:** ≤550.

- [x] Reuse metaengine knownIntentKind through the internal construction boundary; implement validated canonical intent fingerprints: include Actor/Comment/Origin/audit and requested semantic identities, normalize set order, preserve meaningful sequence order, exclude generated version/time/allocation fields.
- [x] Capture zero ExpectedVersion once per call; for nonzero stale CAS look for retained exact intent before allocating IDs. Raw Stage uses canonical full accepted document content, not a generic request key.
- [x] Test equivalent ordering, distinct audit purpose, explicit identity mismatches, unknown history and response loss. Never recompute a write with a fresh expected version or claim an arbitrary replay expiry.

- [x] Supply an internal cross-package constructor or exported value for intent provenance; metaservice cannot construct metaengine.intentRecord directly. Do not add an ordinary application request registry.

**Verification**

- [x] Run pure intent/canonicalization units and `make test`.

### Task 48: Persist accepted intent and first-approval provenance

**Files:** `internal/metaengine` document apply/index code, `internal/metaservice/documents.go`, `planning.go`. **Target:** [T] §3.4, §4.1, §7.1, §12 “Approve”. **Depends on:** 15, 44–47. **Budget:** ≤550.

- [x] Store one intent record per accepted typed version; FSM validates known kind/digest encoding, parent CAS, document validity, allocation floor and replacement diff tuple without duplicating typed mutation logic.
- [x] Approve(version) returns its first recorded attempt regardless of terminal state; RetryOperation is the only retry path. Rebased unapproved versions return structured RebasedTo; missing retained proof returns conflict/HistoryUnavailable without writing. Missing original approval means the whole atomically pruned attempt chain is absent.
- [x] Test optional approval failure preserves Document/RebasedTo, identical replay allocates nothing, first approval versus latest attempt, pruning and forwarding of structured results.

- [x] Original approval lookup never falls back to a later retained retry; absent original evidence returns HistoryUnavailable.

**Verification**

- [x] Run document/provenance units and focused service mutation functional checks; run `make test`.

### Task 49: Require exact predecessors for readmission and reseating

**Files:** `cluster_mutations.go`, `internal/metaservice/mutations.go`, `internal/metaengine/admission*.go`, repair helpers. **Target:** [T] §3.1, §5.3, §7.1; contract C1. **Depends on:** 44–48. **Budget:** ≤550.

- [x] Change ReadmitNode(ctx, name, expectedMemberID, opts) and ReseatReplica(ctx, group, ReplicaKey, opts); update immediate callsites together. Require nonzero exact predecessor and retained replacement evidence before returning an already-accepted result.
- [x] Route readmission/reseat approval through the shared new-approval validation and atomic preparation, including rejection of duplicate consumed Remove, cleanup-owned Cleanup/Stop and DeleteGroupState without uncovered Hosted ownership.
- [x] Allocate fresh monotonic IDs only after replicated CAS/identity checks. Explicit reseat uses narrow repair/rebase with OriginOperator and refuses an owned group naming its operation; readmission keeps exact grant/fence/join lineage and scoped admission prerequisites.
- [x] Test repeated lost responses, arbitrary successor conflict, busy scopes, disjoint staged intent, readmission on a reused node name and no old global Latest=Target=Active admission requirement.

- [x] Scoped issuance uses the exact current approved member and its metadata owner/handover chain, never whole-Target version equality. Verify an unrelated approval after readmission approval still permits issuance, then dependent deletion approval.

**Verification**

- [x] Run replacement units plus focused readmission/reseat functional cases; run `make test`.

### Task 50: Base founding readiness on the exact meta block

**Files:** `internal/metaservice/bootstrap.go`, `join_admission.go`, `internal/metaengine/admission.go`. **Target:** [T] §5.4, §8.4, §10. **Depends on:** 40–41, 49. **Budget:** ≤550.

- [x] Build founding plans from empty proven PlanInput and observed seed membership. Replace whole-operation/Active/FullyConverged readiness checks with exact founding `_meta` convergence and native admission evidence.
- [x] Preserve member, instance, seed and current-term admission checks; retain founding proof until durable admission records make it unnecessary. Failing founding load work must not prevent metadata readiness.
- [x] Add single-participant/protocol-double regressions for a failed load block beside successful founding `_meta`, rejected wrong seed/member and retained restart without refounding.

**Verification**

- [x] Run focused bootstrap/admission functional tests and `make test`.

### Task 51: Fix non-founding admission and state-based barriers

**Files:** `internal/metaengine/admission.go`, `progress.go`, scope/barrier validation. **Target:** [T] §3.3, §5.2/§5.4, §12 “Admission and evacuation barriers”. **Depends on:** 28–41, 49–50. **Budget:** ≤550.

- [x] Bind joining identities to the current nonterminal `_meta` block and its own pinned plan, not global Target.Version. Preserve Target identity consistency and exact learner-first member/instance checks after unrelated approvals/rebases.
- [x] Share barrierSatisfied semantics among dispatch, grant issuance and execution revalidation. Load preparation and founding voter hosts accept exact metadata convergence or consumed preparation from any valid approved operation, with current binding and full readmission checks. Do not gate evacuation/stop/cleanup. Validate resumed admission prefixes rather than demanding repeated Prepare/Add in an already-native successor. Removal scans current and retained approved/proven/intermediate assignments, preparations, native-removal proof and whole-group required stops; nil initial convergence is unknown.
- [x] Test metadata supersession leaves unchanged load blocks waiting/runnable under their original owner, cross-operation dependencies, founding proof, same-process handover/retry, wrong origins and Ready-only refusal. Test load-block supersession does not itself invalidate removal, newly relevant evacuation scopes are checked, detached cleanup alone does not block and same-node readmission replacement uses its own proof path.

- [x] Scoped issuance uses the exact current approved member and its metadata owner/handover chain, never whole-Target version equality. Verify an unrelated approval after readmission approval still permits issuance, then dependent deletion approval.

**Verification**

- [x] Run barrier/admission units and focused join functional checks; run `make test`.

### Task 52: Retain proof closure and prune without resurrecting debt

**Files:** `internal/metaengine/cleanup_evidence.go`, `cleanup.go`, history/snapshot helpers. **Target:** [T] §4.2–4.3, §12 “Collection acceptance versus settlement”. **Depends on:** 42–51. **Budget:** ≤550.

- [x] Implement one identityReferencedLocked predicate covering Latest/Target/history, convergence, every retained attempt/Previous chain, planning/deletion catalogs, admission/readmission and every retained grant including settled grants.
- [x] Derive independently required proof closure before pruning; avoid a cleanup/grant self-retention cycle or map-order dependence. Pin marker/current docs, each debt origin and complete retry chain/block sources, and any distinct readmission admission operation and full batch; compact only after minimal retained evidence suffices for every verification path. Pin consumed admission preparation/provenance for approved members lacking equivalent meta convergence even with no current dependent. Release that barrier root after equivalent convergence, or abandonment without retained load/plan references; other roots still apply.
- [x] Account for the full pinned operation: each retained required completion keeps its exact consumed grants/receipts through Previous, including unrelated successful blocks and their source documents. A single unresolved cleanup can pin all of these; retention estimates and growth metrics must count this closure, without imposing an approval cap.
- [x] Test failed-attempt references prevent pruning, complete orphaned records can prune, repeated removal cannot recreate debt, settled grants remain references when otherwise required and soft history caps do not reject unrelated approvals.

- [x] Retain every never-converged founding grant (any outcome) until convergence or completed deletion with a permanent retired-group marker; pruning must not erase the planner's FoundingIssued evidence and authorize refounding.

- [x] Completed-debt pruning must remain restorable when another proof root still pins a declaration naming the former owner. Prune that ownership context together or retain equivalent completion evidence; do not let tombstone validation rediscover an owner after dropping its only proof.

- [x] Pending deletion pins exact former assignments (including issued never-converged founding/preparation), usable readmission batches and their original admission/operation chains through consumption and cleanup completion.

**Verification**

- [x] Run retention/pruning/snapshot units with varied insertion order and `make test`.

### Task 53: Accept collection receipts and expose no-write settlement checks

**Files:** `internal/metaengine/cleanup.go`, `reads.go`, proposal dispatch. **Target:** [T] §4.2, §6.3, §7.1, §12 settlement algorithms. **Depends on:** 6–15, 42–43, 52. **Budget:** ≤550.

- [x] Implement CollectionCheck under the FSM read lock: Untracked requires no cleanup row and no reference under the shared predicate. Do not add CollectionAcks or create a timestamp, grant, state mutation or Raft proposal on this path.
- [x] Implement applyRecordCollection for retained debt: exact identity/member/token/sequence/retirement/courier linkage, class and monotonic proof; identical duplicates echo accepted evidence, conflicting duplicates fail.
- [x] Test memory records reject collection, unknown identities cannot fabricate milestones, missing-row acceptance refuses, Previous/readmission references prevent settlement and deterministic Apply uses proposal time only.

**Verification**

- [x] Run collection acceptance/read units and `make test`.

### Task 54: Persist the host collection journal state machine

**Files:** `internal/authoritystore/collection.go`, `store.go`, functional tests. **Target:** [T] §4.2, §7.2; contracts C6/C11. **Depends on:** 6. **Budget:** ≤550.

- [x] Implement PrepareCollection, CompleteCollection, RemoveCollection and paged Collections with bbolt transactions, exact-row CAS and durable once-assigned bucket sequence. Prepared precedes Collect; collected rows retain original proof until release.
- [x] Activate complete journal schema 2 and collection bucket validation. Preserve identity/fence/effect rules, reject schema 1 without replacement, and preserve historical MemberID/grant/token on journal rebind.
- [x] Test reopen at both states, duplicate preparation/completion, conflicts, corrupt rows, pagination and removal requiring exact collected receipt. Do not drop a row on reporting failure.

**Verification**

- [x] Run authoritystore functional tests, targeted race checks for store access, and `make test`.

### Task 55: Implement exact walstore collection markers

**Files:** `storage/walstore/lifecycle.go` and narrowly scoped marker helpers. **Target:** [T] §4.2, §12 “Exact collection marker and partial deletion”. **Depends on:** 6. **Budget:** ≤550.

- [x] Implement sibling `<token>.collection` JSON with schema_version=1 and the unchanged full RetiredReplica encoding. Validate confinement, identity/time equality, regular-file/no-symlink state, open store and absence of a live owner.
- [x] Persist and sync the marker and parent before destructive work. Retired enumeration ignores markers. Do not let missing directories or missing/swapped roots create deletion proof.
- [x] Test malformed/traversal tokens, mismatched/corrupt marker, wrong identity/time, symlinks and marker persistence failure preserving retired data. Adapt TiKV ordering only; retain gquorum's stricter proof semantics.

**Verification**

- [x] Run focused walstore functional marker tests and `make test`.

### Task 56: Complete crash-safe Collect and ForgetCollection

**Files:** `storage/walstore/lifecycle.go`, `storage/retirement.go`, `storage/storagetest`, custom/test stores. **Target:** [T] §4.2, §12 collection algorithms; contract C6. **Depends on:** 55. **Budget:** ≤550.

- [x] Delete contents with the exact pin retained, sync the directory, remove the pin, remove the now-empty directory nonrecursively and sync the root. Recover a missing pin only with an exact marker plus empty directory; absent data without marker returns NotFound.
- [x] Implement ForgetCollection as exact marker-only release after retired data is absent; absent marker is repeatable and root sync remains required. Activate the strengthened interface and update actual implementations/conformance helpers together.
- [x] Test every deletion boundary, retry after failure, foreign/nonempty unpinned directory refusal and successor survival. Memory storage has no durable physical obligation; do not implement fake persistent proof from an empty retired list.

**Verification**

- [x] Run storage conformance and walstore functional/race regressions; run `make test`.

### Task 57: Prepare and resume host-owned collection attempts

**Files:** `internal/managed/retirement.go`, host configuration/gates. **Target:** [T] §6.3, §7.2, §12 “Host collection and reporting”. **Depends on:** 42–43, 54–56. **Budget:** ≤550.

- [x] Update collectRetiredReplica to verify exact retirement/ledger/assignment/owner, run the existing serialized retention decision, persist prepared, acquire the existing host/group gate, revalidate and Collect before durable CompleteCollection.
- [x] Implement resumeCollections from journal pages as well as Retired enumeration. Prepared resumes with its saved token even when enumeration disappeared; collected resumes only reporting and can never call Collect again.
- [x] Test failed callback/Collect/persist boundaries, deferred retention status, later policy changes not undoing a prepared attempt and owned work remaining joined on waiter cancellation.

**Verification**

- [x] Run managed collection functional/race tests and `make test`.

### Task 58: Report collected receipts and release proof in order

**Files:** `internal/managed/retirement.go`, existing host lifetime/configuration. **Target:** [T] §6.3, §8.4, §12 “reportCollections”; contract C11. **Depends on:** 54–57. **Budget:** ≤550.

- [x] Implement bounded reportCollections using durable collected rows and injected Report/Status callbacks. Validate exact echo and accepted/settled result before proof release; defer both removals while local readmission references remain.
- [x] Call owningStore.ForgetCollection before exact journal removal. Reporting loss/outage retains durable evidence; do not repeat deletion after marker release. Restrict wakeups to bounded hints, never use a volatile queue as the source of truth.
- [x] Test lost reply, duplicate report, wrong echo, Forget failure and crash after Forget before journal removal. Cancel network waits on stop while retaining unsent rows for restart/courier.

**Verification**

- [x] Run managed reporting functional/race tests and `make test`.

### Task 59: Route restricted collection reporting through MetaControl

**Files:** new `internal/metaservice/cleanup.go`, `rpc.go`, query/forwarding helpers. **Target:** [T] §6.3, §7.3, §12 “ReportCollection”. **Depends on:** 43, 53, 58. **Budget:** ≤550.

- [x] Add report_collection/report_cleanup_status payloads to existing MetaControl. Bind Courier to authenticated process/header, validate cluster/receipt shape and perform safe collection_check before retained proof acceptance.
- [x] For untracked identities return settled with the safe read's index without proposing. Otherwise require full historical host/readmission-courier proof and propose RecordCollection. Recheck only missing-evidence pruning races, including courier lookup; never suppress authentication or conflicting-receipt failures.
- [x] Test excluded historical reporter, valid/invalid successor courier, restricted no-op after pruning, no general ProposeMeta bypass, no Raft append on settled and index/echo preservation through leader forwarding.

**Verification**

- [x] Run metaservice protocol functional tests and `make test`; if a genuine envelope limitation requires .proto changes, use pinned generation and `make proto-check`.

### Task 60: Implement bounded coherent metadata progress pages

**Files:** `internal/metaengine/progress.go`, `reads.go`, topology cursor helpers if needed. **Target:** [T] §3.2, §7.1–7.2, §12 “Progress read”; contract C2. **Depends on:** 15, 40–42, 52–53. **Budget:** ≤550.

- [x] Implement Machine.Progress/progressLocked with one FSM read lock and AppliedIndex. Page the union group/node/operation-block/cleanup in target order; total row/block budget, not one independent limit per array.
- [x] Use versioned base64url JSON cursors bound to filter hashes. AND filters over actual scope, retain deleted-group evidence, repeat operation headers with TotalBlocks/BlocksTruncated while aggregate state covers all blocks.
- [x] Test a huge single operation, limits 1/100/1000, malformed/mismatched cursors, empty results, concurrent changes between pages, ownership copies and insertion behind cursor. Do not promise a frozen export or add server cursor state.

**Verification**

- [x] Run paging/progress units and `make test`.

### Task 61: Expose progress reads and bounded local diagnosis

**Files:** `internal/metaservice/progress.go`, `documents.go`, root `cluster_progress.go`/`cluster_reads.go`. **Target:** [T] §3.1–3.2, §8.3; contracts C1/C2. **Depends on:** 60. **Budget:** ≤550.

- [x] Implement ReadProgress through the existing quorum/apply query path, then decorate only returned rows from matching cached observations. Preserve reporter, received time and per-group source; nil means unknown. Never fan out for this API.
- [x] Prepare the final ReadTopology pair/marker and bounded Status operation headers (100), explicit truncation and CleanupSummary. Convert ClusterStatus.Groups to existing GroupProgress values, keeping raw GroupStatus only in Observation; adapt affected consumers together. Use the same complete declaration row builder and exact observation attachment rule for Status and ReadProgress; nil must never mean an omitted projection. Status and operation lookup remain local diagnosis without a quorum claim.
- [x] Keep ReadView.Active/Views/legacy topology query until Task78 (thread25 consensus); legacy rows project exact Latest/Target/Active, with no inferred scoped owner/Deleting/Blocked. Fold thread24 collection fix into this task: accept a later consumed readmission retirement revision for already-retired debt, preserving its first milestone and validating exact retained batch proof.
- [x] Test quorum loss/read cancellation, metadata-page coherence, stale successor samples, preserved timestamps, bounded projection and no remote read calls. Keep full lookup separate from page-limited projection.

**Verification**

- [x] Run service read/progress functional tests and `make test`.

### Task 62: Replace single-current-operation controller ports

**Files:** `internal/operator/coordinator_contracts.go`, `controller.go`, metaservice coordinator adapter. **Target:** [T] §6.1, §7.2; contract C11. **Depends on:** 40–44, 51, 60. **Budget:** ≤550.

- [x] Implement Coordinator{IsMetaLeader, ReadyBlocks, RecordOperationTransition}, WorkItem{Operation, GroupID}, and ScopedValidator. Retain GroupEffects/MembershipCoordinator and internal package name.
- [x] Preserve one Controller with an exactly-one-coordinator constructor guard (thread26). Static live composition uses LegacyCoordinator until78; scope-only tests cannot supply legacy Store/validators. Final scoped ports/transition handling land now; bounded dispatch/effect execution is63–65 and service validators66. Remove legacy fields/adapters78.
- [x] Select bounded ready blocks from durable state; include only exact current owners with viable next required work. Replace whole-document plan/final validation with exact-scope validation.
- [x] Convert controller record/transition naming and tests in this component. Test selection with running/waiting/failed siblings, leadership changes and incomplete dependencies without holding a durable work slot indefinitely.

**Verification**

- [x] Run controller selection units and relevant protocol functional checks; run `make test`.

### Task 63: Dispatch fair bounded group work with joined shutdown

**Files:** `internal/operator/controller.go`, dispatch helpers/tests; exact GroupEffects adapters in managed/metaservice and node_load.go. **Target:** [T] §6.1, §6.4, §12 “Controller dispatch”. **Depends on:** 5, 39–41, 51, 62. **Budget:** ≤550.

- [x] Implement round-robin ready selection, MaxConcurrentGroups, exact host inflight (AuthorityKey, resource, AuthorityTarget), native inflight (AuthorityKey, group resource) and shared host/group admission. Counts cap actual ready work; never preallocate pools, channels or per-slot state from the configured count. One worker executes one bounded check/effect; release RPC slots on timeout without releasing remote ownership.
- [x] Retain bounded per-host/group backoff, skip busy work and prune idle entries with settled work. Revalidate full action/owner/resource tuple after waits; retain late evidence without advancing superseded work.
- [x] Close dispatch admission under the dispatch mutex before joining workers; eliminate WaitGroup.Add versus zero-count Wait races. Test one hung target with healthy progress, leadership loss and concurrent Stop/new scheduling under race detection.

**Verification**

- [x] Run controller dispatch/shutdown functional race regressions and `make test`.

### Task 64: Dispatch cleanup with a separate fair request budget

**Files:** `internal/operator` cleanup dispatch, metaservice cleanup adapters, existing whole-group dispatch. **Target:** [T] §6.2, §6.4, §9 TiKV lifecycle/worker adaptations. **Depends on:** 43, 59, 63. **Budget:** ≤550.

- [x] Discover durable cleanup in bounded pages; use MaxConcurrentCleanup with the same host/group gate registry and bounded backoff as ordinary work. Allocate only for actual admitted work, not MaxConcurrentCleanup slots. Remove the scoped controller's global fence-error short circuit; legacy composition remains until78.
- [x] Dispatch/record each exact whole-group target independently; one unreachable former host cannot suppress another receipt. Retain required deletion stops in execution rather than disguising them as optional cleanup. Per thread28, completed permanent deletion settles only native group-resource obligations across indexes, queries, retention and restore; preserve host fences and never fabricate native evidence.
- [x] Test ordinary execution survives failed cleanup, shared-resource conflicts remain serialized, repeated grants are reused and fairness resumes on returning hosts. Reference TiKV's retry lifecycle while preserving errors/proof rather than consuming failed deletion.

**Verification**

- [x] Run dispatch/cleanup functional race checks and `make test`.

### Task 65: Reuse exact host/native state under a new attempt's grant

**Files:** `internal/managed/granted_actions.go`, `replica.go`, `retirement.go`, `internal/raftengine/authority_effects.go`, `internal/operator` scoped effect execution. **Target:** [T] §6.1, §6.4, §12 retry/dispatch algorithms. **Depends on:** 44, 63–64. **Budget:** ≤550.

- [x] Complete the scoped worker effect path prepared62–64: exact global sequence within its block, retained approved declaration, before/after-wait authorization, native verification and whole-group completion; reuse existing effect/receipt and membership helpers. Service validators are connected66; complete live composition stays78.
- [x] Reuse actionWorks, acceptGuardedStart, stopRetiringIdentity and beginLocalEffect. After required old-floor fencing, exact already-ready/prepared/joined state may satisfy a new grant with freshly persisted exact evidence.
- [x] Never relabel old receipts, skip predecessor fences because native state looks satisfied or overlap unresolved factory/Close ownership. Preserve native live-quorum and leadership predicates when an effect is already observed.
- [x] Test retry after response loss, same-identity prepare/stop, late factory/callback return, stale floor and missing retained storage parking. Existing grants/receipts remain attributed to their original action tuples.

**Verification**

- [x] Run managed/native authority functional race regressions and `make test`.

### Task 66: Connect scoped planning, approval and explicit retry services

**Files:** `internal/metaservice/planning.go`, `progress.go`, `documents.go`, ApplyVersion handling. **Target:** [T] §3.1/§3.4, §7.2–7.3, §12 approval algorithms. **Depends on:** 44–53, 61–65. **Budget:** ≤550.

- [x] Build coherent PlanInput from approved/proven/intermediate exact scope plus fresh native evidence; capture ScopeExpectations. Use new begin/transition/retry proposals and scoped validators in existing service/controller configuration.
- [x] Expose Operation/WaitOperation/RetryOperation semantics and ApplyVersion discriminator; waits return full aggregate terminal status, explicit retry never replans or chooses a new intent. Remove activateCompleted when its last caller is converted.
- [x] Test remote forwarding, local wait cancellation, old attempt lookup, lost approval/retry replies and no hidden global convergence gate. Record remaining legacy service consumers for Task 73/70. Legacy PlanFor/Apply/WaitOperator and activateCompleted remain isolated until bootstrap73 and controller/persistence78 finish conversion; scoped config/query adapters remain fail-closed until78.

**Verification**

- [x] Run planning/approval/retry functional cases and `make test`.

### Task 67: Make convenience mutations and transfers scope-aware

**Files:** `cluster_ensure.go`, `cluster_mutations.go`, `internal/metaservice/mutations.go`, transfer authority. **Target:** [T] §3.1, §5.3–5.4, §8.1–8.2. **Depends on:** 47–49, 61, 66. **Budget:** ≤550.

- [x] EnsureGroup waits on its requested group's convergence, follows only its own retained rebased provenance and preserves foreign/overlapping staging conflicts. A blocked unrelated group cannot hold its result hostage.
- [x] TransferLeader captures exact group source/owner/target and fresh native leader predicate; unrelated Target changes do not restart it. Revalidate after waits and retain one-shot transfer authority/cancellation semantics.
- [x] Exercise remaining typed mutations/rollback through strict CAS/provenance; rollback cannot reuse retired group or replica IDs. Test optional approval partial results, changed audit intent, local holds and replacement conflict details.

**Verification**

- [x] Run root convenience/transfer functional regressions and `make test`.

### Task 68: Convert built-in repair policy to scoped progress

**Files:** `scheduling/scheduler.go`, `default_repair*.go`, voter policy helpers, `scheduling/schedulingtest`. **Target:** [T] §5.4, §6.4; contract C7. **Depends on:** 5, 46, 61. **Budget:** ≤550.

- [x] Finish final ScheduleInput and remove Latest/Active policy fields after caller adaptation. Built-in policy evaluates approved/proven declarations and rotates eligible groups; a blocked/busy first candidate must not monopolize ticks.
- [x] Preserve voter configuration, pinned learners, healthy seats, domain rules and legacy missing-voter repair for groups without voter configuration. Keep callbacks serialized and clone all maps/slices. Supply observations only in Status; clear Groups[g].Observation as well as Desired at the callback boundary, preserving faithful Clone semantics.
- [x] Test A blocked with B eligible, unchanged B cooldown/history, custom-policy busy-group refusal and no approval of unrelated staged intent. Do not add healthy balancing, priorities or public timing knobs.

**Verification**

- [x] Run policy/conformance units and `make test`.

### Task 69: Revalidate scheduler decisions with scoped CAS

**Files:** `internal/scheduling/driver.go`, `internal/metaservice/scheduling.go`, voter-change adapters. **Target:** [T] §5.3–5.4, §7.1–7.2. **Depends on:** 46, 51, 68. **Budget:** ≤550.

- [ ] Capture Latest/Target plus group SourceVersion/owner and exact repair inputs, recheck health/native membership/leadership after callbacks, then commit the atomic repair/rebase proposal.
- [ ] Remove global equality and global busy-operation gates. A scheduler may recompute after CAS conflict; ordinary caller writes may not. Surface scoped staged_conflict/busy causes without writing unchanged blockers every tick.
- [ ] Test callback serialization/cancellation, decision invalidation only for relevant changes, no mutation from stale native evidence and independent candidate rotation after refusal.

**Verification**

- [ ] Run scheduling/service functional race checks and `make test`.

### Task 70: Preserve observation history across unrelated topology changes

**Files:** `metrics/observation.go`, `internal/observe/metrics*.go`, trackers, status caches. **Target:** [T] §3.2, §5.4, §8.3; contract C7. **Depends on:** 3, 61, 68–69. **Budget:** ≤550.

- [ ] Remove ClusterSnapshot.ActiveVersion; use TargetVersion for context and per-group SourceVersion plus exact assignment for observation validity and collector round keys.
- [ ] Invalidate only changed group evidence, preserve instance/series/sample sequence/received time and validity reasons. Leadership loss still invalidates the whole collector round.
- [ ] Test A changes while B retains failure duration/catch-up samples, stale predecessor reports cannot describe successors, unknown leader names retain numeric IDs and safe progress decoration cannot relabel cached freshness.

**Verification**

- [ ] Run metrics/observe units and applicable functional race checks; run `make test`.

### Task 71: Recover managed replicas from exact scoped evidence

**Files:** `internal/managed/boot.go`, recovery/readmission/ownership helpers, root recovery adapters. **Target:** [T] §5.4, §8.4, §12 host algorithms. **Depends on:** 49–59, 65–66. **Budget:** ≤550.

- [ ] Replace Active ownership/admission/retention lookups with approved assignments, convergence, exact ledgers/grants and cleanup evidence. Verify pins and restored state before acquisition/publication; no global marker wait.
- [ ] Preserve one startup reservation, replay readiness, exact holds and joined late factory/Close behavior. Readmitted owners join/fence predecessor resources before replacement; parked missing/foreign state keeps evidence.
- [ ] Test healthy group recovery beside blocked debt, exact hold surviving recovery, successor protected from stale cleanup and retained startup without data refounding. Do not weaken load snapshot/tail replay contracts.

**Verification**

- [ ] Run managed boot/recovery/readmission functional race cases and `make test`.

### Task 72: Wire collection and scoped controllers into Node lifetime

**Files:** `node*.go`, metaservice/managed configuration and shutdown. **Target:** [T] §7.2, §8.1/§8.4. **Depends on:** 57–59, 63–71. **Budget:** ≤550.

- [ ] Supply the existing authority Store and concrete Report/Status callbacks to the existing managed Host; start/resume/report within Node's accepted lifetime. NewNode remains validation/copy only, with no I/O or callbacks.
- [ ] Control-only excluded-host startup may inspect/report/perform already-authorized cleanup without starting Raft or application replicas. Preserve historical courier proof and no general membership admission.
- [ ] Stop closes dispatch admission, cancels RPC/report waits and joins workers/owners/replay/callbacks before owned stores/journal close. Preserve borrowed-resource ownership and retryable Close ownership.

**Verification**

- [ ] Run Node startup/control-only/shutdown functional race tests and `make test`.

### Task 73: Convert routing, drain status, names and watch consumers

**Files:** `internal/metaservice/documents.go`, bootstrap/session/routing helpers, placement/status/watch, root status adapters. **Target:** [T] §3.2, §5.4, §8.2–8.4. **Depends on:** 50–51, 61, 66, 70–72. **Budget:** ≤550.

- [ ] Finish former Active reads for names/routing using exact proven/approved identities and valid intermediate grants; never staged names or bare name fallback. Drain/removal uses affected prerequisites and reports remaining old-host debt separately.
- [ ] Label placement results approved/proven explicitly; static placement is not native availability. Watch Latest and Target changes only after atomic rebase commit; ReadTopology exposes a coherent pair.
- [ ] Audit sessions/activation/readmission readers beyond the explicitly listed admission lines. Test unknown IDs/names, reused names, coherent rebase watch, deletion and delayed host cleanup. Split this consumer migration before exceeding the budget.

**Verification**

- [ ] Run status/routing/watch functional regressions and `make test`.

### Task 74: Connect the final Cluster methods and small cluster demo

**Files:** `cluster.go`, `cluster_documents.go`, `cluster_reads.go`, root public API tests, `examples/cluster`. **Target:** [T] §3.1–3.4, §7.2; contracts C1–C4. **Depends on:** 61, 66–67, 72–73. **Budget:** ≤550.

- [ ] Connect ReadProgress, Operation, WaitOperation, RetryOperation and exact replacement signatures on Cluster. Finish root caller conversion; leave only explicitly inventoried old symbols needed by later caller tasks, with mandatory removal in Task 78.
- [ ] Keep unchanged Cluster methods and Node/load.Group ownership. Update the meta-only example to use progress/operation terminology without adding load/cleanup product UI; remove unused scaffolding as each final caller converts.
- [ ] Verify public method/type/JSON names against [T], cancellation and error behavior. No deprecated aliases or public new management interface remain at completion.

**Verification**

- [ ] Run root API units/functional checks as applicable and `make test` across all five modules.

### Task 75: Adapt policy helpers and the existing scheduler/cache demos

**Files:** `examples/scheduler`, `examples/inmemory-cache`, public test-helper packages. **Target:** [T] §3, §5.4, §10; current module map. **Depends on:** 68–74. **Budget:** ≤550.

- [ ] Update Operation naming, exact predecessor arguments, ScheduleInput and completion/readiness consumers using actual group evidence. Keep existing application responsibilities and each demo's current purpose.
- [ ] Update API snippets/custom policies to preserve owned inputs and source freshness. Do not add a network operational API or fold full load examples into `examples/cluster`.
- [ ] Compile caller implementations against final interfaces; retain assertions on replay readiness and exact handles rather than waiting on a replacement cluster-wide marker.

**Verification**

- [ ] Run affected example units through `make test`; schedule Compose smoke runs for Task 87, not every API edit.

### Task 76: Adapt authenticated load transport and process-test fixtures

**Files:** `transport/loadgrpc`, `tests/cluster/testapp`, relevant `tests/cluster` helpers. **Target:** [T] §7.3, §8, §10. **Depends on:** 59, 65, 72–75. **Budget:** ≤550.

- [ ] Convert shared authority Operation fields and fixture callers while preserving authenticated exact sender/destination/delivery IDs. Keep unchanged load protocol bytes and event/Origin values; regenerate only genuinely changed .proto sources.
- [ ] Expose necessary test evidence through existing authenticated fixture controls: per-group progress, staged/rebased versions, cleanup milestones and deterministic fault barriers. Avoid new public library test-control APIs.
- [ ] Adapt retirement stores, mutations, policy helpers and fixture status expectations. Preserve exact ownership and physical-resource assertions when removing Active expectations.

**Verification**

- [ ] Run fixture/transport units and relevant protocol functional tests via repository targets; run `make test`.

### Task 77: Remove residual legacy execution consumers

**Files:** remaining `internal/metaengine`, `internal/metaservice`, `internal/operator`, `load`, lifecycle and authority helpers found by audit. **Target:** [T] §3.3, §5.4, §6.4, §10. **Depends on:** 1–76. **Budget:** ≤550; split residual batches before editing if needed.

- [ ] Classify every remaining Active/Operator/CurrentOperator/MaxPendingAuthorityRecords use. Remove old execution APIs, state lookups and global gates; retain legitimate NodeActive, historical documents and intentionally stable event/Origin/encoding strings with a reason.
- [ ] Convert remaining tests/doubles in the owning package without introducing composite state, aliases or an unreviewed shortcut. Remove the listed temporary bridges once their last caller is gone.
- [ ] Audit direct transfers, readmission, authority restore, history retention and controller fence-error handling for hidden cluster-wide serialization. Require exact evidence for every replacement predicate.

**Verification**

- [ ] Run focused tests for each changed consumer batch and `make test`; record the classified search results in the progress log.

### Task 78: Activate complete formats and eliminate transitional state

**Files:** `internal/metastore/store.go`, `internal/metaengine/sm.go`/`codec.go`, snapshot/restart tests, remaining composition. **Target:** [T] §4.1–4.3, §7.1, §8.4. **Depends on:** 1–77. **Budget:** ≤550.

- [ ] Activate complete metadata disk/state/envelope schema 4; verify authority schema 2 from Task 54. Remove Active storage/API, Operator/WaitOperator wrappers, old MutationResult fields, ActivateTarget/legacy envelopes and every registered temporary scaffold now that all callers are converted. No fallback, migration, mixed-version acceptance or refounding old evidence. Replace Task67 legacyEnsureProgress/legacyEnsureFailure/legacyTransferPlan with confirmed scoped progress, exact operation-block diagnostics and transfer_plan; remove schema3 replacement guards when connecting scoped replacement commits.
- [ ] Add intentionally sourced v4/v2 fixtures with provenance; retain old changed-format fixtures for explicit rejection and immutable unrelated physical baseline hashes for unchanged-format checks. Preserve current-index/term/confstate/pins and unchanged buckets.
- [ ] Exercise fresh startup and current-format retained restart through real composition. Confirm all new proposals/queries/dispatch/report callbacks are reachable, with no production dual-model switch or unsupported stub left on an accepted path. Cover successful operationCoordinator pages/cursor advancement, leadership changes across reads and native group-fence recipients (peer29 L1); host/Unsupported/NotLeader functional coverage already exists.

- [ ] Keep raw-Stage RebasedFrom normalization after removing temporary intent guards, and test that a supplied value is never persisted through the public entry point. Shared AuthorityGrant/AuthorizedStep operation vocabulary and JSON tags must be converted before freezing v4 fixtures; AdmissionRecord execution_id stays unchanged.

**Verification**

- [ ] Run focused format/restart functional tests, `make test` and `git diff --check`; confirm baseline files were not regenerated to match implementation.

### Task 79: Add cross-component isolation and race regressions

**Files:** owning `*_func_test.go` packages under metaservice/operator/managed and root. **Target:** [T] §8.5, §10. **Depends on:** 78. **Budget:** ≤550.

- [ ] Add only missing integration regressions after reviewing task-local tests: concurrent A/B completion, approval versus supersession, retry prefix proof, local holds, read cancellation and scoped final validation.
- [ ] Prove a hung host callback remains owned while healthy scopes progress, and late factory/effect/Close cannot overlap replacement or race shutdown. Use barriers and exact retained ownership checks.
- [ ] Verify safe progress does not fan out, normal admission survives unrelated Target change, founding `_meta` ignores failed load work and decommission rechecks current evacuation. Keep all multi-node consensus scenarios in Docker tasks.

**Verification**

- [ ] Run focused functional/race cases and `make test`; do not duplicate already sufficient unit evidence with implementation-mirroring tests.

### Task 80: Complete the storage and receipt crash matrix

**Files:** authoritystore/walstore/managed/metaservice functional tests. **Target:** [T] §8.5, §10, §12 collection algorithms. **Depends on:** 78–79. **Budget:** ≤550.

- [ ] Fill missing crash boundaries before/after Collect, local receipt commit, meta acceptance, ForgetCollection and journal removal. Prove collected rows never Collect after marker release; prepared rows safely recover partial deletion.
- [ ] Cover swapped/missing roots, mismatched markers, pin removed with unexpected contents, journal rebind, successor protection, retained failed-attempt reference and courier-proof pruning races.
- [ ] Assert unknown settled reports append nothing, false/missing proof cannot set CollectedAt, memory storage class cannot be downgraded and lost replies are safe both before and after pruning.

**Verification**

- [ ] Run focused storage/managed/service functional and race regressions, then `make test`.

### Task 81: Add process isolation and rebase survival scenarios

**Files:** new/updated `tests/cluster` clustertest cases using existing pool and controls. **Target:** [T] §5.3, §8.2/§8.5, §10. **Depends on:** 76–80. **Budget:** ≤550.

- [ ] Add `TestFailureIsolation_BlockedGroup` with three nodes: hold A's required progress while B executes/repairs under available meta quorum; assert exact operation/source evidence and no hidden global gate.
- [ ] Add `TestFailureIsolation_StagedRebase`: stage A, repair B, assert atomic R/S parents/RebasedFrom and unchanged A intent, then change metadata leader/restart and prove retained operation/provenance. Approve old L must return RebasedTo without approval of S.
- [ ] Use authenticated group-specific faults and barriers; clean containers/volumes/workers. Register focused names for Task 87; compile now, run Docker only after the implementation set is ready.

**Verification**

- [ ] Compile the clustertest package with `go test -c` as shown below and run `make test`; reserve scenario execution for Task 87. A `-run '^$'` invocation still executes this package's Docker-starting TestMain and is not compile-only.

### Task 82: Add process cleanup, returning-host and deletion scenarios

**Files:** `tests/cluster` cleanup/recovery/deletion cases. **Target:** [T] §6.2, §8.5, §10. **Depends on:** 81. **Budget:** ≤550.

- [ ] Add `TestFailureIsolation_RemovedHostCleanup`: remove an unreachable old load replica through surviving quorum, converge permitted replacement topology, retain exact unconfirmed debt, then return the old host and prove fencing before service and successor survival.
- [ ] Add `TestFailureIsolation_DeletionStopProof`: unreachable former owner keeps the deleted group pending without stop/readmission proof while another group continues; no false FullyConverged or collected milestone.
- [ ] Use distinct retained roots and exact IDs, not sleeps or “host absent means erased.” Use three nodes and reseating/placement available in existing fixtures; require a named justification before adding a fourth.

**Verification**

- [ ] Compile clustertest cases and run `make test`; execute together in Task 87.

### Task 83: Add process receipt-outage and uncertain-response scenarios

**Files:** `tests/cluster` reporting/admission/mutation cases, existing fixture controls. **Target:** [T] §3.4, §6.3, §8.4–8.5, §10. **Depends on:** 82. **Budget:** ≤550.

- [ ] Add `TestFailureIsolation_CollectionReporting`: pause metadata quorum after local collection preparation/completion, verify durable journal survives reporting failure/restart and exact receipt is eventually accepted or safely settled without repeating deletion.
- [ ] Add `TestFailureIsolation_ExactMutationReplay`: interrupt responses for stage/approval/retry/replacement; reuse original CAS/predecessor and assert one accepted version/allocation/retry edge. Use retained evidence, not guessed latest IDs.
- [ ] Reuse existing admission and shutdown process cases for successor courier/control-only behavior where possible; record which existing case supplies evidence instead of creating redundant large fixtures.

**Verification**

- [ ] Compile clustertest cases and run `make test`; execute together in Task 87.

### Task 84: Measure unresolved-debt persistence cost and query bounds

**Files:** metadata benchmark tests and progress scaling tests in their owning packages. **Target:** [T] §4.2–4.3, §6.4, §10–11. **Depends on:** 78–83. **Budget:** ≤550.

- [ ] Add `BenchmarkCleanupDebtApply` comparing representative 0/1000/10000 unresolved exact debts for changed Apply serialization, allocations and persisted bytes; include snapshot/restore cost where the existing benchmark fixture supports it. Use valid retained proof, not impossible fabricated rows.
- [ ] Measure bounded progress/cleanup paging and ordinary grant selection under debt; assert worker/queue/backoff bounds independently from durable state growth. Do not mistake bounded response size for bounded write cost.
- [ ] Record results and environment in this plan's progress log. Incremental persistence remains deferred unless measurements justify a separately reviewed design change; do not silently add a new storage architecture or permanent acknowledgement index.

**Verification**

- [ ] Run the focused benchmark command below, relevant paging units and `make test`; preserve results without inventing an unagreed latency threshold.

### Task 85: Update architecture and navigation contracts

**Files:** `README.md`, `docs/architecture/library.md`, `runtime.md`, `raft.md`, `project-map.md`, `comparision-with-tikv.md`, relevant `design-constraints.md` entries. **Target:** [T] §1–10. **Depends on:** 78–84. **Budget:** documentation only; split any code corrections separately.

- [ ] Describe implemented Latest/Target/convergence, scoped immutable operations, exact retry/rebase, stop/retire/collect boundaries, safe-read freshness and storage proof release. Replace obsolete current-contract Active/global-operator claims.
- [ ] Update the saturated project map with new interfaces/methods/schema/protocol ownership and grep tags from [T]. Distinguish TiKV mechanisms adapted from stronger gquorum proof requirements and remaining gaps.
- [ ] Document ordinary application usage without requiring cleanup/reporting calls; advanced specialized storage obligations stay outside the main Node/Cluster/load.Group learning path. Preserve limits and honest permanently lost-host behavior.

**Verification**

- [ ] Verify links, actual signatures/defaults and every “implemented” claim; run `git diff --check` and `make test` as the plan's checkpoint gate.

### Task 86: Update API audit, testing provenance and contributor guidance

**Files:** `docs/testing/api-refactor-validation.md`, `api-refactor-audit.md`, `api-baseline-physical.md`, `tests.md`, `AGENTS.md`, affected example/test READMEs. **Target:** [T] §4.3, §10–11. **Depends on:** 85. **Budget:** documentation only.

- [ ] Replace current contract names and test expectations; document exact new format fixture provenance and unchanged hashes. Do not rewrite historical validation as a fresh passing run.
- [ ] Record final suite ownership, five application modules, scoped process test names and limitations. Add concise retry/rebase and cleanup recovery examples to existing documentation, not a new public administration product.
- [ ] Carry deferred items from NOT CONSIDERED & TODOs to their relevant code comments (`TODO(not planned)`) only at actual seams. Do not add fake APIs, unreachable NotImplemented branches or recurring warning logs for absent features.

**Verification**

- [ ] Check paths/commands/snippets against code, run `git diff --check` and `make test`.

### Task 87: Run integrated validation and resolve regressions

**Files:** test reports and any focused fixes; record actual results in this plan. **Target:** [T] §10, all §8.5 failure cases. **Depends on:** 1–86. **Budget:** ≤550 Go lines per corrective task; split larger fixes before editing.

- [ ] Run final non-Docker units, lint, units under race, functional and functional-race targets. Inspect report.md/timeline.md/package logs; fix every failure encountered and rerun only affected checks until the final required set passes.
- [ ] Build the process fixture image once after source changes, run focused `TestFailureIsolation_` cases, then the complete ordinary cluster suite. Run the three affected example Compose smoke targets once. Rebuild after application changes; never replace the shared tag during a running suite.
- [ ] Preserve executed command, revision, report path, result and unverified scope. Confirm verified resource cleanup. Do not substitute historical records or compile-only Docker checks for executed consensus/restart evidence.

**Verification**

- [ ] Complete the Validation Commands checklist below; failures remain open work, not an automatically accepted limitation.

### Task 88: Verify and close the architectural delta

**Target:** [T] **all sections**, especially §2 diagram, §7 routing/contracts, §8 flows and §12 algorithms. **Depends on:** 1–87. **Budget:** ≤550 Go lines per corrective task; introduce additional bounded fix tasks before this final task if necessary.

- [ ] Verify public/domain delta: every C1–C4/C7/C9/C12 signature, field/tag, enum, limit and API removal matches [T]; no public aliases or extra ordinary application APIs remain. Verify all five modules and helper packages use the final contract.
- [ ] Verify metadata delta: exact v4 buckets/state/envelopes, every convergence/debt/intent/operation/index shape, proof validation, deterministic restore, source provenance, marker/document pins and old-format refusal. Confirm no Active or CollectionAcks state survives.
- [ ] Verify planner/FSM delta: exact semantic block set, only two barrier shapes, state-based admission/evacuation, stable admission origin and resumed-prefix validation, first approval, immutable retry/Previous receipts, atomic rebase and all strict CAS/allocation checks. Reconcile each §12 algorithm with actual call order.
- [ ] Verify authority/controller delta: all five global gates and controller fence short circuit are removed/scoped; one owner per group, exact floors/receipts, independent budgets, grant reuse, fair backoff and joined shutdown remain effective in transfers, readmission and restore.
- [ ] Verify storage/managed delta: journal v2, exact synced marker, all partial-deletion states, prepared-only Collect, collected-only report/release, Forget-before-journal-removal, immutable storage class, historical courier proof, same-host ownership joins and control-only recovery.
- [ ] Verify observation/service delta: every [T] §7.3 private route and authenticated discriminator exists; bounded safe-read pages/settlement, no fan-out, no-write settled response, exact freshness and local diagnostic limits. Confirm metrics and repair history survive unrelated changes.
- [ ] Verify tests/docs delta: map every [T] §8.5/§10 case to actual passing evidence or an explicit unresolved gap, verify 10k measurement and physical hashes, and reconcile all current architecture docs and contributor guidance with implemented behavior.
- [ ] Record every discovered gap in NOT IMPLEMENTED GAPS, then fix and verify it under the original target. An unresolved complex item is marked `[_]` with impact; it is not silently deferred or called complete. Re-run affected checks and the final required gate after corrections.
- [ ] **LAST subtask — verify actual wiring:** trace real NewNode→Start→metaservice/managed/controller/store construction and Cluster→private query/proposal paths through execution, persistence, recovery, collection and Stop. Confirm every target method/component is constructed, registered, invoked and reachable in normal operation, including excluded-host reporting; no feature exists only as an uncalled helper, test implementation, stub or optional unused callback.

**Verification:** Complete all Validation Commands on the final code revision; leave the task unchecked while any unapproved implementation gap or wiring gap remains.

## Validation Commands

Run from repository root unless a command explicitly changes directory. These commands are instructions for implementation; they were not run during planning. A focused package command supplements, not replaces, the per-task `make test` gate.

```sh
# Every code task: format changed Go files, then relevant focused tests and all units.
gofmt -w <changed-go-files>
go test -count=1 ./topology ./fault ./scheduling ./metrics
make test

# Relevant owner package(s), selecting actual regression names from that task.
go test -count=1 -tags=functional -run '<focused-regression>' ./internal/managed
go test -race -count=1 -tags=functional -run '<focused-regression>' ./internal/managed

# Final non-Docker gates; inspect test-reports/<suite>/latest and lint output.
make test
make lint
make test-race
make test-function
make test-function-race

# Target explicitly requires this measurement; put it in the functional benchmark
# suite if it acquires a store/replica, matching the existing snapshot benchmark.
go test -tags=functional -run '^$' -bench '^BenchmarkCleanupDebtApply$' -benchmem -count=3 ./internal/metaengine

# At the end only: compile cases without running Docker, then build once and run.
(cd tests/cluster && go test -c -tags=clustertest -o /tmp/gquorum-fi-cluster.test .)
make test-cluster-build
make test-cluster-run CLUSTER_ARGS='-v -run ^TestFailureIsolation_'
make test-cluster-run
make test-example
make test-scheduler-example
make test-cache-example

# Only if .proto sources actually change.
make generate
make proto-check

# Final semantic/format audit; classify legitimate historical/stable-name hits.
rg -n 'ActiveVersion|CurrentOperator|MaxPendingAuthorityRecords|WaitOperator|ErrOperatorAuthorization' --glob '*.go'
rg -n '\bActive\b|\bOperator\b|CollectionAcks|ActivateTarget|TODO\(not planned\)' --glob '*.go'
git diff --check
```

- [ ] All five application modules compile; all fast units pass after each task.
- [ ] Required focused functional/race regressions pass; final lint, race and functional suites pass.
- [ ] Focused process scenarios and complete ordinary cluster suite pass on the final image; modified demos pass smoke tests.
- [ ] 10k unresolved-debt measurement recorded, without confusing durable-state growth with worker/page limits.
- [ ] Old changed formats reject; new formats restart; unrelated physical baseline hashes remain unchanged.
- [ ] Remaining name/search hits classified; no hidden global gate, temporary stub or unwired feature remains.
- [ ] Report paths, revision and any limitations recorded; resource cleanup verified.

Do not run broad process performance/chaos campaigns by default; the focused debt benchmark and requested failure scenarios above supply this target's explicit scope. If Docker or another prerequisite is unavailable, record exactly what remains unverified and keep its checkbox open.


## NOT PLANNED CHANGES

- Task66 / peer thread30: reuse PlanRead and remaining-plan simulation, selecting required global steps with original ancestry evidence. BuildPlanPreferred now produces scoped blocks; explicit BuildLegacyPlan isolates the two schema3 preferred-planning callers until78 (the ordinary BuildPlan bridge also remains until78). OperationControllerConfig prepares existing ports without activating partial persistence. Meta drain takes PlanInput and resolves retained native bindings with target node states. No extra task, synthesized Active, retry plan payload or runtime mode switch. Peer29 L1's no-tests premise was disproven by existing functional tests; real page/native-recipient coverage is folded into78.

- Task65: `internal/operator/dispatch_cleanup_func_test.go` now releases the second-host response only after setting the returning-host barrier, then advances its fake retry clock. The full functional run exposed a background-loop wake racing the test driver; production cleanup behavior is unchanged. Ten focused race repetitions and the complete owning functional suites pass.

- Task62 / peer thread26: single Controller keeps explicit LegacyCoordinator until78; New statically rejects both/neither ports and mixed validators/stores. No copied runner, runtime flag or synthetic scoped view. Ready selection uses state barriers rather than raw historical Requires cursors; progress derives dependency/revocation reasons without polling writes. Scoped ports/selection/transition handling are62, dispatch/effects63–65, concrete service validators66 and live cutover78. Extend65 accordingly; no new tasks.

- Task61 / peer threads24–25: `scoped_cleanup_readmission.go` validates the supplied retirement key so a later consumed readmission can prove collection of an already-retired identity; first milestone remains unchanged. Existing closure retains the proof; pruning/restore regression and peer probe pass. Keep ReadView.Active/Views/legacy topology query until78 because placement67/scheduler69/join73 still consume them. Prepare final scoped pair/diagnosis without a new public compatibility type; legacy status projects exact declarations only. Observation producers finish70. All raw status consumers adapt to GroupProgress.Observation together.

- Task60: `internal/metaengine/reads.go` extracts the unchanged authority-grant query arm to keep query dispatch within lint complexity limits after adding progress. New paging helpers live beside existing convergence structs in `progress_*.go`; no new public types or live schema activation. Focused query regressions and all units pass.

- Task59 / peer thread23: reuse AuthorityRead's monotonic retained-member hint for restricted collection/status requests when participation headers are zero or lag the journal pin. Add the narrow internal Machine.ValidateCleanupCourier nil adapter until78; no new query/transport override. ReportCleanupStatus is exported only inside internal/metaservice and consumes existing managed.CleanupStatus so Node can wire the concrete callback without another public data type. The tracked amendment records these precisions.


- Task57 / peer thread22: collection acceptance, deferred status and snapshot validation now accept any retained exact retirement revision with recorded consumption, rather than binding to first receipt delivery order. Same scope, no new task; regression reproduces issuance/fence/reissue and both delivery orders. The tracked admission-target amendment documents why complete local retirement proof replaces a live assignment RPC.


- Task54: Collections returns (CollectionPage, error), with Records/Next fields, matching the existing CleanupPage convention and mandatory two-result lint limit. This mechanically replaces the target's three-result signature without changing paging behavior or adding a public application API.

- Task49: scheduler_policy_func_test.go assumed a 25ms callback deadline remained unexpired when inspected. A full parallel functional run disproved that timing assumption. Assert deadline presence/configured upper bound and retain owned-input/status checks; elapsed time after descheduling is not a contract violation.

- Pre-coding audit after Task 35/thread 13: whole-batch deletion completion, dependent-batch cancellation protection, and complete group/node/marker approval assembly exceed 550 changed Go lines with composed tests. Split former Task 36 into 36 (deletion completion/protection) and 37 (complete approval assembly). Former 37–87 shift by one: 88 tasks, revocation 38, activation 78, final audit 88. No work or verification is omitted; helpers compose through approval now and later transition completion.

- Peer thread 13 settled pending deletion/readmission: ID retirement is approval-time; new omitted readmission hosts need an issued usable batch at approval and whole-batch consumption at completion. Missing issuance is retryable, fenced proof refuses, and depended-on batches cannot be cancelled. Deletion before first convergence keeps source zero/nil declaration. The accepted amendment and Tasks 35–39/48/50/51 contain the exact checks and follow-up regressions; no new entity or public API.

- Approval pre-coding audit after Task 34: first deletion debt plus group/node/marker zero-step completion and their composed regressions exceed 550 changed Go lines. Split former Task 35 into Tasks 35 (deletion proof) and 36 (complete approval candidate); former Tasks 36–86 shift by one. There are now 87 tasks; scoped revocation/isolated installation is 37, activation 77, final audit 87. Shared completion helpers may be introduced at their first approval consumer and reused by later transition tasks. No behavior or verification obligation is removed.

- Approval pre-coding audit after Task 28: origin persistence/restore/join, scoped validation/ownership, consumed-prefix admission handover, and atomic debt-stop/approval installation exceed the 550-line task ceiling together. Split former Task 29 into Tasks 29–34; former Tasks 30–83 shift by three (83 tasks, scoped collection 61, activation 73, final audit 83). No verification obligation or user-approved behavior is removed. The shared handover and exact prefix checks precede all activation. Code removal comments are updated with Task 29.

- Scope pre-coding audit after Task 24: semantic derivation, affected-only planner/service collection and replicated scope/CAS validation exceed 550 Go lines together. Split former Task 25 into Tasks 25–27 before coding; old Tasks 26–81 shift by two (80 tasks, scoped collection 58, activation 70, final audit 80). Use the existing placement evaluator and a small expert topology helper. Keep affected-only helpers isolated until scoped block/approval/persistence wiring is safe; a partial plan must never reach legacy whole-document completion. No original verification obligation is removed.

- Public-planner pre-coding audit at commit 30: former Task 22 combines remaining-plan context/validator signatures, actual native producers, and about 60 construction call sites; its combined implementation/tests exceed 550 Go lines. Split into Tasks 22–24 before coding. Former Tasks 23–79 shift by two: 78 tasks, scoped collection 56, activation 68, final audit 78. Keep immutable proof distinct from new-plan work omission; no synthetic convergence or public compatibility API. Source removal comments will be updated with the next validated correction/task. No implementation commit proceeds until thread 8 is disposed.


- Task 21: `internal/metaengine/sm_test.go` forced-retirement fixture installed documents directly without their allocation counters. Added `sm.rememberAllocations(target)` so it models valid retained metadata; kept the new allocation guard strict. Focused metadata tests and make test pass.

- Retained-planner pre-coding audit after thread 6: the input/identity helpers, native/prepared retirement and shape rules, shared metadata evidence collector, validator callers and native/public planner conversion cannot fit one 550-line task. Replace former Task 18 with Tasks 18–22, preserving every obligation. Former Tasks 19–75 shift by four: 76 tasks, scoped collection 54, activation 66, final audit 76. Private transitional adapters retain explicit removal tasks; no implementation commit is made before the commit-25 peer findings are evaluated.

Earlier split notes below record their numbering at the time; current task headers and dependencies above are authoritative.

- Planner conversion footprint: 71 BuildPlan/BuildPlanPreferred/ValidatePlanShape uses plus voter, shape, membership simulation and readmission helpers exceed one 550-line commit. Split before coding into Tasks 16 (voter proof inputs), 17 (shape/readmission inputs), 18 (public signatures/callers). Old Tasks 17–73 shift by two: 72 tasks total, activation 62 and final audit 72. Preserve the full conversion and safety requirements; use only private intermediate adapters with explicit removal tasks.

- Admission/completion/voter estimate exceeds 550 Go lines when including original-plan and failed-admission regressions. Before editing, split prior Task 14 into current Tasks 14 (admission/completion) and 15 (voter history). Later tasks shift by one; activation is 60 and final audit is 70. All proof obligations remain explicit.

- Remaining proof estimate: exact cleanup milestones/readmission/collection plus admission/voter/completion coverage exceeds 550 Go lines. Before editing, split prior Task 13 into current Tasks 13 (cleanup) and 14 (admission/completion). Prior Tasks 14–72 become 15–69; activation 58 becomes 59. No validation obligation is dropped.

- Proof validation estimate: prior Task 12 requires more than 550 Go lines with authority, cleanup and completed-effect/admission regressions. Split before editing into current Tasks 12 (catalog/actions/revocations) and 13 (cleanup/admission/completed proof). Prior Tasks 13–72 become 14–68; activation 57 becomes 58. Original obligations remain explicit.

- Review checkpoint 2: Correction 2 fixes six confirmed findings. Target §7.1 now carries the full expected AuthorizedStep for cleanup issuance; §7.2 uses internal managed.CleanupStatus with historical MemberID/RetirementKey. These fix proof loss without enlarging ordinary library APIs. Index documents are immutable owned values; obligation gates reuse existing issuer/revocation predicates.

- Restore graph estimate: prior Task 10 needs roughly 750–900 Go lines with corruption coverage. Before coding, split it into current Tasks 10 (documents/attempts) and 11 (convergence/identity/provenance); subsequent splits are recorded above. Activation is now Task 63. All admission/voter effect proof checks remain explicit in Tasks 14–15. No new public validation API is required: reuse ValidatePlanGuardChain for immutable partitions/barriers.

- Restore task split: original Task 9 now expands to Tasks 9–14 (indexes, operation/convergence graph, authority/cleanup/admission proof). Estimated combined implementation and regression coverage exceeds 550 changed Go lines. Later tasks now begin at Task 16; the final audit is Task 73 and activation is Task 63. All requirements and order are preserved; historical progress entries keep their original numbering.

- Review checkpoint 1: bounded Correction 1 addresses confirmed transition/validation/copy defects. Target §3.2 is clarified to keep raw GroupStatus observation-only, add ReporterReplicaID, and reuse GroupProgress in ClusterStatus at Task 46. §6.4 now explicitly documents the already-chosen immutable replicated Limits semantics; no local tuning API is added. Independent preparation may run during peer review, but fixes/disposition precede the next implementation commit.

- Task 1: aggregate calculation/tests move to Task 22, where the FSM consumes them; introducing an unused helper or additional public aggregation method now would conflict with the small API and unused-code lint. Task 1 tests that cloned/paged values preserve the supplied full aggregate. No target behavior is deferred beyond its implementation task.

During implementation record unexpected edits as `path — reason — related task — validation`. Do not hide changes to the design or unrelated behavior in a mechanical rename.

## NOT CONSIDERED & TODOs

These are the target's existing exclusions, not new scope reductions. Add a focused `// TODO(not planned): ...` comment only at a real relevant seam; do not create empty APIs or warning loops.

| Tags | Excluded capability | Required present behavior / comment location |
| --- | --- | --- |
| `postponed`, `porting:deffered` | Physically destroyed-host waiver | Missing exact stop keeps that group deleting; unknown collection stays unconfirmed. Document near deletion/cleanup proof checks. |
| `postponed` | Mixed-version capability negotiation, migrations, disaster restore | Reject changed older formats and preserve evidence; current-format restart remains required. Comment at format checks, not a fake restore API. |
| `potential-improvement`, `porting:postponned-to-the-next-task` | Scheduler priorities, expiry and resource-pressure weighting | Fair bounded dispatch/backoff with existing timing controls. No priority framework. |
| `postponed` | Frozen multi-page export and historical observation storage | Each page has its own index; retain original sample timestamp. Explain cursor semantics. |
| `potential-improvement` | Incremental metadata persistence | Measure whole-state Apply at 10k debts first; unresolved proof remains durable, history caps soft. No permanent completion-ack index. |
| `too-complex`, `porting:simplefication` | TiKV key ranges/epochs, splits/merges, MVCC, TSO and PD service topology | Outside the group-library boundary; do not add placeholders or import another architecture. |

`porting:improvement-over-upstream`: durable collection proof and retry-on-delete-error are mandatory implemented differences, **not deferred work**. Keep their rationale beside the lifecycle/marker implementation.

## NOT IMPLEMENTED GAPS

No implementation audit has run yet. All task checkboxes are initially open; absence of entries here does not mean the target is implemented.

During execution use `- [ ] target section — missing behavior — affected path/task — evidence`, change to `[x]` only after fixing and verifying, or `[_]` for explicitly identified unresolved complexity. Only the user can approve a design deferral; report its impact. The final architectural audit must resolve every item or leave completion explicitly open.

## Progress log

- 2026-10-15 — Plan created from the complete target and closed peer review; inspected current contracts, authority gates, planner/controller, retirement/journal paths and five-module test layout. No source implementation or implementation test run in this planning session.
- 2026-10-15 — Planning validation: checked 64 sequential tasks, backward declared dependencies, per-task target/verification references, local links, code fences and final wiring subtask. Corrected early type dependencies, completion preparation order, final API removal order and compile-only Docker instructions after inspecting TestMain. `git diff --cached --check` passed. No change to the target design was required.
- Execution entries: `date | task | commit | changed contracts/scaffolding | commands and reports | result/gaps`. Record actual ports and temporary bridge removal here.

## Contract excerpts

The following target-state contracts are copied from [T] for implementation tasks that change types/interfaces. Algorithm and schema prose in the cited target sections remains required; these excerpts alone do not authorize effects. Any discrepancy is resolved against [T], then corrected here before implementation continues.

### C1 — Cluster and mutation contracts

```go
// Added.
ReadProgress(context.Context, topology.ProgressQuery) (topology.ProgressPage, error)
RetryOperation(context.Context, topology.OperationID) (topology.OperationID, error)

// Renamed; same local-observation / terminal-wait distinction.
Operation(context.Context, topology.OperationID) (topology.OperationStatus, error)
WaitOperation(context.Context, topology.OperationID) (topology.OperationStatus, error)

// Updated type/meaning; signatures otherwise preserved.
Approve(context.Context, uint64) (topology.OperationID, error)
ReadAuthority(context.Context, topology.OperationID) (topology.AuthorityStatus, error)
WaitRevoked(context.Context, topology.OperationID) (topology.AuthorityStatus, error)
ReadTopology(context.Context) (topology.ReadView, error)
Status(context.Context) (topology.ClusterStatus, error)
EnsureGroup(context.Context, GroupSpec, EnsureOptions) (topology.Group, error)
TransferLeader(context.Context, string, string) error

// Replacement requests now identify the predecessor explicitly.
ReadmitNode(context.Context, string, uint64, MutationOptions) (MutationResult, error)
ReseatReplica(context.Context, string, topology.ReplicaKey, MutationOptions) (MutationResult, error)
```

```go
type MutationOptions struct {
    ExpectedVersion uint64 `json:"expected_version,omitempty"`
    Actor           string `json:"actor,omitempty"`
    Comment         string `json:"comment,omitempty"`
    Approve         bool   `json:"approve"`
    Preview         bool   `json:"preview"`
}
type MutationResult struct {
    Document    topology.Document         `json:"document"`
    OperationID topology.OperationID      `json:"operation_id,omitempty"`
    RebasedTo   uint64                    `json:"rebased_to,omitempty"`
    Placement   *topology.PlacementReport `json:"placement,omitempty"`
}
```

### C2 — Progress values

```go
type FullyConverged struct {
    Version uint64    `json:"version"`
    At      time.Time `json:"at"`
}
type ProgressQuery struct {
    GroupID     string      `json:"group_id,omitempty"`
    NodeName    string      `json:"node_name,omitempty"`
    OperationID OperationID `json:"operation_id,omitempty"`
    Limit       int         `json:"limit,omitempty"`
    After       string      `json:"after,omitempty"`
}
type ProgressPage struct {
    AppliedIndex   uint64             `json:"applied_index"`
    ReadAt         time.Time          `json:"read_at"`
    LatestVersion  uint64             `json:"latest_version"`
    TargetVersion  uint64             `json:"target_version"`
    FullyConverged FullyConverged     `json:"fully_converged"`
    Groups         []GroupProgress    `json:"groups"`
    Nodes          []NodeProgress     `json:"nodes"`
    Operations     []OperationStatus  `json:"operations"`
    Cleanup        []CleanupProgress  `json:"cleanup"`
    Next           string             `json:"next,omitempty"`
}
type BlockedReason struct {
    Code        string      `json:"code"`
    Message     string      `json:"message,omitempty"`
    OperationID OperationID `json:"operation_id,omitempty"`
    StepSeq     int         `json:"step_seq,omitempty"`
    NodeName    string      `json:"node_name,omitempty"`
    ReplicaID   uint64      `json:"replica_id,omitempty"`
}
type GroupProgress struct {
    GroupID          string         `json:"group_id"`
    Desired          *Group         `json:"desired,omitempty"`
    Approved         *Group         `json:"approved,omitempty"`
    Converged        *Group         `json:"converged,omitempty"`
    ConvergedVersion uint64         `json:"converged_version"`
    OperationID      OperationID    `json:"operation_id,omitempty"`
    Deleting         bool           `json:"deleting"`
    Blocked          *BlockedReason `json:"blocked,omitempty"`
    Observation      *GroupStatus   `json:"observation,omitempty"`
}
type NodeProgress struct {
    NodeName         string         `json:"node_name"`
    Desired          *Node          `json:"desired,omitempty"`
    Approved         *Node          `json:"approved,omitempty"`
    Converged        *Node          `json:"converged,omitempty"`
    ConvergedVersion uint64         `json:"converged_version"`
    MemberID         uint64         `json:"member_id"`
    OperationID      OperationID    `json:"operation_id,omitempty"`
    Blocked          *BlockedReason `json:"blocked,omitempty"`
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
    Blocked     *BlockedReason  `json:"blocked,omitempty"`
    UpdatedAt   time.Time        `json:"updated_at"`
}
func (q ProgressQuery) Validate() error
func (p ProgressPage) Clone() ProgressPage
```

### C3 — Operation and immutable plan values

```go
type OperationID string
type OperationState string // existing pending/running/waiting_*/terminal values
type OperationStatus struct {
    ID          OperationID     `json:"id"`
    Version     uint64          `json:"version"`
    Attempt     uint32          `json:"attempt"`
    State       OperationState  `json:"state"`
    Blocks      []BlockStatus   `json:"blocks"`
    TotalBlocks int             `json:"total_blocks"`
    BlocksTruncated bool        `json:"blocks_truncated"`
    ErrorCode   string          `json:"error_code,omitempty"`
    Error       string          `json:"error,omitempty"`
}
type BlockStatus struct {
    GroupID     string          `json:"group_id"`
    State       OperationState  `json:"state"`
    CurrentStep int             `json:"current_step"` // immutable global Step.Seq
    TotalSteps  int             `json:"total_steps"`  // required execution steps
    Blocked     *BlockedReason  `json:"blocked,omitempty"`
    ErrorCode   string          `json:"error_code,omitempty"`
    Error       string          `json:"error,omitempty"`
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
type OperationTransition struct {
    OperationID OperationID    `json:"operation_id"`
    Version     uint64         `json:"version"`
    GroupID     string         `json:"group_id"`
    Attempt     uint32         `json:"attempt"`
    StepSeq     int            `json:"step_seq"`
    State       OperationState `json:"state"`
    Blocked     *BlockedReason `json:"blocked,omitempty"`
    ErrorCode   string         `json:"error_code,omitempty"`
    Error       string         `json:"error,omitempty"`
    At          time.Time      `json:"at"`
}
type Plan struct {
    Version      uint64          `json:"version"`
    Steps        []Step          `json:"steps"`
    VoterBases   []PlanVoterBase `json:"voter_bases,omitempty"`
    Blocks       []PlanBlock     `json:"blocks"`
    Dependencies []StepDependency `json:"dependencies,omitempty"`
}
type PlanBlock struct {
    GroupID       string `json:"group_id"`
    SourceVersion uint64 `json:"source_version"`
    Steps         []int  `json:"steps"`
    Cleanup       []int  `json:"cleanup,omitempty"`
}
type StepDependency struct {
    StepSeq  int   `json:"step_seq"`
    Requires []int `json:"requires"`
}
func MakeOperationID(version uint64, attempt uint32) OperationID
func (s OperationState) Terminal() bool
func (s OperationState) Waiting() bool
func (s OperationState) Valid() bool
func (p Plan) Clone() Plan
func (p Plan) Equal(Plan) bool
func (p Plan) IsEmpty() bool
func (a AuthorizedStep) Matches(OperationID, uint32, uint64, Step) bool
```

### C4 — Structured conflict

```go
type Error struct {
    Code            Code
    Message         string
    Cause           error
    ExpectedVersion uint64
    CurrentVersion  uint64
    RebasedTo       uint64
}
// Existing Error(), Unwrap() and Is(error) methods remain.
```

### C5 — Metadata state and cleanup records

```go
type smState struct {
    SchemaVersion    uint32                   `json:"schema_version"`
    Rules            topology.Rules           `json:"rules"`
    Latest           topology.Document        `json:"latest"`
    Target           topology.Document        `json:"target"`
    History          []topology.Document      `json:"history"`
    Operations       []topology.Operation     `json:"operations"`
    Groups           []groupConvergence       `json:"group_convergence"`
    Nodes            []nodeConvergence        `json:"node_convergence"`
    Cleanup          []cleanupRecord          `json:"cleanup"`
    Intents          []intentRecord           `json:"intents"`
    FullyConverged   topology.FullyConverged  `json:"fully_converged"`
    AuthorityRecords []authorityRecord        `json:"authority_records,omitempty"`
    AuthorityFloors  []resourceFloor          `json:"authority_floors,omitempty"`
    Revocations      []revocationRecord        `json:"revocations,omitempty"`
    RetiredGroups    []retiredGroup            `json:"retired_groups"`
    Allocations      []allocationRecord        `json:"allocations"`
    Admissions       []AdmissionRecord         `json:"admissions,omitempty"`
    Sessions         []sessionState            `json:"sessions"`
    Locks            []lockEntry               `json:"locks"`
    Activated        []string                  `json:"activated,omitempty"`
    VoterSwitches    []topology.VoterSwitchState `json:"voter_switches,omitempty"`
    AppliedIndex     uint64                    `json:"applied_index"`
    AppliedTerm      uint64                    `json:"applied_term"`
}
type groupConvergence struct {
    GroupID       string               `json:"group_id"`
    Declaration   *topology.Group      `json:"declaration,omitempty"`
    SourceVersion uint64               `json:"source_version"`
    Owner         topology.OperationID `json:"owner,omitempty"`
    Deleting      bool                 `json:"deleting"`
    CompletedAt   time.Time            `json:"completed_at"`
}
type nodeConvergence struct {
    NodeName      string               `json:"node_name"`
    Declaration   *topology.Node       `json:"declaration,omitempty"`
    SourceVersion uint64               `json:"source_version"`
    MemberID      uint64               `json:"member_id"`
    Owner         topology.OperationID `json:"owner,omitempty"`
    CompletedAt   time.Time            `json:"completed_at"`
}
type intentRecord struct {
    Version         uint64 `json:"version"`
    ExpectedVersion uint64 `json:"expected_version"`
    Kind            string `json:"kind"`
    Digest          string `json:"digest"` // lowercase SHA-256 hex
    GroupID         string `json:"group_id,omitempty"`
    NodeName        string `json:"node_name,omitempty"`
    PreviousID      uint64 `json:"previous_id,omitempty"`
    SuccessorID     uint64 `json:"successor_id,omitempty"`
}
```

```go
type cleanupRecord struct {
    Identity       storage.Identity       `json:"identity"`
    Storage        topology.StorageClass  `json:"storage"`
    MemberID       uint64                 `json:"member_id"`
    OperationID    topology.OperationID   `json:"operation_id"`
    StopAction     *topology.AuthorizedStep `json:"stop_action,omitempty"`
    RetireAction   *topology.AuthorizedStep `json:"retire_action,omitempty"`
    ReadmissionKey *topology.AuthorityKey   `json:"readmission_key,omitempty"`
    Removed        *topology.EffectReceipt `json:"removed,omitempty"`
    Stopped        *topology.EffectReceipt `json:"stopped,omitempty"`
    Retired        *topology.EffectReceipt `json:"retired,omitempty"`
    Collection     *CollectionReceipt      `json:"collection,omitempty"`
    RemovedAt      *time.Time              `json:"removed_at,omitempty"`
    StoppedAt      *time.Time              `json:"stopped_at,omitempty"`
    RetiredAt      *time.Time              `json:"retired_at,omitempty"`
    NotBefore      *time.Time              `json:"not_before,omitempty"`
    Blocked        *topology.BlockedReason `json:"blocked,omitempty"`
    CreatedAt      time.Time               `json:"created_at"`
    UpdatedAt      time.Time               `json:"updated_at"`
}
type CollectionReceipt struct {
    Identity      storage.Identity      `json:"identity"`
    MemberID      uint64                `json:"member_id"`
    RetirementKey topology.AuthorityKey `json:"retirement_key"`
    Token         string                `json:"token"`
    LocalSequence uint64                `json:"local_sequence"`
    CollectedAt   time.Time             `json:"collected_at"`
}
```

### C6 — Journal record and retirement store

```go
type CollectionRecord struct {
    Retired       storage.RetiredReplica `json:"retired"`
    MemberID      uint64                 `json:"member_id"`
    Grant         topology.AuthorityGrant `json:"grant"`
    Retirement    topology.EffectReceipt `json:"retirement"`
    Sequence      uint64                 `json:"sequence"`
    State         string                 `json:"state"` // prepared | collected
    CollectedAt   *time.Time             `json:"collected_at,omitempty"`
}
```

```go
// storage/retirement.go UPDATED. RetiredReplica remains unchanged.
type RetirementStore interface {
    Retired(context.Context) ([]RetiredReplica, error)
    Collect(context.Context, RetiredReplica) error
    ForgetCollection(context.Context, RetiredReplica) error
}
```

### C7 — Scoped policy input

```go
type ScheduleInput struct {
    Now      time.Time
    Target   topology.Document
    Groups   map[string]topology.GroupProgress
    Status   map[string]topology.GroupStatus
    Health   map[string]topology.GroupHealthEvaluation
    Alive    map[string]time.Duration
    Held     []storage.Identity
    Metrics  metrics.ClusterSnapshot
    Switches []topology.VoterSwitchState
}
func (in ScheduleInput) Clone() ScheduleInput
// RepairPolicy.Schedule(context.Context, ScheduleInput) (ScheduleDecision, error)
// remains the extension point; its signature does not change.
```

### C8 — Scoped authority records

```go
type authorityRecord struct {
    Grant   topology.AuthorityGrant  `json:"grant"`
    Fences  []topology.FenceEvidence `json:"fences"`
    Effects []topology.EffectReceipt `json:"effects"`
    Closed  bool                     `json:"closed"`
    Class   string                   `json:"class"`
}
type revocationRecord struct {
    Requested   bool                       `json:"requested"`
    OperationID topology.OperationID       `json:"operation_id"`
    GroupID     string                     `json:"group_id"`
    Obligations []topology.FenceObligation `json:"obligations"`
}
```

### C9 — Execution limits

```go
type Limits struct {
    MaxNodes             int `json:"max_nodes"`
    MaxReplicas          int `json:"max_replicas"`
    MaxConcurrentGroups  int `json:"max_concurrent_groups,omitempty"`
    MaxConcurrentCleanup int `json:"max_concurrent_cleanup,omitempty"`
}
```

### C10 — Replicated proposal contracts

```go
type ScopeExpectation struct {
    GroupID       string               `json:"group_id"`
    SourceVersion uint64               `json:"source_version"`
    Owner         topology.OperationID `json:"owner,omitempty"`
}
type BeginOperationProposal struct {
    Version      uint64               `json:"version"`
    Plan         topology.Plan        `json:"plan"`
    Scopes       []ScopeExpectation   `json:"scopes"`
    Actor        string               `json:"actor"`
    Origin       topology.Origin      `json:"origin"`
    At           time.Time            `json:"at"`
    ProposalID   string               `json:"proposal_id"`
}
type RetryOperationProposal struct {
    Previous   topology.OperationID `json:"previous"`
    Scopes     []ScopeExpectation   `json:"scopes"`
    At         time.Time            `json:"at"`
    ProposalID string               `json:"proposal_id"`
}
type TransitionOperationProposal struct {
    Transition topology.OperationTransition `json:"transition"`
    ProposalID string                       `json:"proposal_id"`
}
type CommitRepairOperationProposal struct {
    Doc             topology.Document `json:"doc"` // candidate approved repair
    ExpectedVersion uint64            `json:"expected_version"` // captured Latest
    ExpectedTarget  uint64            `json:"expected_target"`
    Scopes          []ScopeExpectation `json:"scopes"`
    Plan            topology.Plan     `json:"plan"`
    At              time.Time         `json:"at"`
    ProposalID      string            `json:"proposal_id"`
}
type RecordCollectionProposal struct {
    Receipt    authoritystore.CollectionReceipt `json:"receipt"`
    Courier    topology.Identity                `json:"courier"`
    ReadmissionKey *topology.AuthorityKey        `json:"readmission_key,omitempty"`
    At         time.Time                         `json:"at"`
    ProposalID string                            `json:"proposal_id"`
}
```

### C11 — Internal coordinator and journal methods

```go
// internal/operator; owned values from a bounded ready-work query.
type WorkItem struct {
    Operation topology.Operation
    GroupID   string
}
type Coordinator interface {
    IsMetaLeader() bool
    ReadyBlocks(context.Context, int) ([]WorkItem, error)
    RecordOperationTransition(context.Context, topology.OperationTransition) error
}
// Scope validators replace validators receiving an ambiguous whole Active doc.
type ScopedValidator func(context.Context, topology.Operation, string) error

// internal/authoritystore; Identity/RetiredReplica are storage types.
func (s *Store) PrepareCollection(context.Context, CollectionRecord) (CollectionRecord, error)
func (s *Store) CompleteCollection(context.Context, storage.Identity, uint64, time.Time) (CollectionReceipt, error)
func (s *Store) RemoveCollection(context.Context, CollectionReceipt) error // exact collected-row CAS
func (s *Store) Collections(context.Context, string, int) ([]CollectionRecord, string, error)
type CollectionResult struct {
    Receipt      CollectionReceipt `json:"receipt"`
    AppliedIndex uint64            `json:"applied_index"`
    Settled      bool              `json:"settled"` // no retained obligation; not historical acceptance
}

// Existing managed configuration gets concrete callbacks; no new public interface.
type CollectionCallbacks struct {
    Report func(context.Context, authoritystore.CollectionReceipt) (authoritystore.CollectionResult, error)
    Status func(context.Context, CleanupStatus) error
}
type CleanupStatus struct {
    Identity storage.Identity
    MemberID uint64
    RetirementKey topology.AuthorityKey
    NotBefore *time.Time
    Blocked *topology.BlockedReason
}
```

### C12 — Planner and observation deltas

Exact signatures from [T] §7.2; existing NativeState remains the native input value.

```go
type PlanInput struct {
    Approved Document
    Groups   map[string]GroupProgress
    Nodes    map[string]NodeProgress
    Native   map[string]NativeState
    Retained map[string]RetainedGroup
}
type RetainedGroup struct {
    FoundingIssued bool              `json:"founding_issued"`
    Replicas       []RetainedReplica `json:"replicas,omitempty"`
}
type RetainedReplica struct {
    ReplicaKey
    Progress RetainedProgress `json:"progress"`
}
type RetainedProgress string
const (
    RetainedHosted  RetainedProgress = "hosted"
    RetainedCleanup RetainedProgress = "cleanup"
    RetainedStopped RetainedProgress = "stopped"
)
func BuildPlan(PlanInput, Document, Rules) (Plan, error)
func BuildPlanPreferred(PlanInput, Document, PlanOptions) (Plan, error)
func ValidatePlanShape(PlanInput, Document, Plan) error
```

The following are field deltas specified in [T] §3.2/§5.4, not replacement definitions of every existing status field:

- ReadView's complete fields are AppliedIndex, Latest, Target, FullyConverged; View exposes only Latest and Target.
- GroupStatus retains existing fields and adds Reporter (`topology.Identity`), ReceivedAt (`time.Time`), SourceVersion (`uint64`), with `reporter`, `received_at`, `source_version` JSON names.
- ClusterStatus replaces ActiveVersion with FullyConverged and Operator with Operations (`[]OperationStatus`), adds OperationsTruncated (`bool`), per-group converged source/owner, and CleanupSummary{Pending, Unconfirmed int; OldestPendingAt *time.Time}. Status headers omit Blocks and explicitly signal truncation.
- metrics.ClusterSnapshot replaces ActiveVersion with TargetVersion. GroupObservation adds SourceVersion; exact assignment, reporter/sample identity and receive timestamps remain part of validity.
- Document adds RebasedFrom (`uint64`, `json:"rebased_from,omitempty"`); ParentVersion is still the actual document base.
- AuthorizedStep.OperatorID and shared public authority/error names become Operation vocabulary; unchanged action identity fields retain their exact semantics. Preserve intentionally stable unrelated event/Origin encodings.

### C13 — Schema activation and private route checklist

| Storage or protocol | Final target |
| --- | --- |
| Meta disk schema | `format/schema_version` ASCII `4`; existing hardstate, entries, snapshot, applied, identity, serve_override and load_replicas buckets/keys otherwise unchanged. |
| Meta state/envelope/snapshot | Schema 4; complete C5 state, no Active or CollectionAcks. All progress mutations travel through replicated Apply. |
| Host authority journal | Schema 2; existing identity/fences/effects plus collection. Key: len32(group)+group+len32(node)+node+replica64, big-endian; JSON C6 record. |
| Store-local marker | `<token>.collection` next to retired directory, JSON `{schema_version:1, retired:RetiredReplica}`; sync marker/root before deletion and root after deletion/proof release. |
| Safe queries | Existing quorum/apply path plus `progress` and private `collection_check`; latter returns `CollectionCheck{Untracked bool; AppliedIndex uint64}` and changes no state. |
| ApplyVersion | Approval/retry discriminator; retry supplies exact Previous operation, never a new Plan. |
| ProposeMeta | Existing authenticated path carrying v4 commands; FSM independently validates scopes and proof. |
| MetaControl | Typed `report_collection`/`report_cleanup_status`; authenticated historical host/readmission courier; no-op settlement exception only after safe absence proof. |
| Existing load actions | Exact grants/receipts, including retained detached stop/retire authority; no new public network service. |

Internal shapes not shown in C10's code block still must be implemented from [T] §7.1–7.3: BeginCleanupEffectProposal, RecordCleanupStatusProposal, ProposeDocument.Intent, CommitVoterChangeProposal.ExpectedTarget/Scopes, QueryRequest.Progress/CollectionIdentity, CollectionCheck and collection/status request/reply payloads. Their required fields, tags and trust boundaries are explicitly specified there. NewNode, metaengine.NewMachine, metaservice's existing constructor and operator.New keep their ownership roles and receive the updated existing configs.
