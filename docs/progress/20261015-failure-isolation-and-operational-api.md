# Failure isolation and operational API execution

Plan: `../plans/20261015-failure-isolation-and-operational-api.md`
Target: `../brainstorms/20261015-failure-isolation-and-operational-api-target.md`
Branch: `feat/failure-isolation-and-api`
Started: 2026-10-15

## Task 1 — Operation domain vocabulary

Date: 2026-10-15. Implementation commit 1 (the commit containing this entry).

Implemented:
- Exact Operation identity/state/status/block/transition/provenance shapes, BlockedReason and owned clones. Cloning never recomputes an aggregate from truncated blocks.
- OperationAuthorization/ErrOperationAuthorization preserve the execution_authorization classification.
- State/ID, deep-copy and partial-projection regressions.

Verified:
- `go test -count=1 ./topology ./fault` passed.
- `make test`: 837/837 cases passed in 9.484s; `test-reports/unit/20261015T152616.413037Z-i1owd12g/report.md`.
- `make lint-staged`: vet both variants, production golangci, revive and merged unused checks passed. Pinned lint tools were installed locally.

Transition inventory:
- 172 existing Go files reference Operator identity/state/status/transition vocabulary. Root Cluster methods/results: Tasks 4/42/50/54; topology authority/status and voter records: Tasks 2/3/14/15/53; metaengine's 46 files: Tasks 7–9/13–29/53; metaservice's 35: Tasks 24–27/35–37/42/49/53; controller: Tasks 38–40; journal/managed/native authority: Tasks 30/41/47–48/52; scheduling: Tasks 44–46; caller examples/tests: Tasks 50–52. Events need stable-encoding classification, not blanket string replacement.
- Existing topology/operator*.go remains the legacy live model while scoped behavior is built. Remove its obsolete type/state/status/record definitions in Tasks 53–54; shared Plan/Step helpers move to operation files as their tasks change them.
- fault.OperatorAuthorization and fault.ErrOperatorAuthorization are temporary compile aliases, removed with remaining callers by Task 54. No compatibility promise or public adapter documentation was added.
- Aggregate calculation/tests moved to Task 16 (documented in plan) so there is no unused production helper or extra public aggregation API. No final target behavior changed.
- Dead-code review is due with implementation commit 5.

## Task 2 — Immutable group blocks and barriers

Date: 2026-10-15. Implementation commit 2 (the commit containing this entry).

Implemented:
- Plan blocks/dependencies with exact JSON fields, deep Clone/Equal, and nonempty zero-step declaration semantics. Moved Plan values out of the legacy operator files.
- Partition validation rejects missing/duplicate ownership, invalid sequence/group/source references, native effects disguised as cleanup and non-admission/removal edges. Cycle detection includes required within-block order.
- Regressions cover malformed partitions, both legal barriers, cycles, zero-step blocks and every new mutable/equality field.

Verified:
- `go test -count=1 ./topology ./internal/metaengine` passed, including unchanged voter-proof regressions.
- `make test`: 857/857 cases passed in 7.878s; `test-reports/unit/20261015T153141.355129Z-mi8mclzo/report.md`.
- Staged lint passed after correcting revive's operand-order finding; focused tests reran after that correction.

Notes:
- The old planner still emits flat plans. ValidatePlanShape validates any supplied blocks/dependencies; its explicit temporary absent-block guard must disappear when Task 12/54 converts all producers. The strict block helper already rejects missing partitions. Dynamic receipt/evacuation checks remain in the planned FSM/barrier tasks.
- The user added peer review at every five implementation commits. The plan now requires dead-code review, paseo-peer findings, reasoned disposition and corrective commits; first review is due after implementation commit 5.

## Task 3 — Progress values and observation provenance

Date: 2026-10-15. Implementation commit 3 (the commit containing this entry).

Implemented:
- Exact progress query/page/group/node/cleanup types, historical marker, cleanup summary and stable blocker codes. Query validation accepts zero as the 100-row default and rejects invalid limits/filters; actual bounded cursor paging remains Task 36.
- Owned page/scope clones preserve nil evidence, full operation aggregate, reporter identity and original timestamps. Extracted the existing group clone used by Document rather than maintaining duplicate nested-field copy logic.
- Added future read/status convergence and observation source fields. Existing Active/Operator fields remain only for consumers converted in Tasks 37/49–54; no synthetic current topology was introduced.

Verified:
- `go test -count=1 ./topology` passed.
- `make test`: 859/859 cases passed in 7.978s; `test-reports/unit/20261015T153535.307700Z-zt9wr8vl/report.md`.
- Staged lint passed after grouping standard-library/third-party imports as required by goimports.
- Mutation tests verify cloned group/node maps, replica slices, hosting/placement values, cached observation maps/identity, blockers and every cleanup timestamp cannot alter retained inputs.

Review cadence: two implementation commits remain before the first peer/dead-code review.

## Task 4 — Mutation outcomes and structured conflicts

Date: 2026-10-15. Implementation commit 4 (the commit containing this entry).

Implemented:
- MutationResult gains OperationID and RebasedTo; staged documents survive optional approval failure. Existing OperatorID remains only until the API cutover in Task 54.
- Expected/current/rebased versions survive FSM results, their custom JSON codec, private forwarding and classified errors. CAS producers now supply expected/current values; Stage no longer replaces forwarded details with an empty result.
- Centralized result classification and mutation outcome propagation, with round-trip and partial-success regressions.

Verified:
- Focused fault/metaengine/metaservice/metatransport units passed.
- Functional mutation regressions for approval failure, exact expected-version/preview semantics and concurrent typed CAS passed.
- `make test`: 861/861 cases passed in 7.408s; `test-reports/unit/20261015T154451.357454Z-edwpqcmz/report.md`.
- `make lint-staged` passed; two initial style findings corrected without reversing the dependent errors.As/nil check.

Findings:
- ApplyResult has a custom disk.go JSON codec: adding struct fields alone lost forwarded conflicts. The new round-trip regression caught this; both codec directions are now updated. Task 8 explicitly includes this file for its forthcoming schema work. Existing zero-valued optional fields preserve unrelated physical fixtures.
- First peer/dead-code review is due after the next implementation commit.

## Task 5 — Execution capacity and scoped policy input

Date: 2026-10-15. Implementation commit 5 (the commit containing this entry).

Implemented:
- Limits adds group/cleanup worker counts with defaults 4/2, explicit-value preservation and negative rejection; node/replica defaults remain 3/5. No execution count is used as a durable-proof cap.
- ScheduleInput.Groups owns approved/proven progress and strips Desired from callback copies. Metrics gains document TargetVersion and exact per-group SourceVersion.
- Tests cover normalization, JSON field names, provenance preservation and callback isolation. Updated the Node defaults regression for the two new defaults.

Verified:
- `go test -count=1 ./topology ./scheduling ./metrics` passed.
- `make test`: 869/869 cases passed in 4.979s; `test-reports/unit/20261015T154724.132535Z-ogu0rmip/report.md`.
- `make lint-staged` passed, including merged unused declarations.

Transition inventory and review checkpoint:
- MaxPendingAuthorityRecords stays live until scoped authority admission replaces its consumers in Tasks 14–15/54. ScheduleInput.Latest/Active stays until Tasks 44/51/54, and metrics.ActiveVersion until Task 46/54. None is a final compatibility promise.
- This is the first five-commit peer/dead-code checkpoint. The batch starts after bbbe148. A dedicated peer thread will review these contracts and actual behavior; justified fixes precede Task 6.

## Checkpoint 1 / Correction 1 — Peer review and transition safety

Date: 2026-10-15. Implementation/correction commit 6 (the commit containing this entry).
Review: docs/review/feat_failure-isolation-and-api.md, thread 2, rounds 1–2. Range reviewed: bbbe148..bab8b91.

Disposition and corrections:
- Accepted required-stop validation: deletion and same-host replacement stops cannot be detached cleanup. Founding prerequisites must name actual founders; block source versions strictly precede their plan version. Added regressions.
- Accepted fail-closed transition guard: the live legacy FSM and restore cannot accept Blocks/Dependencies that their records discard. Shared validateLegacyPlan covers ordinary approval, repair, readmission, voter changes and restored exact plans. Remove this temporary guard at Task 54's complete format/dispatch activation, not merely when a new record type exists.
- Accepted faithful Clone: policy filtering belongs at the driver/conformance callback boundary. Added copy/filter and conformance mutation regressions; a custom policy can use its owned input as scratch without changing its test case.
- Accepted no-effect caller correction: the four repair/healing guards now inspect len(plan.Steps), since declarations can have blocks without effects.
- Accepted observation/progress separation: raw GroupStatus no longer carries ConvergedVersion/OperationID; explicit ReporterReplicaID joins Reporter/ReceivedAt/SourceVersion. Target §3.2 and Task 37 now reuse GroupProgress for ClusterStatus.Groups rather than adding another public header entity. Native observations stay nested and untrusted for convergence.
- Did not adopt peer's local worker-setting proposal: target §6.4/C9 explicitly chooses replicated Limits, consistent with the repository's sole-capacity-setting rule. Worker values therefore require agreement and are immutable on retained restart/join, just like other Limits. Documented this tradeoff in target and Go docs; runtime tuning and a new local API are outside this approved target. The peer proposed treating this as a user decision; Codex retains the already-approved design unless the user changes it.
- Peer follow-up is in round 2. Notification was refused because the terminal was asking its user about worker settings; no draft/input was overwritten. No claim of peer agreement on that optional redesign.

Dead-code review:
- Full make lint (all modules/variants and merged unused declarations) passed at bab8b91; staged lint passed the correction. No newly unreachable declaration was found.
- Removed duplicate placement cloning by reusing PlacementRule.Clone. Retained live aliases/fields are not dead: fault aliases still occur in 80 Go files, the pending cap in 10, and ActiveVersion in 32. Their conversion/removal remains assigned, not a compatibility promise. Broad mechanical renames and new public Group/Node clone APIs were not introduced.

Exact producer/removal inventory:
- ReadView.FullyConverged, ClusterStatus.FullyConverged/Operations/OperationsTruncated/Cleanup and group progress: Task 37, final live activation Task 54.
- GroupStatus.Reporter/ReporterReplicaID/ReceivedAt/SourceVersion: exact cache/source producers and invalidation in Tasks 37/46/49; never relabel a predecessor sample.
- ScheduleInput.Groups and omission of old Latest/Active: Task 44/51; callback filtering is already enforced if Groups is supplied.
- Metrics.TargetVersion/GroupObservation.SourceVersion: Task 46; remove ActiveVersion then/by Task 54.
- Limits worker consumption: Tasks 39–40. Remove old durable-proof cap via Tasks 14–15/54.
- MutationResult.OperatorID and fault.Operator aliases: Tasks 53–54. history_unavailable result classification gets its FSM producer in Task 24.
- These value contracts remain intermediate scaffolding until their producer tasks; current zero fields are not evidence of scoped completion. Final ordinary API/adapter conversions and docs remain Tasks 50–54/61–62.

Verified:
- Focused topology, metaengine, public/internal scheduling and conformance units passed; functional exact-CAS/preview/partial-approval regressions passed.
- To exclude uncommitted independent Task 6 preparation, exported the exact staged index and ran make test there: 875/875 cases passed in 8.726s. Report: /private/tmp/gquorum-correction1.SbYvwJ/test-reports/unit/20261015T161039.763195Z-hq17l3nq/report.md.
- Working-tree make test including Task 6 preparation also passed 896/896 cases; test-reports/unit/20261015T160906.692182Z-84vggl29/report.md.
- The physical baseline meta-store rejection regression passed; immutable fixture paths have no diff. Staged lint and git diff --check passed after extracting the replacement predicate to meet cognitive-complexity limits.

Cadence: correction counts as commit 6; the next peer/dead-code review is due at implementation/correction commit 10. Independent preparation may continue while an immutable range is reviewed, but accepted corrections precede the next implementation commit.

## Task 6 — Specialized collection contracts

Date: 2026-10-15. Implementation/correction commit 7 (the commit containing this entry).

Implemented:
- Internal CollectionRecord, CollectionReceipt and CollectionResult match C6/C11, with owned proof copies and concrete managed CollectionCallbacks. No additional application-facing owner or interface.
- Exact journal keys use length-prefixed group/node and a big-endian replica ID. Validation checks the host pin, retirement target/resource/grant, opaque token, sequence and prepared/collected timestamps. Historical member proof survives a legitimate pin rebind.
- The final Retired/Collect/ForgetCollection contract is specified in target §4.2 and C6. Interface activation and removal of the current absent-is-success behavior remain coordinated with the real marker backend in Task 32, as this task requires; no success placeholder was added. Storage documentation now states that missing enumeration is not collection proof.

Verified:
- Focused authoritystore and managed units passed, including malformed proof/state/key cases and collected-record JSON/copy isolation.
- make test: 896/896 cases passed in 8.918s; test-reports/unit/20261015T161234.998162Z-1vf78n1i/report.md.
- make lint-staged and git diff --check passed. Handwritten Go diff: 314 lines.

Remaining consumers: journal methods in Task 30, marker semantics in Tasks 31–32, host execution/reporting in Tasks 33–35/41 and Node callback wiring in Task 48; complete format activation in Task 54. These contract declarations do not yet enable collection reporting.

Peer notice update: the round 2 pointer was delivered to Claude and verified in his live terminal despite the helper reporting unconfirmed; the queued round 1 notice was a duplicate already processed. Follow-up is still pending. Next five-commit checkpoint remains implementation/correction commit 10.

## Task 7 — Replicated operation and private query payloads

Date: 2026-10-15. Implementation/correction commit 8 (the commit containing this entry).

Implemented:
- Exact internal BeginOperation/RetryOperation/TransitionOperation/CommitRepairOperation, collection receipt/status and cleanup-effect payloads. Retry cannot supply a Plan; no payload can select an authority Class. Shared strict JSON shape decoding rejects unknown fields, absent/null required payloads and blank correlation IDs. Replicated identity/time/CAS validation remains with the respective handler tasks.
- ScopeExpectation and the single intentRecord definition support later persistence and typed mutations. ProposeDocument carries optional Intent; voter change adds ExpectedTarget/Scopes. Existing handlers explicitly refuse these fields until scoped persistence is active, preserving correlation and Unsupported through result/transport classification.
- QueryRequest adds OperationID, Progress and exact CollectionIdentity; incompatible query selectors reject instead of silently producing an unrelated view. CollectionCheck contains only Untracked/AppliedIndex. New query and mutation kinds remain unsupported, with no fabricated response or completion.

Verified:
- Focused metaengine/metaservice units passed. Payload round trips preserve exact receipt/courier/readmission linkage and operation tuples; negative tests cover unsupported fields, required payloads, cleanup authority classes, injected retry plans and unimplemented dispatch. Existing handler tests prove new provenance cannot be silently discarded.
- make test: 907/907 cases passed in 7.950s; test-reports/unit/20261015T162455.806884Z-e14nf9lr/report.md.
- make lint-staged and git diff --check passed after two formatting corrections. No protobuf change: existing MetaRequest/Reply bytes carry the private discriminated JSON payloads.

Transition inventory:
- Live Encode/DecodeProposal retain schema 3 and reject new proposal types; the new types decode only their own isolated JSON payloads. Enable complete kinds/handlers/envelopes together in Task 54, removing the Intent/scoped-voter/OperationID guards and old ActivateTarget/OperatorID consumers then. Existing voter optional tags avoid activating new fields on the legacy path; final required-field validation is part of cutover.
- ScopeExpectation/intentRecord are reused by Task 8; strict shape checks do not replace handler authority checks in Tasks 13–29. Query producers remain Tasks 29/36–37.

Checkpoint follow-up:
- Claude round 3 agrees and withdraws the requested worker-setting decision as a blocker. Thread 2 is closed with a recorded outcome. The user was told the immutable-limit tradeoff.
- Tasks 39–40 explicitly prohibit allocation proportional to arbitrary configured counts; they cap actual ready work. Task 37 shares complete declaration/observation semantics between Status and ReadProgress while preserving their different read guarantees. Task 44 uses only Status for policy observations and clears Groups.Observation at the callback boundary. No new public tuning API or sparse status-row type is introduced.
- Next peer/dead-code checkpoint remains implementation/correction commit 10.

## Task 8 — v4 metadata models and deterministic serialization

Date: 2026-10-15. Implementation/correction commit 9 (the commit containing this entry).

Implemented:
- Complete v4 outer schema, group/node convergence, exact cleanup records, shared intent provenance, authority Class and per-operation/group revocations. Operation snapshots retain the immutable Plan and separate complete block statuses. No Active, executions or CollectionAcks field exists in the v4 shape.
- Document.RebasedFrom preserves staged-edit provenance through cloning/JSON; the legacy staging path refuses caller-supplied values until the real FSM rebase producer exists.
- Snapshot arrays sort owned slice copies and reject duplicate keys. Unchanged session/lock/admission/allocation/resource-floor records retain their encodings. A failed decode never replaces the destination. Nil declarations retain source/timestamp evidence distinguishing tombstones from never-converged rows.

Verified:
- Focused topology/metaengine units passed, including every top-level indexed array's duplicate rejection, canonical round trips, the complete C5 field list, flattened authority records, memory debt without a collection fact and live restore refusal without mutation.
- make test: 927/927 cases passed in 7.187s; test-reports/unit/20261015T163615.340972Z-bz589v_2/report.md.
- Staged lint passed after wrapping long comparisons. Physical baseline metadata rejection test passed; immutable fixtures were not edited. git diff --check passed.

Activation inventory:
- scopedState/scopedAuthorityRecord/scopedRevocationRecord are private isolated wire records, reached through smState.Scoped only when decoding/explicitly constructing v4 codec fixtures. No producer changes schema versions or writes v4 state. Live Restore still rejects v4 before installation.
- At Task 54 replace the legacy smState/disk codec with scopedState, flatten the existing authority record plus Class, replace legacy revocation records and remove the temporary smState.Scoped wrapper. This is a format transition scaffold, not another application API or a selectable runtime authority model.
- Complete graph, owner, identity-floor and retirement-proof validation belongs to Task 9; schema round trips alone are not restore authorization. Existing authority inner Operator naming and related consumers retain their assigned conversion tasks before cutover.

Next checkpoint: implementation/correction commit 10, after the next bounded task, triggers peer and dead-code review of commits 6–10.

## Task 9 — Scoped lookup and unfinished-resource indexes

Date: 2026-10-15. Implementation/correction commit 10 (the commit containing this entry).

Plan refinement before implementation:
- Original Task 9 combined index construction, operation/convergence graph checks and authority/cleanup proof validation; its combined implementation plus meaningful regressions would exceed the mandatory 550-line ceiling. Split it into consecutive Tasks 9–11 without removing requirements. Original Tasks 10–64 are now Tasks 12–66; activation is Task 56. Updated current plan/code navigation references and verified all 66 task headers and backward prerequisites. Historical progress/review entries retain their original numbering.

Implemented:
- Decoder-owned indexes for exact groups/nodes/owners, operations, first/latest attempts, immediate retries, cleanup identities, parent/kind/digest intents, retained document versions, rebase successors and authority grants.
- One resource-obligation index covers unfinished execution and cleanup grants plus missing revocation evidence. It reuses resourceConsumed, deduplicates keys with sets and sorts results. A closed grant cannot erase an independent missing revocation; consumed resources do not block. Work/storage scales with actual retained evidence, without worker-count or legacy debt-cap admission.
- Conflicting document versions, retry/rebase successors and duplicate composite lookup keys fail before replacing a decoded state or its indexes. Indexes are never encoded or accepted as independent durable evidence. Full semantic graph/proof validation remains explicitly in Tasks 10–11; live installation remains disabled until Task 56.

Verified:
- Focused metaengine units passed, including pre/post-decode lookup equivalence, owned row references, exact predecessor/successor separation and atomic refusal. A 2,500-obligation test verifies shared-resource deduplication and absence of old capacity gating. These index fixtures intentionally test lookup structure, not authority acceptance.
- make test: 936/936 cases passed in 5.942s; test-reports/unit/20261015T164649.819386Z-h6lb29s2/report.md.
- Full make lint passed across all modules/build variants and merged unused declarations; /tmp/gquorum-review-batch2-lint.log. Corrected line lengths and extracted the existing-resource consumption loop to meet complexity limits. Focused regressions and all units passed after the extraction.
- git diff --check passed; 364 changed handwritten Go lines including tests/navigation comments.

Five-commit checkpoint 2 / dead-code review:
- Batch is commits 6–10, starting after bab8b91. Correction 1 was already reviewed in thread 2; the new peer request will focus on collection/proposal/state/index contracts and transition safety.
- No additional safe deletion found. toDiskOperator/metaDiskState remain called by the live schema-3 codec; validateLegacyPlan still guards live approval and voter paths; pendingCost/newAuthorityGrantCost remain used by live issue/revocation/readmission/transfer/restore. Removing them before the scoped consumers replace them would weaken current guards. Their removal is assigned to Tasks 16–17/55–56. The broad legacy-name search still reaches 144 Go files; these are live conversion work, not unused code.
- New internal collection callbacks/contracts get their concrete producers in Tasks 32–37/43/50; the v4 codec/indexes are called by isolated decoding and retain no independent storage. Task 56 removes the temporary codec wrapper and guards. Full unused-code lint found no newly unreachable declaration.
- Request peer review before the next implementation commit; assess findings and commit justified corrections as required. Next regular checkpoint is commit 15, counting any corrections.
