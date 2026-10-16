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

## Correction 2 — Second five-commit review
Date: 2026-10-15. Commit: this entry's commit (implementation/correction commit 11).

Implemented
- Accepted all six thread 3 findings after inspecting the live issuer/settlement predicates. Scoped obligations now use current floors plus unresolved revocations, reusing resourceConsumed, fenceSettled, obligationWithEvidence and fenceConsumed. Older non-floor grants remain proof. Transfer receipts permit ordinary next actions but do not erase resumable intent during revocation.
- Raw staging permanently rejects caller RebasedFrom separately from the temporary Intent guard. BeginCleanupEffect carries the full expected AuthorizedStep; managed.CleanupStatus carries the historical MemberID and RetirementKey. Updated target §§7.1/7.2 and matching plan excerpts; no ordinary public API added.
- Document index entries are owned immutable values, including nested declarations. Original approval means attempt 1 without Previous; a retained retry cannot become the original, and index IDs must match version/attempt.
- Recorded future obligations: strict complete status, no duplicate intent on rebase, shared authority naming before v4 fixtures, exact sorted revocations/Class derivation, and an internal cross-package intent construction boundary.
- Split prior Task 10 before implementation into current Tasks 10/11; proof validation is Task 12 and activation Task 57 (67 tasks total). Task 10 preparation remains uncommitted and was excluded from staged verification.

Verified
- Focused scoped/plan regressions passed; exact staged snapshot make test: 941/941, 6.992s. Report: /private/var/folders/yx/srxx8n_s1hlgyr9t9504zy6h0000gn/T/gquorum-correction2-61605seq/test-reports/unit/20261015T170401.342272Z-6w640aaf/report.md.
- Exact staged lint passed: unit/functional vet, production golangci, selected revive and merged unused declarations; /tmp/gquorum-correction2-staged-lint.log. Original-approval checking was extracted to satisfy the complexity limit without suppressions.
- Regression cases cover unnecessary undelivered fence after late consumption, older authority settled by a newer fence, leadership transfer, 2500 independent retained debts, immutable document copying, missing original attempt and exact cleanup payload identity.
- No stores, network or goroutines are acquired by these changes; process/race gates remain in their scheduled ownership/final tasks. No dead declaration is safe to remove beyond replacing the narrower obligation helper. Next review checkpoint is implementation/correction commit 15.

## Task 10 — Validate scoped documents and immutable operation attempts
Date: 2026-10-15. Commit: this entry's commit (implementation/correction commit 12).

Implemented
- The isolated v4 decoder validates its document/operation graph before installing owned state and derived indexes. It checks rule/schema identity, document/parent/rebase/applied boundaries, historical FullyConverged evidence, complete aligned block status, exact attempt IDs and retained planning/approved sources.
- Existing ValidatePlanGuardChain now also validates scoped partitions/barriers; no new public API or synthetic Active document. Exact step shapes and voter guards remain mandatory. Required-step cursors exclude detached cleanup; terminal aggregate precedence retains failure/supersession.
- Previous links require an immediately preceding failed attempt with identical immutable plan/provenance. Retries preserve every successful/superseded sibling row. Missing/cyclic/skipped predecessors and altered plans refuse atomically.
- History has no hard version-count cap; retained operation sources remain pinned until their verification dependency can be released. FullyConverged remains historical rather than equal to current Target/progress.
- Codec fixtures now have valid document/attempt graphs. Convergence/identity validation is Task 11, authority proof preparation Task 12; live Machine restoration/writes remain schema 3 until Task 57. New helpers are reached by decoding, not unused test-only scaffolding.

Verified
- Focused scoped graph and topology block/barrier tests passed. Corruption tests verify both pure candidate non-mutation and decoder refusal preserving prior bytes/index ownership.
- make test: 977/977, 7.330s; test-reports/unit/20261015T171116.388366Z-ak7ti4lg/report.md.
- Exact staged lint passed (unit/functional vet, production golangci, selected revive, merged unused); /tmp/gquorum-task10-lint.log. Extracted document/step/block checks to satisfy complexity and line limits without suppressions.
- No storage/network acquisition or concurrency change. Existing live v4 refusal regression remains green. Changed Go is below the 550-line task ceiling; next review checkpoint remains commit 15.

## Task 11 — Validate convergence, identity floors and retained provenance
Date: 2026-10-15. Commit: this entry's commit (implementation/correction commit 13).

Implemented
- v4 decoding now checks approved group/node coverage, exact live block ownership in both directions, block/source agreement, successful-block checkpoints, source timestamps/declarations, deletion state and permanent group tombstones. Failed/terminal blocks cannot own live execution; independent nonterminal siblings retain their owners.
- A pruned source document is accepted only when another validated retained document supplies the identical live declaration/member context; otherwise verification evidence must stay pinned. Completed tombstones remain independent of operation history. No composite Active or history/debt cap was added.
- Node member IDs describe exact source-document membership; proven absence uses zero, distinguished from never converged by SourceVersion/CompletedAt. Historical effects/retirement records keep their former identities. Clarified the internal field and target §4.1 comment; live replica authorization is unchanged.
- Identity checks bind all retained documents/checkpoints/operation step IDs to monotonic allocation floors and consistent host names. Latest allocation counters cannot regress. Retired IDs cannot reappear in Latest/Target, and retirement cannot be newer than approved Target. Reused unchanged session/lock/retirement validators with only their relevant fields, without synthesizing Active.
- Intent records require accepted parent/document linkage, known kinds, lowercase SHA-256 shape, original-only rebase provenance, ordered replacement IDs and an exact accepted successor assignment. Typed method construction/acceptance in Tasks 26–27 reuses the known-kind seam; admission/readmission and authority/effect proofs remain Task 12.

Verified
- Focused scoped units passed. Corruption cases cover missing/terminal/duplicate owners, lost successful checkpoints, false deletion, source/member mismatches, allocator loss/regression, unapproved retirement, orphan locks, malformed intent and foreign successor IDs. Decoder refusal preserves prior bytes and index identity.
- Positive tests retain completed group/node tombstones and safely compacted checkpoint sources. Codec/index fixtures were updated to satisfy these graph contracts; their authority/cleanup proof data remains explicitly pending Task 12 validation.
- make test: 1007/1007, 7.422s; test-reports/unit/20261015T172723.660513Z-hmpx_f1i/report.md.
- Exact staged lint and diff checks passed; /tmp/gquorum-task11-lint.log. No network/store acquisition or new concurrency, so functional/race work remains in its scheduled owning tasks. No suppressions or public API additions. Next peer/dead-code checkpoint is commit 15.

## Task 12 — Validate scoped authority actions and revocations
Date: 2026-10-15. Commit: this entry’s commit (implementation/correction commit 14).

Implemented
- Isolated v4 decoding now restores exact authority records/floors, validates receipt and fence boundaries, readmission-grant linkage, immutable action tuples and scoped revocations before publishing candidate indexes. Reuses existing catalog predicates without Active, global completion or the obsolete debt cap.
- Class is derived from retained required/cleanup partitions. Archived unfinished cleanup requires its exact debt/action/host/member; unfinished execution cannot lose its plan. Settled historical execution grants may remain proof after operation pruning.
- Grant targets respect replica/member allocation floors and stable host names. Revocation obligations are sorted, unique and scoped; inherited obligations can legitimately have Requested=false, as in existing successor revocation behavior.
- Split the former proof task before coding into catalog validation (12) and remaining cleanup/admission/completion proof (13). Activation is Task 58. Current fixture cleanup milestones, admission rows and voter evidence await that next stage; live v4 installation remains refused.

Verified
- Focused scoped/catalog units passed. Cases reject forged class/action, missing/decreasing floors, contradictory or refused receipts, invalid fence proof and scope; detached unresolved debt remains accepted above the old cap.
- make test: 1024/1024, 8.668s; test-reports/unit/20261015T175040.148142Z-ws9s3ure/report.md.
- Exact staged lint passed (unit/functional vet, production golangci, selected revive and merged unused); /tmp/gquorum-task12-lint.log. Extracted checks for complexity/line limits without suppressions. Changed handwritten Go remains below 550 lines. Pure validation adds no acquired resources or concurrency; no functional/race run required here.
- Next commit is checkpoint 15, followed by full dead-code inspection and peer review before subsequent implementation.

## Task 13 — Validate exact cleanup and collection proof
Date: 2026-10-15. Commit: this entry’s commit (implementation/correction commit 15).

Implemented
- Isolated v4 restoration binds cleanup storage/member/replica identity to retained approved or proven declaration context. A staged-only declaration is insufficient. Exact stop/retirement actions remain bound to their immutable operation when retained, or to independently retained debt after operation pruning.
- Native removal, required stop and retirement milestones bind to identical catalog receipts, exact targets/resources/actions and metadata timestamp ordering. Whole-group deletion has no invented RemovedAt and may retain unfinished required stops. Complete retained readmission batches provide the separate action-free proof path.
- Collection requires exact identity, former member, retirement key, nonempty opaque token, its own nonzero journal sequence and host timestamp. Memory storage rejects collection. Collection sequence comes from a separate bucket and its host clock differs from metadata timestamps; neither is falsely ordered against retirement evidence counters/clocks.
- Tests retain prior state/indexes on malformed proof; codec fixtures now include real stop/removal evidence. Successor key separation remains a pure index test: a copied predecessor receipt is no longer presented as valid successor restore proof.
- Before editing, split the remaining proof task into cleanup (13) and admission/voter/completed-step proof (14). Plan now has 69 tasks; activation is 59. Live Machine restore/writes still reject v4. Fixture admission/voter data and successful-step evidence remain pending Task 14, rather than implied complete by this checkpoint.

Verified
- Focused scoped tests passed; make test: 1048/1048, 9.127s, test-reports/unit/20261015T175807.673416Z-a2qvvsjx/report.md.
- Staged lint and full make lint passed, including all module vet variants, production golangci, revive and merged unused checks. Logs: /tmp/gquorum-task13-lint.log and /tmp/gquorum-checkpoint15-lint.log. No lint suppressions; 391 changed handwritten Go lines; dependency/header audit confirms 69 consecutive tasks and backward dependencies.
- Pure validation introduces no store/network acquisition or concurrency; functional/race/process checks remain assigned to ownership and final validation tasks. Live format rejection regression passed with the scoped suite.

## Five-commit checkpoint 3 — Commits 11–15
Date: 2026-10-15. Reviewed batch begins after f8c7712.

- References and full unused-code lint identify no safely removable declaration. New graph/proof helpers are reached by isolated v4 decoding. validateLegacyPlan still protects live schema-3 approvals/voter changes; pendingCost/newAuthorityGrantCost still protect existing issue/revocation/readmission/transfer/restore. Old cleanupProofsLocked still serves recovery, catalogs and pruning. Removing these before replacing consumers would weaken the live path.
- Scoped authority settlement reuses existing predicates and obligation validation rather than adding parallel implementations. Transition codec/guards and residual legacy consumers are explicitly removed in Tasks 58–59; scoped revocation removes the old debt cap in Task 19.
- Request peer review of Correction 2 plus Tasks 10–13 before the next implementation commit. Admission/voter/completed-effect validation is explicitly unfinished Task 14, and no live format has changed. Next regular checkpoint is commit 20, counting corrections.

## Correction 3 — Approved rebase provenance and compacted checkpoints
Date: 2026-10-15. Commit: this entry's commit (implementation/correction commit 16).

Implemented
- Addressed review thread 4 findings 2 and 5: rebase S must be exactly L+2 with repair parent L+1, retain L, and reject any approval, convergence, plan-source or effect/cleanup-action authority at L. Corrected the codec fixture: approved versions 1/2/3, unapproved L=4, repair R=5 with parent 3, successor S=6.
- Compacted live group/node checkpoints now require identical declaration/member context from approved evidence. Staged-only Latest cannot supply that proof. The common approval-witness helper includes node sources and is shared with cleanup declaration checks.
- Refined one proposed review condition: R.Parent cannot always equal L.Parent because several staged edits can accumulate above Target. R.Parent cannot be later than L.Parent; when L.Parent is independently approved it must equal R.Parent. Regression retains multiple staged edits without granting them approval. This distinction is sent back for peer confirmation.
- Prepared Task 14 is preserved in stash 1f3bf69fa03898a85cefbd1ad7d8c7bef324d476, including its pre-coding task split; no shared review files were stashed. Planned three bounded review corrections before resuming that prepared work. No live format activation or public API addition.

Verified
- Focused scoped regressions passed, including every forbidden rebase approval/action witness and staged-only compaction context. make test: 1066/1066, 8.792s; test-reports/unit/20261015T182207.078737Z-5_j_tmb0/report.md.
- Extracted compacted-group checking to meet complexity limits; focused tests rerun afterward. Exact staged lint and diff checks passed; /tmp/gquorum-correction3-lint.log. Changed handwritten Go remains below 550 lines. Pure restore validation adds no resource acquisition or concurrency.
- Review follow-up still covers pending cleanup/retry and normalization corrections. Next regular peer/dead-code checkpoint remains commit 20.

## Correction 4 — Exact cleanup progress, pending stops and retry lineage
Date: 2026-10-15. Commit: this entry's commit (implementation/correction commit 17).

Implemented
- Addressed thread 4 findings 1/3/7 for the ordinary action path. Every debt pins its originating operation and source/approved context. Removed archived-cleanup authority and optional-origin branches; the codec fixture now retains a real guarded removal/deletion plan.
- Required pending stops bind to the exact chain tip, whose group must either be nonterminal with its exact owner or failed without an owner awaiting retry. Deletion keeps its former declaration and Deleting row; no premature tombstone or supersession. Consumed stops retain their attempt identity. Detached actions and retirement remain bound to the original approval.
- Added reverse grant-to-milestone agreement and retirement issuance boundary checks with one grant traversal and exact debt lookups. Late predecessor-attempt receipts remain historical evidence. A combined meta stop/retire action may issue before its own completion but its consumed receipt supplies both milestones.
- Applied the peer's final ancestry correction: walk retained staged parents to reject any skipped approved/action-bearing ancestor. Explicit decreasing-parent checks prevent malformed map iteration order from causing an infinite walk. Compaction gaps retain the documented verification limit.
- Reused a single fence-obligation comparator and removed the redundant retry block-count comparison. Before coding, split readmission-origin preservation into Correction 5 (250–400 estimated Go lines); normalization is Correction 6. Updated target §4.1/4.3/6.2 with agreed exact proof and retention rules. No API addition or live v4 activation.

Verified
- Scoped regressions cover failed/retried deletion, late predecessor consumption, successor stop receipts, origin loss, premature retirement/tombstones, supersession, wrong retry binding, combined stop/retire proof and unrelated convergence with pinned cleanup context.
- make test: 1078/1078, 6.993s; test-reports/unit/20261015T183722.591921Z-lsmdye09/report.md. Exact staged lint and diff checks passed; /tmp/gquorum-correction4-lint.log. Changed handwritten Go remains under 550 lines; no resources/concurrency added.
- Existing authorityCatalog still rejects duplicate grants for one action. Recorded the mandatory coordinated change in the later cleanup issuance task: multiple reconciled revisions must retain compatible late receipts without rewriting milestones. Do not loosen one validator while leaving the other incompatible.
- Next correction preserves original cleanup actions through complete later readmission; this pending path is not implied finished here. Next regular review/dead-code checkpoint is commit 20.

## Correction 5 — Preserve cleanup origin through complete readmission
Date: 2026-10-15. Commit: this entry's commit (implementation/correction commit 18).

Implemented
- Readmission-created debt retains its admission operation as origin and has no invented removal/actions. Older removal debt retains its original operation, Stop/Retire actions and native/consumed-stop receipts; the later readmission is independently linked through its exact batch/admission operation.
- Validates the historical member/replica against the readmission grant target, not the successor admission's new member ID. Requires the complete consumed batch including _meta and preserves exact readmission retirement proof. Both operation roots remain pinned; updated target/retention notes accordingly.
- Reuses the existing readmission-batch predicate through authorityCatalog for legacy and scoped validation. Reverse milestone checks distinguish the exact action receipt from a milestone supplied by readmission; late old action receipts remain retained historical proof. Existing cleanup grants retain their action bindings instead of being deleted to make restoration pass.
- Fixture now represents an approved later admission independent of the original removal, with valid guarded learner-first steps. Added positive tests for retained consumed stop and readmission-supplied stop, plus readmission-created meta debt and invalid origin/member/key/partial-batch cases.

Verified
- Focused scoped regressions passed. make test: 1088/1088, 9.761s; test-reports/unit/20261015T184127.867404Z-ha0rcqwp/report.md.
- Exact staged lint and diff checks passed; /tmp/gquorum-correction5-lint.log. 235 changed handwritten Go lines, below 550. No public API addition, live v4 activation or new acquired/concurrent resource.
- Next: normalize generated rebase provenance and finish review/retention notes, then restore prepared Task 14. Regular checkpoint remains commit 20.

## Correction 6 — Normalize generated provenance on ordinary edits
Date: 2026-10-15. Commit: this entry's commit (implementation/correction commit 19).

Implemented
- Replaced raw RebasedFrom rejection with normalization in the common ordinary FSM document installer. Clone-derived admission edits and raw read/modify staging both clear the generated field. Typed mutation preparation clears it after edits, so previews are consistent too.
- Atomic rebase remains the only path allowed to set provenance; the later repair task explicitly uses its atomic installer and tests real post-rebase raw/typed edits. The current live format cannot yet generate a real rebase, so focused tests seed the generated field or supply it through an edit and verify installation/history/preview normalization.
- Clarified atomic retry-chain pruning and missing-original HistoryUnavailable semantics in retention/lookup tasks. Updated target comments for excluded declared nodes with no source membership and independent collection-bucket sequence/host time. Historical Correction 2 now points to this replacement contract; public-entry cutover testing expects normalization.
- Thread 4 is resolved without a user decision. Corrections 3–6 incorporate the final ancestry walk, honest failed deletion state, exact successor required-stop identity and distinct readmission origin rules. The prepared Task 14 remains preserved for restoration after this commit.

Verified
- Focused scoped/ordinary-derivation units passed. Focused functional TestMutationNormalizesGeneratedProvenance passed in internal/metaservice, using its existing deterministic FSM fixture.
- make test: 1091/1091, 8.081s; test-reports/unit/20261015T184502.996408Z-nbimk3_b/report.md. Exact staged lint and diff checks passed; /tmp/gquorum-correction6-lint.log. 87 changed handwritten Go lines, below 550; no new concurrency or acquired runtime resources.
- Full functional/race/Docker suites have not been rerun for these pure validation/derivation corrections; cross-process behavior remains assigned to final integration validation. Next commit is checkpoint 20.

## Task 14 — Validate admission lineage and completed required effects
Date: 2026-10-15. Commit: this entry's commit (implementation/correction commit 20).

Implemented
- Isolated v4 restore now validates exact admission identity/process/original linkage, the operation's pinned approved member and learner-first sequence, and readiness/learner/promotion/completion milestones. Reuses existing admission and authority-consumption validators. Unrelated Target changes do not invalidate an owned admission with the same member; failed attempts retain evidence without current execution authority.
- Founding admission requires its exact successful _meta block, independently of failed load siblings, aggregate operation state and FullyConverged. Complete old-owner readmission is required where older approved membership or retained batch evidence exists; local readiness can precede its preparation acknowledgement.
- Completed required steps require exact retained effect consumption from their own attempt or a validated Previous predecessor. Closed historical receipts remain usable; revocation/fencing does not manufacture consumption and old receipts are never relabeled. WaitCaughtUp remains an observational wait under the existing contract. Fresh final native predicates remain mandatory in the later live transition/controller tasks, not invented by restore.
- Restored prepared work from stash 1f3bf69fa03898a85cefbd1ad7d8c7bef324d476 and reconciled it with Corrections 3–6. Shared approval/readmission predicates already landed in those corrections and were not duplicated. Corrected the later admission fixture's missing wait-before-promotion; the admission guard remains strict. The stash is retained as a recovery copy, not an uncommitted dependency.
- The pre-coding split is restored: 70 tasks total, admission/completion here and voter history in Task 15; activation 60, final audit 70. All explicit dependencies point backward. Live Machine read/write remains schema 3 with v4 refused until activation; voter history validation is explicitly still pending.

Verified
- Focused scoped/ordinary-provenance tests passed. Admission corruptions now also verify decoder refusal preserves prior serialized bytes and index ownership. Codec round-trip checks include valid admission and completed-step evidence; retry tests retain successful sibling/prefix receipts and reject fence-only completion.
- make test: 1107/1107, 6.411s; test-reports/unit/20261015T185015.964421Z-dc4xk1i2/report.md. Exact staged lint passed at /tmp/gquorum-task14-restored-lint.log; full checkpoint make lint passed at /tmp/gquorum-checkpoint20-lint.log, covering module vet variants, production lint and merged unused checks.
- 480 changed handwritten Go lines, below 550. Pure validation introduces no acquired resource or concurrency; no additional functional/race/process run required here. Diff and task-number/dependency audits passed.

## Five-commit checkpoint 4 — Commits 16–20
Date: 2026-10-15. Immutable review range begins after 7b1eaa0.

- Removed optional cleanup-origin and archived-cleanup-grant branches, duplicate obligation comparison and redundant retry block count in Correction 4. Removed the obsolete test fixture wrapper while consolidating exact authority proof. No additional safe dead declaration found by references and full unused lint.
- Legacy stepConsumed/readmissionBatchConsumedLocked wrappers still serve live schema-3 callers; they now delegate to shared exact predicates. Active-based plan validation, cleanupProofsLocked and pendingCost remain reachable in the live path and must stay until their scheduled conversion/removal in Tasks 59–60. Scoped adapters contain only selected proof fields, never a synthetic Active topology.
- Request peer review of all four correction commits and admission/completion validation before the next implementation commit. Task 15 voter proof and live behavior remain outside the completed scope. Next regular checkpoint is commit 25, counting further corrections.

## Correction 7 — Preserve retirement across admission successors
Date: 2026-10-16. Commit: this entry's commit (implementation/correction commit 21).

Implemented
- Accepted thread 5's confirmed admission restore defect. Live preparation and scoped restore now share exact node/member and complete original-batch consumption checks across retained admissions. Removed redundant readmissionBatchConsumedLocked and exact-admission fast path; retirement receipts retain their original admission/operator tuple.
- Added a full retry restore regression, atomic rejection of an incomplete predecessor batch independent of cleanup validation, exact identity/batch mismatch checks and rejection of learner progress without its native effect.
- Recorded required-stop takeover in Tasks 18–19: new approvals must carry removed-but-unstopped identities and atomically rebind required stops, including cross-version/changed sequence cases, together with restore validation. Preserves origin/retirement proof and pins both chains. No global repair freeze; whole deletion still cannot bypass stops. Existing isolated validator fails closed until the coupled implementation.
- Task 26 now explicitly rebinds retry admissions while retaining original batch evidence. Task 34 counts the entire retained operation's completed grants/receipts and successful siblings. Target §4.3/5.2/6.2/12 clarified with these requirements; no public API change or live v4 activation.

Verified
- Focused retry/admission/readmission preparation regressions passed. make test: 1110/1110, 8.541s; test-reports/unit/20261015T190625.967877Z-6t0peo43/report.md.
- Exact staged lint passed: /tmp/gquorum-correction7-lint.log. Diff checks passed; changed handwritten Go remains below 550. This change shares pure validation without new resource/concurrency behavior; no full race/functional/Docker run needed here.
- All thread 5 findings accepted with current fix or explicit downstream transition/restore obligations; no remaining disagreement or user decision. Next task 15; next peer/dead-code checkpoint commit 25.

## Task 15 — Validate retained voter history before restore installation
Date: 2026-10-16. Commit: this entry's commit (implementation/correction commit 22).

Implemented
- Completed isolated v4 restore preparation with voter-switch validation. Reuses existing identity, allocation, role/index ordering and bounded cooldown-summary checks; supplies actual scoped declarations, operations, cleanup, admissions and grant identities without legacy operators or synthetic Active.
- Retained records match an exact consumed promotion/demotion action and completed progress in its validated attempt chain. A successor may acknowledge predecessor consumption but cannot relabel the record to an unconsumed attempt or reset cooldown. Completion application indexes must follow grant issuance. Entirely pruned operation chains leave valid bounded cooldown anchors; a missing attempt within a retained chain fails closed.
- Included role-effect provenance in staged-rebase refusal. Replaced the formerly pending dummy codec voter fields with valid archived records. The ancestry regression now explicitly omits v5 role effects because that case repurposes v5 as staged intent; a dedicated test rejects staged role provenance.
- Regression coverage: successful/closed-grant restore, retry lookup, original receipt with later completion, promotion/demotion, operation pruning, one removed last-holder anchor, unreferenced non-last rejection, malformed identities/versions/indexes/roles/summaries, relabeled retries, fence-only proof and atomic decoder refusal retaining prior bytes/index pointers.
- Shared the exact plan-step authority lookup with the existing live consumption helper. Target retention notes and Task 22 explicitly carry the no-reset/original-effect rule into the later transition writer. No public API change; live Machine/schema-3 behavior stays unchanged and refuses v4 until Task 60.

Verified
- Focused Scoped/VoterSwitch/ReplacementPrepare units passed. make test: 1132/1132, 8.060s; test-reports/unit/20261015T191353.626918Z-s7t2gcac/report.md.
- Exact staged lint passed at /tmp/gquorum-task15-lint.log after splitting provenance lookup and wrapping one signature. Diff checks passed; 418 changed handwritten Go lines, below 550.
- No acquired storage, new concurrency or process behavior in this task; no new functional/race/Docker run. Next task 16 needs a pre-coding size audit of planner/helper/caller conversion. Next regular peer/dead-code checkpoint is commit 25.

## Task 16 — Separate voter planning approval and proven declarations
Date: 2026-10-16. Commit: this entry's commit (implementation/correction commit 23).

Implemented
- Pre-coding scope audit found 71 public planner/shape references plus dependent voter/readmission/simulation helpers. Split the original planner task into Tasks 16–18, preserving all obligations. The plan now has 72 tasks: scopes 19, blocks 20, activation 62 and final audit 72. Updated dependent references and existing Go removal comments; preserved historical split notes. Numbered headers and backward dependencies audited.
- Added the exact C12 PlanInput value. Private voter-state, domain, identity and voter-proof helpers now read proven group/node declarations independently of Approved placement intent. No composite Document is assembled and no parallel public planner API added.
- Current whole-document entry points decompose their actual base through private convergedPlanInput. Its production references: constructPlan/topologyPlanBuilder, ValidatePlanShape and ValidateVoterSetTransition; direct voter test references also use it. Planner/shape callers move in Tasks 17–18; any remaining legacy entry adapter is removed at cutover 62. Live schema remains 3.
- Tests cover old-domain labels after approved changes, reservation eligibility based on exact approved membership, native former identities after approved removal, unknown/tombstoned convergence refusing invented identities, unchanged guarded same-domain ordering, caller input immutability and live-quorum refusal.

Verified
- Focused planner/voter/readmission units passed. make test: 1136/1136, 9.907s; test-reports/unit/20261015T192031.516222Z-yb62xg02/report.md.
- Exact staged lint passed at /tmp/gquorum-task16-lint.log after wrapping one signature. Diff check and 72-task dependency audit passed; 222 changed handwritten Go lines, below 550.
- Pure planning/value refactor has no acquired resources or new concurrency, so no additional functional/race/Docker run here. Task 18 includes focused service planning checks when public callers change. Next regular peer/dead-code checkpoint remains commit 25.

## Task 17 — Scope shape, membership simulation and readmission proof
Date: 2026-10-16. Commit: this entry's commit (implementation/correction commit 24).

Implemented
- Private plan shape, simulated native membership, founding/deletion and readmission binding now take PlanInput. Exact former-owner lookups use supplied proven declarations; approval cannot erase required deletion stops. A nonzero converged deletion checkpoint refuses refounding; an initial unknown checkpoint cannot override observed established native membership.
- Readmission uses exact proven member identity from node and meta checkpoints, keeping the greater retained monotonic member ID. Meta membership may be proven before a whole-node checkpoint advances; it must not wait for unrelated load convergence. A fully unknown identity supplies no retirement boundary. Existing independent readmission grant/receipt authorization remains mandatory.
- Removed obsolete topologyPlanBuilder.active. Kept private whole-document adapters only at current public entry points pending Task 18/cutover. New helper names satisfy the linter's capitalization rule; no public signature added beyond Task 16's planned value.
- Added approval/proof disagreement, every-former-host deletion, unknown/tombstone founding, observed native contradiction, exact-member replacement and meta-before-node proof regressions. Existing operator remaining-plan readmission test exposed the necessary meta-identity fallback and passes with it.
- Task 18 now explicitly includes an input representability check: successive failed approvals can leave a native/prepared replica absent from Approved/Converged/Desired. A single NativeState grant/receipt may not enumerate all such identities. This is a final public-planner prerequisite, not a reason to invent convergence or silently discard old effects. Consult peer on the smallest explicit retained-identity input before public wiring; target C12 has not been changed speculatively.

Verified
- Focused topology and operator suites passed. Final make test: 1139/1139, 8.859s; test-reports/unit/20261015T192822.774996Z-kb5vpob5/report.md.
- Exact staged lint passed at /tmp/gquorum-task17-lint.log. Diff checks passed; 224 changed handwritten Go lines, below 550. Earlier failed runs identified incomplete new test observations and the meta-identity fallback; the final run covers all fixes.
- Pure planning changes introduce no acquired resources/concurrency; no new functional/race/Docker run here. Public caller/service coverage remains Task 18. Next regular five-commit checkpoint is commit 25; an early design consultation does not replace that review.


## Correction 8 — Preserve historical load ownership after metadata convergence
Date: 2026-10-16. Commit: this entry's commit (implementation/correction commit 25).

Implemented
- Verified thread 6's boundary defect with a failing regression, then replaced max(node/meta member) with known nonzero node ownership and exact converged-meta fallback. The existing remaining-plan case with no node declaration still passes.
- Completed metadata convergence preserves the load retirement boundary without injecting another Prepare/Add/Wait/Promote sequence. Ordinary metadata role changes remain in the plan. Stale desired member IDs return ReplanRequired; a newer requested readmission is not confused with stale intent. Incomplete/unknown identities provide no boundary.
- Added regressions for immutable admission-boundary lookup after meta convergence, load-only replanning, input ownership, ordinary learner promotion, exact partial/complete deletion coverage, unknown/zero/same-member checkpoints and newer metadata identity. Full retained identity/immutable-plan shape wiring remains Task 18; no claim of completed public conversion.
- Closed thread 6 with no user decision. Target §5.2/§7.2 and plan C12 now specify expert RetainedGroup/RetainedReplica/RetainedProgress, explicit FoundingIssued, exact readmission coverage and retained founding proof. Chose Hosted/Cleanup/Stopped names to avoid implying consumed native removal or application retirement. Ordinary Node/Cluster API stays unchanged. Task 18 must be split before coding; Tasks 26/36 carry the coupled debt/retention obligations.
- Dead-code audit for commits 21–25: removed the obsolete provenMember/max lookup. Earlier batch changes removed redundant readmissionBatchConsumedLocked, the exact-admission fast path and topologyPlanBuilder.active. Shared authority lookups remain used by live and scoped validation. convergedPlanInput still has current public entry callers (planner, shape, voter transition, readmission) and retains explicit Task 18/62 removal obligations. Full unused lint passes; no further safe dead-code deletion found.

Verified
- Focused readmission/operator units pass. Final make test: 1151/1151, 9.003s; test-reports/unit/20261015T195128.893025Z-78lfa_1a/report.md.
- Exact staged lint passed: /tmp/gquorum-correction8-lint.log. Full checkpoint make lint passed: /tmp/gquorum-checkpoint25-lint.log. Extracted plan header validation after the first staged lint reported cognitive complexity 17; reran focused/all units and both lint gates successfully.
- Diff/fence/72-task sequence checks pass; 259 changed handwritten Go lines, below 550. This remains pure planning/validation with no acquired resources or new concurrency; no additional functional/race/Docker run. Live schema 3 still rejects v4 until Task 62.
- Commit 25 completes this five-commit batch. Next action is peer review of 9ce0454..HEAD, then evaluate/correct findings before the next implementation commit. The full architecture remains unfinished and the goal remains active.


## Correction 9 — Close commit-25 review and clarify stale member intent
Date: 2026-10-16. Commit: this entry's commit (implementation/correction commit 26).

Implemented
- Thread 7 closed after three rounds. Both reviewers ran the real service/FSM regression on c17ae5f and d6e09a3: unfinished readmission rollback and raw staging reject retired identity reuse at the allocation guard, before planning. The purported live regression was withdrawn; no duplicate-host exception is needed. Retained this regression to pin the contract.
- Stale desired metadata membership now returns VersionConflict with fresh-intent guidance. Replanning the same identity cannot repair it. Updated existing assertions and target §5.2 wording; refusal semantics stay intact. The legacy input adapter now names DeletionBase/OperatorBase and its dependence on metadata identity non-reuse until genuine scoped inputs replace it.
- Pre-coding footprint split: former Task 18 becomes Tasks 18–22 (retained values, retirement rules, metadata collector, validators, public planner/native callers). Current plan has 76 tasks; activation 66, final audit 76. Updated code removal comments. No obligation deferred outside the target; no synthetic convergence or new ordinary API.

Verified
- Focused topology/operator units and metaservice TestRollback functional checks passed. Both old/new regression probes passed during review; Claude independently reproduced them in archived commits.
- make test: 1151/1151, 11.653s; test-reports/unit/20261015T200834.762307Z-_mvxmue6/report.md. Exact staged lint passed at /tmp/gquorum-correction9-lint.log; diff checks passed. 106 changed handwritten Go lines, below 550.
- No new concurrency or persistence implementation; no additional race/Docker run. Full lint passed at commit 25 and focused staged lint covers this follow-up. Review has no remaining disagreement or user decision. Next regular peer/dead-code checkpoint is commit 30.


## Task 18 — Retained planner identities and possible founding evidence
Date: 2026-10-16. Commit: this entry's commit (implementation/correction commit 27).

Implemented
- Added the exact expert PlanInput.Retained and RetainedGroup/RetainedReplica/RetainedProgress values with Hosted/Cleanup/Stopped semantics. They carry identities and host obligations, never replica roles, convergence or effect authority.
- Observation and shape entry checks reject zero/unnamed/duplicate IDs, desired overlap, contradictory declared hosts, unknown progress and IDs beyond available current allocation counters. Exact older-convergence overlap and multiple historical IDs on one host remain representable.
- Clarified target §7.2: an older converged allocation counter cannot cap later issued identities. Current approved/desired bounds are checked here; when both omit a deleted group, the FSM collector must check full history. No synthetic declaration is introduced.
- Voter identity/proof helpers now resolve retained ReplicaKeys. Possible founding grants or any retained host state prevent refounding even without a converged declaration or remaining founding identities. Unknown versus tombstone and separate approved placement remain intact. Retirement move generation/shape/simulation coverage follows Task 19; public planner conversion remains Tasks 21–22.

Verified
- Focused topology/operator tests passed. Added twice-superseded native/prepared identity, malformed input, exact host overlap, retained/all-desired founding, proof without convergence, old-domain/pinned-learner protection, encoding and caller-copy regressions.
- make test: 1175/1175, 8.993s; test-reports/unit/20261015T201221.794267Z-tf905nhu/report.md. Staged lint passed at /tmp/gquorum-task18-lint.log after fixing one line wrap and constant-block documentation; these final edits do not change behavior.
- Diff checks pass; 334 changed handwritten Go lines, below 550. No new resource acquisition/concurrency, so no additional functional/race/Docker run. No ordinary API method, public overload or temporary test-only export. Next regular review remains commit 30.


## Task 19 — Retirement planning from exact retained progress
Date: 2026-10-16. Commit: this entry's commit (implementation/correction commit 28).

Implemented
- Former-owner enumeration combines actual converged assignments and retained exact identities, with explicit cleanup progress taking precedence. Outgoing native voter/learner operations now use ReplicaKey instead of manufacturing replica roles.
- Native/prepared Hosted owners get guarded or no-op removal plus host retirement. Stopped native owners get removal only, with exact readmission boundary validation; non-native Stopped owners get no host work. Native Cleanup contradicts removal debt and refuses; applied-index classification is the metadata collector's Task 20 obligation.
- Non-native Cleanup carries only a required stop for a same-host incoming owner or whole deletion; no second removal/retirement action. Shape and simulation enforce stop-before-prepare and every deletion stop, including debt-only plans without DeleteGroupState. Existing immutable plans remain attributable after host cleanup.
- Whole deletion uses retained identities without inventing convergence or requiring live quorum. Readmission elides only covered hosts; other retained owners still require stop and retirement. Kept the established readmission pair order after an initial unnecessary same-host ordering change broke its regression; reverted that change before final verification.
- Task 23 explicitly retains scope filtering for deletion checks: unrelated old debt cannot gate an independent group. Scoped partitions and required-stop action rebinding remain Tasks 24–25.

Verified
- Focused topology/operator tests passed, including native/prepared retirement, stopped native boundaries, cleanup-owned stop carryover, native contradictions, immutable old-plan identity, unknown-convergence deletion, every-host stop and partial/full readmission regressions.
- Final make test: 1183/1183, 9.034s; test-reports/unit/20261015T201908.856062Z-e12ose55/report.md. Exact staged lint passed at /tmp/gquorum-task19-lint.log after line-wrap/operand-order/switch-style fixes. Diff checks pass; 433 changed handwritten Go lines, below 550.
- Pure planning changes have no resource acquisition or new concurrency; no new functional/race/Docker run. Live schema/public planner adapters remain unchanged until their planned conversion. Next regular review is commit 30.


## Task 20 — Metadata-derived retained planning evidence
Date: 2026-10-16. Commit: this entry's commit (implementation/correction commit 29).

Implemented
- Added internal PlanningInput with an owned snapshot of actual Active declarations/tombstones, approved intent and caller-supplied NativeState clones. Shared pure collection uses approved historical documents, issued grants, complete readmission batches, cleanup rows and allocation history. No DeletionBase/OperatorBase composite becomes convergence.
- Possible host state comes from issued Prepare/CreateFoundingGroup grants or actual native membership, never approval alone. FoundingIssued survives desired subtraction. Exact group/node/member/replica bindings and historical allocation floors are checked; unknown/foreign native identities refuse. Native reporters are checked independently from membership.
- Consumed removal retains Cleanup ownership; exact stop/retirement or complete readmission retains Stopped. Readmission-created debt stays actionless after a later native-removal receipt; collection does not mutate durable records. A native observation behind a removal receipt is Unavailable; sufficiently recent contradictory membership is UnsafeMembership. Native Stopped requires complete exact readmission provenance.
- Current schema-3 adapter supplies genuine Active and retained authority evidence. The same collector accepts validated scoped cleanup sources; scoped adapters/apply revalidation remain Tasks 25/54, activation 66. Documented legacy composite adapter removal at Task 66. Public planner/validator callers still await Tasks 21–22; no new ordinary Node/Cluster API.

Verified
- Focused metadata collector tests passed: unknown/closed/consumed issuance, multiple supersessions, all-desired founding, no host state from intent alone, native host discovery, allocation/host/member contradictions, late removal/readmission proof, stale native state, completed deletion and owned results.
- Final make test: 1194/1194, 7.442s; test-reports/unit/20261015T202927.809964Z-wc_6l2tz/report.md. Exact staged lint passed at /tmp/gquorum-task20-lint.log after extracting per-owner receipt interpretation and style fixes. Diff checks pass; 528 changed handwritten Go lines, below 550.
- Pure metadata reads/collection introduce no acquired storage or background work; no additional functional/race/Docker run. Next task is public shape validation; its commit 30 triggers full lint/dead-code and peer review before further implementation.


## Task 21 — Public scoped shape validation and immediate approval callers
Date: 2026-10-16. Commit: this entry's commit (implementation/correction commit 30).

Implemented
- ValidatePlanShape now accepts PlanInput directly. Converted its immediate production/test callers atomically; metadata approval, repair, readmission and voter-change paths collect actual retained metadata under the FSM lock. No old-signature alias or composite-to-convergence adapter is added.
- Removed the obsolete private checkPlanShape wrapper and metadata validatePlanCleanupCoverage. Public shape validation now requires every proven/retained deletion stop and the Hosted owner's retirement action, even for empty/stop-only plans. Existing Stopped debt needs no duplicate action. Existing immutable retirement-plan tests now exercise the public input directly after cleanup.
- ReadmissionCovers/ValidateVoterSetTransition are not immediate shape callers. Kept their conversion together with RemainingPlanView/PlanRead and actual native producers in Task 22; recorded this dependency explicitly instead of inventing another document adapter. No design obligation removed.
- Corrected one direct-state forced-retirement fixture to retain its actual allocation counters; the collector guard stays strict. New public deletion-coverage regression supplements the existing independent approved/proven, retained identity, stale member, domain, copy and immutable-plan assertions.
- Commit-30 dead-code audit: removed checkPlanShape and validatePlanCleanupCoverage; former free planDeletedGroup is replaced by the retained-aware builder method. No unused code remains according to full lint. convergedPlanInput still has three production entry users (construction, readmission, voter transition), assigned to Task 22; legacy DeletionBase/OperatorBase retain Task 66 removal obligations for remaining consumers.

Verified
- Focused topology/metaengine/operator units passed. Metaservice functional Test(Plan|Rollback|ReadmissionJoin) passed (1.867s); no new process or concurrency behavior, so no additional race/Docker run here.
- make test: 1195/1195, 8.921s; test-reports/unit/20261015T203346.941216Z-tv8p_lsh/report.md. Exact staged lint passed at /tmp/gquorum-task21-lint.log after two line-wrap fixes; full checkpoint make lint passed at /tmp/gquorum-checkpoint30-lint.log. Diff checks pass; 187 changed handwritten Go lines, below 550.
- This is implementation/correction commit 30. Review range is c17ae5f..HEAD (Correction 9 and Tasks 18–21). Request the five-commit peer review and evaluate/correct its findings before another implementation commit. Task 22 needs a fresh size estimate for the coupled public/native/remaining-plan conversion. Full target remains unfinished; live schema 3 still rejects 4 until Task 66.


## Correction 10 — Keep unissued intent out of new retirement plans
Date: 2026-10-16. Commit: this entry's commit (implementation/correction commit 31).

Implemented
- Reproduced both thread-8 live approval failures through the real FSM: abandoning an unfounded group and dropping an unprepared replacement. Both previously failed with "step 1 does not name a removed replica".
- The existing legacy DeletionBase builder now filters candidate work using actual converged/issued retained evidence from PlanningInput under the same lock. Unissued approved intent creates no host work; established owners and issued-but-unacknowledged effects remain. No new composite-to-convergence adapter. Grant attribution catalogs and immutable OperatorBase identity preservation remain unchanged.
- Collector identity/allocation errors now also fail planning/execution reads closed; approval already enforced those invariants. All actual native/public construction callers are still converted in the planned Tasks 22–24.
- Closed thread 8 after peer verification. Finding 2 was withdrawn as a current defect: immutable shape validation must survive completed cleanup. Made a shared private FSM ownership check explicit for every new scoped approval handler in Task 27, with exact removal/readmission evidence and duplicate-submission versus immutable-retry tests. Target §7.2 and function documentation clarify that distinction. No new public mode or enum.
- Pre-coding estimate split former Task 22 into Tasks 22–24; 78 tasks total, scoped collection 56, activation 68, audit 78. Updated source removal references; historical progress numbering is preserved.

Verified
- make test: 1195/1195, 9.657s; test-reports/unit/20261015T204728.355858Z-k5fq9so1/report.md. Exact staged lint passes at /tmp/gquorum-correction10-lint.log after consolidating an empty-catalog check to keep cognitive complexity within 16. Diff checks pass; 150 changed handwritten Go lines including tests/comments, below 550.
- Focused topology/metaengine/operator units passed, including interrupted issued-effect cleanup and immutable retry attribution. Focused abandonment/rollback/controller-retry functional cases passed (1.571s); after the lint simplification re-ran the affected metadata units and abandonment cases successfully. Additional readmission/restart/fenced/granted-ledger/deletion functional cases passed (2.444s), /tmp/gquorum-correction10-functional.log.
- Peer independently ran both live regressions and an issued-but-unacknowledged founding probe against the staged copy. No new concurrency/process behavior; no additional race/Docker run. Next task 22; next regular five-commit review at commit 35.


## Task 22 — Explicit evidence for immutable remaining-plan validation
Date: 2026-10-16. Commit: this entry's commit (implementation/correction commit 32).

Implemented
- ReadmissionCovers and ValidateVoterSetTransition now accept PlanInput directly; converted all callers. Removed the private hasReadmissionBoundary wrapper. Existing scoped ownership/domain tests now exercise the public validator. No additional public mode or compatibility signature.
- PlanRead and RemainingPlanView carry actual converged and separately retained inputs, replacing their composite Base. The safe query collects owned metadata under the same lock; immutable original steps and grant/receipt evidence stay separate. The remaining OperatorBase consumer is the legacy controller store, removed at cutover.
- Native reads include exact proven, desired and retained candidates without duplicate identities. Immutable founding retry remains allowed only for an unknown, never-converged group and its actual founding step; a completed-deletion checkpoint cannot supply that exception.
- Remaining host checks distinguish Hosted, Cleanup and Stopped: require outstanding host work, preserve same-host/deletion stops, and leave detached retirement with existing debt. Old steps remain valid after host cleanup; no roles or convergence are reconstructed from plan steps. Target signature notes now include these auxiliary validators.

Verified
- Focused topology/metaengine/operator/metaservice units pass. Added exact retained voter transition, cleanup-owned prerequisite, candidate identity and cleaned-identity immutable-query/copy regressions.
- Functional planning/abandonment/readmission/fenced-cleanup checks passed (2.828s), /tmp/gquorum-task22-functional.log. After tightening the tombstone founding exception, controller retry/readmission cases passed again (2.661s).
- make test: 1209/1209, 9.742s; test-reports/unit/20261015T205535.715763Z-ucy72pcc/report.md. Exact staged lint passes at /tmp/gquorum-task22-lint.log. Diff checks pass; 387 changed handwritten Go lines including tests, below 550. No resource or concurrency changes; no new race/Docker run.
- Only construction still uses convergedPlanInput in production; its removal is Task 24. Actual new-plan native collection follows in Task 23. Next regular review remains commit 35.


## Task 23 — Actual native reads for new planning and founding
Date: 2026-10-16. Commit: this entry's commit (implementation/correction commit 33).

Implemented
- New-plan collection calls real ReadNative for desired groups using exact retained/proven/desired candidates, then recollects metadata against those native samples. An approval-version change refuses; the collector checks exact identities, allocations and consumed-removal contradictions. Native reads are owned copies.
- GroupProgress.Observation retains cached health/leadership separately. Existing PlanFor and founding bootstrap now derive membership exclusively from NativeState. legacyPlanningObservations temporarily adapts this to the old construction signature and is removed in Task 24; it never manufactures native proof from status.
- Only a genuinely fresh group can continue after unavailable/not-ready native state. Proven, retained, possibly-founded and deletion-checkpoint groups refuse refounding. Deleted groups do not need a native read: their host-only obligations remain in metadata. Actual founding seed handling passes its existing single-participant tests.
- The old observeGroups helper still serves the legacy scheduler's GlobalStatus; retained it after the compiler identified that remaining consumer. It no longer supplies planner membership and is converted with scheduler/observation wiring in Tasks 59–60. No new public API.

Verified
- New functional tests deliberately disagree cached and actual membership, reject foreign group/unknown reporter/zero applied index, verify owned samples, distinguish fresh from possibly-founded/retained/deleted state, and verify deletion without quorum reads.
- Focused planning/bootstrap/readmission/founding/deletion functional suite passed (4.931s), /tmp/gquorum-task23-functional.log. Final new native-collection cases passed under race (3.593s), /tmp/gquorum-task23-race.log.
- Final make test: 1209/1209, 6.897s; test-reports/unit/20261015T210210.279081Z-diwlhuwx/report.md. Exact staged lint passed at /tmp/gquorum-task23-lint.log after replacing three return values with a private paired read result. Diff checks pass; 238 changed handwritten Go lines including tests, below 550. No Docker run.
- Next Task 24 switches construction signatures and deletes their final adapter. Regular five-commit review remains commit 35.


## Task 24 — Public construction consumes explicit planning evidence
Date: 2026-10-16. Commit: this entry's commit (implementation/correction commit 34).

Implemented
- BuildPlan and BuildPlanPreferred now take PlanInput directly. Converted all immediate callers, including founding bootstrap, PlanFor and fixture/benchmark builders. Deleted production convergedPlanInput and legacyPlanningObservations; no compatibility signature or synthetic convergence remains in construction.
- NativeState exclusively supplies membership and presence. Exact group, nonzero reporter/applied index and known reporter identity are mandatory; cached observations retain separate health/leadership information. Missing native state cannot refound established or possibly-founded groups. Deletion remains host-only.
- Pure fixtures explicitly supply native membership; metadata/service fixtures collect actual converged and retained evidence through PlanningInput. Added public construction regressions for different approved/proven/native membership, independent convergence versions, retained owners, input ownership and invalid/missing native evidence. Existing live-quorum and pinned-learner regressions remain green.

Verified
- Focused topology/metaengine/metaservice/operator units passed. New native tests initially omitted the fixture schema rule, correctly failed validation, and pass after supplying SchemaVersion 1; no production guard was relaxed.
- Focused planning/bootstrap/readmission/rollback/founding functional checks passed (2.967s), /tmp/gquorum-task24-functional.log. Entire scheduling functional suite passed (0.424s), /tmp/gquorum-task24-scheduling.log.
- Final make test: 1216/1216, 6.141s; test-reports/unit/20261015T213824.840957Z-kulsmngb/report.md. Exact staged lint passes at /tmp/gquorum-task24-lint.log. Diff checks pass; 456 changed handwritten Go lines, below 550. Pure signature/construction conversion adds no concurrency; no new race/Docker run.
- Task 25 now derives affected scopes; unchanged groups still participate in this intermediate planner. Next regular review is commit 35. Live scoped-format activation remains Task 68.


## Task 25 — Derive semantic declaration and placement scopes
Date: 2026-10-16. Commit: this entry's commit (implementation/correction commit 35).

Implemented
- Added expert topology.AffectedGroups(approved, desired), reusing AffectedPlacementGroups/EvaluatePlacement. It combines exact declaration/allocation changes, effective placement and candidate eligibility/domain dependencies; node-only edits add _meta. No new Node/Cluster method, runtime owner or dependency registry.
- Exact member changes affect assigned hosts and eligible repair candidates. An unassigned ineligible node does not become a group's identity dependency. Descriptive node fields remain metadata-only; inherited placement changes do not affect a group with an unchanged override. Results are deterministic and owned, and invalid dependency evaluation conservatively conflicts.
- Before coding, split former Task 25 into semantic derivation (25), scoped planner/collection (26), and replicated scope/CAS/independence validation (27). Later tasks shift by two: 80 total, shared new-ownership check 29, scoped service wiring 58, activation 70, final audit 80. Updated source removal references; historical progress numbering stays unchanged.
- Recorded the transition safety requirement: affected-only plans must not enter legacy whole-document completion. Prepare and test isolated scoped helpers until complete block/approval/persistence wiring; no runtime mode flag, compatibility API or fabricated convergence. This commit's helper is not yet a live scope gate; wiring remains the following tasks and activation.

Verified
- Scope/placement units cover disjoint labels/allocation/roles/deletion/creation, inherited and overridden rules, node selector/type/drain/reservation changes, domain reads, participant reseats, ineligible predecessor ownership, provenance/order neutrality, copies and conservative failure.
- Final make test: 1236/1236, 11.312s; test-reports/unit/20261015T214333.022723Z-vl83stzn/report.md. Staged lint passes at /tmp/gquorum-task25-lint.log; full checkpoint make lint passes at /tmp/gquorum-checkpoint35-lint.log. Extracted candidate identity comparison to meet cognitive-complexity 16; reran final lint and units after the change. Diff checks pass; 239 changed handwritten Go lines, below 550. Pure derivation adds no I/O/concurrency; no new functional/race/Docker run.
- Commit-35 dead-code audit: production convergedPlanInput and legacyPlanningObservations are deleted in this batch; only explicit test fixtures retain the former name. No checkPlanShape or validatePlanCleanupCoverage remains. observeGroups still serves legacy scheduling, DeletionBase serves legacy placement/drain/controller reads, and OperatorBase serves the old controller store; retain until their named conversion/removal tasks (activation 70). Reused the group-ID union rather than duplicating it. Full unused lint passes.
- Five-commit review range is 0583913..HEAD: Correction 10 and Tasks 22–25. Request peer review immediately after this commit and evaluate findings before another implementation commit. Full target remains unfinished.


## Correction 11 — Separate ownership from repair candidate reads
Date: 2026-10-16. Commit: this entry's commit (implementation/correction commit 36).

Implemented
- Independently reproduced thread-9 finding 1: adding either an eligible worker or an ineligible metadata node returned every load group in AffectedGroups. The helper is not yet a live ownership gate, but wiring it would capture unrelated groups and require their quorum during admission.
- AffectedGroups now includes changed declarations/effective rules and assigned-host eligibility/state/member changes. It reuses EvaluatePlacement but filters unassigned candidates from ownership. New candidate admission/removal, identity or selector/type/domain changes no longer seize load groups; actual materialized assignments still do. Removed the overbroad candidateIdentityChanged/placementIdentityChanged helpers.
- Clarified target §5.1 FI_SCOPE_READS and made separate candidate read comparisons explicit in Task 27, including ineligible-in-both filtering. No extra public read-set API. The existing placement-report helper stays unchanged for its materialization callers. Retained-host/admission closure remains a separate exact FSM obligation.
- Closed thread 9 with accepted finding and agreed transition constraint: scoped liveness activates with complete scoped completion/persistence, never by feeding a partial plan into legacy Active=Target. No user decision or remaining disagreement.

Verified
- Focused topology units pass, including new admission/removal/materialized-assignment and unassigned selector/type/drain/reservation/domain regressions. Existing assigned-host, effective-rule, allocation, deterministic-order/copy and invalid-dependency tests pass.
- make test: 1245/1245, 10.079s; test-reports/unit/20261015T215259.721387Z-q1kcljmb/report.md. Exact staged lint passes at /tmp/gquorum-correction11-lint.log. Diff checks pass; 145 changed handwritten Go lines, below 550. Pure scope derivation changes no I/O/concurrency/live apply path; no additional functional/race/Docker run.
- Next task 26. Next regular five-commit review is commit 40. Full architecture remains unfinished; schema 3 still rejects scoped v4 until Task 70.


## Design checkpoint — State-based admission consensus
Date: 2026-10-16. No implementation/correction commit; count remains 36.

- Closed peer thread 10 after three rounds. The current PlanInput cannot express admission-dependent invalidation, and same-plan dependency sequences cannot express a later load-only approval waiting on an older metadata operation. Both reviewers recommend a mandatory exact state-based admission predicate instead of adding DependentGroups.
- Unchanged load blocks retain ownership and wait for exact current-binding admission proof. Validated metadata convergence, including founding, or exact fully consumed preparation supplies the fact. Missing proof waits; it does not fail the block. Retirement remains executable without admission.
- Independently verified the same-process handover gap: admission rows are process-keyed, while ReadmissionID embeds the operation. The proposed internal ReadmissionOperatorID preserves origin through retries and metadata supersession, alongside ReadmissionInstanceID. Current binding checks, validated consumed prefixes, immutable receipt attribution and proof retention remain mandatory.
- Created docs/brainstorms/20261016-state-based-admission-target.md with the concrete contract, snippets, task impact and regression matrix. Main target and plan link to it as pending user review; existing normative task contracts and checkboxes are unchanged. User review is requested because this changes the previously agreed supersession behavior, following their requested consensus/design/review workflow.
- Validation: checked 24 local documentation links, code-fence balance, new amendment whitespace, completed peer marker/outcome and git diff --check. No Go changes or new runtime test run. Task 26 remains first unchecked; next regular five-commit review remains commit 40. No claim of completed implementation or active persistence changes.


## Consensus adopted — Continue the active goal
Date: 2026-10-16. User authorized consensus decisions without further approval.

- Accepted the thread-10 state-based admission amendment and reconciled scope/barrier, handover, retention and audit task requirements. PlanInput and ordinary APIs gain no new field; the internal admission origin field preserves exact retirement authority. The main target links the accepted detailed amendment.
- User steering supersedes the preceding pending-review checkpoint. Continue from Task 26, with existing per-task verification/commits and peer review every five implementation/correction commits. The goal is active; implementation/correction count remains 36 until Task 26 lands.


## Task 26 — Prepare planning and collection for affected groups
Date: 2026-10-16. Commit: this entry's commit (implementation/correction commit 37).

Implemented
- Shared constructGroupPlan, plannerObservations, deletion coverage and membership simulation now accept explicit internal group work selection while retaining full approved/proven/member evidence. AffectedGroups selection plans B without reading unchanged unavailable A or metadata, avoids synthesizing another operation's admission, and preserves exact required deletion stops.
- Shared readPlanningInput collects native state/observations only for selected existing groups, preserves full metadata evidence, avoids deleted-quorum reads and rejects an approved-version change during collection. Actual founding seed membership and possible-founding refusal remain intact.
- Legacy public construction and service collection continue selecting full scope until Tasks 58/70. Scoped selections are exercised through test wrappers; lint identified and removed unused production wrappers instead of adding exports. Original-plan shape validation remains independent of a newly empty approval delta. No runtime mode flag or extra public planner/input field.
- Recorded and adopted the peer-consensus admission amendment under the user's instruction to proceed without approval. Updated normative planning/hand-over/retention tasks and committed the detailed amendment; no admission runtime behavior is activated here.

Verified
- Focused topology tests pass, including unavailable sibling, affected evidence refusal, declaration-only work, independent deletion debt, original immutable deletion coverage, exact required hosts, unrelated admission and founding seeds.
- Focused service scoped/native collection functional suite passes (2.130s), /tmp/gquorum-task26-functional.log. Initial fixture used nonexistent Group.Clone, then incorrectly expected proven owners to be duplicated in Retained; fixed the fixture to copy its replica slice and assert exact Converged evidence. Production proof checks were not weakened.
- make test passes 1254/1254 (10.795s), test-reports/unit/20261016T045048.516751Z-g2n4dppi/report.md. Final staged lint passes at /tmp/gquorum-task26-lint.log; git diff --check passes. 342 changed handwritten Go lines including tests, below 550. No new concurrency or production scoped activation; no additional race/Docker run.
- Next Task 27 derives replicated scope/CAS and independence checks. Next regular peer/dead-code checkpoint remains implementation/correction commit 40. Full target remains unfinished.
