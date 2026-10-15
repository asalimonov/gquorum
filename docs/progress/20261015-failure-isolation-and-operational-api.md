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
