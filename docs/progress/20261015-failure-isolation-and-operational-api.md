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
