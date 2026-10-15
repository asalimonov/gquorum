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
