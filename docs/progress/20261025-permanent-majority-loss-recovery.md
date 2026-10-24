# Permanent-majority-loss recovery execution record

Plan: `docs/plans/20261025-permanent-majority-loss-recovery.md`
Target: `docs/brainstorms/20261024-permanent-majority-loss-recovery-target.md`
Branch: `feat/permanent-majority-loss-recovery`
Started: 2026-10-25
Review cadence: completed tasks/commits 5, 10, 15, then task 16 plus final verification.

## Baseline — before Task 1 Go changes

- Source: 88833e8 (Go code unchanged from 2684202).
- `make bench BENCH_TIME=1s`: PASS, planner and metadata snapshot matrices.
- Go 1.26.5, darwin/arm64, Apple M4 Pro (14 logical CPUs in benchmark output).
- Raw log: `test-reports/recovery-baseline/88833e8/bench.log`.

## Task 1 — Approved contracts and pure configuration helpers

Date: 2026-10-25. Commit: included with this task's implementation commit.

Implemented
- Public recovery value aliases, Refusal with cause preservation, and root
  NewRecovery/Inspect/Approve/Run/Cancel/Status/List/RecoveryStartOptions signatures.
- Pure source configuration validation/copying, Node-owned-store restriction,
  resource defaults/overflow checks and explicit temporary I/O refusals.
- Full local/import/seed models, strict bounded duplicate/unknown-key JSON decoding
  and owned-copy helpers. Metadata FSM and existing persisted formats are untouched.
- Optional load.ReplicaSpec.Recovery input; ordinary factory contract unchanged.
- Units cover no acquisitions/callbacks, ignored invalid old runtime options,
  configuration/field copies, mode/phase checks, ID/resource validation, exact JSON
  integers, ambiguous records and public caller-side signature compilation.

Verified
- Baseline make bench BENCH_TIME=1s: PASS (captured before Go edits; see above).
- go test ./internal/recoverymodel ./recovery . ./load: PASS.
- go vet ./recovery ./internal/recoverymodel . ./load: PASS.
- make test: PASS, 1,983 tests across 51 reports/all five application modules,
  final rerun 6.913s. Report: test-reports/unit/20261024T231414.691843Z-pisn1q2w/report.md.
- git diff --check: PASS. No frozen fixture or public Node/storage interface change.

Notes
- I/O methods intentionally refuse implementation_pending per Task 1. Real source
  inspection, journal semantics and runtime reconstruction belong to later tasks.
- Added split model/clone files to keep the full schemas and ownership helpers
  readable; these are the same planned records, not a new API or format.

- Initial commit hook caught import grouping, line length and helper complexity;
  refactored pure validators/JSON scanning and fixed formatting/comments before
  committing. Required production-only golangci check now reports zero issues.
- A direct golangci invocation initially included test rules that the repository
  wrapper intentionally scopes to production; final checks use the repository's
  production/test analyzer split. No unrelated baseline tests were rewritten.
