# Execution progress: group deletion with unreachable former owners

Plan: ../plans/20261026-group-deletion-unreachable-owner.md
Target: ../brainstorms/20261026-group-deletion-unreachable-owner-target.md
Branch: feat/group-deletition-wo-node
Date: 2026-10-26

## Run orientation

Source baseline: b91746c. No implementation tasks were previously complete.
The user's existing deletion of the earlier snapshot-transfer target is unrelated.
The user directed cluster runs to Task 6, the penultimate task; the plan now
records that scheduling override. Task 7 remains the final architecture/wiring audit.

## Task 1 — Plan validation support
Date: 2026-10-26. Commit: this task's commit.

Implemented
- Accepted exact cleanup partitions and inherited deletion debt in plan/remaining validation.
- Kept required same-host replacement stops and hosted-owner identity/order checks.
- Added valid legacy/deferred partition and missing/foreign/reordered effect regressions.
- Kept generator, shared required-stop predicate and approval semantics unchanged.

Verified
- Focused topology/operator tests passed.
- go vet ./topology ./internal/operator passed.
- make lint passed.
- make test passed: 2,323/2,323 tests across five modules in 12.915 seconds.
- Report: test-reports/unit/20261026T130516.077623Z-9xhac48t/report.md.
- Existing generator-partition tests passed, proving activation has not moved early.

Notes
- User execution instruction moves all cluster runs to Task 6; final Task 7 stays audit/non-cluster.
- Unrelated earlier snapshot-target deletion remains outside this commit.

## Task 2 — Metadata coverage and physical authority
Date: 2026-10-26. Previous task commit: 31b52b8. Commit: this task's commit.

Implemented
- Optional internal DeletionSemantics command marker with zero/absent legacy
  encoding, known-value decoding and temporary mode-1 activation refusal.
- Exact source-owner/debt coverage, used for restoring completed logical
  tombstones and ready for approval activation in Task 3.
- Actual host-stop requirement for native authority settlement, detached deletion
  Stop validation and historical-member readmission dispatch deferral.
- Actual ready/issue/record tests after terminal operation, malformed debt
  refusal, partial-batch rejection and independent unmarked command bytes.

Verified
- Focused metadata deletion/cleanup/authority/frozen tests passed.
- go vet ./internal/metaengine and make lint passed.
- make test: 2,342/2,342 tests passed in 11.993 seconds.
- Report: test-reports/unit/20261026T132448.610357Z-il0ggigv/report.md.
- Existing frozen round trips and legacy pending states still pass.

Findings and adjustments
- The missing-debt corruption test first exposed lost former-owner evidence after
  a nil checkpoint. Retained plan source declarations now recover that evidence.
- An initial broad restore relaxation incorrectly accepted a legacy pending
  readmission with a fenced batch; new detached coverage is used only for
  completed tombstones, preserving the legacy pending rule.
- Moved enum to deletion_semantics.go for the public-declaration lint limit.
- Reused the coverage helper in real restore validation and split validation
  predicates to satisfy unused/complexity lint. Target and plan reflect this.
