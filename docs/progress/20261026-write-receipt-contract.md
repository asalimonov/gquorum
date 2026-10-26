# Write receipt execution progress

Plan: ../plans/20261026-write-receipt-contract.md
Target: ../brainstorms/20261026-write-receipt-contract-target.md
Branch: feat/receipt-error-contract
Started: 2026-10-26

## Execution steering

User requires cluster tests only in penultimate Task 6 and Claude review of all implementation commits after final verification. Updated the plan schedule accordingly. Implementation starts from baseline eca48ad.

## Task 1 — Public receipt values and retry eligibility

Date: 2026-10-26. Commit: this task commit.

Implemented load.SubmissionDisposition, Submission, ErrResultUnavailable and CanResubmit, with pure parallel tests for proof requirements, terminal wrapped causes, unknown values and applied business/delivery outcomes. Public proposal methods remain on their legacy signatures until Task 3. Pinned the already-approved design/review documents with the implementation history.

Verified: make test PASS, 2,323/2,323 in test-reports/unit/20261026T130555.653155Z-052w5nkd; make lint PASS; go vet ./load ./fault PASS. Cluster/Compose execution remains scheduled only in Task 6.

## Task 2 — Internal engine receipt path

Date: 2026-10-26. Commit: this task commit.

Implemented Submit and exact identity/evidence conversion using existing slots, queue and buffered reply. Added explicit owner refusal/applied responses, terminal halted-engine errors, waiter-only attribution rejection, cancellation publication priority and shared metric observation. Existing metadata/read/control paths keep their result/error behavior and frozen storage encodings were untouched. No new shutdown gate or queue cleanup.

Verified: make test PASS 2,326/2,326 in test-reports/unit/20261026T130946.881053Z-hx_n36ch; focused make test-function PASS 6/6 in test-reports/function/20261026T130946.880154Z-g2rrcgax; go vet ./internal/raftengine PASS; make lint PASS after correcting line length/receiver style and documenting the deliberate Submit/submit name distinction. Pure and functional receipt tests include successful application, business rejection, pre-dispatch validation/cancellation, late publication, and actual Apply failure followed by terminal refusal.
