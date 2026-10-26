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

## Task 3 — Public activation and caller migration

Date: 2026-10-26. Commit: this task commit.

Switched Group/Leadership/ProposeRetry to Submission, required unchanged logical IDs, generated distinct attempt tokens, and implemented refusal-only retry with capped overflow-safe jitter and one caller budget. Migrated public tests, scheduler/executor, cache and caller fixture together. Read retry policy stays separate. Cache emits uncertainty/applied-unavailable errors instead of TRYAGAIN; loader tests preserve that distinction. Scheduler uses the receipt to yield before a fresh read/CAS decision rather than blindly retrying or permanently failing on short refusal exhaustion.

Verified: make test PASS 2,343/2,343 in test-reports/unit/20261026T132236.365856Z-9zkh58vu; focused functional PASS 8/8 in test-reports/function/20261026T132110.836374Z-4hvuc924; all-five functional compile-only PASS in test-reports/function/20261026T131845.582684Z-6c49o5f5; clustertest compile-only PASS (no Docker execution); root/load/fault/engine, scheduler, cache and fixture vet PASS; pending lint PASS. An initial new cache unit expected a message without its Stopped code; corrected the assertion after reading the failing log and reran the suite. tests/sim contains no proposal call sites, confirmed by search; no sim work needed.

## Task 4 — Evidence and ownership regressions

Date: 2026-10-26. Commit: this task commit.

Added real receipt-path learner forwarding, unknown cancellation/snapshot, native-drop refusal, attribution collision without halting, blocked Apply ownership, metric single-counting and no-runtime-dedup coverage. Public Standalone tests preserve exact logical IDs/runs across restart and verify expired Leadership is invoked once by the safe helper. Existing pressure fixture now verifies saturation is NotSubmitted and accepted canceled calls stay Unknown at a nondefault limit. Known encoded oversize is Invalid on the receipt path and retains legacy private Overloaded classification.

Verified: make test PASS 2,343/2,343 in test-reports/unit/20261026T132830.181322Z-t21a7zww; focused functional/physical/frozen selection PASS 95/95 in test-reports/function/20261026T132707.912371Z-b9e3i9_j. Focused functional race on load and raftengine PASS (including subsequently added encoded-size and scoped-drop receipt assertions); owning vet and pending lint PASS. Frozen/protocol/storage paths have no diff. No cluster execution.

## Task 5 — Authenticated process receipt integration

Date: 2026-10-26. Commit: this task commit.

Registered the fixture-only exact-replica submission route with existing credential protection. It preserves the full returned receipt and library error separately from business rejection. Added three-node follower/learner, business-rejection and concurrent logical-ID cases plus a pure authentication/invalid-group unit. Application fingerprint dedup remains inside the existing caller FSM; no schema or production transport changes.

Verified: make test PASS 2,344/2,344 in test-reports/unit/20261026T133302.966439Z-clcv14j8; pending lint and fixture vet PASS; clustertest compile-only PASS. No Docker executed: the three new process cases are compiled and their runtime gate is explicitly pending penultimate Task 6.
