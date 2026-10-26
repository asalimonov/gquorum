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

## Task 6 steering — Minimal public docs and examples

User requested concise external documentation, then explicitly removed waitProposalReconcile/replyProposalError and all newly added tests under examples. Removed both helpers, the two example-only unit files and added loader test cases, retaining only receipt.Result/options adaptation and ordinary errors. Cache write failures use ERR directly rather than a read-side TRYAGAIN safety inference. Existing example tests remain. Updated target §8 and the plan to reflect this overriding instruction; core receipt and fixture coverage is retained. Full functional/race suites passed for the unchanged library; example checks are refreshed after this simplification.

## Task 6 — Validation underway

Published docs kept concise per user direction; fixture README reduced from 236 to 58 lines, retaining only package map and key use/test information. Removed newly added example policy helpers and tests as requested. Final units and unit race both PASS 2,338/2,338: test-reports/unit/20261026T134926.182363Z-ij294c1c and test-reports/unit-race/20261026T134927.270283Z-689zqxu1. Full functional and functional-race suites each PASS 1,448/1,448: test-reports/function/20261026T133853.164377Z-qli5viig and test-reports/function-race/20261026T134214.748396Z-nahnsg8_. After the example simplification, directly refreshed scheduler/cache functional-race checks, all PASS. Full make lint-full PASS across all modules/build variants, including compile checks for tests/sim. Docker server 29.6.1 is available; dedicated image build started in penultimate Task 6. No cluster gate has been marked complete yet.

## Task 6 — Scheduler regression diagnosis

Scheduler Compose failed after partition/reconnect in example/20261026T140306.342034Z-eddz07y1; diagnostic rerun example/20261026T140651.017372Z-f0at66k4 failed after leader crash. Full logs confirmed a proposal wait timed out and the executor propagated it as a permanent Application.Run failure, stopping the replica. Kept the requested simple example: no receipt helper or new tests. Its existing worker now re-enters drain on transient failure; drain starts with a fresh committed-state read and task CAS guards decide the next command. Updated target §8/plan for this narrow, evidence-driven exception to purely mechanical migration. No uncertain replay was added to the library helper.

## Task 6 — User-requested intermediate commit

User requested committing all changed files, including their background architecture-document simplification and deletion of library.md. This checkpoint includes those edits with Task 6 still open. Full ordinary cluster suite passed 123/123 (cluster/20261026T135718.827563Z-v_n52v9h), focused receipts passed 3/3 (cluster/20261026T135529.050247Z-m0hu8cfs), meta Compose passed (example/20261026T140228.631074Z-jl1t8g10), and cache Compose passed (example/20261026T140652.182746Z-dqiy0ih6). Scheduler still requires verification.

The first executor fix recovered instead of stopping, but its default 30-second proposal wait plus queued work exceeded the existing process test's completion window (example/20261026T141026.513734Z-gv6ltkdu). Moved the small recovery branch into the existing drain loop to satisfy lint and gave scheduler writes an explicit three-second application call budget. Each uncertain return is followed by a committed-state read and application CAS checks; the library helper still never retries uncertainty. No test timeout increase, receipt-specific helper or new example test. Fresh targeted validation follows this checkpoint.

## Task 6 — Complete

Date: 2026-10-26. Source checkpoint: 9befd85; this commit closes validation/documentation. Scheduler Compose PASS in 111.82 seconds: example/20261026T142317.568223Z-fsm2we8j, covering transfer, executor/leader crashes, snapshots, quorum loss and live partition. Scheduler unit-race and functional-race refreshed after the final code change, PASS. Staged vet/style/unused checks PASS in 9befd85. All five-module units previously refreshed PASS 2,338/2,338 in unit/20261026T140957.482916Z-3vlc6it6; unchanged library unit-race/functionals/functional-race and full lint evidence remains as recorded above. All required cluster/Compose gates are now green. No performance/simulation/large-snapshot gate was requested or run.

Run-owned containers and named Compose volumes are gone; unrelated retained cache volumes and build cache were preserved. Diagnostic readers exited. Fixed active references to the user-deleted library.md; the compact receipt summary lives in raft.md. Updated links resolve; older historical references to absent 20261015 planning files remain historical, not new receipt documentation. git diff --check PASS. The fixture README remains 58 lines. No added example tests or removed receipt-specific example helpers survive.

## Task 7 — Architecture audit and review handoff

Audited target §§1–11 against source at 089c1d6 (Go implementation finalized in 9befd85). Public receipt fields/signatures, required exact IDs, zero-unknown semantics, terminal cause precedence and refusal-only retry match. Single buffered publication, command attribution, local applied coordinates and existing callback ownership match. Capacity settings, private metadata proposals, storage/protocol schemas and frozen fixtures are unchanged. No receipt gate/drain, uncertain retry option, duplicate suppression or result resolver exists. The core acceptance mapping remains the plan's DISP-01–10 table; #9 remote authentication/lost-envelope resolution and #12 actual result limits are explicitly deferred.

Wiring traced: Node.LoadGroup through managed publication/loadhandle, and Standalone.Group, both reach public Group.Propose → Submit → owner admission → matching Apply → reply → Submission. Leadership reaches the same path with its captured term; ProposeRetry calls CanResubmit. Cache/scheduler consume the new return type, and registerSubmissionRoutes is reached from the fixture's authenticated route registration. Existing scheduler reconciliation reads committed state before a new guarded transition. All normal modules and process variants were compiled and exercised in Task 6. No new implementation gap found; added the brief user changelog. Final audit changes are documentation only and pass git diff --check.

Requesting the user-directed Claude review of all commits since eca48ad through this audit commit. Task 7 remains open only for independently verified review findings and final disposition; do not declare overall completion before that response.
