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

## Task 3 — Atomic planner and approval activation
Date: 2026-10-26. Previous task commit: b7279ba. Commit: this task's commit.

Implemented
- New public-service approvals select internal mode 1; legacy commands preserve
  their original semantics. Planner emits detached stops for deletion.
- Atomic approval retains exact cleanup and completes nil checkpoints without
  physical receipts. Same-host replacement safety and independent blocks remain.
- Existing readmission/debt retains original provenance, including complete
  actionless batches. Authority conflicts use actual bindings for new deletion.
- Regressions cover inherited/pending/complete readmission, zero-issued and
  interrupted creation, atomic refusal, replay and pending sibling execution.

Verified
- Focused topology/metaengine/operator regressions and vet passed.
- make test: 2,349/2,349 PASS, 11.585s; report unit/20261026T141246.822117Z-aax6k2vj.
- Focused functional: 10/10 PASS; report function/20261026T141247.984946Z-r1lj1h0n.
- Final metadata unit rerun, first-convergence regressions, make lint and diff check passed.
- Functional deletion planning does not read deleted native quorum. Interrupted
  founding completes logically before physical stop; ledger tests consume actual
  deferred Stop receipts before retirement authorization.

Findings and corrections
- Old ledger fixture assumed required deletion stops; adapted actual grant/effect
  flow while retaining successor and foreign-proof refusal assertions.
- Complete readmission debt must retain batch origin rather than new operation ID.
- Conservative inherited Stop entries must not rebind existing debt or advance
  its readmission floor. A regression exposed and now prevents that fence.
- Target §9.15e documents the scoped authority correction; old command behavior
  stays explicit. Helper extraction satisfies lint without public API changes.

## Task 4 — Persistence, replay and local lifecycle verification
Date: 2026-10-26. Previous task commit: 12ce6f8. Commit: this task's commit.

Implemented and verified
- Captured three additional historical snapshot/command/result fixtures from
  unchanged revision 8e57798. New Machine.Restore/Apply reproduces each complete
  original result byte-for-byte, including inherited debt and zero-step pending
  readmission; legacy failure/retry retains immutable plans and pending deletion.
- New marker replay, pruning/restore with unstopped debt, successful deletion
  sibling retry, missing/closed/partial readmission, actual whole-batch settlement,
  late duplicate/conflicting effect receipt tests passed.
- Actual local cleanup covers current/prepared/held replicas. Startup recapture
  now explicitly tests a retired-group refusal before handle publication.
- Existing functional cases cover bounded fence/cleanup alternation, late factory,
  failed Close, canceled waiters, joined replay, exact successor refusal, memory
  retirement and collection transaction/report ordering. Existing frozen physical
  baseline paths have no diff from 8e57798.

Validation
- make test: 2,361/2,361 PASS (9.729s), unit/20261026T142032.190219Z-7brg5tdw.
- Expanded focused functional suite: 256/256 PASS (39.183s),
  function/20261026T142104.423749Z-t3_jye0p; includes all Task 4 required patterns
  plus delayed factory/Close/cancellation/retirement regressions.
- Added terminal receipt and historical retry tests passed after the full unit run.
- make lint, owning-package vet, diff check and unchanged frozen paths passed.

Findings
- Initial functional run failed only the cleanup benchmark fixture's legacy-mode
  approval of a new planner shape; fixed with mode 1 and actual Stop receipt.
- A malformed missing-readmission test initially deleted evidence still referenced
  by other completed debt. The missing-batch case now uses a valid unissued-batch
  source; inherited closed/partial batch cases retain their original evidence.
- The conflict test initially checked only Go errors, overlooking the FSM
  refusal result. Corrected it to check Accepted as well; the foreign replica
  target refuses without changing retained evidence.
- No production correction or public/schema change was needed in this task.

## Task 5 — Three-node process regressions and fixture observations
Date: 2026-10-26. Previous task commit: 00b6cdd. Commit: this task's commit.

Implemented
- DeletionWithoutFormerHost requires logical success before restart, exact pending
  debt, independent traffic, retained volume and eventual physical cleanup.
- DeletionWithoutLoadQuorum withholds real group Raft traffic while metadata and
  an independent group remain available; local Stop barriers preserve proof checks.
- DeletionSnapshotCatchup configures only the existing public metadata snapshot
  threshold through fixture wiring. It requires an accepted inbound boundary,
  same-process applied progress, retained disk and zero transient load factories.
- DeletionDuringReadmission holds the metadata receipt while forwarding load
  receipts through the authenticated proxy, then verifies logical deletion and
  exact retirement after release. The singleton control binds exact admission
  and process incarnation. Existing two library listeners remain unchanged.

Verified
- make test: 2,364/2,364 PASS (9.749s), unit/20261026T143205.563292Z-x0q2n2e4.
- Fixture snapshot/readmission-control units passed. Local fixture functional
  selection ReadFault|Meta|Deletion|Snapshot passed (4.398s).
- Fixture vet and make lint passed; process suite compiled using clustertest,
  -run '^$' and -exec=true. No Docker runtime result is claimed in this task.
- All four process cases are parallel with isolated leases and retained roots.

Scope correction
- User explicitly removed feature documentation changes from Tasks 6 and 7,
  then retained a brief changelog as the exception. Plan now records this;
  full penultimate cluster validation and final implementation audit still apply.

## Task 6 — Validation attempts (in progress)
Date: 2026-10-26. Previous task commit: 1ac9b61.

- make lint-full passed across all modules/tags before Docker execution.
- First focused process run (cluster/20261026T143522.875011Z-0os5n32h)
  reached logical completion in all four cases, then timed out on CollectedAt:
  the fixture retention default disables physical collection. Explicit immediate
  retention fixed the setup; all four passed in 14.366s in
  cluster/20261026T143808.226698Z-8x0mu2ph.
- First full process suite (cluster/20261026T143848.780208Z-xsy043rz)
  passed 121/126. Three fixtures expected deletion stop refusal to fail its
  operation; adapted required retry/fence scenarios to replica replacement and
  deletion refusal to honest asynchronous debt. Resource teardown now waits for
  physical retirement; restored-quorum read assertion retries transient readiness.
- All five failures passed their focused rerun (61.008s),
  cluster/20261026T144849.790855Z-iqh1zdkx.
- Indexed exact deletion action/debt bindings in revocation and inherited-stop
  validation to remove repeated full-debt scans. Metadata units, vet and pending
  lint pass; historical captured replay snapshots remain byte-identical.
- Final rebuilt image for the second full run:
  sha256:dc7b3cb5b64b2f457518654bb2de6ecfd64a958a170827165edc79f61dca4418.
  Full suite is running in cluster/20261026T145139.617382Z-7uv1hte3.
- No README, AGENTS, architecture document or public API comment was updated,
  following the user's documentation scope correction.

## Task 6 — Penultimate cluster validation complete
Date: 2026-10-26. Commit: this task's commit.

- Final full cluster run: 126/126 PASS, 198.906s,
  cluster/20261026T145139.617382Z-7uv1hte3/report.md.
- Final image: sha256:dc7b3cb5b64b2f457518654bb2de6ecfd64a958a170827165edc79f61dca4418.
  No shared image was replaced during an active run.
- All four deletion scenarios and all five initially failing cases passed again
  within that full run, including exact state-file preservation in quarantine.
- Unit suite after final production changes: 2,364/2,364 PASS (13.185s),
  unit/20261026T145140.799701Z-_ik3qrum/report.md.
- Metadata package unit/vet and targeted service/operator functional regressions
  passed. Full lint passed before Docker; pending lint passed after corrections.
- Docker queries for label gquorum.test.run returned no containers, volumes or
  networks after completion; no cluster.test/go cluster test workers remained.
- git diff --check passed. No product documentation was changed.
