# Large load-snapshot transfer execution

Plan: `docs/plans/20261025-large-load-snapshot-transfer.md`
Target: `docs/brainstorms/20261025-large-load-snapshot-transfer-target.md`
Branch: `feat/large-snapshots-transfer`
Started: 2026-10-25

The user goal named `plans/brainstorms/20261025-large-load-snapshot-transfer.md`; that path does not exist. The unique matching committed implementation plan is the docs/plans path above. The subsequent explicit goal authorizes implementation; the earlier colleague plan review was unavailable due to provider limits and is not claimed complete. Initial checkout had only the existing untracked review transcript, which is left untouched.

## Task 1 — public contracts, policy and errors

Date: 2026-10-25. Commit: this task commit.

Implemented
- Added load/snapshot Options, protocol/service/status contracts and structured TransferError; no runtime selection, root source files or persistent changes.
- Pure option resolution validates platform/header allocation arithmetic, directional products, chunk/window and timeout sums; defaults are configurable and 64 GiB policy validates without allocation on 64-bit platforms.
- Added 42 unit cases/subtests covering defaults/idempotence, >2 GB settings, explicit budgets, malformed/overflow policy, fault/cause preservation and conservative zero values.

Verified
- go test -count=1 ./load/snapshot: pass.
- go vet ./load/snapshot: pass.
- make test: all 2,182 tests passed across 61 package reports and five application modules; 11.453 seconds.
- Report: test-reports/unit/20261025T163340.128319Z-0wxmtozx/report.md.

Notes
- No large payload, network service, Docker or runtime snapshot test was run in this contract-only task.
- Existing StateMachine/Store APIs, wire paths and immutable fixtures are untouched. Constructor capability enforcement remains deferred to Task 15.

Task 1 validation follow-up
- The first commit hook rejected strict production lint findings (named constants, switch default, grouped DTO style and a conversion whose positive guard was outside its function). No commit was made on failure.
- Fixed the code, including a local positive chunk/window guard before the unsigned conversion. Kept target DTO grouping with narrow max-public-structs exemptions, matching repository conventions.
- make lint (pending files only): pass, including unit/functional vet, production checks, revive and unused analysis.
- Re-ran make test after fixes: all 2,182 tests pass; 6.484 seconds. Report: test-reports/unit/20261025T163713.024365Z-mb3uckv2/report.md.

## Task 2 — pure facade and Node construction

Date: 2026-10-25. Commit: this task commit; prior task 821ca49.

Implemented
- Added Node.LoadSnapshots and LoadOptions.SnapshotTransfer in existing root files; only configured managed loads construct a facade. Metadata-only Nodes return nil.
- Resolved/copied snapshot policy during construction with no resolver/callback/I/O; immutable options remain available after stop.
- Added internal pure facade, observation-only fallback, closed unknown-session signal and pure typed-nil-aware RequireTransport helper. Constructor capability enforcement remains deferred to task 15.
- Coordinator methods deliberately refuse with ReplicaNotReady until tasks 6–8; root Host attachment remains deferred to task 12. These are the plan's explicit temporary seams, not completed runtime transfer behavior.

Verified
- go vet ./internal/loadsnapshot .: pass.
- go test -count=1 ./internal/loadsnapshot .: pass, including construction/cancellation/capability and stable options tests.
- make lint (pending changes only): pass after correcting stub receiver names/context helper order.
- Initial make test failed one existing event-bus timing test (2193/2194 passed); report test-reports/unit/20261025T164607.543982Z-v_apavlc/report.md.
- Corrected journal/loss observation order in that test only; go test -count=100 -parallel=1 ./events -run '^TestBus_BoundedIngressAndSubscriberLoss$': pass (1.502s). The pre-fix focused repetition also passed, confirming the observed full-suite failure was schedule-dependent rather than a consistently failing case.
- Re-run make test: all 2,194 tests across 62 package reports/five modules passed in 10.476s. Report test-reports/unit/20261025T165018.031700Z-85j2spjz/report.md.

Notes
- Event test correction is recorded with its diff in the plan's NOT PLANNED CHANGES; no bus behavior/encoding changed.
- No new root files, live application replicas, network services, Docker or large-data tests introduced/run for this task. Existing native snapshot send selection is unchanged.

## Task 3 — snapshot RPC and bounded codecs

Date: 2026-10-25. Commit: this task commit; prior task 4a92e8e.

Implemented
- Added version-1 TransferSnapshot protobuf messages/RPC without changing ordinary RaftFrame v2 or administration fields.
- Added bounded Open/Ack codecs, positive native term/index and identity validation, explicit failure details, safe duration conversions and allocation-free worst-case chunk frame sizing.
- Added malformed/native-safety, frame-boundary, 64 GiB declaration and acknowledgement units; no large payload fixtures.

Verified
- go test -count=1 ./transport/loadgrpc: pass; go vet ./transport/loadgrpc: pass.
- make test: all 2,228 tests across 62 reports/five modules pass (9.403s). Report: test-reports/unit/20261025T171829.622598Z-u8agyk72/report.md.
- make generate twice: identical SHA256 for both load generated files; only intended load protocol generated files changed. proto-check deferred until committed final verification.
- make lint (pending changes): pass after checked conversion and style fixes; git diff --check: pass.

Notes
- Codec file has a temporary unused exemption because production stream callers arrive in tasks 10–11. Remove it at cutover; the codecs already have unit coverage.
- A decoded Ack deliberately contains only the wire delivery ID/attachment; task 11 must validate and bind the immutable request peer key before reporting progress.

## Task 4 — owned native ingress and preflight

Date: 2026-10-25. Commit: this task commit; prior task 7c5a013.

Implemented
- Added exact-run monotonic bounds, off-loop envelope preparation, bounded ownership-aware submission and a guarded header-only native preflight.
- Existing beginReady remains the only Ready consumer. Exact pointer/metadata matching binds cached decode to the write job; native skips ignore unused older authority. Owned tickets bypass WAL batching delay to prevent intervening steps from sharing their capture.
- Owned payloads are not cloned by staging/Restore. Full snapshot persistence retries use the engine lifetime; ordinary WAL writes retain their five-second attempt timeout.
- Done is release-once, queued cancellation keeps ownership, failed tickets complete after all workers join, and completed tickets drop decoded buffers/callback references.

Verified
- make test: all 2,241 tests across 62 reports/five modules pass (15.767s); report test-reports/unit/20261025T172538.636558Z-moc3sasg/report.md.
- go vet ./internal/raftengine; make lint (pending files); git diff --check: pass.
- go test -tags=functional -count=1 -timeout=30s ./internal/raftengine -run '^TestSnapshotIngress_': pass.
- Focused functional race run of the same ingress cases: pass, final run 1.389s after matching the specified retry signature.
- Functional regression packages internal/raftengine, internal/metaengine, internal/metaservice and load: pass, including existing standalone/WAL/full snapshot-tail recovery tests.

Notes
- Two new fixture mistakes were corrected before completion: log setup now goes through native MsgApp instead of modifying initialized MemoryStorage, and the Restore gate lets startup Restore(nil) return. The first hung fixture run was terminated for a stack trace; a second bounded run confirmed that fixture issue. No production workaround was needed.
- Tests explicitly cover zero-term index-ahead attacks, timer/ticket/pending-Ready collision gates, higher-term obsolete headers, matching-log skip, pointer reuse, blocked write/Restore, queue cancellation and joined failure. Only one local engine per fixture; no Docker or large payloads.
