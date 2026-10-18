# Binary Raft transport execution

Plan: docs/plans/20261019-binary-raft-transport.md
Target: docs/brainstorms/20261019-binary-raft-transport-target.md
Branch: feat/structural-refactorings-and-fuzzing

## Start
2026-10-19: No implementation or live test processes found. Preserved unrelated file hashes and staged/unstaged patches under test-reports/binary-raft.

## Task 1 — Binary schema and codec
2026-10-19. Added exact binary peer/frame schema and complete size planner/codec. Production RPC switch remains Task 2.

Verified: make test (1,915 passed, unit/20261018T205604.233863Z-q4f7d1fr), focused units, vet and functional protocol tests; generation repeated byte-identically, other schemas unchanged. Initial test used nonexistent Message.Equal; corrected to legacy protobuf semantic equality before validation. No runtime/schema deviation.

Task-boundary correction: pre-commit production golangci reported unused new codec helpers, plus operand-order and named-constant style findings. Fixed style findings; moved codec implementation into Task 2 so Task 1 contains only the schema. No hooks bypassed. Previous 1,915-unit run included the prepared codec, so rerunning schema-only state before commit.

Schema-only validation: make test passed 1,902 units (unit/20261018T210001.110763Z-y4oi1jsi), adapter functional suite and vet passed. An intermediate run discovered temporary .go backups under test-reports through go ./...; renamed them .go.txt and reran successfully.

## Task 2 — Binary delivery end to end
2026-10-19. Task 1 committed as 3359346 (1,460 changed lines). Captured three JSON baselines before runtime edits: test-reports/performance/binary-raft-before-{1,2,3}; each manifest retains source/diff, binary hashes and separate CPU/tracing phase times. Identical one-worker/615-key/GOMAXPROCS=1 settings; all captures complete.

Added native binary size planning/owned decoding and exact peer validation; switched unary/stream requests; removed JSON batch splitters and unary downgrade. Authentication now preserves cancellation classification. Fixed a stream-first-send race discovered during review: publish lastDelivery before buffering and check broken state after Send so an immediate missing-method failure cannot lose its delivery report. Added missing-method/no-fallback regression and adapted native semantic assertions; snapshot fixture deliberately spans frames.

Verified: make test (unit/20261018T210620.713543Z-76n0lybe), focused units and functional tests, both with race; missing-method regression 20 repetitions; vet both tags; make lint; generated output byte-identical, unrelated protocols unchanged. Evidence: test-reports/binary-raft/task2-*.log. No API/storage/application-codec changes; unrelated staged files preserved.

## Task 3 — Safety boundaries and complete codec benchmarks
2026-10-19. Task 2 committed as cedb3a7 (750 changed lines). Added native-kind/large-ID/opaque-byte round trips, buffer ownership, exact byte/varint/count bounds, invalid identities/versions/native endpoints and cancellation regressions. Protocol-double tests cover both ingress modes, authentication before native decode, current recipient rechecks, oversized refusal, and blocked-send cancellation/join. Put new functional cases in raft_wire_func_test.go to keep existing fixtures readable.

The new canceled-ingress regression initially failed: stream admission mapped a canceled parent to Unavailable. Both binary handlers now preserve context status before admission; functional race rerun passed. This is the bounded codec-path correction allowed by Task 3.

Verified: make test (1,961 passed), focused unit and functional race, vet both tags, make lint, and five 500ms single-CPU benchmark repetitions including full splitting/outer protobuf. Retained benchmarks and medians in test-reports/performance/binary-raft-codec. Median decode speedups 8.94–26.74x, encode 1.29–20.02x; bytes/op lower throughout. Small-frame encoding allocation counts increase 6 to 12, although allocated bytes fall; no pooling added. Benchmark startup overlapped the end of lint; repeated medians are reported, not precision/confidence claims. No end-to-end gain inferred yet.

## Task 4 — Validation in progress
2026-10-19. Task 3 committed as b5a1f44 (584 changed lines). Three binary captures completed; median throughput ~4,501/s versus ~3,949/s JSON, estimated CPU/op ~262 versus ~305 microseconds. StreamRaft JSON CPU samples are absent in every binary capture. Original five-second logs omit absolute timestamps: added an unchanged-application timestamped validation pair from an isolated JSON checkout and the current binary tree. It confirms lower interpolated CPU/op (~306 versus ~330 microseconds) but has noisy throughput and overlapping conservative bounds; both results/limitations retained, no guaranteed gain claimed. Heap lifetime totals are explicitly not allocation rates.

Initial real examples failed: cache 20261018T211834.860822Z-ig1lvgef stopped at endpoint discovery after immediate kill/start; scheduler 20261018T211834.860842Z-kri0fg1w failed on credential bind mount. Added killed-process join/readiness barriers to cache test and bundled the public scheduler demo credential. Recorded as NOT PLANNED CHANGES, without touching the five preserved unrelated staged files. Reruns pending. Meta-only example passed (20261018T211922.401136Z-pddqabyu).

Task 4 update: cache restart/loadgen test now passes (example/20261018T212038.573110Z-bonbjpqm). Scheduler passes boot and earlier lifecycle phases but timed out at final post-partition HTTP polling (20261018T212038.573121Z-h3sfa3a2), while returning-node logs show successful quorum commits. Refreshing its Docker-published ephemeral HTTP port on network reconnect; rerunning with old/new endpoint evidence. All-module units (1,961), unit race (1,961), functional (1,054), functional race (1,054), proto-check and 38 Python tests passed.

Scheduler endpoint diagnosis confirmed by rerun: network reconnect changed node3 HTTP mapping 127.0.0.1:32852 to :32853. Fixture now refreshes it; no transport/application correctness assertion weakened.

## Task 4 — Complete
2026-10-19. Cache example passed (20261018T212038.573110Z-bonbjpqm); scheduler example passed (20261018T212406.852766Z-6qi0rw53). Their owned containers, volumes and images were cleaned; original gquorum-cache nodes remained running. Final units/lint passed after example fixture fixes; generated reproducibility, all-module ordinary/race suites and Python tests passed as logged above. Added transport/architecture documentation and the measured validation record, including noisy supplemental results and allocation/timing limitations. All eight captures retain profiles, binaries, exact source evidence and interactive reports. Documentation paths and diff whitespace checked.

The user's all-tests requirement extends final validation beyond the plan's focused process coverage: full cluster suite launched using private image gquorum-test:binary-raft-b5a1f44 after all example workloads stopped. Its result belongs to Task 5's final audit; no production library changes since the passing race suites.
