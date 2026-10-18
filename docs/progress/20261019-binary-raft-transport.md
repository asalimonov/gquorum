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
