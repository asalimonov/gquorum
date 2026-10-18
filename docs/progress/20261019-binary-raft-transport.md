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
