# Frozen metadata schema 4 fixtures

Captured deliberately on 2026-10-17 during Task 78 on `feat/failure-isolation-and-api`
(base commit `bae0fb2`, with the schema cutover and OperationID changes applied).
These are new-format fixtures, not regenerated `api-baseline` artifacts.

`capture_json.go.txt` records the one-time owning-package capture source. It uses
`scopedCodecFixture` and `collectionProposalFixture`: deterministic validated
synthetic evidence for independent group/node checkpoints, failed operations,
retained cleanup/collection, exact grants/floors, admissions, sessions, locks,
allocation tombstones, typed intents, rebase lineage and voter cooldown history.
`snapshot.json` is canonical schema 4 at index 30/term 4, Latest 6, Target 5 and
FullyConverged 1. Collection command/result files bind the exact acceptance echo
at index 31. They retain the intended admission `execution_id` field and use
`operation_id` in shared authorization records.

`capture_store.go.txt` records the one-time fresh metastore capture. `meta.bolt`
contains that snapshot, a committed GrantSession at index 31/term 4, applied
index 30 and ConfState voter 1, pin `c/n/1`, serve override 2 and load ledger
`live/n/1`. Tests copy it to temporary storage, restore and replay the committed
tail, and verify the pins, membership and unrelated records survive.

Capture sources are inert text, not a test update mode. Tests only read these
files, verify the hashes below and modify temporary copies. Investigate any
format change; do not overwrite fixtures to accommodate an implementation bug.
The older `../api-baseline` fixtures remain immutable rejection evidence.

| Artifact | SHA-256 |
| --- | --- |
| `collection-command.json` | `100c0f7f62c166d9d5a13a954165c7e0d45114604b78d1ed6f8e490a492c917a` |
| `collection-result.json` | `f4b5cc5b846699a2e2d52dd64bac3b88435f9f848af896154385ad39ca48ce6f` |
| `meta.bolt` | `dc26a383bf55f803879b43e54616e069b62f0c950e4ed311c2df0082d855281b` |
| `snapshot.json` | `be91b10e7a18c306bba1ac5230777df36b8b0f325e7d8ced91bd558de07f43ac` |
