# Public API refactor acceptance audit

Status: **in progress**. This is an evidence index for the
[target architecture](../brainstorms/20261011-public-api-simplification-target.md),
not a declaration that the goal is complete. Full process validation must use
an image containing the final implementation, including any fixes discovered
by the gates below.

## Requirements and evidence

| Target requirement | Current evidence / remaining verification |
| --- | --- |
| Sections 1–2: clean Node/Cluster API and package direction | `go doc` shows Node/Cluster and explicit load types. Runtime/New(Options)/OpenMeta/OpenLoad and public meta/engine aliases are removed. Public guide and project tree updated. No private library imports in examples or test application. |
| Section 3: pure settings, one owner, asynchronous Start, readiness and shutdown | Root constructor/options tests, managed ownership tests and `node_api_test.go`, `node_lifecycle_api_test.go`. Sequential startup, canceled waits, borrowed resources, stalled callbacks, Close failure/retry and actual meta DB lock assertions. |
| Section 3: identity, limits and local diagnostics | Node settings tests, public readmission/recovery cases and limits tests. Only topology.Limits bounds cluster size; default 3/5. Parked values survive API cleanup with owned observations and typed errors. |
| Section 4: complete administration surface | Cluster facade files cover section 11's migration inventory. Existing document/mutation/session/lock/status tests retained in internal/metaservice, with process clients migrated to public values. Full process acceptance pending. |
| Section 5: deliberate Group and Standalone ownership | `load/group_test.go` checks exact method boundary and no exposed raw fields; retained standalone snapshot/tail/dedup tests; physical baseline recovery. Managed transport tests validate endpoint, payload, delivery and lifetime identity. |
| Section 6: configurable/replaced policies | External policy embedding/contract helpers, scheduler callback tests, serialized lifecycle tests and public three-node scheduler leadership test. Different caller policy selections work across leadership transfer without a replicated agreement gate. |
| Sections 7–8: approved effects and serialized ownership | Managed action/retirement/readmission/transport tests and public fault application. Exact sender/receiver/target and approval tuple checks; retry proofs; successor protection; cancellation and shutdown joining. |
| Sections 7–8: publication before effect observation | New deterministic managed regression holds final startup authorization and refuses a live-effect attestation until publication. A formerly intermittent public Close-failure case passes twenty consecutive race runs after the fix. Full fixture revalidation passes; final-image process run pending. |
| Sections 7–8: holds and recovery evidence | Boot matrix, host holds/registry tests, public stop/restart test and durable-ledger test. Staged intent cannot start a replica; missing/foreign WAL never silently refounds; retained restart/readmission and restoration preserve exact identities. |
| Sections 7–8: retired history and incomplete founding | History-horizon, retained cleanup evidence, interrupted preparation/founding and readmission catalog tests preserve approved identities while excluding staged-only data. |
| Section 9 / Appendix A: physical schema and codec compatibility | Baseline-generated physical meta, WAL, snapshot and journal fixtures, with captured generator and immutable hashes. Current recovery preserves pins, full bucket records, native membership and committed application tail. Existing JSON proposal/result/snapshot fixtures unchanged. |
| Section 10: meta protocol and example HTTP | Meta protocol fields preserved; load.proto remains caller-owned. Strict approval and If-Match tests, legacy rejection, both approval flows, staged-version errors and CAS assertions. Compose example revalidation passes (29.906s). |
| Section 11: public/test migration | Baseline signatures and all 550 test names recorded; 64 renamed/split tests map to present symbols. `runtime-ports.md` records transferred assertions and intentional contract changes. Symbol coverage alone is not behavioral proof. |
| Section 12: complete process/fault/scale coverage | Source inventory retained and target references updated. Selected 3/5/7-node recovery, founding, learner, projection and fifteen-group tests pass. Complete final-image process run pending; all nine performance cases pass; a small demo smoke test cannot substitute. |
| Appendix B: caller integration | Small meta-only example and full public Node test application use the new contracts without application-written consensus lifecycle switches. |
| Appendix C: algorithms and failure matrix | Tests and code paths listed above cover the main contracts. Final source review against every pseudocode/failure row remains required after the complete gates. |

## Validation state

| Gate | Evidence |
| --- | --- |
| `make test` | Passed after publication fix (85.237s for full fixture). |
| `make test-unit` | Uncached pass after publication fix: 12.10s, including physical compatibility. |
| `make test-unit-race` | Passed after publication fix in both modules; managed suite and twenty Close-error repetitions also pass under race. |
| `make lint` / cluster-tagged vet | Passed after publication fix. |
| `make test-fixture` | Passed uncached after publication fix (83.332s); twenty focused Close-error race repetitions also pass. |
| `make test-example` | Passed actual Compose app after publication fix (29.906s). |
| `make test-cluster` | Final-image run in progress with gquorum-test:api-refactor-validation; earlier image run is separate evidence. |
| `make test-perf` | Passed all nine cases against publication-fix image (147.242s total; each case 2.53–42.30s). |
| `make bench` | Passed planner and full snapshot matrices at 3/5/7 nodes. |
| `make proto-check` | Passed after checked-in load protocol comment regeneration. |
| Baseline physical hashes / `git diff --check` | Passed. |

Deferred items stay as explicitly agreed: cluster-agreed policy declarations and
force-new recovery after permanent majority loss. Application configuration,
load wire protocol, external storage formats and transactions/outboxes remain
caller-owned. Idempotence and exact identity verification remain mandatory scope.
