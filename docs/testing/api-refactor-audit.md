# Public API refactor acceptance audit

Status: **complete**, October 11, 2026. This is an evidence index for the
[target architecture](../brainstorms/20261011-public-api-simplification-target.md),
with source review and runtime gates completed against the final implementation.
The [validation record](api-refactor-validation.md) identifies source `355b728`,
the exact process image and the complete passing run, including fixes discovered
during review.

## Requirements and evidence

| Target requirement | Current evidence / remaining verification |
| --- | --- |
| Sections 1–2: clean Node/Cluster API and package direction | `go doc` shows Node/Cluster and explicit load types. Runtime/New(Options)/OpenMeta/OpenLoad and public meta/engine aliases are removed. Public guide and project tree updated. No private library imports in examples or test application. |
| Section 3: pure settings, one owner, asynchronous Start, readiness and shutdown | Root constructor/options tests, managed ownership tests and `node_api_test.go`, `node_lifecycle_api_test.go`. Sequential startup, canceled waits, borrowed resources, stalled callbacks, Close failure/retry and actual meta DB lock assertions. |
| Section 3: identity, limits and local diagnostics | Node settings tests, public readmission/recovery cases and limits tests. Only topology.Limits bounds cluster size; default 3/5. Parked values survive API cleanup with owned observations and typed errors. |
| Section 4: complete administration surface | Cluster facade files cover section 11's migration inventory. Existing document/mutation/session/lock/status tests retained in internal/metaservice, with process clients migrated to public values. Full final-image process suite passes. |
| Section 5: deliberate Group and Standalone ownership | `load/group_test.go` checks exact method boundary and no exposed raw fields; retained standalone snapshot/tail/dedup tests; physical baseline recovery. Managed transport tests validate endpoint, payload, delivery and lifetime identity. |
| Section 6: configurable/replaced policies | External policy embedding/contract helpers, scheduler callback tests, serialized lifecycle tests and public three-node scheduler leadership test. Different caller policy selections work across leadership transfer without a replicated agreement gate. |
| Sections 7–8: approved effects and serialized ownership | Managed action/retirement/readmission/transport tests and public fault application. Exact sender/receiver/target and approval tuple checks; retry proofs; successor protection; cancellation and shutdown joining. |
| Sections 7–8: publication before effect observation | A deterministic managed regression holds final startup authorization and refuses a live-effect attestation until publication. The later shutdown audit found a separate cancellation/recovery race; its exact regression and correction are recorded below. |
| Sections 7–8: holds and recovery evidence | Boot matrix, host holds/registry tests, public stop/restart test and durable-ledger test. Staged intent cannot start a replica; missing/foreign WAL never silently refounds; retained restart/readmission and restoration preserve exact identities. |
| Sections 7–8: retired history and incomplete founding | History-horizon, retained cleanup evidence, interrupted preparation/founding and readmission catalog tests preserve approved identities while excluding staged-only data. |
| Section 9 / Appendix A: physical schema and codec compatibility | Baseline-generated physical meta, WAL, snapshot and journal fixtures, with captured generator and immutable hashes. Current recovery preserves pins, full bucket records, native membership and committed application tail. Existing JSON proposal/result/snapshot fixtures unchanged. |
| Section 10: meta protocol and example HTTP | Meta protocol fields preserved; load.proto remains caller-owned. Strict approval and If-Match tests, legacy rejection, both approval flows, staged-version errors and CAS assertions. Compose example revalidation passes (32.272s). |
| Section 11: public/test migration | Baseline signatures and all 550 test names recorded; 64 renamed/split tests map to present symbols. `runtime-ports.md` records transferred assertions and intentional contract changes. Symbol coverage alone is not behavioral proof. |
| Section 12: complete process/fault/scale coverage | Source inventory retained and target references updated. All 108 final-image process scenarios pass, covering 3/5/7-node recovery, founding, learner, projection and fifteen-group cases. All nine performance cases pass; the separate small Compose demo also passes. |
| Appendix B: caller integration | Small meta-only example and full public Node test application use the new contracts without application-written consensus lifecycle switches. |
| Appendix C: algorithms and failure matrix | Source review completed against each algorithm and failure row; detailed mappings below. Final process and fixture gates pass with the fixes found during this review. |

## Validation state

| Gate | Evidence |
| --- | --- |
| `make test` | Passed after the scheduler/shutdown and fixture corrections (84.784s for the complete fixture). |
| `make test-unit` | Uncached pass after the fixture corrections: 15.61s, including physical compatibility. |
| `make test-unit-race` | Passed after the audit fixes in both modules; managed suite (4.599s) and fifty Close-error repetitions (96.654s) also pass under race. |
| `make lint` / cluster-tagged vet | Passed after the audit fixes. |
| `make test-fixture` | Passed uncached after the audit fixes (95.173s); corrected fixture passes all 28 tests in the final process run (80.056s). Fifty focused Close-error race repetitions also pass. |
| `make test-example` | Passed actual Compose app after the audit fixes (32.272s). |
| `make test-cluster` | Build plus complete run passed against `api-final-gates`: 108 process cases in 1417.516s, exit status 0, no skipped test cases and no remaining run-owned Docker resources. See the [validation record](api-refactor-validation.md). |
| `make test-perf` | Dedicated nine-case run passed (148.917s). All nine also pass against the final image in the complete run, with each case taking 2.12–46.20s. |
| `make bench` | Passed planner and full snapshot matrices at 3/5/7 nodes. |
| `make proto-check` | Passed after checked-in load protocol comment regeneration. |
| Baseline physical hashes / `git diff --check` | Passed. |

Deferred items stay as explicitly agreed: cluster-agreed policy declarations and
force-new recovery after permanent majority loss. Application configuration,
load wire protocol, external storage formats and transactions/outboxes remain
caller-owned. Idempotence and exact identity verification remain mandatory scope.

## Algorithm review

The source review follows the target's control flow and invariants. Private
helper names and file splitting differ where the specification permits them;
public contracts and ownership boundaries are unchanged. In particular, the
shared lifecycle runner lives in `node_policy.go`, approval forwarding is in
`cluster_documents.go`, and recovery supervision is in the managed host.

| Appendix C algorithm | Implementation and assertion evidence |
| --- | --- |
| NewNode | `node.go` / `node_options.go`: validate and copy before I/O; resolve only the selected policy defaults. `node_options_test.go` verifies pure construction, copied settings, ambiguity refusal and Limits-only capacities. |
| Node.Start | `node_lifecycle.go`: serialized one-shot start, one lifetime, meta before admission waits, registered teardown. Public sequential-start, founder gate and retained-restart cases in `tests/cluster/testapp/node*_api_test.go`. |
| Node.WaitReady | Separate readiness signal/wait context; mandatory admission, replay, session and identity rechecked after caller policy. Canceling a wait leaves the node running. Removal immediately overrides readiness. |
| Host.StartReplica | `replica.go` and `boot.go`: exact slot and hold admission, probe before factory, owned successful factory result, replay and ledger CAS, final approval check before publication. Managed ownership/boot/action tests and public lost-WAL recovery cover all paths. |
| Host.StopReplica | Exact hold before cancellation, one serial release attempt, engine join before application Close. `host_test.go` verifies Close failure/retry, late factory return and timeout ownership; canceled recovery cannot create a competing shutdown release. |
| Cluster mutation helper | `internal/metaservice/mutations.go` retains CAS/preview and the staged document when approval fails. Facade and HTTP tests preserve the committed version and reject preview plus approval. |
| Cluster.Approve | Private service retains exact current Latest, attempt reuse/retry, fresh planning and replicated validation. `forwarding_test.go`, planning/mutation tests and baseline FSM guardrails retain rejection/forwarding behavior. |
| RestoreDegraded.Schedule | `policy/restore_degraded.go`: only missing required Active voters, health/stability/hold gates, one allocated replacement, priority of blocked reasons. Default policy tests retain these decisions; node-local holds cannot be bypassed by a custom scheduler. |
| Scheduling Driver.Tick / commitDecision | `internal/scheduling/driver.go`: leader/staging/cooldown gates; copied callback inputs; fresh views, health and membership after return; ordinary repair CAS. A recovered replica, new staged intent, unknown health, lost quorum and leadership transfer during a callback all refuse new work. A canceled callback remains serialized and visible as `policy_stalled` until actual return. |
| Controller.Tick | `internal/operator/controller.go`: exact cursor/attempt and immutable step, fixed membership safety, bounded progress writes, fresh verification and final drain validation. Migrated controller tests cover leader loss, lost replies, resumption, supersession and unreachable deletion hosts. |
| Host.ApplyAction | `actions.go`, `operations.go` and `transport.go`: validate sender/receiver/target before and after group reservation and waits. Exact local proof or fresh native membership is required; no wildcard target. Action tests distinguish learner target from leader receiver and reject malformed affirmative replies. |
| Host.Retire | `retirement.go`: exact catalog, stop/join, probe and ledger check, application proof, reauthorization, quarantine, exact forget. Retirement tests preserve ledger evidence after move/cleanup failure and prevent successor deletion. |
| Host.CollectRetired | Enumerate explicit retirement records; serialize policy calls; check NotBefore, local ownership, ledger and active disk before exact store Collect. Managed retirement tests cover incomplete cleanup; WAL tests validate tokens, paths, timestamps, symlinks and reopen. |
| Host.Readmit | `readmission.go` / `readmission_meta.go`: authenticated grant, fence/join, full former approved catalog, exact application proof, retained pin/backup verification and directory sync. Readmission tests cover interrupted prepared replicas, retries, lost meta, foreign pins and borrowed store ownership. |
| DefaultLifecycle.Readiness / Retention / Removal | `policy/default_lifecycle.go` and `node_policy.go`: defaults and invalid decisions, first-bootstrap-only founder gate, seven-day/forever retention, mandatory exact fencing before optional stop/diagnostics. External embedding and serialized callback tests exercise replacements without internal imports. |
| Node.RequestStop / Stop | `node_lifecycle.go`: immediate admission refusal, one teardown coordinator, joined policies/effects/replicas before owned stores, failed release retained for explicit retry. Public tests inspect the actual bbolt lock and ensure noncooperative callbacks cannot be abandoned. |
| HTTP approval validation / response | `examples/cluster/http.go`: explicit routes, strict object/empty approval input, exact version/If-Match parsing, no query options, 1MiB/10s bounds, staged-version header and renamed operator fields. Unit tests and the Compose test exercise both approval flows and retained restart. |

## Failure-matrix review

| Target failure | Assertion location / checked outcome |
| --- | --- |
| Invalid settings | Root options and constructor tests: classified refusal before stores or workers. |
| Meta open, pin or replay failure | Meta schema/pin/replay and Node lifecycle tests: no replacement of retained evidence, joined teardown. |
| No meta quorum or unapproved join | Public sequential startup plus process admission/founder cases: protocol remains available, waits respect caller deadlines. |
| Established WAL absent | Managed boot matrix and public lost-WAL test: exact parking and durable ledger retained. |
| Foreign or unreadable load state | Host and public mismatch-restart tests: no deletion/rebinding; restored exact state clears parking. |
| Factory fails | Managed startup tests: no published handle; failed factory owns partial resources, successful late result is closed. |
| Close fails | Managed and public Close-retry tests: slot and dependent storage remain owned; explicit retry serializes release. |
| Retirement callback errors or proof mismatches | `retirement_test.go`: full tuple comparison, no ledger forget, successor preserved. |
| Callback ignores cancellation | Lifecycle runner, scheduler stall, factory and retirement tests: callback remains owned, shutdown can time out, no overlapping retry. |
| Stale scheduler decision | Callback regressions plus FSM CAS tests: refreshed membership/health/topology and replicated version guard reject stale work. |
| Changed scheduler across leadership/restart | Public three-node caller scheduler test: different installed implementations run after transfer without replicated agreement metadata; already-approved operators remain concrete plans. |
| Receiver behind cursor | Meta authorization tests: retryable only after matching every immutable retained-plan field. |
| Stale action or successor mismatch | Action, retirement and routing tests: exact process/member/group/replica tuple; no successor stop/delete. |
| Lost action reply | Operator retries and retirement-after-move tests: postcondition/proof reverified without assuming exactly-once invocation. |
| Meta leadership changes mid-effect | Controller failover tests and process operator leadership transfer: no stale progress proposal, admitted membership work joins. |
| Staging succeeds but approval fails | Private mutation and example HTTP tests: staged document/version survives the error. |
| Local hold blocks start | Managed hold tests, default/custom scheduler checks and public stop/restart: hold persists through recovery, exact explicit start clears it, approved retirement can proceed. |
| Quarantine move partly succeeds | Retirement and WAL lifecycle tests: exact moved identity is reverified on retry; application verification still runs. |
| Removal policy fails | Public removal test: engines fenced before callback, error does not reactivate, diagnostics remain local. |
| Watch/event overflow | Private meta watch and local event bus tests: bounded/coalesced delivery and explicit loss, no consensus dependence. |

## Fixes found during the final review

The scheduler originally reused the pre-callback native membership observation.
A deterministic test restored the missing voter during the callback and showed
an unnecessary new identity being approved. The driver now refreshes the
approval boundary before planning, retaining the FSM's final CAS. A separate
test verifies visibility and joined ownership of a stalled scheduler.

The public Close-error test also exposed a shutdown race after the earlier
publication fix. Recovery could finish an assignments read after lifetime
cancellation, see the now-stopped engine and start release before Node's teardown
coordinator. That consumed the first Close error and allowed a second attempt
inside the same Node.Stop. `TestCanceledRecoveryLeavesApplicationReleaseToShutdown`
reproduces that boundary deterministically. Recovery now checks cancellation and
quiescence after observing engine state, leaving release to the shutdown owner.
The original public assertions remain intact.
