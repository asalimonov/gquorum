# Stable learner implementation and validation

API naming note (2026-10-14): current code uses `VoterConfig`,
`scheduling.RepairPolicy` and `lifecycle.Hooks`. The recorded validation below
predates that rename; it is not a new test run.

Scope: [target architecture](../brainstorms/20261012-stable-learner-replicas-target.md),
on baseline `84ffa43`. Validation date: 2026-10-12. This record describes the
implementation and observed checks; the brainstorm remains the design record.

## Requirement mapping

| Target contract | Implementation | Evidence |
| --- | --- | --- |
| Per-group voter targets, default three, stable and pinned learners; preserve legacy nil-policy documents | `topology/voter_config.go`, `document.go`, `validate.go`; typed Cluster mutations in `cluster_mutations.go` and `internal/metaservice/voter_mutations.go` | Policy/clone/legacy validation units; functional policy adoption preserves intentional learner pins and host intent |
| All eligible load hosts, explicit durable host selection, total replica capacity, small founding and target shortfall | `topology/hosting.go`, metaservice hosting materialization and founding | `TestAllEligibleHostingAndCapacity`, `TestFoundingVotersSmallAndPinned`, `TestPolicyTargetShortfallAndUnsafeReduction`, functional atomic overflow and explicit-host admission; seven-host Docker capacity regression |
| Every admitted node hosts meta; learner-first admission, readiness and retained restart | Shared admission sequence validation; meta bootstrap/join/session and recovery integration | Admission validation units; learner-before-serving and admission-pin Docker cases; five-node retained-return acceptance |
| Optional surge, immutable ordering, exact guarded transfer and quorum safety | `topology/voter_plan.go`, guarded authorization/wire encodings, managed and meta membership receivers | Ordering/forged-guard units; four-node process crash after committed promotion/demotion for both surge settings; leader-regain test waits for the exact unavailable transfer target, then resumes the same attempt |
| Automatic actions never approve unrelated staged intent | `internal/scheduling/voters.go` fresh revalidation and replicated `CommitVoterChange` | Callback/admission/manual-stage/CAS/engine-run regression tables; scoped FSM tests reject unrelated document or plan changes |
| Durable completed switch records and bounded retention | `internal/metaengine/voter_switch.go`, codec, disk, restore and pruning | Exactly-once role records, lost acknowledgement recovery, historical operator pruning, allocation/identity corruption rejection and replay |
| Typed local and cluster metrics, bounded history and independent collection | Public `metrics` DTOs, `MetricsOptions`, local meters, authenticated meta RPC, optional load status sample; `internal/observe` collector/read pool | Clone/settings/ring/rate units; idle sources, apply/writer stalls, same-process engine restart, authenticated RPC, bounded run eviction, stale/out-of-order data, partial rounds, dispatch fairness, callback join and generation/cursor exhaustion tests |
| Failure and standby windows use group replication evidence | `internal/observe/evidence.go` and `windows.go`; mandatory `internal/votercheck` | 29/30 and 59/60 boundaries, hysteresis thresholds, positive-lag no-progress, Match advancement, stale owner, unknown/duplicate samples and apply-only ineligibility units; four-node alive-node replication-lag replacement |
| Cooldown survives observer failover; newly failed voter may bypass group cooldown; demoted replica remains suppressed | Replicated role records interpreted by monotonic local timers; observer epoch resets | Fake-clock cooldown/failover tests; five-node second-failure process scenario completes before the prior group cooldown expires and retains the recently demoted learner |
| Scheduler enablement cannot reuse old temporal evidence | Evidence reset occurs while holding the scheduler gate | Functional race regression holds reset open while Tick attempts entry |
| A slow native status peer cannot hide a healthy majority or escape shutdown ownership | `internal/managed/observation.go`, one independent Host-owned read pool and per-group observation gate | Functional tests reproduce cooperative timeout and noncooperative hang, then verify partial healthy results, serialized concurrent rounds, exact-source release before completion, cancellation invalidation and shutdown retry after callback return |
| Transient membership refusals remain distinguishable through application transport | Public `membership.SafetyRefusal` and the caller fixture's versioned error details | Codec units preserve typed reasons without inventing details for unknown payloads; retained-voter drain recovery passed five consecutive process runs |
| Learner/follower mutations forward through native Raft and return after origin-local apply | `internal/raftengine/forwarding.go`, proposal owner loop and authenticated load ingress | Native forwarding protocol-double tests and five-node direct learner-handle proposal; malformed/conf-change/oversize/sender validation, bounded dropped-proposal retry, timeout/cancellation, snapshot uncertainty and stopped-handle coverage |

The target's shared temporal read pool is flat across meta and load destinations.
Native membership observation has a separate Host-owned pool with the same
configured concurrency bound. It does not accumulate temporal eligibility. This
keeps status callers from consuming collector permits and gives native effects
time to execute after a partial observation. Both pools retain actual callback
ownership beyond a caller timeout.

## Process acceptance

`TestStableLearnerFiveNodesAutomaticPromotionAndRetainedReturn` uses the default
30-second failure and 60-second standby windows. It verifies three voters and two
learners in meta and a managed load group, forwards a mutation through an exact
learner handle, and waits for both standbys to qualify. After a voter stops, one
standby replaces it in each group. The former voter restarts with the same IDs
and data as a learner. The remaining standby then replaces a second failed voter
before the original group cooldown can expire; the first demoted replica remains
ineligible. Checks use observable eligibility, operator and membership barriers.

Four-node tests cover both intermediate quorum sizes, lost effect acknowledgement
followed by meta-leader crash, repeated guarded transfer after the victim regains
leadership, and a live node whose load WAL stops making replication progress.
The lag case overrides its group's windows and entry thresholds and disables the
no-progress trigger, proving replication-lag evidence independently of node death.
Seven-node tests cover the distinct default-five-replica overflow and explicit
seven-replica capacity. Ordinary fault tests remain three-node cases.

## Validation runs

All paths below are under `test-reports/`. Counts include subtests.

| Check | Result | Report |
| --- | --- | --- |
| `make test` | 524/524 passed | `unit/20261012T015039.213151Z-f9fc0tzz/report.md` |
| `make test-race` | 524/524 passed | `unit-race/20261012T014252.267437Z-68o6c_3q/report.md` |
| `make test-function` | 642/642 passed | `function/20261012T014236.736065Z-apez2uzv/report.md` |
| `make test-function-race` | 642/642 passed | `function-race/20261012T014256.024523Z-m4g8yl64/report.md` |
| `make lint` | Six module/build-tag vet commands passed | Same source revision as the preceding functional run |
| Full `make test-cluster-run` | 72/72 passed, including the five-node acceptance (246.650s) | `cluster/20261012T015122.723116Z-yayvjjtm/report.md` |
| `make test-example` | 1/1 passed | `example/20261012T013032.520882Z-6igw41cu/report.md` |
| Focused five-node acceptance including second failure | Passed, 256.190s | `cluster/20261012T011341.453072Z-2yji9cp7/report.md` |
| Focused seven-host capacity | 3/3 passed | `cluster/20261012T011540.264648Z-_imqg5x9/report.md` |
| Focused leader-regain guarded demotion | Passed | `cluster/20261012T011643.533577Z-4gnnsyr5/report.md` |
| Focused live-node replication lag | Passed | `cluster/20261012T012024.307031Z-eopxaiqq/report.md` |
| Repeated retained-voter drain recovery (`-count=5`) | 5/5 passed | `cluster/20261012T014357.299903Z-vel8wr8n/report.md` |
| Repeated excluded retained-disk readmission (`-count=5`) | 5/5 passed | `cluster/20261012T015028.833318Z-cb_zmzyw/report.md` |

The final complete Docker run passed all 72 cases in 265.274 seconds. It used image
`gquorum-test:stable-learners-recovery-20261012`, manifest list
`sha256:f0605a6ba256aed7ca63802cdcf394d8647e01086d31c6d795ea5de7ebfd1246`.
The unit, functional and race reports cover the final library and fixture source.
Only the excluded-disk Docker test's startup barrier changed afterward; that
test passed five repeated runs and the complete suite. Vet passed all six
module/build-tag commands. Compose passed before the last native load-observation
and fixture error-envelope fixes; the example is meta-only and uses neither.

Earlier full runs exposed two recovery regressions that were corrected before
this final run. Native status rounds now serialize per group, and completed
callbacks release their pool key before returning, so a concurrent read does
not hide healthy peer evidence. The caller fixture preserves structured
`membership.SafetyRefusal` in its existing reply data envelope, keeping transient
quorum loss retryable. The excluded-disk process test now waits for the observable
parked lifecycle before checking the recovered pin; an open HTTP port alone does
not prove Node has published its retained identity. Both process regressions
passed five consecutive runs after their fixes. The original failure reports
are `cluster/20261012T012951.728044Z-l7nxqf85/report.md` and
`cluster/20261012T014833.494281Z-q1oysuca/report.md`.

Protocol generation was verified with `make proto-check` using an isolated Git
index containing the proposed protocol files. Regeneration produced no difference;
the workspace index was left untouched. Physical baseline files were not
regenerated: all seven byte lengths and SHA-256 hashes match
`api-baseline-physical.json`. Existing compatibility fixtures continue to decode
and replay in the functional suite.

## Deliberate boundaries

The implemented feature does not include forced recovery after permanent majority
loss, automatic load partitioning/migration, healthy voter balancing, routing
from a node with no local replica, online capacity mutation or mixed-binary feature
negotiation. These remain the target's explicit deferred scope. Raw metrics are
volatile; durable switch records conservatively rearm cooldown after observer
failover. Old persisted topology stays in legacy policy mode until explicit
adoption. A compatible binary rollout is required before enabling the new wire
and guarded-step behavior.
