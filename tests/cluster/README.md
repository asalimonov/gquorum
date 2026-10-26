# Process cluster tests

Run `make test-cluster` from the repository root. It builds the caller-owned
key/value test application image once, then runs Docker process tests with the
`clustertest` build tag. Non-Docker functional tests stay in their owning packages
in `*_func_test.go` files, use at most one local participant, and run with `make test-function` (`-tags=functional`).
`make test` runs fast unit checks with default build tags across the
five application modules (library, three examples and the caller-fixture/test module).
Docker must be reachable. For focused process validation, reuse the built image:

```sh
make test-cluster-run CLUSTER_ARGS='-v -run TestPool'
```

The shared runner prints the same summary table as `make test` and
`make test-function`. Each invocation keeps package logs, raw Go events, JUnit
XML, detailed Markdown results and a Markdown Gantt timeline in a fresh
`test-reports/cluster/<timestamp>/` directory. `test-reports/cluster/latest`
links to the latest completed run. The `make test-cluster` report includes the
image-build phase and its full log, including failed builds. See the
[reporting guide](../../docs/testing/tests.md#test-reports).

The console distinguishes test entry, queueing and actual execution. The timeline
sorts leaf tests and parallel subtests by execution start, excluding Go's
`t.Parallel()` queue from their bars. Its timing table retains parent coordinators
and reports entry, execution start, queue time and active time separately.
Fixture acquisition and cleanup are part of execution.

### Optional status replay

```sh
make test-cluster CLUSTER_REPLAY=1 \
  CLUSTER_ARGS='-run ^TestMetaConsensusLoadReplicationAndRetainedLeaderRecoveryThreeNodes$'
open test-reports/cluster/latest/cluster-replay.html
```

`CLUSTER_REPLAY=1` also works with `make test-cluster-run`. It samples each node's
existing authenticated runtime status and cluster status every 500 ms, with a
400 ms timeout per request. Each sweep joins its requests before the next starts;
slow sweeps skip ticks. The default is off, with no extra status requests.
Keep it off for performance comparisons because observation adds HTTP/JSON work.

Each acquired cluster has a separate `replay/capture-*.jsonl`, including multiple
leases in one test. The runner combines these into `cluster-replay.jsonl` and
creates the offline `cluster-replay.html` with a player per capture. Drop the
JSONL into the reusable viewer from `make sim-viewer` to replay it alongside
simulation logs. The report summary links to both files. `TEST_REPORTS_DIR`
also changes the capture destination.

Capture starts after `acquire` / `acquireFrom` returns and stops before lease
cleanup. It covers dynamically added nodes and retained restarts. Initial
deployment, failed acquisition, resource teardown, and pool tests calling
`Pool.Acquire` directly are outside this capture scope. Sequential subtests
sharing a parent lease appear in that parent's timeline. Parallel leases remain
separate, with Go's final test result attached after cleanup. Interrupted captures
stay incomplete and retain their raw records.

The diagram shows `_meta` leader/nonleader observations and **startup readiness**.
It does not infer follower/candidate roles, mutation readiness, process death,
network faults or a live quorum. A failed runtime read clears the previous role
to unknown and marks the endpoint unreachable. Select an evidence entry and
expand **Observation detail** to inspect session liveness, local load lifecycle,
topology versions, group status and original observation timestamps from that
reporter. Views can disagree and are not an atomic cluster snapshot.

`CLUSTER_IMAGE` selects
a different application image; that image must implement the fixture's REST and
startup contracts. Cluster cases run in parallel, with eight concurrent cases by
default (`CLUSTER_PARALLEL=8`). Each case acquires its own isolated lease. The former multi-node functional scenarios
also acquire leases from this pool; see the [migration map](../../docs/testing/cluster-migration.md).

The complete inventory uses the `CLUSTER_TIMEOUT` suite timeout (ten minutes by default). Each
performance case still has its own deadline covering deployment, workload and
cleanup and must complete in less than two minutes.

This separate Go module pins testcontainers and Docker tooling without adding
them to the library's dependency graph or the parallel unit-test loop.

The bounded pool defaults to eight deployment units and eight concurrent cooks.
Pool capacity and startup concurrency follow Go's `-parallel` setting;
`GQUORUM_TEST_POOL_SIZE` and `GQUORUM_TEST_COOK_PARALLEL` override these counts.
Both `make test-cluster` and `make test-cluster-run` pass `CLUSTER_PARALLEL` to Go.

On macOS with a local Docker Unix socket and no `TESTCONTAINERS_HOST_OVERRIDE`,
the fixture publishes HTTP and optional metadata ports on `127.0.0.1`, selecting
unused host ports randomly from 20000–29999. Docker Desktop's VM allocator cannot
see macOS sockets; its automatic ephemeral choices collided with host listeners
and outbound connections. The fixture reserves each host socket through container
creation, hands it to Docker at startup, and keeps an in-process lease until
container removal, including retained restarts. This range avoids the default
macOS ephemeral range. Testcontainers still owns containers and their bindings.
Other Docker endpoints retain Docker's automatic port allocation.

Fixture timing can be configured per isolated lease without changing library
defaults:

```go
voters := topology.DefaultVoterConfig()
voters.FailureWindow, voters.StandbyWindow = 5*time.Second, 5*time.Second
spec := matrixSpec(5)
spec.Scheduling = true
spec.MetaVoterConfig = &voters // copied; applies only to fresh metadata founding
spec.Metrics = gquorum.MetricsOptions{
    SampleInterval: 200 * time.Millisecond,
    CollectInterval: time.Second, CollectTimeout: 800 * time.Millisecond,
}
// Optional shorter elections: 100 ms tick × 5–9 randomized ticks.
spec.Tick, spec.ElectionTicks = 100*time.Millisecond, 5
```

`Spec.MetaVoterConfig` and `Spec.Metrics` are passed through the fixture's
`-meta-voter-config` and `-metrics` JSON flags to the public Node options.
`Spec.ElectionTicks` is passed through `-election-ticks` to both metadata and
load engines. Nil voter configuration, zero metrics options, and zero election
ticks retain library defaults (10 election ticks); the pool's default tick stays
100 ms. Retained metadata uses its replicated configuration, not a new founding
override. Create load groups with `GroupSpec.VoterConfig`, or change an existing
group through an approved `SetGroupVoterConfig` topology mutation.

`TestStableLearnerFiveNodesAutomaticPromotionAndRetainedReturn` uses 5-second
failure/standby windows for both groups and faster metrics collection. It retains
default elections and five-minute cooldowns, including the proof that a second
failure is replaced before the first cooldown expires. Its phase logs expose
qualification, replacement and retained recovery delays.
`TestMetaConsensusLoadReplicationAndRetainedLeaderRecoveryThreeNodes` explicitly
asserts production voter configuration and election defaults, measures leader
replacement, and checks retained identity/data recovery. With three voters and
no standby, it does not exercise the learner qualification window.

Raw `go test -tags=clustertest` also defaults to eight, or accepts `-parallel=N`.
Independent admission-phase subtests each acquire their own
lease and run in parallel. Subtests mutating a shared lease remain sequential.
Dedicated `make test-perf` and `make test-perf-run` default to `PERF_PARALLEL=1`
to measure latency without competition from other cases.
Start returns after the first isolated network is usable; later units finish
warming independently. A unit's lifecycle goroutine owns its resources through
boot, lease, cancellation, reset and final teardown. Release is generation
checked and idempotent. An expired wait context leaves cleanup owned; a later
Release joins that same cleanup.

Each lease has fresh cluster/process credentials, containers and persistent
volumes. Only its private bridge network is reused. Run, unit and generation
labels fence cleanup, including resources whose create acknowledgement was lost.
Once a Docker mutation is admitted, its bounded acknowledgement stays owned and
joined even when the caller cancels its wait. A failed create with an unknown ID
retires the unit; it cannot be safely returned to the available pool.
Reset verifies that no matching containers or volumes remain before publishing
the unit as ready. Failed cleanup retires the unit and fails the suite. Close
cancels and joins every lifecycle worker before returning; the reaper is a
fallback, not the success criterion for cleanup.

Ordinary quorum, fault, API and performance tests use three nodes. Generic five-
and seven-node repetitions are omitted. Four-node cases with named capacity requirements include:

- `TestSnapshotTransfer_RepairProtectionFourNodes`: three voters retain a live majority while a fourth stable learner proves guarded replacement after bounded snapshot protection expires.
- `TestMetaAdmissionUsesLearnerBeforeServingAndRetainsIdentityOnRestart`: learner-first admission, early-serving refusal and retained promotion.
- `TestAdmissionOperatorResumesAfterMetaLeaderElectionWithoutNewAttempt`: leadership changes during admission must retain the exact operator attempt.
- `TestMetaPinnedAdmissionRestartsBeforeAndAfterReadyWithoutAllocatingIDs`, with `pending` and `ready` cases: a crash after durable pinning must preserve the member ID and admission attempt.
- `TestAuthoritySupersessionRetiresIntermediateMetaLearnerFourNodes`: three established voters plus an admitted learner prove removal of a native member that never reached a proven group checkpoint, including prepared load cleanup and stale-action refusal.

A new larger-cluster case must explain the specific failure requiring the extra
member. Ordinary checks must not grow a node-count matrix.

Current coverage proves isolated simultaneous clusters, fresh persistent state
and retired-ID isolation on reuse, canceled partial boot, joined cleanup and
three-node consensus with worker-only load placement, acknowledged
local replication, abrupt leader death and retained process recovery. The fifteen
load-group lifecycle case verifies independent command identities, propagation to
every host, reserved meta protection and physical removal of every deleted store.
Additional cases cover non-host meta-controller effects, inert staged documents,
follower drain/activation, observed-majority drain refusal/recovery and fifteen
concurrent CAS contenders in a three-node cluster.
Learner growth uses cancelable effect barriers and reads the returning process's
own identity, membership and values. Compacted catch-up verifies a received full
snapshot and all 1,100 acknowledged commands. Dynamic admission starts a genuine
joining process, observes its meta learner phase, refuses early application
serving and verifies retained identities across process replacement.
Admission also isolates the meta leader's Raft traffic during the learner phase,
elects its replacement through the healthy majority and requires
the same execution ID, attempt and exact authorized promotion to finish. A real
one-second repair interval preserves intentional learners through several ticks;
a staged promotion remains inert until approval. Whole-group deletion waits for
an unreachable former host while its last proven declaration and retained local
state remain intact; independent groups may still converge. Multi-group shutdown uses three founders, verifies each host's membership and
acknowledged values, then requires
all concurrent process stops to finish within thirty seconds with exit code zero.
Node limits come from `topology.Limits`; routine scenarios use three nodes. Fixtures use the library default 100ms Raft tick and a five-second
activation TTL. Build the image before running cluster cases, so compilation
does not contend with the processes being measured. The image build retains Go
module and compiler caches across source changes. Source adaptations are tracked
in the [migration map](../../docs/testing/cluster-migration.md);
[validation](../../docs/testing/validation.md) retains historical results. Current
scoped process cases and final-gate requirements are indexed in the
[test guide](../../docs/testing/tests.md#scoped-operations-and-cleanup-validation).

The [test application](testapp/README.md) is a dedicated binary under `tests/cluster/testapp`; the ordinary Compose
example contains no test instrumentation. Adding a
joining process keeps its containers/volumes in the lease's generation-scoped
resource catalog, so normal release verifies their removal with the founders.

Measured performance gates and operational workloads are documented in
[performance checks](../../docs/testing/performance.md). `make test-perf` runs
health/status pressure and successful concurrent meta/load writes at three nodes; each complete case includes resource release and must
finish within two minutes. The full cluster suite also retains the fifteen-
second exact local-read, twenty-second readiness/status and three-cycle
retained-process recovery workloads from the source stability tests.

`TestDeadPeerPreservesContinuousLeadershipThreeNodes` abruptly kills
a process leading neither meta nor either load group, then requires every
survivor's native leader/term to remain unchanged throughout twenty seconds.
Two independent groups keep acknowledging commands during that window, and
the killed process must recover every acknowledged value under its retained ID.
`TestLoadIngressAndExactIndependentCommandsThreeNodes` checks
routing through every ingress, exact follower state in independent groups,
quiet-state stability, same-ID retries/conflicts and subsequent leader transfers.
All cases use independent parallel leases and remain outside the root unit suite.

WAL coalescing compares zero and five milliseconds with the same 48-command,
eight-worker workload, checking exact local values/entry counts and larger
successful persistence batches. Correlated storage-stall cases arm durable-write
stalls on all three load owners for fifteen seconds, sample independent native meta
and serving/session readiness, commit a meta document and reject unpersisted
acknowledgments. After release they wait separately for serving readiness and
actual Raft consensus, retry the same command IDs and inspect every owner after
a retained-process restart. Controls affect only the authenticated test image.

The remaining durability ports verify ten acknowledged commands recovered from
the killed process's own full snapshot, physical bbolt/WAL files and unchanged
member/replica identities, without receiving a peer snapshot. An eleventh command
proves continued serving. Auxiliary projection tests freeze caller-owned file
writers for twenty seconds while eight clients keep committing through local
WALs. Every command and local state is exact; no request is shed and WAL stalls
stay below 500ms. The real stored projections catch up automatically after
release. All these cases run separately at three nodes.

Data-loss ports verify durable voting before an exact WAL loss, persisted parking,
explicit fresh-ID reseat and recovery on the returning process. Losing the current
meta leader's bbolt store instead uses fresh admission from the healthy majority;
retained stores start their pinned Raft engine directly. The fixture keeps its
cluster-affiliation marker outside Raft storage, as caller-owned startup policy.
The library reads no application configuration or marker files.

Operator staging is observed across real reconciliation ticks before explicit
remove/repair approval. Address-free cases use the caller manifest resolver from
founding through meta and load routing, with locations enriched only in status.
Minority cases check identity-targeted persisted serve overrides, their retained
restart and their inability to bypass either Raft quorum. Forced retirement checks
actual removed membership, actor/reason audit, stale-process fencing and new IDs.
Each family runs at three nodes under the existing per-case deadline.
Only the authenticated fixture exposes store-loss and serve-override controls;
none of these routes or policies is part of the small Compose demo.

Founding cases use an optional caller startup policy in `testapp`: hold load
bootstrap and application HTTP APIs until every configured founder is observed
under its declared meta identity. Liveness and the meta protocol remain available
throughout the wait. Delayed members release the same waiting processes; a caller
supplied deadline produces a diagnosed exit with code 1. Successful startup writes
an atomic, synced caller marker. Timeout leaves that marker absent, so same-disk
restart still waits. A retained established majority bypasses this initial gate,
recovers leaders and commands, and keeps its entire topology and replica identities
with a configured peer absent. All four families use three nodes, with no
slow-test skip. The library's WaitReady contract still checks its own
exact activated meta voter and acknowledged session. These policies and tests are
entirely under `tests/cluster`, independent of the small `_meta` Compose demo.

Duplicate-name cases allocate a separate host and private volume claiming a live
nonleader's node name. The intruder must exit with code 1; every founder then reads
the identical typed warning, including the actual intruding process ID from its
startup record. The legitimate holder's running process, durable member ID,
resolver address, readiness and full topology remain unchanged. These cases use
the library's public parked registry instead of importing application UI/gossip
rendering, and run at three nodes.

Retained-voter restart ports select a meta nonleader explicitly. Every founder
keeps the same document/member/load IDs and reads an empty parked registry after
the same container/volume returns. The recovering process restores ten commands
from its own full snapshot and WAL without receiving a peer snapshot, then commits
an eleventh command. Physical durable file checks and exact native membership
replace the source application's boot-verdict log and UI assertions.

Excluded returns drain first, then wait for the exclusion's actual self-exit
(code 0) and Active convergence on the survivors. The caller selects retained
Join for the same private volume; the excluded process stays live, not ready and
without load engines while every observer reads its exact typed refusal. Operator
Add resumes that same process under fresh IDs. Down-node rotation uses the public
ReadmitNode mutation, with live/unknown health and pending-intent refusal before
staging; a committed staged version survives approval failure and is approved by
its exact version. No down process can finish its admission or delete its own
files before returning. Both families inspect the actual quarantined old bbolt
pin and exact native meta/load IDs, ledger and recovered commands; each runs at
three nodes. Constructor Join and public parked reads replace the source
application's configuration-driven boot probe and UI/gossip surface. These
controls and storage adapters remain wholly outside the small Compose demo.

The caller-deletion refusal cases run at three nodes. A caller-owned
occupancy control refuses the first destructive, fully authorized callback.
The failed execution survives meta-leader replacement and retained restart;
surviving replicas retain exact values, and the restarted host retains the
byte-identical state file and boot ledger without reopening a retired group.
The caller then permits a fresh attempt of the same desired version. Only that
attempt cleans the stores, and the retired group ID remains unavailable.

`TestPerf_FifteenGroupResourcesThreeNodes` measures the actual
Linux test processes with fifteen live load groups, including heap/RSS, CPU,
goroutines and descriptors. It checks every acknowledged command on each owner,
then approves a sixty-step deletion plan and verifies bounded joined teardown,
absent usable store paths and meta readiness. Limits and measurement conditions
are recorded in `docs/testing/performance.md`. The authenticated measurement
route exists only in the full test fixture.

The API contract cases cover three-node status and document views,
nonmutating previews, exact CAS refusals, historical page slices and exact
version reads. Lifecycle cases inspect staged drain and learner-first restore
plans before approval, prove native membership remains unchanged until apply,
then check policy caps, exact transfer, type immutability and staged-admission
cancellation. The stopped-follower cases retain unapproved placement drift
through follower and survivor restarts, verify reconciliation's staged blocker
on every observer and measure both planes' nullable health from Active policy.
Caller HTTP codes and envelopes are deliberately separate from SDK contracts.

The runtime controls use the private lease credential and the public Node/Cluster
APIs for sessions, locks, replica holds, scheduling and exact approved actions.
Topology watches stream owned updates; their readers cancel and join before
lease release. An `after=true` effect barrier holds an acknowledgement after
the local effect completed, allowing abrupt meta-leader loss to test retry safety.
The admission pin driver pauses a joining process before engine acquisition,
after `Join` and optionally `JoinReady`; its replacement uses the ordinary SDK
join path and the retained bbolt pin. These controls belong only to the test image.
Removal policies verify mandatory fencing inside the callback. A retry mode
refuses the first response and retains diagnostics after the second; the process
test stops the participant and then inspects its closed bbolt store for durable
exclusion, membership history and applied-index ordering.

The consistent-read cases use three-node pool leases and the fixture's safe read
route. `TestConsistentReadWaitsForFollowerApplyAfterConfirmationThreeNodes` drops
appends while allowing a real ReadIndex reply, then verifies waiting and local
catch-up. `TestConsistentLearnerReadLostRepliesKeepRoundsBoundedThreeNodes` uses
two voters and one learner, drops replies and floods canceled reads before
checking fresh-context recovery and queue bounds. `TestConsistentReadRejectsIsolatedFormerLeaderThreeNodes`
isolates the naturally elected load leader, verifies majority writes and stale
leader read refusal, then heals and verifies local recovery. These cases retain
exact member/replica/process scopes and do not require larger clusters.

`TestAuthorityRevocationBoundaryAndLostFenceReplyThreeNodes` holds an old demotion
with separate metadata and application leaders. Its two isolated cases verify
permitted delivery before the resource fence, refused replay after it, pending
revocation while a durable fence reply is withheld, and required replanning after
a permitted effect invalidates an immutable successor plan. Both native
membership and retained application/storage identities are asserted. These cases
enable the fixture metadata proxy so the barriers intercept real revocation RPCs.

The resource-boundary cases also withhold metadata replication to the load leader
and assert its retained older Target. The lost-reply case forces a metadata
leader election while keeping the former courier process alive and revocation
pending. Both cases later transfer application leadership and verify the old
grant/action remains fenced at the new leader with membership and data intact.

`TestAuthorityHostRevocationWaitsForExactRetainedProcessReturnThreeNodes` holds an
old stop delivery, terminates its host, and supersedes the deletion. It verifies
completed group evidence cannot discharge the unreachable host obligation or
permit successor effects. A retained restart completes fencing and cleanup;
replaying the old grant/action remains refused after physical data removal.

`TestAuthorityEarlierLearnerActionCannotUndoSameOperatorPromotionThreeNodes`
reseats a replica with a fresh ID, observes actual learner installation, then
advances the same approved operator to promotion. Replaying its earlier action
before and after promotion cannot toggle the voter back to a learner. A changed
target set using the same revision is also refused; retained values and storage
pins are checked alongside membership.

## Explicit large-snapshot qualification

Ordinary units, functional checks, performance targets and `clustertest` alone exclude large generated payloads. The additional `snapshotlarge` build tag selects `TestLargeSnapshot_`; there is no default Make dependency or checked-in snapshot blob. The generated cases request 256,000,000, 1,000,000,000 and 2,000,000,000 encoded bytes, with observed envelope calibration and explicit per-node policy. This ceiling is a test choice; production options accept larger representable sizes.

Use one unique image tag for the final ordinary suite and qualification; do not replace it during a run:

```sh
SNAPSHOT_TEST_IMAGE=gquorum-test:snapshot-20261026
make test-cluster-build CLUSTER_IMAGE="$SNAPSHOT_TEST_IMAGE"
make test-cluster-run CLUSTER_IMAGE="$SNAPSHOT_TEST_IMAGE" \
  CLUSTER_PARALLEL=1 CLUSTER_TIMEOUT=2h \
  CLUSTER_ARGS='-tags=clustertest,snapshotlarge -run ^TestLargeSnapshot_ -v'
```

The test refuses insufficient resources rather than shrinking a case. It requires Docker memory of at least twelve times the requested encoded length and available storage of at least eight times that length. Each case generates reproducible actual bytes, uses production chunks/Node-owned WAL/Restore, verifies tail/new writes/restart and a lost chunk Ack, and checks ordinary traffic on another group. Reports retain compact sizes/hashes and sampled heap/RSS/store high-water, reservations, wire chunk/window peaks and phase timing. These observations are not RSS/throughput guarantees; serialized full-buffer callbacks and resident state remain material costs.

Qualification passed on 2026-10-26 with Go 1.26.5, etcd Raft 3.6.0 and gRPC 1.79.3 on Linux/arm64 Docker 29.6.1 (39,878,758,400 bytes VM memory). All three exact encoded ceilings passed, with reconnect, ordinary traffic, persistence/Restore, tail/new writes, retained restart and zero transfer reservations after completion. Evidence: `test-reports/cluster/20261025T222708.955256Z-4acyzfy_/report.md`. Sizes above 2 GB remain unmeasured. Five-module units/race/functional/race, full lint, proto-check, the gRPC teardown race gate and all 122 ordinary cluster cases also passed. See [measured resource details](#measured-snapshot-resources) for the scope and sampling limits.

### Measured snapshot resources

Final image: `gquorum-test:snapshot-20261026-final3`, SHA256 `a48e0c77d8fb56480230e5915f1dc8555207c0af7874091b6246a6077f41300e`. The explicitly tagged ladder took 92.558 seconds including acquisition and cleanup. No other cluster suite ran concurrently.

| Encoded bytes (requested = actual) | Sampled peak heap | Sampled peak RSS | Load-store disk high-water | Creation | Transfer with injected hold/reconnect | Verify/persist/Restore wait | Retained restart | Ordinary traffic |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 256,000,000 | 1,224,085,968 | 1,911,697,408 | 260,460,001 | 0.996s | 0.428s | 1.680s | 6.784s | 2.13ms |
| 1,000,000,000 | 4,838,602,520 | 6,902,104,064 | 1,004,460,003 | 3.293s | 1.453s | 5.939s | 6.502s | 3.50ms |
| 2,000,000,000 | 9,669,169,368 | 13,639,368,704 | 2,004,460,004 | 6.218s | 2.409s | 14.704s | 11.649s | 2.34ms |

Heap/RSS/disk are sampled maxima across the sender and receiver, not their sum or a guaranteed complete allocation peak. Sampling starts after creation and the held first chunk, so it does not capture every creation allocation. Creation is measured separately; transfer includes the deliberate first-chunk hold/ordinary-traffic check and reconnect. Restore wait starts at sampled full byte verification and includes decoding and persistence, not only the application callback. Phase totals exclude acquisition/cleanup. The baseline was an empty fixture (roughly 35–38 MB RSS), not an equally large resident application.

Both directional reservation peaks equaled each requested encoded size; all counts/bytes returned to zero before restart. Each maximum chunk was 1,048,576 bytes. Observed client outstanding-window peaks were 6/5/6 against configuration eight; stats ordering can under-sample that peak, while transport tests separately assert the actual window bound. Data-frame observations were 250/958/1,912. Results demonstrate this fixture/host, with substantial full-buffer amplification; they establish no general throughput, latency or RSS guarantee.

### Post-review decoder and refusal correction qualification

The two review findings against dd10d21 were corrected without changing application/store/wire encodings. The decoder now reads the encoded slice directly, preserving strict envelope/nested-authority validation. The 4 MiB application allocation regression limits bytes/op to application size + 256 KiB. An isolated three-sample comparison on Go1.26.5/M4 Pro measured about 4,209,405–4,209,490 bytes/op for slice decode versus 20,981,805–20,981,835 for the previous buffered decoder. CPU ranges overlap (17.54–18.12ms versus 17.46–18.59ms); this is allocation evidence, not a promised CPU speedup.

A new three-node process case, `TestSnapshotTransfer_LocalOversizeRefusalIsObservable`, builds ordinary application state larger than an explicitly small 8 KiB transfer allowance. Local snapshot creation succeeds, the unissued send refusal exposes actual size/limit/count/time without a permit, both oversize/refusal events reach the journal, and an unrelated group still commits. Remote refusal dimensions, 600-route bounded cache/event pressure and reminder/reason-change rate limits have owning functional/race coverage.

The complete ordinary suite passed 123 reported cases (202.439s), report `test-reports/cluster/20261026T053118.067136Z-1t1siqzj/report.md`. The refreshed generated ladder passed all exact encoded ceilings (92.046s), report `test-reports/cluster/20261026T053440.558197Z-nmspls08/report.md`, using `gquorum-test:snapshot-review-p1-r2-20261026` image SHA256 `7387ad063385e8738845493883a9df22db5d8f2fcff69eb464197014248f54bf`.

| Encoded bytes | Sampled peak heap | Sampled peak RSS | Creation | Transfer with injected hold | Verify/persist/Restore wait | Retained restart |
|---|---:|---:|---:|---:|---:|---:|
| 256,000,000 | 1,479,471,192 | 1,583,050,752 | 0.882s | 0.428s | 1.620s | 3.750s |
| 1,000,000,000 | 5,772,420,408 | 6,802,784,256 | 3.718s | 1.387s | 6.961s | 7.213s |
| 2,000,000,000 | 11,529,869,736 | 13,557,772,288 | 9.315s | 3.009s | 17.160s | 12.944s |

Both reservation peaks still match encoded size and all counts/bytes return to zero. Observed chunks are 1 MiB and window peaks 6/6/6 against configured eight. At 2 GB, the unrelated-group probe took 2.56ms. The same sampling caveats above apply: maxima are across source/receiver processes, not a sum, sampling excludes some creation allocations, and the baseline has no large resident application. Whole-process RSS did not show a reliable reduction from the earlier ~13.6 GB observation; remaining creation/persistence/restore buffers and GC materially affect it. The removed per-decode input allocation does not establish a new RSS bound or qualification above 2 GB.
