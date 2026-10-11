# Performance and operational checks

Run `make test-perf` to build the test application and execute the nine measured
cluster cases. `make test-perf-run` reuses the existing image. The full
`make test-cluster` includes these cases and the separate stability cases.
Run `make bench` for planner and full meta FSM snapshot benchmarks; `BENCH_TIME`
controls the sampling interval, defaulting to 200 ms.

Every performance and stability case is a separate three-, five- or seven-node
test. Its 80-second work deadline starts before deployment. Generation cleanup
uses the bounded pool release, and a final elapsed-time assertion includes
setup, correctness verification and release and requires less than two minutes.
Docker image compilation and suite-level network warmup are outside that case
measurement. Cases remain sequential; only each workload's requests run
concurrently. These gates apply to the specified fixtures, rather than claiming
unmeasured arbitrary-size throughput.

| Workload | Required gate |
| --- | --- |
| Ten clients, 50 readiness requests each, plus 50 real status reads | All 500 requests succeed with exact serving node/version; stable declared node count; at least 100 health operations/s and p95 below 500 ms. Periodic status reads are included in operation latency and elapsed time. |
| Fifteen concurrent meta edits through distributed ingress nodes | Every writer commits using typed CAS retries; distinct contiguous committed versions; all values survive on every local document and approved Active; at least five commits/s and p95 below three seconds, including client retry time. |
| Fifteen concurrent unique load commands through distributed ingress nodes | Every command is acknowledged; all values exist on each owning replica's own FSM under its exact replica identity; at least five commits/s and p95 below three seconds. |
| Meta/load convergence after acknowledged concurrent writes | Less than ten seconds for all applicable replicas, with no tolerated lost command. |
| Fifteen-second sustained reads of ten acknowledged keys | Each tested node owns the FSM and returns exact local node/value/index/consistency evidence; zero errors/mismatches; at least 100 reads per node. |
| Twenty-second readiness and status reliability | Stable active version, live serving sessions and every declared member alive; zero errors/mismatches; at least 20 samples per node. |
| Three retained-disk follower stop/restart cycles | Every survivor observes departure; retained majority commits ten commands per cycle; recovering process contains all accumulated acknowledged commands, preserves meta/load IDs, has a new process ID and produces no topology-version churn. |
| Forty-eight commands with eight workers, WAL wait zero versus five milliseconds | Both arms acknowledge every command; each owning FSM has the same exact values and advances by exactly 48 applied entries; both persist exactly 144 replica entries, while five milliseconds increases mean entries per durable write. |
| Fifteen-second correlated load-store stall | Every load store is armed under its exact replica identity; at least one real durable writer times out and retries. Meta leader/term stay fixed, a new meta document commits, serving/session readiness remains available, and no unpersisted command is acknowledged. Typed availability refusals recover without a restart; same-ID retries yield exact state on all owners and survive a subsequent retained-process restart. |
| Ten-command recovery from the process's own durable store | Before/after kill, identity/state.bolt/WAL/snapshot/meta paths exist with nonempty files; full local snapshot and WAL bytes are positive. Exactly ten values recover without a peer snapshot, member/replica IDs remain fixed, process identity changes, topology stays unchanged and an eleventh command commits. |
| Twenty-second unrelated caller-projection outage | Eight workers at a 400ms gap; every request succeeds, at least twenty per worker. Actual projection writers remain waiting while stored state stays unchanged. At least fifty WAL samples show zero failures and stalls below 500ms. All owner FSMs contain exactly the acknowledged commands, and releasing the writers restores full physical projections including request fingerprints. |
| Fifteen-group replacement plan at three/five/seven nodes | Complete in less than 200 ms; deterministic exact transfer/remove/stop/cleanup/prepare/add/catch-up/promote ordering. Benchmark reports time and allocations. |
| Full meta FSM snapshot at three/five/seven nodes | Fifteen groups, 128 retained documents and executions, a pending transfer and a retired group/allocation floor; payload at most four MiB; serialize/validate/restore/roundtrip in less than two seconds; canonical bytes match and restored retirement refuses ID reuse. Benchmarks report payload bytes, serialization/restore time and allocations. |

The fifteen-group resource cases create fifteen independent three-voter load
engines on the eligible workers of each three/five/seven-node cluster. Every
observer remains in meta while owning no load engine. Twenty-five samples per
process cover creation and ten rounds of writes across all fifteen groups.
Measurements come from the actual Linux process: Go heap, resident pages,
goroutine count, open file descriptors and user/system CPU time. Missing metrics
or a changed process identity fail the case.

Each full fixture process must stay below 128 MiB heap and 256 MiB resident
memory, with at most `baseline + 15*32 + 64` goroutines and
`baseline + 15*8 + 32` descriptors. Average CPU during the write interval must
not exceed four cores. All 165 acknowledged commands have exact results on
every owning FSM. One approved sixty-step deletion plan then removes all
fifteen groups. Joined teardown must leave zero load engines, at most
`baseline + 96` goroutines and `baseline + 32` descriptors, no usable load-store
paths and a healthy retained meta quorum. The relative allowance includes
caller transport connections opened during the workload. These bounds apply
to the tested full fixture, including its caller protocol and storage, and make
no arbitrary-size resource guarantee.

The load transport remains caller-defined. `Transport.Send` receives owned
messages and must support concurrent calls for different destinations and honor
cancellation; calls for one destination are serialized. The engine's bounded
per-destination queues keep a stalled peer from delaying healthy majority
traffic. Removed destinations discard stale backlog, retain the final committed
removal attempt and release their worker when that attempt completes. Stop
cancels and joins all owned send workers. Actual Raft and queue-lifecycle
regressions verify these guarantees.

For a busy load group, a nonzero `Timing.WALWriteWait` delays capture of the next
Raft Ready batch after recent successful entry persistence. Proposals and
incoming messages continue through the loop during that bounded window; ticks
and cancellation remain active. Snapshot and membership controls wait for batch
capture. The writer retains one outstanding durable batch, and application and
outbound messages still follow persistence. A zero wait keeps immediate capture.

Pending-admission cases retain a ten-second baseline and thirty-second
measurement with forty healthy writers paced at 200 ms, alongside eight
overload writers paced at 20 ms and 4,000 excess admission attempts. A real
single-voter load WAL is blocked to keep its leader deterministic; meta and
the independent three-voter load group remain distributed. Native observations
must show at least 80% of the default 256 commit-waiter slots occupied throughout,
never exceed the bound, and clear all callers/queued/registered waiters before
the writer is released. Every excess request returns typed overload with p95
below one second. Healthy writes retain at least 30% of the baseline fraction
within 100 ms and exact state on every owner. Exact canceled-request retries
materialize 256 values once, and none of the refused identities appear.

The source's 4,000 application reservations are replaced by 4,000 excess
proposal attempts against the library's bounded admission, rather than
claiming 4,000 held engine waiters. Domain admission ratios, reservation indexes
and HTTP retry-header policy belong to the caller. Fifteen-group resource
measurements are recorded below; supporting source obligations remain tracked
in the implementation plan and inventory.

## Recorded measurements

Measured October 11, 2026 (Asia/Almaty), on Apple M4 Pro, 14 logical CPUs,
Go 1.26.5, macOS arm64, Docker 29.6.1 linux arm64 with 37 GiB available.
Cluster values use the race-instrumented harness with the normal compiled test
application. Unit timing includes concurrent image compilation and functional
validation. These results use a warm Go/Docker build cache and uncached tests.

| Nodes | Health ops/s | Health p95 ms | Meta commits/s | Meta p95 ms | Load commits/s | Load p95 ms | Health/write case seconds, including cleanup |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 3 | 9,805.059 | 2.197 | 39.659 | 378.182 | 1,677.610 | 8.895 | 2.131 / 4.197 |
| 5 | 9,635.536 | 2.280 | 26.524 | 565.440 | 858.678 | 17.413 | 2.688 / 5.880 |
| 7 | 9,539.625 | 2.308 | 21.560 | 695.553 | 566.025 | 26.470 | 2.911 / 5.702 |

Every performance operation succeeded. Meta/load convergence was respectively
63.381/64.288 ms, 65.473/62.231 ms and 18.238/62.763 ms. The fifteen meta
writers experienced 101/94/89 validated CAS conflicts; retry time is included
in throughput and p95.

`make bench` used the default 200 ms sampling interval. All planner fixtures
produce the same complete 120-step fifteen-group replacement plan.

| Nodes | Planner ns/op | Planner B/op / allocs | Full FSM bytes | Serialize ns/op | Serialize B/op / allocs | Restore ns/op | Restore B/op / allocs |
| --- | ---: | --- | ---: | ---: | --- | ---: | --- |
| 3 | 35,690 | 76,737 / 483 | 1,185,766 | 3,177,963 | 3,009,258 / 19,404 | 8,169,266 | 3,702,675 / 27,930 |
| 5 | 36,054 | 77,664 / 484 | 1,273,274 | 3,384,539 | 3,193,434 / 21,238 | 8,664,609 | 4,160,535 / 31,085 |
| 7 | 36,584 | 77,856 / 484 | 1,360,782 | 3,646,323 | 3,243,430 / 23,072 | 9,008,522 | 4,381,947 / 33,980 |

The full FSM fixture retains 128 documents and executions, fifteen live load
groups, a pending exact meta transfer, one permanently retired ID and seventeen
allocation floors. Native Raft ConfState is verified separately by the existing
storage/runtime snapshot tests. Snapshot correctness matrix tests pass under
race detection; benchmark results are measured without race instrumentation.

The sustained read/health cases took 24.15/24.08/24.68 seconds, including setup
and cleanup, with 1,500 exact local reads and 100 successful readiness/status
samples per node. Three retained-follower cycles took 29.38/27.24/28.25 seconds,
including setup and cleanup. Each recovering process held all thirty commands
acknowledged while it was absent, retained its exact member/replica identity and
used a new process identity. Every performance/stability case remained below
the two-minute whole-case bound.

The broader regression invocation also ran learner growth, compacted catch-up,
admission/leader-resume and five/seven-node multi-group shutdown. All selected
cases except one admission startup passed. That case failed when testcontainers
could not inspect the joining container's published port; its logs show ongoing
meta/load joining. The same case passed three subsequent uncached race runs in
7.94/8.94/7.24 seconds. The broad invocation is retained as a failed run, rather
than reported as a successful suite. All run-labeled containers, networks and
volumes were absent after teardown.

The additional dead-peer isolation cases abruptly kill a process leading none
of three groups (meta plus two load groups). At three/five/seven nodes they
collect 100 native leader/term observations throughout twenty seconds and
acknowledge 100 commands per load group. Every surviving owner and the restarted
process contain the exact acknowledged values. Case times including deployment
and cleanup are 32.60/30.22/29.37 seconds. Dedicated native inspection avoids
aggregate status's caller-controller probes; no missing response or leader is
skipped. The independent load-ingress/accounting cases take 6.65/7.23/8.18
seconds and prove exact independent group state after every ingress's retries,
business conflicts and subsequent leadership changes.

The WAL-coalescing and correlated-stall cases use the same hardware and build
conditions above. Final race-harness measurements include generation cleanup:

| Nodes | Entry writes, wait off / 5 ms | Entries per write, off / 5 ms | Coalescing case seconds | Correlated-stall case seconds |
| --- | --- | --- | ---: | ---: |
| 3 | 65 / 51 | 2.215 / 2.824 | 8.13 | 24.50 |
| 5 | 78 / 49 | 1.846 / 2.939 | 9.72 | 23.31 |
| 7 | 80 / 44 | 1.800 / 3.273 | 9.21 | 25.48 |

Every coalescing arm acknowledges all 48 commands, persists exactly 144 replica
entries and checks each local owner's exact values and 48-entry applied delta.
Each stall retains 75 native meta observations, 39/65/91 successful readiness
samples, eleven typed availability refusals and fourteen exact recovered values.
A meta edit commits during the fifteen-second outage. Every store is armed and
actual writers time out/retry; idle followers need not enter a writer if a
blocked leader cannot send its unpersisted entries. Serving/session readiness
and actual consensus recovery are checked separately. Successful retries and
retained-process restart require every owner's full expected state and local WAL.

The initial frozen-Ready writer delay did not consistently increase batching in
repeat runs. Capture now waits while the loop continues collecting input, and
controls wait until the pending batch is captured. Real learner/promotion/
transfer/removal tests run with zero and five milliseconds under race detection.
The corrected six cluster cases passed twice in 209.951 seconds, followed by the
final image with the control guard in 102.777 seconds. Individual case bounds
remain below two minutes. Full library tests, engine races and all three vet
commands pass; no labeled containers, networks or volumes remain. The separate
uncached 317-case parallel unit command takes 8.12 seconds.

The normal durable-restart and unrelated caller-store outage ports pass in a
combined final race-harness run of 110.783 seconds. No labeled containers,
networks or volumes remain after cleanup.

| Nodes | Own-store recovery seconds | Local snapshot index / bytes | Local WAL bytes | Projection outage seconds | Exact commands | Peak WAL stall ms |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| 3 | 11.44 | 12 / 1,241 | 1,048,576 | 25.39 | 393 | 5.373 |
| 5 | 10.97 | 12 / 1,241 | 1,048,576 | 24.53 | 393 | 4.447 |
| 7 | 11.06 | 12 / 1,241 | 1,048,576 | 24.91 | 393 | 3.819 |

Own-store recovery checks the killed process itself, including its physical
files and zero received peer snapshots, before continuing with an eleventh
command. Each projection case retains the full twenty-second freeze and all
eight source-shaped clients, with 100 WAL samples per host and zero shedding.
The auxiliary worker is caller code compiled only into the test image; it
persists full Machine state via atomic file replacement. Fault handshakes wait
for a real pending write before recording a stable disk baseline, so an earlier
admitted write cannot produce a false failure. Release catches up every exact
value and request fingerprint without restarting. Worker cancellation/retirement
and ordinary-build control exclusion are verified separately. Full make test
passes, tagged example races pass in 35.513s, all vet/diff checks pass and the
unchanged 317-case uncached parallel unit loop takes 11.21 seconds.

The dedicated-fixture pressure matrix passes at three/five/seven nodes in
48.02/48.71/48.20 seconds including cleanup. Each observes all 256 native
waiter slots for thirty seconds, refuses 15,024/14,976/15,184 excess and paced
requests with overload p95 2.553/2.870/2.002 ms, and retains 100% of both
baseline and pressure proposals inside 100 ms in the independent group. Its
7,751/7,761/7,816 exact healthy values reach every owner. Canceling the held
callers clears in-flight/queue/registered counts while persistence remains
blocked; exact retries recover 257 held-group values including warmup, with
no refused-request keys. These are 4,000 burst attempts plus sustained refusals
against bounded engine waiters, not 4,000 held application reservations.
The seven-case regression invocation also passes learner growth, admission
leader transfer, own-store recovery and projection outage in 199.882 seconds,
using the separate `tests/cluster/testapp` binary rather than the demo.

### Fifteen-group process resources

All three cases pass in 130.785 seconds combined. Each row reports the maximum
measured value across that case's processes; elapsed time includes deployment
and cleanup. Every case verifies all 165 acknowledged commands before deleting
all fifteen groups and checking the retained meta quorum.

| Meta nodes | Case elapsed | Peak heap | Peak RSS | Peak goroutines | Peak descriptors | Maximum average CPU cores |
| --- | --- | --- | --- | --- | --- | --- |
| 3 | 40.68 s | 38.98 MiB | 74.00 MiB | 161 | 82 | 0.212 |
| 5 | 44.93 s | 40.27 MiB | 73.86 MiB | 156 | 84 | 0.207 |
| 7 | 42.74 s | 40.02 MiB | 74.03 MiB | 168 | 84 | 0.181 |

After deletion, every process reports zero load engines. The largest observed
cleanup increase is 30 goroutines and 8 descriptors over the initial baseline,
within the explicit allowances for caller transport connections. All usable
load-store paths are absent. Measurements use Linux process counters inside
the normal compiled fixture and a race-instrumented Go test harness.
