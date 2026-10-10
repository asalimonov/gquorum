# Performance and operational checks

Run `make test-perf` to build the test application and execute the six measured
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
| Fifteen-group replacement plan at three/five/seven nodes | Complete in less than 200 ms; deterministic exact transfer/remove/stop/cleanup/prepare/add/catch-up/promote ordering. Benchmark reports time and allocations. |
| Full meta FSM snapshot at three/five/seven nodes | Fifteen groups, 128 retained documents and executions, a pending transfer and a retired group/allocation floor; payload at most four MiB; serialize/validate/restore/roundtrip in less than two seconds; canonical bytes match and restored retirement refuses ID reuse. Benchmarks report payload bytes, serialization/restore time and allocations. |

The load transport remains caller-defined. `Transport.Send` receives owned
messages and must support concurrent calls for different destinations and honor
cancellation; calls for one destination are serialized. The engine's bounded
per-destination queues keep a stalled peer from delaying healthy majority
traffic. Removed destinations discard stale backlog, retain the final committed
removal attempt and release their worker when that attempt completes. Stop
cancels and joins all owned send workers. Actual Raft and queue-lifecycle
regressions verify these guarantees.

Additional fault injection,
backpressure/resource-pressure and source WAL-coalescing obligations remain in
the implementation plan and test-port inventory.

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
