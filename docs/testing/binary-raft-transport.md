# Binary Raft transport validation

Implemented 2026-10-19. Target: [binary Raft transport](../brainstorms/20261019-binary-raft-transport-target.md).

## Scope and compatibility

The included loadgrpc adapter sends Raft requests as version-2 protobuf frames containing complete exact peers and native raftpb.Message bytes. Ordinary traffic reuses ordered streams; snapshot-bearing batches use sequential unary requests with completion only after every frame succeeds. Administrative requests and unary replies retain JSON. Application commands, snapshots, public APIs and persisted formats are unchanged. All users of this unreleased adapter must rebuild and upgrade together; no fallback or migration is provided.

## Codec experiment

Apple M4 Pro, darwin/arm64, Go 1.26.5. Five repetitions, one CPU, 500 ms per case. Both variants include outer protobuf encoding/decoding; encoding includes complete batch validation, splitting and buffering. The JSON baseline is the former splitter retained only in the benchmark file. Each input is semantically round-tripped before timing.

```sh
go test ./transport/loadgrpc -run '^$' -bench '^BenchmarkRaftFrameCodecs$' -benchmem -benchtime=500ms -count=5 -cpu=1
```

| Case | Encode JSON/binary ns | Decode JSON/binary ns | Encode B/op JSON/binary | Decode B/op JSON/binary | Wire bytes JSON/binary | Frames JSON/binary |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| heartbeat | 1,032/802 | 5,339/580 | 1,200/864 | 2,360/724 | 423/64 | 1/1 |
| app-response | 814/581 | 4,796/537 | 1,200/864 | 2,360/724 | 423/64 | 1/1 |
| read-index | 856/614 | 5,067/551 | 1,264/920 | 2,416/772 | 469/90 | 1/1 |
| append | 1,056/686 | 6,992/653 | 2,096/1,416 | 4,960/1,292 | 804/333 | 1/1 |
| batch | 205,121/10,245 | 278,106/19,158 | 319,634/43,008 | 281,936/92,028 | 36,284/18,834 | 10/5 |
| snapshot | 21,949/7,665 | 171,789/6,425 | 98,613/82,696 | 220,992/74,632 | 44,222/32,858 | 1/1 |

Median decoding improves 8.94–26.74× and encoding 1.29–20.02×. Allocated bytes fall in every case. Small-frame encoding allocation counts increase from 6 to 12; decoding allocations decrease, e.g. heartbeat 23→9 and append 30→11. Buffer pooling remains deferred. The benchmark startup overlapped the tail of lint; these are repeated medians, not confidence intervals.

Raw [benchmark output](../../test-reports/performance/binary-raft-codec/benchmarks.txt) and [medians](../../test-reports/performance/binary-raft-codec/medians.json) retain every reported metric.

## Three-node cache experiment

Each variant was captured three times serially on the same Apple M4 Pro/Colima linux/arm64 host: one loader worker, 615 generated keys, 80% reads/20% writes, GOMAXPROCS=1 per container, disabled load fsync, five-second warmup and 30-second CPU capture. Syscalls were traced in a separate subsequent ten-second phase. The existing user cache deployment remained running with its loader disabled. No tests or benchmarks were run concurrently with CPU captures.

JSON source: 3359346 (schema-only change, old RPC path still active). Binary source: b5a1f44. Identical pre-existing credential/profiler fixes were preserved for both. Each capture retains its manifest, source patch, exact binaries/build metadata/hashes, logs, heap/CPU/goroutine profiles, syscall events, CSV summaries and interactive heatmap/flamegraph.

| Capture | Load leader | Node1/2/3 sampled CPU seconds | Steady operations/s | Write/read mean µs | Estimated cluster CPU µs/op |
| --- | --- | ---: | ---: | ---: | ---: |
| [before-1](../../test-reports/performance/binary-raft-before-1/report.html) | node2 | 9.88/16.48/9.88 | 4,000 | 315/232 | ≈302.0 |
| [before-2](../../test-reports/performance/binary-raft-before-2/report.html) | node3 | 9.79/9.84/16.53 | 3,948 | 318/235 | ≈305.3 |
| [before-3](../../test-reports/performance/binary-raft-before-3/report.html) | node3 | 9.60/9.80/16.75 | 3,906 | 320/240 | ≈308.5 |
| [after-1](../../test-reports/performance/binary-raft-after-1/report.html) | node3 | 9.51/9.83/15.93 | 4,359 | 300/210 | ≈269.7 |
| [after-2](../../test-reports/performance/binary-raft-after-2/report.html) | node3 | 9.57/9.75/15.91 | 4,546 | 285/203 | ≈258.3 |
| [after-3](../../test-reports/performance/binary-raft-after-3/report.html) | node1 | 16.18/9.42/9.77 | 4,501 | 292/207 | ≈262.0 |

Rates average the six logged completed reads+writes rates over loader elapsed 5–35 seconds; latency values average the six equal-duration progress intervals. CPU samples cover the nearby actual 30-second pprof windows, whose timestamps are retained in manifests/profiles. Because these original loader logs omit absolute timestamps, CPU/op values in this table are steady-state estimates, not exact synchronized operation counts. They do not divide CPU samples by lifetime operations. A timestamped validation pair below checks that approximation.

Repeated median throughput rose about 14%, from 3,949 to 4,501 operations/s; sampled total cluster CPU fell about 2%, from 36.16 to 35.27 seconds despite the greater completed work. Estimated CPU/op fell about 14%. Binary throughput varies 4,359–4,546 operations/s, versus JSON 3,906–4,001; no statistical confidence interval is claimed. All six untraced comparison windows had zero read errors, write errors, stale or corrupt results.

The actual load leader moved between nodes: the first JSON run led on node2; the remaining listed runs identify their own leader in the table. No loadgrpc.decodeJSON samples remain under StreamRaft in any binary run. Other application and administrative JSON remains intentional. The retained interactive reports show the new native decode stacks and the remaining gRPC/authorization/Raft costs.

## Timestamp alignment and allocation evidence

An additional serial pair uses unchanged application binaries and the same capture settings, adding only Docker log timestamps. JSON was built in an isolated detached checkout of 3359346 with the same prerequisite fixes; that checkout was removed afterward. The wrapper is retained as `test-reports/binary-raft/profile_timed.py` and the analysis as `analyze_comparison.py`.

Each pprof start/duration was matched to neighboring absolute-time loader counters. Linear interpolation estimates the boundary counts; the conservative bounds use the adjacent observed counters without interpolation. These remain estimates, not exact boundary measurements. In particular, the last bracketing five-second interval can overlap tracing after the CPU capture; interpolation may therefore undercount the untraced tail. The bounds retain that uncertainty.

| Timestamped capture | Node1/2/3 estimated CPU µs/op | Cluster estimate | Conservative cluster bounds |
| --- | ---: | ---: | ---: |
| [before](../../test-reports/performance/binary-raft-before-timed/report.html) | 88.8/90.5/151.2 | ≈330.4 | 303.3–388.4 |
| [after](../../test-reports/performance/binary-raft-after-timed/report.html) | 81.4/84.6/140.3 | ≈306.3 | 260.9–370.6 |

Both supplemental runs led on node3 and had zero workload errors. Their estimated CPU/op improves about 7%, but the bounds overlap. This pair had substantially more variable throughput: the binary run's five-second rates ranged from 1,990 to 4,747 operations/s within the comparison window. It did not reproduce the throughput/latency improvement of the initial three-run comparison. The cause of that variability was not established; it cannot be attributed to this codec from these samples. The report therefore supports a measured codec improvement and removal of stream JSON CPU, with workload gains dependent on conditions rather than a guaranteed percentage.

Heap profiles were captured immediately after CPU profiling, before strace. Sampled cumulative allocation across all three processes is listed below. Process lifetimes include startup and differ slightly; these totals are not steady-state allocation rates or bytes per operation.

| Run | JSON cumulative GiB | Binary cumulative GiB |
| --- | ---: | ---: |
| 1 | 13.37 | 10.72 |
| 2 | 13.29 | 11.23 |
| 3 | 12.96 | 11.19 |

Per-node allocation and CPU estimates, exact profile timestamps, counter bounds and source evidence are retained in [comparison.json](../../test-reports/performance/binary-raft-codec/comparison.json) and [allocations.json](../../test-reports/performance/binary-raft-codec/allocations.json). Raw heap files remain usable with `go tool pprof -alloc_space`.


## Validation and failures

The following checks have completed on the implementation. Counts include parent tests and subtests, following the repository runner.

| Check | Result/evidence |
| --- | --- |
| `make test` | 1,961 passed; [final unit report](../../test-reports/unit/20261018T212038.573082Z-zy0vlcfj/report.md) |
| `make test-race` | 1,961 passed; [unit race report](../../test-reports/unit-race/20261018T211834.874901Z-_mk7k3tc/report.md) |
| `make test-function` | 1,054 passed; [functional report](../../test-reports/function/20261018T211855.391110Z-ipl_2ba4/report.md) |
| `make test-function-race` | 1,054 passed; [functional race report](../../test-reports/function-race/20261018T211937.294502Z-xp59cmgc/report.md) |
| `make lint` | All modules/tags passed; `test-reports/binary-raft/task4-lint-final.log` |
| `make proto-check` | Passed; generated output reproducible, other protocols unchanged |
| Python script tests | 38 passed; `test-reports/binary-raft/final-python-tests.log` |
| `make test-example` | Meta-only Compose lifecycle passed; [report](../../test-reports/example/20261018T211922.401136Z-pddqabyu/report.md) |

`make test-cache-example` passed after the restart-barrier fix: [report](../../test-reports/example/20261018T212038.573110Z-bonbjpqm/report.md). It covers retained WAL/value/replica identity after one-node and full-cluster restart, health-aware failover and 1/10/256 loader workers. `make test-scheduler-example` also passed: [report](../../test-reports/example/20261018T212406.852766Z-6qi0rw53/report.md), covering execution, leadership transfer, executor/leader crashes, snapshot recovery, quorum loss and a returning partitioned executor. Both private example projects/images/volumes were cleaned; the user cache containers remained running. The final full cluster suite is running and will be recorded in the architectural audit.

Failure history is retained rather than replaced: initial schema commit lint rejected unused preparatory codec helpers; they were moved to the RPC-switch commit. Temporary `.go` evidence backups were renamed `.go.txt` after Go discovered them as packages. A new stream cancellation test found admission converting Canceled to Unavailable; both binary handlers now preserve context classification.

The first cache example run failed on endpoint discovery after immediate kill/start. Docker signal delivery can race a subsequent Start; the test now joins killed containers and waits for health on restart, retaining identity/value assertions. The first scheduler run failed because its credential host bind was unavailable in Colima; the demo image now bundles its public credential. These example setup corrections are recorded as deviations in the plan. A subsequent scheduler run reached the final partition check but polled the old published HTTP port. The rerun confirmed Docker reassigned node3 from port 32852 to 32853 on network reconnect. The fixture now refreshes the endpoint and closes stale idle HTTP connections; its accepted-attempt equality assertion is unchanged. Initial failure reports: [cache](../../test-reports/example/20261018T211834.860822Z-ig1lvgef/report.md), [scheduler](../../test-reports/example/20261018T211834.860842Z-kri0fg1w/report.md).

## Deferred work

Administrative/reply protobuf conversion, application codecs, buffer pooling, header compression, coalescing, routing/authorization indexing, oversized-snapshot chunking and broader queue accounting remain outside this change. Heap lifetime allocation totals include startup; they are not steady-state allocation rates. Traced syscall durations include blocking/tracer overhead and are never added to sampled CPU time.
