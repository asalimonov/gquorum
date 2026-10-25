# Permanent-majority-loss recovery validation

Implementation follows Q-01 and the
accepted [target](../brainstorms/20261024-permanent-majority-loss-recovery-target.md).
The [plan](../plans/20261025-permanent-majority-loss-recovery.md) retains sixteen
substantial tasks; [progress](../progress/20261025-permanent-majority-loss-recovery.md)
records commands, corrections and commits. Task 15 qualification is complete. Task 16 performs the final architectural delta
and wiring audit; its review outcome is appended before overall completion.

This is the independent gquorum library and caller fixture. Meridian wiring,
configuration, builds and deployment are excluded. Ordinary borrowed stores stay
supported; disaster donors require Node-owned WAL. Recovery requires one complete
stopped survivor and preserves its durable commit, with explicit possible loss of
acknowledged writes. Same-root backups, new lineage, normal founding/import,
normalized checkpoints, deferred Run, retained deficits and explicit reset/admission
are implemented. Multi-survivor selection, dropped groups, rollback, streaming
snapshots and GQ-02 large remote transfer remain excluded.

## Functional and process evidence

| Scope | Actual evidence |
| --- | --- |
| Fast all-module units | 2,129 passed; [report](../../test-reports/unit/20261025T085224.595176Z-kxpqlqus/report.md) |
| Unit race | 2,129 passed; [report](../../test-reports/unit-race/20261025T090950.568013Z-50igaps_/report.md) |
| Owning-package functional | 1,274 passed; [report](../../test-reports/function/20261025T085516.903043Z-eilp47_c/report.md) |
| Functional race | 1,274 passed; [report](../../test-reports/function-race/20261025T085745.128200Z-2uoic3c4/report.md) |
| Dedicated process performance | 3 passed, 61.958s at parallel 1; [report](../../test-reports/cluster/20261025T090508.316326Z-hzu3f_h3/report.md) |
| Complete example-tag suite | Compose, scheduler and cache: 3 passed, 175.012s; [report](../../test-reports/example/20261025T090928.764879Z-ptpbkpjx/report.md) |
| Simulations | 21 passed across seeds 1–3; [report](../../test-reports/sim/20261025T090817.245140Z-no8_q9n2/report.md) |
| Full cluster suite | 107 passed, 185.290s; [report](../../test-reports/cluster/20261025T082710.420939Z-aul3fmio/report.md) |
| Remote worker handoff race | Exact confirmation persisted while Inspect still owns its slot; joined queued work completes; `/tmp/gquorum-task14-worker-queue-race.log` |
| Source size refusal | Metadata/frame/group bounds produce resource_bound, no application callback or accepted operation, unchanged source hashes; `/tmp/gquorum-task15-bounds-race.log` |

The full cluster image was
`sha256:26db0b07efb6eff1a10d92d1eae1b90a39b3bf7dc7977958ff97e21b8d0a89a2`,
created 2026-10-25T08:24:28Z. The pool reported complete teardown; no
`gquorum-test-` containers or volumes remained. Detailed process coverage is in
[the REC mapping](../plans/20261025-permanent-majority-loss-recovery.md#requirement-to-evidence-map).
Cases use three original nodes, retain real KV state and return the two old nodes
via explicit reset. They cover a lagging acknowledged-write donor, an actual
uncommitted load tail verified by offline L>C, incomplete inventory, approved
cleanup debt/staged intent, old-member reachability, SIGKILL boundaries,
Backup/Undo failure/retry, strict three-AZ regrowth, stale authority and all-node
restart. Private byte corruption, exact live-owner locks, no-hook cancellation
and finer filesystem cuts remain owning-component tests.

The later performance image is
`sha256:e8b7afa094e194e9bdecb3cdf787c48e00f6be33c5439f44f55ff0993e080a63`,
created 2026-10-25T09:05:07Z, with the resource diagnostic refinement. Existing
healthy process behavior is unchanged; the affected refusal paths have fresh
functional/race coverage. No protobuf changed. The default `make test-example`
selects only the Compose demo; complete example coverage used
`python3 scripts/test_report.py example --reports-dir test-reports --timeout=4m -- -run .`.

The first full functional qualification found a fixture that changed acceptance
without updating its history timestamps, two five-second test deadlines under
concurrent load, and an incorrect assertion equating participant readiness with
load publication. The corrected tests preserve exact parked identity,
unpublished handles, zero factory calls and absent WAL bytes. The complete
functional rerun passed; these are not skipped tests or historical pass claims.

## Resource qualification

Machine: Apple M4 Pro, 14 logical CPUs, 64 GiB RAM, macOS 27.0/darwin-arm64, Go 1.26.5, APFS on
`/Volumes/M4DATA`. Each large case uses a compiled functional test binary under
macOS `/usr/bin/time -l`, so peak RSS excludes compiler processes. On this tool,
maximum resident set size and peak memory footprint are bytes. Process real time
includes source construction, negative probes and teardown. Benchmark recovery
wall/allocation measurements exclude source setup and separate one-byte-over
probes. Peak process RSS includes the whole measured process; it is not a hard
application memory guarantee. Snapshot and replay contracts still use full buffers. The disk observer samples
logical file lengths every 10ms and deduplicates inodes across archive renames;
shorter peaks can be missed. Its joined observer allocation overhead is included.
Final extra files and sampled temporary peaks are reported separately.

Defaults are encoded WAL frame 64 MiB, selected snapshot plus logical tail 1 GiB per
group, and logical retained metadata 128 MiB. No default was widened to pass a case.
Every root case exercises Inspect/Approve/Run, ordinary Node recovery startup,
confirmed Complete, exact application tail count and joined Stop. Both ordinary
DisableFSync values are selected; seed/checkpoint protection still forces durable
recovery writes. Sequential groups share one Node. Negative metadata/frame/group
cases exceed the selected configured bound by exactly one byte and never approve.
A cancelled Files hook separately measures joined Run and durable Cancel latency.

Compile and run one selected case at a time:

```sh
go test -c -tags=functional -o /tmp/gquorum-recovery-resources.test .
/usr/bin/time -l /tmp/gquorum-recovery-resources.test \
  -test.run '^$' -test.bench '^BenchmarkRecoveryResources/snapshot-64MiB-tail/disable-fsync-false$' \
  -test.benchtime=1x -test.benchmem

go test -c -tags=functional -o /tmp/gquorum-recovery-metadata.test ./internal/metaengine
/usr/bin/time -l /tmp/gquorum-recovery-metadata.test \
  -test.run '^$' -test.bench '^BenchmarkRecoveryMetadataResources/cleanup-debts-10000$' \
  -test.benchtime=1x -test.benchmem
```

`BenchmarkRecoveryResources` also has metadata-only, sequential-three-groups and
near-1GiB shapes, each with disable-fsync-false/true. Large cases do not run in
normal unit or functional suites. Their source machine consumes every committed
command, preserves a count in its full snapshot, and verifies that exact count
and snapshot length after restored Node teardown.

The metadata-only reconstruction benchmark derives 10,000 unresolved debts from
real approval/consumption graphs and persists both materialized application state
and its complete native snapshot, plus a committed session-command tail. It
measures the readonly metadata/replay path, including all retained key/value bytes;
load donor coverage is exercised by the separate end-to-end root cases. This
fixture uses 51,303,994 materialized bytes, 68,405,452 snapshot-envelope bytes and
119,710,000 total logical metadata bytes. It fits the 134,217,728-byte default and
reconstructs all 10,000 debts. Final reconstruction took 2.833s and peaked at 1,113,473,024 RSS bytes. The
one-byte-over logical metadata limit refused before typed record loading.


Final cases (decimal GB for RSS/allocations/files; logical input positions and
exact byte limits are in each raw log):

| Shape | Ordinary DisableFSync | Selected input MB | Recovery s | Peak RSS GB | Cumulative alloc GB | Final extra GB | Sampled peak extra GB | Cancel/join ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| metadata-only | false | 0.005 | 0.684 | 0.035 | 0.013 | 0.000 | 0.000 | 69.981 |
| metadata-only | true | 0.005 | 0.724 | 0.034 | 0.013 | 0.000 | 0.000 | 83.962 |
| near-1GiB | false | 1040.217 | 133.389 | 4.075 | 235.842 | 1.209 | 1.276 | 1441.867 |
| near-1GiB | true | 1040.217 | 133.462 | 3.506 | 235.841 | 1.209 | 1.276 | 1487.785 |
| sequential-three-groups | false | 21.018 | 5.646 | 0.200 | 3.293 | 0.437 | 0.639 | 569.139 |
| sequential-three-groups | true | 21.018 | 5.669 | 0.196 | 3.294 | 0.437 | 0.639 | 575.559 |
| snapshot-64MiB-tail | false | 90.196 | 6.465 | 1.251 | 11.912 | 0.314 | 0.381 | 422.530 |
| snapshot-64MiB-tail | true | 90.196 | 6.409 | 1.072 | 11.912 | 0.314 | 0.381 | 426.273 |

These are one-pass qualification results, not throughput promises. In particular,
the near-1GiB case makes about 235 GB of cumulative allocations and takes about
134 seconds. Full-buffer replay and repeated evidence validation remain costly;
peak RSS remains a measured value rather than an enforced bound. Streaming/hard
RSS guarantees remain explicitly excluded. Recovery disk growth retains A's
original data and creates B; it is not an in-place rewrite or collection promise.

[Raw logs and machine-readable measurements](../../test-reports/recovery-resources/20261025T094118.922044Z/measurements.json)
retain S/C/L, effective bounds, exact limit refusals, OS counters and each case's
command name. Root test binary SHA-256:
`032892b32b1795d700f9e5fb0c6d484a2885c5073009b44529b2dd82d49ed396`;
metadata binary:
`10098f909aedf8c0a6c571769728f22aac1472ae8161b944181d35c92e47bff2`.

## Ordinary-path benchmark comparison

`make bench BENCH_TIME=1s` was captured before Task 1 and repeated with the same
Go 1.26.5/M4 Pro settings. Planner allocations stay unchanged and timing moves
from approximately 0% to +1.94%; all metadata snapshot byte sizes are identical.
The initial metadata pass showed a small variation, so an independent archive of
baseline 88833e8 and the current worktree each ran matching five/seven-node
serialize/restore cases three times at 1 second per sample. Median time deltas were
-1.95%,+0.30%,-0.09%,+3.44%, respectively. Restore byte allocations are unchanged
within 0.01%, with three additional allocations per operation. Serialize memory
varies with the sample; no ordinary-state encoding or benchmark threshold changed.
The separate process performance gates passed at parallel 1. Raw original and
matched logs are retained with the resource report.

## Frozen formats and compatibility

The [physical provenance](api-baseline-physical.md),
[native 2684202 capture](../../internal/metaengine/testdata/native-2684202/README.md)
and [recovery schema 1 fixtures](../../internal/metaengine/testdata/recovery-schema1/README.md)
remain separate. Seven original physical hashes match their retained manifest;
all 31 tracked fixture/capture files under the baseline/scoped/native/recovery
paths are byte-identical to the prior commit. Fixtures are readonly inputs and
are never regenerated to satisfy tests.

Recovery snapshots keep ordinary metadata schema 4 and add an optional schema 1
subtree. Nil-recovery encodings remain unchanged. The importing/complete golden
fixtures canonically re-encode with their frozen hashes. The actual old 2684202
Machine.Restore was run in an isolated checkout during Task9 and rejects both
with unreadable/unknown recovery field; its probe and provenance remain beside
the fixtures. This is explicit refusal, not migration or mixed-version support.
There is no protobuf delta. Public WAL aliases and ordinary borrowed store/caller
modules remain covered by their existing full suites.
