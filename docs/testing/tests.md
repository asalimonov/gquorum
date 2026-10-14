# Test suites

Tests are grouped by what they exercise. Units check deterministic logic and
isolated components. Non-Docker functional tests cover a single running
participant, isolated Raft engines, protocol doubles, background application
workers and persistence/recovery fixtures. Units and functional tests stay in
the original packages beside the code they test. Multi-node consensus and
application scenarios live in `tests/cluster` and use real Docker processes
through the bounded pool.

`make test` compiles the library, the separate `examples/cluster`,
`examples/scheduler` and `examples/inmemory-cache` modules and the caller application fixture, then runs their uncached units with `-count=1`.
Go build constraints select the suites at the file level:

| File | Build constraint | Contents |
| --- | --- | --- |
| `*_test.go` | `//go:build !functional` | Unit tests and benchmarks |
| `*_func_test.go` | `//go:build functional` | Non-Docker functional tests and exclusive helpers |
| `*_helpers_test.go` | None | Helpers shared by both suites |

Put each build constraint at the top of the file, followed by a blank line before
`package`. Shared helper-only files may retain their existing names, such as
`fixtures_test.go`. Unit helpers used only by units stay with their unit files.

`make test-function` uses `go test -tags=functional` in all four modules, so it
runs only functional tests. The `!functional` constraint excludes unit tests
from that command; default `go test` excludes functional files. Classification
does not depend on `testing.Short()` or per-test skip guards. Lightweight value
stubs and callbacks testing a single local component are units; fixtures modeling
interacting services are functional.

Suite reports count parent tests and subtests separately. Use the latest report
for executed counts; source-level test inventories do not include table cases.

The scheduler Compose suite runs separately from the small metadata example:
`make test-scheduler-example` builds an isolated image and tests transfer,
executor/leader failure, retained recovery, quorum loss, and a live partition.

Independent units call `t.Parallel()`. The defaults run four packages and eight
cases per package concurrently; `UNIT_PACKAGES` and `UNIT_PARALLEL` override them.
Keep units fast and the complete warm-cache unit command below 30 seconds on the
development machine. Dependency download and a cold compiler cache are separate
setup costs. Tests must not mutate process environment, working directory or the
upstream WAL segment-size global while running in parallel.

Independent functional tests also call `t.Parallel()` before creating their
fixtures. Each test owns its temporary storage, listeners on ephemeral ports,
transport and cleanup. Independent table cases may run in parallel; cases that
mutate one shared component or store stay sequential within their parent test.
Functional runs default to four packages and eight cases per package, controlled
by `FUNCTIONAL_PACKAGES` and `FUNCTIONAL_PARALLEL`. The same limits apply to race
runs. This bounds disk and network contention while isolated components
progress concurrently. Functional fixtures must not start a multi-node cluster.
The five modules are run in sequence, including `examples/scheduler` and
`examples/inmemory-cache`.

| Command | Coverage |
| --- | --- |
| `make test-cache-example` | Isolated cache Compose recovery after one-node and full process restarts |
| `make test-scheduler-example` | Isolated three-process scheduler Compose lifecycle, recovery and partition checks |
| `make test` / `make test-unit` | Compile the library, example and fixture; run uncached parallel units |
| `make test-race` / `make test-unit-race` | The same units under the race detector |
| `make test-function` | Non-Docker functional tests in `*_func_test.go` files |
| `make test-function-race` | Functional tests under the race detector |
| `make test-example` | Actual small Compose demo REST e2e |
| `make test-cluster` | Build the image, then run Docker process tests with `-tags=clustertest` |
| `make test-perf` | Three measured Docker cases, each under two minutes |
| `make bench` | Planner and complete meta FSM snapshot matrices |
| `make lint` | Vet all module/build variants; TiDB revive style rules and merged Staticcheck unused declarations |
| `make proto-check` | Regenerate pinned protocol code and require no diff |

`make test-functional` and `make test-functional-race` remain aliases for the
`test-function` targets. Use `make test-function FUNCTIONAL_ARGS='-v -run TestName'` for a focused
non-Docker regression. `FUNCTIONAL_TIMEOUT` defaults to eight minutes per package.
Use functional race checks for changes to ownership and concurrency. Keep the
full Docker suite for the end of a related set of changes.


`make lint` installs versioned binaries under the ignored `.tools/lint/` directory:
revive v1.17.1 and Staticcheck v0.8.2. The checked-in [revive configuration](../../revive.toml)
copies `tools/check/revive.toml` from the local TiDB checkout, including its 16
enabled style rules, confidence threshold and severity. These
[revive rules](https://github.com/revive-lint/revive/blob/v1.17.1/RULES_DESCRIPTIONS.md)
do not detect unused declarations. Staticcheck supplies `U1000`, with a
[merged build matrix](https://staticcheck.dev/docs/running-staticcheck/cli/build-tags/)
so helpers used only in functional or process tests remain live. The runner vets
unit/functional builds in all five application modules, clustertest/example builds
in the test module and the lint wrapper's unit build. It passes every selected
source file to a small wrapper around the pinned revive library. The wrapper groups files by directory to preserve package comments
and cross-file type information while including all selected build tags. Generated files retain their
generated-code exclusion. Lint compiles and analyzes tests without executing them or starting Docker. Build
failures and findings return nonzero status; there are no blanket unused-code
suppressions. The wrapper's regression check is available with
`cd scripts/revive && go test ./...`.

Enable the pre-commit hook once after cloning with `make install-hooks`.
The hook runs `make lint-staged`'s checks against a temporary copy of the Git
index, so partially staged files use the contents being committed. It vets
affected packages and runs the same production, revive and merged unused-code
checks, reporting lint findings only in staged Go files. Compilation failures
still block the commit. Renames and deletions include the affected packages;
lint configuration changes check every module, and staged `go.mod`/`go.sum`
changes check their entire module. Documentation-only commits skip lint.
Pinned tools are installed automatically when needed. The checks never run tests
or change staged files. Run `make lint-staged` to invoke them manually.

## Test reports

The test targets require Python 3.9 or newer for the shared
[`scripts/test_report.py`](../../scripts/test_report.py) runner. It uses the
standard library and Go's `-json` event stream. Unit, functional, Docker cluster,
Compose and race runs use the same Meridian-style console summary:

```text
REPORT                                               TESTS    PASS    FAIL     ERR    SKIP
...
TOTAL (26 reports)                                     ...     ...     ...     ...     ...
Unit tests: PASS
Wall time: ...s
```

Console output shows package progress, failures and report paths. Cluster and
Compose runs also show progress for each top-level test. Verbose test output is
flushed to files while the run is in progress. The summary counts all testcase
instances, including parent tests, subtests, repeated runs and executable
examples, matching JUnit reporting; these counts differ from the top-level
source inventory above. Package failures outside a test and unfinished tests
produce entries in the `ERR` column.

Each invocation creates a unique UTC timestamped directory under
`test-reports/<suite>/`. Suites are `unit`, `function`, `cluster`, `example` and
their `-race` variants. Concurrent runs use separate directories; old reports
are preserved. `test-reports/<suite>/latest` links to the latest completed run.
`TEST_REPORTS_DIR` overrides the report root. The default root is ignored by Git.

Every run saves:

- `report.md`: summary table, durations, commands and links to package details.
- `timeline.md`: timing report with an ASCII Gantt diagram and the complete
  test timing table. The chart shows command phases and top-level tests; the
  table also includes subtests. Paused `t.Parallel()` intervals are distinct
  from execution. Wall time includes compilation, setup, teardown and the image
  build when running `make test-cluster`.
- `timeline.json`: machine-readable timings and execution segments.
- `events.json`: the merged raw Go event stream.
- `console.log` and `command-<module>.log`: full output, including compiler,
  process and Docker build diagnostics.
- `<module>-<pkg>.xml`, `.json`, `.log` and `.md`: JUnit XML, raw events,
  verbose output and detailed Markdown results for each package.

Reports are written on test/build failures and interruptions as well as success.
The runner preserves failure exit status, and a failed Docker image build stops
before starting tests. Non-Docker runs finish the remaining modules after a
failure so their results are also available.

```sh
make test UNIT_ARGS='-run TestName'
make test-function FUNCTIONAL_ARGS='-run TestName'
make test-cluster-run CLUSTER_ARGS='-run TestName'
python3 -m unittest discover -s scripts -p '*_test.py'  # Reporting-tool regressions
```

## Docker suites

The small `_meta` demo contains topology/status/drain/activate HTTP routes.
Its actual Compose e2e lives in `tests/cluster/example_test.go` and uses private
project/image names, random ports and temporary volumes. Full load FSMs,
protocols, application retirement and fault controls belong to
`tests/cluster/testapp`; all process scenarios belong to `tests/cluster`.
The separate test module keeps Docker tooling outside the library dependency
graph. The example module keeps Kong outside the library dependency graph;
root test and vet targets include its tests explicitly. Use
`go -C examples/cluster test ./...` for a focused example check.

Multi-node scenarios run only here with the `clustertest` build tag, including
the former in-process meta, engine and caller-fixture runtime cases. The
[migration map](cluster-migration.md) records their process replacements and
retained component assertions. Single-node functional fixtures cannot accept
a cluster-size argument.

Ordinary cluster and quorum scenarios use three nodes; larger node-count
matrices are omitted. Four-node admission/promotion, durable admission-pin restart
and interrupted voter-swap, guarded leadership transfer and replication-lag
regressions require a fourth member for three voters plus one learner. Strict
three-AZ placement uses the fourth member as a same-AZ standby to verify its
guarded voter switch and refusal after a survivor loss. The five-node automatic-promotion acceptance case needs two
standbys to prove that exactly one is promoted and the other retains its learner
role; it also checks retained return and a second failure during cooldown.
The seven-host capacity regression proves atomic rejection at the default five
load replicas and complete all-host placement after explicitly raising the cap.
Larger cases do not repeat the ordinary fault matrix.

Cluster cases and independent admission-phase subtests call `t.Parallel()` before setup.
`CLUSTER_PARALLEL` defaults to eight for both cluster targets. Pool capacity and
concurrent startup follow Go's `-parallel` setting (also eight by default for raw
cluster invocations); existing `GQUORUM_TEST_POOL_SIZE` and
`GQUORUM_TEST_COOK_PARALLEL` overrides remain available. Each case owns isolated
testcontainers resources; the bounded pool reuses only its private network after
verified cleanup. Subtests sharing a lease stay sequential. Dedicated performance
targets default to `PERF_PARALLEL=1` for comparable latency measurements.
`CLUSTER_TIMEOUT` sets the suite deadline and defaults to ten minutes. Each performance case separately
bounds deployment, workload and release and must finish in less than two minutes.
Use `make test-cluster-run CLUSTER_ARGS='-race -v -run TestRuntimeGrowth'` to
reuse a built image for focused checks. Short or focused runs complement the
complete regression.

See [the source inventory](../brainstorms/20261010-raft-library-test-port.md),
[process harness](../../tests/cluster/README.md),
[Node ownership](../architecture/runtime.md) and
[measured performance gates](performance.md). Milestone validation and earlier
timings are retained in the [implementation log](../plans/20261010-implementation.md)
and repository history.

The scheduler module has unit validation (CAS transitions, codec, revocation
rules, executor choice, configuration) and functional SQLite/FSM, snapshot,
schema-reset, retirement and single-replica recovery coverage under the same
tags as the library. `make test` and `make test-function` include it, and
`make lint` checks both variants. The public load gRPC adapter has
protocol-double tests under its owning package's `functional` tag. Process
failover acceptance is the separate `make test-scheduler-example` Compose
scenario.

In-memory load storage is covered at every layer. `storage/memstore` units run
the shared `storagetest.Run` conformance suite plus snapshot, retirement and
ownership cases without touching disk. `internal/managed` functional tests check
store selection by storage class, conflicting state, retirement through the
memory store and the own-reseat loop (memory voters only, pinned to the observed
Active version, joined on Stop). A root functional test creates a memory group
with `EnsureGroup`. `TestMemoryGroupReseatsARestartedReplicaThreeNodes` in
`tests/cluster` restarts a node and verifies that its memory replica is reseated
without an operator while its persistent replica keeps its identity. The
in-memory cache example's functional test closes and reopens a WAL store with
`DisableFSync` enabled, rebuilding a fresh cache machine from snapshots and log.
`make test-cache-example` runs the actual three-node cache Compose application
with private volumes, ports and an image tag. It kills/restarts one process and
then all three, verifying retained values, unchanged replica IDs and new writes.
It exercises process failure with the OS running, not host/VM power loss.
