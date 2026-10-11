# Test suites

`make test-unit` runs uncached unit tests with `-short -count=1`. Independent
top-level unit cases call `t.Parallel()`. The default runs four packages and four
cases per package concurrently; `UNIT_PACKAGES` and `UNIT_PARALLEL` can override
those values. Storage fixtures use separate temporary directories and the same
registered WAL segment size. Tests do not mutate process environment, working
directory or the upstream WAL segment-size global.

Unit tests cover deterministic FSM commands, codecs, validation, mutation
previews, planning, membership policy, worker components with fake sources,
subscriptions, local journals, bbolt adapters and WAL recovery. Keep this suite
below 30 seconds of elapsed time on the development machine. The latest uncached
refactor run completed in **15.61 seconds**, including physical baseline
meta/journal compatibility and the separate fixture module. This uses Go 1.26.5 on
macOS arm64, Apple M4 Pro with 14 logical CPUs, and a warm compilation cache;
dependency download and a cold compiler cache are separate setup costs.

Functional cases explicitly skip when `testing.Short()` is true. These exercise
real running Raft engines, network protocols, public multi-node lifecycles, or
integrated FSM/planner/operator/scheduling workflows. Constructor and
direct RawNode recovery checks that do not start a runtime remain unit tests.
New functional cases must include the short-mode guard before opening fixtures;
new isolated unit cases should call `t.Parallel()` before creating fixtures.

| Command | Coverage |
| --- | --- |
| `make test-unit` | Uncached parallel library and fixture units |
| `make test-unit-race` | The same parallel units under the race detector |
| `make test` | All library and fixture unit/functional cases |
| `make test-race` | All library and fixture cases under the race detector |
| `make test-fixture` | Caller load/meta integration in `tests/cluster/testapp` |
| `make test-example` | Actual small Compose demo REST e2e |
| `make test-cluster` | Build once, then run the Docker process suite |
| `make test-perf` | Nine measured Docker cases, each under two minutes |
| `make bench` | Planner and complete meta FSM snapshot matrices |
| `make lint` | Library and fixture vet |
| `make proto-check` | Regenerate pinned protocol code and require no diff |

The small `_meta` demo contains topology/status/drain/activate HTTP routes.
Its actual Compose e2e lives in `tests/cluster/example_test.go` and uses private
project/image names, random ports and temporary volumes. Full load FSMs,
protocols, application retirement and fault controls belong to
`tests/cluster/testapp`; all process scenarios belong to `tests/cluster`.
The separate test module keeps Docker tooling outside the library dependency
graph and the normal development loop.

Cluster cases remain sequential. Each owns isolated testcontainers resources;
the bounded pool reuses only its private network after verified cleanup.
The complete suite has a 45-minute deadline. Each performance case separately
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
