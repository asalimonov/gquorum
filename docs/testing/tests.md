# Test suites

Tests are grouped by what they exercise. Units check deterministic logic and
isolated components. Non-Docker functional tests cover running Raft engines,
network fixtures, service doubles, background application workers and substantial
persistence/recovery fixtures. Both stay in the original packages beside the
code they test. Tests against separately running applications live in
`tests/cluster` and use Docker.

`make test` compiles the library, the separate `examples/cluster` module and the
caller application fixture, then runs their uncached units with `-count=1`.
Go build constraints select the suites at the file level:

| File | Build constraint | Contents |
| --- | --- | --- |
| `*_test.go` | `//go:build !functional` | Unit tests and benchmarks |
| `*_func_test.go` | `//go:build functional` | Non-Docker functional tests and exclusive helpers |
| `*_helpers_test.go` | None | Helpers shared by both suites |

Put each build constraint at the top of the file, followed by a blank line before
`package`. Shared helper-only files may retain their existing names, such as
`fixtures_test.go`. Unit helpers used only by units stay with their unit files.

`make test-function` uses `go test -tags=functional` in all three modules, so it
runs only functional tests. The `!functional` constraint excludes unit tests
from that command; default `go test` excludes functional files. Classification
does not depend on `testing.Short()` or per-test skip guards. Lightweight value
stubs and callbacks testing a single local component are units; fixtures modeling
interacting services are functional.

There are **191 unit tests**, **349 functional tests**, **108 Docker cluster
tests** (including nine performance cases) and **one Compose example test**.
These are top-level test functions, excluding table cases and subtests. The
unit suite also contains one executable documentation example and two benchmarks.

Independent units call `t.Parallel()`. The defaults run four packages and four
cases per package concurrently; `UNIT_PACKAGES` and `UNIT_PARALLEL` override them.
Keep units fast and the complete warm-cache unit command below 30 seconds on the
development machine. Dependency download and a cold compiler cache are separate
setup costs. Tests must not mutate process environment, working directory or the
upstream WAL segment-size global while running in parallel.

| Command | Coverage |
| --- | --- |
| `make test` / `make test-unit` | Compile the library, example and fixture; run uncached parallel units |
| `make test-race` / `make test-unit-race` | The same units under the race detector |
| `make test-function` | Non-Docker functional tests in `*_func_test.go` files |
| `make test-function-race` | Functional tests under the race detector |
| `make test-example` | Actual small Compose demo REST e2e |
| `make test-cluster` | Build the image, then run Docker process tests with `-tags=clustertest` |
| `make test-perf` | Nine measured Docker cases, each under two minutes |
| `make bench` | Planner and complete meta FSM snapshot matrices |
| `make lint` | Vet unit and functional builds of the library, example and fixture |
| `make proto-check` | Regenerate pinned protocol code and require no diff |

`make test-functional` and `make test-functional-race` remain aliases for the
`test-function` targets. Use `make test-function FUNCTIONAL_ARGS='-v -run TestName'` for a focused
non-Docker regression. `FUNCTIONAL_TIMEOUT` defaults to ten minutes per package.
Use functional race checks for changes to ownership and concurrency. Keep the
full Docker suite for the end of a related set of changes.

The small `_meta` demo contains topology/status/drain/activate HTTP routes.
Its actual Compose e2e lives in `tests/cluster/example_test.go` and uses private
project/image names, random ports and temporary volumes. Full load FSMs,
protocols, application retirement and fault controls belong to
`tests/cluster/testapp`; all process scenarios belong to `tests/cluster`.
The separate test module keeps Docker tooling outside the library dependency
graph. The example module keeps Kong outside the library dependency graph;
root test and vet targets include its tests explicitly. Use
`go -C examples/cluster test ./...` for a focused example check.

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
