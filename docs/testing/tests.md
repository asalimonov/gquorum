# Test suites

Use the root `Makefile` for repository checks. `make test` compiles all five application modules—the library, three examples and the cluster caller fixture—and runs uncached unit tests. Root `go test ./...` covers only the library module.

## Choose a suite

| Command | Coverage |
| --- | --- |
| `make test` (`make test-unit`) | Fast units across all five modules; use during development. |
| `make test-unit-race` (`make test-race`) | The same units with the race detector. |
| `make test-function` | Local functional, storage and protocol tests across all five modules. |
| `make test-function-race` | Functional tests with the race detector. |
| `make test-cluster` | Build the caller image and run Docker process scenarios. |
| `make test-cluster-build` / `make test-cluster-run` | Build once, then run or focus process tests against that image. |
| `make test-example` | Small `_meta` Compose demo. |
| `make test-scheduler-example` / `make test-cache-example` | Separate application Compose suites. |
| `make test-sim` | Opt-in deterministic three-node Gosim scenarios. |
| `make test-perf` / `make bench` | Process resource gates / planner and snapshot benchmarks. |
| `make lint` / `make lint-full` | Pending Go files / all modules and build tags. |
| `make proto-check` | Regenerate pinned protocols and require no diff. |

`make test-functional` and `make test-functional-race` are aliases for the functional targets. Run focused checks while editing; reserve full race, lint and Docker suites for the final checkpoint of a feature. Use `make lint` on pending changes and `make lint-full` once before cluster validation. The pre-commit hook checks staged Go files; install it with `make install-hooks`.

## Where tests belong

| Files or module | Build tag | Scope |
| --- | --- | --- |
| `*_test.go` | `!functional` | Isolated units and benchmarks. |
| `*_func_test.go` | `functional` | One local participant, storage or protocol doubles. |
| `*_helpers_test.go` | None | Helpers shared by unit and functional suites. |
| `tests/cluster` process cases | `clustertest` | Multi-node consensus and application behavior in Docker. |
| `tests/sim` | `sim` | Deterministic multi-node simulation in a separate module. |

Put a build constraint on the first line, then a blank line before `package`. Select suites with tags rather than `testing.Short()` or skip guards. Keep private implementation tests beside their package; public recovery API and single-Node coverage belongs in `tests/recovery`.

Units call `t.Parallel()` when independent and must not start services, replicas or subprocesses. Functional tests call `t.Parallel()` before acquiring private temporary roots, ephemeral listeners and joined cleanup. Parallel table cases need separate fixtures; mutations of one shared component stay sequential. Do not put multi-node fixtures in functional packages. Do not mutate process environment, working directory or the upstream WAL segment-size global from parallel tests. Keep `make test` below 30 seconds on the documented warm machine.

Unit and functional runs default to four packages and eight parallel tests per package (`UNIT_PACKAGES`, `UNIT_PARALLEL`, `FUNCTIONAL_PACKAGES`, `FUNCTIONAL_PARALLEL`). The five modules run sequentially. Preserve frozen baseline fixtures; copy them to private roots for tests instead of regenerating them.

## Docker process tests

Use three nodes for ordinary quorum and fault cases; use more only for a regression that requires the extra capacity. Process cases call `t.Parallel()` before acquiring an isolated pool lease, and must join and remove their containers, volumes and workers. Subtests sharing one lease stay sequential. `CLUSTER_PARALLEL` defaults to eight; pool size and concurrent startup follow Go's `-parallel` setting unless explicitly overridden. Performance runs default to one parallel case.

Build the image after caller-application changes. Do not replace a shared image tag during a run. `CLUSTER_TIMEOUT` defaults to ten minutes. A compile-only check needs `-exec=true` because the process suite's `TestMain` starts Docker even with no selected tests:

```sh
make test-cluster-build
make test-cluster-run CLUSTER_ARGS='-v -run TestName'
go -C tests/cluster test -tags=clustertest -run '^$' -exec=true ./...
```

Use [the process harness](../../tests/cluster/README.md) for fault controls and cleanup details. `CLUSTER_REPLAY=1` adds optional status captures. Large generated snapshots use the separate `snapshotlarge` tag and explicit resource qualification described in that guide; they are outside ordinary suites. The [simulation guide](../../tests/sim/README.md) covers seeds and reproducible network faults.

## Reports and focused runs

Targets require Python 3.9 or newer. Each run writes `test-reports/<suite>/<timestamp>/`; `latest` points to the last completed run, and `TEST_REPORTS_DIR` changes the root. Read `report.md` for results, `timeline.md` for execution order, and package `.log` files for failures. JUnit XML, raw events and command logs are retained. Counts include parent tests and subtests. Failed builds and interrupted runs retain reports and nonzero exit status.

```sh
make test UNIT_ARGS='-run TestName'
make test-function FUNCTIONAL_ARGS='-run TestName'
make test-sim SIM_SEEDS=1-3 SIM_ARGS='-run TestName'
```

If you change the report runner, run `python3 -m unittest discover -s scripts -p '*_test.py'` and exercise its affected Make targets. Historical results in repository history do not replace a fresh run.
