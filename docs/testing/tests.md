# Test suites

`make test-unit` runs uncached unit tests with `-short -count=1`. Independent
top-level unit cases call `t.Parallel()`. The default runs four packages and four
cases per package concurrently; `UNIT_PACKAGES` and `UNIT_PARALLEL` can override
those values. Storage fixtures use separate temporary directories and the same
registered WAL segment size. Tests do not mutate process environment, working
directory or the upstream WAL segment-size global.

Unit tests cover deterministic FSM commands, codecs, validation, mutation
previews, planning, membership policy, individual worker components with fake
sources, owned subscriptions, local journals, bbolt adapters and WAL recovery.
Keep this suite below 30 seconds of elapsed time on the development machine.

Functional cases explicitly skip when `testing.Short()` is true. These exercise
real running Raft engines, network protocols, public multi-node lifecycles, or
integrated FSM/planner/convergence/reconciliation workflows. Constructor and
direct RawNode recovery checks that do not start a runtime remain unit tests.
New functional cases must include the short-mode guard before opening fixtures;
new isolated unit cases should call `t.Parallel()` before creating fixtures.

`make test` runs all unit and functional tests. Short-mode separation does not
replace the full milestone check. `make test-unit-race` checks the parallel unit
suite with the race detector; `make test-race` checks the full suite. Cluster and
example suites will have explicit targets when their harnesses are implemented.

Measured on the development machine (Go 1.26.5, macOS arm64, 14 logical CPUs):
`make test-unit` completed in **6.86 seconds** with test-result caching disabled
and an existing Go compilation cache. The slowest package was WAL storage at
4.398 seconds. This measurement does not include a clean dependency download or
a fully cold compiler cache.

After the meta lifecycle/removal additions, another uncached run completed in
**8.36 seconds** while the full functional suite ran separately. Unit cases
remain parallel; functional and cluster cases remain outside short mode.

After the runtime facade and cold-boot additions, an uncached unit run completed
in **7.26 seconds**. The three new integrated runtime cases remain sequential
and skip under short mode; all new isolated unit cases run in parallel.

The bootstrap identity/freshness audit adds parallel corruption and orphan-state
cases. The uncached unit suite completed in **7.40 seconds** after these changes.
