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

The admission/mutation source audit's latest uncached run completed in
**13.23 seconds**, including the separate fixture module, while full runtime
and Docker validations ran separately. The added isolated cases remain
parallel; the real forwarding and joining-engine cases skip short mode.

Functional cases explicitly skip when `testing.Short()` is true. These exercise
real running Raft engines, network protocols, public multi-node lifecycles, or
integrated FSM/planner/convergence/reconciliation workflows. Constructor and
direct RawNode recovery checks that do not start a runtime remain unit tests.
New functional cases must include the short-mode guard before opening fixtures;
new isolated unit cases should call `t.Parallel()` before creating fixtures.

`make test` runs all unit and functional tests. Short-mode separation does not
replace the full milestone check. `make test-unit-race` checks the parallel unit
suite with the race detector; `make test-race` checks the full suite. `make test-example` runs the REST functional suite. `make test-cluster` builds
the fixture image once and runs the separate Docker test module. Cluster tests
remain sequential and are excluded from the root unit and functional commands.

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

With the example and public placement/readmission helpers, the uncached unit
suite completed in **8.13 seconds**. The audit found 307 parallel top-level unit
cases and 95 functional cases at that checkpoint. The additional exclusion and
readmission case is also functional and skips before opening fixtures in short
mode. `make test-example` covers real HTTP/meta/load protocols, snapshots,
idempotent retry, local event streaming, durable restart and fresh-ID readmission.
Compose retained-volume recovery is verified separately from the in-process
HTTP suite. Functional tests account for the longer `make test` duration; use
`make test-unit` for the below-30-second development loop.

A subsequent uncached unit run completed in **16.87 seconds while the full
functional suite ran concurrently**. Both measurements remain below 30 seconds.
The final callback audit adds a parallel constructor/storage case for multiple
historical replica IDs and refusal to quarantine a foreign retained identity.

The final reviewed example adds 308 parallel top-level unit cases in total,
with 96 functional cases kept outside short mode. The uncached unit command
completed in **18.99 seconds during a simultaneous Docker image rebuild**,
including compilation/linking overhead, and stays below the 30-second target.

The first process-cluster harness remains outside the unit dependency graph. An
uncached unit run completed in **24.02 seconds while Docker cluster validation
was running**. Commit-only WAL and snapshot-boundary durability regressions use
independent readers before close/probe can flush the encoder, and run in parallel.

After the WAL retry audit, **311 parallel top-level unit cases** remain in the
root module. The uncached unit suite took **23.17 seconds during Docker image
compilation and cluster validation**. Full functional coverage still runs with
`make test`; the Docker suite runs explicitly with `make test-cluster`.

The apply-mode regression brings the root module to **312 parallel top-level unit
cases**. After restoring and retaining the existing HTTP identity/error tests,
`make test-unit` completed in **8.34 seconds**, with result caching disabled.
The full library suite, example race suite and focused HTTP guard race checks
passed separately; process-cluster cases remain sequential.

The ordinary example's control-route exclusion test brings the root unit suite
to **313 parallel top-level cases** (verified from Go syntax trees). The uncached
unit run completed in **11.96 seconds during Docker snapshot-catch-up testing**.
Test-image hooks have separate `clustertest` coverage, including cancellation,
authentication and release; their full example race suite passed in 35.613s.
Functional and cluster cases continue outside the short parallel loop.

The learner-policy, deletion-wait, leader-resume and larger-cluster shutdown ports
remain in the separate sequential Docker suite. The root's **313 parallel unit
cases** are unchanged; an uncached run completed in **7.95 seconds** during the
test-image rebuild. Full root tests and tagged example races passed separately.

The performance/recovery milestone brings the root module to **317 parallel
top-level unit cases**, counted from Go syntax trees. A fresh `make test-unit`
run completed in **16.18 seconds** during image compilation and full functional
validation. Planner/full-snapshot matrix assertions and isolated outbound queue
checks run in parallel; the real slow-destination Raft regression remains
functional and skips before opening fixtures in short mode. Performance and
sustained recovery cases stay in the separate sequential Docker module.

The load-ingress/dead-peer ports stay in the sequential Docker module. Native
meta status is exposed through `meta.Group.LocalStatus()` without calling load
controllers; the real public create/delete test verifies it stays available
with the caller controller locked and returns owned membership. The unchanged
317 parallel unit cases take **21.77 seconds** uncached during concurrent full
functional, race and Docker validation (17.64s in the preceding run).

The WAL-fault/coalescing ports stay in the separate Docker module. Their storage
control unit runs in parallel under the test image's `clustertest` tag. The
ordinary 317-case unit loop is unchanged and completes uncached in **8.12
seconds** (8.34s and 16.11s in preceding runs). The syntax audit uses the active
Go build context, excluding the tagged test-image unit from this default count.

The retained-store/projection outage ports add two parallel unit tests in the
test image's build context, covering full-state catch-up and retirement of a
blocked caller worker. The ordinary example's existing parallel test now checks
that all projection and WAL control methods are absent. The default-build count
remains **317 parallel unit cases**, with the uncached command taking **11.21
seconds** during Docker/race verification. Full unit/functional tests and tagged
example races (35.513s) pass separately.

The October 11 scope correction separates the small `_meta` demo from all Raft
fixtures and scenarios. Root default-build syntax audit counts **312 parallel
unit cases**, and the dedicated `tests/cluster/testapp` adds **ten parallel
units**. `make test-unit` executes both uncached in **9.49 seconds**. Functional
fixture tests skip in short mode and run sequentially through `make test` or
`make test-fixture`; Docker cases use `make test-cluster`. `make test-example`
now runs the actual Compose demo's REST e2e from `tests/cluster/example_test.go`,
with independent ports/projects/images/volumes and verified cleanup. Historical
`clustertest` measurements above predate this relocation; those build tags and
instrumentation are removed from the example.
