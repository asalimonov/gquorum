# Process cluster tests

Run `make test-cluster` from the repository root. It builds the caller-owned
key/value test application image once, then runs this module with the `cluster` build
tag. Docker must be reachable. For focused validation, reuse the built image:

```sh
make test-cluster-run CLUSTER_ARGS='-v -run TestPool'
```

`CLUSTER_IMAGE` selects
a different application image; that image must implement the fixture's REST and
startup contracts. Cluster cases remain sequential.

This separate Go module pins testcontainers and Docker tooling without adding
them to the library's dependency graph or the parallel unit-test loop.

The bounded pool defaults to two deployment units and one concurrent cook.
`GQUORUM_TEST_POOL_SIZE` and `GQUORUM_TEST_COOK_PARALLEL` override these counts.
Start returns after the first isolated network is usable; later units finish
warming independently. A unit's lifecycle goroutine owns its resources through
boot, lease, cancellation, reset and final teardown. Release is generation
checked and idempotent. An expired wait context leaves cleanup owned; a later
Release joins that same cleanup.

Each lease has fresh cluster/process credentials, containers and persistent
volumes. Only its private bridge network is reused. Run, unit and generation
labels fence cleanup, including resources whose create acknowledgement was lost.
Once a Docker mutation is admitted, its bounded acknowledgement stays owned and
joined even when the caller cancels its wait. A failed create with an unknown ID
retires the unit; it cannot be safely returned to the available pool.
Reset verifies that no matching containers or volumes remain before publishing
the unit as ready. Failed cleanup retires the unit and fails the suite. Close
cancels and joins every lifecycle worker before returning; the reaper is a
fallback, not the success criterion for cleanup.

Current coverage proves isolated simultaneous clusters, fresh persistent state
and retired-ID isolation on reuse, canceled partial boot, joined cleanup and
three/five/seven-node consensus with worker-only load placement, acknowledged
local replication, abrupt leader death and retained process recovery. The fifteen
load-group lifecycle case verifies independent command identities, propagation to
every host, reserved meta protection and physical removal of every deleted store.
Additional cases cover non-host meta-controller effects, inert staged documents,
follower drain/activation, observed-majority drain refusal/recovery and fifteen
concurrent CAS contenders at each of the three/five/seven-node capacities.
Learner growth uses cancelable effect barriers and reads the returning process's
own identity, membership and values. Compacted catch-up verifies a received full
snapshot and all 1,100 acknowledged commands. Dynamic admission starts a genuine
joining process, observes its meta learner phase, refuses early application
serving and verifies retained identities across process replacement.
Admission also transfers meta leadership during the learner phase and requires
the same execution ID, attempt and exact authorized promotion to finish. A real
one-second repair interval preserves intentional learners through several ticks;
a staged promotion remains inert until approval. Whole-group deletion waits for
an unreachable former host while Active and its retained local state remain
intact. Multi-group shutdown grows from three founders to five/seven admitted
workers, verifies each host's membership and acknowledged values, then requires
all concurrent process stops to finish within thirty seconds with exit code zero.
All node counts come from topology.Limits; seven meta voters do not require seven load
replicas. Fixtures use the library default 100ms Raft tick and a five-second
activation TTL. Build the image before running cluster cases, so compilation
does not contend with the processes being measured. The image build retains Go
module and compiler caches across source changes. Source-test audit, additional
fault controls and remaining performance scenarios are still tracked in the
implementation plan.

The [test application](testapp/README.md) compiles authenticated controls with
`clustertest`; the ordinary Compose example has no control routes. Adding a
joining process keeps its containers/volumes in the lease's generation-scoped
resource catalog, so normal release verifies their removal with the founders.

Measured performance gates and operational workloads are documented in
[performance checks](../../docs/testing/performance.md). `make test-perf` runs
health/status pressure and successful concurrent meta/load writes at three,
five and seven nodes; each complete case includes resource release and must
finish within two minutes. The full cluster suite also retains the fifteen-
second exact local-read, twenty-second readiness/status and three-cycle
retained-process recovery workloads from the source stability tests.
