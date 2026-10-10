# Process cluster tests

Run `make test-cluster` from the repository root. It builds the caller-owned
key/value application image once, then runs this module with the `cluster` build
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
All node
counts come from topology.Limits; seven meta voters do not require seven load
replicas. Fixtures use the library default 100ms Raft tick and a five-second
activation TTL. Build the image before running cluster cases, so compilation
does not contend with the processes being measured. The image build retains Go
module and compiler caches across source changes. Source-test audit, additional
fault controls and remaining performance scenarios are still tracked in the
implementation plan.
