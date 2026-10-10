# gquorum

A Go 1.26 library for clustered applications with a replicated topology group
and application-defined Raft load groups.

The library owns Raft loops and recovery. `_meta` uses bbolt and the included
meta protocol; load groups use etcd WALs with caller-supplied state machines and
transports. Callers pass settings, identities and storage paths directly.
Topology supports caller-defined node types. All admitted nodes join `_meta`;
placement rules determine which nodes can host load groups. Only `topology.Limits`
sets cluster capacity; its defaults are three nodes and five replicas per group.

Start with the [three-node example](examples/cluster/README.md):

```sh
make run-example
make stop-example
```

The example is a small `_meta` application with topology/status/drain/activate
REST routes. The full caller-defined load application, protocols, recovery
adapters and fault scenarios live under `tests/cluster`, separate from the demo.
Callbacks must be idempotent and verify their exact authorized identities.
Deleted load-group IDs are permanently retired. External application transactions
and outboxes remain caller-owned.

For development:

```sh
make test-unit       # uncached parallel unit tests, target <30 seconds
make test-unit-race
make test           # all unit and functional tests
make test-example   # actual Compose demo REST e2e, tests under tests/cluster
make test-fixture   # load/meta integration fixture, outside the demo
make test-cluster   # build once, then isolated Docker process clusters
make lint
make generate       # protoc required; pinned Go generators
```

`make test-unit` uses short mode, four concurrent packages and four concurrent
unit cases per package. Functional tests skip in short mode and remain sequential.
Override `UNIT_PACKAGES` or `UNIT_PARALLEL` when needed. See
[testing notes](docs/testing/tests.md) and [runtime ownership](docs/architecture/runtime.md).

Implementation is ongoing. The [process-cluster harness](tests/cluster/README.md)
uses isolated reusable testcontainers units in a separate test module. Complete
source-test ports and bounded cluster performance checks remain required stages
in the [implementation plan](docs/plans/20261010-implementation.md).
