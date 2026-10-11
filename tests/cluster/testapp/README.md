# Process-test application

The Dockerfile builds the dedicated caller-owned key/value fixture in this
directory. It supplies a state-machine factory, typed load transport, application retirement
callback and process lifecycle around a `gquorum.Node`. Node owns Raft startup,
recovery, approved effects, membership, readmission and local stop holds.
`node.Cluster()` supplies the topology API. The fixture imports no private meta
or Raft engine packages. The Compose demo
is a separate application; fixture controls and integration tests stay here.

Test controls live in `hooks_cluster.go`. They provide direct
local replica and actual storage identities, membership/materialized values, durable snapshot size and
index, received-snapshot counts, exact successful effect receipts and cancelable
effect barriers. Barriers can hold prepare, promote, stop, cleanup or delete
effects. Released effects recheck their complete authorization before changing
state. The transport also routes local actions through the Node receiver, so
fault injection covers the same path on every host. RPC cancellation releases its own wait; the armed barrier
remains for a later retry until the test explicitly releases it.

Local inspection includes the library's native proposal-admission observation.
`POST /internal/test/propose/{group}/{replica}` submits to the exact local
engine without aggregate peer probes. Its bounded one-minute HTTP lifetime
allows the thirty-second pending-waiter test; ordinary example requests keep
their ten-second deadline. Request cancellation still participates in the
application's owned effect lifetime and engine waiter cleanup.

`hooks_wal_cluster.go` wraps the actual load store with cancelable write stalls
and bounded transient-failure injection, keyed by group and exact replica ID.
Local inspection distinguishes attempted writes, entry attempts, failed calls,
active calls and successfully persisted entry batches. Failed or canceled calls
do not contribute to successful entry accounting. Meta uses its own unwrapped
bbolt store. The fixture passes its requested load WAL coalescing wait through
the application's constructor settings.

`hooks_projection_cluster.go` implements an auxiliary caller-owned full-FSM
projection only after an authenticated test requests it. It writes atomic JSON
files below the application's supplied data directory, independently of Raft
and the local event journal. Controls target an exact group/replica and can stall
a real pending projection write. Observation reads the actual stored file and
reports waiting/active writes separately from completed writes. A stalled worker
does not hold the FSM lock; release resumes catch-up, and cancellation/replica
retirement joins the worker before application storage teardown. Projection files
carry their complete storage identity. The retirement callback verifies that pin,
removes only the exact file, syncs its directory and repeats safely after success. The fixture runs independently of the ordinary Compose demo.

Every control request requires the fixture's fresh credential in
`X-Gquorum-Test-Credential`. The normal application protocol keeps its existing
credential and exact cluster/member/replica checks. None of these controls add
library configuration or load-protocol fields.

Use `make test-cluster-build` to build the image. Run focused scenarios with
`make test-cluster-run CLUSTER_ARGS='-race -v -run TestRuntimeGrowth'`.

HTTP mutations use `approve` and return `operator_id`. Separate approval uses
`POST /api/topology/{version}/approve`; progress uses `GET /api/operators/{id}`.
The fixture retains its additional load/group/event and fault endpoints. The
small example remains limited to `_meta` topology/status/drain/activate.

In-process application and private metadata-ledger assertions run through
`make test-function`, in tagged `*_func_test.go` files in `tests/cluster/testapp`
and `internal/managed`, together with exact retirement
and readmission tests. Process tests still prove missing-store parking, persisted
identity, fresh-ID reseating, verified cleanup and recovery from actual WAL data;
its inspection endpoint reads the caller-supplied store rather than meta internals.
