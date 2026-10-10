# Process-test application

The Dockerfile builds the dedicated caller-owned key/value fixture in this
directory. It owns its FSM, load protocol, effect receiver, process lifecycle
and fault instrumentation. It uses the public library APIs. The Compose demo
is a separate application; fixture controls and integration tests stay here.

Test controls live in `hooks_cluster.go`. They provide direct
local replica identity/membership/materialized values, durable snapshot size and
index, received-snapshot counts, exact successful effect receipts and cancelable
effect barriers. Barriers can hold prepare, promote, stop, cleanup or delete
effects. Released effects recheck their complete authorization before changing
state. Controller RPC cancellation releases its own wait; the armed barrier
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
retirement joins the worker before application storage teardown. The fixture runs independently of the ordinary Compose demo.

Every control request requires the fixture's fresh credential in
`X-Gquorum-Test-Credential`. The normal application protocol keeps its existing
credential and exact cluster/member/replica checks. None of these controls add
library configuration or load-protocol fields.

Use `make test-cluster-build` to build the image. Run focused scenarios with
`make test-cluster-run CLUSTER_ARGS='-race -v -run TestRuntimeGrowth'`.
