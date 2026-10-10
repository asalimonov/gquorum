# Process-test application

The Dockerfile builds the caller-owned key/value application in
`examples/cluster` with the `clustertest` tag. It shares the example's actual Raft
state machine, application transport, effect receiver and lifecycle. The normal
Compose image compiles without this tag and exposes no test control routes.

Test controls live in `examples/cluster/hooks_cluster.go`. They provide direct
local replica identity/membership/materialized values, durable snapshot size and
index, received-snapshot counts, exact successful effect receipts and cancelable
effect barriers. Barriers can hold prepare, promote, stop, cleanup or delete
effects. Released effects recheck their complete authorization before changing
state. Controller RPC cancellation releases its own wait; the armed barrier
remains for a later retry until the test explicitly releases it.

Every control request requires the fixture's fresh credential in
`X-Gquorum-Test-Credential`. The normal application protocol keeps its existing
credential and exact cluster/member/replica checks. None of these controls add
library configuration or load-protocol fields.

Use `make test-cluster-build` to build the image. Run focused scenarios with
`make test-cluster-run CLUSTER_ARGS='-race -v -run TestRuntimeGrowth'`.
