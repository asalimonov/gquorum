# Internal meta lifecycle

These are private service contracts used by Node. Applications use
`NewNode`, `Start`, `WaitReady`, `RequestStop` and `Stop`; see
[Node ownership](../architecture/runtime.md).

`internal/metaservice.New` constructs an owned handle without starting workers. `Start` begins
Raft and networking. Startup and shutdown serialize, including concurrent
`Start` and `Stop` calls. A stopped or quiescent handle cannot restart; create a
new handle with the same explicit database path to recover its pinned identity.

`Quiesce(ctx)` refuses new mutations and effect authorizations, cancels and joins
convergence, reconciliation, health, document watches and admitted effects, then
attempts session revocation while Raft is still available. Revocation is bounded
and best effort under quorum loss. It then cancels networking, joins admitted
RPCs, and stops the Raft runtime. The meta database stays open so Node
can join its managed load replicas before `Stop(ctx)` closes that database.

Both calls are idempotent. A timeout returns `fault.ShutdownTimeout` while the
same teardown continues; retry to join it. Storage remains owned until teardown
completes. Borrowed load storage, transport, event bus and local journal are caller-owned;
Node owns the managed effect adapter and successful factory applications.

`OnSelfRemoved` runs asynchronously after applied membership is durable and an
approved Target excludes the local node or replaces its exact member ID. Staged
edits, drain, learner demotion, outgoing joint membership and another member's
removal do not trigger it. The handle immediately refuses further mutable work,
even with no callback. `Status.SelfRemoval` reports its identity, approved
version, applied index, notification and callback failure. Callback failures
preserve the fence and emit a local runtime error when events are configured.

A local membership-seen marker persists in the same bbolt transaction as applied
membership. It distinguishes pre-membership joiners and survives compaction.
Recovery can notify again after a crash, so the callback must be idempotent.
It must honor cancellation and return after scheduling application teardown;
use `RequestStop()` from the callback. Waiting for the same handle's `Stop` or
`Quiesce` inside the callback would wait for that callback itself. Successful
quiescence joins every admitted lifecycle callback.

The leader sends a bounded final, prefix-checked append carrying a committed
removal, after application durability. Retained document identities permit that
outgoing delivery after activation while incoming retired identities remain
refused. Peer pruning drains admitted messages before closing their connection;
Stop cancels and joins those retiring workers. Delivery to an offline peer is
best effort; normal explicit join and
fresh-ID readmission remain the recovery path. No local membership is invented
from an unverified notification.

Tests in `internal/metaservice/lifecycle*_test.go`,
`internal/metaservice/removal*_test.go` and
`internal/metastore/membership_test.go` cover cancellation and joining, concurrent
startup, durable notification, source removal predicates, failure observability,
outgoing routing and snapshot recovery. Integrated cases remain outside short mode;
isolated cases run in parallel.
