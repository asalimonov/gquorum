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
effect barriers. Barriers can hold prepare, add learner, promote, demote,
leadership transfer, stop, cleanup or delete
effects. Released effects recheck their complete authorization before changing
state. The transport also routes local actions through the Node receiver, so
fault injection covers the same path on every host. RPC cancellation releases its own wait; the armed barrier
remains for a later retry until the test explicitly releases it.

Action errors preserve structured `membership.SafetyRefusal` details in the
existing reply data envelope, including on loopback calls. A transient loss of
post-change live quorum therefore keeps the affected operation block waiting instead
of becoming an unclassified terminal failure. Unknown detail schemas retain the
conservative unstructured refusal; error messages are never parsed for policy.

Local inspection includes the library's native proposal-admission observation.
`POST /internal/test/propose/{group}/{replica}` submits to the exact local
engine without aggregate peer probes. The thirty-second pending-waiter test
explicitly sets `-proposal-timeout=1m`; other cases use the library's thirty-second
default. The control's HTTP deadline is one minute and ordinary application
requests keep their ten-second deadline. Request cancellation participates in the
application's owned effect lifetime and engine waiter cleanup.

`GET /api/load/{id}/read/{key}` queries `Group.Read`: any ready local hosting
replica, including a follower or learner, obtains leader-confirmed ReadIndex,
waits for its own application state and runs the optional KV Query on the machine
owner. Successful replies identify `consistency: linearizable`; missing keys
return 404 after the same barrier. Query errors do not halt the replica. The
existing `GET /api/load/{id}/keys/{key}` remains an explicit local observation
with `consistency: local`. Both routes apply the caller's serving gate; neither
redirects a stopped local handle. The process-test `Cluster.Read` helper uses the
consistent route. No internal authority data appears in application query payloads.

The typed load transport preserves `ReplicaQuery.Native` and `ReplicaStatus.Native`
through its authenticated ReadReplica route, including exact applied indexes and
member/replica identities. Diagnostic replies do not supply native proof. Node
performs the authority and identity checks after the protocol validates its header
and route. Pending authority maps to HTTP 503; a fenced grant or replan-required
result maps to HTTP 409.

Approved load learners can serve local observations and forward writes through
their Raft leader. The serving gate reads exact group/node convergence from one
bounded ReadProgress page and checks the local handle plus matching native role, so prepared replicas cannot serve early. Bootstrap gives
the `workers` group an explicit two-voter target and places learners on remaining
eligible hosts. Group administration also exposes `voter-config`, `hosting` and
`pinned-learner` mutations through the fixture's public API adapter.

`PUT /internal/test/fences/{group}/{host|group}` arms a resource fence delivery
barrier. Its body requires the current node/member/process `identity`; group
barriers also bind the exact local `replica_id` (zero selects the current one).
`purpose` defaults to revocation `fence`, `min_revision` selects newer grants,
and `after: true` holds response delivery after Node durably installs the fence.
The default holds delivery before installation. Cancellation ends an individual
wait while the barrier remains armed for retries; authenticated `DELETE` releases
it. A former-member or replaced group-replica control cannot hold its successor.

`GET /internal/test/authority/{group}` returns bounded owned captures of delivered
actions, successful receiver receipts, installed fence evidence and armed fence
barriers. Captures keep exact uint64 revisions and clone immutable grant/proof
fields. They are diagnostic and do not authorize an effect. Authenticated
`POST /internal/test/authority/{group}/action` replays a supplied action through
Node's ordinary ingress with the original courier/grant/target; refusals do not
produce completion receipts. `GET /internal/test/revocation/{operation}` uses the
public safe `Cluster.ReadAuthority` API to observe pending/completed obligations.
These controls let process tests distinguish approval, local fencing and receipt
acknowledgement, including a lost fence reply.

`PUT /internal/test/reads/{group}/{replica}` arms exact local read faults using
an `identity` equal to the current node/member/process. Flags can drop append and
snapshot traffic (`drop_append`), drop all incoming load Raft (`drop_raft`), drop
only ReadIndex replies (`drop_read_response`), or hold the actual machine owner
in Apply/Query (`hold_apply`, `hold_query`). `DELETE` with the same identity clears
that exact control. All routes require the test credential. Other groups and
successor replica IDs remain independent. A dropped snapshot reports failure
instead of manufacturing snapshot delivery success.

Local inspection includes callback attempts, active/completed callbacks, actual
applied indexes, dropped-message counts, accepted ReadIndex reply indexes and
sent request/context counts. It retains one diagnostic read context per owned
replica, so retransmission does not grow the observation store. Controls are
volatile and disappear only after Node joins the machine and application Close.
Canceling a Query wait releases its callback without halting the replica; stopping
a replica cancels and joins even an armed machine barrier.

`read_queue` and `read_queue_stamp` expose the engine's existing metrics sample:
request limit, in-flight/queued requests, quorum/apply waits and local/remote
round counts. They share the Raft owner's generation and age; blocked callbacks
freeze the sample and status reads cannot freshen it. These fields are diagnostics
for queue-bound assertions, never read or authority evidence.

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

HTTP mutations use `approve` and return `operation_id`. Separate approval uses
`POST /api/topology/{version}/approve`; scoped convergence uses `GET /api/progress`
and exact execution status uses `GET /api/operations/{id}`. Explicit failed-attempt
retry uses `POST /api/operations/{id}/retry`; lost-response retries preserve the
original CAS/identity. Version-conflict errors preserve `rebased_to`.
The fixture retains its additional load/group/event and fault endpoints. The
small example remains limited to `_meta` topology/status/drain/activate.

In-process application and private metadata-ledger assertions run through
`make test-function`, in tagged `*_func_test.go` files in `tests/cluster/testapp`
and `internal/managed`, together with exact retirement
and readmission tests. Process tests still prove missing-store parking, persisted
identity, fresh-ID reseating, verified cleanup and recovery from actual WAL data;
its inspection endpoint reads the caller-supplied store rather than meta internals.

`-meta-fault-proxy=true` opts into a caller-owned metadata listener proxy for
resource-fencing scenarios. It forwards authenticated unary and client-streaming
protocol frames to the real library service and applies the existing fence
barriers to actual metadata `InstallFence` delivery or replies. Captured evidence
comes from successful real receiver replies. The proxy owns and joins its
listener, handlers and outgoing connection; normal process and performance cases
leave it disabled. No test-only hook was added to the library.

The proxy's authenticated `/internal/test/meta` control can withhold Raft message
and snapshot delivery to one exact metadata process/member while continuing to
forward control/proof RPCs. Its dropped-frame counter and the receiver's local
Target show metadata staleness. Snapshot drops return a transport refusal, and
controls cannot affect a successor identity.

For collection-outage tests, the same proxy accepts authenticated
`PUT/GET/DELETE /internal/test/collection-report` for one exact `storage.Identity`.
It holds only matching ReportCollection frames, records the immutable receipt
and releases individual canceled RPC waits. Other metadata traffic remains live.
`GET /internal/test/collections/{group}/{replica}` returns process-local Collect
call counts (bounded to 128 identities); restarting resets those counters. Durable
success evidence comes from the library journal, not these counters.
`-retention` explicitly configures the fixture lifecycle retention; zero preserves
the library default of seven days. Fast collection scenarios use a positive short
duration. The retirement wrapper delegates actual proof handling to WAL storage.

Collection recovery needs no application cleanup loop: Node resumes prepared
journal rows, retries original collected receipts, and releases markers only
after verified metadata acceptance/settlement. Tests copy the authority database
only while its process is stopped; opening a second live owner is forbidden.
