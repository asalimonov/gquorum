# Runtime ownership and load recovery

`gquorum.Runtime` manages handles for one node and cluster. `New` opens no
stores and starts no workers. Open `_meta` first with `OpenMeta(meta.Options)`;
the options carry the caller's identity, limits, database path, listener,
credentials, timers and integration callbacks. Start the returned meta handle
with a caller context and wait for readiness. `Limits` is the sole capacity
setting; its default maximum node count is three.

Meta construction checks retained stores without writing them. A complete
versioned identity pin can resume its exact member. An absent pin requires a
fresh database, including absence of local ledgers, overrides and membership
markers. Partial pins, incomplete/unsupported schemas and orphan retained state
are refused. Caller-owned readmission cleanup must leave a genuinely fresh
database before a new member pin can be installed.

`OpenLoad(load.Options)` constructs a load engine with the caller's machine,
transport and store. Start and wait for readiness on the returned handle.
Constructor readiness and elected leadership are separate: a restored engine
can replay its committed tail before it has a leader. Load stores, machines,
transports and a supplied event bus remain caller-owned. The runtime inherits
its logger and event bus into options that omit them.

`load.Group.ProposalStats()` observes the local admission limit, in-flight
callers, queued proposals and registered commit waiters. The commit-waiter
count comes from the Raft loop; its refresh and the concurrent channel counts
are independent observations. Canceled callers are pruned while persistence
is blocked. Cancellation does not roll back an already proposed command;
the caller's machine must deduplicate exact request identities on retry.

`Transport.Send` must honor cancellation and accept concurrent calls for different
destinations. Calls to one destination are serialized. The engine copies outbound
messages and routes them through a bounded queue per destination, so a stalled
peer cannot delay healthy-majority traffic. Removed peers discard obsolete queued
traffic while retaining a bounded attempt to deliver the committed removal.
Idle removed-peer workers are released; successful `Stop` joins all send workers.

An attached meta handle requires the exact approved Target node, cluster,
member and load-replica identities. Staged Latest intent cannot open a load
replica. The observing process ID defaults to the current meta process and an
explicit different process ID is refused. Fresh founding voters must match
the approved voter set; intentional learners use joining mode. A prepared
replica joins without founding peers. Retired load-group IDs remain refused.

Topology effect receivers must call `meta.Group.Authorize` with the complete
execution, version, attempt, step, operation, group, node and replica tuple.
They must verify an existing effect before acknowledging an idempotent retry.
Runtime boot checks add local identity protection; they do not replace effect
authorization or the application's protocol validation. Applications own any
transactions or outboxes needed for their external effects.

The node-local meta bbolt database records each opened load identity in a boot
ledger, independently of replicated topology. Opening checks storage before
binding it, restores or creates durable state, then records the identity.
Repeated ledger writes preserve the first audit fields. Approved reseating
replaces only the exact previous ledger ID through a bbolt transaction.

Boot decisions preserve these cases:

| Local evidence | Result |
| --- | --- |
| New approved identity, no prior store | Fresh creation or joining preparation |
| Matching store, missing ledger | Adopt the durable store |
| Matching store and ledger | Resume |
| Approved fresh ID replaces an older ledger ID | Reseat with exact ledger replacement |
| Previously opened identity loses its store | Park; retain its ledger |
| Active or completed membership proves establishment, but the log is empty | Park |
| Valid prepared learner has an empty log | Resume preparation |
| Foreign or unreadable store | Park; preserve durable state |

`LoadParkedError` matches `fault.ErrReplicaNotReady` and retains any underlying
error. `ParkedLoads` returns owned local observations. Restarts reconstruct
parking from durable evidence; volatile timestamps and observations are not
replicated. Successful construction of a restored or freshly approved replica
clears its observation. The caller must still start it before serving.

The runtime never automatically removes a foreign WAL. Caller code uses the
store's exact-ID `Quarantine` operation as part of its verified recovery or
cleanup policy. `StopLoad(ctx, groupID, expectedReplicaID)` joins and unregisters
exactly that handle while preserving its store and ledger. Authorized storage
cleanup may call `ForgetLoadReplica`; whole-group deletion supplies the exact
old local ID. Delayed cleanup cannot erase a replacement ledger entry.

`Close` first quiesces meta work and joins admitted callbacks, then joins load
handles, then closes the meta database. Timeout retains ownership while the
same teardown continues. Accessors remain available during teardown. Callbacks
that initiate whole-runtime shutdown call `RequestClose` and return; waiting
for `Close` inside a callback that it must join would deadlock. Constructors
already in flight finish before the shutdown coordinator captures handles.
New constructors and local stop operations are refused after shutdown starts.

Standalone runtimes can open load engines without meta. Their callers supply
consensus identities, membership approval and protocol validation; no meta
boot ledger exists in that mode. `load.New` also remains available directly.

`meta.LoadStoredIdentity(path)` reads a closed database's validated durable pin
without creating or mutating storage. It intentionally returns no process ID.
Caller readmission hooks can compare the exact old node, cluster and member tuple
before moving their supplied path; an absent pin does not prove an existing file
is fresh (the constructor separately checks all retained state).

An effect receiver behind the required execution progress returns retryable
unavailability only after the immutable retained plan matches every authorized
field and the attempt is current. It never authorizes a pending or future step.
Stale steps, retired attempts, terminal executions and mismatched tuples remain
permanent authorization refusals.

`meta.Group.SetGroupPlacement` edits one load group's policy and desired replicas
through the same CAS/preview/approval pipeline as other mutations. It retains
eligible identities, assigns fresh IDs on expansion and refuses an unsatisfied
minimum. Approval uses observed membership and the fixed current-quorum floor.
Staging can succeed before approval fails; callers must inspect the committed
version instead of assuming a refusal rolled the edit back.

`topology.GroupStatus.Term` reports the observed Raft term alongside leader,
commit and applied cursors. Compare both leader and term when checking
leadership continuity: a leader can step down and regain leadership between
samples while retaining its ID. Each status remains a locally applied observation.

`meta.Group.LocalStatus()` reads owned local meta consensus state without
probing peers or invoking application controllers. It reports the exact local
host, term, leader, membership and cursors; `Found` is false before the engine
starts and after it stops. Use the aggregate `Status(ctx)` for cluster/load
observations. Neither method supplies a linearizable read or proves quorum.

The `_meta` Compose demo is intentionally small. The full application-defined
load FSM/protocol, exact effect receiver, recovery adapter and fault controls
belong to the independent `tests/cluster/testapp` fixture. Library integration
and process-cluster scenarios reside in `tests/cluster`; the example compiles
without test instrumentation or a load protocol. Its actual Compose REST e2e
also lives in the test module.

The fixture's restart policy reads the public durable meta pin before constructing
its runtime. A retained store resumes without obsolete founding declarations;
a missing store with the caller's affiliation marker requests fresh admission.
It retains no cluster events in the meta quorum. Its authorized cleanup callback
serializes stop, identity probe, optional quarantine and ledger retirement with
local opens and retries. An already absent store satisfies cleanup, while a
mismatched successor remains protected by exact approved-identity verification.
The caller's local serve override can relax readiness using fresh health data;
Raft membership, activation sessions and write quorum requirements stay intact.

The process fixture optionally holds application bootstrap/HTTP readiness until
all configured founders are observed with the declared meta IDs. The meta service
and liveness remain available during this caller-owned gate; a late founder needs
no peer restart. Its caller-supplied deadline requests joined shutdown and reports
a fatal startup error to the executable. The successful startup marker is written
through a synced temporary file, atomic rename and directory sync before opening
the gate. Timeouts retain no successful marker; established retained-state recovery
bypasses the initial gate with only a healthy majority. This policy is fixture code,
not a library configuration dependency or a change to meta.WaitReady.

Join identity refusals are terminal: a live name fence or incompatible approved
identity/type wakes WaitReady with ErrIdentityMismatch and joins the stopped
runtime. Availability and pending/excluded admission refusals continue parking
for approval. The leader retains its volatile exact-process name_conflict warning
after the refused process exits; it cannot poison the holder's cached route or
allocate membership. Followers read that current leader-owned warning through
ParkedJoins. Executables decide how a terminal library error affects their process;
the process fixture propagates it to joined fatal startup/exit 1.
