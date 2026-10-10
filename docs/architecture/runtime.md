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
