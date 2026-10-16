# Node ownership and load recovery

`gquorum.Node` owns one participant, its private `_meta` service and all managed
load replicas. `NewNode` performs pure validation. `Start` acquires storage and
networking and begins recovery without waiting for quorum. `WaitReady` observes
mandatory admission/recovery/session requirements plus `lifecycle.Hooks`.
The default gate is local admission; `lifecycle.AllFounders` can require every
configured founder on the first bootstrap. Retained recovery does not repeat
that initial gate. Node never loads application configuration.

`Cluster` is a stable administration view of Node. `load.Group` is a view of one
exact local replica. Neither is an independent lifecycle owner. The sole capacity
settings are in `topology.Limits`; the default node maximum is three.

## Startup evidence and local ownership

`NodeOptions.StorageDir` is required and belongs outside application/load
retirement roots. The library derives fixed `_meta.bolt` and `_authority.bolt`
paths; callers cannot override either filename. Constructor validation performs
no I/O; startup rejects aliased system files and resolved paths inside the
configured WAL root. It also refuses former `meta.bolt`/`authority.bolt` filenames
without modifying their evidence; there is no automatic filename migration. Node
acquires the authority database before creating a metadata pin and keeps it open
until managed callbacks, replicas and metadata have joined teardown. Fresh joins
acquire it under their exact granted member identity. A missing file beside a
retained meta pin or load evidence is an error, never an invitation to recreate
empty fences.
`storage.Store.HasState` must report evidence for all groups, including unknown
and partially created groups absent from the metadata catalog.

Meta opening validates the complete versioned identity pin and retained state
before writing. An absent pin requires a genuinely fresh database, including
absence of ledgers, overrides and membership markers. Partial pins, unsupported
schemas and orphan retained state are refused. A retained pin resumes its exact
member; a new process supplies a fresh InstanceID.

Managed hosts require the authority-store accessor, quorum grant/courier validators
and current-floor reader at construction; construction only checks their presence.
Every managed engine enables authority enforcement. Startup and readmission have
no path that omits durable authority, including metadata-only readmission.

Managed startup derives identity and mode from approved scope; founding peers
come from the immutable creation grant. Once removal excludes an identity from
Target, its retained restart remains refused even if native removal is still
pending; recovery does not widen membership admission.
Staged Latest alone cannot authorize a replica. The local bbolt ledger and actual
WAL pin are inspected before application acquisition. Durable store creation or
restore precedes recording the identity. Repeated ledger writes preserve original
audit fields; reseating replaces only the exact prior ID transactionally.

| Local evidence | Decision |
| --- | --- |
| New approved identity, no prior store | Fresh founding or joining preparation. |
| Matching store, missing ledger | Adopt that exact durable store. |
| Matching store and ledger | Resume. |
| Approved fresh ID replaces an older ledger ID | Reseat with exact ledger replacement. |
| Previously opened identity loses its store | Park and preserve its ledger. |
| Proven membership or retained creation/native receipt establishes the identity but log is empty | Park; never silently refound an established identity. |
| Valid prepared learner with empty log | Resume preparation. |
| Foreign or unreadable store | Park and preserve the evidence. |

One startup reservation owns each group. Concurrent requests for the same exact
replica wait for that startup and share its handle; other groups proceed
independently. Canceling a waiter does not cancel the owned startup. Factory
inputs are copies. A successful factory transfers `Application.Close` to Node,
including when cancellation occurs before its late return. Factory failure leaves
partial acquisitions with the factory. Node joins replay, engine work and optional Application.Run before
Close. Failed Close retains ownership and blocks replacement until a serial retry
succeeds. Publication occurs only after local replay readiness. Factory ApprovedVersion is
context: after a wait, compare the exact group/replica and founding seed, rather
than the global document version. Recheck newly learned creation/establishment
proof against the original storage probe before opening the engine. Recovery
rotates its next attempted reservation before waiting on a fence, preventing one
blocked group from monopolizing every bounded round.

`StopLoadGroup(ctx, ref)` installs an exact volatile hold before joining the
replica. Recovery and ordinary approved starts respect the hold. An explicit
start clears only that matching hold; a fresh Node has no previous lifetime's
holds. Retirement can still join and remove a held identity. A stopped handle
never becomes a view of its successor. `ParkedLoads` and `Node.Status` return
owned observations. `LoadParkedError` matches `fault.ErrReplicaNotReady` while
preserving its cause. Successful startup clears the matching parked observation.

### In-memory load groups

A group's approved `Storage` class selects its store at preflight: the default
class uses the configured WAL or borrowed store, and `topology.StorageMemory`
uses an in-memory `storage/memstore` that Node creates whenever loads are
configured and closes after the managed host joins. Paths that can run after a
group left approved topology (cleanup, retirement, readmission and collection)
probe both stores, and retired records are collected through the store that
listed them. State for a group in the store of the other class, or in both
stores, parks as `identity_mismatch`. The startup authority check still reads
only the default store; the memory store is always empty at startup.

The decision table above is unchanged. A memory replica that restarts finds no
state, so an established identity parks as `store_missing` with its ledger
preserved. For an established voter, the host then reseats its own replica: it
calls the ordinary `ReseatReplica` for its node only when that exact approved
voter matches the group's proven declaration, with no group owner/deletion.
Each reseat call pins current Latest once; scoped repair approves only the exact
replacement and rebases independent staging. Overlapping staged intent conflicts. A busy or unconverged sibling does not block this check. Refusals retry
with backoff up to 5 seconds. Recheck local holds after progress reads. The approved
plan prepares a fresh learner, catches it up from the leader, promotes it and
cleans up the old identity. The loop ends when the exact old assignment leaves
Target or is explicitly held; host shutdown joins it. The internal local progress
projection remains schema3 until the complete scoped activation in Task78.

Learners and replicas that restart before becoming established are not reseated;
they stay parked with a `NotImplemented` warning. If a majority of a memory
group's voters restart together, its committed state is lost: the group cannot
elect a leader or complete a reseat, and it stays unavailable until it is deleted
and recreated under a new ID. All nodes must run a release that understands the
storage class before memory groups are created.

## Approved actions, transport and retirement

The internal operator controller dispatches an immutable approved plan. Typed
load requests distinguish sender, receiver and target replica. Node receivers
verify the entire operator/version/attempt/step/action/identity tuple and recheck
authority after waits. Unknown future approvals, stale attempts and mismatched
identities never authorize work. A remote refusal may be retried by a dispatcher
whose complete local approval is still valid; refusal is never completion proof.
After inherited resource fences complete, a safe metadata query and native
membership reads validate the remaining immutable plan before each step. Missing
quorum remains a wait; an impossible membership sequence or omitted retained
cleanup fails with `replan_required`. Same-version retries keep the exact plan,
and snapshot restoration rejects differing plans for attempts of one version.

The application supplies `load.Transport` and authenticates its protocol before
calling Node's typed receivers. Calls for different destinations are independent;
the Raft engine copies messages into bounded per-destination queues. Slow peers
cannot block healthy-majority delivery. Snapshot completion reports identify the
issued delivery and exact source/destination process and replica. Old reports
cannot reach replacement handles. Successful shutdown joins send workers.

`load.Group.ProposalStats` exposes pending limits, queued work and commit waiters;
these are local observations. Canceling a proposal wait cannot undo a committed
command. State machines must deduplicate application request IDs on uncertain
retries. `WALStall` and `Error` expose local persistence/engine diagnostics.

`load.Group.Metrics` and `MetricsHistory` return owned local observations for
that exact engine run. `NodeOptions.Metrics` bounds sampling, source ages and
history. Raft and writer generations advance only on their respective owners;
independent aggregation cannot freshen a blocked owner. Raw history is volatile,
bounded by count and retention age, and never becomes recovery or membership
authority. A stopped handle cannot read its successor's series.

The meta-leader collector has an independent joined lifecycle. It samples during
scheduler callbacks, cooldown and approved operator execution. A shared bounded
read pool spans group destinations, rotates dispatch order and retains slots for
callbacks that have not returned after cancellation. An undispatched request or
rejected identity cannot become failure evidence. Source generations, process
and engine-run identities, term, membership and voter-configuration scope fence continuous
windows; reading cached data never earns extra qualifying time.
Collector counter exhaustion cannot reuse an observer generation or history
cursor: it invalidates decision evidence until a fresh collector starts, while
retaining bounded diagnostic history.

Native load membership reads use a separate Host-owned pool, bounded by the
same `Metrics.MaxParallelReads` setting across all load groups. Their read
budget is at most `CollectTimeout` and half the remaining effect/caller budget,
leaving time for planning or dispatch. Partial replies from an exact leader and
healthy peers remain usable; missing replies never count as live voters. Caller
or owner cancellation invalidates the observation. A stuck callback retains its
permit and exact identity exclusion until return, and Host shutdown joins it.
Keeping this pool separate prevents native status callers from consuming the
temporal collector's permits.
Concurrent native rounds for the same group wait on a separate observation gate,
so another observer's active read cannot appear as missing peer health. The
read pool releases an exact-source exclusion before publishing a completed
result; callbacks that ignore cancellation retain it until they return.

Completed role effects are materialized in replicated metadata. An observer
measures cooldown from its first observation of each record using monotonic time;
a new observer epoch conservatively rearms the configured duration. New-failure
exceptions require a healthy observation after that record followed by qualifying
failure. Recently demoted replicas keep their own cooldown. Approved operators
resume their immutable steps independently of these decision delays. Demotion
preserves replica identity and storage; it does not retire the replica.

Managed cleanup runs through granted action ingress, using the exact targets
in the immutable grant and its installed durable host fence. There is no
approval-only retirement entry point or ungranted ledger-forget callback. The
library joins the owned replica, probes retained storage and verifies its ledger. If application state
exists outside Raft, `LoadOptions.Retire` must independently verify its complete
identity and request proof (`RetirementRequest.Verify` checks the identity,
grant resources, target and reason-specific tuple), perform idempotent cleanup,
and return the entire matching `RetirementResult` with `Verified=true`
(`RetirementRequest.Result`). Absence on retry still requires
verification. A foreign or successor resource must be preserved. Only then may
the library quarantine the exact Raft state and forget its matching ledger.
Application transactions and outboxes remain caller-owned.

`storage.RetirementStore` enumerates exact retired identities and opaque tokens.
Lifecycle retention decides when collection is eligible; collection rechecks the
identity/token, local ownership and ledger. Complete exact retirement proof
provides permanent assignment exclusion because retired replica IDs cannot be reused. There is no implicit age-based
quarantine sweep. Retained approval evidence survives history pruning until its
cleanup obligations can be proven complete. Staged-only identities never enter
that retirement authority.
The independent host authority journal now uses schema 2. Its collection bucket
persists exact retirement proof before deletion and a collected receipt afterward.
Preparation assigns one durable sequence; completion retries retain the first
host timestamp. Rebinding preserves historical member IDs and proof. Exact
collected-row removal follows reporting and storage-proof release. Journal pages
are bounded owned values; their exclusive cursors survive row removal and rebind.
Schema 1 is refused without migration. Managed collection scans journal pages
as well as storage enumeration. New attempts require complete exact local
retirement proof before serialized retention; preparation survives later policy
changes. Accepted attempts use the shared group gate and recheck local resources
before deleting. Fenced hosts can resume accepted deletion without current
metadata membership or quorum. Collected rows never repeat deletion. Reporting
scans bounded pages and verifies the exact receipt echo plus nonzero applied
index, for both acceptance and read-only settlement. It releases the store marker
before removing the exact journal row. Lost replies or either failed release
retain evidence. Local readmission reservation, current-member readmission floors
and incomplete superseding fences postpone release. Historical completed floors
cease blocking after member rebind. Collection maintenance uses the authority
lifetime: participation fencing leaves it running; Stop/readmission cancel and
join it, including delayed callbacks. The restricted MetaControl service now
provides ReportCollection and ReportCleanupStatus callbacks. It authenticates the
request process, confirms exact absence before settlement, and otherwise checks
the historical courier before proposing receipt acceptance. Only missing history
permits a repeated absence check; authentication/conflicting proof is never
suppressed. A retained-member hint follows AuthorityRead semantics when the
participation header is zero or lags the journal pin. General ProposeMeta refuses
collection/status envelopes. Node supplies both callbacks and its existing journal
to the managed Host; its accepted lifetime owns reporting, including explicit
control-only joining restarts. RequestStop cancels report waits; delayed storage
callbacks remain joined before owned stores and the journal close. Borrowed stores
remain caller-owned. Scoped state acceptance remains Unsupported on schema3 until
the complete Task78 activation; an unsuccessful report keeps its durable receipt.

WAL collection writes and syncs an exact sibling marker before deleting contents,
keeps the identity pin until those deletions are synced, and finishes with
nonrecursive removal of the empty directory. A retry accepts an empty unpinned
directory only with that marker. Missing data without proof returns an error
matching fs.ErrNotExist. Atomic marker publication requires hard-link support on
the WAL volume; publication failure preserves retired data. A crash can leave a
.collection-* temporary file: inventory conservatively treats it as retained
evidence, while retired enumeration ignores it. ForgetCollection releases only
an exact marker after the retired directory is absent; it syncs the root even on repeated release.
Memory storage retains equivalent retry evidence only within its lifetime and
cannot prove collection after process loss.

Planning also retains unfinished preparations in surviving groups. Even an old
replica absent from native membership needs an exact remove/stop/cleanup sequence;
the remove can produce an already-satisfied receipt under its new grant. New
plans omit proven cleanup, while an immutable retry retains the exact historical
identities named by its previously approved steps.

## Readmission and removal

`ReadmitNode` rotates a declared active node's meta/load identities only after
fresh health reports it dead. Live/unknown health or unconverged documents are
refused before staging. The ordinary CAS/optional approval pipeline preserves a
staged version on later approval failure. Retry `Approve` for that version.

A returning process joins with its caller-supplied retained paths. The library
validates the exact admission grant, fences and joins the former host, verifies
actual storage against the retained approved catalog, and invokes application
retirement with a readmission proof. It quarantines exact old load state and
moves the closed old meta database to a deterministic backup derived from its
member/admission identity. The backup retains its ledger; a verified retry can
resume after a prior move. Foreign pins, symlinks and replacement state are
refused. A new host starts only after the exact granted fresh meta pin is installed.

The authority FSM binds readmission grants to an explicit former member proved
by approved history or retained grants, including after completed exclusion.
Its opaque admission identity includes operator, granted member, node and process;
different nodes may use the same process identifier without sharing grants.
When a pending admission changes process for the same member and operator, the
FSM retains its original retirement process in `ReadmissionInstanceID`. The new
process receives that same immutable grant batch and deterministic backup name.
Only the FSM assigns this lineage; snapshots require the exact original
superseded admission. Readiness still belongs to the newly bound process.
Before rebinding an admission to another process, Join probes the previous cached
endpoint for that exact admitted node and instance. This process probe stays
available through metadata retirement, absence and replacement, and through
participation fencing. A live reply prevents a competing instance from taking
the admission or replacing its route. Full protocol shutdown ends the probe.
Its reply proves only process liveness; native health, readiness and effect
completion retain their separate checks.
The metadata grant covers the exact former pin, and load grants exclude approved
successor replicas. Replacement metadata preparation waits for all of the
admission's resource receipts; a completed fence alone is insufficient.

Approved plans retain the full replacement metadata admission sequence even when
the new member is already observed as a learner or voter. That preparation is the
receipt-gated host retirement boundary: the plan omits redundant stop and cleanup
calls to the former metadata identity, while retaining native membership removal.
Metadata steps run before load groups, which then run in name order. Deletion of
a group hosted entirely by returning owners is covered by their preparations;
partial deletion still retires the other hosts. Actual readmission receipts remove
only their exact load identities from subsequent deletion grants. Planning retains
those identities for native removal until the replacement becomes Active.
Completed retirement receipts survive grant closure and can satisfy preparation
by a successor operator for the same exact granted member. They do not authorize
a different member or make a superseded admission ready.

Join carries the independent retained authority member separately from the live
metadata member. The leader commits the readmission batch and returns its exact
former member, grants and load scopes. The receiver validates this whole response
before changing routes or retiring storage. Node supplies both the retained
authority identity and receipt lookup. Receipt proof is collected before opening
the replacement metadata engine and accompanies every JoinReady retry.

Metadata records a receipt batch atomically against its exact admitted identity.
Historical completion remains acceptable after supersession and may settle its
revocation, but cannot make the superseded admission ready. Fresh-member readiness
requires all readmission resource receipts. A still-owned in-process grant also
permits retry after rebind when its old engine/store are joined and the live meta
path is fresh. After a process restart, the rebound admission retains the same
retirement proof. Before rebind, retirement resumes against its retained receipts
and exact backup. After rebind, Node verifies the durable previous/granted/member
transition and admission ID before returning receipts; the successor is not
retired again. Complete operator convergence remains tracked in Task 15 of the
authority-contract implementation plan.

The independent authority store records metadata acquisition for its current
durable member. Node records it after the metadata pin is durable and before the
engine is published or started. A failed acquisition hook closes the store and
leaves the pin for a retained retry. Rebind to a newer member clears this marker
atomically; retrying the same rebind preserves it. Once recorded, missing metadata
cannot be recreated under that member, even if a Join response names it. A fresh
approved readmission is required. Before recording acquisition, an interrupted
creation can resume against the exact granted identity and original readmission
proof after the new process receives its committed admission binding.

The managed retirement boundary accepts the exact readmission grant batch. With
Node-owned authority configured, it validates the full prior scope before writing
any floor, persists all pending host fences, and joins participation and retained
administrative callbacks before completing those fences. Each application/storage
retirement and the final metadata quarantine has durable intent and an exact
receipt. Retrying completed work skips the application callback but rechecks that
the old live storage is absent. Contradictory storage stays intact and parks the
scope. Node joins any metadata administrative worker before this boundary and
requires all receipts before rebinding its authority pin. The authority database
and former metadata backup remain owned and retained across this transition.

`LoadStoredIdentity(storageDir)` reads `_meta.bolt`'s validated durable pin without
creating or mutating storage and intentionally returns no process ID. Callers
may use it to choose retained restart versus joining. Configuration and any application
affiliation marker remain outside the library.

After applied, durably authorized self-removal, Node immediately fences meta and
load participation. `lifecycle.Hooks.Removal` chooses stop or retained diagnostics
only after mandatory fencing/joining. A failed or delayed callback cannot undo
the fence. Removal proof includes exact process/member and approved version/index;
staged edits, drain, demotion, outgoing joint membership or another member's
removal do not trigger it. The durable membership-seen marker survives snapshots.

Join classification waits for native and durable FSM cursors to cover the
committed current-term state. Identity conflicts are terminal and wake WaitReady;
availability and pending/excluded admission can remain parked. A leader's volatile
exact-process name-conflict diagnostic never displaces the live holder's route or
allocates membership. `Cluster.ParkedJoins` exposes these observations.

## Shutdown, reads and decision callbacks

`RequestStop` schedules teardown; callbacks may invoke it and return. `Stop(ctx)`
joins the same owned teardown, retaining ownership on timeout. Node cancels its
lifetime, quiesces meta producers, joins admitted effects, decision callbacks and
managed replicas, closes owned load storage, then closes meta storage. Callback
or Close errors remain observable; unsuccessful release requires a serial retry.
No new replica can overlap an unresolved owner. A callback must not wait for its
own group's lifecycle or Node.Stop, which would wait for that callback itself.

Internally, metadata teardown separates participation from the protocol lifetime.
`FenceParticipation` rejects new ordinary work and joins existing effects, service
workers and Raft while retaining the transport and database. `Quiesce` additionally
cancels and joins protocol calls and the removal callback; `Stop` then closes the
database. A timed-out waiter retains ownership at either boundary. The intermediate
phase cannot restart Raft or serve ordinary authoritative reads. Node's current
full teardown still uses `Quiesce`; authority acknowledgement integration uses
the separate participation boundary.

The managed Host also separates ordinary work from granted host administration.
Its internal `FenceParticipation` joins application replicas while retaining
host fences and granted stop/cleanup/delete effects. Both lifetimes share group
serialization; new creation, preparation and native group effects remain fenced.
Full Host.Stop and readmission close authority admission and join its callbacks
before releasing resources or retiring metadata. Node's granted action/fence
ingress retains this exact owner while fenced. During readmission, a separate
Node-owned worker accepts only exact host revocation fences. It persists the
new floor, cancels and joins the retirement callback, joins the former Host's
remaining work, fences uncertain older intents without inventing receipts, and
then completes the boundary. Courier cancellation only ends its wait; one worker
bounds the queue. A callback that has not returned keeps revocation pending.
New readmission attempts cannot overlap that worker. After completion, a newer
authenticated grant can replace the canceled admission reservation and reconcile
the old Host without restarting its ordinary lifetime. Full shutdown refuses
ingress. Node starts its Host before exposing metadata control networking.
Ordinary load operations keep the participation gate. Receiver-side grant and
courier validation applies to retained ingress.

Injected stores, transports, event buses and journals remain caller-owned. A WAL
store selected through `LoadStorageOptions.WAL` is Node-owned. Close borrowed
resources only after Node has joined. The factory's returned application resources
are the exception: ownership transfers on success through `Application.Close`.

Scheduler calls are serialized per node. The lifecycle runner serializes readiness,
retention and removal callbacks together. Deadlines bound accepted results; a
noncooperative callback stays owned until actual return, with no overlapping
replacement invocation. Policy decisions cannot bypass core authorization,
persistence, identity or membership checks. Scheduling status reports
`policy_stalled` while a canceled callback has not returned. The scheduler refreshes
topology, health and native membership after the callback, then uses the ordinary
replicated CAS to guard approval against concurrent document changes.

Repair candidates are rechecked against the approved node selector,
reservation, learner-only marker and strict voter domains after callback
results. Retained startup uses the replicated approved labels and exact
identity; changed local `NodeOptions.Node.Labels` cannot override them. A
reserved approved replica may restart, while a successor ID still needs an
eligible host. [Placement](placement.md) describes these rules and their
diagnostic reports.

`Node.Status()` reads local owned snapshots without probes. `Cluster.Status(ctx)`
collects observations and separate static placement reports for Active and
Target; neither proves quorum, physical failure isolation or linearizability.
Compare leader and term when assessing continuity. Follower approvals make at
most one control hop to the observed leader. Domain errors preserve exact
expected/current CAS versions; empty success replies cannot identify an operator
and are refused.

Status adds `MetaLeaderNodeName` and `LeaderNodeName` to the existing numeric
Raft observations. Names resolve through the local Active topology. Unknown or
retired replicas have empty names; staged replacements cannot supply them.
Cached status re-resolves names against its current Active view.

The small `examples/cluster` app is a separate Go module. Kong parses its commands
and environment; `cluster.go` constructs Node and evaluates readiness, `http.go`
adapts the public API to HTTP, and `main.go` joins their lifetimes. It demonstrates
`_meta` topology, status, approval, drain and activation. Full application
FSM/protocol/retirement and fault controls
belong to `tests/cluster/testapp`; lifecycle orchestration belongs to the library.
See the [public integration guide](library.md) and
[ownership test mapping](../testing/runtime-ports.md).


### Authority control after participation fencing

`AuthorityRead` is a narrow, authenticated metadata RPC for a retained grant,
its committed receipts and the current courier identity, captured together by a
leader-confirmed FSM query. Joining hosts with no replicated metadata and hosts
whose participation is already fenced can request this evidence from the live
metadata leader. Public topology/application reads keep their ordinary lifetime
and do not redirect through this path. Fully stopped owners refuse proof reads.

When metadata is missing, the read-only proof request carries the independent
authority member as `reader_member_id`. The leader checks that it belongs to the
authenticated node through current membership or the exact retained grant target.
Grant-scoped identity probes and host revocation fences likewise use that retained
pin; Raft and ordinary protocol identity remain unchanged. A committed admission
publishes its control address before retirement grant issuance, so predecessor
fences can reach it while Join remains pending. Identity/name-conflict refusals
still cannot replace an accepted route.

An authenticated fresh endpoint may listen before Join installs its first member
pin. A scoped prepare identity probe returns `replica_not_ready` while that member
is zero; a conflicting nonzero pin returns `identity_mismatch`. Connection/DNS and
remote transient control failures retain the retryable `unavailable` classification.
Caller cancellation, transport shutdown and authentication refusals stay distinct.

The proof also identifies exact cleanup targets absent from approved Target.
A fenced metadata owner can use this evidence to forget an old load ledger even
when its stopped catalog predates the grant. It rechecks its captured identity,
durable metadata pin and exact replica-ID CAS. The managed host retains the group
gate until the cleanup receipt is durable, so successor fencing joins this work
before admitting later effects. Active owners keep the local FSM read lock through
ledger mutation. Managed action/fence ingress validates current courier identity
and retained grant together through the safe proof route; immutable grant cache
hits remain usable under protocol ownership after participation ends.

Meta host effect and fence ingress retain protocol ownership while validating
that proof; native metadata effects still require the active effect lifetime.
Grant-scoped identity probes verify the exact target member and courier, so host
routing can survive session deactivation without claiming that stopped Raft is
healthy. Ordinary identity probes continue to require a running engine. Node-owned meta host callbacks now serialize durable host floors and effect
receipts under a bounded owned worker. Prepare verifies the acquired pin and
replay-ready engine; stop joins metadata participation and local load work before
persisting its receipt. Courier cancellation ends only its wait. Removal hooks
waits until one quorum read confirms the exact stop receipt, convergence of
approved Target into Active, and settlement of this member's retained host
authority. Convergence covers later plan steps whose grants have not yet been
issued. A metadata stop receipt alone cannot trigger optional shutdown while
load cleanup remains. The local authority store retrieves the completed stop
even after a newer fence replaces its current floor; rebind cannot reuse a former
member's stop receipt.

Deletion grants exclude exact replicas already covered by durable cleanup proof,
so later group deletion does not resurrect work on a removed host. A retry after
whole-group deletion preserves the latest deletion grant's exact target set and
still obtains its own receipts; it does not reintroduce earlier retired replicas.

Private planning retains approved intermediate `_meta` identities and node
attributes as well as load identities. An admitted learner may already be native
before its document reaches Active; supersession must still remove and retire it.
Unresolved metadata preparation pins its approved document through bounded history
pruning. Inbound member routing uses an index rebuilt on FSM mutation/restore:
historical intermediate members remain routable until their exact native removal
is consumed, or a later Active proves convergence. Staged rows and already removed
members never authorize inbound traffic. Outbound final-commit/cleanup routing is
separate. Durable retirement receipts remain cleanup proof after their completed
operator leaves ordinary history, preventing retained grants from resurrecting
collected resources. Native removal alone preserves unfinished host retirement.
Deleting the node row in approved Target is also self-removal when its previously
seen member disappears from applied native membership. A caller whose startup
wait returns stopped for that proven removal should keep administrative control
available until required retirement finishes and its removal hooks decide the
process lifetime; pre-readiness removal does not establish application readiness.

A retained metadata stop intent (running, completed or fenced with uncertain
outcome) prevents the same member from reopening Raft after a process restart.
Fresh-member readmission must explicitly replace that identity and preserve its
evidence. An explicit joining restart opens retained metadata in control-only
mode when the durable authority guard refuses participation. It verifies the
existing pin and materialized evidence without constructing Raft or claiming
replay readiness. Ordinary reads/effects remain fenced; protocol ownership keeps
retained proof and administrative work available. A same-member join response
remains pending. Non-joining restarts still fail closed. Fresh-member grant,
retirement receipts and the exact durable rebind precede replacement participation.

Removal hooks capture the exact metadata owner and recheck it after quorum
reads and callback waits. Readmission reservation and accepting a stop decision
share Node's admission mutex. An old-member decision cannot stop a pending or
installed replacement, and a stopped Node cannot accept a new reservation.

## Application background work

An optional `load.Application.Run` starts only after the exact handle is ready,
authorized and published. Its reservation is part of publication so a concurrent
stop cannot miss the callback. The callback inherits engine lifetime cancellation,
including failure, and must join its own children. Leadership loss does not stop
Run; `Group.RunWhileLeader` supplies a separately canceled leadership scope.

A callback returning nil is not restarted. A cancellation error matching its
canceled context is ordinary teardown. Any other error fails the exact engine,
emits `application_run_failed`, and installs a local hold. Status retains the
failure after resources release; an explicit StartLoadGroup clears the matching
hold and may create a fresh run only after the old owner has released resources.
Other replicas continue normally.

Engine and Run cancellation precede their joins. A stop timeout or ignored
callback cancellation retains ownership and prevents Close or a replacement.
Close follows both engine and callback joins. Local Node/Group watches coalesce
owned state snapshots and close with a terminal snapshot after shutdown; watch
cancellation never stops the owner itself. A Group observation carries the run's
local `AppliedIndex`, republished after each applied batch or snapshot restore;
it is a wake-up hint for reading committed state, not a quorum barrier.
`Leadership.Replicas` reads the leader's cached Raft progress under the engine
lock and resolves peer names through the managed catalog outside that lock.
