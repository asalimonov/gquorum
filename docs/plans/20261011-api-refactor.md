# Public API refactor implementation

Target: [accepted architecture](../brainstorms/20261011-public-api-simplification-target.md).
Baseline: `75ba513`; implementation branch: `feat/api-reafctor`.

The target document is authoritative. This checklist records implementation and
verification progress; checking a stage does not waive any target requirement.

- [x] Policy and retirement foundations: public Scheduler/LifecyclePolicy,
  RestoreDegraded/DefaultLifecycle, external contract helpers, explicit WAL
  retirement enumeration/collection, existing scheduling algorithm migration.
- [x] Public values and persistence boundaries: Identity/document/session views,
  Operator/Scheduling names, private legacy disk/wire/event DTOs and baseline
  compatibility fixtures; internal metaservice/operator/scheduling packages.
- [x] Deliberate load.Group and Standalone owners, explicit application values,
  state-machine adapter and typed managed transport contracts; no engine aliases.
- [x] Managed host: exact boot evidence, factory/Close ownership, start/stop
  reservations and holds, recovery supervision, action verification, delivery
  reports, retirement/retention, readmission and removal fencing.
- [x] Preserve topology cleanup proof across the 128-document history horizon
  and interrupted founding before Active, including exact approval retries.
- [x] Feed all retained approved replica evidence into readmission; exclude
  staged-only identities from its cleanup catalog and verify actual retained
  identities against the exact admission grant.
- [x] Node/Cluster facade and settings: startup/readiness/shutdown, policy wiring,
  complete document/mutation/session/lock/status surface, observer ownership.
- [x] Migrate the small example and full cluster fixture to Node/Cluster; align
  HTTP approval/operator routes, requests, responses, clients and README.
  - [x] Small meta-only example and its process test use public Node/Cluster.
  - [x] Full load application, typed protocol adapter and fault fixtures.
- [ ] Preserve/migrate all unit, functional and process-cluster assertions;
  add meaningful coverage for new ownership/policy/transport boundaries.
- [ ] Update library guide and current architecture/testing docs; remove all old
  public compatibility surfaces; inspect complete public API and target inventory.
- [ ] Complete validation: make test, test-unit (<30s), test-unit-race, lint,
  test-fixture, test-example, test-cluster, test-perf, bench; proto-check if changed.
- [ ] Requirement-by-requirement completion audit against the entire target.

## Progress and evidence

- Initial inspection: branch and both worktrees clean; existing unit suites pass
  in both modules. Docker daemon available (29.6.1). No implementation-stage
  behavior claims yet. Local stage commits are authorized; no push requested.
- Policy/storage stage: added public default and replaceable decision contracts,
  external policy contract helpers and topology.Identity. Moved the existing
  repair driver into internal/scheduling, ported its decision tests and added
  custom-callback timeout/input-ownership/hold checks. Replaced SweepQuarantine
  with exact Retired/Collect, including restart reconstruction and identity,
  timestamp, confinement, symlink and successor protection tests.
- Stage validation: `make test` passed (root meta functional tests 105.526s;
  cluster testapp 40.426s); `make lint` passed; targeted policy/scheduling/WAL
  short race tests passed. Uncached `make test-unit` passed in **9.40 seconds**.
  This is foundation validation only: Node integration, public-name migration,
  the example refactor and process-cluster acceptance are still outstanding.
- Boundary stage (in progress): moved meta and all its tests into
  internal/metaservice; moved identity/document/session/parked-join values to
  topology. Existing Runtime and applications temporarily call the private
  service while their Node/Cluster replacement is implemented; this is not the
  final application-facing API. Storage paths and protocol literals are preserved.
- Renamed public event values to OperatorChanged/SchedulingChanged/OperatorID
  with a separate unchanged disk record. Fixed pre-rename fixtures verify exact
  bytes, filtering, append sequence and reopen without rewriting rows. Captured
  original meta proposal, reply and snapshot fixtures before the Operator rename;
  their exact round trips and restored documents/admission/session/lock pass.
- Boundary validation: `make test` passed (metaservice 101.827s; cluster testapp
  36.665s); `make lint` passed; event/metaservice short race tests passed. Both
  `cluster` and `example` build-tag suites compile; process execution is still
  required after caller migration. Operator/Scheduling public types, private meta
  codec adapters, and internal/operator remain the next boundary work.
- Operator boundary complete: renamed public approval/progress and scheduling
  values, removed public GroupController, moved execution into the concrete
  internal/operator.Controller, and renamed fixed membership SafetyRules.
  Controller construction now returns classified invalid-setting errors. Private
  snapshot/proposal/result/MetaControl records preserve legacy bytes and all
  authorization fields; JoinReady retains its original capitalized reply key.
  Golden fixtures were not rewritten. Application responses now use operator_id;
  full HTTP route/approval and Node caller migration remains a later stage.
- Operator validation: `make test` passed (metaservice 102.205s, testapp 39.677s),
  `make test-unit`, `make lint`, targeted short race checks and both tagged test
  compiles passed. Final acceptance must additionally exercise physical retained
  meta/WAL stores, Docker cluster behavior and every target ownership contract.
- Load ownership: replaced public engine aliases with the deliberate Group
  handle and separate Standalone owner. External-package tests verify the public
  method boundary, zero-handle refusals, fixed membership safety, borrowed store
  ownership, snapshot plus committed-tail recovery and retained request IDs.
  Existing Runtime and fixture code still use the private engine until their
  Node migration; those transitional surfaces must be removed before completion.
- Managed host (in progress): implemented per-group startup reservations,
  independent owned contexts, duplicate waiters, exact stop holds, local replay
  readiness, serial Close/retry ownership, startup cleanup, parked evidence,
  assignment recovery and joined host shutdown. Ported the existing boot evidence
  matrix. Engine construction now accepts the owner's cancellation context.
  Scope includes the approved version for factory specifications; factory inputs
  cannot mutate founding peers. Explicit stop can retry cleanup of an owned
  identity after it leaves the current topology.
- Managed transport foundation: typed sender, Raft receiver, replica observations
  and delivery reports validate complete identities; snapshot acknowledgements
  require exact issued tuples. Per-engine pending reports are bounded and expire;
  old reports cannot affect replacement handles. A three-host test exercises
  founding, replication, local stop/restart and snapshot delivery through these
  methods. This host is not yet connected to Node or the cluster fixture:
  approved-action dispatch, retirement/readmission, policy integration and the
  public facade remain required work.
- Load/host validation: `make test` and `make lint` passed; the uncached unit
  suite completed in **10.34 seconds**. Full race tests passed for managed/load/
  raftengine, including the three-host snapshot scenario. Both `cluster` and
  `example` build-tag suites compile. Docker acceptance and the final architecture
  audit remain outstanding until the Node and caller migrations are complete.
- Approved effects (in progress): added exact action endpoint/approval checks,
  per-group operation reservations, founding fan-out, typed verified replies,
  leader-directed membership/transfer with the fixed safety coordinator, and the
  private Operator adapter. Startup revalidates its originating approval after
  factory/replay waits. Unknown future approvals are refused; the authorized
  dispatcher may retry a remote refusal without treating it as completed work.
- Topology retirement now joins the exact replica, checks catalog/store/ledger
  identities, verifies the complete application proof, quarantines exact Raft
  state and forgets only the matching ledger. Retries verify the application even
  after a previous quarantine move; approved successors remain untouched.
  Retention callbacks are bounded/serialized, and collection refuses live or
  still-recorded identities. Host shutdown cancels and joins in-flight effects,
  including callbacks. Readmission retirement intentionally still requires its
  separate grant path and is not wired through ordinary topology authorization.
- Added a private FSM/service catalog for current routing and approved deletion
  targets, including retained interrupted preparation and excluding staged intent.
  Durably completed cleanup/deletion avoids unnecessary repeat contacts. No
  durable fields, proposal encodings or protobuf definitions changed. This
  foundation still needs the history-horizon audit listed above, readmission,
  removal policies and Node/Cluster composition before application migration.
- Checkpoint validation: `make test` and `make lint` pass, including the exact
  replica-query regression for reseating. Docker acceptance remains outstanding.
- Cleanup evidence: terminal preparation records survive while their identities
  are outside Active and lack verified cleanup. Founding documents needed to
  reconstruct exact founders remain pinned in the existing bounded history;
  cleanup proof dependencies are retained deterministically and released once
  ordinary history and live views no longer need them. No disk record, bucket,
  proposal encoding or schema version changed. At 127 unresolved founding
  documents, a further founding approval is refused until prior founding or
  cleanup completes, preserving a history slot for cleanup edits. This is a
  bound on unresolved evidence, not the number of nodes or groups.
- Deletion planning and the private Operator store include retained approved
  identities for removed groups, including groups that never reached Active.
  Public Active stays the fully converged document. The FSM rejects plans that
  omit required whole-group cleanup and deduplicates an already accepted exact
  approval before consulting cleanup observations changed by its effects.
- Added snapshot/restart and history-horizon regressions for interrupted
  preparation/founding, bounded-history cleanup capacity, proof release after
  cleanup, immutable approval retries, and a running meta-service deletion of
  interrupted founding. Updated the mutation test fixture to record actual
  stop/delete steps; its existing rollback/retired-ID assertions are preserved.
- Cleanup stage validation: `make test` and `make lint` pass. Uncached
  `make test-unit` completes in **10.95 seconds**. Race checks pass for the full
  metaengine suite and the running-service interrupted-founding/retired-group
  regressions. Readmission catalog composition, the public Node/Cluster facade,
  application migration and Docker acceptance remain outstanding.
- Readmission foundation: the private FSM recovery catalog includes Active,
  Target, retained approved documents and exact older Operator identities;
  staged-only Latest never authorizes retirement. The meta service captures
  both materialized and replayed evidence. An optional `previous_groups` field
  in the existing JSON Join reply carries the leader's exact former identities
  when the joining process has lost its meta database. Existing fields and
  protobuf definitions are unchanged; no durable schema or bucket changed.
- The service retains an owned authenticated grant and verifies the entire
  grant at the private Node/host boundary. Managed Readmit validates node,
  process, member pair, path and catalogs; fences/joins its old lifetime; checks
  closed-meta ledger and actual load identities; verifies application retirement
  with a readmission proof; quarantines only exact Raft identities; then backs up
  the old meta file under the existing deterministic admission-derived name.
  The backup retains its ledger, parent sync completes the move, and the live
  path must be fresh. Exact backup plus absent original is a verified retry;
  foreign pins, symlinks and overlapping replacement identities are refused.
- Readmission retries re-run application verification even after a WAL move.
  Host Stop cancels and joins in-flight readmission callbacks; failed cleanup
  preserves ownership and evidence. Former owned identities remain recorded
  across retries after their handles close, so an incomplete catalog cannot
  become accepted on its second attempt. A metadata-only recovery helper shares
  the pin/backup checks and refuses former load resources without a load host.
- Node composition must still wire these private paths and replace the stopped
  host only after the granted meta pin is installed. The public API and fixture
  migration have not yet occurred. Stage validation: `make test`, `make lint`,
  full managed/metaengine/metastore race suites and focused meta admission race
  tests pass. The uncached unit suite takes **10.53 seconds**. Tests cover
  history/snapshot catalogs, malformed grants, real machine Close ordering,
  interrupted quarantine, backup retry, foreign state and joined cancellation.
- Node settings foundation: added the explicit public NodeOptions/Bootstrap/
  Meta/Load/Policy settings and factory/retirement callback types. Validation
  resolves built-ins without invoking caller code or opening stores; ambiguous
  policy settings and load storage selections are refused. Shared meta and
  engine validation preserves timing constraints. Limits remains the only
  cluster-size setting. Nested topology, credentials, TLS configuration slices
  and WAL prewrite values are owned copies; injected dependencies are borrowed.
- The private meta service now installs the selected Scheduler, callback bound
  and held-replica observation source. A running-service test proves a custom
  scheduler receives owned inputs and its configured deadline. This fixes a
  missing connection between the existing scheduling driver and Node settings.
- Added the private lifecycle runner shared by readiness, retention and removal.
  It serializes all three callback kinds, owns queued and active calls through
  actual return, propagates lifetime/deadline cancellation, rejects late/invalid
  decisions and refuses removal decisions without exact mandatory fencing.
  Deterministic concurrency tests prove timeout ownership, joined retry and
  absence of callback overlap. Node construction and worker composition still
  need to connect this runner to the managed host and public facade.
- Settings/policy stage validation: `make test`, `make lint` and focused root/
  metaservice race tests pass. Uncached `make test-unit` completes in **13.39
  seconds**. No generated protocols or durable schemas changed. The complete
  Node/Cluster facade, mandatory removal flow, caller migrations and Docker
  acceptance remain outstanding.
- Node/Cluster composition: added the complete deliberate public facades,
  identity helpers, local status/session/load methods and document/mutation/
  approval/lock forwarding. NewNode is pure; Start owns one context and launches
  admission without waiting for quorum. WaitReady combines mandatory local
  activation with the selected gate and does not own replica lifetimes. Applied
  topology wakes recovery. One lifecycle runner serves all decision kinds and
  exposes callbacks still held after their cancellation deadline.
- Node owns teardown ordering: private meta failures request owner shutdown,
  Quiesce retains its database, policies and managed replicas join before owned
  stores close, and a failed application Close remains owned until a serial Stop
  retry. Removal immediately fences managed engines independently of a stalled
  readiness callback; the response policy runs only after meta/load participation
  joins. Successful KeepDiagnostics retains local reads and cached cluster status
  without new load probes. Normal joined engine Stop is not a startup failure.
- Load routing binds process/member identities to the local applied admission
  or node session. Current Raft/query authorization is separate from retained
  cleanup routing; unknown locally unapplied replica declarations are refused as
  retryable observations. Readmission replaces the permanently stopped host only
  after the granted meta pin is installed, using dynamic service/host adapters.
  A failed old-meta close keeps its store reference for final owned teardown.
- Public integration tests live in `tests/cluster/testapp/node*_api_test.go`
  with only public library imports. They cover sequential three-node startup,
  canceled readiness waits, managed counter replication, exact local holds,
  stable old handles, snapshots/retained restart, delayed-founder gates, first-
  bootstrap timeout versus retained recovery, noncooperative callbacks, Close
  failure/retry with an actual bbolt ownership check, removal policy retry after
  mandatory fencing, and fresh-ID readmission with both lost and retained meta
  databases while the former load WAL remains. Pure constructor/routing tests
  stay next to their packages; functional tests skip short mode.
- Facade stage validation: final `make test` and `make lint` pass, including the
  full existing fixture tests (78.028s). All new public scenarios pass under the
  race detector; retained-readmission and Close-error additions also pass under
  race. Short race suites pass for root/managed/metaengine/metaservice. Uncached
  `make test-unit` completes in **12.46 seconds**. Runtime/OpenMeta/OpenLoad are
  still transitional surfaces used by the existing example and fixture; migrate
  those callers and remove the old API before final completion. Full Docker,
  physical baseline compatibility, documentation and target audit gates remain.
- Small example migrated to public Node/Cluster with caller-owned startup settings
  and joined Stop. Its HTTP adapter uses explicit approve/operators routes,
  public mutation values, strict empty-object approval input and exact If-Match
  parsing before cluster calls. Readiness combines Node's gate/session with the
  Active node state. Legacy mutation fields/routes are refused; staging errors
  preserve X-Staged-Version. Compose paths, volumes, ports and commands stay intact.
- Example verification: `make test`, `make lint` and example race tests pass;
  uncached `make test-unit` takes **11.03 seconds**. `make test-example` passes
  against the actual Compose application, covering previews, both approval flows,
  node types, drain/activate readiness, CAS conflicts, unchanged meta membership
  and exact retained topology after restart. The harness rediscovers ephemeral
  ports after restart. Full load-fixture migration and process/performance suites
  remain required; the demo smoke test does not substitute for those suites.

- Full application migration: the cluster fixture now owns only its application
  FSM/projection, typed load protocol, borrowed store fault wrappers and HTTP
  adapter. Node owns founding gates, managed replicas, membership effects,
  readmission and mandatory removal. The fixture has no private library imports.
  Root public mutation values and approve/operator HTTP names are used by every
  process client. Retired-meta assertions inspect actual backup files through an
  authenticated test-only discovery endpoint, independent of log wording.
- Docker recovery exposed transient stale process pins after restart. Remote
  observation refusals provide no live attestation; successful replies must still
  match exact peer/group/found state. Remote approved-action refusals retry only
  while the dispatcher's complete approval remains valid. Unit/race regressions
  cover refusal classification and malformed affirmative replies.
- Fixture stage checks passed: make test (71.750s for the full test application),
  make lint and tagged process vet. Selected Docker cases cover founders/delay/
  timeout, learner promotion/restart, snapshot catch-up, refused deletion,
  projection stalls, parked reseating, lost-meta recovery, same-disk exclusion
  and rotated identity readmission. These selections do not close the full
  process/performance gates.
- Removed Runtime/New(Options)/OpenMeta/OpenLoad and the duplicate root boot
  implementation after recording baseline signatures/test names. Public parked
  values remain in node_status.go; boot decisions belong to internal/managed.
  Root integrated tests moved to public Node tests in the cluster module; fast
  ownership/boot/ledger assertions live with the managed implementation. The
  updated runtime-ports coverage map records each transferred obligation and
  the intentional idempotent-start/owned-lifetime contract changes.
- API-removal stage validation: make test, make lint, tagged cluster vet and
  proto-check pass. The managed suite and new public recovery/effect-join tests
  pass under race. Uncached make test-unit completes in **10.81 seconds**.
  Public go doc lists Node/Cluster and explicit load types without Runtime or
  OpenMeta/OpenLoad. The current library/ownership guides describe the new API.
  Docker selections also pass at five/seven nodes for same-disk readmission and
  multi-group shutdown, and for fifteen independent load groups with deletion
  of every local store (76.738s total). The complete process suite is now running;
  physical baseline fixtures, remaining acceptance gates and final audit remain.
- Physical compatibility: generated closed meta bbolt, load etcd WAL/snapshot/
  applied-state files and event bbolt from an isolated export of baseline75ba513
  (Go source identical to target baseline4b7721a). The captured generator,
  provenance and seven file hashes are documented under docs/testing. Current
  tests restore the actual meta engine and compare every retained bucket row;
  recover full load snapshot plus committed tail and retained request IDs;
  and filter/append/reopen legacy event rows without rewriting them. Existing
  JSON golden files remain unchanged.
- Physical stage checks pass: make test, make lint, complete make test-unit-race
  and focused physical recovery races. Uncached make test-unit takes **11.06
  seconds**. All physical hashes match the captured baseline manifest. The full
  Docker suite remains in progress; no broad process/performance claim yet.
