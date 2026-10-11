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
- [ ] Managed host: exact boot evidence, factory/Close ownership, start/stop
  reservations and holds, recovery supervision, action verification, delivery
  reports, retirement/retention, readmission and removal fencing.
- [ ] Audit retained cleanup/readmission proof across the 128-document history
  horizon and interrupted founding before Active. Current catalog extraction
  uses retained Operators/history; pruning must not silently omit an outstanding
  prepared identity or cause false completion of whole-group deletion.
- [ ] Node/Cluster facade and settings: startup/readiness/shutdown, policy wiring,
  complete document/mutation/session/lock/status surface, observer ownership.
- [ ] Migrate the small example and full cluster fixture to Node/Cluster; align
  HTTP approval/operator routes, requests, responses, clients and README.
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
