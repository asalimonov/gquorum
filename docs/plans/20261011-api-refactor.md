# Public API refactor implementation

Target: [accepted architecture](../brainstorms/20261011-public-api-simplification-target.md).
Baseline: `75ba513`; implementation branch: `feat/api-reafctor`.

The target document is authoritative. This checklist records implementation and
verification progress; checking a stage does not waive any target requirement.

- [x] Policy and retirement foundations: public Scheduler/LifecyclePolicy,
  RestoreDegraded/DefaultLifecycle, external contract helpers, explicit WAL
  retirement enumeration/collection, existing scheduling algorithm migration.
- [ ] Public values and persistence boundaries: Identity/document/session views,
  Operator/Scheduling names, private legacy disk/wire/event DTOs and baseline
  compatibility fixtures; internal metaservice/operator/scheduling packages.
- [ ] Deliberate load.Group and Standalone owners, explicit application values,
  state-machine adapter and typed managed transport contracts; no engine aliases.
- [ ] Managed host: exact boot evidence, factory/Close ownership, start/stop
  reservations and holds, recovery supervision, action verification, delivery
  reports, retirement/retention, readmission and removal fencing.
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
