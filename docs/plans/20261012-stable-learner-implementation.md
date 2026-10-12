# Stable learner implementation

Authoritative scope: [target architecture](../brainstorms/20261012-stable-learner-replicas-target.md).
Requested acceptance also includes a passing five-node process regression with
three voters, two stable learners, and automatic promotion after one voter stops.
Status: implementation and validation complete. Baseline: `84ffa43`.
Requirement mapping and final reports: [validation record](../testing/stable-learner-validation.md).

## Implementation and acceptance checklist

- [x] Replicated voter policy, pins, hosting, cloning, limits and legacy decoding.
- [x] Public policy/hosting/pin mutations; all-host placement and atomic overflow
  rejection; small founding, explicit hosting and target shortfall.
- [x] Stable meta learner founding/admission/readiness/session/restart behavior.
- [x] Deterministic surge/no-surge planning and exact guarded leadership transfer.
- [x] Atomic scoped voter-change command, immutable recovery, durable role records,
  codec/restore/pruning and staged-intent concurrency safeguards.
- [x] Public metrics DTOs, constructor validation, local bounded history, source
  instrumentation, internal collector, transport extensions and owned shutdown.
- [x] Temporal evidence with generation/identity/freshness fencing, failure and
  stability windows, apply readiness, cooldown and new-failure exception.
- [x] Built-in and custom scheduling integration; mandatory driver revalidation.
- [x] Meridian-style native proposal forwarding, authentication, bounded timeout,
  owned retry, uncertain snapshot outcome and application RequestID preservation.
- [x] Unit/functional regressions from target verification matrix.
- [x] Five-node automatic promotion process test, role retention after restart,
  plus focused admission/forwarding/surge/recovery scenarios.
- [x] Current architecture, README, public comments and test guide aligned.
- [x] Passing `make test`, `make lint`, `make test-race`, `make test-function`,
  `make test-function-race`, `make test-example`, `make test-cluster`, protocol
  regeneration verification and physical fixture compatibility.
- [x] Completion audit against every target section and user acceptance criterion.

## Progress and validation

- 2026-10-12: inspected baseline, target, ownership/architecture/testing contracts.
  Docker server 29.6.1 and Go 1.26.5 are available. No prior implementation exists.
  Implementing the replicated model first; default behavior will be connected
  with role-aware admission and scheduling in subsequent iterations.
- 2026-10-12: implemented topology policy/hosting/pins, learner-aware founding
  and admission, typed mutations, guarded replacement plans, scoped atomic voter
  commands and replicated completed-role records. At that stage `make test`
  passed 471 checks and `make test-function` passed 600 checks. Process-level
  role transitions remain unverified until collector/scheduler integration.
- 2026-10-12: added the public metrics DTOs, owned clones, local bounded history,
  validated constructor settings, Meridian-derived EWMA/rate primitives, engine
  run identity, owner-specific Raft/writer generations and peer acknowledgement
  tracking. Node wires settings to both kinds of engine. Added Group metrics
  reads, optional load status samples and authenticated MetaReplicaMetrics RPC;
  regenerated protobuf code with `make generate`.
- 2026-10-12: added continuous multi-source windows and an owned bounded read
  pool. They are primitives for the next collector integration, not yet a running
  cluster-wide evidence pipeline. Fixed retained singleton startup so a nil
  constructor policy cannot impose the fresh default over durable membership.
- 2026-10-12: implemented ordinary native load proposal forwarding, strict
  inbound normal-entry validation and native-member checks, local result waits,
  a 30s configurable timeout, one owner-loop retry for ErrProposalDropped and
  explicit uncertainty on snapshot coverage. Incoming forwarded proposals cannot
  use an unpublished managed owner or relay through a stale leader. Functional
  protocol doubles exercise one real local replica; Docker forwarding and the
  requested five-node automatic promotion case still remain.
- Latest broad functional validation: `make test-function-race` passed 613/613
  checks in 26.255s, report
  `test-reports/function-race/20261011T235549.147277Z-yffsku8v/report.md`.
  `make lint` passed all six module/build-tag commands. A parallel unit table
  initially reused a mutated options value; its fixture is corrected.
  `make test` then passed 491/491 in 3.723s
  (`test-reports/unit/20261011T235713.502031Z-uslxa0ri/report.md`), and
  `make test-race` passed 491/491 in 10.816s
  (`test-reports/unit-race/20261011T235714.937286Z-7r7ri6ho/report.md`).

- 2026-10-12: integrated independent collection, authenticated exact meta/load
  reads, bounded fair read dispatch and retained callback ownership, bounded
  history, source/observer scope fencing, temporal windows and durable cooldown
  interpretation. Cluster snapshot/history facades and built-in/custom scheduler
  inputs/actions are wired. Mandatory driver revalidation performs fresh reads
  after callbacks and planning before atomic voter-change approval.
- The requested five-node regression passes with default 30s/60s windows. It
  verifies both meta and load role swaps, one remaining standby, native membership,
  learner forwarding, cooldown observations and retained former-voter identity/data.
  It passed in focused report `cluster/20261012T002947.541993Z-8vfwe9dy`, then
  in both full runs `cluster/20261012T003312.139889Z-k4a1t6o4` and
  `cluster/20261012T004409.568072Z-qdmihnvl` (under `test-reports/`).
- Broad integration checks: unit 518/518
  (`unit/20261012T004317.638173Z-906mpr4s`), functional 629/629
  (`function/20261012T004338.073840Z-o9wyrlvs`), earlier functional race 629/629
  (`function-race/20261012T003138.436566Z-cyqx7h46`) and all lint commands passed.
  These precede later restore-validation and fixture edits; final checks remain.
- Full Docker run improved from 50/64 to 59/64. Updated fixtures distinguish
  hosting from voter counts, explicitly configure small voter targets, pin
  intentional learners, serve approved learners and preserve explicit interrupted
  promotion tests. Remaining failures in that second run were three founding
  role-count assertions, a fixture bootstrap/CAS race and a pressure measurement
  crossing the new proposal timeout. Corrections are written and awaiting rerun.
- Strengthened switch-record restoration against allocation floors, exact known
  identities and protected historical references. The pruning regression now
  advances Active/Target beyond the history horizon and proves the original
  operator was removed before restoring records and rejecting corrupt records.
- Added a four-node process regression for surge zero/one: pause after a native
  role effect commits, crash the meta leader before acknowledgement, and verify
  the same approved attempt resumes with exact retained identities and data.
- The five corrected cases passed in focused run
  `cluster/20261012T005139.073933Z-5dd2_2ep`. The new recovery test initially
  assumed the first effect's reply barrier also prevented verification; the
  controller correctly verified the already committed native role. A separate
  barrier before the second effect now proves exact resumed authorization and
  preserves the intermediate membership for inspection. Both surge variants
  passed (`cluster/20261012T005338.933555Z-gp4t34iz`, 3/3 including parent).
- `make test` passed again (518/518,
  `unit/20261012T005019.507289Z-vo48_t7p`). Focused meta-engine, metaservice,
  collector and scheduler functional race checks passed after record validation.
  `make test-example` passed (`example/20261012T005130.689513Z-e63ocyao`).
  `make proto-check` passed using an isolated Git index containing the proposed
  protocol files, preserving the workspace index while proving regeneration is
  byte-for-byte reproducible. All seven physical baseline SHA-256 hashes match.
- Moved enablement evidence reset inside the scheduler gate. Concurrent enable/
  disable and Tick calls can no longer reuse a pre-disable stability window
  between setting the switch and resetting the collector. A functional race
  regression holds the reset open, proves Tick cannot enter, then checks that
  enabling does not approve from old evidence. Focused scheduling/metaservice
  race checks pass. Unit race also passed 518/518 before this final reset change
  (`unit-race/20261012T005610.700534Z-ns5p0g1h`).
- After the enablement fix, `make test` passed 518/518
  (`unit/20261012T005843.835953Z-lntx6iev`), `make test-function-race` passed
  630/630 (`function-race/20261012T005843.836008Z-s55hqh6k`), and all lint
  commands passed. The updated full Docker run passed **67/67** in 151.749s:
  `cluster/20261012T005934.307837Z-otex50ld`. This includes the required five-node
  automatic-promotion/retained-return case (139.930s) and both four-node
  interrupted-swap orderings. Image manifest list:
  `sha256:1a0cf7aad6939c582298c5d1a4aed1996da819a91c1674f5323abc775f7afe4c`.

## Final audit and validation

- Fixed native membership observation after reproducing both timeout and ignored-
  cancellation failures. Healthy partial replies now survive the bounded read
  budget. One Host-owned pool caps work across groups, prevents overlapping reads
  of a stuck source, and retains callback ownership through shutdown. Caller and
  owner cancellation invalidate the observation. Six additional functional cases
  cover these boundaries, including late replies and serialized shutdown retry.
- Extended the default-window five-node acceptance to a second failure during
  group cooldown, while the recently demoted retained replica stays ineligible.
  It passed focused (256.190s) and in the final full suite (291.680s).
- Added four-node guarded-demotion recovery: after the victim regains leadership,
  an unavailable exact transfer target leaves the same approved demotion waiting;
  restarting that target resumes transfer and demotion without another attempt.
- Added seven-host capacity tests for atomic rejection at the default five load
  replicas and full placement after explicitly configuring seven. Updated the
  shared document-view helper to compare exact approved meta roles instead of
  assuming every node is a voter.
- Added an alive-node replication-lag process test. An exact load WAL stall leaves
  the node's meta session live; custom per-group thresholds/windows and disabled
  no-progress detection prove the replication-lag trigger. The qualified standby
  replaces that voter, and the original replica catches up as a retained learner.
- Checks at that checkpoint passed: unit 518/518, unit race 518/518, functional 636/636,
  functional race 636/636, all six vet commands, Compose 1/1, full Docker 71/71
  plus the separately added replication-lag case 1/1. The lag case compiled after
  the full run began; both used the same immutable application image. No library
  or fixture code changed between those runs. Protocol regeneration was verified
  earlier and the protocol files remained unchanged afterward. All seven physical
  fixture lengths and SHA-256 hashes were verified again without regeneration.
- The requirement audit maps the target to implementation and tests in the linked
  validation record. README, current architecture, target integration seams,
  MetricsOptions comments and testing documentation are aligned. Documentation
  fences and whitespace checks pass. Forced-majority recovery, automatic data
  partitioning and other explicitly deferred target scope remain deferred.
- No commits or workspace-index changes were made.
- Final arithmetic audit: source-meter counters already saturated safely, but
  collector probe/epoch/series/history counters could wrap. They now saturate and
  invalidate actionable evidence without destroying retained diagnostic history.
  One unit regression and four functional cases (including the parent) pass,
  including the functional race detector. The complete suites are rerunning
  against the final image; the last Docker run will include the lag case too.
- The following complete 72-case run passed every new feature case, including
  default-window five-node recovery, but exposed an intermittent drain failure
  after a retained voter returned. Native status rounds could overlap and omit
  the same healthy peer through exact-source exclusion; the fixture then erased
  SafetyRefusal details and made a transient quorum refusal terminal. Native
  rounds now serialize per group independently of the membership effect gate,
  and completed pool callbacks release their key before publishing completion.
  The fixture preserves structured safety details in its existing error envelope.
  Focused functional race and codec regressions pass; repeated drain and final
  complete process coverage are running on the rebuilt recovery image.
- Repeated drain recovery passed five consecutive runs. The rebuilt source also
  passed 524 unit checks, 642 functional checks, both race variants and all six
  vet commands. The next complete process run exposed an independent retained-
  restart test race: HTTP readiness preceded publication of the recovered meta
  identity. The test now waits for the observable parked lifecycle before
  checking the exact retained pin and fresh process identity. Library behavior
  and the container image are unchanged; repeated process validation is running.
- Final validation passed: 524 unit checks, 642 functional checks, both race
  variants, all six vet commands and the complete **72/72** Docker suite in
  265.274 seconds (`cluster/20261012T015122.723116Z-yayvjjtm`). The required
  five-node automatic-promotion, retained-return and second-failure/cooldown case
  passed in 246.650 seconds. Both recovery regressions also passed five repeated
  process runs. The latest unit rerun is
  `unit/20261012T015039.213151Z-f9fc0tzz`. Compose, protocol reproducibility and
  immutable physical fixtures passed as recorded in the validation document;
  their checked scope remains unchanged. Final documentation links, fenced
  snippets and whitespace checks pass. No required implementation work remains.
