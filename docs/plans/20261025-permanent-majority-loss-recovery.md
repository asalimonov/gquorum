# Implementation Plan: GQ-01 permanent-majority-loss recovery

**Plan file:** `docs/plans/20261025-permanent-majority-loss-recovery.md`

**Project source:** `.` — module `github.com/asalimonov/gquorum`

**Date:** 2026-10-25

**Branch:** `feat/permanent-majority-loss-recovery`

**Code baseline:** `26842024d2fb4fe6fd74b15efa5106749a8b0674`

**Target architecture:** [docs/brainstorms/20261024-permanent-majority-loss-recovery-target.md](../brainstorms/20261024-permanent-majority-loss-recovery-target.md)

**Full brainstorm:** [docs/brainstorms/20261024-permanent-majority-loss-recovery.md](../brainstorms/20261024-permanent-majority-loss-recovery.md)

**Review/clarifications:** [docs/review/feat_permanent-majority-loss-recovery.md](../review/feat_permanent-majority-loss-recovery.md)

**Status:** implementation plan; no implementation task has been executed.

## Scope and precedence

Implement the accepted target, including the final D19–D23 refinements. The user
approved this order: API/guards → inspection/journals → archive/cancel → metadata
import/repair → Node replay/publication → test fixture/end-to-end verification.
The user then required **at most 16 substantial tasks**. This plan has exactly 16;
subtasks are execution checkpoints inside those tasks, not additional tasks.
Some cohesive tasks, especially the existing WAL implementation move and process
fixture work, may exceed the skill's suggested 1,500 changed-line size. Keep
internal changes focused and buildable instead of splitting into more tasks.
Peer-review corrections move ordinary Node-owned fixture groundwork to Task 3 so
startup/lease changes receive an early process smoke test. The recovery job stays
in Task 13; the failure-boundary audit is consolidated into Task 16. The approved
library workflow order and 16-task limit remain intact.

Latest scope clarification, verbatim:

> Don't check wiring against Meridian, just write tests for tests cases which wants Meridian and tests all test scopes (regular and cluster tests). it is independent library. We will do check wiring in future.

Meridian contributes acceptance scenarios only. Do not modify, compile, run,
deploy or verify integration with Meridian. Target §4's downstream HTTP/job
contract remains deferred. Equivalent test-only management behavior exercises the
library through public APIs in `tests/cluster/testapp`. The final task verifies
**gquorum's own** composition and public reachability, not Meridian wiring.

The target is the design source. Earlier backup documents and superseded Session,
Execute/ReadStatus, public Paths, recovery-specific factory and synthetic-genesis
proposals are historical. Do not implement them. Appendices below copy the target's
important declarations to make task execution concrete; if an approved target
change occurs, reconcile the plan and appendices together before continuing.

## Non-negotiable outcomes

- One complete survivor, approved Target inventory, validated snapshot/commit
  prefix, exact acceptance digest and explicit potential-loss acknowledgement.
  Never infer commit from last log/applied hint or silently create an empty group.
- Node-owned WAL selected through `LoadStorageOptions.WAL`. Recovery rejects
  injected Store/Retirement, even a standard caller-created walstore.Store.
- Same configured roots and mount bindings; generated timestamped backup names
  inside each root. Preserve original library bytes and unresolved old evidence.
- New cluster lineage and fresh monotonic identities; preserve allocation floors,
  retired group IDs and desired policies. Archive old operations/sessions/leases.
- Resource-free `NewRecovery(NodeOptions, ...recovery.Options)`; six lifecycle
  methods and a pure `RecoveryStartOptions` helper. No public executable manifest,
  plan codec, force flag, Rollback method or storage-recovery extension interface.
- Approve guards without moving data. Run owns Backup/archive. Cancel is explicit,
  allowed only before durable Archiving, with journaled Undo and retained ownership.
  Context cancellation is not operator cancellation. Run(Cancelling) resumes only Undo.
- Ordinary B metadata founding/import and normal Node-owned GroupFactory/replay/
  Close. Library-owned private readiness, consumption and publication/Run barriers.
- Explicit reset-for-join for returning A nodes. No automatic reset on mismatch,
  old-lineage admission or reseeding a consumed/missing B identity from A.

## Current-code findings that shape execution

| Evidence | Consequence for this plan |
| --- | --- |
| `node_lifecycle.go:24` checks paths, then inspects authority, constructs metaservice and opens load resources; `node_authority.go:154` can acquire authority. | Install storage guards before any destructive API becomes reachable. Write SuccessorStartedAt in early Node preflight, after pure validation/lease acquisition and before any B store acquisition. The later coordinator is too late. |
| `tests/cluster/testapp/app.go:314` passes `fixtureStore`/`fixtureRetirement`; `store.go` explicitly describes borrowing. | Add a Node-owned WAL fixture mode. Existing wrapper-dependent fault scenarios keep their current mode; recovery scenarios do not bypass NewRecovery's injected-store refusal. |
| `internal/metaservice/mutations.go:151` checks voter counts through `checkLoadBounds`; placement preparation/reports and FSM approval also validate whole documents. | Recovery allowances must reach all preparation, mutation, planning, validation and repair paths, not just one topology helper. Test adding node two while node three is absent. |
| `internal/metaengine/scoped_approval.go` already prepares an owned approval candidate; `scoped_stage.go` records floors/history. | Reuse these preparations for atomic import. Do not hand-write a synthetic metadata database or expose a partially staged import. |
| `internal/managed/replica.go:187` currently finishes the effect, publishes the handle and starts Application.Run together. | Introduce private readiness so creation receipts can complete while public handles/Run stay gated. Preserve reservations and exact Stop/Close ownership. |
| `internal/raftengine/group.go` applies a five-second restore context; WAL replay currently treats generic UnexpectedEOF as termination. | Add owned recovery restore context and segment-aware bounded source decoding. Do not claim ordinary startup or GQ-02 already solves either. |
| Metadata FSM v4 uses strict decoding; authority v2 and engine v2 have frozen fixtures. | Add only the specified optional recovery subtree/private commands; preserve ordinary encodings and frozen fixture hashes. Older binaries must reject recovered state. |
| `project.txt` is absent; docs are ordinary directories, not a separate symlinked documentation repository. `docs/plans` and `docs/brainstorms` are ignored. | Source is this checkout, established by go.mod/README/AGENTS.md. Keep the existing feature branch, produce only this plan during planning, and validate ignored documents explicitly. Do not invent a nested repository. |

These findings refine implementation locations and test plumbing without changing
accepted product behavior. The peer's metadata-lag creation example was retracted;
earlier-segment CRC handling was narrowed to the actual short-read issue. Do not
reintroduce those superseded bug claims in code comments or tests.

## Execution rules and intermediate safety

1. Read each task's target sections and relevant nearby code/tests before editing.
   Preserve owned copies, exact grants, native quorum checks and joined shutdown.
2. Tasks 1–4 establish contracts and the startup safety boundary. Any not-yet-wired
   mutating method or recovery startup path must return a typed, explicit
   implementation-pending refusal before effects. It must never report success,
   clear a guard, start an empty group or fall through to ordinary fresh founding.
   Remove all temporary implementation-pending paths by Tasks 10–12/16. These are
   implementation scaffolds, not new public APIs or final unsupported behavior.
3. Each task ends with a green build and relevant checks. Run `make test` across
   all five application modules; use focused functional checks for the changed
   owners and race checks for concurrency. Fix every encountered failure,
   including unrelated failures; record unrelated fixes in NOT PLANNED CHANGES.
   Do not waive failures, regenerate frozen fixtures or claim an old report passed.
4. Units remain pure/default-tag tests. Real stores, engines, Node and protocol
   doubles use owning-package functional tests. Multi-node behavior belongs in the
   three-node Docker pool. No new multi-node functional fixture or library HTTP API.
5. Do not run broad Docker suites after intermediate edits. Task 3 has one early
   focused owned-WAL smoke run after its fixture batch settles because Task 2 changes
   every Node startup. Build the later recovery fixture after Task 13 settles, run
   focused process cases, then complete the required qualification suites. Do not replace a shared image mid-run.
6. Use instance-owned barriers/fault seams and deterministic state observations;
   never process-global WAL-size mutations, environment changes or sleep-based
   race assertions in parallel tests. Preserve existing test-suite build tags.
7. Log implementation progress, actual commands/report paths and unplanned changes
   in this plan. Proposed task/test names below are future work, not existing tests.
8. Keep implementation commits reviewable within the 16-task structure. No push,
   merge, release or Meridian work is authorized by this plan.

## Task overview

| Task | Deliverable |
| --- | --- |
| 1 | Define the approved API and shared recovery models |
| 2 | Establish private WAL implementation and exclusive startup ownership |
| 3 | Expose Node-owned WAL and independent Node lifetimes in the process fixture |
| 4 | Implement durable recovery journals, names and discovery |
| 5 | Build bounded read-only readers and the shared replay interpreter |
| 6 | Implement complete-survivor inspection and exact durable approval |
| 7 | Implement backup, cancellation and in-root execution |
| 8 | Implement narrowly scoped recovery placement and repair planning |
| 9 | Add the replicated import, exact consumption and private command gates |
| 10 | Integrate Node bootstrap, reset admission and the recovery coordinator |
| 11 | Implement durable recovered WAL seeds and normalized checkpoints |
| 12 | Complete managed replay, consumption, publication and shutdown |
| 13 | Add durable recovery jobs and independent remote test controls |
| 14 | Validate permanent loss and returning nodes in the three-node Docker pool |
| 15 | Qualify resources, compatibility, all suites and current architecture docs |
| 16 | Verify the architectural delta, close gaps and prove library wiring |

## Task List

### Task 1: Define the approved API and shared recovery models

**References:** Target §§1–3, §§5.2/7.2 (models), §10.1, §15.1/15.10;
brainstorm D19–D23. Appendices A–C contain the target declarations.
**Files:** CREATED `recovery.go`, `recovery_{inspect,approve,run,cancel,status,start_options}.go`,
`recovery/{doc,types,errors}.go`, `internal/recoverymodel/{types,codec,validate}.go`;
UPDATED `load/application.go`; owning unit tests and external caller compile coverage.
**Dependencies:** none. **Outcome:** exact API and pure functionality, with explicit
refusals for unimplemented I/O paths.

- [ ] Before the first Go edit, capture `make bench BENCH_TIME=1s` at the current
  code baseline with machine/toolchain/source information. Preserve its raw output
  for the matching after-change run in Task 15; this is measurement, not a new task.
- [ ] Implement the public value/error types and exact root signatures in Appendix A.
  Keep root → recovery → private model/topology/storage dependency direction;
  neither the value package nor internals import root gquorum. Do not expose Paths.
- [ ] Define shared private local/import/seed/receipt models from Appendices B/C and
  target §10. Use aliases where specified, one canonical encoding, owned-copy
  helpers, and explicit nil omission for the future FSM subtree. No SQL/ORM/cache
  service is introduced. The current metadata FSM is not extended until Task 9.
- [ ] Implement NewRecovery's zero-or-one Options rule and required-field copying.
  Preserve zero-vs-explicit resource settings. Reject Store/Retirement and invalid
  source configuration without I/O; ignore irrelevant old bootstrap, factory,
  transport, listener, policy and lifecycle inputs. Defer physical path/pin checks.
- [ ] Implement Request/ID validation, the finite resource defaults, Issue.Remedy
  and Refusal.Error/Unwrap with existing fault categories and original causes.
  Generated IDs are created during Inspect, not during pure construction.
- [ ] Implement RecoveryStartOptions exactly per target §3.4: phase/mode checks,
  fresh B member or zero-member join, single founder or preserved join seeds,
  caller-supplied fresh InstanceID, unchanged roots/policy/credentials, and copied
  mutable values. Do not call listener/resolver methods or create resources.
- [ ] Add `Recovery *load.RecoveryInfo` to ReplicaSpec with the exact Appendix A
  fields. Keep GroupFactory/Application/StateMachine signatures unchanged and nil
  Recovery on ordinary acquisition. Do not start recovery replay from this field alone.
- [ ] Add explicit temporary typed refusals for Inspect/Approve/Run/Cancel/Status/List
  until their implementing tasks land. Constructor success must not imply recovery
  readiness, and no method may synthesize a successful phase.
- [ ] Add meaningful units for configuration copying/refusal, ignored unrelated
  invalid Node settings, zero/one/multiple options, helper field transformations,
  fresh-ID requirements, status-not-authority and errors.Is/As. Add caller-side
  compilation against all public signatures, including nil optional recovery input.

#### Verification

- [ ] `make test` passes in all five application modules; run focused recovery/model
  units and root/load vet checks. Do not create engines or stores in these units.
- [ ] Verify no public recovery package import cycle, no exported Rollback/Paths/
  executable Plan codec and no changes to existing persisted fixtures or Node API.
- [ ] Record commands/results; fix all failures before moving on.

### Task 2: Establish private WAL implementation and exclusive startup ownership

**References:** Target §§5.1/5.2, §6.1, §7.1, §§10.2/10.3/10.5, §12;
current runtime ownership contract. **Files:** MOVED implementation/private tests
from `storage/walstore` to CREATED `internal/walbackend`; UPDATED public WAL facade,
`node.go`, `node_lifecycle.go`, `node_authority.go`, shutdown/resource ownership;
CREATED `internal/recoverydisk/owner.go` and namespace/path helpers.
**Dependencies:** Task 1. **Outcome:** no live owner can overlap offline mutation,
and pending recovery evidence blocks ordinary startup before B acquisition.

- [ ] Move existing WAL implementation files named in target §10.3 without changing
  their persisted behavior. Preserve public Store/Options/IdentityMismatchError
  aliases, exported constants and Open. Move private-field tests beside their owner;
  keep public conformance coverage at storage/walstore. No private recovery methods
  become exported Store methods. The move may exceed the task line-size guideline.
- [ ] Implement root-relative no-follow validation and stable system/WAL leases:
  `Acquire`, `Owner.CheckRoots/Close`, `TakeWALLease`, `Lease.Close`. Support WAL
  nested under StorageDir, refuse the reverse/aliases/private archive roots, and
  retain lock files in place across archives. Nonblocking contention reports busy.
- [ ] Make ordinary Node and WAL Open/Close honor these leases for their full resource
  lifetimes. Normal injected storage ownership remains borrowed. Preserve explicit
  ownership on timeout/failed Close; never unlink a lock to “release” it early.
- [ ] Implement internal `walbackend.OpenWithLease` with exact root verification and
  adoption only on successful acquisition. Failure leaves ownership with the caller;
  Node shutdown closes the transferred WAL owner only after managed work joins.
- [ ] Add early Node recovery-namespace inspection before inspectAuthority/metaservice
  construction/load-store acquisition. Recognize only validated control records;
  malformed, conflicting or pending evidence refuses. At this stage **all** pending
  recovery starts refuse until Task 10 installs the accepted restart path.
- [ ] Make HasState/fresh-start detection distinguish the stable lock/validated
  archive namespace from genuine live data without ignoring unknown/partial entries.
  Metadata-only nodes and independent live WAL owners receive the same protection.
- [ ] Audit every system/WAL-root enumeration, not only Store.HasState: root
  inspectFreshLoadStorage currently rejects any entry, and retirement/collection,
  snapshot/segment scans and tests may assume old directory contents. Recognize
  only the stable lock and validated control namespace in the appropriate scan;
  unknown/partial data remains evidence. Add cases for lock-only fresh roots,
  valid history and forged/unknown control names. Audit resource accounting too:
  `/internal/test/resources` counts open FDs, not disk bytes, so account for actual
  lock descriptors rather than hiding them. Never touch frozen fixture trees.
- [ ] Preserve existing authority-first acquisition and readmission/shutdown ordering.
  NewNode remains I/O-free; no filesystem locks are acquired by constructors.
- [ ] Add functional/race cases for simultaneous Node/recovery/WAL owners, release
  after actual teardown, nested roots, alias/symlink/root substitution, partial
  acquisition and invalid namespaces. Keep existing public WAL and retirement
  conformance intact; use fixed compatible segment settings in parallel fixtures.

#### Verification

- [ ] `make test`; focused functional tests for root Node, authority and both WAL
  facade/backend; race variants for ownership/lease transfer and Stop/Close failures.
- [ ] Run `make lint` and `make test-sim SIM_SEEDS=1-3` now, not just at final
  qualification. Audit the actual pinned gosim hooks: ordinary ownership/startup
  must remain supported (Flock/openat/Renameat/fstatat are hooked; renameat2/linkat
  are not in the inspected ARM64 hooks). Verify compiled recovery helpers do not
  break simulation either; do not bypass ownership checks under simulation tags.
- [ ] Compile all caller modules using public aliases; compare immutable physical
  fixture hashes and normal WAL encode/reopen behavior before/after the move.
- [ ] Prove no pending marker permits normal fresh founding. Record and fix failures.

### Task 3: Expose Node-owned WAL and independent Node lifetimes in the process fixture

**References:** Target §§2/3.3, §§8.4/14.2; D23 and the independent-library scope.
**Files:** UPDATED `tests/cluster/testapp/{app,main,http,store,serving,load_transport,routing}.go`,
local/resource/fault observation handlers, `tests/cluster/controls.go`, fixture
configuration and owned-mode smoke tests. **Dependencies:** Tasks 1–2.
**Outcome:** exercise the new ordinary startup/lease behavior on real bind mounts
before the recovery workflow is implemented. Recovery jobs/routes come in Task 13.

- [ ] Add test-only `config.NodeOwnedWAL bool` (false retains the existing borrowed
  fixture) and an owned copy of construction settings in
  `application.nodeOptions gquorum.NodeOptions`. In owned mode select WAL with the
  current fixture root, `SegmentBytes: 1 << 20` and a pointer to a local false
  Prewrite value; leave Store/Retirement nil and do not open a second a.store.
- [ ] Separate process/management lifetime from each embedded Node generation.
  Stop/join and replace a Node while keeping authenticated management HTTP alive;
  recreate consumed meta listeners and fresh InstanceID. Keep normal startup and
  final whole-process cleanup behavior, and make every handler safe when no Node
  is active. Do not implement the recovery job or call unimplemented recovery I/O.
- [ ] Audit every a.store dereference and every wrapper-dependent handler, including
  local observations and shutdown. Preserve borrowed-mode observations/tests.
  In owned mode expose Node.Status/Identity/ParkedLoads, load.Group.Identity/Status,
  application values and existing transport counters. A stopped/unpublished handle
  is explicitly unavailable; it is not proof that its physical WAL is absent.
- [ ] Extend localObservation and the matching LocalObservation client with the
  exact availability fields below. Do not manufacture StorageIdentity/Stats/SnapshotBytes or
  WAL-wrapper counters from a public runtime identity. Physical-store assertions
  remain in owning-package tests or original borrowed-mode scenarios. Task 13 adds
  recovery Status/List/Inspect evidence for durable recovery-specific positions.
- [ ] In owned mode explicitly refuse POST `/internal/test/lose-store/{group}/{replica}`,
  PUT/DELETE `/internal/test/wal/{group}`, and wrapper-based collection-call counts
  from GET `/internal/test/collections/{group}/{replica}`. Preserve supported controls
  that use public Node/Group APIs, application callbacks, or transport/proxy behavior:
  proposals, snapshots, replica start/stop, read/append-drop controls, metadata proxy
  drops and collection-report holding. Audit newly touched routes with the same rule.
- [ ] Keep the original physical-stat/wrapper tests unchanged in borrowed mode.
  Add owned-mode variants of ordinary create/write/snapshot/retained-restart and
  readmission scenarios using only their available public observations and exact
  post-restart data/identity checks. Do not run an unchanged helper that dereferences
  a.store or asserts physical Storage.Stats against the owned fixture.
- [ ] Add `TestOwnedWALRuntimeSmoke` process coverage through the existing three-node
  pool, plus pure fixture/configuration units and single-participant lifecycle/race
  checks. Use actual bind mounts, repeated Stop/start generations and final cleanup
  to expose lock-file/root-enumeration issues immediately after Task 2.

Test-only DTO delta (all existing fields remain):

```go
// tests/cluster/testapp/hooks_cluster.go: localObservation
// tests/cluster/controls.go: LocalObservation
StorageAvailable    bool              `json:"storage_available"`
WALControlAvailable bool              `json:"wal_control_available"`
GroupIdentity       *storage.Identity `json:"group_identity,omitempty"`
```

StorageAvailable/WALControlAvailable are false in owned mode. GroupIdentity comes
from the actual published load.Group and is nil when unavailable; it is explicitly
not a physical pin probe. Borrowed mode retains the existing Probe/Stats fields
and sets the appropriate capabilities. Recovery-specific evidence is added by
Task 13 through the public recovery values, not by expanding library status APIs.

#### Verification

- [ ] `make test`; focused testapp functional/race checks for both ownership modes,
  nil/stopped Node handling and correct callback/process-resource ownership.
- [ ] Once this fixture batch settles, build `gquorum-recovery:local` and run
  `make test-cluster-run CLUSTER_IMAGE=gquorum-recovery:local CLUSTER_ARGS='-v -run ^TestOwnedWALRuntimeSmoke'`.
  This early targeted process run is required because Task 2 changes every startup;
  it is not a broad cluster run after each library edit.
- [ ] Keep Task 2's passing default-seed simulation evidence unless library code
  changed; rerun it if a smoke fix changes the library. Record image/report/cleanup
  evidence, and preserve the original borrowed-mode regressions.

### Task 4: Implement durable recovery journals, names and discovery

**References:** Target §§3.3, §§5.1–5.3, §§9.1–9.3, §10.2,
§11.3 and §15.10; Appendix B. **Files:** `internal/recoverymodel/{codec,validate}.go`,
CREATED `internal/recoverydisk/{journal,archive}.go` and atomic filesystem helpers,
`recovery_status.go`; tests beside these owners.
**Dependencies:** Tasks 1–3. **Outcome:** durable operation history and startup guards,
with no source archive or application callback yet.

- [ ] Implement bounded canonical encoding/strict decoding for every schema-1 record
  in Appendix B. Reject duplicate/unknown keys, unsafe names, enum/version errors,
  overflow and conflicting immutable fields. Enforce target manifest/header bounds
  before unbounded allocation. Reserved rollback fields decode but authorize no action.
- [ ] Implement Create/Open/Read/List/View/Update and durable progress methods from
  target §10.2, including per-action MarkFiles, cancellation audit/completion,
  SuccessorStartedAt, matching-only ClearCurrent, seed and phase updates. Keep
  immutable acceptance separate from mutable progress; store callbacks nowhere.
- [ ] Generate BackupName once from AcceptedAt UTC and effective RecoveryID; use the
  same name in both roots. Validate directory-name/manifest agreement, duplicates
  across timestamps, root bindings, action audit and monotonic revisions.
- [ ] Implement temp-write/fsync/close/atomic publication/parent fsync, restrictive
  permissions, no-clobber moves and exact evidence reconciliation. Publish no
  successful phase until its facts are durable. Initial approval publication
  failure must preserve A; only identifiable durable accepted evidence can resume.
- [ ] Reconcile a durable manifest with a missing/stale current pointer. A pointer
  never proves completion or overrides another pending record. Terminal cancelled
  history releases its guard before matching pointer clearing; reserved IDs persist.
- [ ] Implement read-only Recovery.Status/List without runtime leases, WAL payload
  scans or quorum calls. Return valid listed statuses plus typed diagnostics for
  invalid entries, sorted by AcceptedAt descending then RecoveryID ascending;
  exclude conflicting duplicate IDs. An empty current pointer means not-found.
- [ ] Preserve historical completed/cancelled records through subsequent operations.
  Missing forensic material cannot authorize source reseeding or deleting B.
  Enforce the startup namespace checks from Task 2 against these exact codecs.
- [ ] Add functional fault cases at manifest/current publication, phase sync and
  pointer clearing; duplicate IDs/names, corrupt manifests, renamed directories,
  unavailable WAL mount during status, and no ownership adoption from names alone.

#### Verification

- [ ] `make test`; model/codec units plus recoverydisk/status functional tests and
  writer/reader race tests. Check both roots' timestamps/names and stable historical IDs.
- [ ] Verify approval-format records alone guard startup and List/Status never invoke
  Files, factories or storage writers. Preserve both/no-path conflict evidence.
- [ ] Record fault-boundary results and fix failures; no public mutation is enabled yet.

### Task 5: Build bounded read-only readers and the shared replay interpreter

**References:** Target §6, §§10.2/10.3, §12 and §14.1 REC-04/05;
Appendix D. **Files:** CREATED `internal/metastore/offline.go`,
`internal/authoritystore/offline.go`, `internal/walbackend/offline.go`,
`internal/raftengine/offline.go`; UPDATED WAL replay/snapshot readers and engine
startup/interpreter extraction; owning tests.
**Dependencies:** Tasks 1–4. **Outcome:** trustworthy immutable source evidence,
without application reconstruction, workers or source repair.

- [ ] Implement the target Reader/OpenReadOnly/Recovery/Evidence methods for metadata
  and authority using existing complete pin/bucket/record validation. No bucket
  creation, migration, writable open or materialized-state rewrite is permitted.
  Bound decoded metadata and close partial acquisitions on every exit.
- [ ] Implement bounded WAL framing/segment inspection, preserving CRC chain and
  segment/offset evidence. Reject unchecked oversized lengths before allocation;
  use bounded descriptors and checked arithmetic. Earlier-segment UnexpectedEOF
  is corruption, not an older complete prefix. Do not broaden upstream torn-CRC claims.
- [ ] Implement nonmutating snapshot selection/CRC checks from retained markers.
  A required corrupt snapshot refuses; orphan files/markers above proven commit
  are reported, never used to raise C. Do not invoke a snapshotter path that renames
  corrupt source files to `.broken` or otherwise repairs them.
- [ ] Enforce `0 < S <= C <= L`, hint ≤ C, contiguous entries, valid envelopes,
  nondecreasing terms and supported native configurations. A final torn record is
  tolerated only with target §6's final-segment and retained-boundary proof.
- [ ] Extract the shared committed-entry interpreter and restored-core initializer.
  Implement `raftengine.Replay`/ReplayOptions/ReplayResult from Appendix D. It starts
  no Raft workers/elections/transport and writes no store. Reuse actual native and
  authority semantics rather than maintaining a divergent source interpreter.
- [ ] Replay metadata with `metaengine.NewMachine(rules, nil)` so AfterApply cannot
  persist to A. Load preflight uses a discarding application machine; no caller
  factory, Restore, Apply, Snapshot, Run or Close runs during inspection.
- [ ] Release group payloads between groups; enforce MaxRecord/MaxGroup/MaxMetadata
  limits before allocation wherever the existing full-buffer contracts permit.
  Keep the stated limitation on application allocations; no hard RSS claim.
- [ ] Add pure malformed-frame/boundary units and real WAL/bbolt functional fixtures
  for early/final EOF, zero-sector/CRC, gaps, descending terms, stale hints, tail > C,
  foreign/unsupported state and read-only byte preservation. Reuse immutable
  fixtures through temporary copies; never regenerate expected legacy data.

#### Verification

- [ ] `make test`; focused functional and relevant race checks for readers and engine
  interpreter extraction. Existing normal replay suites must still pass.
- [ ] Assert source hashes/names unchanged on success, refusal and cancellation;
  prove no source persister/worker is invoked. Test exact replay through C, not H/L.
- [ ] Record resource/refusal evidence and fix every failure before Inspect uses readers.

### Task 6: Implement complete-survivor inspection and exact durable approval

**References:** Target §§3.1–3.3, §5.3, §6, §7.3, §§9.1/10.1/10.2,
§15.1/15.2; brainstorm D01/D06–D09/D18–D23. **Files:**
`internal/recoveryinspect/{inspect,inventory,digest}.go`, metaengine recovery inventory
projection, `recovery_inspect.go`, `recovery_approve.go`; model helpers/tests.
**Dependencies:** Tasks 1–5. **Outcome:** usable public Inspect/Approve, still no
application work or archive move from approval.

- [ ] Reconstruct metadata through retained commit and validate its whole graph,
  pins, authority and ledger. Compare configured source node and optional cluster
  to the retained pin; never infer identity from stale bootstrap declarations.
- [ ] Separate required groups (approved Target plus additive caller assertions)
  from eligible exact donor replicas (approved/proven historical inventory).
  Include a legitimate pending-replacement predecessor; exclude staged-only,
  foreign, ambiguous or retired sources. No global FullyConverged prerequisite.
- [ ] Classify **every** unmatched live WAL as retired, approved-deleted, a proven
  predecessor or a blocker. Collect missing/corrupt/unsupported groups instead of
  silently archiving them. Preserve old operations, cleanup debt and audit summaries.
- [ ] Implement reset inspection without a replay promise: require valid ownership
  and named destination; corrupt load payloads may be archived only when exact
  owner/inventory evidence is established. Contradictory owner schemas still refuse.
- [ ] Generate the effective ID for empty requests and derive a fresh lineage from
  source identity/fingerprint/ID. Allocate destination identities above retained
  floors, preserve tombstones, check overflow and donor voter eligibility. Public
  reports include `_meta` and each required load group's S/C/L/terms and loss risk.
- [ ] Build the canonical accepted digest over all target §5.3 evidence, declared
  hook presence and request/audit. Exclude its own value, acceptance time and
  irrelevant padding. Validate future manifest/import size against normal metadata
  bounds; never approve a source that will need silent evidence omission.
- [ ] Implement Approve(Request,Acceptance) with same-owner reinspection and exact
  comparison, attestation checks, immutable resource latching and journal creation
  in Approved. Repeat existing exact approvals from saved evidence, not evolving B.
  Cancelled IDs cannot be reapproved. No callback/rename/Node startup occurs here.
- [ ] Add REC-02/03/05/06/11/12 units/functional tests: stale evidence, additive
  RequiredGroups, missing coverage, pending replacement, staged-only allocations,
  known retired IDs, generated-ID retries, changed audit/destination, hook mismatch,
  empty-ID Approve refusal and historical repeat after B replaces A.

#### Verification

- [ ] `make test`; inspection/approval functional tests and root lease/approval race
  tests. Exercise metadata-only and unsupported memory/injected-store cases.
- [ ] Prove blockers cause no Files invocation or source move and Approved blocks
  normal startup. Verify all diagnostic positions and potential-loss acceptance.
- [ ] Record results; retain temporary startup refusal for AwaitingStart until Task 10.

### Task 7: Implement backup, cancellation and in-root execution

**References:** Target §§2–5, §9, §11, §15.2/15.3; brainstorm D02/D03/D05/D10/D21.
**Files:** `recovery_run.go`, `recovery_cancel.go`,
`internal/recoverydisk/archive.go`, journal transitions, owning functional/race tests.
**Dependencies:** Tasks 1–6. **Outcome:** complete owned offline executor and
cancellation; public destructive Run remains temporarily guarded until Task 12
finishes the matching Node restore/publication paths.

- [ ] Implement the executor behind a temporary implementation-pending check before
  public Run starts Backup/archive in either mode. Cancelling may still resume
  recorded Undo and terminal/history paths remain readable. Exercise destructive
  transitions through the same private executor in owning-package tests. Keep the
  destructive public check until Task 12, so an intermediate feature commit cannot
  archive A while B startup remains incomplete. This is temporary scaffolding,
  not a public option or final unsupported case.
- [ ] Implement Run's phase dispatch from the exact saved operation. Approved enters
  Files; Files runs Backup; Cancelling resumes only saved cancellation; terminal/
  post-archive phases return history/resume status. Reserved rollback states refuse.
  Do not inspect/rearchive current B as a new A source on retry.
- [ ] Invoke one node-wide FilesBackup with the generated application BackupDir.
  Journal running attempt before callbacks and completion only after nil return.
  Nil Files is an explicit no-external-work declaration, not fallback on hook error.
  A retry of a shared database backup must not reset already reconstructed siblings.
- [ ] On cancellation/error, retain the exact callback/locks until actual return.
  Context cancellation never triggers Undo or releases the pending guard. Produce
  last durable Status plus cause-preserving Refusal with a phase-appropriate Remedy.
- [ ] Implement Cancel's exact allowed phases and write-once actor/reason/time.
  Refuse while another Run owns roots; management must join it first. Any recorded
  Backup attempt requires idempotent FilesUndo, even if Backup failed before mutation.
  Track Undo independently; a failed Undo remains Cancelling and guarded.
- [ ] Verify unchanged original library evidence before/after Undo, record Cancelled
  before clearing the matching current pointer, retain history/reserved ID, and
  handle lost cancellation responses without reactivating the operation.
- [ ] After Backup, revalidate A and persist Archiving before the first library move.
  Rename authority, metadata, verified readmission backups and owned WAL entries
  into their respective in-root backups. Never move a root/mountpoint, copy across
  filesystems, overwrite a destination or collect old forensic data.
- [ ] Reconcile each uncertain move against its immutable evidence and both paths.
  Source absent alone is not success. Both/neither/conflicting paths refuse and
  retain the guard. Parent fsync precedes recording Moved. Finish with durable
  AwaitingStart/JoinReady; no factory, Raft worker or Application.Run executes here.
- [ ] Implement reset preparation through the same machinery, without seed/replay.
  Keep target cluster binding and ownership checks. Cancel has the same pre-archive
  boundary for reset; successful preparation does not claim completed admission.
- [ ] Add comprehensive lifecycle tests for duplicate Run/Cancel, wrong audit,
  source mutation, shared hooks, late callback return, guard release, disk-full/
  rename/sync faults, separate and nested roots, missing mounts and no-clobber
  conflicts. Assert old bytes and backup names remain unchanged across retries.

#### Verification

- [ ] `make test`; root recovery/recoverydisk functional and race suites with all
  callback and archive boundaries. Tests use private roots and joined owners.
- [ ] Show Cancel succeeds through Files, refuses after durable Archiving even before
  its first rename, and Run(Cancelling) never executes Backup/archive.
- [ ] Confirm ordinary startup still refuses incomplete output and no implementation
  claims a running cluster from AwaitingStart/JoinReady. Record and fix failures.

### Task 8: Implement narrowly scoped recovery placement and repair planning

**References:** Target §7.4, §10.4, §15.9; existing placement/repair contracts.
**Files:** CREATED `internal/recoveryplacement/placement.go` and tests; UPDATED
placement preparation/report/planning adapters in metaservice, metaengine and
internal scheduling, including `mutations.go`, `operation_planning.go`,
`scoped_stage.go`, `scoped_approval.go`, repair planning and voter validation owners.
**Dependencies:** Tasks 1–7. **Outcome:** private deficit-aware planning with strict
ordinary behavior; Task 9 supplies the authoritative replicated context.

- [ ] Implement Context and the exact Prepare/Validate/BuildPlan/ValidatePlan/
  CanRelease signatures in Appendix D. Authorize only exact unreleased policy
  scopes from replicated recovery provenance; caller observations/audit text never
  create this permission. Zero context delegates to existing strict behavior.
- [ ] Model source policy deficits for `_meta` and load groups separately from load
  seed consumption. Allow unchanged recovered assignments and safe progressive
  repair, preserving actual identities, selectors/types, pins, reserved/learner-only
  rules, capacities and distinctness of current voters. Never remove the last voter.
- [ ] Normalize only count targets/minima in owned planning/validation copies; call
  existing pure topology helpers and restore original policy fields in output.
  Normalized intent must never be staged, persisted, exposed as desired policy or
  used to bypass catch-up/live-quorum/leadership-transfer checks.
- [ ] Trace **all** refusal paths: declarative preparation, placement explanations,
  checkLoadBounds, ValidateDesired, approval-plan creation, plan-shape validation,
  FSM graph validation, repair candidates and post-callback revalidation. Refactor
  internal helpers where necessary; no public skip-validation option or new
  application-supplied recovery context is allowed.
- [ ] Keep default/custom repair-policy public signatures unchanged. Verify built-in
  candidate selection can make progress toward the retained target with two
  eligible nodes. Internal authorization rechecks exact context after callbacks;
  do not silently rewrite the policy document supplied to applications.
- [ ] Provide the context plumbing initially as absent/default until Task 9 installs
  the optional replicated state. Every ordinary operation must remain equivalent
  in that case. Finish actual recovery-context wiring in Task 9, not an isolated
  adapter used only by tests.
- [ ] Define permanent release based on proven original-policy satisfaction or a
  normally valid explicit policy edit; retirement closes the scope. Completion of
  import alone does not release it. Group policy digests and identity scope must
  still match after restart/pruning; permission never reactivates automatically.
- [ ] Add pure selection/plan cases for singleton→two→three, strict AZ, unchanged
  deficient groups during unrelated edits, legacy/nil voter policy, policy changes,
  release, forged context, worsened deficit, pinned learners and last-voter removal.
  Functional tests cover the service's extra count/report checks with owned views.

#### Verification

- [ ] `make test`; focused placement, scheduling/votercheck, metaengine and
  metaservice tests. Add race coverage only where callback/context plumbing changes.
- [ ] Existing ordinary strict-placement and membership-safety suites stay green;
  all returned/persisted policy values remain the originals.
- [ ] Record exact call sites routed through the adapter for Task 9/16 review.

### Task 9: Add the replicated import, exact consumption and private command gates

**References:** Target §§7.1–7.4, §10.4, §12, §15.4/15.7/15.8;
Appendix C and private metadata ports in Appendix D. **Files:**
CREATED `internal/metaengine/recovery.go`, `internal/metaservice/recovery.go`;
UPDATED scoped state/canonicalization/indexes/graph/retention, codec/dispatch,
stage/approval/allocation/retirement helpers, metaservice RPC/mutation gates.
**Dependencies:** Tasks 1–8. **Outcome:** ordinary v1 founding followed by one atomic
v2 recovery import with preserved proof, not synthetic genesis.

- [ ] Add the optional schema-1 Recovery subtree and three private command kinds
  exactly as Appendix C. Keep outer metadata store/FSM/envelope v4, authority v2
  and engine v2 otherwise unchanged. Ordinary snapshots omit the field and retain
  canonical bytes. Strict validation covers every record/receipt/policy binding.
- [ ] Implement Machine.RecoveryState/RecoveryInventory and the target metaservice
  WaitRecoveryReplay/ImportRecovery/ReadRecovery/RecordRecoveredGroup/CompleteRecovery
  methods. Reads return owned data; waiters must not depend on public Node readiness.
- [ ] Authorize first import only against the accepted local operation and normal
  completed singleton v1 founding, expected version 1, exact source/destination,
  complete source coverage and no intervening topology mutation. Repeated exact
  proposals return original results before first-application preconditions.
- [ ] Prepare the complete import candidate atomically: raise allocation floors,
  reserve old deleted IDs at B's baseline version, derive original approved group
  definitions with fresh single-survivor replicas, retain other names as Excluded,
  and stage/approve v2 through existing owned preparations and graph validation.
  Install no partially staged topology or partially raised floor map on error.
- [ ] Generate ordinary creation steps/grants/receipts. Never copy A operations,
  sessions, locks, admissions or execution authority into B. Floors from staged
  allocation history prevent reuse without importing that unapproved intent.
- [ ] Wire Task 8's exact replicated policy context into all mutation, approval,
  restore/graph and repair paths. Include `_meta`. Persist permanent policy release
  through normal authoritative transitions, never from a local status estimate.
- [ ] Validate consumed group evidence against destination identity, exact ordinary
  host/group receipts and a durable normalized B checkpoint at/after source C.
  Pin/ledger existence and replay observations are insufficient. CompleteImport
  requires all required groups and the actual import operation; handle zero loads.
- [ ] Enforce pending-import command restrictions in the FSM/service in addition
  to Task 10's local manifest-derived gate before the import record exists. A nil
  replicated Recovery field is never permission to bypass an active local recovery
  start. Coordinate handoff with no unguarded interval.
- [ ] Gate general RPC/public topology mutation and joining during import while
  permitting exact internal founding/session/grant/fence/receipt progress. Reject
  private recovery commands at general mutation ingress; codec recognition is not
  authorization. No new protobuf or public metadata mutation method is required.
- [ ] Extend pruning/retention to preserve pending import and consumed identity/
  grant provenance and active policy exceptions. Completed records remain bounded
  history; later retirement/replacement must validate without resurrecting A.
- [ ] Add deterministic FSM tests for atomic failure/no partial candidate, exact
  retries/lost replies, high floors/overflow, tombstones, staged/unapproved source,
  pending topology work, ordinary founding/session proof, premature completion,
  receipt mismatch, post-completion policy repair, pruning and recovered-state restart.

#### Verification

- [ ] `make test`; focused metaengine/metaservice/placement functional suites and
  relevant races. Exercise internal progress while the public mutation gate is closed.
- [ ] Frozen ordinary fixture bytes/hashes remain unchanged. Add separate recovered
  state fixtures/capture provenance and verify the older strict decoder rejects
  the new subtree without claiming mixed-version compatibility.
- [ ] Check all adapter paths consume actual replicated context, private commands
  cannot enter through generic RPC, and no synthetic founding proof is added.

### Task 10: Integrate Node bootstrap, reset admission and the recovery coordinator

**References:** Target §§3.4, §7.1, §9.3/9.4, §10.5, §15.10;
Appendix D coordinator/ports. **Files:** CREATED `internal/recoveryruntime/coordinator.go`
and ports; UPDATED root Node lifecycle/authority/load/meta-authority/lifecycle-hook
composition and joined shutdown; owning functional tests.
**Dependencies:** Tasks 1–9. **Outcome:** exact accepted B startup and coordinator
ownership, with data/publication held until Tasks 11–12 complete restoration.

- [ ] Implement the coordinator Config, MetaPort, HostPort, New/Start/Stop and
  readiness/completion methods from Appendix D. Node owns it; it owns no separate
  application lifetime. Private startup/receipt progress must not wait for the
  public readiness gate the coordinator itself will release.
- [ ] Implement early prepareRecoveryStart under system/WAL ownership, after pure
  option checks and immediately before first B acquisition. Reject Approved/Files/
  Cancelling/Archiving/unsupported rollback and contradictory evidence. Validate
  accepted roots, exact destination/bootstrap mode and pending/historical identity.
- [ ] Activate a private local recovery-start gate from the accepted rebuild
  manifest **before metadata founding/start**, including the interval with no
  replicated Recovery record. Disable scheduler/repair ticks and reject public
  Cluster stage/approve/mutations, admission/join and StartLoadGroup with an existing
  typed fault plus `recovery_in_progress` reason. Allow only exact internal v1
  founding/auto-approval, local session activation and needed grants/receipts.
  The gate and replicated restrictions overlap until committed import completion;
  no caller-supplied context/flag can bypass them. Publication/Run remains separately
  gated through readiness validation. Reset joins retain their distinct allowed
  normal-admission path; do not accidentally block the authorized reset join.
- [ ] Durably mark SuccessorStartedAt before any B authority/meta/WAL creation,
  including reset joins. A failed marker sync permits no B file. The marker never
  clears; existing B evidence without its required pending marker is a conflict.
- [ ] Preserve whether Limits were explicitly supplied before constructor defaults.
  At Start adopt accepted source Rules/Limits for zero input, reject conflicting
  explicit values, and derive only private temporary singleton meta voter settings.
  Leave public helper output/original policy unchanged; import restores source policy.
- [ ] Perform normal OriginFounding v1 admission/session activation, then submit
  the exact stable import request. Retain original IDs/time/actor/comment/CAS on
  uncertain responses and use exact ordinary RetryOperation for failed attempts.
  No blind approval of Latest or unbounded factory/approval retry loop.
- [ ] Install the recovery barrier/manifest context on the managed host before
  ordinary operator actions can create a load. Until seed capability is present,
  refuse those effects explicitly; never allow empty founding during partial work.
- [ ] Transfer the held WAL lease through OpenWithLease with no release/reacquire
  gap, and preserve ownership on partial acquisition. Join coordinator, managed
  effects/callbacks and stores in dependency order on Stop or failed startup.
- [ ] Implement reset JoinReady restrictions: named cluster, Join=true, fresh process
  identity and normal fresh admission. Record JoinedIdentity/Joined only after exact
  durable acquisition. Reject wrong cluster/founding; interrupted admission retries
  cannot archive the successor again. Terminal history does not veto later valid B
  readmission or turn source archives into automatic reseeding permission.
- [ ] Hold the private controller between v1 founding and import in an owning test;
  attempt public Stage/Approve/mutation, a scheduler tick, StartLoadGroup and a
  joining protocol request. Prove no intervening topology change occurs while
  internal founding/session/authority progress remains possible, then release the
  controller and complete the same exact import. Test handoff/restart without a gap.
- [ ] Add functional/race tests for marker failure, admission/stop races, invalid
  phase/root/destination, metadata-only completion, inherited/conflicting limits,
  default/all-founder readiness, lost import response and exact retry ownership.

#### Verification

- [ ] `make test`; Node/metaservice/recoveryruntime functional and race suites.
  Until Task 12 releases the public destructive Run check, prepare these disposable
  startup fixtures through the same private executor, not a public bypass flag.
- [ ] Assert ordinary nil-recovery startup/shutdown remains unchanged and every
  pending recovery path is gated before application work/publication.
- [ ] Verify no premature B acquisition or duplicate WAL lease owner, and that reset
  cannot silently join a different lineage. Record remaining seed/publication wiring
  as Tasks 11–12 work, not successful recovery.

### Task 11: Implement durable recovered WAL seeds and normalized checkpoints

**References:** Target §8.1–8.3, §10.3, §15.5/15.7; Appendix D storage/engine
contracts and Appendix B seed provenance. **Files:** `internal/walbackend/recovery.go`,
private managed backend adapter, `internal/raftengine/{group,startup,offline,recovery_checkpoint}.go`,
writer/control/snapshot owners and functional tests.
**Dependencies:** Tasks 1–10. **Outcome:** valid new-identity retained input, with
real durable proof distinct from publication, replay and consumption.

- [ ] Implement private recoveryStore adapter and backend functions from Appendix D.
  Keep all new recovery operations inside internal packages. ProtectRecovery runs
  before opening a group and forces fsync independently of ordinary DisableFSync.
- [ ] Revalidate archived source semantics/digest under the accepted operation and
  exact ordinary creation grant. Build a complete private staged B directory with
  fresh pin/metadata, source application snapshot at S, B singleton ConfState and
  the actual B creation grant installed at that retained snapshot position.
- [ ] Copy application entries `(S,C]` byte-for-byte with original indexes, terms,
  RequestID/correlation. Validate source administrative/configuration semantics,
  then replace only those entries with empty normal entries at the same positions.
  Exclude `(C,L]`, retain source HardState.Term, set Vote=0/Commit=C and hint=S.
- [ ] Fsync the complete staged store and marker before no-clobber directory
  publication/parent sync. Record prepared then installed evidence. Preserve owned
  incomplete staging for reconciliation; refuse foreign paths. Never publish a
  partial WAL, overwrite a live store or rename a mountpoint.
- [ ] Distinguish never-published staging from an installed/acquired identity.
  Missing live WAL after installation/acquisition parks; never silently recreate
  that established B identity from A. Retry grants must follow the exact immutable
  predecessor chain and destination without rewriting original seed provenance.
- [ ] Add the private NewRecoveryContext path using the startup owner's lifetime,
  removing the fixed five-second Restore cutoff for this recovery path only.
  Keep ordinary startup semantics and callback ownership intact. Cancellation
  joins restore/engine work; waiter timeout does not cancel accepted startup.
- [ ] Implement RecoveryCheckpoint through the serialized engine/writer control
  path. Persist a portable normalized B application snapshot and return actual
  durable Position/Digest. Validate checkpoint/seed identity through the backend;
  status/applied observations are not durability proof.
- [ ] Permit legitimate intermediate B checkpoints during resumed unconsumed
  replay: the initial seed checksum is not a permanent live-store checksum.
  After normalized checkpoint plus replicated consumption, A is never refill input.
- [ ] Implement FinishRecovery at a joined writer boundary after exact consumed
  proof; restore the configured ordinary durability option without weakening
  another pending group. Manifest/marker writes always remain synchronous.
- [ ] Validate seeded authority positions through the real metaengine authority
  catalog: B's creation grant/receipt/fence uses the source snapshot index/term,
  and a subsequent legitimate higher-revision group effect has strictly later
  position and nondecreasing term. Recheck after B restart and metadata pruning;
  include a real promote/transfer/demote path in Task 14. A fabricated monotonic
  tuple alone is not a substitute for validating the actual retained receipts.
- [ ] Add functional tests for exact transformed entries/config/authority, tail
  exclusion, index continuity, snapshot normalization, interrupted staging/rename,
  post-install missing data, retry grants, long Restore, DisableFSync protection
  and checksum changes caused by legitimate B snapshots.

#### Verification

- [ ] `make test`; focused WAL/engine functional and race suites, including existing
  normal startup/snapshot/compaction tests after extraction.
- [ ] Validate seeded stores through the ordinary retained reader and no InitialAuthority
  override; compare preserved A bytes. Verify forced synchronization before any
  returned checkpoint/seed proof and correct ownership after errors.
- [ ] Record results; no data is publicly released until Task 12 validates all groups.

### Task 12: Complete managed replay, consumption, publication and shutdown

**References:** Target §§3.2, §8.2–8.4, §10.5, §11, §15.6–15.8;
Appendix D host/coordinator ports. **Files:** `internal/managed/{replica,application,granted_actions,recovery}.go`,
load ingress/publication/observation owners, `internal/recoveryruntime`, root
load/lifecycle/readiness/shutdown composition and functional/race tests.
**Dependencies:** Tasks 1–11. **Outcome:** fully usable library rebuild/reset workflow
through normal Node ownership, including all completion and stop races.

- [ ] Install recovery context before the ordinary host's effects can start. Under
  the one exact startup reservation, begin/validate the ordinary host effect,
  protect/install the seed, then **re-probe** it and select retained startup with
  no InitialAuthority. Never use a stale pre-install fresh probe.
- [ ] Supply an owned RecoveryInfo to the normal GroupFactory only for unconsumed
  recovery input. The machine may accept the declared A snapshot or a resumed B
  snapshot and must emit portable B state. Preserve ordinary factory timeout,
  late-success transfer, failed-factory cleanup and serialized Close retries.
- [ ] Introduce private-ready completion distinct from public start completion.
  Exact granted internal actions may obtain the ready core and finish receipts;
  public LoadGroup/StartLoadGroup, application proposals/forwarding, observations
  that imply ready publication and Application.Run remain gated as specified.
- [ ] Preserve Task 10's manifest-derived gate from before v1 founding through
  replicated import completion; the public readiness gate is not a substitute
  for guarding direct Cluster mutations or join ingress before that record exists.
- [ ] Ensure the operator never waits for a public handle to finish the import that
  will publish that handle. Preserve current grant/identity/floor rechecks after
  waits and scope changes; no unguarded raw-core access becomes public.
- [ ] Coordinate serial required-group replay, normalized checkpoints, ordinary
  host/group receipts and exact RecordRecoveredGroup proposals. Persist local
  consumed progress only from confirmed replicated evidence. Resume lost replies
  idempotently and never confuse ledger existence with consumption.
- [ ] Complete import only after every required group and the actual ordinary
  operation are complete. Support metadata-only recovery. Publish all recovered
  handles for application validation, but hold Application.Run until existing
  serialized lifecycle readiness approves the initial recovery startup.
- [ ] Recheck mandatory recovery facts, exact identity and stop state; finish temporary
  durability protection at joined boundaries, persist local Complete, then release
  deferred Run. A racing stop wins without a late callback launch. External traffic
  remains application-controlled; historical Status.Complete is not live readiness.
- [ ] Join coordinator/effects/replay/Run before Close and storage release on failure,
  cancellation, readmission or Stop. Failed Close retains ownership and prevents a
  successor. Preserve StopLoadGroup holds, ordinary nil-Recovery behavior and later
  valid B readmission. Remove all temporary library recovery stubs, including the
  public Run guard from Task 7, only after both rebuild/reset restart paths are wired.
- [ ] Add meaningful multi-group **single-participant** functional/race tests for
  one group failing while another replays, stalled callbacks, explicit holds,
  receipts before publication, validation failure, slow Restore, late factory
  success, stopped handles, missing consumed B WAL and repeated terminal recovery IDs.

#### Verification

- [ ] `make test` and `make lint`; complete focused root/managed/engine/recoveryruntime functional
  and race suites. Use real single-participant Node APIs where lifecycle wiring is
  under test; do not add in-process multi-node fixtures.
- [ ] Prove no early Application.Run/public business path, no import/publication
  deadlock, no overlapping unresolved owner and no old-source reseeding after B
  accepts newer work. Validate reset admission completes without A data replay.
- [ ] Verify all public methods now execute their intended behavior or documented
  final refusals, not implementation-pending placeholders. Record/fix failures.

### Task 13: Add durable recovery jobs and independent remote test controls

**References:** Target §§2–4, §9, §11, §13, §14.2; no Meridian integration.
**Files:** CREATED testapp recovery worker/job/files-hook/HTTP files; UPDATED the
Task 3 fixture generation/observation support and harness clients.
**Dependencies:** Tasks 1–12. **Outcome:** complete public-API recovery scenarios
can run remotely while the embedded Node is stopped.

- [ ] Extend Task 3's already-tested Node-owned fixture and independent management
  lifetime; do not recreate the borrowed-store wrapper for recovery fault injection.
  Keep existing controls/capability distinctions and generation-safe routing.
- [ ] Implement an application-owned durable job using public recovery values.
  Persist maintenance before stopping A, effective Request/digest before confirmation,
  acceptance intent before Approve, and cancellation intent before joining Run.
  Reconcile public Status after process restart without parsing private journals.
- [ ] Register authenticated test-only prepare/confirm/retry/cancel/status/list routes
  under `/internal/test/`. Model target §4 semantics without implementing Meridian
  endpoints. Paths and backup names remain server-configured; request contexts do
  not own long work. Add Recovery.Status/List and Inspect report positions to the
  owned-mode observations; none of these observations is proof of current quorum.
- [ ] Before approval cancel only the fixture preparation job. After approval join
  Run and call Cancel with stable audit; release maintenance only after Cancelled.
  Repeat lost HTTP replies using the same IDs/request/acceptance. Test every supported
  control through the owned-mode client, never an unsupported physical-stat helper.
- [ ] Supply node-wide FilesBackup/FilesUndo for fixture-owned affiliation/projection
  state at fixed paths using the generated BackupDir. Exclude the active maintenance
  job from archival. Use real small application files/shared projection to exercise
  Backup/Undo, not Meridian SQL/workflow repair.
- [ ] Start B using RecoveryStartOptions, recreated listeners and fresh InstanceID;
  switch application transport/routing to B before service and reject A delivery
  reports. Reconcile affiliation/boot mode from the job before old pin checks.
- [ ] Exercise readiness validation and counters/barriers proving Application.Run is
  held. Use normal GroupFactory and recovery input, not a test-specific recovery
  engine. Keep management available through failure, retry and cancellation.
- [ ] Make REC-04's load-tail setup executable using existing public-transport test
  controls: stabilize a load leader and committed snapshot/prefix; enable inbound
  DropAppend on both followers (drops MsgApp/MsgSnap, not heartbeat traffic); propose
  a unique command to the leader; observe dropped-append counters and unchanged
  commit/application values; stop lost peers and prepare recovery of that leader
  while the drops remain active. Require Inspect to report LastLog > Commit before
  accepting the test setup. A proposal timeout/queue counter alone is not durable
  tail proof. Metadata-tail/CRC/corruption proofs stay in component tests.
- [ ] Add pure input/job-state units and single-participant functional/race tests for
  persistent maintenance, lost responses, unknown IDs, cancelling Run, callback
  joins, HTTP availability and process generation/affiliation replacement.

#### Verification

- [ ] `make test`, `make lint`; testapp functional/race checks using public APIs.
  Compile normal/process-tag variants. No import of gquorum/internal, new library
  HTTP API or arbitrary remote filesystem argument is introduced.
- [ ] All Node-owned observations work without a.store; unsupported controls refuse
  explicitly and borrowed-mode tests retain their original physical assertions.
- [ ] Verify tail preparation checks actual offline Inspect evidence and all
  job/Node/process resources join. Build the final process image after edits settle.

### Task 14: Validate permanent loss and returning nodes in the three-node Docker pool

**References:** Target §13 scenario reuse, §§14.1/14.2, §11 flows; source GQ-01
REC-01–REC-12. **Files:** CREATED `tests/cluster/permanent_majority_loss_*_test.go`
and public fixture clients; small existing pool/harness extensions only as needed.
**Dependencies:** Tasks 1–13. **Outcome:** real process/consensus acceptance tests
for the requirements requested by Meridian, without running Meridian.

- [ ] Build the settled Node-owned recovery test image once with an explicit tag;
  use existing isolated pool resources and authenticated controls. Ordinary cases
  use three original nodes; return/re-admit the two old nodes rather than adding
  a five/seven-node matrix. Call t.Parallel before independent resource acquisition.
- [ ] REC-01: write real application data/snapshots, stop two voters, remotely inspect/
  approve/run the complete survivor, restart B on identical roots, verify new lineage
  and monotonic identities, exact retained data/deduplication and successful new writes.
  Management remains responsive throughout and no operator shell/file copy is needed.
- [ ] REC-02/04: hold the donor behind writes acknowledged by the other majority and
  separately create a valid uncommitted load tail using Task 13's follower
  DropAppend setup. Require offline Inspect LastLog > Commit and an absent unique
  tail command before proceeding; fail setup instead of accepting a mere proposal
  timeout. Metadata-tail proof remains in component tests. Prove loss acknowledgement,
  report positions and recovered prefix correctness; never assert zero loss from
  donor readability or apply beyond its retained commit.
- [ ] REC-03/11: use sparse placement for an incomplete survivor and staged/unapproved
  edits plus approved in-flight work/cleanup for a complete one. Missing coverage
  refuses with names; approved groups recover; old work/debt remains archived without
  being relabelled complete. No global convergence prerequisite is introduced.
- [ ] REC-06/07/12: reject stale inspection, active owner, changed acceptance/audit/
  destination and repeated cancelled IDs. Retry exact remote operations after lost
  replies without double archive, duplicated import or unintended new identity.
- [ ] REC-08/10: kill/restart at observable management, hook, archive/start/import
  boundaries available through public behavior. Use deterministic barriers; keep
  finer private filesystem cut points in owning-package tests from Tasks 4–12.
  Confirm no early Run/readiness and status/journal resume across process death or disconnected HTTP callers.
- [ ] Keep rejoin snapshots/frames comfortably below the fixture's current 64 MiB
  send/receive limit (including encoding overhead). Large local recovery resources
  are measured separately in Task 15; do not claim GQ-02 remote transfer coverage.
- [ ] REC-09: return A nodes and delayed old packets/reports; ordinary retained pins
  cannot join B. Explicit reset-for-join backs up their state, then normal B admission
  grants fresh identities. Verify wrong-destination refusal, no merge/reseed, data
  catch-up and new writes after each returning node.
- [ ] Rebuild source `_meta` and loads with strict three-AZ/MinReplicas policy, then
  grow singleton→two→three through ordinary administration/repair. Desired policy
  never silently drops; second-node growth and unrelated safe edits work while a
  deficit remains; normal safety and permanent release hold after restart.
  Validate actual later group-effect receipts through the authority catalog after
  the seeded creation receipt, including restart/pruning (Task 11).
- [ ] Include remote Cancel before approval, approved/no-hook cancellation, FilesUndo
  retry, refusal after Archiving, generated-name/List history and same-directory
  backups. Confirm consumed B state survives later normal restart and old retries.
- [ ] Assert teardown: no leaked containers/volumes/network workers or replaced
  shared image during a run. Keep complete failure reports and fixture logs.

#### Verification

- [ ] `make test-cluster-build CLUSTER_IMAGE=gquorum-recovery:local`, then
  `make test-cluster-run CLUSTER_IMAGE=gquorum-recovery:local CLUSTER_ARGS='-v -run ^TestPermanentMajorityLoss'`.
  Use that prefix for the planned scenarios so filtering is executable.
- [ ] After all scenario/image changes settle, run the **entire** cluster suite using
  the same image. Rebuild only after fixture changes and rerun affected/full gates
  as appropriate; record image, report paths, exit status and cleanup evidence.
- [ ] Complete the REC mapping with actual tests, distinguishing detailed component
  corruption/ownership evidence from three-process consensus behavior.

### Task 15: Qualify resources, compatibility, all suites and current architecture docs

**References:** Target §§3.3, §12–14, §16; current testing/physical compatibility
contracts. **Files:** recovery resource benchmarks/profile fixtures in owning packages;
README/doc.go, `docs/architecture/{library,runtime,raft,placement,project-map}.md`,
relevant testing/capture records and this plan. **Dependencies:** Tasks 1–14.
**Outcome:** measured supported behavior, honest current documentation and complete
library regression evidence; no Meridian integration work.

- [ ] Measure metadata-only, 64 MiB snapshot plus tail, multiple sequential groups,
  near-1 GiB aggregate group and just-over-limit cases with real fsync and ordinary
  DisableFSync settings. Record S/C/L, encoded sizes, group count, effective bounds,
  wall time, peak process RSS, allocations, temporary disk and cancellation/join latency.
  Design defaults are not measurements and no hard application-RSS guarantee is added.
- [ ] Put large fixtures in opt-in benchmarks/resource qualification, not fast units
  or every default functional run. Use `BenchmarkRecoveryResources` under the owning
  functional build where real stores are required. Measure a compiled test binary
  separately from compilation when reporting peak process RSS; record OS/tool units.
- [ ] Verify every bounds refusal occurs before uncontrolled allocation/mutation
  where the contracts allow, including manifest/import size under ordinary metadata
  limits. If selected defaults fail qualification, record a release-blocking gap
  and a concrete resolution; do not truncate data or silently widen them.
- [ ] Repeat `make bench BENCH_TIME=1s` with the same settings as Task 1 and compare
  matching planner/metadata-snapshot cases, allocations and timing. Run dedicated
  `make test-perf CLUSTER_IMAGE=gquorum-recovery:local PERF_PARALLEL=1` after all
  image changes settle, isolated from other heavy suites. A parallel full cluster
  run is not the comparable dedicated performance measurement. Investigate regressions
  without weakening existing gates; record actual source/image/hardware and results.
- [ ] Finalize separate recovery-format fixture provenance and old-decoder refusal
  evidence while preserving all existing frozen hashes. Check ordinary nil-recovery
  snapshots, public WAL aliases, normal borrowed-store support and all caller modules.
- [ ] Update **current implemented** architecture/README/package docs only after the
  behavior exists. Explain the public handle/lifecycle, same-root backups, loss
  semantics, Node-owned scope, reset join, policy deficits, consumed evidence and
  callback ownership. Distinguish ordinary restart recovery from disaster recovery.
- [ ] Document remote embedding as an independent caller pattern/test fixture.
  Do not add a library HTTP server or claim Meridian is integrated. Document reserved
  Rollback and other exclusions with `TODO(not planned): ...` at relevant code
  boundaries, typed refusals where callable, and no repeated warning noise.
- [ ] Run all required library test scopes listed below: regular units/all modules,
  lint, unit race, functional, functional race, full Docker cluster, dedicated
  performance, before/after benchmarks, existing Compose examples and default-seed
  simulations. GQ-02 remains separate; these tests must not
  claim unsupported large remote snapshot rejoin is solved by local reconstruction.
- [ ] Save actual commands, fresh reports, source/image revision, fixture hashes,
  resource measurements and cleanup outcome in the Progress log/validation record.
  Diagnose every failure; missing prerequisites are reported blockers, not passes
  or implicit waivers. No Meridian build/test/deployment appears in the evidence.

#### Verification

- [ ] All commands in Validation Commands pass, including their real multi-module/
  process scopes. Large benchmark/resource runs produce measurements, not assertions
  copied from the design. Repeat a gate only after relevant changes or unresolved failures.
- [ ] Documentation paths/API names/commands match implemented behavior;
  `git diff --check` passes, and no baseline fixture was regenerated to satisfy tests.
- [ ] No in-scope implementation or validation gap is hidden by a documentation claim.

### Task 16: Verify the architectural delta, close gaps and prove library wiring

**References:** Target **all sections**, Appendices A–D, brainstorm D01–D23 and the
user's independent-library clarification. **Files:** this plan and only affected
implementation/tests/docs when closing discovered gaps. **Dependencies:** Tasks 1–15.
**Outcome:** every in-scope target contract exists, matches and is reachable through
gquorum's real entry points. This is not a Meridian wiring review.

- [ ] Verify public root/recovery/load API delta against Appendix A and target §3:
  exact signatures/types/defaults, constructor purity, helper copies, typed errors,
  no public Paths/Session/codec/force/Rollback and no public Storage interface growth.
- [ ] Verify local persistence delta against Appendix B/§5: every record/tag/phase,
  immutable field, timestamped name, cancellation/Undo fact, startup marker,
  current/history semantics, bounds, atomic fsync ordering and ownership constraint.
  Confirm unchanged existing buckets/formats, no SQL migration/cache service and
  no adopted unknown namespace or unsupported rollback execution.
- [ ] Verify source inspection/replay (§6/§10.2/§10.3): complete required inventory,
  exact donor eligibility, every WAL classified, read-only metadata with nil
  persister, strict framing/snapshot selection, valid S/C/L and digest/acceptance.
- [ ] Verify metadata/import/placement delta (§7/§10.4): ordinary founding, atomic
  stage+approve, source policy/floors/tombstones, ordinary grants/receipts, strict
  private ingress, actual service/FSM/scheduler integration, permanent policy
  release, pruning/restart and new-fixture/old-decoder behavior.
- [ ] Verify WAL/engine delta (§8/§10.3): exact seed transformation, real fsync,
  prepared/installed/replayed/consumed distinctions, re-probe/no InitialAuthority,
  owned Restore context, portable checkpoints and no refill of lost consumed B.
- [ ] Verify Node/managed/coordinator delta (§9/§10.5): early marker, limits/bootstrap,
  lease transfer, manifest-derived pre-import mutation/join gate, gap-free FSM
  handoff, normal factory lifetime, private-ready/public barrier, readiness
  validation, Application.Run release, exact retry, reset admission and joined Stop.
- [ ] Verify independent fixture/test coverage (§11/§13/§14): all REC cases, remote
  management without Node/quorum, durable job/cancellation behavior, strict policy
  regrowth, old-lineage refusal, actual resource measurements and every test scope.
  Target §4's Meridian-specific integration is an approved deferral, not a missing
  gquorum endpoint or a reason to inspect that application's wiring.
- [ ] Verify exclusions (§16) remain explicit comments/refusals with no accidental
  fallback, and current docs describe only implemented/validated behavior. Ensure
  historical backups/proposals did not leak superseded APIs into code.
For the following failure-boundary audit, reuse the preceding owning tasks'
verified tests and add/fix only missing evidence. This is not a duplicate test suite.

- [ ] Map each target crash/failure row to an actual test/assertion and fill gaps;
  do not duplicate tests merely to match a filename or implementation branch.
  Cover initial approval publication, pointer loss, Backup/Undo attempt/result,
  Archiving-before-first-rename, every archive move and phase publication.
- [ ] Cover SuccessorStartedAt failure/uncertain startup, normal v1/import lost
  responses, prepared/installed seed ambiguity, checkpoint-before-consumed reply,
  replicated completion-before-local handoff and later B WAL loss. A non-identifiable
  partial record must refuse with original evidence, never fabricate accepted work.
- [ ] Exercise system-only/separate/nested roots, unavailable mounts, root alias/
  replacement, no-follow traversal, both/neither rename paths, unknown entries,
  cancelled/history/current-pointer ambiguity and duplicate IDs with new timestamps.
- [ ] Exercise required snapshot corruption, gaps/term regressions, unsupported
  schemas, contradictory authority/hints, oversized early-segment length, valid
  final torn tail and future orphan snapshot. Check original bytes/names on every
  success/refusal; no repair/truncation/`.broken` rename of A.
- [ ] Exercise factory/Restore/Apply/Snapshot/Run/Close failure, waiter cancellation,
  noncooperative late return, concurrent Cancel/Run/Start/Stop and serial ownership
  retry. Prove no callback or owner is abandoned after a timeout and no successor
  starts until the previous owner actually releases.
- [ ] Exercise complete approved inventory despite unfinished topology, unrelated
  staged Latest, predecessor donors, archived unresolved cleanup, imported high-water
  marks and tombstones, exact idempotency, ordinary post-recovery retirement and
  another later recovery from B. Old historical identity is not a permanent veto.
- [ ] Verify the policy adapter remains active after import and releases permanently
  only on proof/valid policy change, with ordinary strict behavior for unrelated
  groups. Test the extra service count/report paths identified in the research table.
- [ ] Keep filesystem faults in instance-owned owning-package seams and tests; do
  not add global failpoints or production force switches for process testing.
  Each test owns its roots/listeners/transport and joins cleanup under race runs.

- [ ] Record every mismatch in NOT IMPLEMENTED GAPS with target reference, evidence,
  owner and resolution. Fix each in-scope gap and rerun affected validation. A truly
  uncorrected item is marked `[_]` with its reason; do not mark this plan complete
  while required work remains. Only explicit user approval can defer in-scope work.
- [ ] **Verify final actual wiring:** construct the configured Recovery handle through
  public NewRecovery, execute Inspect/Approve/Run/Cancel/Status/List through real
  library paths, transform startup options, start Node, observe exact import/seed/
  consumption/readiness and normal new writes, then reset/join a returning process.
  Trace internal constructors, ports, startup/shutdown gates and authenticated test
  routes to real calls—not just isolated helpers or passing tests. Verify nil-recovery
  normal callers still use their ordinary paths. No Meridian wiring is checked.

#### Verification

All prior validation evidence must describe the final code after gap fixes. Use
focused reruns for each correction, then refresh impacted full-suite gates; an
unresolved failure or `[_]` gap prevents completion. The last implementation
checkbox above deliberately verifies real composition/reachability after gap closure.

## Validation Commands

Run from the project root with its pinned toolchain. These commands are for
**implementation**, not claims that this planning session executed tests.
Each task runs `make test` and focused checks appropriate to its changes. At the
qualification stage all library test scopes below are required. A passing report
from an earlier task remains valid if no later change affects what it exercised;
do not repeat broad suites solely because documentation or this plan changed.

| Scope | Command / condition |
| --- | --- |
| All five application modules, fast uncached units/build | `make test` |
| All-module vet/style/unused variants, including process compilation | `make lint` |
| Fast units with race detector | `make test-race` |
| All owning-package functional tests | `make test-function` |
| Functional races | `make test-function-race` |
| Settled process image | `make test-cluster-build CLUSTER_IMAGE=gquorum-recovery:local` |
| Focused new process cases | `make test-cluster-run CLUSTER_IMAGE=gquorum-recovery:local CLUSTER_ARGS='-v -run ^TestPermanentMajorityLoss'` |
| Entire process/cluster suite | `make test-cluster-run CLUSTER_IMAGE=gquorum-recovery:local` (equivalent full suite after the explicit build; `make test-cluster` may be used instead when a rebuild is needed) |
| Existing planner/metadata performance baseline and comparison | `make bench BENCH_TIME=1s` before Task 1 Go changes and again in Task 15 |
| Dedicated process performance | `make test-perf CLUSTER_IMAGE=gquorum-recovery:local PERF_PARALLEL=1` after the fixture settles; do not run concurrently with another image build/heavy suite |
| Existing Compose examples, public caller integration | `make test-example` (the complete example-tag suite; use named scheduler/cache targets for focused diagnosis) |
| Existing deterministic simulations | `make test-sim SIM_SEEDS=1-3` (regression coverage, no new simulation framework/dependency in the library) |
| Recovery resource qualification | `go test -tags=functional -run '^$' -bench '^BenchmarkRecoveryResources$' -benchtime=1x -benchmem ./internal/walbackend ./internal/recoveryinspect` after Task 15 adds the benchmarks |
| Generated protocol consistency | `make proto-check` if a protocol/generated artifact changed; the approved design requires no protobuf delta |
| Documentation/patch hygiene | `git diff --check`; explicit Markdown link/fence/path checks for ignored documents |

Examples of focused, owner-specific gates during implementation:

```sh
make test UNIT_ARGS='-run Recovery'
make test-function FUNCTIONAL_ARGS='-run Recovery'
make test-function-race FUNCTIONAL_ARGS='-run Recovery'
go test -tags=functional -count=1 ./internal/walbackend ./storage/walstore
go test -tags=functional -race -count=1 ./internal/managed ./internal/recoveryruntime
```

Use actual test names/owners introduced by each task; expand beyond a `Recovery`
filter for modified normal startup/placement/retirement paths. A filter matching
zero tests is not validation. Make targets cover separate modules; root
`go test ./...` alone does not. Keep reports under `test-reports/<suite>/` and read
report.md/timeline.md/package logs on failures. Do not run multi-node tests under
functional tags or hide them behind testing.Short.

For peak RSS qualification, compile the selected functional test binary first,
then measure its benchmark execution using the platform's process resource tool
(macOS `/usr/bin/time -l`, Linux `/usr/bin/time -v`) so compiler memory is not
reported as recovery memory. Record units, selected case, binary/source revision,
filesystem, wall time and temporary bytes. Run large measurements without unrelated
heavy suites in parallel. No new chaos/performance framework is added. Existing dedicated performance
and planner/snapshot benchmarks protect ordinary-path cost; controlled crash cases
and the target's recovery resource measurements cover the new failure/size scope.

## Requirement-to-evidence map

Test names are planned names until implemented. Update this map with actual
owners/names/report paths; preserve every required assertion.

| Requirement | Primary component evidence | Process evidence |
| --- | --- | --- |
| REC-01 complete survivor/new writes | `TestRecoveryMetadataOnly`, managed import/readiness tests | `TestPermanentMajorityLossRebuild` |
| REC-02 acknowledged-write loss risk | inspection/acceptance loss checks | `TestPermanentMajorityLossLaggingSurvivor` |
| REC-03 incomplete sparse coverage | inventory classification and no-effects assertions | `TestPermanentMajorityLossMissingGroup` |
| REC-04 committed prefix only | strict metadata/load reader, Replay and seed exact-entry proofs | `TestPermanentMajorityLossCommittedPrefix`: follower DropAppend control; offline L>C required before recovery; unique tail command never applied |
| REC-05 corrupt/foreign/unsupported source | WAL, snapshot, authority, metadata fixtures with unchanged originals | Public refusal paths where reproducible without private-format application code |
| REC-06 changed source after Inspect | `TestRecoveryApprovalSourceChanged`, backup revalidation | `TestPermanentMajorityLossApprovalAndRetry` |
| REC-07 live owner refusal | `TestRecoveryExclusiveOwners`, lease handoff/races | ApprovalAndRetry busy-owner case |
| REC-08 interrupted durable work | journal/archive/seed/checkpoint crash matrices | `TestPermanentMajorityLossInterruptedWorkflow` |
| REC-09 returning old nodes/messages | exact ingress/reset/grant tests | `TestPermanentMajorityLossResetAndRejoin` |
| REC-10 callback/Close failure/cancellation | managed lifecycle and owner-join races | InterruptedWorkflow and readiness/Run barriers |
| REC-11 approved vs staged/debt | metadata inventory/import/floor/tombstone/pruning cases | `TestPermanentMajorityLossApprovedTopology` |
| REC-12 conflicting repeat | immutable operation/audit/destination/terminal history tests | ApprovalAndRetry and ResetAndRejoin repeats |
| Policy repair after import | adapter plus service/FSM/scheduler call-path tests | `TestPermanentMajorityLossStrictPolicyRegrowth` |
| Cancel/Undo, remote management and generated names | journal/root API/testapp functional/race tests | `TestPermanentMajorityLossCancelAndHistory` |
| Resource and compatibility bounds | Task 15 benchmarks, frozen fixtures/old decoder | Existing full caller/cluster/Compose/simulation regressions |

These scenarios originate in the requirement/target and Meridian's tested behavior,
but execute against gquorum and its independent fixture only. Meridian's old
max(commit, applied), log truncation/fallback, Latest authority and retained
lineage behavior must not be copied as expected outcomes.

## NOT CONSIDERED & TODO

Already agreed exclusions are carried forward; no renewed scope approval is
needed. Use the exact comment form `// TODO(not planned): ...` at the applicable
implementation boundary. A callable unsupported path returns an existing typed
fault/refusal; a comment or log alone must not allow unsafe progress. Do not add
public stubs just to represent deferred features.

| Tags | Excluded behavior | Required treatment / owner |
| --- | --- | --- |
| postponed; too-complex | Multi-survivor merge, arbitrary backup import, cross-group recovery-point selection | Inspect reports missing coverage/unsupported source and refuses. No hidden source search or merger. |
| postponed | Intentionally dropping/resetting an approved group | RequiredGroups only supplements inventory; missing data blocks. No allow-loss switch. |
| postponed | Memory/custom/injected-store or log-only recovery | NewRecovery/Inspect typed unsupported_source; preserve evidence. Node-owned standard WAL only. Ordinary custom-store support elsewhere remains. |
| postponed | Rollback after Archiving | Reserve schema fields/marker/phases; export no Rollback method. Unsupported rollback records remain guarded. Explicit pre-archive Cancel/Undo is implemented. |
| postponed | Backup purge and destroyed-host cleanup waiver | Preserve originals/debt; no automatic deletion or false collection proof. |
| potential-improvement | Streaming application snapshots and hard RSS control | Existing full-buffer API, finite encoded bounds and measured behavior; no unbounded fallback. GQ-02 remote transfer remains separate. |
| postponed; porting:postponned-to-the-next-task | Meridian HTTP/UI/jobs, SQL/workflow validation, deployment and wiring | No Meridian work or wiring audit in this plan. Only independent public-API fixture tests model required scenarios. |
| postponed; porting:too-high-risk | Meridian commit reconstruction/SQL repair/in-place tail truncation | Do not port; retain gquorum commit evidence and application-owned semantics. |
| potential-improvement; porting:improvement-over-upstream | Compaction of completed replicated import provenance | Retain bounded original record; no speculative proof deletion in this release. |
| postponed | Mixed-version/downgrade migration | Reject unsupported formats with evidence intact; no compatibility fallback or older-binary guard promise. |

## NOT IMPLEMENTED GAPS

No execution audit has run yet. This section tracks **discovered mismatches**, not
the unchecked planned work above. Every entry must include target section,
implementation evidence, impact, assigned task and resolution/validation.

Use `- [ ]` for an open gap, `- [x]` only after the fix and relevant validation,
and `- [_]` for a gap explicitly left uncorrected with its reason. Required gaps
prevent completion; an accepted out-of-scope item belongs in the table above.
Do not pre-mark the absence of implementation as a passed delta audit.

## NOT PLANNED CHANGES

None during planning. During implementation record each necessary out-of-plan
change with path, rationale, relation to a required fix, validation and commit/diff
reference. This includes fixes for unrelated failing checks; do not silently grow
scope or discard user changes.

## Progress log

### Planning — 2026-10-25

- Read the current target, final brainstorm decisions D19–D23 and peer closure;
  the only approved WAL mode is Node-owned configuration.
- Inspected baseline `2684202`, current architecture/testing contracts, persistence,
  managed startup, metadata approval/placement and independent process fixture.
- User accepted execution order, then limited the plan to at most 16 substantial
  tasks and excluded Meridian wiring checks. Those instructions are reflected above.
- No implementation, runtime tests, Docker runs or resource measurements performed
  during planning. Only the plan is produced; target/brainstorm remain unchanged.
- Planning validation passed: exactly 16 sequential tasks, explicit target references/
  files/dependencies/verification for each, final wiring checkbox last, REC-01–REC-12
  coverage, all local links, balanced code fences and clean whitespace. Thirteen
  declaration blocks match the target verbatim; source fingerprints still match.
  Every named Make target exists. These are document checks, not runtime test results.
- The invoked skill requests a documentation commit. This checkout has no separate
  documentation repository; commit only this plan on the existing feature branch.
  The ignored target/brainstorm and pre-existing review file are not staged.

### Peer-review corrections — 2026-10-25

- Reviewed Thread 3 round 1 against the current plan/code and pinned gosim source.
  Verdict was ready with corrections. No implementation/runtime tests were run.
- B1: Task 10 now explicitly activates a local manifest-derived gate before v1
  founding and before any replicated import record exists, with exact internal
  allowlisting and a gap-free handoff. Tasks 9/12/16 verify it too.
- B2/B3: moved owned-WAL fixture/lifetime/public-observation groundwork to Task 3,
  with an early focused real-process smoke run. Jobs/routes remain Task 13. Physical
  Store.Stats/Probe are not fabricated from runtime identity; unsupported controls
  are explicit. The former standalone crash audit is retained in Task 16. Still 16 tasks.
- M1: REC-04 now uses existing follower DropAppend controls and requires offline
  Inspect L>C, not a proposal timeout. The process scenario covers a load tail;
  complete metadata-tail/corruption proof remains in component tests.
- M2/M3: all root enumerations are audited and simulations/lint run after Task 2.
  The resource handler counts actual FDs, not disk bytes; lock descriptors stay visible.
  Hook support was read from pinned gosim v0.9.1, not runtime-tested in this review.
- M4: validate seeded and later higher-revision authority receipts through the real
  catalog after restart/pruning. M5: baseline/after make bench and dedicated serial
  test-perf supplement recovery-size qualification and the full cluster suite.
- L1–L4: added intermediate lint, corrected app.go:314, retained temporary public Run
  refusal until complete restart paths, and bounded process rejoin frames below
  the existing 64 MiB transport limit. No public API or approved behavior changed.
- Target/brainstorm and their recorded hashes remain unchanged: these are explicit
  implementation/verification refinements of the approved design, not new API proposals.

## Appendix A: Public target declarations

The following snippets are copied from target §3. They are the target state for
Task 1 and the implementation checks; omitted import lists are not API changes.
```go
package recovery

type Mode string
const (
    Rebuild      Mode = "rebuild"
    ResetForJoin Mode = "reset_for_join"
)

type Request struct {
    RecoveryID     string   `json:"recovery_id,omitempty"` // empty: generate during Inspect
    Mode           Mode     `json:"mode"`
    ClusterID      string   `json:"cluster_id,omitempty"`
    Actor          string   `json:"actor"`
    Comment        string   `json:"comment"`
    RequiredGroups []string `json:"required_groups,omitempty"`
}

type Acceptance struct {
    PlanDigest          string `json:"plan_digest"`
    AcknowledgeDataLoss  bool   `json:"acknowledge_data_loss"`
    OldDeploymentFenced bool   `json:"old_deployment_fenced"`
}

type Position struct {
    Index uint64 `json:"index"`
    Term  uint64 `json:"term"`
}

type Remedy string
const (
    RemedyRetry       Remedy = "retry"
    RemedyCancel      Remedy = "cancel"
    RemedyInvestigate Remedy = "investigate"
)

type Issue struct {
    Code     string `json:"code"`
    GroupID  string `json:"group_id,omitempty"`
    Blocking bool   `json:"blocking"`
    Message  string `json:"message"`
    Remedy   Remedy `json:"remedy,omitempty"`
}

type GroupRecovery struct {
    GroupID  string           `json:"group_id"`
    Source   storage.Identity `json:"source"`
    Snapshot Position         `json:"snapshot"`
    Commit   Position         `json:"commit"`
    LastLog  Position         `json:"last_log"`
    Bytes    uint64           `json:"bytes"`
    Usable   bool             `json:"usable"`
}

type Summary struct {
    LatestVersion       uint64                 `json:"latest_version"`
    ApprovedVersion     uint64                 `json:"approved_version"`
    OperationsNotResumed []topology.OperationID `json:"operations_not_resumed"`
    UnresolvedCleanup   []storage.Identity     `json:"unresolved_cleanup"`
    SessionsNotRestored uint64                 `json:"sessions_not_restored"`
    LocksNotRestored    uint64                 `json:"locks_not_restored"`
    RetiredGroupIDs     []string               `json:"retired_group_ids"`
}

type Plan struct {
    Digest                    string            `json:"digest"`
    Request                   Request           `json:"request"`
    Source                    topology.Identity `json:"source"`
    Identity                  topology.Identity `json:"identity"`
    Resources                 Resources         `json:"resources"`
    Groups                    []GroupRecovery   `json:"groups"`
    Archived                  Summary           `json:"archived"`
    Issues                    []Issue           `json:"issues"`
    MayLoseAcknowledgedWrites bool              `json:"may_lose_acknowledged_writes"`
}

type Operation struct {
    RecoveryID string    `json:"recovery_id"`
    PlanDigest string    `json:"plan_digest"`
    BackupName string    `json:"backup_name"`
    AcceptedAt time.Time `json:"accepted_at"`
    Mode       Mode      `json:"mode"`
}

type FilesAction string
const (
    FilesBackup FilesAction = "backup"
    FilesUndo   FilesAction = "undo"
)

// One node-wide action, not one per group. Retries may invoke it again.
// All fields are owned. Both actions must be idempotent for this operation.
type FilesTask struct {
    Action     FilesAction       `json:"action"`
    RecoveryID string            `json:"recovery_id"`
    PlanDigest string            `json:"plan_digest"`
    Mode       Mode              `json:"mode"`
    Source     topology.Identity `json:"source"`
    ClusterID  string            `json:"cluster_id"`
    Groups     []string          `json:"groups"`
    BackupDir  string            `json:"backup_dir"`
}
type FilesHook func(context.Context, FilesTask) error

// Advanced overrides; zero selects library defaults. Units are encoded bytes.
type Resources struct {
    MaxRecordBytes   uint64 `json:"max_record_bytes,omitempty"`
    MaxGroupBytes    uint64 `json:"max_group_bytes,omitempty"`
    MaxMetadataBytes uint64 `json:"max_metadata_bytes,omitempty"`
}
type Options struct {
    Files     FilesHook
    Resources Resources
}

type Phase string
const (
    Approved       Phase = "approved"
    Files          Phase = "files"
    Cancelling     Phase = "cancelling"
    Cancelled      Phase = "cancelled"
    // Reserved values: this version never initiates rollback.
    RollingBack    Phase = "rolling_back"
    RolledBack     Phase = "rolled_back"
    Archiving      Phase = "archiving"
    AwaitingStart  Phase = "awaiting_start"
    Importing      Phase = "importing"
    Validating     Phase = "validating"
    Complete       Phase = "complete"
    JoinReady      Phase = "join_ready"
    Joined         Phase = "joined"
)

type GroupProgress struct {
    GroupID string   `json:"group_id"`
    State   string   `json:"state"` // pending, prepared, installed, replayed, consumed
    Applied Position `json:"applied"`
    Issue   *Issue   `json:"issue,omitempty"`
}
type Status struct {
    BackupName string            `json:"backup_name"`
    AcceptedAt time.Time         `json:"accepted_at"`
    RecoveryID string            `json:"recovery_id"`
    PlanDigest string            `json:"plan_digest"`
    Mode       Mode              `json:"mode"`
    Phase      Phase             `json:"phase"`
    Identity   topology.Identity `json:"identity"`
    Resources  Resources         `json:"resources"`
    Groups     []GroupProgress   `json:"groups"`
    Issue      *Issue            `json:"issue,omitempty"`
    Complete   bool              `json:"complete"`
}
```

```go
package gquorum

// A configuration handle only; no I/O, leases, callbacks or goroutines here.
type Recovery struct { /* immutable private source configuration/options */ }

// Zero or one options value; more than one is Invalid. Common case: NewRecovery(opts).
func NewRecovery(opts NodeOptions, options ...recovery.Options) (*Recovery, error)
func (r *Recovery) Inspect(ctx context.Context, req recovery.Request) (recovery.Plan, error)
func (r *Recovery) Approve(ctx context.Context, req recovery.Request, acceptance recovery.Acceptance) (recovery.Operation, error)
func (r *Recovery) Run(ctx context.Context, id string) (recovery.Status, error)
func (r *Recovery) Cancel(ctx context.Context, id, actor, reason string) (recovery.Status, error)
func (r *Recovery) Status(ctx context.Context, id string) (recovery.Status, error)
func (r *Recovery) List(ctx context.Context) ([]recovery.Status, error)

// Pure configuration helper. See the explicit field changes in §3.4.
func RecoveryStartOptions(status recovery.Status, opts NodeOptions) (NodeOptions, error)

// Reserved concept only: no exported Rollback method in this release.
```

```go
type Refusal struct {
    Issue Issue
    Cause error
}
func (e *Refusal) Error() string
func (e *Refusal) Unwrap() error
```

```go
type RecoveryInfo struct {
    RecoveryID   string
    Source       storage.Identity
    SnapshotIndex uint64
    CommitIndex  uint64
    CommitTerm   uint64
}

type ReplicaSpec struct {
    Identity        storage.Identity
    Node            topology.Identity
    Group           topology.Group
    ApprovedVersion uint64
    Recovery        *RecoveryInfo // NEW; nil for ordinary acquisition
}
```

## Appendix B: Complete local journal schema delta

Copied from target §5.2. These are private records; existing metadata/authority
bucket schemas and application databases are not replaced by these models.
Tasks 1/4 define and implement them; Tasks 7/10/11/12 own their transitions.

```go
// Private source roots, derived once from NodeOptions; not a public API.
type Paths struct {
    StorageDir string `json:"storage_dir"`
    LoadWALDir string `json:"load_wal_dir"`
}
type Current struct {
    BackupName string `json:"backup_name"`
    Schema     uint32 `json:"schema"`
    RecoveryID string `json:"recovery_id"`
    PlanDigest string `json:"plan_digest"`
}
type LocalManifest struct {
    BackupName string `json:"backup_name"`
    Schema      uint32        `json:"schema"`
    Revision    uint64        `json:"revision"`
    Request     Request       `json:"request"`
    Acceptance  Acceptance    `json:"acceptance"`
    Paths       Paths         `json:"paths"`
    Resources   Resources     `json:"resources"`
    Source      topology.Identity `json:"source"`
    Identity    topology.Identity `json:"identity"`
    Fingerprint string        `json:"fingerprint"`
    PlanDigest  string        `json:"plan_digest"`
    Rules       topology.Rules `json:"rules"`
    Approved    topology.Document `json:"approved"`
    Floors      []AllocationFloor `json:"floors"`
    Retired     []string       `json:"retired"`
    Sources     []SeedSource   `json:"sources"`
    Archive     []ArchiveEntry `json:"archive"`
    Files       FilesProgress  `json:"files"`
    Seeds       []SeedProgress `json:"seeds"`
    Phase       Phase         `json:"phase"`
    Failure     *Issue        `json:"failure,omitempty"`
    AcceptedAt  time.Time     `json:"accepted_at"`
    CompletedAt *time.Time    `json:"completed_at,omitempty"`
    JoinedIdentity *topology.Identity `json:"joined_identity,omitempty"`
    Cancellation *ActionAudit `json:"cancellation,omitempty"`
    CancelledAt *time.Time `json:"cancelled_at,omitempty"`
    SuccessorStartedAt *time.Time `json:"successor_started_at,omitempty"`
    Rollback *ActionAudit `json:"rollback,omitempty"` // reserved; never initiated in v1
    RolledBackAt *time.Time `json:"rolled_back_at,omitempty"` // reserved
}
type AllocationFloor struct {
    GroupID string `json:"group_id"`
    NextID  uint64 `json:"next_id"`
}
type SeedSource struct {
    GroupID      string           `json:"group_id"`
    Identity     storage.Identity `json:"identity"`
    MemberID     uint64           `json:"member_id"`
    SourceVersion uint64          `json:"source_version"`
    Snapshot     Position         `json:"snapshot"`
    Commit       Position         `json:"commit"`
    LastLog      Position         `json:"last_log"`
    HardStateTerm uint64          `json:"hard_state_term"`
    Digest       string           `json:"digest"`
    Bytes        uint64           `json:"bytes"`
}
type ArchiveEntry struct {
    Root           string `json:"root"` // system or wal
    Name           string `json:"name"` // one top-level relative entry
    Kind           string `json:"kind"` // regular_file or directory
    Classification string `json:"classification"`
    EvidenceDigest string `json:"evidence_digest"`
    Moved          bool   `json:"moved"`
    Restored       bool   `json:"restored"` // reserved reverse-move proof
}
type ActionAudit struct {
    Actor  string    `json:"actor"`
    Reason string    `json:"reason"`
    At     time.Time `json:"at"`
}
type FilesProgress struct {
    Required bool `json:"required"`
    Backup HookProgress `json:"backup"`
    Undo HookProgress `json:"undo"`
}
type HookProgress struct {
    State string `json:"state"` // pending, running, complete
    Attempt uint64 `json:"attempt"`
}
type SeedProgress struct {
    GroupID    string           `json:"group_id"`
    Destination storage.Identity `json:"destination"`
    State      string           `json:"state"` // pending/prepared/installed/replayed/consumed
    Attempt    uint64           `json:"attempt"`
    MarkerDigest string         `json:"marker_digest,omitempty"`
    Checkpoint Position         `json:"checkpoint"`
}
type SeedMarker struct {
    Schema      uint32                   `json:"schema"`
    RecoveryID  string                   `json:"recovery_id"`
    PlanDigest  string                   `json:"plan_digest"`
    Source      SeedSource               `json:"source"`
    Destination storage.Identity        `json:"destination"`
    MemberID    uint64                   `json:"member_id"`
    Grant       topology.AuthorityGrant `json:"grant"`
    ImageDigest string                  `json:"image_digest"`
}
```

## Appendix C: Replicated recovery schema and commands

Copied from target §7.2. The scopedState snippet shows the **only new field**;
all existing fields/encodings remain. The import/group/policy/consumption and
proposal records below are complete new shapes. In internal/recoverymodel use
the corresponding exported internal aliases specified by target §10; do not
create two encodings. Groups/Sources are load seeds; Policies also includes `_meta`.

```go
type scopedState struct {
    // All existing fields and encodings remain in their existing order.
    Recovery *recoveryImportState `json:"recovery,omitempty"` // NEW
}
type recoveryImportState struct {
    Schema        uint32                `json:"schema"` // 1
    RecoveryID    string                `json:"recovery_id"`
    PlanDigest    string                `json:"plan_digest"`
    Source        topology.Identity     `json:"source"`
    Survivor      storage.Identity      `json:"survivor"` // B's exact _meta identity
    Version       uint64                `json:"version"` // recovery topology version, initially 2
    Phase         string                `json:"phase"` // importing or complete
    Groups        []recoveryImportGroup `json:"groups"`
    Policies      []recoveryPolicyState `json:"policies"`
    AcceptedAt    time.Time             `json:"accepted_at"`
    CompletedAt   *time.Time            `json:"completed_at,omitempty"`
}
type recoveryImportGroup struct {
    GroupID       string           `json:"group_id"`
    Source        SeedSource       `json:"source"`
    Destination   storage.Identity `json:"destination"`
    Consumed      *recoveryConsumed `json:"consumed,omitempty"`
}
type recoveryPolicyState struct {
    GroupID        string `json:"group_id"` // includes _meta
    PolicyDigest   string `json:"policy_digest"`
    PolicyReleased bool   `json:"policy_released"`
}
type recoveryConsumed struct {
    Identity       storage.Identity       `json:"identity"`
    Checkpoint     Position               `json:"checkpoint"`
    SnapshotDigest string                 `json:"snapshot_digest"`
    HostReceipt    topology.EffectReceipt `json:"host_receipt"`
    GroupReceipt   topology.EffectReceipt `json:"group_receipt"`
}

// New private metadata command kinds, encoded in the existing envelope.
type RecoveryImportProposal struct {
    Schema          uint32              `json:"schema"`
    RecoveryID      string              `json:"recovery_id"`
    PlanDigest      string              `json:"plan_digest"`
    ExpectedVersion uint64              `json:"expected_version"` // exactly 1
    Source          topology.Identity   `json:"source"`
    Survivor        storage.Identity    `json:"survivor"`
    ApprovedSource  topology.Document   `json:"approved_source"`
    Sources         []SeedSource        `json:"sources"`
    Floors          []AllocationFloor   `json:"floors"`
    Retired         []string            `json:"retired"`
    Actor           string              `json:"actor"`
    Comment         string              `json:"comment"`
    At              time.Time           `json:"at"`
    ProposalID      string              `json:"proposal_id"`
}
type RecoveryGroupConsumedProposal struct {
    RecoveryID string           `json:"recovery_id"`
    PlanDigest string           `json:"plan_digest"`
    GroupID    string           `json:"group_id"`
    Evidence   recoveryConsumed `json:"evidence"`
    ProposalID string           `json:"proposal_id"`
}
type CompleteRecoveryImportProposal struct {
    RecoveryID string    `json:"recovery_id"`
    PlanDigest string    `json:"plan_digest"`
    At         time.Time `json:"at"`
    ProposalID string    `json:"proposal_id"`
}
```

## Appendix D: Internal signatures, ports and backend composition

Copied from target §§10.2–10.5. The task references establish when each seam is
implemented and wired. Comments indicating opaque private ownership fields do
not authorize omitting the durable fields in Appendices B/C.

```go
// internal/recoverydisk/owner.go — CREATED
type Owner struct { /* stable locks and root-relative descriptors */ }
func Acquire(ctx context.Context, paths model.Paths) (*Owner, error)
func (o *Owner) CheckRoots() error
func (o *Owner) Close() error
// Transfer only to Node-owned backend acquisition; offline methods never detach.
type Lease struct { /* exact root lock and descriptor owner */ }
func (o *Owner) TakeWALLease() (*Lease,error)
func (l *Lease) Close() error

// internal/recoverydisk/journal.go — CREATED
type Journal struct { /* immutable accepted plan; serialized atomic revisions */ }
func Create(ctx context.Context, owner *Owner, accepted model.AcceptedPlan,
    acceptance model.Acceptance) (*Journal, error)
func Open(ctx context.Context, owner *Owner, recoveryID string) (*Journal, error)
func Read(ctx context.Context, paths model.Paths, recoveryID string) (model.LocalManifest, error)
func List(ctx context.Context, paths model.Paths) ([]model.LocalManifest, error)
func (j *Journal) View() model.LocalManifest
func (j *Journal) Update(ctx context.Context, next model.LocalManifest) error
func (j *Journal) MarkFiles(ctx context.Context, action model.FilesAction, state string, attempt uint64) error
func (j *Journal) BeginCancellation(ctx context.Context, audit model.ActionAudit) error
func (j *Journal) CompleteCancellation(ctx context.Context, at time.Time) error
func (j *Journal) MarkSuccessorStarted(ctx context.Context, at time.Time) error
func (j *Journal) ClearCurrent(ctx context.Context) error
func (j *Journal) MarkArchive(ctx context.Context, root, name string) error
func (j *Journal) MarkSeed(ctx context.Context, progress model.SeedProgress) error
func (j *Journal) SetPhase(ctx context.Context, phase model.Phase, issue *model.Issue) error

// internal/recoverydisk/archive.go — CREATED
func Archive(ctx context.Context, owner *Owner, journal *Journal) error
func ReconcileArchiveEntry(ctx context.Context, owner *Owner, entry model.ArchiveEntry,
    backupName string) (moved bool, err error)
func ValidateNamespace(ctx context.Context, root string) error
func HasLiveEvidence(ctx context.Context, root string) (bool, error)
func AtomicWrite(root *os.Root, relative string, data []byte, replace bool) error
func RenameNoReplace(root *os.Root, from, to string) error
func SyncParents(root *os.Root, relatives ...string) error

// internal/recoveryinspect/inspect.go — CREATED
type SourceState struct { /* owned source FSM/evidence; never an application machine */ }
func Inspect(ctx context.Context, owner *recoverydisk.Owner, request model.Request,
    resources model.Resources) (model.AcceptedPlan, error)
func InspectRebuild(ctx context.Context, state SourceState, request model.Request) (model.AcceptedPlan, error)
func InspectReset(ctx context.Context, state SourceState, request model.Request) (model.AcceptedPlan, error)
func ClassifyReplicas(ctx context.Context, state SourceState) ([]model.ReplicaEvidence, []model.Issue, error)
func DeriveIdentity(source topology.Identity, recoveryID, fingerprint, override string,
    metaFloor uint64) (topology.Identity, error)
```

```go
// internal/raftengine/offline.go — CREATED
type ReplayOptions struct {
    Identity storage.Identity
    MemberID uint64
    InstanceID string
}
type ReplayResult struct {
    Index, Term uint64
    ConfState raftpb.ConfState
    Authority raftstate.AuthorityState
}
func Replay(ctx context.Context, opts ReplayOptions, retained storage.Recovery,
    machine Machine) (ReplayResult, error)

// internal/raftengine/group.go/startup.go — UPDATED
func newRestoredCore(ctx context.Context, opts Options, state groupRecovery) (*Group,error)
func NewRecoveryContext(ctx context.Context, opts Options, key model.SeedKey) (*Group,error)

// internal/raftengine/recovery_checkpoint.go — CREATED
func (g *Group) RecoveryCheckpoint(ctx context.Context, key model.SeedKey) (model.CheckpointProof,error)
```

```go
// internal/managed/recovery.go — CREATED, private capability (not an app extension)
type recoveryStore interface {
    RecoveryRoot() string
    ProtectRecovery(ctx context.Context, key model.SeedKey) error
    InstallRecoverySeed(ctx context.Context, seed model.SeedSpec) (model.SeedMarker,error)
    ReadRecoverySeed(ctx context.Context, key model.SeedKey) (model.SeedMarker,bool,error)
    VerifyRecoveryCheckpoint(ctx context.Context, key model.SeedKey,
        point model.Position) (model.CheckpointProof,error)
    FinishRecovery(ctx context.Context, key model.SeedKey, proof model.Consumed) error
}
```

```go
// storage/walstore/store.go — UPDATED, existing public facade
// Existing exported constants are forwarded unchanged.
type Store = walbackend.Store
type Options = walbackend.Options
type IdentityMismatchError = walbackend.IdentityMismatchError
func Open(opts Options) (*Store, error) // delegates to walbackend.Open

// internal/walbackend/recovery.go — CREATED, internal functions only
func OpenWithLease(opts Options, lease *recoverydisk.Lease) (*Store,error)
func RecoveryRoot(s *Store) string
func ProtectRecovery(ctx context.Context, s *Store, key model.SeedKey) error
func InstallRecoverySeed(ctx context.Context, s *Store, seed model.SeedSpec) (model.SeedMarker,error)
func ReadRecoverySeed(ctx context.Context, s *Store, key model.SeedKey) (model.SeedMarker,bool,error)
func VerifyRecoveryCheckpoint(ctx context.Context, s *Store, key model.SeedKey,
    point model.Position) (model.CheckpointProof,error)
func FinishRecovery(ctx context.Context, s *Store, key model.SeedKey, proof model.Consumed) error
```

```go
// internal/metaengine/recovery.go — CREATED
func (m *Machine) RecoveryState() *model.ImportState
func (m *Machine) RecoveryInventory(ctx context.Context,node string) (model.SourceInventory,error)
func (s *scopedState) applyRecoveryImport(index uint64,p RecoveryImportProposal) (proposalApplication,error)
func (s *scopedState) applyRecoveryGroupConsumed(index uint64,p RecoveryGroupConsumedProposal) (proposalApplication,error)
func (s *scopedState) applyCompleteRecoveryImport(index uint64,p CompleteRecoveryImportProposal) (proposalApplication,error)
func validateRecoveryState(state *scopedState,index *scopedIndexes) error
func (s *scopedState) recoveryCommandAllowed(kind ProposalKind,body []byte) error

// internal/recoveryplacement/placement.go — CREATED; pure, no I/O
type Context struct { /* exact scopes/policy digests from replicated import */ }
func Prepare(ctx Context,view topology.ReadView,edited topology.Document,
    rules topology.Rules,at time.Time) (topology.PreparedPlacement,error)
func Validate(ctx Context,approved,base,desired topology.Document,rules topology.Rules) error
func BuildPlan(ctx Context,input topology.PlanInput,desired topology.Document,
    options topology.PlanOptions) (topology.Plan,error)
func ValidatePlan(ctx Context,input topology.PlanInput,desired topology.Document,plan topology.Plan) error
func CanRelease(ctx Context,progress topology.GroupProgress,target topology.Document) bool

// internal/metaservice/recovery.go — CREATED
func (g *Group) WaitRecoveryReplay(ctx context.Context) error
func (g *Group) ImportRecovery(ctx context.Context,p RecoveryImportProposal) (topology.OperationID,error)
func (g *Group) ReadRecovery(ctx context.Context) (*model.ImportState,error)
func (g *Group) RecordRecoveredGroup(ctx context.Context,p RecoveryGroupConsumedProposal) error
func (g *Group) CompleteRecovery(ctx context.Context,p CompleteRecoveryImportProposal) error
```

```go
// internal/recoveryruntime/coordinator.go — CREATED
type Config struct {
    Manifest model.LocalManifest
    Journal *recoverydisk.Journal
    Meta MetaPort
    Host HostPort
    OnChange func()
}
func New(config Config) (*Coordinator,error)
func (c *Coordinator) Start(ctx context.Context) error
func (c *Coordinator) Stop(ctx context.Context) error
func (c *Coordinator) DataReady() bool
func (c *Coordinator) ReadyToPublish(ctx context.Context,identity topology.Identity) error
func (c *Coordinator) CommitLocalCompletion(ctx context.Context,identity topology.Identity) error

// internal/managed/recovery.go — CREATED
func (h *Host) SetRecovery(ctx context.Context,state *model.ImportState,source model.LocalManifest) error
func (h *Host) PrepareRecoverySeed(ctx context.Context,ref load.ReplicaRef,grant topology.AuthorityGrant) error
func (h *Host) RecoveryCheckpoint(ctx context.Context,ref load.ReplicaRef) (model.CheckpointProof,error)
func (h *Host) PublishRecovery(ctx context.Context,identity topology.Identity) error
func (h *Host) ReleaseRecoveryRun(ctx context.Context,identity topology.Identity) error
```

```go
// internal/recoveryruntime/ports.go — CREATED; Node wires these privately.
type MetaPort interface {
    Identity() topology.Identity
    WaitRecoveryReplay(context.Context) error
    ImportRecovery(context.Context, model.ImportProposal) (topology.OperationID,error)
    ReadRecovery(context.Context) (*model.ImportState,error)
    RecordRecoveredGroup(context.Context, model.GroupConsumedProposal) error
    CompleteRecovery(context.Context, model.CompleteImportProposal) error
    Operation(context.Context, topology.OperationID) (topology.OperationStatus,error)
    ReadAuthority(context.Context, topology.OperationID) (topology.AuthorityStatus,error)
    RetryOperation(context.Context, topology.OperationID) (topology.OperationID,error)
}
type HostPort interface {
    SetRecovery(context.Context, *model.ImportState, model.LocalManifest) error
    RecoveryCheckpoint(context.Context, load.ReplicaRef) (model.CheckpointProof,error)
    PublishRecovery(context.Context, topology.Identity) error
    ReleaseRecoveryRun(context.Context, topology.Identity) error
}
```

The private backend values described textually by target §10.3 have this field
shape (no new serialized/public format):

```go
type SeedKey struct {
    RecoveryID string
    PlanDigest string
    Destination storage.Identity
    MemberID uint64
}
type SeedSpec struct {
    Key SeedKey
    Source SeedSource
    Grant topology.AuthorityGrant
    Resources Resources
}
type CheckpointProof struct {
    Identity storage.Identity
    Position Position
    Digest string
}
```

Read-only store method signatures from target §10.2:

```go
// internal/metastore/offline.go
func OpenReadOnly(ctx context.Context, path string, maxBytes uint64) (*Reader,error)
func (r *Reader) Identity() (Identity,error)
func (r *Reader) Recovery(ctx context.Context) (storage.Recovery,map[string]ReplicaRecord,error)
func (r *Reader) Close() error

// internal/authoritystore/offline.go
func OpenReadOnly(ctx context.Context, path string, maxBytes uint64) (*Reader,error)
func (r *Reader) Identity() (topology.Identity,error)
func (r *Reader) Evidence(ctx context.Context) (model.AuthorityEvidence,error)
func (r *Reader) Close() error

// internal/walbackend/offline.go, replay.go and snapshot.go
func ReadRecovery(ctx context.Context, root,group string, limits model.ReadBounds) (model.WALEvidence,error)
func readLogStrict(ctx context.Context, dir string, point walpb.Snapshot, limits model.ReadBounds) (model.WALEvidence,error)
func readSnapshotStrict(ctx context.Context, dir string, markers []walpb.Snapshot, maxBytes uint64) (raftpb.Snapshot,[]model.Issue,error)
```

## Planning source fingerprints

These identify the reviewed documents at plan creation, not a runtime trust or
recovery-plan mechanism. If an approved design changes, update the references and
reconcile affected tasks rather than implementing stale declarations.

- `docs/brainstorms/20261024-permanent-majority-loss-recovery-target.md`: SHA-256 `587068c901f0174a682bd2b9eb8dad491cc9d09d70490682dab96ca60df7d845`.
- `docs/brainstorms/20261024-permanent-majority-loss-recovery.md`: SHA-256 `5ae72f61c80ec4b0bb1e8cfe208c93ea1440f1ffac30e69e1a4a394c07be040e`.
