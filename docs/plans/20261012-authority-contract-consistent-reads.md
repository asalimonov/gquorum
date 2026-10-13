# Authority fencing and consistent reads implementation plan

Plan: `docs/plans/20261012-authority-contract-consistent-reads.md`
Source: `./`
Target: `docs/brainstorms/20261012-authority-contract-consistent-reads-target.md`
Session: `docs/brainstorms/20261012-authority-contract-consistent-reads.md`

Status: complete, including the example simplification follow-up (Task 20).
Tasks 1–19 completed at `da25a79`, with implementation `c0a768c` and their recorded
validation. Task 20's final validation is recorded below. The user authorized
commits grouping related subtasks. No backward compatibility is required.
Preserve old baseline assets unchanged and test explicit rejection where formats change.

## Task list

Group related subtasks into reviewable commits, as requested by the user. Run
focused checks while editing and `make test` after each stable implementation
batch. Keep Docker cluster runs for major integration milestones and the final
validation, never after small edits.

### Task 1: Authority value types and exact validation

References: target §§3.3, 3.5, 4.1.
- [x] Add grant/resource/key/evidence/receipt/status declarations, owned clones,
  structural equality and strict action/resource/target validation.
- [x] Add classified authority-fenced and authority-pending errors.
- [x] Test malformed scopes, altered actions, proof shapes and copy ownership.
- [x] Verification: focused topology/fault units and `make test`.

### Task 2: Complete engine snapshot and entry codecs

References: target §4.3, §9 snapshot algorithm.
- [x] Implement dependency-light raftstate codecs and authority snapshot validation.
- [x] Distinguish application, authority, observed-effect, transfer and managed /
  standalone configuration entries; reject malformed and old formats.
- [x] Verification: codec corruption/round-trip units and `make test`.

### Task 3: Read options and serialized request ownership

References: target §§3.1, 4.4, 6 read engine.
- [x] Add optional query interface/adapters and validated read configuration.
- [x] Implement bounded read admission, ownership, cancellation and cleanup.
- [x] Verification: settings/ownership tests and `make test`.

### Task 4: Safe ReadIndex rounds and message admission

References: target §§4.4, 5.6, 9 read algorithms.
- [x] Implement frozen cohorts, safe ReadIndex, Ready.ReadStates, apply waiting.
- [x] Bound upstream rounds, retries, remote source admission and term resets.
- [x] Run Query on serialized machine owner; retain overdue callback ownership.
- [x] Verification: focused single-engine functional/race regressions; `make test`.

### Task 5: Public load and metadata read APIs

References: target §§3.1–3.2, 5.7, 6 metadata queries.
- [x] Expose load.Read/ReadBarrier and optional application Query adapters.
- [x] Add atomic topology and committed-lock queries using captured meta engine.
- [x] Wire read options through managed, standalone and meta constructors.
- [x] Verification: API and local functional tests, `make test`, relevant vet.

### Task 6: Local authority database and durable identity

References: target §4.2, §6 authoritystore.
- [x] Implement explicit-path bbolt store, pin validation, monotonic pending /
  completed fences and exact running/completed local effects.
- [x] Preserve floors during exact readmission rebind; reject missing retained state.
- [x] Verification: isolated durable functional/race tests and `make test`.

### Task 7: Metadata authority catalog and bounded grant issuance

References: target §§4.1, 5.1, 6 metaengine.
- [x] Add catalog/floor/receipt schemas, codec proposals and index-based revisions.
- [x] Issue exact current-step grants; reserve revocation capacity through Limits.
- [x] Validate/rebuild indexes during restore, retain evidence through pruning.
- [x] Verification: deterministic FSM units, snapshot regressions, `make test`.

### Task 8: Supersession, revocation obligations and receipts

Split into 8a (operator revocation and receipt FSM) and 8b (nonoperator grants,
remaining approval paths and authoritative catalog queries).

References: target §§5.2–5.3, 9 supersession/RecordFence.
- [x] Close issuance and carry unresolved obligations across supersession/retries.
- [x] Record exact fence/consumption evidence; reject unrelated/foreign receipts.
- [x] Cover nonoperator transfer/readmission grants and consumed effects.
- [x] Verification: FSM state-transition/retention tests and `make test`.

### Task 9: Integrate complete snapshots and explicit formats

Split into 9a (complete snapshot boundaries) and 9b (entry and physical format
changes and final rejection fixtures).

References: target §§4.1, 4.3, 5.5.
- [x] Wrap/unwrap complete snapshots at startup, live install and snapshot creation.
- [x] Update meta snapshot preflight and atomic materialized-state installation.
- [x] Bump explicit formats; retain immutable old fixture assets with rejection tests.
- [x] Verification: focused storage/recovery functional tests and `make test`.

### Task 10: Group authority installation and membership consumption

Split into 10a (managed mode, founding state and durable installation) and 10b
(exact membership consumption and rejected-entry progression).

References: target §§2, 5.4, 6 raftengine, 9 authority/config algorithms.
- [x] Add managed mode, initial authority, ordered fence installation and receipt reads.
- [x] Enforce exact grants on owner loop and deterministic configuration application.
- [x] Preserve live-quorum/native safety and recovered administrative-entry gates.
- [x] Verification: stale/rejected-then-valid config, snapshot/tail tests; `make test`.

### Task 11: Transfer intent and observed-effect recovery

Engine/catalog work is 11a. Runtime grant wiring is 11b. Retry recovery and removal
of superseded managed direct-transfer paths are complete.

References: target §§1.2, 5.3–5.4.
- [x] Commit transfer intents and exact already-satisfied effects under grants.
- [x] Resume under new leaders and stop pending old initiation when fenced.
- [x] Route public metadata transfers through grants and durable effect receipts.
- [x] Recover unfinished direct-transfer grants across canceled callers/retries.
- [x] Remove superseded direct-transfer paths, preserving election limitations.
- [x] Checkpoint verification: engine/meta functional tests and `make test`.

### Task 12: Node authority-store lifecycle and startup evidence

Split into 12a (required path and confinement), 12b (store-wide retained evidence
checks and Node lifetime), and 12c (authoritative floor reconciliation with Task 13).

References: target §§3.5, 5.5, 6 root wiring.
- [x] Require explicit authority path, validate aliases/retirement boundaries.
- [x] Acquire/retain/close store in Node ownership order; update all constructors.
- [x] Reconcile recovery floors through authoritative meta reads before effects.
- [x] Verification: node startup/shutdown/recovery regressions, `make test`.

### Task 13: Host fencing and callback ownership

Split into 13a (historical local receipt lookup), 13b (owned host fencing and
startup-floor reconciliation), and 13c (local effect execution/reconciliation).

References: target §§2 host authority, 5.5, 9 host/action algorithms.
- [x] Persist floor before admission blocking; join old reservations/callbacks.
- [x] Persist exact local effect receipts; reconcile interrupted running records.
- [x] Integrate prepare/stop/retirement and protect successor resources.
- [x] Verification: blocked callbacks, cancellation, missing state functional/race tests;
  `make test`, `make test-function`.

### Task 14: Administrative transport and controller wiring

Split into 14a (metadata grant queries/proposals, public authority reads and fence
transports), 14b (durable demotion-transfer prerequisites, controller and metadata progress
enforcement plus Node/engine wiring), and 14c (remove temporary old paths and migrate their fixtures).

References: target §§3.4, 5.1–5.3, 6 routes/operator.
- [x] Add authenticated fence transport, Node ingress and generic meta fence route.
- [x] Drive grants/fences/receipts from actual controller steps and meta actions.
- [x] Require exact receipts before controller and metadata progress, including
  cached receipts after failover and snapshot restoration.
- [x] Add quorum-confirmed native membership reads through the engine, load
  transport and Node; require them for final convergence of surviving groups.
- [x] Validate immutable successor plan against post-fence state and report
  replan-required when necessary.
- [x] Remove temporary legacy paths and nil-authority fallbacks; migrate fixtures.
- [x] Remove approval-only retirement entry point and callbacks; migrate retirement
  and durable-ledger fixtures to exact grants, host fences and receipts.
- [x] Require authority dependencies in managed constructors; remove startup and
  readmission nil-authority paths and migrate remaining managed fixtures.
- [x] Remove legacy action dispatch/receiver and raw membership/transfer handlers;
  migrate their coverage to grants and remove `ActionResult.Verified`.
- [x] Checkpoint verification: dispatcher/controller tests, `make test`, `make lint`.

### Task 15: Readmission, founding and revocation public APIs

References: target §§3.2, 4.3, 5.2, 5.5.
- [x] Preserve authority store across meta replacement and fence old owners.
- [x] Seed approved load founding authority and require group barrier evidence.
- [x] Expose ReadAuthority/WaitRevoked and persist inherited obligations.
- [x] Separate metadata participation fencing from protocol shutdown; retain
  authenticated authority-proof reads and exact host-identity probes.
- [x] Persist bounded metadata host effects, confirm stop receipts before optional
  removal policy, and reject old-member participation after durable stop intent.
- [x] Separate managed Host participation from granted stop/cleanup/fence work;
  full shutdown/readmission close and join both lifetimes, including overdue callbacks.
- [x] Wire safe courier/grant validation into managed ingress and permit fenced
  metadata owners to forget exact cleanup ledgers using quorum proof plus local CAS.
- [x] Wire retained load administration through fenced Node ingress; defer final
  removal until all required host obligations settle.
- [x] Preserve completed metadata stop proof across later floors; exclude completed
  individual retirements from future deletion grants while preserving exact retries.
- [x] Support explicit control-only startup for durably stopped retained members;
  preserve evidence and refuse same-member readmission responses without reopening Raft.
- [x] Bind readmission authority to an explicit approved former member and unique
  admitted node/member/process identity; gate replacement preparation on resource receipts.
- [x] Persist readmission host fences, retirement intents and exact receipts at
  managed/meta-only boundaries; verify receipt completion before Node authority rebind.
- [x] Carry retained authority identity and committed grants through Join, and
  atomically acknowledge exact retirement receipts before JoinReady admission.
- [x] Wire fresh-member readmission grants, resource receipts and authority rebind.
- [x] Retry pending metadata native phases within the existing effect lifetime;
  verify lost-leader, rotated retained-disk and excluded-process readmission.
- [x] Bind immutable readmission plans to receipt-gated metadata preparation;
  retain native removals while eliminating host calls to rebound former members.
- [x] Persist metadata acquisition before engine publication; distinguish an
  interrupted replacement acquisition from loss of an already acquired member.
- [x] Resume exact authority-store rebind after crashes without reopening old Raft.
- [x] Preserve revocation progress during failed/pending readmission, joining its
  callbacks before a successor resource fence completes.
- [x] Probe exact admitted process ownership through metadata absence/replacement;
  refuse competing instances and preserve the live control route until shutdown.
- [x] Retain approved intermediate metadata identities/routing through supersession
  and history pruning; distinguish native removal from host retirement receipts.
- [x] Preserve exact retirement proof after operator pruning and fence approved
  node deletion before readiness without closing required control in the fixture.
- [x] Revalidate removal-policy ownership across readmission and callback waits,
  preventing an old-member decision from stopping a granted successor.
- [x] Checkpoint verification: founding, authenticated fenced control, metadata host
  receipt/restart regressions, `make test`, `make test-function-race`, `make lint`.
- [x] Verification: remaining admission and retained-administration lifecycle cases.

### Task 16: Fixture integration and deterministic process scenarios

References: target §§6 callers, 7 validation.
- [x] Wire fixture transport, Query, authority paths and authenticated fault controls.
- [x] Add resource fence delivery/response barriers and bounded owned action/proof
  captures; replay through exact Node ingress and expose safe revocation status.
- [x] Add fixture KV Query and consistent HTTP reads; preserve authenticated native
  read scope/evidence and map authority failures to appropriate HTTP statuses.
- [x] Add exact authenticated read fault controls and owner-sampled queue/round
  diagnostics, including cancellation and joined cleanup coverage.
- [x] Add three-node stale-controller/fence/partition/leader/follower/learner scenarios.
- [x] Intercept real metadata-protocol revocation fences with an opt-in fixture
  proxy; preserve authentication, opaque stream forwarding and joined shutdown.
- [x] Verify delayed old actions before/after resource fencing, pending lost
  fence acknowledgement and required replanning with separate meta/load leaders.
- [x] Verify stale receiver metadata, metadata leader replacement during pending
  revocation and rejected replay after a later load leader change.
- [x] Verify independent completed group/pending unreachable-host obligations,
  retained-process return, exact successor cleanup and refusal after data is absent.
- [x] Verify earlier same-operator add-learner cannot undo later promotion, with
  fresh replica identity, exact attempt and altered same-revision grant refusal.
- [x] Verify group revocation remains pending without application quorum while
  metadata/proof traffic stays live; healing completes fencing and rejects replay.
- [x] Preserve admission retry boundaries before endpoint/member acquisition;
  retain independent authority evidence in both durable-pin restart phases.
- [x] Verify three-node follower apply waiting, learner lost-reply/canceled-read
  bounds and isolated former-leader refusal through the real read protocol.
- [x] Verify four-process supersession of an admitted metadata learner before
  Active, including prepared load cleanup, surviving reads and stale replay refusal.
- [x] Align forced retirement, cleanup replay and interrupted voter changes with
  required exact resource receipts; exercise guarded demotion through a real election.
- [x] Verify retained removed metadata owners can obtain narrow authority proofs
  through approved catalog routes without leader hints or joining seeds.
- [x] Complete the first major focused Docker read checkpoint after wiring/image build.
- [x] Keep example meta-only; update API examples and required settings.
- [x] Verification: all ordinary units/vet/function/race; first major focused Docker
  checkpoint only after image and complete wiring are ready.

### Task 17: Install and enable dead/unused-code linting

References: user's explicit post-wiring requirement.
- [x] Verify official revive rules, pin/install revive and add default `make lint`
  rules for unreachable/unused code. If revive cannot detect unused declarations,
  supplement it with an appropriate pinned analyzer rather than claim coverage.
- [x] Cover library, example and test modules and relevant build tags.
- [x] Delete dead/unused implementation after wiring; remove superseded authority
  paths instead of blanket suppressions. Preserve required public APIs.
- [x] Verification: `make lint`, focused regressions and `make test`.

### Task 18: Documentation and final complete validation

References: target §§7–8, user requirement that cluster tests are last.
- [x] Update README/architecture/testing/backlog/comparison to implemented contracts.
- [x] Run Python runner tests, `make test`, `make lint`, `make test-race`,
  `make test-function`, `make test-function-race`, `make bench`, `make proto-check`.
- [x] Run Compose example and performance suites before the final full cluster run.
- [x] Rebuild the final fixture image; run full cluster suite last. If fixes change
  code afterward, repeat relevant earlier suites and make full cluster final again.
- [x] Record actual report paths/results; fix all failing tests, including existing
  failures exposed by the final suites.

### Task 19: Architectural delta verification (final)

References: target ALL sections and complete user objective.
- [x] Audit authority public types, errors, options, grant catalog and capacity.
- [x] Audit local DB identity/fence/effect schemas and readmission lifetime.
- [x] Audit engine entry/snapshot codecs, replay and membership/transfer enforcement.
- [x] Audit safe reads, application serialization and canceled upstream round bounds.
- [x] Audit public load/Cluster APIs and committed lock semantics.
- [x] Audit supersession, effect recovery, cleanup retention and successor isolation.
- [x] Audit tests against each target failure scenario and inspect final reports.
- [x] Audit default lint configuration and absence of unused/dead implementation.
- [x] Fix every NOT IMPLEMENTED GAP; after code changes repeat required validation
  with cluster tests last. No unresolved gap counts as completion.
- [x] Verify ALL components are wired from real constructors, authenticated ingress,
  controller and public APIs; tests-only reachability is insufficient.

### Task 20: Simplify example construction and standardize system storage

References: user follow-up; current library/runtime contracts. The user selected
preserving explicit `Loads.Storage`; no per-group storage API is introduced.
- [x] Add `util.NewInstanceID()` for process identities only; use it in example
  and caller fixture startup without reusing it for request IDs or revisions.
- [x] Replace public authority/meta file options with required `StorageDir` and
  fixed `_authority.bolt`/`_meta.bolt`; make `LoadStoredIdentity` accept the directory.
- [x] Preserve pure construction, alias/retirement-root checks, independent authority
  lifetime and readmission. Reject former filenames without resetting old evidence.
- [x] Update callers, fault controls and current docs; add newcomer comments to
  example bootstrap, resource ownership, readiness and joined shutdown blocks.
- [x] Fix cleanup grant catch-up classification exposed by process validation;
  preserve unknown/altered/foreign refusals and protect the ledger until exact apply.
- [x] Verify units/lint, functional/race and Compose; validate retained metadata
  restart, authority proof recovery and readmission through final process coverage.

## Validation commands

Use root Makefile commands, which cover separate modules. Focus Go tests by package
and tag during implementation. Use race checks for concurrency/ownership changes.
Only single-participant/protocol-double functional fixtures live in library packages.
Use the existing Docker pool for multi-node cases. Final commands are listed in
Task 18, with `make test-cluster` strictly last after all code and cleanup changes.
Reports are retained in `test-reports/<suite>/`; never treat historical logs as new
validation. Run `git diff --check` on every completed task.

## NOT IMPLEMENTED GAPS

None. All tasks and required verification are complete. Target §8 exclusions
remain deferred as designed; no required P0/P1 subsystem is waived.

## NOT PLANNED CHANGES

Task 6 adds the original fence sequence to each local effect and an exact
readmission transition record to the authority DB. The former is necessary to
validate an old owned effect after a newer pending fence replaces the current
record; the latter permits exact idempotent rebind after a committed pin change.
Both schemas are reflected in the target. They strengthen the selected contract.
Task 16 adds `metrics.ReplicaSample.ReadQueue` to observe request and upstream-round
bounds through public APIs in process tests. It shares the existing Raft stamp and
sampler; blocked callbacks freeze it. The target documents this diagnostic addition.
Task 7 bumps the metadata JSON snapshot schema to 2 when adding the catalog;
Task 9 retains the engine wrapper and physical database format integrations.
Baseline snapshot assets stay immutable and their tests now require rejection.
Task 8 adds a Requested flag to revocation records, distinguishing an operator
being revoked from a successor waiting on superseded nonoperator authority.
Task 8b adds a separate committed BeginReadmissionAuthority proposal after
approval and predecessor revocation. A single apply index cannot identify both
a group revocation fence and a different readmission grant for that group.
The new proposal requires the exact retained approved admission and returns
the complete grant set atomically; the target documents this necessary boundary.

## NOT CONSIDERED & TODOs

Retain target §8 exclusions: lease reads, independent per-group scheduling,
asynchronous cleanup separation, database-specific MVCC/ranges, external/time-valid
lock fencing, worker/snapshot query execution, mixed-version migration, forced
recovery, Byzantine certificates. Use explicit existing failure behavior or scope
comments; do not expose misleading no-op APIs or weaken required safety.

## Progress log

- 2026-10-12: inspected clean implementation baseline, target and build/test tooling;
  created task checklist before code changes. Two brainstorm files were the only
  untracked files. No implementation tests run yet.

- Task 1 complete: exact authority types/validation and classified errors. Focused
  topology/fault tests passed; `make test` passed 550 tests. Report:
  `test-reports/unit/20261012T103437.554109Z-ax7i6p5i/report.md`.

- Task 2 complete: complete snapshot/entry codecs reject foreign, partial and
  contradictory records. Focused codec tests passed; `make test` passed 561 tests.
  Report: `test-reports/unit/20261012T103704.969769Z-uweohz3b/report.md`.

- Task 3 complete: validated read options, optional adapters and bounded admission
  ownership. Protocol servicing remains Task 4; public read APIs remain Task 5.
  Focused units and `make test` passed 564 tests. Report:
  `test-reports/unit/20261012T103919.194978Z-teq2q3mz/report.md`.

- Task 4 complete: safe frozen ReadIndex cohorts, bounded remote admission and
  canceled-round retention, serialized queries and lifecycle invalidation. Full
  raftengine functional suite passed with race detection; `make test` passed 564
  tests. Report: `test-reports/unit/20261012T104652.806235Z-garcrhxy/report.md`.

- Task 5 complete: public load and Cluster read APIs, atomic serialized metadata
  queries and explicit read-option wiring. Focused public-load/meta functional
  tests passed with race detection; `make lint` passed; `make test` passed 567
  tests. Report: `test-reports/unit/20261012T105014.504634Z-5596hcj1/report.md`.

- Task 6 complete: explicit-path durable authority database, pending/completed
  fences, exact effect receipts and preserved floors through readmission rebind.
  Functional/race and focused vet passed; `make test` passed 567 tests. Report:
  `test-reports/unit/20261012T105523.653741Z-a4x7tycc/report.md`. Node lifecycle
  integration and callbacks remain Tasks 12–13.

- Task 7 complete: replicated authority catalog, exact current-step grant issuance,
  capacity reservation, restored indexes and retained historical identity evidence.
  Metadata JSON snapshots now use schema 2; immutable old assets are rejected.
  Metadata/service functional race suites and vet passed; `make test` passed 580
  tests. Report: `test-reports/unit/20261012T110315.507928Z-ehd611d5/report.md`.

- Task 8a complete: atomic operator supersession, reserved fence work, inherited
  obligations and exact fence/consumption recording. New evidence is checked
  against recorded resource ordering, including during restore. Metadata/service
  functional race suites and lint passed; `make test` passed 584 tests. Report:
  `test-reports/unit/20261012T111158.333645Z-bini5jc5/report.md`. Task 8b remains
  required before Task 8 is complete.

- Task 8b validation exposed and fixed pruning of a successor prerequisite
  before its operator was inserted. The failing unit report is retained at
  `test-reports/unit/20261012T112146.010270Z-in64kzvf/report.md`.

- Task 8b complete: one-shot direct-transfer grants, separate exact approved
  readmission authority, all approval-path revocation and authoritative catalog
  query handlers. Snapshot validation covers admission bindings, request identity
  and orphan fence predecessors. Focused regressions, metadata/service functional
  race suites and lint passed; `make test` passed 587 tests. Report:
  `test-reports/unit/20261012T112428.936521Z-2kyez3nl/report.md`. Live controller,
  host and transport use of these proposals remains Tasks 12–15.

- Task 9a complete: engine-owned complete snapshots at founding, recovery,
  compaction and live installation; atomic unwrapped metadata materialization.
  Snapshot admission rejects regressing authority and impossible index/term
  boundaries. Focused engine/meta/store/load functional race suites and lint
  passed; `make test` passed 594 tests. Report:
  `test-reports/unit/20261012T113140.166611Z-ibsxp1r5/report.md`.

- Task 9b complete: explicit schema-2 application/configuration log envelopes,
  metadata proposal and physical database formats. Legacy immutable fixtures now
  prove rejection without rewriting stored records; recovery validates even the
  uncommitted tail. Full unit/function suites and lint passed. Reports:
  `test-reports/unit/20261012T113951.081003Z-a6n1vmsu/report.md`,
  `test-reports/function/20261012T113900.108303Z-a4wltxi9/report.md`.

- Task 10a complete: managed-mode grant installation, founding authority and safe
  exact receipt reads. Raw membership/transfer APIs reject managed engines;
  recovery rejects mixed modes and missing managed-load snapshot authority.
  Recovered administrative tails block admission. Functional engine race checks,
  full unit suite and lint passed. Unit report:
  `test-reports/unit/20261012T114815.714437Z-r1mln25b/report.md`.
  Options also carries the host MemberID to bind group fence evidence to the
  actual durable host rather than inferring identity from caller-supplied grants.

- Task 10b complete: exact granted configuration consumption and durable receipts,
  owner-loop native state/quorum revalidation and deterministic rejection of stale
  or duplicate committed configurations. Single-engine recovery tests demonstrate
  that Advance permits the next valid change after either rejection. Native
  receipt snapshot/restart tests pass under race. The initial full functional
  race suite, lint and `make test` passed, but a subsequent focused repetition
  exposed optimistic RecentActive on a new learner; correction is recorded below.
  Unit report:
  `test-reports/unit/20261012T115314.631111Z-8ye8lojo/report.md`.

- Task 10b correction: a new Raft learner initially reports RecentActive before
  acknowledging any entry. Native liveness now additionally requires nonzero
  Match before that peer can support promotion/quorum checks. The safety test
  passed ten race repetitions; full engine functional race, lint and unit suites
  then passed. Report: `test-reports/unit/20261012T115440.499207Z-6js_e3dj/report.md`.

- Task 11 design correction: transfer receipts consume the logged command, but
  the retained intent can resume on a replacement leader. They cannot alone
  settle revocation. The catalog now reserves fencing capacity/retention until a
  newer group floor is acknowledged, while allowing the next grant after receipt
  recording. Ordinary one-shot membership consumption still settles revocation
  without contacting a removed group. This exception is explicit in the target.

- Task 11a complete: durable transfer intents, per-leader-term resumption,
  cancellation of pending native transfer work at newer fences, and committed
  already-satisfied native predicates. Managed engines reject forwarded raw
  transfer requests. Catalog retention and revocation distinguish resumable
  transfers from permanently consumed membership changes. Engine/meta/service
  functional race suites, focused replay regressions, lint and unit suite passed.
  Runtime direct-transfer APIs remain part of Task 14; this task is not yet fully
  wired. Unit report: `test-reports/unit/20261012T120223.532412Z-8iboprxx/report.md`.

- Task 12a complete: required AuthorityDBPath, pure constructor validation,
  startup symlink/hardlink/retirement-root checks, and explicit paths in every
  caller. Unit, lint and full functional suites passed. The first functional run
  (`20261012T120622.670892Z-b0hbhet8`) exposed a one-second startup waiter in the
  approved-creation fixture; it now retries the exact approval while preserving
  the factory-once assertion. Five focused race repetitions passed. Reports:
  `test-reports/unit/20261012T120852.357007Z-885hoa4s/report.md`,
  `test-reports/function/20261012T120759.078188Z-6wyj2oex/report.md`.

- Task 12b complete: Node acquires authority before meta pin creation, recognizes
  retained identity/load evidence, and closes authority after successful joined
  application/meta teardown. Fresh joins use the exact granted identity through
  a private pre-engine hook. Read-only authority identity inspection and required
  Store.HasState close the catalog-loss freshness gap; the storage conformance
  suite checks active and quarantined evidence. This necessary storage-interface
  addition is reflected in the target/current docs. Unit, lint and full functional
  suites pass, plus root/store/authority and application-close functional race
  checks. Reports: `test-reports/unit/20261012T121411.267696Z-20pzvw5f/report.md`,
  `test-reports/function/20261012T121414.270829Z-fxxupdc2/report.md`.
  Authoritative floor reconciliation remains 12c/13; readmission rebind/recovery
  and managed engine activation remain Tasks 14–15.

- Task 13a complete: read-only exact historical receipt lookup survives member
  rebind and newer floors, without granting old-member execution. Regression
  covers running-versus-completed records, rebound restart and altered admission
  proofs. Authority-store functional race suite and `make test` passed. Report:
  `test-reports/unit/20261012T121845.010575Z-4k113b_x/report.md`.

- Task 13b design clarification: a joined interrupted operation may have no
  provable successful outcome. The local store now retains a terminal `fenced`
  record without a receipt; only a newer pending floor and joined owner allow
  this transition. Partial resources remain untouched. This is reflected in the
  target and prevents either false success or permanently pending crash recovery.

- Task 13b complete at the managed boundary: durable admission floors, owned
  coalescing fence workers, delayed factory/Close joining, terminal uncertain
  intent fencing, and authoritative floor import before new recovery owners.
  Ready replicas keep running; fence-only grants cannot create fresh storage.
  Managed/authority functional race tests, full unit/function suites and lint
  passed. Node/controller injection remains Task 14 (and therefore 12c remains
  unchecked until runtime wiring). Reports:
  `test-reports/unit/20261012T123504.594863Z-5xb7ka9p/report.md`,
  `test-reports/function/20261012T123506.004459Z-66u6ugw8/report.md`.

- Task 13c complete at the managed boundary: owned local actions share exact
  requests and persist receipts; fresh startup records intent before factory and
  completion before handle publication. Granted retirement preserves full callback
  proof/identity checks while allowing admitted work to finish during a newer
  pending fence. Managed load engines now seed founding grants and consume
  membership/transfer through engine authority when the authority dependency is
  installed. Host/group fence ingress is implemented for Task 14 transport wiring.
  Five repeated focused race runs, full managed race suite, unit/function suites
  and lint passed; the additional incomplete-callback-proof regression passed in
  the final repeated race run. Reports:
  `test-reports/unit/20261012T124507.864866Z-c3cl445r/report.md`,
  `test-reports/function/20261012T124510.963669Z-gfpimj5y/report.md`.
  Existing nil-authority branches and Verified action replies are temporary until
  Task 14 switches Node/controller construction and migrates old fixtures; they
  must be removed, not retained as production compatibility modes.

- Task 14a complete: committed-grant cache/miss reads, service proposal wrappers,
  public ReadAuthority/WaitRevoked, required load fence transport and Node ingress,
  and the authenticated generic meta fence route. The fixture echoes the full
  retirement grant. Protobuf generation used pinned Make tools; the target now
  correctly notes that Go's named Call dispatcher still needs a protobuf RPC.
  Unit suite (618 tests), lint, meta service/transport functional race suites and
  focused load-fence protocol race test passed. Prior focused managed/meta/fixture
  functional suites also passed. Report:
  `test-reports/unit/20261012T125952.868069Z-zfsjlwy3/report.md`.
  Authority dependency injection, actual controller grant/fence/receipt driving,
  group-founding receipts and meta host-fence ownership remain 14b/15; new protocol
  availability alone does not complete runtime authority enforcement.

- Task 14b1 fills a required engine prerequisite before runtime conversion:
  demotion grants with an exact fallback target now log a resumable transfer
  prerequisite without marking membership complete. Snapshot/order checks retain
  it until exact demotion consumption or a newer fence. New fences can enter
  Raft during an active native transfer, and exact retries can resume attempts
  that expired in the same term. Single-RawNode protocol tests cover admission,
  durable intent restore, leader changes, exact retry, membership consumption and
  actual fence proposal/application for both transfer forms. The target documents
  these necessary additions to the authority snapshot schema and algorithm.
  Validation: five focused race repetitions, full engine functional race suite,
  full lint plus focused post-change vet, and `make test` (624 tests) passed.
  Report: `test-reports/unit/20261012T131210.249435Z-mx0ia_35/report.md`.

- Task 14b2 provides granted load dispatch and the metadata revocation driver.
  Dispatch records exact installed fences, preserves partial host receipts,
  serializes historical targets on each host, and obtains a separate durable
  founding-group receipt. Retained grants resolve exact process/member identities
  independently of the evolving topology catalog. Pending fences reconcile
  through bounded independent attempts even after operator termination, with
  newest-boundary coalescing and exact authenticated reply validation.
  Actual controller invocation, required dependency injection, metadata progress
  enforcement and removal of old dispatch remain 14b3/14c; this checkpoint does
  not claim production runtime conversion is complete.
  Validation: three focused race repetitions, `make test` (625), `make lint`, and
  full `make test-function-race` (701) passed. An ad hoc unbounded package race run
  overlapping compilation hit fixture deadlines; the repository's bounded full
  runner passed without changing timeouts or weakening assertions.
  Reports: `test-reports/unit/20261012T132602.489760Z-q0wcuuau/report.md` and
  `test-reports/function-race/20261012T132649.139515Z-sue5h_sx/report.md`.

- Task 14b3 prerequisite: granted cleanup can now erase an exact local boot
  ledger after its operator is superseded. The FSM holds the approved-assignment
  boundary through the local ledger transaction, validates the full retained
  grant/host/member/replica tuple, and preserves both reassigned identities and
  successor records. The managed owner remains responsible for its installed
  floor and joined callback; catalog validation alone cannot admit cleanup.
  Focused unit and functional regressions passed three race repetitions;
  `make test` (633) and `make lint` passed. Report:
  `test-reports/unit/20261012T133454.455820Z-furvp6ei/report.md`.
  Before switching controller interfaces, resolve the meta removal lifecycle:
  current full Quiesce closes networking before a later host StopReplica grant
  could be acknowledged. Mandatory participation fencing must stay independent
  of the narrowly scoped authority control path and optional final node stop.

- Task 15 lifecycle prerequisite: `FenceParticipation` now independently joins
  metadata producers, admitted effects and Raft while retaining owned networking
  and storage. Full Quiesce then joins protocol calls/removal notification, and
  Stop closes storage. The intermediate phase refuses ordinary work and cannot
  restart Raft. Tests retain a blocked effect and RPC independently, exercise the
  authenticated listener after Raft joins, and prove timeout/retry ownership.
  Node removal still uses full Quiesce until narrow authority RPCs, meta host
  receipts and acknowledgement-before-optional-stop are wired together. The target
  records why pre-admission and fenced engines need safe remote grant verification.
  Five focused participation race repetitions and three existing lifecycle race
  repetitions passed. `make test` (633), lint and full `make test-function-race`
  (707) passed. The first unit run exposed a manually constructed lifecycle
  fixture missing the new completion channel; its initialization was updated.
  Reports: `test-reports/unit/20261012T134212.057714Z-mzkx88fq/report.md` and
  `test-reports/function-race/20261012T134243.317878Z-sp0c51fr/report.md`.

- Task 14b3 controller/progress checkpoint: all actual controller effect methods
  now issue immutable grants and accumulate exact receipts; reconciliation runs
  before operator selection, including terminal/empty operators. Committed
  consumption survives controller failover without repeating host effects.
  Metadata independently rejects missing/partial receipts, direct first-step
  success bypasses, and success while revocation is pending. Snapshot restore
  rejects completed cursors with missing consumption evidence. Native observed
  state remains required for membership/create/transfer verification. Forced
  retirement cannot fabricate host completion.
- Node now injects its authority store, retained-grant routing, fence recording
  and granted ledger cleanup into managed load hosts; live load dispatch uses
  grants. Meta native configuration/transfer use managed engine authority, and
  direct administrative transfers commit a grant before dispatch. Meta host
  prepare/stop dispatch and protocol envelopes are in place, but the Node-owned
  meta host callback and authority-only reads during admission/removal remain
  Task 15. Legacy internal paths remain for Task 14c. Immutable successor-plan
  validation after fencing and direct-transfer retry recovery remain open.
- Migrated FSM/controller fixtures to explicitly consume resource effects before
  progress, and added regressions for malformed/missing/unrecorded receipts,
  failover reuse, no-op revocation and snapshot evidence loss. The public
  single-participant load fixture verifies approved creation, lost-WAL parking,
  retained recovery and retirement with the actual authority injection.
  Validation: `make test` passed 635 checks; `make lint` passed; full bounded
  `make test-function-race` passed 716 checks after updating the unapproved-start
  fixture to expect AuthorityPending (and confirming storage stays untouched).
  Reports: `test-reports/unit/20261012T140601.818434Z-g4cf9t6b/report.md` and
  `test-reports/function-race/20261012T140812.818027Z-h8wp2m0g/report.md`.
  Docker remains deferred until complete admission/removal/readmission wiring.

- Task 15 protocol prerequisite: added authenticated `AuthorityRead` with generated
  protobuf/transport wiring. Its leader-confirmed FSM query atomically returns
  the exact retained grant, recorded receipts and current courier identity.
  Joining/participation-fenced owners use bounded remote attempts without
  redirecting ordinary reads or reopening a fully stopped protocol owner.
  Meta host control/fence requests now validate through this path and stay owned
  by protocol lifetime; native group effects still require active participation.
  Grant-scoped identity probes verify the expected member and courier after
  session deactivation, preserving exact routing without claiming Raft health.
- Validation: `make generate`, `make test` (636), `make lint`, focused proof/control
  and authentication race checks, then full `make test-function-race` (720) passed.
  Reports: `test-reports/unit/20261012T141821.908852Z-w4pc_vb0/report.md` and
  `test-reports/function-race/20261012T141931.853399Z-13whdjwx/report.md`.
  Node-owned meta host callbacks, deferred removal policy, startup/readmission
  recovery of durable host intent and removal of legacy paths remain open.

- Task 15 meta host ownership checkpoint: Node now supplies metadata host fence
  and prepare/stop callbacks. One owned worker bounds/serializes administrative
  work; canceled couriers only stop waiting. Prepare verifies an existing durable
  pin and replay readiness; stop joins metadata participation and load owners
  before persisting its receipt. Removal policy waits for quorum confirmation
  that metadata recorded that exact receipt. Stopped metadata no longer reports
  leadership; shutdown deactivation retains its private native proposal path.
- Durable metadata stop intent (running, completed or fenced uncertain outcome)
  now blocks reopening the same member after restart, even under a newer floor.
  Only exact fresh-member rebind can clear that participation restriction. The
  explicit readmission startup path still needs integration with this guard;
  currently it fails closed rather than restarting stopped participation. Failed
  pre-engine authorization releases the acquired metadata transport/listener.
- Validation: focused Node prepare/stop/restart and all durable intent-state race
  regressions passed; `make test` passed 636; `make lint` passed; full bounded
  `make test-function-race` passed 725. Reports:
  `test-reports/unit/20261012T142905.224685Z-0vanqjr7/report.md` and
  `test-reports/function-race/20261012T142905.224681Z-o5c8a6cs/report.md`.
  Prior protocol checkpoint also passed `make proto-check` after commit.
- Remaining lifecycle audit: plans sort `_meta` before load groups. Normal
  exclusion follows an already completed drain, but forced/mixed removal can
  leave later load stop/cleanup or revocation obligations after Node/Host fencing.
  The managed administrative owner must remain able to fulfill those obligations,
  or plan dependencies must be revised safely. Current Host.Fence/Stop and
  Node.loadHost refuse that work. Do not run/claim the final process milestone
  until this path, fresh-member readmission, and legacy-path removal are complete.

- Task 15 managed participation checkpoint: Host now retains a separate bounded
  administrative lifetime after participation fencing. Granted stop, cleanup,
  delete and host fences share existing group gates while replicas and ordinary
  work join. Creation and native group effects remain fenced. Full shutdown and
  readmission close admission and join authority callbacks before releasing or
  retiring resources; canceled couriers do not release callback ownership.
- Race validation exposed a start waiting behind a pending fence's blocked Close.
  Start admission now checks the durable floor before waiting on that gate and
  revalidates after acquisition. Functional regressions cover all three retained
  effect kinds, durable retry receipts, refusal to restart participation, and
  shutdown/readmission waiting for noncooperative retirement callbacks.
- Before this checkpoint commit, marked the completed managed lifetime item and
  retained the separate Node ingress/metadata authorization obligation unchecked.
  Validation: `make test` passed 636; `make lint` passed; full
  `make test-function-race` passed 731. After final start-input validation, the
  entire managed functional package passed again under race detection and
  `make test` passed again. Reports:
  `test-reports/unit/20261012T144516.043362Z-63ozlspw/report.md` and
  `test-reports/function-race/20261012T144353.310259Z-_hgk54tu/report.md`.
  No Docker suites ran; whole-node retained control and readmission are still open.

- Task 15 retained authorization checkpoint: managed action/fence ingress now
  uses the required safe grant-and-courier validator wired by Node. Immutable
  grant validation belongs to protocol lifetime, with a safe proof fallback for
  absent local catalog entries after participation fencing. Full shutdown still
  refuses both paths.
- AuthorityProof now includes exact cleanup targets absent from approved Target,
  computed under the same FSM query lock as retained grant evidence. A fenced
  metadata owner uses that proof, its captured identity and durable metadata pin,
  and the existing exact ledger CAS to finish admitted cleanup. Active owners
  retain the local FSM lock through ledger deletion. Successor pins/replicas,
  unknown grants and fully stopped owners remain protected.
- Before this checkpoint commit, marked retained metadata authorization complete;
  Node ingress, final host-obligation acknowledgement and readmission remain open.
  Focused FSM/protocol/managed regressions passed; `make test` passed 637;
  `make lint` passed; full `make test-function-race` passed 737. Reports:
  `test-reports/unit/20261012T145324.839581Z-nr_hds0g/report.md` and
  `test-reports/function-race/20261012T145324.839501Z-ape9phce/report.md`.
  No Docker suites ran at this intermediate checkpoint.

- Task 15 retained Node ingress checkpoint: granted load action/fence ingress now
  reaches the exact managed owner after whole-node fencing. Ordinary operations,
  pending readmission and full shutdown retain their existing admission limits.
  Metadata stop and removal join managed participation while leaving host
  administration available for later approved load cleanup.
- Optional removal now requires one safe proof of the exact metadata stop receipt,
  approved Target/Active convergence (covering not-yet-issued cleanup steps), and
  settlement of the member's retained host authority. CompletedMetaStop finds the
  durable receipt after later floors and refuses prior-member receipts after rebind.
  The proof response adds ReleasedHosts without changing persisted schemas.
- Fixed deletion grant expansion resurrecting already retired historical hosts.
  Individual cleanup proofs exclude those identities; a retry after whole-group
  deletion preserves the most recent deletion grant's exact target set and still
  obtains exact receipts. Same-grant retries remain immutable.
- Plan checkboxes updated before commit. Focused FSM, Node ingress/removal-policy
  and durable restart/rebind regressions passed; `make test` passed 638;
  `make lint` passed; full `make test-function-race` passed 738. Reports:
  `test-reports/unit/20261012T150538.313845Z-nothp2tu/report.md` and
  `test-reports/function-race/20261012T150538.312366Z-8yuz57fs/report.md`.
  No Docker suites ran. Readmission startup/rebind and policy ownership during
  replacement remain open before the process milestone.

- Task 15 control-only startup checkpoint: an explicit joining restart whose
  durable authority guard refuses the old member now opens verified retained
  metadata evidence without constructing Raft. Ordinary reads/effects and prepare
  readiness remain fenced; immutable proof lookup and protocol ownership remain
  available. Same-member join replies stay pending, and non-joining restarts
  still fail with AuthorityFenced. The retained pin, Raft entries/hard state and
  snapshot remain unchanged by the control-only admission attempt.
- Removal decisions capture their exact owner and revalidate it after proof and
  callback waits. Accepting StopNode and reserving readmission share Node's mutex;
  stale callback results cannot stop a pending/installed successor. Readmission
  rechecks teardown admission after its authorization wait.
- Checkboxes updated before commit. Focused control-store, public Node restart
  and blocked removal-callback race regressions passed. `make test` passed 638;
  `make lint` passed; full `make test-function-race` passed 739. Reports:
  `test-reports/unit/20261012T151626.258568Z-fkbtu216/report.md` and
  `test-reports/function-race/20261012T151626.258028Z-u_obyh9o/report.md`.
  Fresh-member grant issuance/transport, resource receipts and crash-safe rebind
  remain open; this checkpoint does not claim end-to-end readmission. No Docker
  suites ran.

- Task 15 readmission issuance checkpoint: BeginReadmissionAuthority now requires
  PreviousMemberID backed by approved history or retained grants, including after
  completed exclusion. Exact retries cannot change that former owner. Metadata
  authority names only the former live pin; load scope excludes approved successor
  replicas and evidence belonging to later members.
- Replacement metadata preparation stays pending until its exact admission has
  recorded every readmission receipt. This leaves the host floor available for
  cleanup instead of allowing preparation to block grant issuance. Fence evidence
  alone does not satisfy the prerequisite. Snapshot restore rejects mixed former
  members, an incorrect metadata pin, or a missing metadata grant in the batch.
- Readmission IDs now include operator, granted member, length-prefixed node name
  and process identity. Join payloads use the same helper. A two-learner regression
  proves that equal process IDs on different nodes cannot collide or break restore.
- Marked this completed issuance item before commit; end-to-end grant delivery,
  durable resource receipts/rebind and crash recovery remain unchecked. Recovery
  must account for completed readmission proof when a later process/operator resumes
  an already rebound member, and revoke or resume unfinished grants when admission
  binding changes. These are required follow-ups before process validation.
- Validation: focused readmission/prepare regressions passed three race runs;
  `make test` passed 646; `make lint` passed; full `make test-function-race`
  passed 739. Reports:
  `test-reports/unit/20261012T153726.430281Z-leba99iu/report.md` and
  `test-reports/function-race/20261012T153726.430266Z-cn1k948u/report.md`.
  No Docker suites ran at this intermediate checkpoint.

- Task 15 resource retirement checkpoint: internal readmission payloads now own
  exact grant batches. Managed readmission validates the entire historical scope
  before any floor write, persists pending floors before joining prior owners,
  then completes fences and records durable per-target retirement intent/receipts.
  The application receives its exact authority grant. Metadata quarantine is the
  final effect; meta-only readmission uses the same authority-backed boundary.
- Failed callbacks preserve running intent for serialized retry. Completed receipts
  suppress repeated application callbacks, while probes still reject reappeared
  former load storage or metadata. Old metadata backups and independent authority
  evidence remain intact. Tests cover missing/partial/foreign grant batches without
  writes, blocked prior callbacks, failed retirement retry and receipts through
  rebind/restart. The prior short-timeout join regression now waits for observable
  authority closure and its pending floor before cancellation.
- Node reserves readmission against metadata administration, joins any admitted
  metadata worker and requires complete receipts plus absent live metadata before
  Rebind. Same-process retry after successful rebind verifies its exact historical
  receipts/proof instead of retiring the successor. Missing grant delivery now
  fails closed before retirement. Join grant transport and receipt acknowledgement
  are still open, as is startup recovery after a process crash during rebind.
- Added an explicit remaining revocation obligation: pending/failed readmission
  closes old Host administration, so successor fences need a joined readmission
  control path before end-to-end delivery can be considered finished. Metadata
  administration currently refuses overlapping pending readmission; this ownership
  guard is not a claim that readmission supersession/recovery is fully wired.
- Marked the completed resource-boundary item before commit. Three focused race
  repetitions passed; `make test` passed 646; `make lint` passed; full
  `make test-function-race` passed 749. Reports:
  `test-reports/unit/20261012T155005.006149Z-h5xxvp57/report.md` and
  `test-reports/function-race/20261012T155005.005864Z-qpevvlqp/report.md`.
  No Docker suites ran at this intermediate checkpoint.

- Task 15 join transport checkpoint: Node now supplies the retained authority
  identity independently of its metadata member and exposes completed receipt
  lookup. Join commits/returns readmission grants and exact former load scopes;
  the client validates the entire batch before route/storage mutation and gathers
  durable receipts before opening replacement metadata. JoinReady retries carry
  the same receipts and bind the request to the authenticated process header.
- Added the committed RecordReadmissionReceiptsProposal. It validates the exact
  admitted node/process/member/operator and atomically records the batch, with no
  valid-prefix writes on contradiction. Historical receipts can settle revocation
  after supersession; a separate ReadyAdmission check still refuses the superseded
  owner or incomplete readmission consumption. Codec and snapshot coverage include
  the new command and receipt-bound readiness.
- Same-process retry after rebind now recognizes the exact still-owned transition
  only with no old engine/open store and a fresh live metadata path. This covers
  a temporary proof lookup failure both with missing metadata and with control-only
  retained metadata. Callback errors preserve their retry classification. It does
  not infer cross-process recovery authority from mismatched pins.
- Marked the completed protocol item before commit. End-to-end operator convergence,
  restart recovery and revocation during interrupted readmission remain open. The
  control-path work must also cover predecessor fences before readmission grant
  issuance, including address/identity routing when only the authority pin remains.
  Post-readmission plans must not strand old-member stop/cleanup steps after rebind.
- Validation: focused FSM/response and authenticated join/receipt regressions passed
  three race repetitions. `make test` passed 659; `make lint` passed; full
  `make test-function-race` passed 752 after fixing the retained fixture to reuse
  its persisted cluster limits. The initial full race run's only failure was that
  fixture mismatch (parent plus subtest). Reports:
  `test-reports/unit/20261012T161110.800478Z-xpl_9t5r/report.md` and
  `test-reports/function-race/20261012T161204.962642Z-5o_s_s85/report.md`.
  No Docker suites ran; complete readmission/revocation recovery remains required
  before the first process milestone.

- Task 15 acquisition-boundary checkpoint: authority now persists an exact current
  member metadata-acquisition marker. Rebinding a newer member clears it in the
  identity transaction; exact retries preserve it. Node records acquisition after
  the durable metadata pin and before engine publication or participation. A
  recorded member with missing metadata cannot be recreated under the same ID.
- Added crash-boundary regressions before pin creation, after pin creation, after
  marker persistence and after subsequent metadata loss. Retained process restarts
  preserve acquisition evidence; corrupt foreign markers fail opening; an owner
  hook failure retains the durable pin without publishing or starting an engine.
- Marked this narrow acquisition item before commit. Cross-process admission-proof
  reuse and interrupted-readmission revocation remain unchecked and required.
  Focused race checks, `make test` (659), `make lint`, and full
  `make test-function-race` (760) passed. Reports:
  `test-reports/unit/20261012T162503.951823Z-_99dfzsg/report.md` and
  `test-reports/function-race/20261012T162506.879615Z-gbhg7i2p/report.md`.
  No Docker suites ran at this intermediate checkpoint.

- Task 15 crash-recovery checkpoint: a pending admission rebound to another
  process under the exact same member/operator retains its original retirement
  instance. The FSM assigns this field and flattens repeated restarts to the
  original admission; callers cannot select it. Grant lookup, receipt recording
  and preparation use the retained proof, while readiness still requires the
  current process. Snapshot validation checks the exact original superseded
  record and the catalog resolves the unique original rather than its aliases.
- Node's receipt callback now verifies the persisted previous/granted/admission
  rebind proof before returning historical receipts. This read-only check survives
  process changes and refuses substituted former members or admission IDs.
- Added FSM tests before/after grant issuance, repeated process replacement,
  snapshot restore, forged origin rejection and old-process readiness rejection.
  Authenticated protocol tests restart a single participant after retirement,
  after rebind, after pin creation and after acquisition but before readiness.
  They retain one deterministic metadata backup, preserve the exact receipt,
  avoid retiring an already rebound successor and acknowledge with the new process.
- Marked exact crash recovery complete before commit. Revocation during interrupted
  readmission and complete operator convergence remain required. Focused race
  regressions, `make test` (667), `make lint` and `make test-function-race` (765)
  passed. Reports:
  `test-reports/unit/20261012T163601.873592Z-nne_5v7y/report.md` and
  `test-reports/function-race/20261012T163601.873851Z-lqfi08_5/report.md`.
  No Docker suites ran at this intermediate checkpoint.

- Task 15 pending-revocation checkpoint: Node retains an explicit readmission
  callback owner and a bounded host-fence worker independent of courier waits.
  A newer host revocation persists its pending floor before canceling retirement,
  joins the callback and former Host, fences uncertain intents without completion
  receipts, then acknowledges the boundary. New retirement/metadata work cannot
  overlap it. Later approved readmission can replace the canceled reservation.
- Missing-metadata control now uses the independent authority member for exact
  grant proof reads, identity probes and host revocation fences. Ordinary metadata
  identity and Raft headers remain unchanged. Once an admission is committed, Join
  publishes its control route even while predecessor revocation prevents grant
  issuance; identity/name refusals retain the accepted route.
- Added actual Node/authenticated-ingress regressions for metadata and load fences
  while an application retirement callback ignores cancellation. They prove the
  floor remains pending and worker owned after courier timeout, completion waits
  for callback return, no retirement receipt is invented, and a newer admission
  can subsequently reconcile resources and rebind. Real leader proof tests cover
  a zero-metadata-member client with a retained authority pin and foreign claims.
  A single voter with an absent learner tests routing before predecessor fencing.
- Inspection found an ingress/startup race: a fence could reach the Host before
  its Start call. Node now starts Host ownership before exposing metadata control
  networking. Full checks were repeated after that change.
- Marked pending-readmission revocation complete before commit. Remaining full
  operator convergence, post-fence successor-plan validation, process identity
  overlap scenarios and final audit remain required; this is not complete P0/P1
  delivery. Focused race repetitions and final `make test` (667), `make lint`,
  `make test-function-race` (769) passed. Reports:
  `test-reports/unit/20261012T165447.196783Z-yfeho2bu/report.md` and
  `test-reports/function-race/20261012T165447.196937Z-4zqiegfy/report.md`.
  The initial routing fixture attempted to replace its sole voter and correctly
  failed planning; it now replaces an absent nonvoting learner. No Docker suites
  ran at this intermediate checkpoint.

- Task 14 native-read prerequisite complete: `ReadNative` uses the existing bounded
  safe-read queue and captures native membership on the Raft owner after local
  apply. It works on learners and write-only machines. Load transports preserve
  an explicit native query/result separate from diagnostic status; missing,
  foreign and late replies cannot establish confirmed membership. Managed reads
  retain exact replica ownership across the quorum wait and try another catalog
  replica when an earlier owner is unavailable. Node and metadata controller
  adapters are wired, and final convergence now requires confirmed membership
  for affected surviving groups before advancing Active.
  Remaining-plan reachability and retained cleanup identity validation are still
  unchecked; this prerequisite does not complete the successor-plan task.
  Focused native/controller race regressions passed three repetitions. Validation:
  `make test` passed (667), `make lint` passed, and `make test-function-race` passed
  (786). Reports: `test-reports/unit/20261012T171231.961078Z-i9eph3wm/report.md`
  and `test-reports/function-race/20261012T171230.488861Z-pborn85r/report.md`.
  Earlier verification caught a missing Node adapter (fixed) and two timing
  assumptions in functional tests. Founding dispatch now retries the same exact
  grant after an attempt timeout within one overall deadline. The readmission
  fence test now cancels its courier after observing the pending durable boundary,
  instead of assuming authorization dispatch finishes within 100 ms; that final
  test adjustment passed five focused race repetitions. No Docker suite ran.

- Task 14 successor-plan validation complete: the actual controller requires a
  validator after inherited fences and before every step, including empty-plan
  completion. One safe metadata query captures the exact attempt/plan, Target and
  retained identities and checks pending revocations. Quorum-confirmed native
  reads include historical candidates, then simulate only the immutable suffix.
  Missing additions, unplanned members/roles, exhausted voter capacity, unsafe
  destructive ordering, lost transfer prerequisites and omitted host cleanup
  fail with `replan_required`; unavailable confirmation remains waiting.
  Planning now retains interrupted host preparations in surviving groups, even
  when absent from native membership, and emits exact remove/stop/cleanup steps.
  This also preserves cleanup in automatic repairs. New planning omits proven
  cleanup while `OperatorBase` retains the identities required by an immutable
  retry. Failed-version approval reuses its original plan, the FSM rejects changed
  retry actions, and snapshot restore rejects attempts with divergent plans.
  Native queries can inspect an exact retained ready replica without expanding
  ordinary Raft peer admission. Internal metadata control reads retain bounded
  default byte limits while sharing the configured read queue and timeout; public
  read limits still apply to public topology, lock and authority queries.
  Verification covered pure post-fence membership cases, orphan host planning,
  immutable retry and corrupt snapshots, the actual single-participant controller
  refusing an unreachable empty plan, new-version replanning, retained native
  routing, and control reads under small public limits. Focused race regressions
  passed three repetitions. Final `make test` passed (684), `make lint` passed,
  and `make test-function-race` passed (792); `git diff --check` passed.
  Reports: `test-reports/unit/20261012T173414.278657Z-h_q3hwal/report.md` and
  `test-reports/function-race/20261012T173417.213759Z-hnlqof67/report.md`.
  Earlier runs exposed inaccurate absent-replica protocol doubles and the repair
  fixture's obsolete omission of former-host cleanup; both were corrected. No
  Docker suite ran. Task 15 readmission operator convergence and Task 14 legacy
  removal remain open, as do process integration, lint tooling and final audits.

- Task 15 readmission-plan boundary complete: immutable plans retain full metadata
  preparation/admission even when a prior attempt already added the new member.
  Preparation requires actual receipts for the entire readmission batch. Plans
  keep native removal but omit subsequent stop/cleanup calls to the rebound former
  metadata identity. Complete returning-host deletion is covered by preparation;
  partial deletion still targets the other hosts. Metadata runs first even when
  a load-group name sorts before `_meta`; load groups then run in name order.
- Readmission receipts now supply exact permanent cleanup evidence. Later delete
  grants exclude those retired hosts, while planning retains their native routing
  identities until the replacement becomes Active. A successor operator can reuse
  completed retirement proof for the same granted member after grant closure;
  incomplete batches, fences and another member cannot satisfy the gate.
- Regressions cover old/already-learner/already-voter membership, full and partial
  deletion, numeric group names, incomplete boundary refusal, original and
  successor FSM convergence across snapshot restore, retained native identities,
  and exact residual deletion grants. These are planner/FSM/protocol-boundary
  checks; remaining actual lifecycle and Docker process verification stay open.
- Marked the completed subtask before commit. Final `make test` passed (696),
  `make lint` passed, `make test-function-race` passed (792), focused race tests
  passed three repetitions, and `git diff --check` passed. Reports:
  `test-reports/unit/20261012T175548.057975Z-_jmwz_dc/report.md` and
  `test-reports/function-race/20261012T175548.057560Z-h5o7b27g/report.md`.
  An earlier test fixture used inconsistent fence/receipt sequence numbers; it
  was corrected and checks repeated. No Docker suite ran at this checkpoint.

- Task 15 process-ownership checkpoint: Join now probes the exact admitted node
  and instance before rebinding admission or replacing the cached control route.
  The private process probe remains available while metadata is absent, changing
  members or participation-fenced. It proves only process liveness and cannot
  substitute for member, readiness or effect evidence. Full shutdown ends it.
  Ordinary health/session probes and grant-scoped identity checks keep their
  separate participation and authority requirements.
- Added one-participant/protocol-double regressions for old/zero/new metadata
  identity, exact-instance refusal, malformed/mixed scopes, foreign clusters,
  participation fencing and full stop. The actual Join routing test now refuses
  a competing process while the old control-only owner is alive and rebinds the
  exact pending member after it stops. No multi-node in-process fixtures added.
- Marked the completed subtask before commit. `make test` passed (696),
  `make test-function-race` passed (793), and `make lint` passed after correcting
  a protobuf-header value copy in the test fixture. Focused race regressions
  passed three repetitions after that correction; `git diff --check` passed.
  Reports: `test-reports/unit/20261012T180052.324531Z-fgn2k6ft/report.md` and
  `test-reports/function-race/20261012T180052.324615Z-z589034t/report.md`.
  Remaining lifecycle/process integration and final audit tasks stay open.
  No Docker suite ran at this intermediate checkpoint.

- Task 11 retry recovery complete: a new internal request resumes the current
  open unconsumed direct-transfer grant only for the exact Active version/group/
  node/replica action. The original grant and request ID remain unchanged.
  Conflicting targets stay pending, original-request retries remain idempotent,
  and a new call after receipt consumption receives a fresh grant/floor.
- Added snapshot-restoration coverage for lost callers, conflicting retries and
  post-consumption requests. An actual metadata participant with a load protocol
  double exercises caller cancellation after dispatch, retry with the original
  grant, durable receipt acknowledgement and a subsequent fresh transfer.
- Marked retry recovery done before commit. `make test` passed (697), `make lint`
  passed, `make test-function-race` passed (794), the focused canceled-caller race
  regression passed three repetitions, and `git diff --check` passed. Reports:
  `test-reports/unit/20261012T180721.899602Z-zq1b2t0q/report.md` and
  `test-reports/function-race/20261012T180721.899736Z-gtbfwngw/report.md`.
  Legacy-path removal remains open under Tasks 11/14; no Docker suite ran.
- Startup audit: explicit Join already postpones the retained metadata engine's
  Start until the leader's response, and fresh readmission stops/retires that
  engine before replacement. No speculative lifecycle reset was added from a
  sticky-flag observation alone; actual readmission process integration remains
  a required later validation gate.

- Tasks 11/14 action-path cleanup: removed Host.Dispatch/sendAction, local
  approval-only action validation, raw native membership/transfer execution and
  the now-unused membership coordinator. Managed ApplyAction always takes the
  granted path. Removed the obsolete public ActionResult.Verified field;
  receipts and the full exact response tuple are the acknowledgement contract.
  Standalone engine administration remains supported and unchanged.
- Migrated creation/identity and learner-target/catch-up coverage to real host
  floors, managed engine grants and receipts. Consolidated redundant legacy
  fanout/reply tests into granted tests, retaining independent-host progress,
  missing evidence, wrong process/action/local target and incomplete-fence cases.
  Added explicit identity refusal/closed-grant checks and bare-approval rejection.
  The factory/fence regression additionally proves no storage opens after fencing
  while ownership and Close remain joined. Intermediate checks exposed ordinary
  initial-election authority/status waits; the migrated tests now wait on the
  actual pending/unavailable boundaries within their existing deadlines.
- Marked the action-path cleanup and Task 11 legacy-transfer removal complete
  before commit. `make test` passed (697), `make lint` passed,
  `make test-function-race` passed (793), and `git diff --check` passed. Reports:
  `test-reports/unit/20261012T181352.461383Z-6e4yjphx/report.md` and
  `test-reports/function-race/20261012T181352.460635Z-fhqcy34d/report.md`.
  Task 14 remains open for constructor/startup/retirement/readmission nil-authority
  fallbacks and their older fixtures. No Docker suite or baseline regeneration ran.

- Task 14 retirement cleanup: removed the approval-only Host.Retire entry point,
  action-target and authorization fallback callbacks, and ungranted ledger forget.
  All managed retirement now enters ApplyAction with its exact grant and installed
  host fence. Constructor dependencies require granted ledger forget.
- Migrated retirement and durable-ledger regressions to durable authority stores.
  They preserve post-quarantine retry verification, mismatched application proof
  refusal, exact successor protection and shutdown ownership of late callbacks.
  A changed approval alone is no longer treated as a completed resource fence.
- Marked this subtask complete before commit. `make test` passed (697), `make lint`
  passed, `make test-function-race` passed (793), focused retirement/ledger/dispatch
  race checks passed, and `git diff --check` passed. Reports:
  `test-reports/unit/20261012T182037.052053Z-rxnq7tzx/report.md` and
  `test-reports/function-race/20261012T182039.881879Z-merkahrm/report.md`.
  Constructor/startup/readmission nil-authority migration remains open. No Docker
  suite or baseline regeneration ran at this checkpoint.

- Task 14 managed fallback removal: constructors require the authority accessor,
  validators and current-floor reader without calling them. Every managed engine
  enables authority; startup, application readmission and metadata-only readmission
  cannot bypass durable fences/intents/receipts. Removed nil receiver success paths
  from readmission authority and redundant optional-validator branches.
- All managed functional fixtures now own durable authority stores and exact
  immutable startup grants. Retained startup, failed Close, holds, transport,
  readmission and retirement regressions run through the production authority path.
  Moved metadata readmission storage coverage into the functional suite; added
  explicit rejection without an authority store and fresh-join receipt verification.
  Completed readmission receipts suppress repeated callbacks; uncertain moves
  still retry exactly their unfinished identity and preserve replacement evidence.
- The first full race run exposed a per-effect timeout in the canceled-startup
  regression. Its retry now uses the same grant within the existing overall
  deadline and still requires one factory and the exact receipt. Focused race
  regressions passed three repetitions, then the complete rerun passed.
- Marked this subtask complete before commit. Final `make test` passed (696),
  `make lint` passed, `make test-function-race` passed (795), and
  `git diff --check` passed. Reports:
  `test-reports/unit/20261012T182629.346526Z-po529l4e/report.md` and
  `test-reports/function-race/20261012T182629.346591Z-f0nb9nev/report.md`.
  Task 14 retains its umbrella cleanup item for obsolete metadata approval-only
  helpers and their tests. Process transport/read integration remains Task 16.
  No Docker suite or immutable baseline regeneration ran.

- Task 14 cleanup complete: removed the remaining metadata service approval-only
  target reader and ledger-forget handler, and removed the obsolete metadata FSM
  target API. Ordinary load catalogs remain observations; executable targets come
  from the committed immutable grant. Startup scope documentation now states the
  additional grant and installed-fence requirement.
- Migrated ledger cleanup coverage to issued grants, preserving wrong attempt,
  wrong local target, exact ledger ownership and idempotent retry refusals.
  The history/snapshot regression now issues an actual cleanup grant after restore
  and checks its complete retained identity set. Catalog units exercise the shared
  deletion catalog used by issuance and future approvals fail through BeginEffect.
- Marked Task 14 complete before commit. `make test` passed (696), `make lint`
  passed, `make test-function-race` passed (795), focused ledger race checks and
  metadata engine units passed, and `git diff --check` passed. Reports:
  `test-reports/unit/20261012T183049.585607Z-uhbd8244/report.md` and
  `test-reports/function-race/20261012T183049.585273Z-r6mqupkr/report.md`.
  Task 15 lifecycle integration and Tasks 16–19 remain required. No Docker suite
  or immutable baseline regeneration ran at this checkpoint.

- Task 16 read integration checkpoint: the fixture KV machine implements optional
  Query with bounded strict application input and owned JSON output. The new
  `GET /api/load/{id}/read/{key}` uses Group.Read on the exact local replica and
  reports linearizable consistency; the existing keys route reports local
  observations. Added the process Cluster.Read helper for subsequent scenarios.
  Application query payloads contain no grant or internal management fields.
- Added a single-participant integration through the real caller factory, granted
  controller/native-read transport, Group.Read and HTTP adapter. It checks committed
  values, missing-key barriers, healthy replicas after Query errors, explicit local
  observations and refusal after exact local stop. It waits for Active publication
  after operator completion instead of treating receipts as immediate serving proof.
- Added authenticated native-read protocol coverage for wrong credentials, sender
  and replica routes, preserved Native flags, exact applied indexes above 2^53 and
  native membership evidence. Diagnostic replies stay separate. HTTP pending
  authority returns 503; fenced authority and replan-required return 409.
- Marked the read integration subtask complete before commit. `make test` passed
  (697), `make lint` passed, `make test-function-race` passed (797), focused machine
  units and read/protocol race checks passed, and `git diff --check` passed. Reports:
  `test-reports/unit/20261012T183658.525264Z-zo1qpk4r/report.md` and
  `test-reports/function-race/20261012T183659.965920Z-5d43wmip/report.md`.
  Task 16 remains open for deterministic read/fence fault controls and three-node
  process scenarios, alongside remaining Task 15 lifecycle integration. Revive
  installation/dead-code cleanup and final all-suite/Docker validation remain later
  required gates. No Docker suite or immutable baseline regeneration ran.

- Task 16 read fault-control checkpoint: added exact current node/member/process
  and group/replica controls for dropping append/snapshot traffic, all incoming
  load Raft or only ReadIndex responses, and holding the actual Apply/Query owner.
  Delivery still validates the ordinary authenticated protocol and retained
  messages still enter Node's exact ingress. Dropped snapshots report failure;
  they never manufacture successful snapshot delivery. Controls on an older ID
  cannot affect a successor, and application Close removes them after engine join.
- Added callback/progress and accepted-read-confirmation counters, plus one copied
  diagnostic context per owned replica to distinguish retry from a new round.
  The public metrics sample exposes bounded request slots/queues, quorum/apply
  waits and local/remote rounds with the existing Raft stamp's generation and age.
  No additional sampler or authority/read capability is introduced.
- Extended the real single-participant fixture regression with credential/process
  refusal, controlled Query cancellation/recovery and stop joining an armed Query.
  Protocol-double coverage checks independent append/confirmation handling,
  snapshot-drop failure, successor isolation, canceled Apply and owned context
  copying. The engine lost-reply regression now checks published queue bounds.
- The added sample wait exposed old retransmissions still queued in the protocol
  capture. The regression now waits for a distinct context within its fixed
  deadline and refuses caller completion before that new confirmation. Focused
  race repetitions and the complete rerun passed after this correction.
- Marked this subtask complete before commit. Final `make test` passed (697),
  `make lint` passed, `make test-function-race` passed (798), focused engine and
  fixture race checks passed, the clustertest binary compiled without running
  Docker, and `git diff --check` passed. Reports:
  `test-reports/unit/20261012T185400.349189Z-mp8wpf1e/report.md` and
  `test-reports/function-race/20261012T185400.349198Z-sgwfkazg/report.md`.
  Deterministic authority/fence controls, three-node read/authority scenarios and
  remaining lifecycle integration are still required before the first major
  focused Docker checkpoint. Revive and final validation remain open. No Docker
  suite or immutable baseline regeneration ran.

- Task 16 authority-control checkpoint: the real load Fence route now supports
  exact receiver/member/process and group-replica barriers before installation
  or after successful installation but before response delivery. Purpose and
  minimum revision selectors isolate revocation from ordinary effect setup.
  Cancellation ends one wait; authenticated release permits later retries.
  Old-member controls cannot block a successor, and group controls bind an exact
  local replica instead of following a replacement.
- Added bounded owned captures of complete action requests, successful receiver
  receipts and installed fence evidence. Authenticated replay re-enters the
  public Node action receiver with the original courier/grant/target and cannot
  turn a refusal into completion. Safe revocation observation calls the actual
  Cluster.ReadAuthority API. Ingress, constructors, Query, authority paths and
  deterministic read/authority controls are now wired in the process fixture.
- Protocol-double race coverage distinguishes pre-installation from lost-response
  boundaries, validates credentials/process scope, cancellation/retry, exact
  revisions above 2^53, successor isolation, bounded capture retention, cloned
  grant/proof fields and propagation of fenced refusals. These are protocol
  checks; multi-node resource/consensus outcomes still require the process cases.
- Marked the completed wiring/control items before commit. `make test` passed
  (697), `make lint` passed, `make test-function-race` passed (802), focused
  authority-control race regressions passed, the clustertest binary compiled
  without executing Docker, and `git diff --check` passed. Reports:
  `test-reports/unit/20261012T190839.314551Z-9n4cl0_s/report.md` and
  `test-reports/function-race/20261012T190840.741351Z-89kdsut5/report.md`.
  Three-node scenarios, remaining lifecycle integration and the first major
  focused Docker checkpoint remain open. Revive/dead-code cleanup and final
  all-suite validation with the full cluster suite last remain required.

- Task 16 first process-read checkpoint: added three isolated three-node scenarios
  through the public fixture read route and authenticated exact faults. A follower
  receives a confirmed ReadIndex ahead of its applied state and cannot finish
  until append catch-up. A two-voter/one-learner group survives lost read replies
  and a 512-request cancellation flood with bounded wrapper/upstream diagnostics;
  a fresh post-recovery query uses a new context and preserves nonvoting learner
  membership. An isolated naturally elected leader refuses a fresh read while
  the majority elects another leader, commits and reads a new value, then the
  original replica catches up and serves a fresh read after healing.
- Corrected test setup exposed by the initial runs: the intentional learner's
  voter target requires a two-voter placement minimum. Leader hints precede an
  applied current-term barrier, so setup now confirms one through Group.Read.
  The partition case selects the naturally elected leader: an explicitly granted
  setup transfer leaves durable intent that a replacement leader can legitimately
  resume until a newer floor. The read case must not conflate that administrative
  intent with its intended partition boundary. No production contract was weakened.
- Built a separate image `gquorum-authority-contract:20261013-p1` without replacing
  a shared running tag. The focused Docker harness used the race detector and
  three parallel isolated pool leases; all three scenarios passed. Pool/container
  cleanup passed and subsequent inspection found no leftover application containers
  or test networks. The full final cluster suite remains a later, distinct gate.
- Marked these completed read/process items before commit. `make test` passed
  (697), `make lint` passed, clustertest vet passed, and `git diff --check` passed.
  Final focused process report:
  `test-reports/cluster/20261012T192736.974391Z-u1me4sko/report.md`.
  Unit report: `test-reports/unit/20261012T191914.750057Z-xyahhbsl/report.md`.
  Library/fixture functional-race code was unchanged by these process-only cases;
  its previous 802-pass report is historical, not a new run at this checkpoint.
  Authority scenarios, remaining readmission/lifecycle integration, revive/dead-code
  cleanup and final complete validation remain required.

- Task 15 metadata native-phase/readmission checkpoint: grant issuance and fence
  recording share the metadata Raft loop with native administrative effects.
  Proposal results can become visible before persistence/Advance finishes; an
  immediate native request then returns AuthorityPending. Retrying the whole
  effect reinserts metadata proposals and can repeatedly recreate this boundary.
  Metadata effects now wait on only that pending native phase within their
  existing caller/effect lifetime. Each engine attempt rechecks the exact grant
  and native guards; fencing, lifecycle and other refusals terminate the wait.
  No new worker or replacement-engine redirection was introduced.
- Added a single-participant repeated metadata-transfer regression that requires
  exact durable intent receipts and target leadership. Process setup retries only
  classified transient transfer refusals and still observes the exact target ID.
  The retained excluded-process case now checks its typed parked refusal through
  both surviving participants, while explicitly requiring ordinary metadata reads
  on the control-only member to remain refused. Its retained pin, fresh process,
  nonserving state, subsequent same-process readmission and recovered values remain
  asserted. The production participation/read contract was not broadened.
- Marked completed wiring and native-phase/readmission items before commit.
  `make test` passed (697); `make lint` and clustertest vet passed; focused transfer
  race regressions passed three repetitions; `make test-function-race` passed
  (803). Built the separate image
  `gquorum-authority-contract:20261013-meta-wait`; the focused race-enabled Docker
  milestone passed all three readmission cases with three isolated parallel pool
  leases. Subsequent inspection found no leftover application containers.
  Reports: `test-reports/unit/20261012T202148.269000Z-y6tt0_wj/report.md`,
  `test-reports/function-race/20261012T202115.778865Z-6rt0je_5/report.md`, and
  `test-reports/cluster/20261012T202157.636216Z-xqcha2sz/report.md`.
  Remaining retained-administration cases, P0 process fault scenarios,
  post-wiring lint/dead-code cleanup and final validation/audit remain required.

- Task 16 resource-boundary process checkpoint: two independent three-node
  leases hold an exact old demotion while an empty successor plan is approved.
  Metadata controller and load leader differ. With the group fence withheld,
  replay through Node may still consume the old grant; after fencing, replay is
  refused and cannot alter native membership, application values or storage
  identity. The stale empty plan reports replan_required and leaves Active at
  its prior version; a fresh approved plan restores the voter, and replay remains
  refused. In the second case, the actual fence is durable while its reply is
  withheld: metadata revocation stays pending, old actions already refuse, and
  releasing the reply resumes the same successor and completes revocation.
- The first run exposed incomplete fixture interception: reconciliation uses the
  metadata InstallFence RPC, not the application transport route. Added an opt-in
  caller-owned metadata proxy using the existing credential and opaque gRPC
  frames. It forwards unary/client-streaming traffic to the real service without
  internal package imports or re-encoding protocol messages. Only exact fence
  requests/replies enter existing delivery/evidence controls; real Node checks
  still own authority. Streaming stays frame bounded, and Stop joins handlers,
  servers, listener and client ownership. Ordinary process/performance cases leave
  the proxy disabled. Protocol-double regressions cover authentication, before/
  after installation, cancellation/retry, evidence above 2^53, opaque multi-frame
  forwarding and shutdown while a reply is held.
- Marked completed proxy and boundary items before commit. `make test` passed
  (697), `make lint` and clustertest vet passed, focused proxy race regressions
  passed three repetitions, and `make test-function-race` passed (806).
  Built the separate image `gquorum-authority-contract:20261013-meta-proxy`.
  Final focused race-enabled Docker run passed both cases and their parent (3
  report checks); pool cleanup passed, with no leftover application containers.
  Reports: `test-reports/unit/20261012T203228.894517Z-5umvqsst/report.md`,
  `test-reports/function-race/20261012T203228.894092Z-gx0mz8fg/report.md`, and
  `test-reports/cluster/20261012T203445.140995Z-8cu8ykl_/report.md`.
  Stale metadata/leader-change/host-unreachable process cases and remaining
  integration, lint/dead-code and final validation/audit tasks remain open.

- Task 16 stale-metadata/leadership checkpoint: the opt-in proxy now exposes an
  authenticated, exact process/member metadata-Raft fault. It withholds both
  message and snapshot delivery, explicitly refusing snapshots rather than
  manufacturing successful persistence. Control and proof RPCs keep forwarding;
  fault ownership cannot affect a successor identity. Fence envelopes now also
  check the numeric courier member before entering diagnostic holds.
- Expanded both resource-boundary process cases to retain the load leader's
  older approved Target while the majority approves its successor. Actual
  dropped-frame counters and the unchanged local version prove stale receiver
  metadata. A permitted old demotion and subsequent durable rejection still
  follow the resource boundary. The lost-reply case heals that receiver and
  isolates only the metadata leader's Raft ingress, preserving its exact courier
  process while a healthy majority elects a replacement. Revocation remains
  pending across the change until the real fence reply is released. Both cases
  later move load leadership and replay the unchanged old grant/action at the
  exact new leader; membership, application values and storage identities remain
  intact. No administrative transfer bypass was introduced during pending work.
- Marked the completed stale-metadata/leader item before commit. `make test`
  passed (697), `make lint` and clustertest vet passed, focused proxy race tests
  passed three repetitions, and `make test-function-race` passed (806). Protocol
  doubles additionally verify authenticated fault controls, snapshot refusal,
  exact member filtering and continued fence delivery under the Raft fault.
  Separate image: `gquorum-authority-contract:20261013-meta-stale`.
  Focused race-enabled Docker milestone passed both cases and parent (3 checks).
  Reports: `test-reports/unit/20261012T204055.356970Z-_fqfzheq/report.md`,
  `test-reports/function-race/20261012T204055.356973Z-7sl6n9rp/report.md`, and
  `test-reports/cluster/20261012T204153.779188Z-hoo07mgs/report.md`.
  Host-unreachable, same-operator consumed-effect cases and remaining integration,
  lint/dead-code cleanup and final complete validation/audit remain required.

- Task 16 unreachable-host checkpoint: added an isolated three-node case with
  metadata controller on node2 and load leader on node3. It holds an exact
  node1 stop delivery, kills that process with retained storage, and approves a
  successor deletion. Both surviving metadata participants observe completed
  inherited group evidence alongside the pending node1 host obligation. Active
  remains unchanged; no successor effect is delivered, while the reachable
  application majority still serves a safe read.
- Returning the same retained member with a fresh process identity completes
  exact host fencing and successor stop/cleanup. The old grant/action is then
  delivered to that currently authenticated host identity with its original
  historical storage scope: authority_fenced is required even after data is
  absent. All three actual storage paths remain absent and no replica reopens.
  Group IDs remain permanently retired; the test supersedes deletion intent
  rather than attempting to reuse a deleted ID.
- Marked this completed process item before commit. `make test` passed (697),
  clustertest vet and `git diff --check` passed. The focused race-enabled Docker
  case passed with verified lease cleanup; it reused the already built separate
  `gquorum-authority-contract:20261013-meta-stale` image because only process
  harness code changed. Reports:
  `test-reports/unit/20261012T204610.661220Z-zf5ogm40/report.md` and
  `test-reports/cluster/20261012T204632.159593Z-p09y2kml/report.md`.
  The unchanged library/fixture's previous 806-pass functional-race report is
  historical, not a new run here. Same-operator consumed-effect coverage and
  remaining integration/lint/dead-code/final validation/audit tasks remain open.

- Task 16 consumed-effect checkpoint: added an isolated three-node removal/
  reseating case. The returning load replica uses a fresh monotonic ID; an
  already retired replica is not reopened for the test. The approved growth
  operator installs it as a learner, holds the actual add-learner reply, then
  advances to its later guarded promotion with the same version/attempt and a
  strictly newer grant. Replaying the earlier action refuses at that boundary
  and again after promotion, preventing Raft's add-learner opcode from demoting
  the current voter. All three exact native identities, storage pins and prior
  application values remain intact, including a safe read on the fresh replica.
- A structurally valid altered target set using the earlier authority revision
  is also refused by full immutable-grant equality, without membership, storage
  or application mutation. The case uses separate metadata and load leaders and
  actual authenticated effect delivery/replay; no observed role substitutes for
  a recorded exact effect receipt.
- Marked this completed process item before commit. `make test` passed (697),
  clustertest vet and `git diff --check` passed. The focused race-enabled Docker
  scenario passed and reused the unchanged already built image
  `gquorum-authority-contract:20261013-meta-stale`. Reports:
  `test-reports/unit/20261012T205041.954230Z-26e3f5pm/report.md` and
  `test-reports/cluster/20261012T205118.919259Z-1o1ucymc/report.md`.
  Library/fixture functional-race code was unchanged here; its 806-pass report
  remains historical. Group-quorum-loss fencing and remaining integration,
  lint/dead-code cleanup and final complete validation/audit remain required.

- Tasks 15–16 integration batch: group revocation without load quorum remains
  pending despite healthy metadata/control paths; healing completes fencing and
  exact old-action replay is refused. Fresh admission and both retained pin
  phases now preserve the independent authority DB before metadata acquisition.
  The pin diagnostic reads the actual granted disk member while the constructed
  Node remains unstarted. Protocol regressions preserve retained/foreign bytes.
- Metadata control connection/DNS and remote transient failures now retain the
  library's unavailable classification; authentication and identity refusals
  remain terminal. An authenticated fresh endpoint with no acquired member returns
  replica_not_ready for scoped preparation probes; conflicting nonzero members
  still refuse. Shutdown tests wait for caller rescheduling after joined work.
- Admission leadership-change coverage uses a genuine majority election during
  learner execution because administrative transfers require settled topology.
  It observes the healthy replacement before checking exact attempt/step recovery.
  Process setup and later load transfers retry only classified transient refusals
  and confirm fresh current-term reads. README/API and example retention guidance
  are aligned; the example remains meta-only.
- Completed items above were marked before this grouped commit. `make test`
  passed 697 checks, existing vet-only `make lint` passed, focused protocol race
  checks passed five repetitions, and `make test-function-race` passed 816 checks.
  Three previously failing process cases passed three race-enabled repetitions
  each; the complete selected authority/read/admission/removal/readmission milestone
  then passed all 19 checks with verified Docker cleanup. Reports:
  `test-reports/unit/20261012T213320.019278Z-3mkqo9kz/report.md`,
  `test-reports/function-race/20261012T213207.991310Z-s24jcq95/report.md`,
  `test-reports/cluster/20261012T213207.991310Z-5jlzn3vo/report.md`, and
  `test-reports/cluster/20261012T213431.037054Z-ebkuzgy8/report.md`.
  Earlier failed reports are retained. Image:
  `gquorum-authority-contract:20261013-admission-ready`. Post-wiring pinned lint
  installation/unused cleanup, final documentation/complete suites and the final
  architectural audit remain required; no full final cluster suite has run.

- Task 17 complete as one post-wiring lint/cleanup batch: installed and pinned
  revive v1.17.1 and Staticcheck v0.8.2. Official revive rules cover unreachable
  statements, unused parameters and receivers; U1000 supplies unused declaration
  reachability. Default `make lint` now vets all three modules and applicable
  unit/functional/clustertest/example tags. Explicit Go-list source selection
  lets revive see tagged files; merged U1000 matrices preserve shared helpers
  referenced only by functional/process tests. No tests execute during lint.
- Removed the unused session lookup, obsolete applied-index fixture helper,
  unused controller signal and caller leader helper. Removed an unused snapshot
  argument and receiver from a pure authority-target helper; required interface
  callbacks explicitly omit unused parameter/receiver names. Generated protobufs
  retain standard exclusions; no blanket suppressions were added.
- Python runner regressions cover concatenated package JSON, external/tagged test
  sources, analyzer/build failure propagation and merged matrices. All 15 Python
  tests passed. Actual temporary-module analyzer probes confirmed nonzero failure
  for unreachable/unused code and that a functional-only shared helper stays live.
  `make lint` passed, `make test` passed 697 checks, and the complete functional
  race suite passed 816 checks. Reports:
  `test-reports/unit/20261012T213845.773093Z-j5u3ap6r/report.md` and
  `test-reports/function-race/20261012T213847.030647Z-1ktubi9s/report.md`.
  Marked Task 17 before committing. Final complete validation and architecture
  delta verification remain required; the full cluster suite remains last.

- Tasks 15/16/18 grouped audit correction: private planning had skipped historical
  `_meta` members. It now retains approved intermediate identities and node
  attributes through supersession, pins unresolved metadata preparation documents
  under bounded history, and restores a mutation-maintained inbound member index.
  Exact native-removal consumption or later Active convergence ends inbound
  routing. Native absence does not discard pending host retirement. Durable exact
  cleanup/stop/readmission receipts preserve retirement facts after completed
  operators leave history, without retaining those operators forever.
- Pure FSM regressions cover intermediate metadata membership, history pressure,
  owned node attributes, planning removal, snapshot/index restore, native removal,
  exact host stop and proof retention after pruning. A separate regression verifies
  that archived grants cannot resurrect a collected load replica after its
  completed cleanup operator is pruned. Staged identities stay excluded.
- A four-process regression uses three established voters plus one admitted
  metadata learner; a held load action prevents Active advancement. Supersession
  removes the intermediate native member, cleans its prepared load state, preserves
  survivor data/consistent reads and refuses replay of the old action.
- Process validation exposed two caller integration gaps. Load transport now
  retains member-bound cached endpoints and can resolve approved historical node
  attributes while exact authenticated ingress still owns authorization. Staged
  or foreign-member routes cannot replace that source. The fixture's startup
  waiter preserves control after a proven pre-readiness removal instead of closing
  before mandatory host receipts. Applied approved deletion of the node row now
  triggers self-removal for a previously seen member, including learners.
- Updated README, public integration/ownership/Raft docs, testing and physical
  fixture guidance, backlog and TiKV comparison to implemented P0/P1 contracts.
  Legacy meta/load formats are deliberately rejected without rewriting immutable
  baseline assets; old API-refactor results are explicitly historical. Verified
  local documentation links and `git diff --check`. Marked completed items before
  this grouped commit; final complete validation/audit items remain unchecked.
- `make test` passed 700 checks, expanded `make lint` passed, and full
  `make test-function-race` passed 818 checks. Focused metadata, retirement,
  self-removal and fixture routing/startup regressions passed three repetitions.
  The race-enabled Docker admission-supersession/removal checkpoint passed both
  cases using `gquorum-authority-contract:20261013-meta-history5`, with owned
  container cleanup verified. Earlier failed reports remain available and led to
  the corrections above. Latest reports:
  `test-reports/unit/20261012T221606.627097Z-c2utfe51/report.md`,
  `test-reports/function-race/20261012T221606.627104Z-8ouxtp15/report.md`, and
  `test-reports/cluster/20261012T221606.627104Z-cvyfxbwh/report.md`.
  The unit run contended with race compilation and Docker (33.264s); final
  validation still needs the uncontended unit timing and all remaining gates.


- Final-suite regression batch: the first complete race-enabled cluster run passed
  75 of 82 checks and exposed outdated process expectations plus one library gap.
  Forced retirement now verifies pending completion while its required host is
  unreachable, then returns retained storage for exact fencing/cleanup. Concurrent
  cleanup replays the captured immutable grant through authenticated exact ingress.
  Interrupted voter swaps recover the resource's durable receipt after controller
  failover rather than advancing from role observations alone. Guarded demotion
  regains leadership through a genuine election during pending topology, preserves
  the exact transfer guard while its target is isolated, and resumes after healing.
- A retained removed metadata replica can have no joining seeds or leader hint.
  Narrow authority-proof routing now resolves approved member names from its local
  catalog through the configured resolver. These candidate routes confer no
  authority: the remote leader still validates the full proof and courier. A
  single-owner/protocol-client regression proves this path while ordinary topology
  reads remain replica_not_ready. Authenticated fixture vote-request filtering
  isolates selected competing requests while preserving responses and other Raft
  traffic; owned diagnostics and exact receiver identity are covered under race.
- The isolated-former-leader case initially hit Docker setup contention; its
  assertions subsequently passed unchanged. The final invocation will bound pool
  concurrency at four without changing repository defaults or hiding failures.
  Earlier failed reports are retained, beginning with
  `test-reports/cluster/20261012T223327.511433Z-6r7cqwgx/report.md`.
- Marked both completed regression items before this grouped commit. `make test`
  passed 700 checks, expanded `make lint` passed, and `make test-function-race`
  passed 820 checks. Focused routing and election-control race regressions passed
  three repetitions. The seven previously failing cluster checks all passed with
  image `gquorum-authority-contract:20261013-retained-routes`; lease cleanup passed.
  Reports: `test-reports/unit/20261012T231445.590289Z-dzsapcqc/report.md`,
  `test-reports/function-race/20261012T231447.048901Z-2bqca50z/report.md`, and
  `test-reports/cluster/20261012T231444.189454Z-4lr8vzlb/report.md`.
  Final complete validation and failure-scenario/report audit remain open; the
  full cluster suite must run last again on the final fixture image.


- Tasks 18–19 complete: final implementation `c0a768c` passed every required
  validation gate. The last test invocation was the complete race-enabled cluster
  suite, using immutable image `gquorum-authority-contract:final-c0a768c`, four Go
  test slots and matching pool/startup concurrency. All 82 checks passed, with no
  failures, errors or skips; wall time was 354.015s. No code changed afterward.
  Pool teardown returned successfully; inspection found no containers from this
  image or networks/volumes from its pool or the isolated Compose test. Unrelated
  Docker resources were preserved.
- Final results below are actual runs of the final implementation, including the
  successful ordinary units, lint and functional-race batch preceding its commit:

| Gate | Result | Actual report or command log |
| --- | --- | --- |
| `make test` | 700 passed; 29.670s | `test-reports/unit/20261012T231445.590289Z-dzsapcqc/report.md` |
| `make test-race` | 700 passed | `test-reports/unit-race/20261012T231936.057575Z-lohuioii/report.md` |
| `make test-function` | 820 passed | `test-reports/function/20261012T231936.057573Z-hl1gv1h8/report.md` |
| `make test-function-race` | 820 passed | `test-reports/function-race/20261012T231447.048901Z-2bqca50z/report.md` |
| `make lint` | All module/tag vet, revive and merged U1000 checks passed | `/tmp/gquorum-final-retained-routes-lint0.log` |
| Python runner tests | 15 passed | `/tmp/gquorum-final-batch-python1.log` |
| `make proto-check` | Passed; no generated diff | `/tmp/gquorum-final-batch-proto1.log` |
| `make bench` | Passed | `/tmp/gquorum-final-batch-bench1.log` |
| `make test-example` | 1 passed | `test-reports/example/20261012T231957.559768Z-ne5akyp6/report.md` |
| `make test-perf-run` with final image, serial slots | 3 passed | `test-reports/cluster/20261012T232029.104433Z-qewenha6/report.md` |
| Full `make test-cluster-run`, final image, `-race -v`, four slots; LAST | 82 passed | `test-reports/cluster/20261012T232126.745065Z-nkm7b_c5/report.md` |

- Final architectural delta audit inspected target guarantees, real constructor/
  controller/protocol/public API wiring and the final package logs. Every target
  §5.8 failure mode has evidence in the passing suites:

| Required failure behavior | Verified implementation/test evidence |
| --- | --- |
| Behind grant creation; altered exact tuple/targets; exhausted grant capacity | Metadata authorization/query/authority units; `TestAuthorityCapacityReservesRevocation`; exact dispatcher refusal tests; same-operator process replay |
| Stale metadata before/after fencing; controller and group leader changes; lost fence acknowledgement | `TestAuthorityRevocationBoundaryAndLostFenceReplyThreeNodes` exercises both boundaries, actual stale receiver, leadership changes and durable lost reply |
| Effect before progress; consumed earlier step; replay/current-term administrative gate | Durable engine effect/transfer restart tests; interrupted voter-swap process cases; same-operator learner/promotion replay; deterministic snapshot/tail authority tests |
| Host ignores cancellation; incomplete fence recovery; unreachable required host | Managed canceled factory/Close/retirement ownership tests; authority DB reopen/uncertain-effect tests; retained-host revocation and forced-retirement process cases |
| Group quorum missing; isolated former leader; learner/local application behind read index | Group-revocation quorum-loss process test; all three consistent-read process scenarios; frozen cohort and fresh-round functional checks |
| Apply/Restore failure; Query error/cancellation; delayed or lost read response; term change | Engine halt/recovery tests; Query-only error and owned canceled callback tests; canceled-round bounds/lost-reply recovery and leader-change functional cases |
| Missing retained authority DB; missing/foreign snapshot authority; successor at reused endpoint | Authority store missing-file/foreign marker/exact rebind tests; complete snapshot codec rejection; managed/protocol identity refusals and process readmission cases |
| Committed lock ownership only; atomic owned metadata state | Metadata query/session tests and consistent metadata-read functional tests; public APIs preserve observation versus authoritative-read distinctions |
| Supersession/history pruning cannot discard intermediate identity or resurrect collected state | Intermediate metadata-owner and retirement-receipt units; four-process admitted-learner supersession; retained proof routing without leader hints/seeds |

- Verified all seven immutable physical fixture lengths and SHA-256 hashes against
  `docs/testing/api-baseline-physical.json`; no baseline asset was regenerated.
  Expanded lint confirms removed legacy dispatch paths and unused implementation
  stay absent across unit/functional/process/example build variants. README and
  architecture describe the selected resource-fence and safe-read contracts,
  including pending unreachable resources, committed lock semantics and explicit
  legacy-format rejection. Checked final documentation paths and `git diff --check`.
  Marked all remaining tasks done before this final documentation commit. Earlier
  failed reports remain evidence of the fixes; they are not counted as final passes.


- Task 20 complete as one related implementation batch. `util.NewInstanceID()`
  supplies only process incarnation IDs, using the standard library's secure
  random generation. Example and fixture startup use it; request IDs and authority
  revisions retain their separate mechanisms. No UUID dependency was introduced.
- Public `NodeOptions.StorageDir` replaces `AuthorityDBPath` and `Meta.DBPath`.
  The library derives fixed `_authority.bolt` and `_meta.bolt` filenames, and
  `LoadStoredIdentity` accepts the directory. The user explicitly selected keeping
  application storage in `Loads.Storage`; no replicated per-group path was added.
  Constructor validation still performs no I/O. Startup checks real directory/
  file ownership, aliases and resolved retirement-root boundaries. Former system
  filenames produce unreadable-state errors without creating replacement stores
  or altering retained evidence. Authority retains its independent Node lifetime
  through metadata replacement; actual readmission backups use the new prefix.
- Added newcomer comments to manifest/bootstrap setup, process versus durable
  identities, listener acquisition, system storage, authentication/address routing,
  readiness, process cancellation, shutdown ordering, HTTP observations, CAS edits,
  asynchronous operator completion and bounded request waiting. Updated current
  architecture, README, example instructions and all caller/fault-control paths.
- Process validation caught old backup-path assertions, which now inspect the
  actual new-prefix backup and its unchanged durable pin through the directory API.
  It also exposed a real cleanup catch-up race: a returning live metadata replica
  could receive a verified committed grant before applying its catalog record.
  Local ledger cleanup now returns retryable unavailable for a well-formed same-
  cluster grant beyond its applied index and never invokes the ledger mutation.
  Pure regressions verify successful exact retry after catch-up and terminal
  unknown, altered and foreign refusal. Captured process diagnostics remain on
  failure; all three retained-host/readmission cases passed three repetitions
  after the fix. Report:
  `test-reports/cluster/20261013T065018.837216Z-l45dsqgu/report.md`.
- One full run passed 81/82 with a Docker inspection timeout during initial
  container startup; the scenario assertions had not started. The unchanged
  deletion-refusal case passed three focused repetitions. The next full run
  exposed a non-host transfer test requiring immediate success despite a valid
  transient unavailable response. It now shares the metadata transfer's bounded,
  classified transient retries and still checks exact resulting leadership and
  retained application/storage identities. Three focused repetitions passed.
  Reports: `test-reports/cluster/20261013T065845.165391Z-yz9y9mts/report.md` and
  `test-reports/cluster/20261013T070814.433735Z-m74jx0bw/report.md`.
- Final successful checks for this batch:

| Check | Actual result | Report/log |
| --- | --- | --- |
| `make test` | 704 passed; warm run 3.753s | `test-reports/unit/20261013T065147.953032Z-7zjb4moa/report.md` |
| `make test-race` | 704 passed | `test-reports/unit-race/20261013T065104.413077Z-bnv1d0yt/report.md` |
| `make test-function-race` | 820 passed | `test-reports/function-race/20261013T065018.837213Z-_a3n0fta/report.md` |
| `make lint` after final process-harness fix | All modules/tags/analyzers passed | `/tmp/gquorum-example-layout-lint-final.log` |
| Compose after storage/example refactor | 1 passed | `test-reports/example/20261013T064234.814694Z-u0m1vzz2/report.md` |
| Full cluster, `-race -v`, final image, three slots; LAST | 82 passed, zero failures/errors/skips; 368.580s | `test-reports/cluster/20261013T070836.078291Z-zua4z0hd/report.md` |

- Compose covers the meta-only paths, which the subsequent cleanup correction
  does not exercise or change. The final cluster includes all three performance
  scenarios. Image: `gquorum-authority-contract:fixed-system-storage-lag-20261013`.
  No Go code changed after the final full run. Pool cleanup returned successfully,
  with no remaining containers from this image. Earlier failed reports are
  preserved. Verified documentation links, `git diff --check`, and all seven
  immutable physical fixture hashes; no baseline file was renamed or regenerated.
  Marked every Task 20 checkbox before this grouped commit.
