# Permanent-majority-loss recovery execution record

Plan: `docs/plans/20261025-permanent-majority-loss-recovery.md`
Target: `docs/brainstorms/20261024-permanent-majority-loss-recovery-target.md`
Branch: `feat/permanent-majority-loss-recovery`
Started: 2026-10-25
Review cadence: completed tasks/commits 5, 10, 15, then task 16 plus final verification.

## Baseline — before Task 1 Go changes

- Source: 88833e8 (Go code unchanged from 2684202).
- `make bench BENCH_TIME=1s`: PASS, planner and metadata snapshot matrices.
- Go 1.26.5, darwin/arm64, Apple M4 Pro (14 logical CPUs in benchmark output).
- Raw log: `test-reports/recovery-baseline/88833e8/bench.log`.

## Task 1 — Approved contracts and pure configuration helpers

Date: 2026-10-25. Commit: included with this task's implementation commit.

Implemented
- Public recovery value aliases, Refusal with cause preservation, and root
  NewRecovery/Inspect/Approve/Run/Cancel/Status/List/RecoveryStartOptions signatures.
- Pure source configuration validation/copying, Node-owned-store restriction,
  resource defaults/overflow checks and explicit temporary I/O refusals.
- Full local/import/seed models, strict bounded duplicate/unknown-key JSON decoding
  and owned-copy helpers. Metadata FSM and existing persisted formats are untouched.
- Optional load.ReplicaSpec.Recovery input; ordinary factory contract unchanged.
- Units cover no acquisitions/callbacks, ignored invalid old runtime options,
  configuration/field copies, mode/phase checks, ID/resource validation, exact JSON
  integers, ambiguous records and public caller-side signature compilation.

Verified
- Baseline make bench BENCH_TIME=1s: PASS (captured before Go edits; see above).
- go test ./internal/recoverymodel ./recovery . ./load: PASS.
- go vet ./recovery ./internal/recoverymodel . ./load: PASS.
- make test: PASS, 1,983 tests across 51 reports/all five application modules,
  final rerun 6.913s. Report: test-reports/unit/20261024T231414.691843Z-pisn1q2w/report.md.
- git diff --check: PASS. No frozen fixture or public Node/storage interface change.

Notes
- I/O methods intentionally refuse implementation_pending per Task 1. Real source
  inspection, journal semantics and runtime reconstruction belong to later tasks.
- Added split model/clone files to keep the full schemas and ownership helpers
  readable; these are the same planned records, not a new API or format.

- Initial commit hook caught import grouping, line length and helper complexity;
  refactored pure validators/JSON scanning and fixed formatting/comments before
  committing. Required production-only golangci check now reports zero issues.
- A direct golangci invocation initially included test rules that the repository
  wrapper intentionally scopes to production; final checks use the repository's
  production/test analyzer split. No unrelated baseline tests were rewritten.


## Task 2 — WAL implementation and exclusive startup ownership
Date: 2026-10-25. Commit: included with this task's implementation commit.

Implemented
- Moved WAL implementation and private tests to internal/walbackend. Public
  walstore retains aliases/constants/Open and public conformance coverage.
  All moved files except store.go are byte-identical after the package rename.
- Stable system/WAL lock files, retained root descriptors, nonblocking ownership,
  exact lease adoption/return, partial-acquisition cleanup and root/lock substitution
  checks. Nested WAL roots work; reverse, symlink, alias/private-root misuse refuses.
  Canonical private-root refusal happens before creating any directory there.
- Ordinary Node startup owns both roots before authority/meta/store acquisition.
  Failed WAL adoption returns its lease to Node until actual joined teardown.
  Injected stores retain borrowed ownership. Pending/corrupt/unknown recovery
  evidence refuses before either metadata database is created.
- Validated terminal system history authorizes only exact WAL archive names.
  HasState ignores stable locks and proven archives, rechecks namespace entries,
  and preserves unproven standalone/unknown/partial evidence.
- Audited root enumeration: Retired filters exact retirement-token directories;
  snapshot/segment/collection scans address replica-local subtrees and require no
  namespace exception. Resource endpoint counts actual /proc/self/fd descriptors;
  new lock/root descriptors remain counted and no adjustment hides them.
- Added lifecycle regression coverage for constructor purity, metadata-only and
  separately held WAL contention, pending startup, failed application Close,
  factory shutdown timeout, lease transfer/failed adoption and retained lock files.

Verified
- make test: PASS, 1,983 tests/53 reports across all five application modules,
  18.659s with concurrent qualification. Report:
  test-reports/unit/20261024T233915.936345Z-c09yo0oq/report.md.
- Focused functional suites for root, recoverydisk, backend and public facade: PASS;
  final durations 22.967s/0.290s/7.225s/6.773s respectively.
- Same full focused functional suites under -race -count=1: PASS;
  root19.420s, disk1.414s, backend8.744s, facade8.549s.
- make lint: PASS across all configured modules/build tags and tooling modules.
- make test-sim SIM_SEEDS=1-3: PASS, 21/21 cases, 53.648s. Report:
  test-reports/sim/20261024T233914.702621Z-huqufugx/report.md.
- All 12 recorded legacy/scoped physical fixture hashes match; no fixture edits.
  Existing WAL encode/reopen/retirement and public conformance suites pass.
- git diff --check: PASS.

Notes
- Pinned Gosim does not hook Go's internal/syscall/unix.Fstatat used by os.Root.Lstat.
  Startup instead uses os.Lstat plus root/lock identity checks; root descriptors and
  actual locking remain active in simulation. No simulation-tag bypass was added.
- Concurrent source edits during two intermediate analyzer/simulator runs produced
  input-resolution/compile races; stable-source reruns above pass. Early lint/style
  and a facade conformance fixture's wrong cluster ID were corrected before commit.
- Full manifest semantics and root-relative archival mutations remain Task 4/7 work;
  this task's normal-start guard conservatively refuses all pending operations.


## Task 3 — Node-owned process fixture and independent runtime generations
Date: 2026-10-25. Commit: included with this task's implementation commit.

Implemented
- Added opt-in NodeOwnedWAL configuration/CLI/pool setting. Owned mode selects
  the existing /data/load root, 1 MiB segments and Prewrite=false through public
  NodeOptions; it opens no second Store. Default borrowed wrappers remain intact.
- Retained private construction settings and copied caller peer/voter inputs.
  Added synchronized Node/Cluster accessors throughout production fixture code;
  the original fields remain immutable compatibility inputs for focused fixtures.
- Added per-generation request cancellation/join, background-worker ownership,
  serialized Stop/restart, fresh process identity and recreated metadata listeners.
  HTTP/load listeners, authentication, status and process liveness survive Node
  shutdown. Metadata-proxy calls, load deliveries and fault filters retain their
  accepted generation. New installation waits for the old generation to join.
- Added management stop/start/status routes and public-only owned observations:
  StorageAvailable/WALControlAvailable and exact published GroupIdentity.
  Physical WAL statistics/wrapper controls are unavailable in owned mode;
  unsupported loss/WAL/collection-counter routes refuse explicitly.
- Owned-mode functional/race coverage exercises replayed data across repeated
  snapshot/restarts, stale concurrent Raft/HTTP ingress, accepted-request shutdown
  and failed startup before listener transfer. A failed Start can be joined and
  retried without leaving a listener bound.
- Added TestOwnedWALRuntimeSmoke with parallel three-node retained-generation and
  metadata-loss/readmission cases. The latter reuses only public identity/native
  membership/value assertions; original borrowed scenarios keep their behavior.

Verified
- make test: PASS, 1,984 tests/53 reports, 5.957s, all five modules. Report:
  test-reports/unit/20261024T235132.129491Z-8xaca872/report.md.
- Full testapp functional suite: PASS (5.334s); full functional -race -count=1:
  PASS (7.869s), including new generation/ownership regressions.
- make lint: PASS, all configured modules/build tags/tooling.
- Cluster-tag caller modules compile with -run '^$'.
- Built gquorum-recovery:local, immutable final image ID
  sha256:2737a8f6b428b34c8c79107e1d5dd8e47cdceb595e093c0c178ce26d56f8073e.
- Required targeted Docker run: PASS, both smoke scenarios/3 report rows, 17.879s.
  Report: test-reports/cluster/20261024T235138.663795Z-23xl27wm/report.md.
  Repeated embedded Node restarts preserve exact numeric/storage identities and
  data while changing InstanceID. Readmission rotates metadata/load identities
  and then preserves the fresh pin through another process restart.
- Pool teardown verified all application containers, labeled volumes and labeled
  networks removed; only the ordinary testcontainers reaper remained. Tests use
  the pool's existing persistent Docker volumes mounted at /data, retaining the
  same root mounts across runtime generations and process restarts.
- Task 2 simulation evidence remains applicable: no library code changed.
- git diff --check: PASS.

Notes
- The first retained-generation smoke reached group creation before load leadership
  election and returned not_leader on its initial write. Added explicit observed
  leadership and same-request retry barriers; both subsequent Docker runs passed.
  No product timeout or consensus behavior was weakened.
- Focused real-replica tests likewise wait for leader election before proposing,
  and wait for asynchronous request cancellation rather than assuming AfterFunc
  executes synchronously. Initial lint findings were fixed before final validation.
- Recovery jobs/routes remain Task 13; no stubbed recovery I/O method is called.


## Task 4 — Durable journals and read-only discovery
Date: 2026-10-25. Commit: included with this task's implementation commit.

Implemented
- Canonical accepted evidence/digest, bounded schema-1 manifest/current/seed
  codecs, complete identity/inventory/resource/phase validation and monotonic CAS
  updates. Exact JSON field spelling rejects case aliases as well as duplicate,
  unknown, trailing, oversized and malformed records. Resource settings remain
  latched immutable acceptance even though the content digest excludes them.
- Durable Create/Open/View/Update, per-action Files attempts, cancellation audit
  and completion, write-once successor marker, archive/seed progress and matching
  pointer clearing. Reset journals deliberately contain no replay seeds; actual
  join identity is write-once. Reserved rollback records decode/report but refuse
  mutation/startup. No public Inspect/Approve/Run/Cancel behavior is enabled yet.
- Private filesystem helpers use no-follow directory handles, exclusive-owner
  no-clobber rename, temporary file sync/close/publication and both parent syncs.
  Newly created directory ancestors are synced. Fault dependencies belong to each
  Owner; no global failpoint was introduced. Unpublished owned empty directories
  are cleaned up; observed approved manifests survive uncertain publication.
- Read-only Status/List work while runtime leases are held and the configured WAL
  mount is unavailable, without live-pin/quorum reads, factories or Files hooks.
  Discovery retains valid rows plus typed errors, excludes duplicate/renamed IDs,
  bounds each read before allocation and sorts acceptance time/ID deterministically.
  Stale/missing pointers cannot bypass startup guards or reserve an ID anew.
- Startup now uses these exact semantic codecs. Guard tests use complete pure
  records via internal/recoverytest rather than earlier temporary header fixtures.
  WAL archive inventory supports verified collection-marker files as well as dirs.

Verified
- make test: PASS, 2,019 tests/54 reports, 14.390s across all five modules. Report:
  test-reports/unit/20261025T003100.441106Z-bvjesz_r/report.md.
- Full root and recoverydisk functional suites: PASS (18.715s and 1.034s).
- Same full functional suites with -race -count=1: PASS (20.087s and 2.079s).
- make lint: PASS across every configured module/build tag/tooling module.
- make test-sim SIM_SEEDS=1-3: PASS, 21/21 cases, 47.803s. Report:
  test-reports/sim/20261025T002415.782454Z-ss564phi/report.md.
- Fault coverage includes manifest/current temp sync, rename and parent sync;
  uncertain phase reopen, explicit Undo after an attempted Backup, stale/empty/
  matching-only pointers, reserved IDs, concurrent atomic readers/writers,
  duplicate/mismatched names, sparse oversized records and symlink/hard-link refusal.
- Reset progress regression verifies no replay seeds, JoinReady completion,
  successor-before-join ordering and immutable joined identity.
- Legacy physical fixture hashes match, scoped fixture trees have no diff,
  and git diff --check passes. Original source files remain intact in fault tests.

Notes
- Direct Go os.Root.Lstat was already known unsupported by Gosim. This task also
  verified that pinned Gosim's fs.dirInode accepts only AT_FDCWD and Openat rejects
  real directory descriptors, despite exposing Openat/Fstatat/Renameat hooks.
  Source: gosim v0.9.1 internal/fs/ops.go:33-66. Initial simulation failed on
  openat(".") accordingly. Ordinary startup now checks namespace absence under
  the existing stable leases before entering descriptor-based journal readers.
  This is the same production path, with no simulation tag or lock bypass.
  Existing journal namespaces and all recovery writes retain the no-follow handle
  protocol; recovery filesystem operations are verified by native functional/
  process suites, not new Gosim recovery scenarios.
- A pure test-model helper package and split validation/progress files keep the
  shared schema and tests coherent. No persisted legacy encoding was changed.


## Task 5 — Bounded source readers and shared committed replay
Date: 2026-10-25. Commit: included with this task's implementation commit.

Implemented
- Read-only metadata/authority Readers retain their descriptors, validate schema,
  pins, ledgers and complete journal facts, and bound encoded database key/value
  input before typed decoding. Missing stores are never initialized; no bucket
  creation, materialized-state rewrite or migration occurs. Files with unused
  bbolt pages are not confused with decoded-byte budgets.
- A streaming WAL frame decoder checks lengths before allocation, chains CRCs
  across gap-free segments and holds one segment descriptor at a time. Earlier
  unexpected EOF/CRC/gaps refuse; only the final file can supply a reported torn
  candidate, and retained commit/snapshot/applied/floor boundaries still gate it.
  Normal readLog now shares these rules with its original addressable-input
  ceiling; recovery-specific byte defaults are not imposed on ordinary stores.
- Nonmutating bounded snapshot CRC/marker selection refuses a required corrupt
  image without .broken renames. Above-commit markers/files are reported and never
  promote C. Retained positions/semantic digest include hints, tail and torn evidence;
  selected snapshot plus materialized tail obey the group input budget.
- Extracted newRestoredCore and reused the live committed-entry interpreter in
  raftengine.Replay. It restores the complete snapshot and only entries through C,
  interprets native/authority effects and starts no workers, elections, transport or
  persistence. DiscardMachine supports load preflight without caller callbacks;
  metadata replay uses NewMachine(rules,nil).
- Added metadata snapshot-boundary validation in shared restore paths. An
  application metadata position cannot hide tail commands or exceed the native
  checkpoint; the normal empty index-1 founding checkpoint remains valid.
- Added source-preservation, bounded/absent/legacy/refusal tests, exact C-versus-H/L
  replay, native authority replay, malformed padding, early/final EOF, earlier CRC
  with zero sectors, gaps, corrupt/orphan snapshots and cancellation coverage.

Verified
- make test: PASS, 2,021 tests/54 reports, all five modules, 9.262s. Report:
  test-reports/unit/20261025T012030.588943Z-nrwskv_6/report.md.
- Full focused functional suites: backend9.958s, facade9.659s, engine8.990s,
  metastore4.609s, authority7.876s, metadata FSM3.383s; all PASS.
- Same six full functional suites under -race -count=1: all PASS
  (backend11.977s/facade11.435s/engine10.647s/meta-store6.798s/
  authority9.248s/meta-engine27.389s). Additional discard replay -race: PASS1.504s.
- Full root functional suite: PASS15.277s. make lint: PASS across all configured
  modules/build tags/tooling; focused production lint also reports zero issues.
- make test-sim SIM_SEEDS=1-3: PASS21/21,55.091s. Report:
  test-reports/sim/20261025T011634.373399Z-43548mi0/report.md.
- Rebuilt gquorum-recovery:local image
  sha256:e4b2c2eac0290c73948ea4d2b95b90c1b24378b7361c9ea28a914dc18a4b9539;
  repeated required three-node owned-runtime/readmission smoke: PASS3/3,18.349s.
  Report: test-reports/cluster/20261025T011900.269055Z-cu6ynlns/report.md.
  Verified no labeled application containers, volumes or networks remain.
- Recorded legacy physical hashes and scoped metadata/authority hashes match.
  git diff --check: PASS. Read-only success, refusal and cancellation leave source
  bytes and names unchanged; no frozen artifact was regenerated.

Notes
- The frozen scoped-v4 meta.bolt is a store/FSM fixture: its captured snapshot and
  entries are bare metadata payloads, not native engine envelopes. Read-only
  metadata loading is supported, but native replay correctly refuses them.
  Modern end-to-end metadata replay instead uses an actual single-node runtime's
  retained store, preserving the frozen fixture rather than adapting it in place.
- The first shared initializer accidentally retained the cancelled startup context
  in dormant groups. An existing synchronous-owner leadership regression caught
  this; normal groups again obtain their context only at Start, while offline
  replay explicitly owns its context. The unchanged regression and full suites pass.
- Snapshot boundary validation extends cited files as recorded in NOT PLANNED
  CHANGES. Authority journal persisted record definitions remain untouched; typed
  offline evidence is a working projection, not a replacement disk format.
- Review cadence reached: ask the existing colleague to review Tasks1–5 before
  continuing with complete-survivor inspection/approval in Task6.


## Tasks 1–5 review checkpoint — crash publication and retained-source defenses
Date: 2026-10-25. Commit: included with this checkpoint fix commit.

Implemented
- H1: initial approval now writes/syncs a manifest inside reserved
  @approve-<32 lowercase hex> staging, syncs the directory, then renames the
  entire directory without replacement to the immutable BackupName and syncs
  its parent. Empty namespaces and strictly shaped unpublished staging never
  become operations/guards. New approval collects staging under the lease;
  foreign files, nested directories, links and oversized manifests still refuse.
- Tests retain real interrupted disk shapes across owner restart, cover namespace
  creation/directory creation/file sync/rename/parent sync/current-pointer faults,
  preserve A's source bytes, distinguish prepublication from the durable guard,
  and prove retry repairs publication/pointer state without manual filesystem work.
- M1/M2: final zero headers require a streaming all-zero remainder; a zero header
  in an earlier segment refuses. File-fit checks precede record/group allocation
  budgets, so an oversized incomplete final frame gets the explicit torn path.
- M4: CRC-valid conflicting committed rewrites refuse. Exact committed retries
  preserve the durable committed suffix even after a partial repeated Ready;
  they discard the old uncommitted suffix consistently with replacement replay.
  Pure and real-WAL tests cover term/type/payload conflicts, exact retries and
  legitimate replacement beyond C. No source repair or tail rewrite occurs.
- L1: an already-closed owner lock no longer prevents joining the root descriptor.
- M7: generated a separate native fixture from an isolated exact baseline
  26842024d2fb4fe6fd74b15efa5106749a8b0674 export, never current codecs. It contains
  a nonempty native snapshot at 33/term2, commit35 and two tail entries plus the
  matching authority journal. Capture source, raw/compressed hashes and provenance
  accompany frozen gzip files. Read-only replay leaves source bytes unchanged;
  ordinary current Node restart succeeds even with abandoned approval staging.
  Received snapshots already use the same restoreMachine validator as startup.
- M6/L5 are explicit additions within existing Task10/Task15 checkboxes: owned
  archive-history fresh-start checks, and 10k-cleanup-debt metadata budget shape.
  No seventeenth task was created. Public API signatures remain unchanged.

Verified
- make test: PASS, 2,027 tests/54 reports across all five modules, 6.604s.
  test-reports/unit/20261025T014404.401680Z-nyia1m_3/report.md.
- make lint: PASS across all configured modules/build tags/tooling.
  /tmp/gquorum-recovery-review-lint.log.
- Full affected functional suites under -race -count=1: disk4.550s,
  WAL7.625s, metadata FSM27.555s, engine5.447s, root20.443s, all PASS.
  /tmp/gquorum-recovery-review-race.log.
- After final committed-retry refinements, repeated full WAL/facade functional
  race suites: PASS7.465s/6.911s. Final baseline restart including staging:
  PASS -race3.017s. New higher-term-tail and CRC-valid rewrite cases also PASS.
- make test-sim SIM_SEEDS=1-3: PASS21/21,50.524s.
  test-reports/sim/20261025T014034.842676Z-upl00pjq/report.md.
- git diff --check: PASS. No existing frozen fixture was overwritten. No Docker
  image rebuild or new process-suite run at this checkpoint; the Task5 smoke
  evidence is historical, and final qualification remains scheduled.

Review dispositions / design notes
- M3 remains the pre-existing conservative refusal: a retained entry term above
  HardState.Term refuses even beyond C. Added a realistic interrupted-write,
  source-preservation regression; no term inference or hidden normalization.
  Requesting the peer's response before any decision to widen this contract.
- M5's proposed fallback is not in the approved target: sections3.3/15.10 select
  the exact current pointer. Kept that behavior, documented Status's valid-value
  plus unrelated-namespace-error result, and documented List as discovery when
  the current pointer is absent. Missing pointers never bypass startup scanning.
- L2 descriptor-relative lock hardening is optional; hostile concurrent mutation
  is excluded, existing identity rechecks remain, and Gosim's missing dirfd hooks
  cannot justify weakening the shared ownership path. L4 optional typed progress
  strings are not needed for correctness and approved declarations stay intact.
- Clarified the local target's approval publication and existing conservative
  term/pointer semantics; saved its previous content at
  docs/brainstorms/20261024-permanent-majority-loss-recovery-target.before-implementation-review-20261025.md.
  Brainstorm sources remain ignored local design inputs as before; the tracked
  plan records the implementation correction and the checkpoint commit its diff.


## Task 6 — Complete-survivor inspection and exact approval
Date: 2026-10-25. Commit: included with this task's implementation commit.

Implemented
- Replaced public Inspect/Approve stubs. Inspection owns both root leases,
  retains read-only system readers, reconstructs native metadata through C with
  a nil-persister machine, validates the whole scoped graph and source pins,
  and returns owned S/C/L/term/byte reports and collected blockers.
- Metadata projection separates Target requirements and exact approved/proven
  donors, includes pending-predecessor/retirement evidence, archives operation,
  session, lock and unresolved-cleanup summaries, and preserves staged allocation
  high-water marks plus approved-deleted/retired group reservations. No global
  FullyConverged prerequisite or import of staged-only intent.
- Bounded WAL inventory checks every entry. Physical pins must match source
  identity, exact metadata proof and the local ledger; completed retirement
  excludes donors. Unknown live WAL blocks and reports readable retained
  positions. Collection markers need exact validated journal proof. Verified
  readmission backups bind their admission-derived names and old pins.
- Reset classifies proven ownership without load application replay: a damaged
  owned payload can be archived, but foreign/unrecognized/contradictory identity
  evidence still blocks. No application callback, source move, live engine,
  network service or application Run executes during inspection/approval.
- Generated effective request IDs, deterministic fresh lineages, unused monotonic
  identities, overflow and survivor eligibility checks. Caller RequiredGroups
  supplements every approved group. Future manifest/import/identity/seed header
  bounds are checked before approval, including the engine's base64 entry wrapper
  and normal metadata entry ceiling, rather than only raw proposal JSON.
- Added logical bbolt digests for every ordered bucket/key/value and sequence.
  They bind cached state, pins, ledger, hints and journal counters while ignoring
  unused pages. Active WAL fingerprints retain validated semantic input; opaque
  retained artifacts use streaming canonical whole-content hashes. Tree checks
  reject symlinks, writable hard links, special files and device crossings,
  without hashing active preallocation. Task7 completes same-device mount proof
  before destructive execution, as explicitly recorded in its existing checkbox.
- Approval re-inspects under the same owner, requires exact digest and loss/fencing
  attestations, latches resources/hook presence and publishes Approved through the
  crash-safe journal. Exact historical repeats use saved evidence, never evolving
  B; changed audit/destination/hook declaration/explicit bounds conflict. Cancelled
  IDs remain reserved. Concurrent exact approvals share one guard/result.
- Accepted peer M3 correction: track WAL batch term provenance; report only a
  final unsynced newer-term suffix wholly beyond C; refuse a later lower-term
  HardState or any such committed entry. Preserve the complete tail/digest/bytes,
  and pass model.CommittedPrefix to the unchanged shared replay validator. The
  real managed-native snapshot test proves malformed uncommitted application
  bytes are not interpreted. Ordinary startup remains strict. Task11 reuses the
  same prefix helper. This supersedes the checkpoint's temporary refusal limit.

Verified
- make test: PASS, 2,045 tests/55 reports across all five modules, 8.895s.
  test-reports/unit/20261025T025209.809063Z-xovhelk5/report.md.
  An earlier run overlapping race compilation/lint took30.237s; isolated run above
  confirms the fast-unit budget. No service/filesystem tests were moved into units.
- Full make lint: PASS across configured modules/build tags/tooling.
  /tmp/gquorum-task6-lint-final.log.
- Full affected functional suites -race -p4 -parallel8 -count=1: all PASS:
  root32.623s, disk4.260s, backend10.170s, facade8.225s, metadata FSM28.226s,
  meta-store4.200s, authority7.409s, engine9.328s.
  /tmp/gquorum-task6-functional-race-final.log.
- Root recovery non-race cases PASS; all owning disk/backend functionals PASS.
  Additional exact concurrent-approval race PASS3.934s and actual unknown-WAL
  positions race PASS7.272s. Pure changed-package race checks all PASS, including
  model1.448s/inspector1.345s/disk1.302s/FSM3.936s/backend1.336s.
- make test-sim SIM_SEEDS=1-3: PASS21/21,65.578s.
  test-reports/sim/20261025T025420.849250Z-uvw3w784/report.md.
- Source-preservation, same-digest unused-page padding, stale logical metadata,
  complete load/metadata-only, additive coverage, damaged reset, unsupported memory,
  source busy/mismatch, historical B mutation, cancellation ID, bounds/header,
  pending/proven/retired classification, staged floors and retired-ID cases pass.
- Existing frozen artifacts remain untouched. git diff --check passes. No new
  Docker run/image replacement at this task; process recovery coverage remains
  scheduled after the fixture/executor/startup pieces are complete.

Notes
- Thread4 closed after round3: peer confirmed checkpoint fixes and no Task6
  blocker; M3 accepted as above. No pending user decision or public API redesign.
- Actual source proof is layered: ClassifyReplicas projects trusted local metadata
  classes, then the owned WAL scan validates physical pins/payloads and collects
  every unmatched artifact. A declaration alone never promises usable data.
- The first added unknown-WAL fixture omitted the standard Store's required
  NodeName/ClusterID. Fixed that fixture setup; product validation stayed strict.
- Public Run remains temporarily non-destructive and startup refuses AwaitingStart
  until the scheduled matching Node integration. Continue with Task7. Next regular
  peer checkpoint remains after Task10 (five completed tasks since Task5).


## Task 7 — Joined backup/cancellation and in-root archive execution
Date: 2026-10-25. Commit: included with this task's implementation commit.

Implemented
- Added private recoveryexec above the disk and WAL/system-reader layers. It
  borrows one owner/journal per call, starts no engine/application/workers and
  joins callbacks directly. This avoids the disk↔reader dependency cycle;
  filesystem operations remain descriptor-bound disk capabilities. Public
  lifecycle signatures are unchanged and the local target records the placement.
- Approved/Files enters journaled Backup; each callback attempt is durable before
  invocation and success is recorded after actual return. Nil hook declares no
  external work. A cancelled waiter cannot detach an ignoring callback or free
  its root leases. Successful late callbacks record completion before returning
  cancellation; errors never invoke automatic Undo. Failure diagnostics are
  bounded/sanitized while original causes remain returned.
- Cancel validates the immutable operation and original library evidence before
  write-once audit/Cancelling. Any Backup attempt requires separately journaled
  Undo even after Backup failure. Failed Undo retains guard/ownership semantics;
  Run(Cancelling) resumes only that Undo. Verify unchanged source again, persist
  Cancelled before clearing the matching pointer, retain history/reserved IDs.
- Verify original inventory before Backup and again afterwards. Prepare private
  destinations, then persist Archiving before any source move. Move authority,
  metadata, verified readmission files and owned WAL data within their original
  roots; no mountpoint/root rename, cross-volume copy, overwrite or collection.
- Reconcile exact accepted evidence at source or backup after uncertain rename.
  Both/neither/conflicting paths and durable-Moved reversals refuse. Sync both
  parents before marking Moved. Full logical bbolt/WAL digests verify active data;
  opaque retained artifacts use streaming whole-content hashes. Unknown new live
  or backup entries refuse. Finish with AwaitingStart/JoinReady, not a running B.
- Added descriptor mount identity to artifact/private-directory validation:
  Darwin Fstatfs mount names and Linux Statx MNT_ID distinguish same-device bind
  mounts. Unsupported proof refuses. No ordinary Node/simulation lock bypass.
- Public Cancel and cancellation/history Run are wired. Destructive public Run
  still refuses before Backup/archive until Task12 supplies safe matching Node
  startup; tests exercise that same private executor without exposing a force flag.

Verified
- make test: PASS, 2,045 tests/56 reports, all five modules, 7.279s.
  test-reports/unit/20261025T032739.790108Z-41wuvbko/report.md.
- Full make lint: PASS across modules/build tags/tooling.
  /tmp/gquorum-task7-lint.log.
- Full affected functional-race suites -p4 -parallel8 -count=1: PASS
  root38.727s/disk2.930s/backend10.870s/facade7.016s/meta-store3.165s/
  authority6.541s/meta-FSM27.441s/engine8.380s.
  /tmp/gquorum-task7-functional-race.log.
- Non-race root/disk functional suites: PASS27.242s/1.232s.
  /tmp/gquorum-task7-functional.log. Focused callback/move/guard races PASS,
  including final actual AwaitingStart startup refusal9.452s.
- make test-sim SIM_SEEDS=1-3: PASS21/21,51.847s.
  test-reports/sim/20261025T032742.134625Z-1_p178f9/report.md.
- Linux/arm64 static probe built from the captured inert source in
  internal/recoverydisk/testdata/mount-probe. Cached alpine:3.23.3 ran one unique
  labeled volume mounted twice: device65025 matched, distinct descriptor mount
  identity was specifically refused, source bytes unchanged. Probe PASS; both
  containers and volume removed and label-filtered listings empty.
  /tmp/gquorum-task7-mount-probe.log. No shared image rebuild or broad cluster run.
- Tests cover failed Backup after application-file move, failed/retried Undo,
  no automatic Undo, callback late return/context/lease contention, cancellation
  before Backup, conflicting audit, durable Archiving before first move, lost
  move acknowledgement, both/neither/changed backup paths, source mutation,
  separate/nested/missing roots, rename/parent-sync/ENOSPC faults, no-clobber,
  preserved original bytes, reset preparation and public destructive/startup guards.
- git diff --check passes; no frozen fixture changed. Temporary Linux .go probe
  was removed; only inert source and qualification notes remain in testdata.

Notes
- The first callback-race test reused its local result type name as a variable;
  fixed that test declaration. No product contract was weakened.
- A missing WAL root now fails source verification before application Backup.
  Root acquisition may create its stable empty directory/lock, but never treats
  missing data as completion, runs the hook, archives A or clears its guard.
- Ordinary post-archive startup remains intentionally refused until Task10–12.
  Policy repair (Task8) is next; peer review remains scheduled after Task10.


## Task 8 — In progress: private placement adapter foundations
Date: 2026-10-25. Not complete; no task checkbox ticked or implementation commit.

- Created private recoveryplacement Context/FromImport/PolicyDigest, exact policy
  scoping including released-state refusal, count-normalized owned copies,
  Prepare/Validate/BuildPlan/ValidatePlan/CanRelease scaffolding and pure tests.
- Initial strict-AZ singleton→two→three preparation/promotion and forged/released
  provenance tests pass: go test ./internal/recoveryplacement -v -count=1.
- Preparation preserves ordinary learner-first admission. Normalized minima must
  follow currently assigned voters, while normalized voter targets follow eligible
  capacity; requiring the attainable target as the current minimum blocked safe
  intermediate learner admission. Original count policies are restored in output.
- These files are uncommitted. No authoritative replicated recovery state exists
  until Task9 and no adapter is wired into service/FSM/repair paths yet. Do not
  claim Task8 complete from its isolated pure tests.
- Next: preserve zero-context behavior precisely (the adapter currently adds a
  load minimum-count check that must not silently tighten expert Stage semantics),
  add the planned native-plan/deficit/pin/policy-edit/release cases, trace/refactor
  every service/FSM/repair refusal path and wire absent/default context until Task9.
  Verify initial v1→v2 import scope and progressive repair separately. Then run
  all task verification and commit Task8 as one substantial task.


## Task 8 — Scoped recovery placement and repair adapters
Date: 2026-10-25. Commit: included with this task's implementation commit.

Implemented
- Completed private Context/FromImport/PolicyDigest and the approved Prepare,
  Validate, BuildPlan, ValidatePlan and CanRelease contracts. Context owns exact
  imported group/policy identities; malformed, duplicate, foreign and permanently
  released provenance cannot create permission. Initial singleton-v1→v2 import
  has its own exact founder/destination check. Data consumption and retained
  policy deficit are independent facts.
- Count-normalized owned analysis copies preserve identities, source/proven/native
  membership, selectors/types, pin/reservation rules, actual distinct voter domains,
  bounds and original policies. Preparation restores original desired policy and
  report targets. Public topology helpers remain strict; no public recovery context
  or skip flag was introduced. No last-voter removal or worsened assigned/eligible
  deficit is permitted by the exception. Native planner catch-up/live-quorum guards
  remain the existing implementation.
- Kept structural document validation strict, including immutable history. Cardinal
  deficit allowances belong only in placement/count/planning paths; applying a
  transition deficit comparison to old history would incorrectly reject valid
  earlier checkpoints after progressive repair.
- Admission remains learner-first. Normalized minimum reflects current assigned
  voters; normalized target follows eligible capacity. A second eligible node can
  be hosted/promoted while the original three-AZ target remains unchanged. Public
  default/custom repair signatures and the original documents given to callbacks
  remain unchanged; the existing built-in chooser already selects that promotion.
- Release requires proven original-policy satisfaction or normally valid explicit
  policy change/retirement; data import completion alone is insufficient. Released
  records cannot reauthorize a later deficit.

Routed call sites / Task9 handoff
- metaengine: RecoveryPlacementContext and scoped helpers; staged historical
  transition/tombstone validation; scoped placement approval; immutable plan shape;
  materialized/history structural checks; planning and repair query validation;
  repair rebase transitions/placement. Removed superseded strict helper wrappers.
- metaservice: declarative preparation; mutation structural/count checks; PlanFor
  placement; operation/repair plan construction. Placement explanations restore
  original required-voter targets and existing status reports retain original policy.
- internal scheduling: exact replicated policy/deficit revalidation before planning
  and again after policy callback/native recapture, without replacing existing
  votercheck, liveness, membership or CAS checks. Public built-in candidate regression
  proves promotion with only two eligible AZs under the retained three-AZ intent.
- PlanRead has an optional, owned Recovery provenance field, omitted for ordinary
  reads. recoveryPlacementState currently returns nil by design: Task9 installs
  the actual optional replicated state and durable release updates. Task8's call
  paths are wired now; no isolated adapter-only implementation is being claimed.

Verified
- make test: PASS, 2,061 tests/57 reports, all five modules, 10.477s.
  test-reports/unit/20261025T035424.443687Z-p607hg61/report.md.
- make lint: PASS all configured modules/build tags/tooling.
  /tmp/gquorum-task8-lint.log.
- Focused unit suites: metadata FSM/service, internal scheduler, public repair
  policy and placement all PASS. Same changed pure packages under -race PASS:
  placement1.296s/internal-scheduler1.294s/repair1.273s/FSM3.894s/service1.436s.
  /tmp/gquorum-task8-unit-race.log.
- Full owning non-race functionals PASS FSM5.253s/service16.737s/scheduler0.559s.
  Full root/FSM/service/scheduler functionals -race -p4 -parallel8 -count=1 PASS
  40.654s/26.498s/26.450s/2.103s.
  /tmp/gquorum-task8-function-race.log.
- New service functional check uses the real FSM document store with owned views:
  absent context cannot be forged through audit text, the extra voter-count gate
  stays strict, ordinary preparation is identical and source views remain unchanged.
- Pure cases cover exact initial import, singleton→two→three, native wait/promote
  steps and lost quorum, unchanged/unrelated deficient groups, nil legacy policy,
  policy edit/release, forged/released context, worsened deficit, pinned/learner-only
  voters, selector changes, duplicate AZs and last-voter removal. Inputs/original
  policies are preserved.
- make test-sim SIM_SEEDS=1-3: PASS21/21,71.495s.
  test-reports/sim/20261025T035613.995209Z-s73988rn/report.md.
- git diff --check PASS. Existing frozen fixtures/ordinary persisted encodings
  remain unchanged. No Docker image change or new broad process suite at this task.

Notes
- Ordinary founding regressions caught that an empty pre-founding approved reference
  must also supply the desired cluster identity as the placement base. Corrected
  the adapter to preserve that existing special case; full founding suites pass.
- A first normalization gave _meta a separate placement override, which is invalid;
  metadata changes only its private voter count in analysis. Original policy remains
  restored. Structural-history validation and count gates remain separate.
- Continue Task9; no new task section, public API or user decision was added. The
  next regular peer checkpoint remains after Task10.
