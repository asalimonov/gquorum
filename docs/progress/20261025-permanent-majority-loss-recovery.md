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
