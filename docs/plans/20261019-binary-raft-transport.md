# Implementation plan: binary Raft transport

**Plan:** `docs/plans/20261019-binary-raft-transport.md`
**Project source:** `.` (repository root; `../..` relative to this file)
**Date:** 2026-10-19
**Target architecture:** [docs/brainstorms/20261019-binary-raft-transport-target.md](../brainstorms/20261019-binary-raft-transport-target.md)
**Brainstorm log:** [docs/brainstorms/20261019-binary-raft-transport.md](../brainstorms/20261019-binary-raft-transport.md)
**Source baseline:** `631a606`, branch `feat/structural-refactorings-and-fuzzing`
**Status:** in progress; Tasks 1–2 complete; boundary regressions and benchmarks are next.

## Important notices

- The user's explicit limit is **5,000 changed lines per commit**. Count additions
  plus deletions, including generated Go, tests, schemas and documentation. This
  overrides the skill's default task-size preference where a coherent change
  needs more space. Aim below the limit; estimates below are not measured diffs.
- Each task is one independently buildable commit. Never commit broken generated
  interfaces or a half-switched sender/receiver. If a task exceeds the limit,
  split a complete preparatory or test portion and recheck both commit boundaries;
  do not omit generated files, tests or documentation from the count.
- The user confirmed **no backward compatibility is needed**. Final production
  Raft requests use binary version 2 exclusively. No Sonic, protocol negotiation,
  JSON fallback, application codec change or storage migration.
- Preserve unrelated work. At planning time, Dockerfile/Compose/cache README
  credential fixes are unstaged, and two profiler files are already staged.
  Record their hashes/diffs before execution; do not commit, reset or overwrite
  them as part of these tasks. Never use `git add .` or blanket `git commit -a`.
- `docs/plans` and `docs/brainstorms` are ignored ordinary directories in this
  checkout, not symlinks to separate Git repositories. `project.txt` is absent;
  the source is this repository. The skill's nested-documentation-repository
  branch/commit procedure does not apply. This planning session creates only
  this plan; it neither stages files nor creates a commit.
- Log deviations in **NOT PLANNED CHANGES**, unresolved target gaps in
  **NOT IMPLEMENTED GAPS**, and executed commands/results in **Progress log**.
- No new database tables, ORM models, persistent caches or migrations. Existing
  bbolt stores, WALs, journals, application payloads and frozen compatibility
  fixtures stay unchanged. The only cache removed is `Transport.unaryOnly`.
- Run the task's checks before committing. Investigate all observed failures,
  including apparently unrelated ones; do not skip, weaken or mark them passed.
  Fix in-scope regressions and separately record any pre-existing blocker and
  its resolution without mixing unrelated work into the optimization commits.

## Design and implementation clarifications

The target file is the design authority. Its diagram in **Objective and boundary**
and sections **Protocol schema**, **Exact routing and validation**, **Frame
planning and buffer ownership**, **Delivery flows**, and **Persistence and
compatibility** govern all tasks.

The public `load.Transport` and `Ingress` interfaces, constructors, options and
application composition stay unchanged. The adapter uses its existing gRPC and
protobuf dependencies plus native `raftpb.Message` methods. Node still performs
current exact-peer authorization; no transport cache substitutes for that check.

Concrete interpretations needed to implement the target consistently:

1. Add schema types first, while the existing RPCs keep serving the old implementation.
   Pure codec helpers land with their production callers to satisfy unused-code lint. Switch both RPCs, sender and receiver together
   in Task 2. This is development sequencing, not a production dual-protocol mode.
2. Header/structural/native encoding errors map to InvalidArgument, oversize
   input to ResourceExhausted, and authentication failures to Unauthenticated.
   Once a valid header attributes a delivery, native From/To or local-recipient
   mismatch remains a typed identity refusal, as required by the target failure
   table. In a stream that means StreamRefusal; unary uses the existing Reply.
   A stale but well-formed identity is revalidated by Node on every frame.
3. Stream header validation precedes admission so overload never produces an
   unattributable delivery response. Authenticate before native message decoding.
   Preserve context cancellation/deadline classification and release ownership
   on every failure. Do not map cancellation to malformed input.
4. Protobuf strings require UTF-8. Sender preflight validates all identity/group
   strings before network activity, alongside complete peer checks, so an outer
   protobuf serialization failure cannot occur after a prior frame was sent.
   Application bytes remain unrestricted. Receiver headers have already passed
   generated protobuf string decoding.
5. `maxRaftMessagesPerFrame = 4096` bounds native message expansion per frame;
   gRPC must still decode the bounded outer protobuf first. It is not a claim
   that all outer-decoder allocation is bounded by the decoded message count.
6. The count and byte limits split batches transparently; no per-batch rejection
   merely because a valid batch contains more than 4096 messages. No message or
   snapshot is split internally. A single oversize message remains Overloaded.
7. Preserve native protobuf semantics and opaque bytes, using semantic equality
   rather than JSON-specific nil/empty representation equality in adapted tests.
   Native v3.6.0 decoders copy Context, Entry.Data and Snapshot.Data; verify that
   ownership with tests instead of introducing pooling or zero-copy aliases.
8. Administrative JSON and unary Reply JSON remain intentional. Assertions that
   there is no JSON must inspect the Raft request/accepted-stream path, not ban
   JSON imports from the whole adapter.

These clarify error mapping and sequencing without changing target structures
or public behavior. No target-file edits are required by this plan.

## Task List

| Commit/task | Scope | Estimated additions + deletions |
| --- | --- | ---: |
| 1 | Add binary schema | 1,000–1,800 |
| 2 | Add codec/core units, switch both Raft RPCs and remove fallback | 900–1,800 |
| 3 | Expand regression coverage and codec benchmarks | 650–1,300 |
| 4 | Process/performance validation and architecture docs | 250–700 |
| 5 | Architectural delta verification and gap fixes | 100–500 expected; split fixes if needed |

Every commit has an independent hard ceiling of 5,000 lines. Generated code is
included; current adapter generated files total 583 lines before this change.

### Task 1: Add the binary frame schema

**Commit:** `transport/loadgrpc: add binary Raft frame schema`
**Files:** `transport/loadgrpc/internal/loadpb/load.proto`, generated `load.pb.go`.
**References:** target §Protocol schema and §Implementation map/generated files.

- [x] Add the exact RaftPeer and RaftFrame definitions from the target, retaining
  existing RPC input types until Task 2.
- [x] Regenerate with pinned make generate; verify other protocols unchanged.

#### Verification

- [x] Run make test, focused adapter functional tests and vet on schema-only state.
- [x] Regenerate byte-identically and verify the commit line limit.

### Task 2: Switch Raft delivery to binary frames end to end

**Commit:** `transport/loadgrpc: use binary frames for Raft delivery`
**Files:** UPDATED `load.proto`, generated `load.pb.go`/`load_grpc.pb.go`,
`client.go`, `server.go`, `stream.go`, `transport.go`, `wire_test.go`,
`stream_func_test.go`, `transport_func_test.go`.
**References:** target §Protocol schema RPC table; §Delivery flows (all);
§Exact routing and validation; §Implementation map (`client.go`, `server.go`,
`stream.go`, `transport.go`); §Key algorithms for send/receive; brainstorm
§Request and decisions (no backward compatibility).

- [x] Create the complete helper surface below. Move `validateBatch` rather
  than leave duplicate definitions; strengthen it to complete exact peers,
  matching cluster/group, valid UTF-8 strings, nonzero delivery/message list,
  and matching native From/To. Keep its old caller valid until Task 2.

  ```go
  const raftWireVersion uint32 = 2
  const maxRaftMessagesPerFrame = 4096
  type raftFrameRange struct { start, end int }
  func encodeRaftPeer(load.Peer) *loadpb.RaftPeer
  func decodeRaftPeer(*loadpb.RaftPeer) (load.Peer, error)
  func validateBatch(load.RaftBatch) error
  func planRaftFrames(load.RaftBatch, int) ([]raftFrameRange, error)
  func encodeRaftFrames(load.RaftBatch, int) ([]*loadpb.RaftFrame, error)
  func decodeRaftHeader(*loadpb.RaftFrame, int) (load.RaftBatch, error)
  func decodeRaftMessages(context.Context, *loadpb.RaftFrame, *load.RaftBatch) error
  ```

- [x] Plan contiguous ranges using header `proto.Size`, native `Message.Size`,
  and `protowire.SizeTag(5) + protowire.SizeBytes(n)`. Check limit arithmetic
  without overflow; reject an individually oversize message before encoding or
  sending anything. Enforce both frame byte and message-count limits.
- [x] Encode each native message once after full planning. Prepare every frame
  and verify final sizes before return. Do not mutate input messages, interpret
  application bytes, introduce pooled buffers, or use JSON inside these helpers.
- [x] Separate bounded header conversion from native decoding. Require version
  2; reject nil/missing peers, zero delivery IDs, bad group IDs, empty/oversize
  arrays and corrupt native messages. Decode into fresh locals and assign
  `batch.Messages` only after the entire frame is valid; check cancellation.
- [x] Add parallel isolated unit tests for peer round trips, native message/data
  round trips, message order, frame splitting, oversized later messages and
  malformed headers/bytes. Use `//go:build !functional`; do not start services.
- [x] Add concise `// TODO(not planned): ...` comments at the relevant binary
  codec boundary for snapshot chunking and buffer pooling, documenting existing
  refusal/ownership behavior without introducing stubs or hot-path warnings.


- [x] Before changing runtime behavior, retain three fresh JSON baseline captures
  using the commands below. Record source revision, dirty diff, binary hashes,
  actual leader, host conditions and phase timing. Task 1 is additive; label this
  baseline accurately. Keep existing credential fixes identical across A/B.
- [x] Replace the two RPC request types in place and regenerate with the pinned
  command. Perform all callers/handlers/test adaptations in this same commit:

  ```proto
  rpc SendRaft(RaftFrame) returns (Reply);
  rpc StreamRaft(stream RaftFrame) returns (stream StreamRefusal);
  ```

- [x] Change `sendBatchFrames` to call `encodeRaftFrames`. Implement this target
  signature delta; keep public `SendRaft`, `sendOutcome.report`, routes, IDs and
  snapshot completion classification unchanged:

  ```go
  func (t *Transport) sendFrames(context.Context, load.RaftBatch,
      []*loadpb.RaftFrame, sendOutcome) error
  func (t *Transport) sendUnary(context.Context, topology.Identity,
      []*loadpb.RaftFrame) error
  func (t *Transport) sendStream(context.Context, load.RaftBatch,
      []*loadpb.RaftFrame) error
  func (s *raftStream) sendFramesLocked(context.Context, uint64,
      []*loadpb.RaftFrame) error
  func (s *serverAdapter) SendRaft(context.Context,
      *loadpb.RaftFrame) (*loadpb.Reply, error)
  func (s *serverAdapter) receiveStreamed(context.Context,
      *loadpb.RaftFrame) (refused, terminal error)
  ```

- [x] Route every ordinary batch through the exact-route stream; route every
  frame of a snapshot-bearing batch through ordered unary calls. Continue
  using existing bounded reply decoding for unary responses. Do not acknowledge
  a snapshot early or add command retries.
- [x] Remove trial JSON splitting (`splitBatch`, `splitValidBatch`, `batchChunk`,
  `batchSearch`, `findBatchChunk`, `encodeBatchChunk`, `batchSearch.try`). Update
  their unit callers to the binary codec. Leave administrative envelope helpers.
- [x] Implement binary unary ingress with `begin`/Release, bounded header decode,
  authentication, exact local recipient check, native decode and Node ingress.
  Keep malformed input, identity refusals and cancellation distinct. Preserve
  existing Reply error details and success body.
- [x] Implement stream receipt using the same codec validation order. Return
  terminal status for unattributable/malformed frames and invalid credentials;
  report attributable identity/Node refusals using original delivery IDs. Check
  refusal `proto.Size` against the configured bound before sending; an oversized
  refusal terminates with ResourceExhausted rather than bypassing byte limits.
- [x] Preserve serialized Send, reusable stream routing, cancellation linkage,
  last-delivery reporting, address-change replacement, idle retirement,
  references and joined shutdown. Do not cache authorization for stream lifetime.
- [x] Remove `unaryOnly`, its initialization/lookups/writes, `unaryRetry` and
  Unimplemented fallback. `sendStream` no longer returns a fallback boolean;
  `raftStream` cannot return nil/nil to signal an older peer. Missing methods
  remain failures subject to the existing unreachable/report semantics.
- [x] Adapt existing protocol functional tests and splitting assertions to native
  semantic equality. Replace `olderServer`/`TestOlderRecipientReceivesUnaryCalls`
  with a missing-stream-method regression: observe failure/report, zero unary
  fallback calls, and no goroutine/ref leak. Allow the existing asynchronous
  error timing instead of requiring immediate SendRaft failure.
- [x] Confirm ReadReplica, ApplyAction and InstallFence tests retain JSON strict
  envelope checks and typed safety/CAS error preservation. Their RPC signatures
  must not change.

#### Verification

- [x] Run `make test`, focused adapter functional tests and the same suite with
  `-race`, focused unit race tests, and `make lint`.
- [x] Recheck generated reproducibility and the independent commit line budget.
  No temporary unimplemented binary handlers or old-version adapters may remain.

### Task 3: Cover boundary failures and benchmark the complete codec

**Commit:** `transport/loadgrpc: verify binary frame safety and cost`
**Files:** UPDATED `raft_wire_test.go`, `stream_func_test.go`,
`transport_func_test.go`; CREATED `raft_wire_bench_test.go`; minimal codec fixes
only if the new regressions reveal an implementation defect.
**References:** target §Exact routing and validation; §Frame planning and buffer
ownership; §Delivery flows/Failures; §Verification and performance experiment;
§Persistence and compatibility.

- [ ] Extend table-driven unit coverage to every native Raft message kind the
  engine uses; include entry data, ConfState, snapshots, contexts, large numeric
  IDs and arbitrary/non-UTF-8 application bytes. Assert exact peer preservation
  and distinguish meta MemberID from ReplicaID.
- [ ] Test exact byte-limit and limit+1 behavior, protobuf length-varint
  transitions, 4096 versus 4097 messages (split on send, reject an excessive
  single received frame), and oversized later messages with no returned frames.
  Keep fixtures small except the deliberate count-bound case.
- [ ] Test nil peers, incomplete fields, invalid UTF-8 identity strings on send,
  version 0/1/unknown, empty message encodings, corrupt later native messages,
  mismatched native endpoints, and already-canceled decode contexts. Assert no
  partial `batch.Messages` publication when decoding fails.
- [ ] Verify ownership: mutate original messages after encoding and raw frame
  bytes after decoding; decoded commands/contexts/snapshots must not change.
  Avoid reflect.DeepEqual assertions that mistake nil/empty normalization for
  lost data. Do not require unknown native fields unsupported by the pinned
  codec to round-trip; this is not a mixed-Raft-version feature.
- [ ] Add functional protocol-double checks for authentication before native
  decoding/Node ingress, current recipient change, invalid input yielding zero
  ingress calls, and a healthy stream surviving an attributable refusal.
  Keep real authorization/quorum behavior in existing Node/example coverage.
- [ ] Retain and strengthen ordered stream reuse, cancellation while blocked,
  ignored-cancellation Close ownership, and partial snapshot failure assertions.
  Ensure reduced binary frame sizes do not accidentally turn the old multi-frame
  snapshot fixture into a single-frame case; size its messages deliberately.
- [ ] Test oversized outgoing streaming refusal handling as changed in Task 2,
  preserving shutdown/join cleanup and original delivery identity. Malformed
  native-frame regressions must cover both streaming and unary reception.
- [ ] Add `BenchmarkRaftFrameCodecs`, using test-only legacy JSON framing against
  the binary codec for heartbeat, app response, ReadIndex, append commands,
  multiple messages and snapshot. Measure encode and decode separately with
  the outer protobuf layer included, plus full split/encode path where relevant.
  Test-only JSON helpers must not become a production fallback.
- [ ] Report ns/op, B/op, allocs/op, encoded bytes and frames per case. Use the
  same input and semantic validation for both codecs; do not substitute the old
  cache-command/Sonic benchmark for this transport benchmark. Save repeated raw
  results under `test-reports/performance/binary-raft-codec/`.

#### Verification

- [ ] Run `make test`, focused adapter units/functional tests with race, and vet
  for both tags. Run repeated benchmarks with the exact command below.
- [ ] If benchmark results regress, inspect actual frame/codec costs and fix or
  record a blocker; do not proceed on an unmeasured performance claim.
- [ ] Check the commit's changed-line total, including any defect fixes.

### Task 4: Verify real applications and document measured behavior

**Commit:** `docs: document binary Raft transport and validation`
**Files:** UPDATED `transport/loadgrpc/README.md`, `docs/architecture/raft.md`;
CREATED `docs/testing/binary-raft-transport.md`; example test files only if a
specific missing assertion is necessary. Raw artifacts stay under test-reports.
**References:** target §Objective and boundary/diagram; §Delivery flows;
§Persistence and compatibility; §Verification and performance experiment;
§NOT CONSIDERED & TODO.

- [ ] Run `make test-cache-example` and `make test-scheduler-example` once the
  implementation/regression set is complete. Both build their real applications
  using loadgrpc. Verify cache retained restart and scheduler lifecycle behavior,
  private image/volume cleanup, and no alteration of the user's running cluster.
- [ ] Do not use the separate tests/cluster/testapp transport as sole evidence
  for loadgrpc. No new multi-node fixture belongs in root functional tests;
  extend the existing three-node Compose cases only if coverage is missing.
- [ ] Capture three binary runs with the same settings as the JSON baseline and
  retain source/build provenance, logs and profiles. Compare actual leaders,
  total cluster CPU/op, per-node CPU/op, allocation estimates, completed operation
  rates, read/write average latency and errors. Report run-to-run variation.
- [ ] Align workload intervals with CPU capture timestamps. Do not divide a
  30-second CPU sample by lifetime operation counts. If needed, add one separate
  measurement run with counters at exact start/end boundaries; do not pretend
  5-second progress logs provide exact boundary counts.
- [ ] Check accepted ordinary-stream stacks contain no `loadgrpc.decodeJSON`.
  Keep syscall tracing in a separate phase and exclude its distorted workload
  rates from CPU-window comparisons. Do not compare heap lifetime totals as a
  steady allocation rate without matching durations/startup or profile deltas.
- [ ] If results are noisy, rerun baseline and binary serially on isolated
  checkouts/images containing the same prerequisite credential fixes. Do not
  switch branches over unrelated dirty work, reuse a tag during another run,
  or remove the user's containers/volumes. Record why a rerun was necessary.
- [ ] Document the exact version-2 schema behavior, public API genericity,
  coordinated upgrade, absence of JSON Raft input/fallback, preserved controls
  and reply codecs, frame bounds, snapshot limitations, byte ownership and
  shutdown sequencing. Add scoped TODO comments for deferred work as listed
  below; no runtime warning on normal traffic.
- [ ] Write the validation record with commands actually executed, report paths,
  failure/retry history and measured results/limitations. Do not invent latency
  percentiles from averages or translate codec speedup directly into throughput.
- [ ] Run final `make test`, `make lint` and generated diff checks if runtime or
  test changes were needed in this task; otherwise reuse passing checks from
  unchanged code and perform documentation/link/diff checks.

#### Verification

- [ ] Confirm both example reports passed and their owned resources were cleaned.
- [ ] Confirm the performance comparison is reproducible, excludes tracing
  overhead and does not claim improvements unsupported by repeated measurements.
- [ ] Verify document paths/API names and the 5,000-line commit limit.

### Task 5: Verify the architectural delta and close implementation gaps

**Commit:** `docs: record binary Raft architecture verification` (include bounded
gap fixes if required; otherwise documentation-only).
**Files:** every implementation/test/documentation file touched above; record
verification in `docs/testing/binary-raft-transport.md` and this plan.
**References:** target architecture **all sections**, especially §Implementation
map and method signatures, §Persistence and compatibility, and §Key algorithms.

- [ ] Verify protocol component: every RaftPeer/RaftFrame field number/type,
  version rule, two changed RPC inputs, generated artifacts and unchanged
  administrative/reply definitions match the target.
- [ ] Verify codec component: all seven target helpers, range type and two
  constants exist; size planning, complete preflight, limits, owned native
  decoding and error classifications match the target and clarifications.
- [ ] Verify client/stream component: every signature in Task 2 matches; streaming
  ordinary traffic, unary snapshots, delivery-report identity, cancellation,
  reuse, rerouting, idle retirement and joining remain implemented. Confirm all
  JSON trial splitters and `unaryOnly` downgrade paths are gone.
- [ ] Verify server component: both handlers authenticate before native decode,
  reject malformed inputs before Node, and invoke current exact authorization
  per frame. Confirm control handlers preserve their original behavior.
- [ ] Verify lifecycle/configuration component: no new public options, callbacks,
  default routes or lifetime owners; New remains I/O-free and Close retains
  accepted work. No unplanned pooling, header caching or authorization cache.
- [ ] Verify persistence/application component: no schema/DDL/migration delta,
  public API changes or application codec changes; frozen fixture hashes and
  storage versions are untouched. No regeneration of old fixtures to pass tests.
- [ ] Verify coverage/evidence component: unit/functional suite separation,
  race coverage, real cache/scheduler tests, codec benchmarks and repeated
  profiling all address the target. Deferred scenarios have their documented
  existing behavior and narrowly placed TODO comments.
- [ ] Record every deviation in NOT IMPLEMENTED GAPS, then resolve each against
  the target/plan, rerunning affected checks. Do not mark completion while a
  mandatory component is missing. If a gap cannot be corrected, leave it `[_]`,
  explain the blocker, and report it rather than silently narrowing the target.
- [ ] Verify actual wiring LAST: trace cache and scheduler runtime construction
  through New/Bind/Register, generated SendRaft/StreamRaft dispatch, binary
  sender/receiver helpers, Node.ReceiveLoadRaft and delivery reporting. Every
  new production component must be reachable from real entry points, not merely
  present in tests; there must be no disabled branch, temporary stub or old
  active Raft JSON path.

#### Verification

Record the requirement-to-file/test/evidence mapping. Run the relevant validation
commands for any newly fixed gap; otherwise do not repeat expensive checks on
unchanged code. Audit each final commit's actual addition/deletion total. Finish
only when every mandatory requirement is implemented and documented, with the
last wiring check satisfied.

## Validation Commands

Run from the repository root. These are execution instructions, not checks run
during planning. Use the root Makefile for all-module and real-example coverage.

```sh
# Pinned generation, at schema changes only.
make generate

# Fast all-module validation at each implementation commit boundary.
make test

# Focused codec/unit checks; these must not start services.
go test -count=1 ./transport/loadgrpc/...
go test -race -count=1 ./transport/loadgrpc/...
go vet ./transport/loadgrpc/...

# Protocol doubles and joined concurrency checks, not multi-node clusters.
go test -tags=functional -count=1 -timeout=2m ./transport/loadgrpc/...
go test -race -tags=functional -count=1 -timeout=3m ./transport/loadgrpc/...
go vet -tags=functional ./transport/loadgrpc/...

make lint

# End-of-implementation process coverage on actual users of this adapter.
make test-cache-example
make test-scheduler-example

git diff --check
```

For generated reproducibility before committing, hash the generated adapter
files, rerun `make generate`, and require identical hashes. Inspect
`git diff -- internal/metapb tests/cluster/testapp/loadpb` for unintended changes.
`make proto-check` compares generated files with the Git state; intended
uncommitted generated changes will therefore fail it. Run it after the relevant
commit (or after explicit review/staging of those generated files); do not stage
unrelated work to make it pass.

```sh
mkdir -p test-reports/performance/binary-raft-codec
go test ./transport/loadgrpc -run '^$' -bench '^BenchmarkRaftFrameCodecs$' \
  -benchmem -benchtime=500ms -count=5 -cpu=1 \
  > test-reports/performance/binary-raft-codec/benchmarks.txt
```

Run before the Task 2 switch with a fresh output path for each of three captures:

```sh
make profile-cache PROFILE_ARGS='--workers 1 --keys 615 --gomaxprocs 1 --seconds 30 --warmup 5 --syscall-seconds 10 --output test-reports/performance/binary-raft-before-1'
```

Run in Task 4 for three corresponding binary captures:

```sh
make profile-cache PROFILE_ARGS='--workers 1 --keys 615 --gomaxprocs 1 --seconds 30 --warmup 5 --syscall-seconds 10 --output test-reports/performance/binary-raft-after-1'
```

Change `-1` to `-2` and `-3`; never overwrite retained evidence. Omitting `--fsync`
matches the original capture's disabled load fsync. Use the same host/resource
settings for both variants. The existing historical profile
`test-reports/performance/20261018T201051Z-e899dd0a` supports diagnosis but does
not replace contemporaneous comparison runs. No general chaos suite is planned.

Commit line accounting: after explicitly staging only this task's reviewed
paths, inspect `git diff --cached --name-status` and `git diff --cached --numstat`.
Do not commit if the index still contains the pre-existing unrelated profiler
changes. Use an isolated commit/index workflow that preserves their original
staged state; never reset or silently include them. Sum both numeric columns
for **every file in the prospective commit** and require <=5000. Binary artifacts
do not belong in these commits. After committing, verify with:

```sh
git diff-tree --no-commit-id --numstat -r HEAD
```

Generated code and documentation count. If the audit exceeds 5000, do not create
the commit; split the task at a tested boundary and record the change here.

An execution-time pre-commit gate for the selected index (including an alternate
index selected through `GIT_INDEX_FILE`) is:

```sh
python3 - <<'PY'
import subprocess
rows = subprocess.check_output(['git', 'diff', '--cached', '--numstat'], text=True).splitlines()
total = 0
for row in rows:
    added, removed, path = row.split('\t', 2)
    if not added.isdigit() or not removed.isdigit():
        raise SystemExit(f'Unexpected binary artifact: {path}')
    total += int(added) + int(removed)
print(f'Prospective commit: {total} added/deleted lines')
if total > 5000:
    raise SystemExit('Commit exceeds 5000 changed lines; split at a passing boundary')
PY
```

The gate checks quantity, not authorization of paths; the explicit path review
and preservation of unrelated staged content remain required. Use named paths
and temporary index/worktree resources when needed, never a broad index reset.

## NOT CONSIDERED & TODOs

These exclusions were already established in the target architecture. Preserve
their current behavior; no new NotImplemented error or WARN logging is needed
for normal, supported operations.

| Classification | Excluded work | Planned note/default |
| --- | --- | --- |
| postponed | Administrative JSON RPC and unary reply conversion | Keep codecs; `// TODO(not planned): Convert administrative and unary reply payloads in a separate change.` near control codec boundary |
| postponed | Application commands and persisted snapshot encodings | Keep opaque bytes; comment at native codec boundary that application format is caller-owned |
| potential-improvement | Buffer pooling and zero-copy | Fresh owned buffers; one TODO at binary codec, no borrowed engine data |
| potential-improvement | Stream-header compression and cross-batch coalescing | Per-frame exact peers/current ordering; one TODO near framing/send boundary |
| potential-improvement | Routing indexes and lifecycle allocation reduction | Current per-frame authorization and begin/Release; document exclusion, no edits to metaengine just to add a comment |
| postponed | Oversized snapshot chunking | Existing Overloaded refusal; TODO at single-message size check |
| potential-improvement | Broader queue/memory/backpressure accounting | Existing bounded queues/calls plus frame count limit; TODO beside count-limit rationale |

Comments should describe the default behavior and be grouped where several
exclusions share a boundary; do not repeat them on every method. Mixed-version
compatibility is deliberately unsupported by user decision, not a missing TODO.

## NOT IMPLEMENTED GAPS

No discovered design gap at planning completion. This does **not** claim that
implementation exists: all tasks remain unchecked. During execution add:

- `[_] Component — target reference — observed gap — attempted resolution — blocker`.
- Replace with `[x]` only after the fix and relevant verification pass; an
  approved deferral must quote the user's decision and explain its scope.

## NOT PLANNED CHANGES

2026-10-19: Pre-commit production lint rejects unused pure codec helpers even
when unit tests exercise them. Task 1 is schema-only; its codec/test work moves
into Task 2 so each committed production build passes lint without suppression.
Target design and the 5,000-line cap are unchanged.

Record file, reason, target impact and validation for every
execution deviation. Do not absorb unrelated credential, profiler or Sonic work.

## Progress log

| Date | Step | Result |
| --- | --- | --- |
| 2026-10-19 | Planning | Read target/log, inspected codec/RPC/lifecycle/tests/generation/profiling entry points; designed five buildable commits with <=5000 changed lines each. No runtime code changed or tests claimed. |
