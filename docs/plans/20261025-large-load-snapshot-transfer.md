# Implementation plan: Large load-snapshot transfer

**Plan:** `docs/plans/20261025-large-load-snapshot-transfer.md`
**Date:** 2026-10-25
**Project source:** `../..` relative to this plan (repository root `.`)
**Branch:** `feat/large-snapshots-transfer`
**Source checkout inspected:** `646e9c1` (implementation baseline `14a54c2`, transport-document correction `2ba951a`, current lint/testing updates in `646e9c1`)
**Target architecture SHA256:** `2be53d3cfda9357b564e1b1e3be73cb16fba38ac7f026eef8ad6f49e37c1962f` (current target, including Task 7 transport-cap refinement; original reviewed digest retained in review history)
**Target architecture:** [20261025-large-load-snapshot-transfer-target.md](../brainstorms/20261025-large-load-snapshot-transfer-target.md)
**Full brainstorm log:** [20261025-large-load-snapshot-transfer.md](../brainstorms/20261025-large-load-snapshot-transfer.md)
**Review history:** [feature review thread](../review/feat_large-snapshots-transfer.md)
**Status:** implementation in progress under the user's subsequent explicit goal. Task completion and verification are recorded below and in the execution log; the earlier plan-review provider limit remains historical.

## Execution contract and current-state reconciliation

The reviewed target is the design authority. This plan sequences its interfaces, algorithms, schema and lifecycle; it does not introduce a different architecture. All implementation checkboxes start unchecked. This planning session only prepares/reviews/fixes this document, then stops for the user's instructions.

Preserve the user's constraints:

- All new mechanism code belongs in `internal/loadsnapshot`, public integration contracts in `load/snapshot`; one narrow managed adapter belongs in `internal/managed/snapshot_ports.go`. Modify existing root files only for composition/options/accessor. No new root source or root test files.
- Snapshot traffic uses a separate bounded gRPC connection to the **existing load listener/address/port**. Ordinary Raft/heartbeats have independent workers, queues and deadlines. Keep the existing separate _meta listener; no third listener or port.
- Full-buffer application/storage APIs and persisted encodings stay unchanged. In-memory receive staging only, behind the specified interface. Exactly one planned code TODO covers a future group-scoped disk backend; the user's instruction overrides generic requests to add a TODO for every excluded feature.
- Construction-time options: 256 MiB default encoded snapshot allowance; derived byte budgets; one incoming/one outgoing transfer per Node; 30-minute total and 1-minute verified-progress defaults; 1 MiB chunks/eight outstanding chunks. All defined policy fields remain configurable as specified. No hard-coded 2 GB production maximum.
- Resume only an exact retained transfer in unchanged processes/replica runs. Stop/restart/replacement requires a fresh transfer. Engine-only receiver run churn does not renew the source-retained repair episode.
- Large qualification generates payloads at runtime, uses the explicit `snapshotlarge` cluster tag, tops out at 2 GB encoded, and never commits payloads. Default test targets never generate those large fixtures.
- Keep all immutable baseline fixtures unchanged. Do not add persistent transfer tables, files, caches, migrations, app configuration loading or storage-root choices.

Current source differs from historical wording in two operational details. `make lint` now covers pending Go files only; repository-wide final lint is **`make lint-full`**. Current AGENTS reserves full `make test-race`, full lint, Docker and broad simulation/performance runs for the final checkpoint. The current uncommitted `docs/architecture/design-constraints.md` reinforces memory-only receiving and caller-owned service discovery; preserve it and other unrelated work.

`project.txt` and a nested documentation Git repository are absent; source is this worktree and architecture lives in `docs/architecture`. `docs/plans`/`docs/brainstorms` are ignored by the project Git repository. The planning deliverable may be explicitly force-added alone on the existing branch; do not initialize another repository or stage unrelated files. The skill-requested documentation commit uses the feature name and `(go-brainstorm)` suffix.

## User-directed implementation reviews

After Task 8 is verified and committed, request an analysis-only implementation review from Paseo agent `43cff8c6-3dab-4f3f-a0eb-d4393c777725` using paseo-committee. Include this plan, the target architecture and full brainstorm paths. Focus on reliability/ownership, compatibility with the pinned Go runtime and libraries, and performance. The user selected this existing colleague; do not launch additional reviewers.

Repeat the review after every six further implementation commits, including review-fix commits. Record the pinned reviewed commit and the next checkpoint in the progress/review log. Review a fixed commit range while later work proceeds; distinguish intentionally deferred Tasks 9–20 from defects in completed work. Verify and fix actionable findings before production activation. Do not poll or interrupt the reviewer; await Paseo's finish notification.

## Dependency strategy and buildable checkpoints

The constructor capability requirement, engine `SnapshotJobTransport` selection and rejection of old ordinary MsgSnap ingress form one **activation boundary in Task 15**. Do not enforce any of these early. Existing applications/tests must remain operational while the new mechanism is being built.

- Tasks 1–3 add contracts, pure configuration/facade wiring and additive wire schema.
- Tasks 4–5 implement engine ownership/report primitives behind unselected integration seams.
- Tasks 6–9 implement coordinator/staging/receive/send logic against those seams.
- Tasks 10–11 implement server/client and prove the gRPC teardown assumption before activation.
- Tasks 12–14 wire managed ownership, migrate every adapter/fixture, and add repair evidence.
- Task 15 atomically selects/enforces the new path, removes temporary unavailable bridges and closes legacy ingress bypasses.
- Tasks 16–19 add fixture/process/large qualification cases and align documentation.
- Task 20 runs final verification and audits the full architectural delta, ending with actual production wiring.

Temporary internal declarations may return a definite not-ready/unsupported error before their owning task is implemented, but they must be unreachable from the selected production path and must never claim successful delivery. Record each bridge's removal task in implementation notes. No public rollout flag, hidden fallback or compatibility mode survives Task 15.

Every task is intended to fit approximately 1,500 non-generated changed Go/test lines; generated protocol output is the explicit exception. If measured changes exceed that boundary, split the task into smaller independently green checkpoints while preserving dependencies, references and the activation boundary. Do not silently reduce coverage.

**Per-task verification rule:** for code-changing tasks, format changed Go files and run `make test` across all five application modules, focused owning-package vet, and the focused functional/race checks listed for the task. All executed checks must pass, including failures outside the immediate edited files. Fix necessary regressions without discarding unrelated work; unresolved failures block task completion and go in NOT IMPLEMENTED GAPS. Do not run full Docker/full lint/full unit-race after every task. Documentation-only tasks validate paths/names/commands/diffs without unrelated test execution. Functional tests remain local single-participant/protocol doubles with t.Parallel and joined fixtures; multi-node behavior belongs to Docker.

## Private-port realization and validation-name clarification

Target §9.4 requires managed ports to forward exact-attempt worker progress into `Group.NoteSnapshotProgress`, while its short §9.2 Ports declaration omits the callback field. Make that existing obligation concrete with `Ports.NoteSnapshotProgress func(Owner, raftengine.SnapshotAttempt, metrics.SnapshotCatchUp)` (Appendix C). Coordinator validates/stamps the update, this port resolves/checks the exact source owner and forwards it, and the engine owns episode state. This adds no public API or new policy; it closes a private wiring shorthand for the implementer. The colleague should verify this elaboration against the target intent.

The target's historical final `make lint` command means full validation; on the inspected checkout its actual command is `make lint-full`. Do not change the target design merely to retain a stale target name.

## Review checkpoint after Task 8

Review baseline: `251a594`; findings in review Thread 3. The next review is due after six subsequent implementation commits, including these corrections.

- [x] P1-1: gate consumption of the bounded owned-ingress queue at a safe Ready boundary; retain defensive handler checks and verify queued payload ownership through write/timer stalls.
- [x] P1-2: preserve immediate legacy `_meta`/standalone snapshot failure reports; retain managed exact-attempt backoff and non-lossy mailbox.
- [x] P2-1: advance snapshot publication on both recovery-checkpoint publication paths and require a nonzero digest generation only for bulk jobs.
- [x] P2-4: use the write job's exact ingress ticket during Restore instead of leaving the field write-only.
- [ ] P2-2: remove idle voter-map allocation and avoid repeated report-deadline scans/timer resets.
- [ ] P2-3a: reduce duplicate-chunk allocation, redundant contexts and per-chunk wake-channel allocation.
- [ ] P2-3b: Task 12 must implement/verify constant-time Authorize through exact routing indexes.
- [ ] P2-5: preserve a transfer's observation ID when moving from active state into completed history.

## Task index

| Task | Goal | Prerequisites | Approximate non-generated LOC |
|---|---|---|---|
| 1 | Public transfer contracts, options and classified errors | — | 800–1,200 |
| 2 | Pure Node facade and configuration composition | 1 | 600–1,000 |
| 3 | Snapshot wire schema, codecs and limits | 1–2 | 900–1,300 + generated |
| 4 | Owned native ingress, isolated preflight and Ready classification | 1–3 | 1,100–1,500 |
| 5 | Outbound offers, leadership epochs and non-lossy report mailbox | 4 | 1,100–1,500 |
| 6 | Coordinator state, budgets and in-memory staging | 1–5 | 1,000–1,400 |
| 7 | Authenticated receive/resume and chunk integrity | 6 | 1,000–1,400 |
| 8 | Finalization, diagnostic cache and joined lifecycle | 4,7 | 1,000–1,400 |
| 9 | Sender pins, digest cache and progress callbacks | 5,8 | 900–1,300 |
| 10 | gRPC receiving, quotas and teardown race gate | 3,7–8 | 1,100–1,500 |
| 11 | gRPC sending, bulk connections and bounded resume | 9–10 | 1,100–1,500 |
| 12 | Managed ports and Node/Host lifetime integration | 8–11 | 900–1,300 |
| 13 | Fixture and custom-adapter migration before enforcement | 12 | 1,000–1,500 |
| 14 | Bounded repair protection and observer integration | 5,9,12–13 | 1,100–1,500 |
| 15 | Atomic production activation and compatibility enforcement | 1–14 | 700–1,100 |
| 16 | Synthetic snapshot fixture and phase controls | 15 | 800–1,200 |
| 17 | Ordinary process regressions and authenticated fault controls | 16 | 1,000–1,500 |
| 18 | Generated, opt-in qualification through 2 GB | 17 | 800–1,200 |
| 19 | Current architecture/API/testing documentation | 15–18 | Mostly documentation |
| 20 | Final verification and architectural delta audit | All previous | Verification + gap fixes |

## Task list

### Task 1: Add public transfer contracts, options and classified errors

**Goal:** define the target integration surface without changing existing runtime selection.
**Files:** create `load/snapshot/{options,protocol,service,status,error}.go` and owning unit tests.
**References:** target [§3 configuration](../brainstorms/20261025-large-load-snapshot-transfer-target.md#3-configuration-admission-and-bounded-resources), [§4 integration](../brainstorms/20261025-large-load-snapshot-transfer-target.md#4-public-integration-contracts), §9.1 and §12 Options.Resolve. Exact target states are reproduced in Appendix A.

- [x] Implement `snapshot.Options.Resolve/Validate`, including zero defaults, negative count/duration refusal, overflow-safe products and deadline sums, platform allocation representability, byte budgets accommodating a maximum snapshot, and ProgressTimeout <= TotalTimeout. No I/O/callbacks/goroutines or maximum-sized allocation during validation.
- [x] Add Key, Manifest, Session, BeginRequest, Chunk, Ack, Send, Progress, DeliveryState, ReceiverOutcome, Transport and borrowed Service exactly as Appendix A/target §4. Include SessionDone and Send.ReportProgress; no service Start/Stop or application export/import API.
- [x] Add Query/Page/Status with bounded observation semantics, and TransferError with the target Reason/Size/Limit/fault mapping. Permanent size/frame/capability refusal must differ from transient capacity/group_busy. NotNeeded never means delivered payload or installed state.
- [x] Keep imports acyclic: this child package may import `load.Peer`; `load` must never import `load/snapshot`. Use 64-bit totals/offsets and immutable copied small metadata.
- [x] Add parallel units for zero/negative/overflow settings, budgets, >2 GB configurations without allocating payloads, exact fault classification, and zero-value conservative status/error behavior.

#### Verification

- [x] `make test`; `go vet ./load/snapshot`. Verify all five modules still compile with their existing transports.
- [x] Confirm no root file and no persistent or baseline encoding changed; no constructor capability enforcement yet.

### Task 2: Compose the pure snapshot facade into existing Node files

**Goal:** expose the stable borrowed service and validated settings without activating incomplete transport work.
**Files:** existing `node.go`, `node_options.go`, `node_load.go`; create `internal/loadsnapshot/service.go` and minimal package declarations needed by it; update existing root tests/Node mocks, not new root test files.
**References:** target [§4.2 facade](../brainstorms/20261025-large-load-snapshot-transfer-target.md#42-package-service-minimal-existing-root-wiring), §6.2, §8.1, §9.1 and §12 NewService.

- [x] Add `LoadOptions.SnapshotTransfer snapshot.Options` and `Node.LoadSnapshots() snapshot.Service`. NewNode resolves/copies policy and constructs `NewService(options, Resolver)` without invoking the resolver. For configured managed loads, Options works before Start and ingress returns ReplicaNotReady until a real coordinator exists. Meta-only Nodes retain no snapshot coordinator/service; their accessor returns nil and loadgrpc.Bind refuses nil support, consistent with target §9.4.
- [x] Implement Service Options/Begin/Write/Finish/Detach/Abort/SessionDone/List delegation and the observation-only last-coordinator fallback. No fallback is allowed for mutations. Unknown/stale SessionDone is closed; pre-host List is empty.
- [x] Keep current-root edits limited to existing composition files. Define internal Resolver/coordinator seam from target §4.2, but leave it unattached until Task 12; temporary errors cannot become successful API responses.
- [x] Update Node interface mocks in all modules as necessary. Add construction tests showing caller options mutation is isolated and no transport, resolver, factory or filesystem callbacks occur.
- [x] Implement a pure capability-check helper against snapshot.Transport, but **do not invoke its rejecting path from NewNode until Task 15**; existing adapters migrate in Task 13. Record this temporary boundary explicitly.

#### Verification

- [x] `make test`; `go vet ./internal/loadsnapshot .`; focused existing Node-construction tests.
- [x] Verify no resource acquisition before Start and no new independent service lifetime; existing small snapshot delivery still follows the old path.

### Task 3: Add the versioned snapshot RPC and bounded wire codecs

**Goal:** make the target protocol available additively while ordinary RaftFrame v2 remains unchanged.
**Files:** `transport/loadgrpc/internal/loadpb/load.proto` and generated files; create `transport/loadgrpc/snapshot_wire.go` and owning unit tests.
**References:** target [§5 wire/resume](../brainstorms/20261025-large-load-snapshot-transfer-target.md#5-wire-protocol-and-resume-semantics), §4.1, §9.4 and §10 SNAP-04/05/11. Exact proto field numbering remains the target §5 block, not an independently invented schema.

- [x] Add SnapshotOpen/Data/Finish/Abort/Frame/Failure/Ack and bidirectional TransferSnapshot. Include relative timing fields and ReceiverRunID; retain all ordinary/admin service schemas unchanged.
- [x] Implement encodeSnapshotOpen, decodeSnapshotOpen, encode/decodeSnapshotAck and snapshotChunkCapacity with checked protobuf overhead, bounded 64 KiB native header and individual MaxMessageBytes. Encode no complete application snapshot as one new native/wire message.
- [x] Enforce positive message term/index, `0 < SnapshotTerm <= MessageTerm`, exact endpoints, MsgSnap type, nil native-header Data and no Entries/Responses at this boundary. Direct service/native validation later must repeat safety checks.
- [x] Implement explicit unknown-version/capability, malformed, size/frame, checksum, busy and restart classifications. Preserve accepted versus not_needed; not_needed can have no attachment/chunk negotiation but must bind delivery and receiver run.
- [x] Generate using pinned `make generate`. Compare resulting file hashes across a second generation to prove idempotence while changes are uncommitted. Do not hand-edit .pb.go or regenerate baseline fixtures.
- [x] Add units at frame edges, maximum integer arithmetic, malicious length declarations, digest size, zero/inconsistent terms, original-requested versus negotiated ChunkBytes and timing conversions.

#### Verification

- [x] `make test`; `go vet ./transport/loadgrpc`; generator idempotence as above. Generated-code volume is exempt from the 1,500-line task estimate.
- [x] Do not run `make proto-check` against intentionally uncommitted generated changes; its HEAD-relative no-diff check belongs after generated changes are committed, at Task 20.

### Task 4: Implement owned snapshot ingress and isolated native preflight

**Goal:** the engine can safely receive a validated immutable snapshot through the new internal seam.
**Files:** create `internal/raftengine/snapshot_ingress.go`; update existing group/loop/loop_events/ready_apply/remote_validation/restore/writer code as required; owning units/functional tests.
**References:** target [§7.2 incoming handoff](../brainstorms/20261025-large-load-snapshot-transfer-target.md#72-incoming-owned-handoff), §7.5, §9.3 and §12 PrepareOwnedSnapshot, snapshotBoundaryReady, handleSnapshotPreflight, classifyOwnedSnapshotReady. Exact type states are in Appendix B.

- [x] Add SnapshotIngress with cached `raftstate.Snapshot` and one Done callback, SnapshotBounds, SnapshotPreflight and the exact methods from Appendix B. Give enqueue ownership an explicit enqueued/not-enqueued result; caller cancellation does not release enqueued payloads.
- [x] PrepareOwnedSnapshot fully decodes/validates the envelope on the caller worker using immutable engine options. JSON decode is not claimed to be interruptible: cancellation waits for actual return before releasing buffers.
- [x] Publish exact-run monotonic term/commit lower bounds for cheap obsolescence checks. Cached bounds prove only obsolescence; they cannot authorize acceptance, restore or a native reply.
- [x] Enable the bounded header-only/owned ingress queue only with no pending write, Ready timer, full/header ticket or outstanding Ready. Queued work waits while busy; defensive handler checks still refuse a closed boundary without Step. Validate nonzero/consistent terms/index before Step.
- [x] Implement header-only not_needed only when the owner immediately proves lower positive term or index <= commit. Preserve native term/reply/persistence behavior; require no Snapshot in the ensuing Ready. Clear its ticket immediately if no Ready results.
- [x] Keep one Ready consumer: store the owned ticket, let existing beginReady classify install versus skip, and apply dynamic authority-progress validation only to a selected snapshot. Reuse cached state for Restore and avoid cloning the immutable received payload again.
- [x] Invoke Done exactly once on refusal, skip, successful persistence/Restore, or joined failure. Abnormal shutdown callbacks run after all engine workers join and before closing done; never release a buffer while the writer still owns it.
- [x] Use the engine-owned context for full-snapshot persistence retries; ordinary WAL attempts retain their current timeout. Do not modify disk formats or persistence-before-publication ordering.

#### Verification

- [x] `make test`; `go vet ./internal/raftengine`; focused `go test -tags=functional -count=1 ./internal/raftengine -run '^TestSnapshotIngress_'`.
- [x] Focused functional race cases for queued cancellation, blocked Restore/write and exactly-once Done. Test zero-term index-ahead attacks and the WALWriteWait ticket collision explicitly, with no native mutation on refusal.
- [x] Regress existing _meta, standalone and full snapshot/tail recovery behavior locally; no multi-node functional fixture.

### Task 5: Add outbound offers, leadership epochs and reliable reports

**Goal:** independent bulk dispatch and exact reports exist before managed transport selects them.
**Files:** create `internal/raftengine/snapshot_outbound.go`, `snapshot_reports.go`; update contracts/ready_apply/writer/loop/outbound/group/metrics as needed.
**References:** target [§7.1 sending](../brainstorms/20261025-large-load-snapshot-transfer-target.md#71-outgoing-snapshots), [§7.4 isolation/reporting](../brainstorms/20261025-large-load-snapshot-transfer-target.md#74-heartbeats-leadership-epochs-and-non-lossy-reporting), §7.5, §9.3/9.4 and §12 report algorithms. Appendix B holds SnapshotOffer/Job/Attempt contracts.

- [x] Add SnapshotJob.Run/Discard, SnapshotOffer with exact attempt/publication generation/epoch, and SnapshotJobTransport. Only transports implementing the new seam use it; do not yet add the method to managed engineTransport (activation is Task 15).
- [x] Split eligible MsgSnap jobs from ordinary Ready messages without a payload marshal/unmarshal copy. Reserve/pin through PrepareSnapshot; store jobs alongside writeJob and release them only after the existing Ready durability/application boundary.
- [x] Dispatch through joined bulk workers independent of per-destination ordinary workers. Discard every unstarted job on persistence failure, dropped dispatch, cancellation or stop. Preserve ordinary-segment order without waiting for bulk completion before heartbeats.
- [x] Create/cancel source leadership epochs on native term/role changes. Pin source run/term; no resolver, hash, network or large copy on the loop.
- [x] Add per-run snapshot publication generation at local publication, received publication and startup restore; no new persisted field.
- [x] Replace dropping snapshot reports with the exact-attempt coalescing mailbox and wake signal. Match run/term/destination/generation before applying; adapt legacy standalone reports without removing their API.
- [x] Implement short 100ms–1s jitter for local capacity and remote pre-admission capacity/group_busy; admitted failures back off 1s→30s, permanent refusals use the ceiling. No byte-progress reset of failed backoff; no payload/permit retention while waiting to report failure.
- [x] Introduce the exact metrics.SnapshotCatchUp DTO from Appendix D here so the engine method signatures compile; defer PeerProgress publication and policy behavior to Task 14. Add bounded progress-update plumbing for Task 14 without manufacturing heartbeat freshness or changing current repair policy yet.

#### Verification

- [x] `make test`; `go vet ./internal/raftengine`; focused functional/race tests for non-lossy wake saturation, stale reports, epoch cancellation, mixed batches and unstarted-job discard.
- [x] Use a protocol double blocked in bulk Send to prove the same peer's ordinary sender continues. Verify _meta/standalone still use their existing paths where intended.

### Task 6: Implement coordinator budgets and the memory staging backend

**Goal:** bounded Node-wide transfer ownership with a reusable backend seam.
**Files:** create `internal/loadsnapshot/{coordinator,state,ports,staging}.go`, owning units and shared helpers; complete Task 2's declarations without attaching to Host yet.
**References:** target [§3.2 budgets](../brainstorms/20261025-large-load-snapshot-transfer-target.md#32-what-the-budgets-cover), [§6 state/storage](../brainstorms/20261025-large-load-snapshot-transfer-target.md#6-state-storage-and-ownership-schema), §9.2 and §12 reserve/staging. Appendix C reproduces Owner/Ports/volatile state and staging signatures.

- [x] Implement NewCoordinator/Start/Fence/Stop and directional count/byte reservations atomically. Include exact owner/run pins and one active incoming group slot. Reserve before payload allocation or extending a source pin.
- [x] Implement memorySnapshotStaging Append/ReadAt/Seal/Materialize/Close exactly as specified. Append owns chunks; Materialize returns immutable borrowed data without copying; Close never zeroes/recycles native-retained bytes.
- [x] Place the **single** future-disk TODO at snapshotStaging. Stable logical name incoming-snapshot is group-scoped; no filesystem paths, backend selector or disk journal.
- [x] Implement release-once counters and finite attachment/history sequences; overflow refuses. Disconnected resumable work keeps its reservations; stop timeout retains ownership.
- [x] Create bounded active/history/digest/terminal metadata containers. Completed Ack cache is evictable and never admission capacity; unresolved handoffs stay in the live map. No failure Ack rows.
- [x] Keep locks short: no hash/decoder, application callback, I/O, waiting or joins under coordinator/host locks.
- [x] Add pure units for multi-group quotas, distinct direction limits, group slot collision, duplicate release, arithmetic, cache-pressure independence and immutable view lifetime.

#### Verification

- [x] `make test`; `go vet ./internal/loadsnapshot`; pure budget/backend units allocate only small fixtures.
- [x] Verify declared >2 GB values/overflows reject or validate before allocating; no root/persistent-file changes.

### Task 7: Implement authenticated Begin, chunk receive and same-run resume

**Goal:** the common receiver validates and assembles a resumable complete payload.
**Files:** `internal/loadsnapshot/receive.go`, `resume.go`, state/service helpers and tests.
**References:** target §4.1–4.2, [§5 protocol](../brainstorms/20261025-large-load-snapshot-transfer-target.md#5-wire-protocol-and-resume-semantics), §7.5, §8.2–8.3 and §12 Begin/resume/Write/cached-bounds algorithms.

- [x] Implement BeginSnapshot, WriteSnapshot, DetachSnapshot, AbortSnapshot, SessionDone, validateSnapshotPeer, findTransfer, resumeSnapshot and replaceAttachment with Appendix A/C exact types.
- [x] Validate/authenticate through ports before substantial reservation; verify exact source/destination/run after waits and before publication. Require positive/consistent terms, snapshot index, bounded metadata and declared length in service calls too.
- [x] Preflight before allocation; return owner-confirmed not_needed without claiming payload delivery. Use exact-run cached monotonic bounds on chunk/resume; preempt only proven obsolete **pre-handoff** work, cancel/join before slot reuse.
- [x] Bind immutable manifest fields and original requested ChunkBytes. Store negotiated chunk size separately and echo it; resume cannot renegotiate offsets/ordinals or reset deadlines.
- [x] Implement contiguous append, chunk/whole digest state, exact duplicate comparison and cumulative acknowledgements. Reject holes, conflicting overlaps, corrupt chunks, short/oversized chunks and overflow before copy. Duplicates/keepalives never refresh useful progress.
- [x] Fence old attachments with a new generation and SessionDone; join active Node mutations before new writes. Unknown resume returns restart_required without allocation. Never mix successor bytes or use a terminal miss as proof of installation.

#### Verification

- [x] `make test`; `go vet ./internal/loadsnapshot`; focused functional/race cases with one local owner/protocol doubles for reconnect, concurrent old/new attachments and cancellation during append.
- [x] Cover source/destination run replacement, key reuse with changed data, cache hit/miss and pre-handoff obsolete preemption. Do not yet test a multi-node cluster.

### Task 8: Complete finalization, diagnostics and joined receiver teardown

**Goal:** verified bytes reach native Raft exactly once per retained attempt without freeing live buffers.
**Files:** `internal/loadsnapshot/{receive,lifecycle,status,resume}.go`, service delegation and tests.
**References:** target §4.3, §6.2, [§7.2–7.3 ownership](../brainstorms/20261025-large-load-snapshot-transfer-target.md#72-incoming-owned-handoff), §8.4–8.5 and §12 Finish/completion/expiry/terminal-cache algorithms.

- [x] Implement FinishSnapshot, completeReceivedSnapshot, cleanupCompletedSnapshot, cleanupReceive, expire, StopOwner, retainTerminal and List/observe. Populate Status/Query/Page from Appendix A without unbounded logs or copied payloads.
- [x] Claim one finalizer, seal/check total digest, materialize, call PrepareOwnedSnapshot off-loop, revalidate identity and hand encoded+decoded state to StepOwnedSnapshot. Cache/await the same completion on duplicate Finish/resume.
- [x] Separate ingress acceptance from receiver installed/skipped/failed and from a canceled caller wait. An enqueued handoff survives waiter cancellation; only actual owner outcome can release its permit.
- [x] Make engine callbacks record/signal only; cleanup workers join independently and never join themselves or a Finish waiter that waits for their reply.
- [x] Evict oldest completed accepted/not_needed Ack metadata at bounded capacity/TTL. Failure diagnostics are separate. Keep handing-off payloads non-evictable until owner completion.
- [x] Join all operations on Stop/Abort/expiry; close only owned staging references. Preserve retryable stop/close ownership, last bounded observations and exact old-owner cleanup after replacement.

#### Verification

- [x] `make test`; owning-package vet; targeted functional/race cases for lost final Ack, canceled queued ingress, disk/Restore failure after accepted Step, retained permit, failed Close and Stop retry.
- [x] Combine the real one-participant engine from Task 4 with protocol doubles to prove persistence/restore/skip outcomes; no partial snapshot enters recovery state.

### Task 9: Implement sender pins, cached digests and verified progress reports

**Goal:** one admitted source job owns immutable bytes and produces one exact terminal result.
**Files:** `internal/loadsnapshot/{send,digest}.go`, ports/state and tests.
**References:** target §4.1 Send/Progress/Transport, §7.1/7.4/7.5, §9.2/9.4 and §12 PrepareSend/Run/Discard/digest/reporting.

- [ ] Implement Coordinator.PrepareSend(Owner, raftengine.SnapshotOffer), snapshotSendJob.Run/Discard/recordVerifiedProgress and finishSnapshotDelivery. Preparation is bounded/no-I/O; hash/resolve/network happen on the bulk worker.
- [ ] Borrow the pinned immutable native slice without a sender clone. Charge every destination conservatively even for a shared artifact. Release only after actual workers join.
- [ ] Implement SourceSnapshotID (RunID, generation, index, term, bytes) and a two-completed-entry-per-live-run digest cache with singleflight misses. Failed/incomplete hashes never cache; completed entries retain no payload reference.
- [ ] Bind resolved recipient, exact source run/epoch, original total deadline and SnapshotAttempt to managed delivery registration. A canceled/deposed source cannot send/report for a successor.
- [ ] Supply Send.ReportProgress as a nonblocking closure tied to the job. Validate cumulative verified offsets/receiver run/negotiated bounds; stamp time locally; duplicates cannot renew progress. Callback cannot run after SendSnapshot returns/joins.
- [ ] Handle accepted and native-proven not_needed distinctly; report one managed outcome through the exact-attempt mailbox. No duplicate adapter ReportLoadDelivery.

#### Verification

- [ ] `make test`; `go vet ./internal/loadsnapshot`; functional/race tests for digest sharing, cancellation during hashing, old source bytes surviving replacement, stale callbacks and delayed reports.
- [ ] Repeated receiver capacity refusal must not clone/rehash the unchanged snapshot or retain payload during cooldown.

### Task 10: Implement gRPC receiving and qualify the teardown mechanism

**Goal:** receive chunks without coupling bulk traffic to ordinary call capacity or leaking half-open RPC owners.
**Files:** create `transport/loadgrpc/snapshot_server.go`; update adapter fields/Register/Close; owning functional tests using generated client protocol doubles.
**References:** target [§7.6 server cancellation](../brainstorms/20261025-large-load-snapshot-transfer-target.md#76-server-attachment-cancellation-and-half-open-streams), §5, §9.4, §12 server-pump algorithm and §13 release gates.

- [ ] Implement TransferSnapshot and receiveSnapshotOpen using the borrowed Service. Register on the existing load service/listener; no additional listener/address configuration. Before Task 15, obtain LoadSnapshots through a private optional provider type assertion so existing ordinary-only Ingress doubles remain buildable; absent support makes only the new RPC refuse. Do not yet add the method to the mandatory Ingress interface or hard-fail Bind. Migrate the doubles in Task 13 and remove this bridge at activation.
- [ ] Add separate pre-Open quota max(8,2*MaxIncoming) and at-most-three-second timeout; authenticate before consuming authenticated 2*MaxIncoming handler slots or transfer byte/count permits. Neither quota uses ordinary MaxConcurrentCalls.
- [ ] Run one adapter-owned Recv pump and one Send pump per admitted handler, with bounded queues. Observe SessionDone, stream/adapter contexts and phase deadlines. Defer exact detach after any active Node mutation returns.
- [ ] On cancellation return the handler so pinned gRPC can terminate actual stream I/O; do not self-join a blocked Recv before return. Retain pump wait-group ownership and quota until both pumps return. Await final Ack write under cancellation/deadline bounds on successful completion.
- [ ] Revalidate authenticated session identity/attachment before dispatch; reject data before Open, repeated Open, stale generations and unsupported protocol versions.
- [ ] **Required feasibility gate:** functional `-race` tests with real stats.Handler for empty/half-open Recv, blocked Send, replacement attachment, Abort, deadline, Node stop and adapter Close. Assert exactly one RPC completion, no late callback/resource release, no leaked pump or quota.
- [ ] If this pinned-version-sensitive mechanism fails, record a blocking NOT IMPLEMENTED GAP and revise the target before proceeding to activation. Do not waive or suppress the race, invent an unreviewed teardown protocol, or leave a success stub.

- [ ] Populate BeginRequest.MaxChunkBytes from the adapter's checked frame capacity before Service.Begin. Preserve Manifest.ChunkBytes unchanged; reject a resumed negotiated chunk that no longer fits. This transport-local field was added in Task 7 to express the target's existing separate bounds without rewriting the immutable manifest.

#### Verification

- [ ] `make test`; `go vet ./transport/loadgrpc`; `go test -tags=functional -race -count=1 ./transport/loadgrpc -run '^TestSnapshotRPC_'`.
- [ ] Stress pre-Open quota independently from an already-authenticated progressing transfer and ordinary RPCs. This gate is a focused functional race run, not an early full `make test-race`.

### Task 11: Implement bulk client connections, pipelining and resume

**Goal:** bounded sender throughput with independent ordinary-Raft connections and original deadlines.
**Files:** `transport/loadgrpc/snapshot_client.go`, `snapshot_connections.go`, existing Bind/Close fields and tests.
**References:** target §4.1, §5, §7.4/7.6, §9.4 and §12 SendSnapshot reconnect loop.

- [ ] Implement SendSnapshot, sendSnapshotAttempt, openSnapshotStream, sendChunks, receiveAcks, snapshotClient and closeSnapshotConnections with the exact target signatures.
- [ ] Give bulk transfers a separate bounded ClientConn map to the **same** existing load endpoint. Hold connections/streams under MaxOutgoing; old attempts must join before replacement. Ordinary Raft streams/control use their current pool.
- [ ] Implement eight-chunk default pipelining with independent send/receive goroutines, no Data until admission Ack, and Finish only after all bytes are cumulatively acknowledged. Validate every Ack against exact transfer/destination run and local/negotiated bounds.
- [ ] Wire ReportProgress for admission/new verified prefix/final outcome, coalescing safely and never after return. No fabricated progress from duplicate Acks, keepalives or reconnect.
- [ ] Resume with original manifest/requested ChunkBytes and stored negotiated value. Honor original total/progress clocks; negotiate only tighter bounds. Keep source preparation/finalization on total lifetime, not fake zero-byte progress.
- [ ] Retry transient connection loss with 100ms→2s bounded jitter inside the same transfer; do not retry permanent or restart_required in that delivery. Capacity/busy returned before admission goes to the short native retry policy, not a 30-minute adapter loop.
- [ ] Preserve explicit plaintext credentials, typed errors and application endpoint resolution; no silent unary fallback.

#### Verification

- [ ] `make test`; owning vet; functional/race tests for lost Open/chunk/final Ack, disconnect/resume, old stream join, unsupported peer, differing frame/time policies and bounded outstanding chunks.
- [ ] Block the bulk connection's flow control and prove ordinary traffic to the same peer address proceeds on the ordinary connection. Reuse Task 10's teardown gate for client Close.

### Task 12: Wire managed ports, Node lifetime and readmission barriers

**Goal:** the service becomes a real Node-owned mechanism while legacy send selection remains until Task 15.
**Files:** new `internal/managed/snapshot_ports.go`; existing managed host/replica/transport/readmission and root node.go/node_load.go/node_options.go; internal facade implementation.
**References:** target §4.2, §6.2, [§7.3 shutdown](../brainstorms/20261025-large-load-snapshot-transfer-target.md#73-shutdown-and-readmission), §7.8, §8.1/8.4 and §9.1–9.2.

- [ ] Implement Owner and Ports, including the explicit NoteSnapshotProgress callback, from Appendix C over exact existing Host slots, run IDs, authority/peer checks, native ingress/preflight and exact SnapshotAttempt delivery reports. Do not import managed/root from internal/loadsnapshot.
- [ ] Host owns Coordinator; Node passes its already-resolved policy and the pure facade resolver returns the current coordinator. Bind captures the stable service and Options before Start; all actual acquisition waits for Start/admission.
- [ ] Integrate Start/Fence/Quiesce/Stop and per-replica StopOwner. Request engine cancellation before waiting on engine-dependent snapshot cleanup; join source/pumps/queued handoff/Restore before application/store release.
- [ ] Extend joinReadmission: old coordinator fences and fully joins, with zero live reservations, before authority completion/new Host Start. Timeout retains old ownership; two coordinators must never admit under separate Node allowances simultaneously.
- [ ] Retain completed diagnostics through the observation-only facade fallback; stale sessions cannot mutate it or redirect into a new host/run.
- [ ] Add resolved snapshot deadlines to managed delivery records and preserve ordinary EffectTimeout. Store attempt token for non-lossy reports and terminal CAS between expiry/Ack.
- [ ] Keep `engineTransport.PrepareSnapshot` selection and rejecting constructor/ordinary-ingress behavior deferred to Task 15. Private adapters may delegate internally for focused tests but must not accidentally satisfy the selecting interface early.

- [ ] Resolve review P2-3b: Authorize/revalidation on each chunk must use constant-time exact owner/peer routing indexes, without a topology scan or remote I/O.

#### Verification

- [ ] `make test`; focused managed/root vet; local functional/race tests for constructor→Bind→Start, StopLoadGroup, failed Close, unpublished joining replica, readmission and old-host join.
- [ ] Prove Node Stop joins owned work while borrowed adapter/server remains available and no context waiter can free a successor's resources.

### Task 13: Migrate fixture, examples and custom transports before enforcement

**Goal:** every consumer remains buildable when Task 15 requires the new transport capability and closes old MsgSnap ingress.
**Files:** tests/cluster/testapp app/transport/hooks and helpers; existing root/recovery/managed functional helpers; example and adapter mocks as required. No new root test files.
**References:** target [§9.5 migration prerequisite](../brainstorms/20261025-large-load-snapshot-transfer-target.md#95-prerequisite-fixture-migration-generation-and-documentation), §4 capability contract, §10, §11 compatibility; current testapp README/fault contracts.

- [ ] Inventory implementations with `rg` across all five application modules, root recovery helpers and optional tests/sim. Add snapshot.Transport to managed transports and LoadSnapshots to Node/Ingress doubles. Unused doubles may return Unsupported; exercised loopbacks must call the real common service contract, not report success without transfer.
- [ ] Construct/bind/register production loadgrpc before serving the fixture's existing load listener. The fixture's `gquorum.fixture.load.v1` service and production `gquorum.load.v1` service can coexist on that listener; no extra port.
- [ ] Ensure snapshot process cases use the real production SendSnapshot/RPC/service. Their ordinary-traffic isolation cases must also use production SendRaft/StreamRaft, not an unrelated legacy fixture connection that makes isolation assertions vacuous.
- [ ] Preserve existing custom-transport coverage via a compliant wrapper/delegate. Preserve authenticated drop_raft/drop_append/snapshot delays/corruption, read tracking, storage/Close barriers and identity-bound controls. Dropped snapshots are failures, not completed deliveries.
- [ ] Add service/ingress decorators in the **test fixture** for exact chunk/Finish/receipt fault injection and counters. Distinguish header-only not_needed from a complete received handoff and from durable restoration. Do not add test-only application semantics to the library.
- [ ] Adapt example Node binding and test doubles. Keep examples/cluster meta-only. Tests/sim is currently meta-only and needs no new load mechanism; verify its source dependencies and do not add simulation dependencies to the library.
- [ ] Add fixture unit/functional tests for default mode registration, credential rejection, exact successors, legacy wrapper migration and callback/join ownership. The still-selected old send path must continue to pass until Task 15.

#### Verification

- [ ] `make test`; focused fixture/root/recovery/managed functional checks and vet in each owning module.
- [ ] Compile process tests without running TestMain using `go -C tests/cluster test -c -tags=clustertest -o "$SNAPSHOT_TEST_COMPILE_DIR/cluster.test" .`; create the temporary directory using the Validation Commands sequence and remove only those artifacts afterward. No intermediate Docker run.

### Task 14: Add bounded catch-up evidence to repair policy inputs

**Goal:** a progressing transfer prevents premature lag repair without protecting an endlessly failing receiver.
**Files:** metrics/sample.go/clone.go; raftengine metrics/reports/episode state; internal/observe evidence_round/evidence_snapshot and owning tests; scheduling/votercheck tests; managed progress ports.
**References:** target [§7.7 repair evidence](../brainstorms/20261025-large-load-snapshot-transfer-target.md#77-repair-evidence-during-genuine-snapshot-catch-up), §9.3–9.4, §12 fixed episode and catch-up predicate, and §13 receiver-churn correction. Exact metrics fields are in Appendix D.

- [ ] Use the SnapshotCatchUp DTO introduced in Task 5; add optional PeerProgress.Snapshot publication with all target JSON fields; clone its pointer-owned data and retain omission for absent values. No persisted topology/voter-config schema changes.
- [ ] Connect Node-owned Send.ReportProgress to the bounded source-engine mailbox. Only the owner loop samples fresh Raft observations; preserve raw lag and never manufacture RecentActive, AckSequence, live quorum, readiness or promotion proof.
- [ ] Implement the per-peer episode: locally stamped first admitted attempt; one fixed TotalTimeout+ProgressTimeout deadline; anchor at historical native Match; sticky failure for total/progress timeout. Retry, bytes, artifact/delivery changes and source term renewal cannot extend it.
- [ ] Preserve episode deadline/failed latch across **engine-only receiver RunID** changes for the same process/durable peer. Actual Match advancement or a new process/durable replica identity can create a new episode. Unknown receiver run cannot grant protection. Source-run restart loses volatile evidence; no replicated crash-loop detector.
- [ ] Observer validates exact/fresh leader/target/run and positive verified progress, ages duration fields, and suppresses only lag/stall failure predicates while episode/progress/grace remain valid. After expiry/failure, later partial retries cannot reset ordinary failure accumulation.
- [ ] Feed the same derived metrics.Evidence to both built-in `scheduling/voters.go` and mandatory `internal/scheduling/scoped_repair.go` checks via votercheck. Preserve default voter windows, learner promotion and live-quorum guards.
- [ ] Add units with controllable clocks for repeated timeout attempts, engine-run churn, term renewal, observation replay/staleness, receiver failure, unknown run, Match advancement and bounded grace. Source callbacks must not set episode start or failed latch.

#### Verification

- [ ] `make test`; `go vet ./metrics ./internal/observe ./internal/votercheck ./internal/scheduling ./scheduling ./internal/raftengine`.
- [ ] Focused functional/race progress ownership tests; full process repair scenario is Task 17/20. No broad race suite yet.

### Task 15: Activate the complete managed snapshot path atomically

**Goal:** all public paths use the reviewed mechanism, with no bypass or temporary bridge left.
**Files:** existing node options/composition; managed engineTransport/ReceiveRaft; raftengine selecting seam; loadgrpc client/wire/Bind; migrated tests/mocks.
**References:** target §4.1–4.2, §7.1–7.4, §8.1–8.2, §9 and §11 compatibility.

- [ ] Verify Tasks 1–14 completed and the Task 10 teardown race gate passed; migrated production adapters, fixtures and exercised loopbacks are real implementations, not compile-only stubs.
- [ ] Add the actual `engineTransport.PrepareSnapshot(raftengine.SnapshotOffer)` method delegating to Coordinator.PrepareSend so native managed MsgSnap selects the new bulk path. Preserve _meta/standalone semantics and ordinary ordering.
- [ ] Enforce NewNode's pure missing/nil snapshot.Transport capability refusal. Promote LoadSnapshots into the mandatory loadgrpc.Ingress method set and remove Task 10's optional-provider bridge. Activate LoadSnapshot service Bind validation; defaults originate only from resolved Node options.
- [ ] Reject managed ordinary MsgSnap in ReceiveLoadRaft and ordinary loadgrpc SendRaft/planning. Every managed snapshot, including small ones, goes through common budgeted ingress; no giant unary downgrade.
- [ ] Remove all temporary unavailable bridges and intermediate selection helpers introduced earlier; do not leave a runtime feature flag. Ensure acceptance/report exactly once, deadlines bypass half-election/EffectTimeout/CallTimeout, and adopted buffers remain owned through install/skip/failure.
- [ ] Exercise a real local Node→adapter→common service→engine path with a protocol double, not only private method tests. Demonstrate the pipeline is actually selected by native Ready and terminates through native report handling.

#### Verification

- [ ] `make test`; focused Node/managed/engine/adapter functional checks and their relevant races; compile process tests.
- [ ] Search for remaining old managed unary snapshot sends, unsupported bridge methods or callback gaps. Existing root/recovery and application fixture suites remain green.

### Task 16: Prepare the synthetic snapshot fixture and phase controls

**Goal:** supply reusable public-API test instrumentation before adding the process matrix.
**Files:** tests/cluster/testapp synthetic FSM, snapshot service/transport decorators, authenticated hooks; tests/cluster Spec/config helpers and fixture tests.
**References:** target §9.5, §10.1–10.2 and §8.5 failure matrix; current testapp identity/fault-control contracts.

- [ ] Implement a deterministic synthetic application state machine selected only by explicit fixture configuration/control. Its Snapshot generates real bytes containing seed/version, logical counter and opaque staging/replay records; Restore checks their full digest and semantics. No checked-in payload or giant HTTP response.
- [ ] Add bounded authenticated commands to configure generator size/seed, force local snapshot, observe encoded transport length/digest/index and select exact phase barriers. Mutation paths stay on public Node/Group/Service/Transport APIs; no private imports.
- [ ] Extend existing migration decorators with snapshot-specific lost admission/chunk/final Ack, delay/disconnect/corruption, and before/after handoff/persistence/Restore controls. Bind every control to exact node/process/group/replica identity and ensure drop produces failure rather than false success.
- [ ] Record bounded phase/progress/resource observations, with accepted handoff distinct from installed state and header-only not_needed. Counters cannot infer installation from ingress acceptance.
- [ ] Plumb per-case snapshot options through existing Spec/app configuration and maintain exactly the existing meta/load listeners. Synthetic allocation starts only when the selected test explicitly requests it; ordinary startup remains small.
- [ ] Add fixture unit/local functional regressions for authentication, successor refusal, deterministic round-trip content, callback cancellation and joined cleanup. Keep actual multi-node scenarios for Task 17.

#### Verification

- [ ] `make test`; focused testapp vet/functional/race checks; compile process cases without executing TestMain. Use small generated data in these local checks.
- [ ] Confirm no `testdata` payload, runtime blob, implicit root/path or extra listener was introduced.

### Task 17: Add small process regressions and the named repair scenario

**Goal:** verify all cross-node behavior through public APIs and real production transport.
**Files:** tests/cluster/snapshot_transfer_test.go; tests/cluster/testapp synthetic FSM/helpers/authenticated controls; existing Spec/config wiring.
**References:** target [§10.1 SNAP matrix](../brainstorms/20261025-large-load-snapshot-transfer-target.md#101-acceptance-mapping), §7.7, §8.5 and full GQ-02 SNAP-01–15.

- [ ] Add three-node small generated cases for 32 MiB multi-frame transfer, >500ms under ordinary elections, same-peer ordinary traffic progress, compaction+tail+later leadership, stale identities, lost Acks, source election change, callback/storage failures and crash boundaries. Use observable phase barriers rather than correctness sleeps.
- [ ] Cover remaining SNAP cases through the appropriate existing unit/functional suite where multi-node behavior is unnecessary; maintain the explicit matrix in this plan.
- [ ] Add local/remote capacity-pressure multi-group process coverage: one receiving permit becomes available, cheap pre-admission retries refill it without long idle gaps; payload/metadata/worker limits remain bounded.
- [ ] Add `TestSnapshotTransfer_RepairProtectionFourNodes` as the one justified larger fixture: three voters retain majority while one falls behind, fourth is an eligible stable learner. Verify no premature replacement during genuine progress, then repeated terminal timeouts/no Match advance restore failure eligibility. Use explicit shortened windows where practical; test the real default-window interaction separately if required for the regression.
- [ ] Add receiver automatic recovery/run-churn scenario without assuming failed engines are held. A new run requires a fresh transfer but cannot renew the source episode for the same peer.
- [ ] Preserve isolated pool leases, t.Parallel before acquisition, exact resource identity and verified joined container/volume/worker cleanup. Do not import private library packages into the test application.

#### Verification

- [ ] `make test`; fixture-focused checks; compile all `clustertest` cases without running Docker. Validate scenario/test naming and matrix coverage.
- [ ] Execute these process cases only in final Task 20 after full lint and one image build; do not rebuild a shared tag during a run.

### Task 18: Add generated opt-in qualification through 2 GB

**Goal:** keep routine tests small while making measured resource qualification reproducible.
**Files:** tests/cluster/snapshot_large_test.go and tag-matched large helpers; reuse the synthetic fixture generator/metadata observations from Task 16 and the process helpers from Task 17.
**References:** target [§10.2 large qualification](../brainstorms/20261025-large-load-snapshot-transfer-target.md#102-large-generated-qualification), §3.2 memory disclosure, §7.8 and user decisions in the brainstorm log.

- [ ] Gate large process cases with `//go:build clustertest && snapshotlarge`; use the `TestLargeSnapshot_` prefix. Do not add a default make dependency, environment-only skip gate or checked-in blob.
- [ ] Generate 256 MB, 1 GB and at most 2,000,000,000 encoded bytes at runtime. Choose valid application length leaving room for the unchanged engine envelope and verify/report actual encoded size. Use observable transport length/limited calibration rather than importing private encoding packages or calling an implementation-specific internal test API.
- [ ] All generated bytes traverse actual production chunks and real persistence/Restore; do not substitute the seed, sparse files or compression. Return only bounded seed/size/digest/position metadata in reports.
- [ ] Explicitly configure per-case limits/budgets; >2 GB configurations remain valid in nonallocating unit checks. Do not turn test ceilings into production constants.
- [ ] At the largest case, verify catch-up/tail/new writes/restart, one mid-transfer reconnect, ordinary traffic progress, and cleanup. Record RSS/heap where available, store-disk high-water, reservations, chunk/window peaks and separate transfer/restore durations.
- [ ] Use `CLUSTER_PARALLEL=1` for qualification, independent per-test fixtures and explicit resource preflight. If resources are insufficient, report unqualified/blocked—never silently shrink a requested case or claim it passed.
- [ ] Document and test selection so default unit/functional/clustertest/performance commands cannot select the large case. Keep the test's large allocation behind execution of that explicitly tagged case.

#### Verification

- [ ] `make test`; compile `clustertest,snapshotlarge` to a temporary binary without executing TestMain. Inspect build-tag selection with Go file lists; no 2 GB data generation in this task's ordinary checks.
- [ ] Run the explicit qualification command only in Task 20 on a suitable host. Leave its result unqualified if not run; no historical report substitutes for this execution.

### Task 19: Align public documentation and compatibility records

**Goal:** describe implemented behavior accurately without broadening scope.
**Files:** existing README, transport/loadgrpc/README.md, docs/architecture/{raft,library,runtime,load-transport-multiplexing}.md, docs/testing/tests.md, tests/cluster and testapp READMEs; update current design constraints only where feature facts require it while preserving unrelated edits.
**References:** target §1–4, §6.1, §7.6–7.8, §10–11 and §13.

- [ ] Document NewNode options/defaults, minimal accessor and package layout, shared load listener with isolated bulk connections, explicit plaintext support and custom transport upgrade requirement.
- [ ] Describe ingress acceptance versus installed/not_needed/failed, same-run resume, immutable deadlines and retained ownership after waiter cancellation.
- [ ] Explain payload budgets versus actual RSS, resident snapshot costs/local callbacks, configurable sizes above 2 GB, no receive disk files and the single deferred-backend TODO.
- [ ] Explain finite repair episode/timeout latch and engine-only receiver churn: one timeout can end protection while a later retry runs; no quorum/promotion proof is inferred from chunk progress.
- [ ] Document explicit qualification command, measured limits versus unmeasured sizes, four-node exception and all required release gates. Leave results blank/unqualified until Task 20 actually runs them.
- [ ] Keep wire v2 ordinary/admin protocols and persistent formats/baselines unchanged; document new snapshot RPC version and no legacy fallback after activation. No new GQ-03/06/07 client or standalone service claim.

#### Verification

- [ ] `make test` if any Go example snippet/support code changes; otherwise verify paths, names, runnable commands and `git diff --check`. Do not claim current architecture is implemented before the corresponding tasks are complete.
- [ ] Ensure only one code TODO remains for disk staging, not a WARN/NotImplemented stub for each previously excluded scenario.

### Task 20: Final verification and architectural delta audit

**Goal:** prove the implemented feature matches every target subsystem and is actually wired. This is the mandatory final task.
**Files:** all changed feature components; this plan's progress/gap records; verification reports.
**References:** target **all sections**, especially §6 schemas, §8 flows, §9 exact inventory, §10 acceptance, §11 exclusions and §13 final gates.
**Verification:** run the final-checkpoint commands below, including full unit race/full lint only here, followed by process coverage using one per-run image tag. Fix findings and rerun affected checks. Explicit large qualification is a separate opt-in command with its own recorded result. The last checkbox is intentionally the actual-wiring audit.

- [ ] Run all final non-Docker checks and the gRPC `-race`/stats-handler gate; record commands, versions, report directories and outcomes. No skipped failing case is an acceptance proof.
- [ ] Build one uniquely tagged fixture image and run the focused snapshot process suite and the final full ordinary cluster suite. Run explicit large qualification when the host meets its resource envelope; otherwise retain a clearly blocking/unqualified qualification record instead of marking it passed.
- [ ] Audit configuration/public contracts: every Options field/default/validation, Service/Transport method, protocol/status/error type, construction rule and minimal root composition matches target §§3–4 and Appendix A.
- [ ] Audit volatile/persistent schema: exact owner/key/manifest/generation maps, counters, two-digest bound, Ack eviction/TTL, live-handoff retention and history/page bounds match §§3.2/6; no persistent schema, WAL/application encoding or frozen baseline changed.
- [ ] Audit engine: strict preflight, one Ready consumer, cached off-loop decode, selected-only authority checks, ownership callbacks, epoch cancellation, publication generations, mailbox/cooldown and preserved _meta/standalone behavior match §§7.1–7.5/12.
- [ ] Audit coordinator/managed lifecycle: immutable pins, whole digest, chunk/resume/preemption, finalization, Host handover, Node stop/Close retries and observation-only fallback match §§6–8 and Appendix C.
- [ ] Audit gRPC: exact wire fields/method registration, frame math, independent same-endpoint connection pools, pre-Open/authenticated quotas, Recv/Send pumps, deadlines, progress callback and teardown gate match §§5/7.6/9.4.
- [ ] Audit metrics/repair: exact observations, local episode timestamps, timeout latch, same-peer receiver-run churn, Match reset, finite grace and shared policy/revalidation evidence match §7.7/13 and Appendix D; no weakening of quorum, promotion, availability or identity checks.
- [ ] Audit examples/fixtures/test modules: all custom transports and mocks migrated, production path actually tested, exact fault controls preserved, no private imports, all SNAP-01–15 mapped, only named four-node case, large fixtures generated/tagged/cleaned and never checked in.
- [ ] Audit documentation, scope and final validation evidence. Review every NOT IMPLEMENTED GAP; fix against target and rerun affected checks. A difficult unresolved item is marked `[_]` with evidence, not silently dropped or treated as done. No new deferral is inferred from effort or elapsed time.
- [ ] **Verify all methods/components are WIRED for actual use:** NewNode constructs the pure facade and validates capability; Start creates the owned coordinator; adapter Bind/Register reaches the same load listener; native Ready selects PrepareSnapshot; actual transport Acks invoke the source-bound ReportProgress; received verified data reaches the real owned native ingress; persistence/Restore/skip complete the retained permit; native reports and repair evidence consume the results; Stop/readmission joins every owner before release. Trace these through real entry points/process assertions, not only test-created objects or unused declarations.

## GQ-02 traceability

| Acceptance case | Implementation tasks | Required evidence |
|---|---|---|
| SNAP-01 complete chunked state | 3,6–11,15–18 | 32 MiB default process case; digest/state equality; opt-in size ladder |
| SNAP-02 independent deadlines | 5,7–12,15–17 | >500ms with ordinary elections unchanged |
| SNAP-03 ordinary traffic isolation | 5,10–11,17 | blocked bulk I/O and same-peer production ordinary traffic |
| SNAP-04 count/byte/size bounds | 1,3,6,9–11,17 | refusal before allocation, both directions, cache pressure, multi-group refill |
| SNAP-05 duplicates/corruption | 3,7–8,11,17 | exact duplicates harmless; holes/conflicts/digest failures refused |
| SNAP-06 exact successors | 4–9,12,17 | old runs/attachments/reports cannot reach replacement |
| SNAP-07 cancellation/shutdown | 4–12,17 | joined or retained ownership; gRPC stats/race gate |
| SNAP-08 crash boundaries | 4,8,12,17–18 | full durable state only, lost-final-Ack replay/native skip |
| SNAP-09 slow vs stalled | 7–11,14,17 | original clocks across retries; no indefinite repair exemption |
| SNAP-10 snapshot/tail/leader | 4–5,8–9,14–18 | identical state, committed tail and subsequent leadership/writes |
| SNAP-11 incompatibility | 1–3,10–13,15 | pure constructor capability refusal; explicit wire/server limit errors |
| SNAP-12 opaque application state | 7–8,17–18 | replay/staging records remain complete and uninterpreted |
| SNAP-13 obsolete snapshot | 4,7–8,11,17 | positive-term gated header-only skip and late native skip; no obsolete restore |
| SNAP-14 matching retained log | 4,8,17 | normal commit fast-forward with no offered-data restore |
| SNAP-15 accepted then failure | 4,8,12,17 | ingress acceptance never promoted to installed after failure |

Additional reviewed regressions: terminal cache never gates admission (6–8); deposed source cancellation (5/9/16); zero-term/ticket-collision preflight (4); receiver-run churn and timeout episode latch (14/16); no dropped snapshot report (5); no sender clone/repeated digest (9); remote busy retries (5/11/16); original versus negotiated ChunkBytes (3/7/11); pre-auth quota isolation (10); exact Host handover (12); real production fixture path and preserved fault hooks (13/16).

## Validation Commands

These are implementation commands, **not results obtained while writing this plan**. A focused task may use smaller `-run` selections, but all five module units must remain green. Current repository tools and tags take precedence over historical target command names.

### Routine task checkpoints

```sh
make test
# Vet only the packages changed by that task, for example:
go vet ./load/snapshot ./internal/loadsnapshot ./internal/raftengine
# Local functional checks; test prefix is introduced by the relevant task:
go test -tags=functional -count=1 ./internal/raftengine -run '^TestSnapshotIngress_'
go test -tags=functional -race -count=1 ./transport/loadgrpc -run '^TestSnapshotRPC_'
# Only pending changes, if linting while editing:
make lint
git diff --check
```

Do not run commands naming a not-yet-created package before its task. Use a task-specific temporary output path for compile-only `go test -c`; it compiles clustertest code without executing TestMain or starting Docker. Do not use ordinary `go test -run '^$'` as the process compile-only check, since TestMain would still execute.

### Compile-only process checks (no TestMain/Docker)

```sh
(
  SNAPSHOT_TEST_COMPILE_DIR="$(mktemp -d)" || exit 1
  trap '
    rm -f "$SNAPSHOT_TEST_COMPILE_DIR/cluster.test" "$SNAPSHOT_TEST_COMPILE_DIR/snapshot-large.test"
    rmdir "$SNAPSHOT_TEST_COMPILE_DIR"
  ' EXIT
  go -C tests/cluster test -c -tags=clustertest \
    -o "$SNAPSHOT_TEST_COMPILE_DIR/cluster.test" . || exit $?
  # After Task 18 introduces the large cases:
  go -C tests/cluster test -c -tags=clustertest,snapshotlarge \
    -o "$SNAPSHOT_TEST_COMPILE_DIR/snapshot-large.test" . || exit $?
)
```

Record each compiler exit status before cleanup and do not let cleanup mask a failed compile. These commands only compile; even the large-tag command does not execute its generator. Use only the first compile at checkpoints before Task 18.

### Final checkpoint (Task 20 only)

```sh
make test
make test-race
make test-function
make test-function-race
make proto-check
make lint-full
```

Choose an unused per-run image tag and keep it unchanged throughout each run. Do not overwrite another run's shared tag. Then:

```sh
make test-cluster-build CLUSTER_IMAGE="$SNAPSHOT_TEST_IMAGE"
make test-cluster-run CLUSTER_IMAGE="$SNAPSHOT_TEST_IMAGE" \
  CLUSTER_ARGS='-v -run ^TestSnapshotTransfer_'
make test-cluster-run CLUSTER_IMAGE="$SNAPSHOT_TEST_IMAGE"
```

`SNAPSHOT_TEST_IMAGE` must be set to that unique tag before executing these commands. The focused run provides feature evidence; the full ordinary run covers existing admission/retirement/recovery/read/fault fixtures after transport migration. If one successful final full run includes every focused case with sufficient diagnostics, retain that evidence rather than redundantly repeating an unchanged suite.

### Explicit large qualification only

```sh
make test-cluster-run CLUSTER_IMAGE="$SNAPSHOT_TEST_IMAGE" \
  CLUSTER_PARALLEL=1 CLUSTER_TIMEOUT=2h \
  CLUSTER_ARGS='-tags=clustertest,snapshotlarge -run ^TestLargeSnapshot_ -v'
```

Never attach that tag to ordinary defaults. A source-level implementation is not 2 GB qualification unless this explicit run completes on a suitable host. Capture all resource/cleanup evidence described in Task 18; unavailable resources or failed cases are recorded as incomplete, not inferred success. No separate broad `make test-perf`, benchmark or chaos sweep is required; the specified snapshot resource tests cover the requested measured scope.

### Conditional tooling and retained-baseline checks

- If Makefile/test_report runner changes become necessary: `python3 -m unittest discover -s scripts -p '*_test.py'`, plus relevant Make targets. Prefer existing CLUSTER_ARGS passthrough over modifying the runner.
- Simulations are currently meta-only. Run a focused documented `make test-sim` selection only if shared-engine/source changes leave a concrete simulation concern; do not add a broad simulation run to every task.
- Verify immutable files in `events/testdata`, `load/testdata/api-baseline`, `internal/metaengine/testdata/api-baseline` and scoped/legacy compatibility fixtures retain their baseline hashes. No “regenerate to fix” step is permitted.
- Read failed run `report.md`, `timeline.md` and package logs. Preserve failure exit codes and record exact commands/report directories in the progress log.

## NOT CONSIDERED & TODOs

These exclusions were agreed in the brainstorm/target and are carried forward without reopening them. No runtime-success stub or warning is added merely to advertise an absent optional feature.

| Tag | Excluded scope | Required behavior in this plan |
|---|---|---|
| postponed | Disk receive staging | Memory implementation only; one group-scoped backend interface and exactly one code TODO from target §6.3. No path configuration/files/journal. |
| postponed | Streaming StateMachine/Store and constant-memory restore | Preserve current APIs; document/measure full-buffer and resident costs. |
| postponed | Cross-process/run resume | Exact old transfer refuses; new delivery restarts. No durable partial cache. |
| postponed | Measured qualification above 2 GB | Configurable larger values remain valid; no measured claim above the explicit test ceiling. |
| potential-improvement | Remote durable-install receipt | Accepted ingress remains distinct; local outcomes plus honest unknown remote state. |
| potential-improvement | Compression, strict fairness/bandwidth scheduling and adaptive windows | Fixed bounded no-compression policy; short busy retries and measured multi-group progress, no strict FIFO/bandwidth guarantee. |
| postponed | Standalone chunk service | Existing standalone ownership/transport preserved; no new service advertised. |
| postponed | GQ-03/GQ-06/GQ-07 general clients/preflight/disposition APIs | No new application router, generic proposal limits API or receipt client. |
| postponed | Expanded permanent-majority-loss recovery limits | Separate subsystem remains unchanged; larger transfer allowance does not mutate recovery policy. |
| postponed | Cross-process replicated failure history/crash-loop detector | Source-local episode only; exact observation-scope limitations disclosed. |

The single code TODO is reproduced with the staging interface in Appendix C. Do not scatter additional TODO/WARN/NotImplemented markers for these exclusions: the user explicitly requested one deferred disk-backend note and the reviewed target specifies ordinary supported/refusal behavior.

## NOT PLANNED CHANGES

Task 2 required one unrelated test-only correction to keep the required five-module unit checkpoint green. `events/events_test.go` sampled subscriber loss before journal progress and could combine stale ingress-loss evidence with a later append. Read journal progress first, establishing that the first dispatch has consumed ingress loss before inspecting subscriber overflow. Production bus code and persisted events are unchanged. Failure evidence: test-reports/unit/20261025T164607.543982Z-v_apavlc/library-events.log. The corrected focused test passed 100 repetitions; all 2,194 module unit tests then passed.

Diff with tabs expanded for documentation:

```diff
diff --git a/events/events_test.go b/events/events_test.go
index 997cc85..ec269f7 100644
--- a/events/events_test.go
+++ b/events/events_test.go
@@ -298,13 +298,17 @@ func TestBus_BoundedIngressAndSubscriberLoss(t *testing.T) {
    must(t, <-finished)
    // Wait until both accepted events have been dispatched into the bounded subscriber.
    for {
+       // The second append follows the first dispatch, which consumes the
+       // ingress-loss count. Read the journal first so a later lost count
+       // cannot be confused with that earlier ingress loss.
+       journaled := len(listTest(t, j, Query{})) >= 2
        b.mu.Lock()
        pendingLoss := uint64(0)
        for s := range b.subs {
            pendingLoss = s.lost
        }
        b.mu.Unlock()
-       if pendingLoss == 1 && len(listTest(t, j, Query{})) >= 2 {
+       if journaled && pendingLoss == 1 {
            break
        }
        select {
```

Record any later necessary deviations with files, reasons, target references and verification; preserve unrelated user work.

Task 5 internal reporting refinement:
- `internal/raftengine/snapshot_reports.go`: the specified status-only report cannot distinguish pre-admission capacity from permanent or admitted failures. Added an error-bearing companion while retaining the exact original signature. Managed delivery wiring in Task 12 must call it on errors; no new public load API or policy decision.

```diff
+func (g *Group) ReportSnapshotAttemptError(a SnapshotAttempt, err error) {
+    // Overloaded: 100 ms–1 s; Invalid/Unsupported: 30 s; other: 1 s–30 s.
+    // Classified result is retained in the same exact-attempt mailbox.
+}
```

Task 7 transport-cap refinement:
- `load/snapshot/protocol.go`: added optional `BeginRequest.MaxChunkBytes`. The service has no gRPC frame limit and must preserve the original Manifest.ChunkBytes for resume identity; this separate scalar supplies the adapter's bound without changing a wire field, Node option or immutable manifest. Target §4.1 and Appendix A now include it; Task 10 populates it from the gRPC frame calculation.

```diff
 type BeginRequest struct {
+    MaxChunkBytes uint32 // optional adapter payload cap; zero adds no cap
     Manifest Manifest
 }
```

## NOT IMPLEMENTED GAPS

No implementation exists yet; ordinary unchecked tasks are not defects in the plan. During execution, record discovered deviations here with task/target references and evidence. Use `[_]` for an unresolved gap that could not be corrected; such a mark is not a passed task or an approved deferral.

| Gap | Target/task | Evidence | Resolution / status |
|---|---|---|---|
| Private progress-port shorthand | Target §9.2/9.4; Tasks 6/12 | NoteSnapshotProgress is required but omitted from the Ports sketch | Explicit private callback in Appendix C; external review still pending |
| External plan review unavailable | Planning/review only | Selected colleague returned a provider session-limit message, no findings | Historical review gap; subsequent explicit implementation goal authorizes execution. Do not describe the plan as colleague-reviewed |

## Progress log

| Date | Stage | Work and evidence |
|---|---|---|
| 2026-10-25 | Task 2 | Pure Node service/options composition and unavailable coordinator seam; all 2,194 units, vet and pending-file lint passed. Corrected an observed event-test ordering flake, recorded below. |
| 2026-10-25 | Task 1 | Public snapshot contracts/options/errors implemented; 42 package cases, all 2,182 five-module units and focused vet passed. See execution log/report path. |
| 2026-10-25 | Planning | Read reviewed target/log, current source/contracts, AGENTS and current Makefile/testing guide. Prepared task sequence and activation boundary. No code implementation, Go tests, Docker runs or large allocations. |
| 2026-10-25 | Self-review | Checked all 20 task dependencies/references/unchecked lists, exact type appendices, acceptance mapping and final wiring. Clarified early metrics DTO declaration, optional pre-activation Ingress provider, temporary compile artifacts and final-only expensive checks. Documentation checks only. |
| 2026-10-25 | Colleague request | Review dispatched to the selected existing colleague. Provider session limit prevented review; reset reported as 23:30 Asia/Almaty. No review findings or readiness verdict were received. |

## Plan review

Review was requested from the user-selected colleague `43cff8c6-3dab-4f3f-a0eb-d4393c777725`, with the complete plan, target and context in review thread 2. The provider returned: “You've hit your session limit · resets 11:30pm (Asia/Almaty)”. **No review was performed and no colleague findings/readiness verdict exist for this plan.** The prior architecture review does not substitute for plan review.

Self-review corrected the explicit private progress port, metrics DTO declaration order, optional pre-activation Ingress bridge and compilation/validation sequencing. A future colleague pass must still check dependency/buildability, target coverage, schema/signature completeness, migration/cutover and the final wiring audit. Do not substitute another reviewer without the user's direction. The user subsequently supplied an explicit implementation goal, authorizing execution despite the earlier unavailable plan review. Do not misrepresent that provider-limit result as a completed review.

## Target-state appendices

The following declarations are copied from the target for task-level execution clarity. The linked target remains authoritative if a later approved amendment changes one of them. The private progress-port callback is the explicit target-obligation elaboration noted above. All imports are their existing module packages; snippets are specification declarations, not code already present in the repository.


### Appendix A: public options, protocol/service, status and errors

```go
type Options struct {
    MaxSnapshotBytes uint64
    MaxIncoming     int
    MaxOutgoing     int
    MaxReceiveBytes uint64
    MaxSendBytes    uint64
    ChunkBytes      int
    WindowChunks    int
    TotalTimeout    time.Duration
    ProgressTimeout time.Duration
}

func (o Options) Resolve() (Options, error)
func (o Options) Validate() error
```

```go
type Key struct {
    From, To   load.Peer
    DeliveryID uint64
}

type Manifest struct {
    Key          Key
    NativeHeader []byte // native MsgSnap encoding with Snapshot.Data cleared
    TotalBytes   uint64
    SHA256       [32]byte // digest of complete encoded Snapshot.Data
    ChunkBytes   uint32  // sender requested payload size; negotiated downward
}

type Session struct {
    Key        Key
    Attachment uint64 // receiver-issued nonzero fencing generation
}

type BeginRequest struct {
    MaxChunkBytes uint32 // optional adapter frame cap; zero adds no cap; not part of manifest
    Manifest       Manifest
    RemainingTotal time.Duration // relative, not a sender wall-clock timestamp
    ProgressTimeout time.Duration // sender policy; negotiated downward
    Resume         bool
}

type Chunk struct {
    Session Session
    Ordinal uint64
    Offset  uint64
    Data    []byte
    SHA256  [32]byte
}

type Ack struct {
    Session       Session
    NextOffset    uint64
    ChunkBytes    uint32
    State         DeliveryState
    RestartNeeded bool
    RemainingTotal time.Duration
    ProgressTimeout time.Duration
    ReceiverRunID string
}

type DeliveryState string // receiving, verifying, handing_off, accepted, not_needed, failed

type ReceiverOutcome string // unknown, installed, skipped, failed

type Send struct {
    Manifest Manifest
    Data     []byte // immutable borrowed view for this SendSnapshot call only
    ReportProgress func(Progress) // library callback; nonblocking; only verified cumulative Acks
}
type Progress struct {
    ReceiverRunID string
    VerifiedBytes uint64
    State DeliveryState
    RemainingTotal time.Duration
    ProgressTimeout time.Duration
}

type Transport interface {
    SendSnapshot(context.Context, Send) (Ack, error)
}
```

```go
// Service is a borrowed view of one Node lifetime, not an independent owner.
// Transports authenticate the sender before calling ingress methods.
type Service interface {
    Options() Options
    Begin(context.Context, BeginRequest) (Ack, error)
    Write(context.Context, Chunk) (Ack, error)
    Finish(context.Context, Session) (Ack, error)
    Detach(context.Context, Session) error
    Abort(context.Context, Session) error
    SessionDone(Session) <-chan struct{} // closes on attachment fencing/expiry/stop
    List(Query) Page
}
```

```go
type Resolver func() (*Coordinator, error)
func NewService(options snapshot.Options, resolve Resolver) snapshot.Service
func (s *service) Options() snapshot.Options
func (s *service) Begin(context.Context, snapshot.BeginRequest) (snapshot.Ack, error)
func (s *service) Write(context.Context, snapshot.Chunk) (snapshot.Ack, error)
func (s *service) Finish(context.Context, snapshot.Session) (snapshot.Ack, error)
func (s *service) Detach(context.Context, snapshot.Session) error
func (s *service) Abort(context.Context, snapshot.Session) error
func (s *service) SessionDone(snapshot.Session) <-chan struct{}
func (s *service) List(snapshot.Query) snapshot.Page
```

```go
type Status struct {
    Key             Key
    Direction       string // incoming or outgoing
    SnapshotIndex   uint64
    SnapshotTerm    uint64
    TotalBytes      uint64
    VerifiedBytes   uint64
    StartedAt       time.Time
    LastProgressAt  time.Time
    Deadline        time.Time
    Delivery        DeliveryState
    ReceiverOutcome ReceiverOutcome
    ReservedBytes   uint64
    ErrorReason     string
}
type Query struct {
    GroupID string
    Cursor  uint64
    Limit   int
}
type Page struct {
    Items      []Status
    NextCursor uint64
    Truncated  bool
    Incoming, Outgoing int
    ReceiveBytes, SendBytes uint64
}
```

```go
type TransferError struct {
    Reason string
    Size, Limit uint64
    Cause error
}
func (e *TransferError) Error() string
func (e *TransferError) Unwrap() error
```



### Appendix B: engine jobs, owned ingress and exact reports

```go
type SnapshotJob interface {
    Run(context.Context) // owns exactly one terminal managed delivery report
    Discard(error)       // release/report for a job never run; idempotent
}
type SnapshotOffer struct {
    Message raftpb.Message
    Attempt SnapshotAttempt
    PublicationGeneration uint64
    Epoch context.Context // canceled on source term/leadership loss
}
type SnapshotJobTransport interface {
    PrepareSnapshot(SnapshotOffer) (SnapshotJob, error)
}
```

```go
type SnapshotInstallOutcome uint8
const (
    SnapshotInstallUnknown SnapshotInstallOutcome = iota
    SnapshotInstalled
    SnapshotSkipped
    SnapshotInstallFailed
)
type SnapshotIngress struct {
    Message raftpb.Message // owned immutable data; new chunk path only
    Decoded raftstate.Snapshot // fully validated off-loop, exact immutable payload
    Done func(SnapshotInstallOutcome, error) // internal nonblocking completion
}
type SnapshotBounds struct { RunID string; Term, Commit uint64 }
func (g *Group) SnapshotBounds() SnapshotBounds
func (g *Group) PreflightSnapshot(context.Context, raftpb.Message) (SnapshotPreflight, error)
type SnapshotPreflight struct {
    Term, Commit uint64
    NotNeeded bool
    HeaderStepped bool
}
func (g *Group) PrepareOwnedSnapshot(context.Context, raftpb.Message) (raftstate.Snapshot, error)
func (g *Group) StepOwnedSnapshot(context.Context, SnapshotIngress) error
func (g *Group) handleOwnedSnapshot(*request, *loopState)
func (g *Group) handleSnapshotPreflight(*request, *loopState)
func (g *Group) snapshotBoundaryReady(*loopState) bool
func (g *Group) finishOwnedSnapshot(SnapshotInstallOutcome, error)
func (g *Group) finishSnapshotIngressAfterJoin()
```

```go
type SnapshotAttempt struct { RunID string; Term, ReplicaID, Generation uint64 }
func (g *Group) ReportSnapshotAttempt(SnapshotAttempt, raft.SnapshotStatus)
func (g *Group) ReportSnapshotAttemptError(SnapshotAttempt, error) // classified failure companion
func (g *Group) drainSnapshotReports(time.Time)
func (g *Group) refreshSnapshotEpoch()
func (g *Group) NoteSnapshotProgress(SnapshotAttempt, metrics.SnapshotCatchUp)
func (g *Group) recordSnapshotEpisode(snapshotProgressEvent)
func (g *Group) observeNativeMatchAdvance(uint64, uint64)
```



### Appendix C: coordinator ownership/schema, ports and staging interface

```go
type OwnerKey struct {
    Peer load.Peer
    RunID string
}
type Owner interface {
    Key() OwnerKey
    Context() context.Context
    SnapshotBounds() raftengine.SnapshotBounds // exact-run monotonic lower bounds
    PreflightSnapshot(context.Context, raftpb.Message) (raftengine.SnapshotPreflight, error)
    PrepareOwnedSnapshot(context.Context, raftpb.Message) (raftstate.Snapshot, error)
    StepOwnedSnapshot(context.Context, raftengine.SnapshotIngress) error
}
type snapshotOwner struct {
    key OwnerKey
    handle Owner // opaque exact owner supplied by managed ports
}

type snapshotReceive struct {
    key         snapshot.Key
    manifest    snapshot.Manifest
    owner       snapshotOwner
    attachment  uint64
    state       snapshot.DeliveryState
    staging     snapshotStaging
    next        uint64
    chunkBytes  uint32
    digest      hash.Hash
    started     time.Time
    progress    time.Time
    deadline    time.Time
    reservation *snapshotReservation
    cancel      context.CancelFunc
    attached    *snapshotAttachment
    completion  *snapshotCompletion // exactly one finalization/ingress result
}

type snapshotReservation struct {
    direction snapshotDirection
    bytes     uint64
    owner     snapshotOwner
    // release-once state; remains owned until joined
}

type snapshotTerminal struct {
    owner snapshotOwner // exact run for same-process local restart fencing
    manifest snapshot.Manifest // bounded header, never full payload
    ack      snapshot.Ack
    outcome  snapshot.ReceiverOutcome
    expires  time.Time
}

type Coordinator struct {
    options snapshot.Options
    ports Ports
    // Protected by a short-held mutex; no I/O/callback/hash/joins under it.
    incoming map[snapshot.Key]*snapshotReceive
    groups   map[string]*snapshotReceive // current exact local owner checked separately
    terminal map[snapshot.Key]*snapshotTerminal
    incomingCount, outgoingCount int
    receiveBytes, sendBytes uint64
    sequence uint64 // attachment generation; fail on exhaustion
    history []snapshot.Status // bounded ring, monotonically numbered
    digests map[SourceSnapshotID][32]byte // max two completed entries per live run
    // Hash singleflight work is separately owned by admitted outgoing jobs.
    // Context, wait group, closed fence, active outgoing jobs and completion signal.
}
```

```go
// TODO: Add a disk-backed snapshotStaging in the group owner's storage namespace,
// using one stable incoming-snapshot name per group rather than configurable
// per-transfer paths; preserve exact transfer fencing and joined cleanup.
type snapshotStaging interface {
    Append(context.Context, uint64, []byte) error
    ReadAt(context.Context, []byte, uint64) (int, error)
    Seal(context.Context) error
    Materialize(context.Context) ([]byte, error)
    Close() error
}

type memorySnapshotStaging struct {
    data []byte
    written uint64
    sealed, closed bool
}
func newMemorySnapshotStaging(total uint64) (snapshotStaging, error)
func (s *memorySnapshotStaging) Append(context.Context, uint64, []byte) error
func (s *memorySnapshotStaging) ReadAt(context.Context, []byte, uint64) (int, error)
func (s *memorySnapshotStaging) Seal(context.Context) error
func (s *memorySnapshotStaging) Materialize(context.Context) ([]byte, error)
func (s *memorySnapshotStaging) Close() error
```

```go
type Ports struct {
    Authorize func(context.Context, snapshot.Key) (Owner, error)
    ResolvePeer func(context.Context, Owner, uint64) (load.Peer, error)
    ValidateOutgoing func(context.Context, Owner, load.Peer) error
    RegisterDelivery func(Owner, raftengine.SnapshotAttempt, load.Peer, time.Time) (uint64, error)
    ReportDelivery func(Owner, snapshot.Key, snapshot.Ack, error)
    NoteSnapshotProgress func(Owner, raftengine.SnapshotAttempt, metrics.SnapshotCatchUp)
    Transport snapshot.Transport // optional; nil capability returns Unsupported
}
```



### Appendix D: repair observation and fixed episode state

```go
type SnapshotCatchUp struct {
    DeliveryID       uint64        `json:"delivery_id"`
    SnapshotIndex    uint64        `json:"snapshot_index"`
    SnapshotTerm     uint64        `json:"snapshot_term"`
    LeaderTerm       uint64        `json:"leader_term"`
    Destination      ReplicaIdentity `json:"destination"`
    VerifiedBytes    uint64        `json:"verified_bytes"`
    TotalBytes       uint64        `json:"total_bytes"`
    ProgressSequence uint64        `json:"progress_sequence"`
    ProgressAge      time.Duration `json:"progress_age_ns"`
    ProgressTimeout  time.Duration `json:"progress_timeout_ns"`
    RemainingTotal   time.Duration `json:"remaining_total_ns"`
    Phase            string        `json:"phase"` // sending or handoff_grace
    RemainingGrace   time.Duration `json:"remaining_grace_ns"`
    EpisodeAnchorMatch uint64       `json:"episode_anchor_match"`
    EpisodeRemaining time.Duration  `json:"episode_remaining_ns"`
    EpisodeFailed bool              `json:"episode_failed"`
}
// Added to metrics.PeerProgress:
// Snapshot *SnapshotCatchUp `json:"snapshot,omitempty"`
```

```go
type snapshotEpisode struct {
    Destination metrics.ReplicaIdentity
    AnchorMatch uint64
    Deadline time.Time
    Failed bool
}
```
