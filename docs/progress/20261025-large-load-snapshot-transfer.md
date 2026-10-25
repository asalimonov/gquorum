# Large load-snapshot transfer execution

Plan: `docs/plans/20261025-large-load-snapshot-transfer.md`
Target: `docs/brainstorms/20261025-large-load-snapshot-transfer-target.md`
Branch: `feat/large-snapshots-transfer`
Started: 2026-10-25

The user goal named `plans/brainstorms/20261025-large-load-snapshot-transfer.md`; that path does not exist. The unique matching committed implementation plan is the docs/plans path above. The subsequent explicit goal authorizes implementation; the earlier colleague plan review was unavailable due to provider limits and is not claimed complete. Initial checkout had only the existing untracked review transcript, which is left untouched.

## Task 1 — public contracts, policy and errors

Date: 2026-10-25. Commit: this task commit.

Implemented
- Added load/snapshot Options, protocol/service/status contracts and structured TransferError; no runtime selection, root source files or persistent changes.
- Pure option resolution validates platform/header allocation arithmetic, directional products, chunk/window and timeout sums; defaults are configurable and 64 GiB policy validates without allocation on 64-bit platforms.
- Added 42 unit cases/subtests covering defaults/idempotence, >2 GB settings, explicit budgets, malformed/overflow policy, fault/cause preservation and conservative zero values.

Verified
- go test -count=1 ./load/snapshot: pass.
- go vet ./load/snapshot: pass.
- make test: all 2,182 tests passed across 61 package reports and five application modules; 11.453 seconds.
- Report: test-reports/unit/20261025T163340.128319Z-0wxmtozx/report.md.

Notes
- No large payload, network service, Docker or runtime snapshot test was run in this contract-only task.
- Existing StateMachine/Store APIs, wire paths and immutable fixtures are untouched. Constructor capability enforcement remains deferred to Task 15.

Task 1 validation follow-up
- The first commit hook rejected strict production lint findings (named constants, switch default, grouped DTO style and a conversion whose positive guard was outside its function). No commit was made on failure.
- Fixed the code, including a local positive chunk/window guard before the unsigned conversion. Kept target DTO grouping with narrow max-public-structs exemptions, matching repository conventions.
- make lint (pending files only): pass, including unit/functional vet, production checks, revive and unused analysis.
- Re-ran make test after fixes: all 2,182 tests pass; 6.484 seconds. Report: test-reports/unit/20261025T163713.024365Z-mb3uckv2/report.md.

## Task 2 — pure facade and Node construction

Date: 2026-10-25. Commit: this task commit; prior task 821ca49.

Implemented
- Added Node.LoadSnapshots and LoadOptions.SnapshotTransfer in existing root files; only configured managed loads construct a facade. Metadata-only Nodes return nil.
- Resolved/copied snapshot policy during construction with no resolver/callback/I/O; immutable options remain available after stop.
- Added internal pure facade, observation-only fallback, closed unknown-session signal and pure typed-nil-aware RequireTransport helper. Constructor capability enforcement remains deferred to task 15.
- Coordinator methods deliberately refuse with ReplicaNotReady until tasks 6–8; root Host attachment remains deferred to task 12. These are the plan's explicit temporary seams, not completed runtime transfer behavior.

Verified
- go vet ./internal/loadsnapshot .: pass.
- go test -count=1 ./internal/loadsnapshot .: pass, including construction/cancellation/capability and stable options tests.
- make lint (pending changes only): pass after correcting stub receiver names/context helper order.
- Initial make test failed one existing event-bus timing test (2193/2194 passed); report test-reports/unit/20261025T164607.543982Z-v_apavlc/report.md.
- Corrected journal/loss observation order in that test only; go test -count=100 -parallel=1 ./events -run '^TestBus_BoundedIngressAndSubscriberLoss$': pass (1.502s). The pre-fix focused repetition also passed, confirming the observed full-suite failure was schedule-dependent rather than a consistently failing case.
- Re-run make test: all 2,194 tests across 62 package reports/five modules passed in 10.476s. Report test-reports/unit/20261025T165018.031700Z-85j2spjz/report.md.

Notes
- Event test correction is recorded with its diff in the plan's NOT PLANNED CHANGES; no bus behavior/encoding changed.
- No new root files, live application replicas, network services, Docker or large-data tests introduced/run for this task. Existing native snapshot send selection is unchanged.

## Task 3 — snapshot RPC and bounded codecs

Date: 2026-10-25. Commit: this task commit; prior task 4a92e8e.

Implemented
- Added version-1 TransferSnapshot protobuf messages/RPC without changing ordinary RaftFrame v2 or administration fields.
- Added bounded Open/Ack codecs, positive native term/index and identity validation, explicit failure details, safe duration conversions and allocation-free worst-case chunk frame sizing.
- Added malformed/native-safety, frame-boundary, 64 GiB declaration and acknowledgement units; no large payload fixtures.

Verified
- go test -count=1 ./transport/loadgrpc: pass; go vet ./transport/loadgrpc: pass.
- make test: all 2,228 tests across 62 reports/five modules pass (9.403s). Report: test-reports/unit/20261025T171829.622598Z-u8agyk72/report.md.
- make generate twice: identical SHA256 for both load generated files; only intended load protocol generated files changed. proto-check deferred until committed final verification.
- make lint (pending changes): pass after checked conversion and style fixes; git diff --check: pass.

Notes
- Codec file has a temporary unused exemption because production stream callers arrive in tasks 10–11. Remove it at cutover; the codecs already have unit coverage.
- A decoded Ack deliberately contains only the wire delivery ID/attachment; task 11 must validate and bind the immutable request peer key before reporting progress.

## Task 4 — owned native ingress and preflight

Date: 2026-10-25. Commit: this task commit; prior task 7c5a013.

Implemented
- Added exact-run monotonic bounds, off-loop envelope preparation, bounded ownership-aware submission and a guarded header-only native preflight.
- Existing beginReady remains the only Ready consumer. Exact pointer/metadata matching binds cached decode to the write job; native skips ignore unused older authority. Owned tickets bypass WAL batching delay to prevent intervening steps from sharing their capture.
- Owned payloads are not cloned by staging/Restore. Full snapshot persistence retries use the engine lifetime; ordinary WAL writes retain their five-second attempt timeout.
- Done is release-once, queued cancellation keeps ownership, failed tickets complete after all workers join, and completed tickets drop decoded buffers/callback references.

Verified
- make test: all 2,241 tests across 62 reports/five modules pass (15.767s); report test-reports/unit/20261025T172538.636558Z-moc3sasg/report.md.
- go vet ./internal/raftengine; make lint (pending files); git diff --check: pass.
- go test -tags=functional -count=1 -timeout=30s ./internal/raftengine -run '^TestSnapshotIngress_': pass.
- Focused functional race run of the same ingress cases: pass, final run 1.389s after matching the specified retry signature.
- Functional regression packages internal/raftengine, internal/metaengine, internal/metaservice and load: pass, including existing standalone/WAL/full snapshot-tail recovery tests.

Notes
- Two new fixture mistakes were corrected before completion: log setup now goes through native MsgApp instead of modifying initialized MemoryStorage, and the Restore gate lets startup Restore(nil) return. The first hung fixture run was terminated for a stack trace; a second bounded run confirmed that fixture issue. No production workaround was needed.
- Tests explicitly cover zero-term index-ahead attacks, timer/ticket/pending-Ready collision gates, higher-term obsolete headers, matching-log skip, pointer reuse, blocked write/Restore, queue cancellation and joined failure. Only one local engine per fixture; no Docker or large payloads.

## Task 5 — independent outbound jobs and exact reports

Date: 2026-10-25. Commit: this task commit; prior task 717df68.

Implemented
- Added optional SnapshotJobTransport offers, immutable pins and publication generations. Ready retains admitted jobs until persistence/application completes; joined bulk workers run separately from ordinary per-peer senders.
- Added cancelable native leadership epochs and idempotent unstarted-job discard, including Ready failure and shutdown. Managed engineTransport still does not implement the selecting method (Task 15).
- Replaced dropping report channels with exact run/term/peer/generation terminal records and one wake signal. Separate cooldown timers do not depend on Raft tick timing; capacity uses 100ms–1s jitter, admitted failures exponential 1s–30s, permanent refusals 30s.
- Added SnapshotCatchUp DTO and bounded locally timestamped progress storage; no observer/repair behavior is enabled yet. Backoff survives engine-only receiver restart and source epoch changes; actual Match advance or a changed receiver process resets it.
- Added ReportSnapshotAttemptError alongside the specified status-only method so future managed wiring can retain typed refusal classification. Recorded this internal refinement in the plan.

Verified
- make test: all 2,245 tests across 62 reports/five modules pass, 20.313s; report test-reports/unit/20261025T173823.586896Z-q8kw1xn0/report.md.
- go vet ./internal/raftengine ./metrics; make lint (pending changes); git diff --check: pass.
- Focused functional race for all SnapshotIngress/Outbound cases: pass (final run 1.403s).
- Focused unit race for SnapshotReports: pass (1.374s), including concurrent terminal reports with a saturated wake signal.
- Functional regressions in internal/raftengine, internal/metaengine, internal/metaservice and load: pass.

Notes
- A native rejection fixture initially used Index=2 instead of the append prefix Index=1; correcting the protocol-double reply produced the real StateSnapshot path. Another new fixture omitted required entry correlation; fixed before verification.
- Tests prove heartbeat sends while bulk is blocked, epoch cancellation plus join, stale report rejection, native failure cooldown, failed-Ready discard, legacy snapshot copy behavior and publication generation. No multi-node functional fixture or large payload was added.
- Reuse the existing status capture for epoch refresh to avoid a second RawNode.Status allocation on every ordinary refresh. Empty bulk dispatch does no extra work.

## Task 6 — coordinator admission and memory staging

Date: 2026-10-25. Commit: this task commit; prior task dde2701.

Implemented
- Added exact Owner/Ports contracts and volatile transfer state, plus pure coordinator construction, Host lifetime binding, fencing and retryable joined Stop.
- Atomic directional count/byte reservations include exact owner/run validation and a group slot reserved before allocation/publication. Release is idempotent, rejects foreign reservations and cannot erase a successor slot.
- Added memory staging with contiguous owned appends, poisoned failed appends, bounded ReadAt, complete Seal and zero-copy immutable Materialize. Close drops references without zeroing native-retained bytes.
- Added the single future-disk TODO at the staging interface, using the stable group-scoped incoming-snapshot name.
- Added evictable completed-Ack metadata, bounded history and two digest entries per run; none of these caches is an admission permit. Attachment/history counters never wrap.

Verified
- make test: all 2,257 tests across 62 reports/five modules pass (7.748s); report test-reports/unit/20261025T174716.663600Z-17j1vsen/report.md.
- go test -race -count=1 ./internal/loadsnapshot: pass (1.342s); go vet ./internal/loadsnapshot: pass.
- make lint (pending changes); git diff --check: pass.
- Pure units cover independent budgets, concurrent 64-way admission, duplicate release, group successors, stop timeout retention, cache eviction independent of admission, overflow and borrowed-view lifetime. A 64 GiB reservation uses metadata only; no large payload allocation.

Notes
- Receive operations remain explicit unavailable methods in receive.go until tasks 7–8. New state/backend declarations have temporary unused exemptions until those tasks and task 9 connect them; remove them during that wiring/cutover.
- No root, persistent-format, fixture-baseline or filesystem-staging changes. Coordinator Stop currently joins reservations and operation ownership; receiver cleanup/expiry is completed with receiver lifecycle in task 8 before Host attachment.

## Task 7 — authenticated receive, chunks and same-run resume

Date: 2026-10-25. Commit: this task commit; prior task c8b1e07.

Implemented
- Added bounded authorization-operation admission, independent service/native-header validation, exact owner checks before/after waits, native preflight before allocation and post-allocation authorization before publication.
- Added contiguous chunk SHA verification, cumulative offsets, duplicate comparison without clock renewal, fixed negotiated mapping and cached exact-run obsolescence checks. Hashing/staging do not hold the coordinator or transfer metadata mutex.
- Resume fences the old attachment and joins its active mutation before issuing a new generation. Abort/failed chunks keep reservations until joined cleanup. Obsolete pre-handoff work and canceled old owners can be joined before group-slot reuse.
- Added optional transport-local BeginRequest.MaxChunkBytes, preserving the original requested Manifest.ChunkBytes. Documented the gap/refinement in the target and plan; gRPC supplies this cap in Task 10. Recorded the formerly locally ignored target so this refinement and its design basis are reviewable with the implementation.

Verified
- make test: all 2,257 tests across 62 reports/five modules pass (7.956s); report test-reports/unit/20261025T180528.339047Z-ejwvga9h/report.md.
- Focused functional/race receive tests: pass (1.506s), including blocked backend writes, reconnect fencing/join, Abort timeout retention, bounded blocked authorization and Stop ownership, native-skip cache/miss, malformed headers, chunk corruption/duplicates, timeout/obsolete cleanup, source process and receiver-run replacement, post-allocation owner change, and negotiated-cap immutability.
- go vet ./internal/loadsnapshot ./load/snapshot; make lint (pending changes); git diff --check: pass.

Notes
- Finish, diagnostic List and background expiry remain for Task 8; no incomplete native handoff can return success. The memory backend now has production callers, so its temporary unused exemption was removed.
- Cleanup currently handles pre-ingress work. Task 8 must extend it to wait for finalizer/native completion, retain close failures for retry, and guard preemption/Abort races against an entered handoff.
- No generated payload files, root files, new listeners or persistent format changes.

## Task 8 — finalization, observations and joined receiver teardown

Date: 2026-10-25. Commit: this task commit; prior task 174d4f8.

Implemented
- One finalizer verifies the full digest, seals/materializes staging and prepares the complete envelope off-loop before exact-owner native handoff. Duplicate Finish/resume shares its completion.
- Native callbacks only record/signal. A failed native callback racing a successful Step reply cannot publish a false transport refusal. Installed/skipped completion also proves prior acceptance when a waiter was canceled.
- Cleanup joins active writes, finalizer return and any entered native owner before closing staging/releasing its reservation. Failed Close retains ownership for serialized Stop retries. Exact-owner stop, background expiry and group preemption preserve entered native ownership.
- Added bounded active/history List observations and cursor paging, completed-Ack cache replay/TTL and post-stop observations. Accepted delivery remains separate from installed/skipped/failed receiver outcome.
- User steering: request the named colleague's review after this task, then every six implementation commits; cadence is now in the plan.

Verified
- make test: all 2,258 tests across 62 reports/five modules pass (8.712s); report test-reports/unit/20261025T184048.279611Z-yjxgubnr/report.md.
- go vet ./internal/loadsnapshot; make lint (pending changes); git diff --check: pass.
- go test -race -tags=functional -count=1 -timeout=90s ./internal/loadsnapshot: pass (1.470s), including real single-participant engine persistence, blocked Restore, Restore/write failure, native skip and partial-data exclusion.
- Protocol-double cases cover lost final Ack replay, canceled queued ingress, acceptance-versus-failed-callback race, duplicate native completion, off-loop decode cancellation, retained permits, Close/Stop retry, exact-owner stop, detached expiry, and callback/error-inspection separation.
- Pure status tests verify bounded history, bounded filtered pagination and owned result values. Pure admission tests now construct live value state without launching the expiry worker; functional fixtures exercise Start/Stop.

Notes
- No new root files, persistent encodings, listeners or large payloads. Sender, network RPC and managed production activation remain in Tasks 9–15.
- Review request will pin this completed checkpoint and include target/brainstorm/plan. The earlier plan-review session-limit response is not claimed as a completed review.

## Review 1 — reliability and compatibility corrections

Date: 2026-10-26. Commit: this correction commit. Review anchor: 251a594. This is the first implementation commit after the anchor; five more commits trigger the next requested colleague review.

Findings accepted and fixed
- P1-1: the engine now consumes owned snapshot/preflight requests only at a safe Ready boundary. Fully prepared data waits in the existing bounded queue through a writer stall or WAL batching timer. Handler refusal remains a defensive invariant check.
- P1-2: legacy ReportSnapshot retains immediate native delivery, including failure, through the non-lossy mailbox. Managed exact-attempt reports alone use the cooldown policy.
- P2-1: recovery checkpoint publication/normalization increments the publication generation; legacy snapshots do not require the bulk digest generation.
- P2-4: Restore now reads and validates the exact ingress ticket bound to its persisted write job.

Verified
- make test: all 2,259 tests across 62 reports/five modules pass (11.507s); report test-reports/unit/20261025T185952.913055Z-2jlssmiu/report.md.
- Pending make lint, focused go vet ./internal/raftengine and git diff --check: pass.
- Focused functional SnapshotIngress/Outbound race tests: pass (1.420s), including queued handoff through a blocked HardState write and explicit Ready timer, immediate legacy native reports for _meta and standalone engines, and both recovery publication paths.
- Focused SnapshotReports unit race: pass (1.299s).
- Functional regressions: internal/raftengine, internal/metaservice, load and tests/recovery all pass (5.885s / 23.294s / 1.613s / 54.947s).

Remaining review work
- P2-2, P2-3a and P2-5 form the next performance/observation checkpoint. Constant-time Authorize remains an explicit Task 12 wiring check. The full response and dispositions are retained in review Thread 3; no gRPC/Host runtime verification is claimed yet.

## Review 1 — performance and observation corrections

Date: 2026-10-26. Commit: this correction commit. Prior correction: 1bb509c. This is implementation commit two after review anchor 251a594; four further commits trigger the next requested review.

Implemented
- P2-2: idle epoch refresh avoids report locking/map work when no records exist, and membership checks use existing voter maps. The mailbox caches its earliest deadline; unchanged deadlines do not rescan records or reset timers on every event. Timer consumption explicitly permits rearming a retained deadline.
- P2-3a: duplicate bytes compare directly in memory staging, with a bounded ReadAt fallback for future backends. A chunk uses one cancellable operation context; attachment cancellation reuses it. Coordinator wake channels are recreated only while a join waiter exists, with waiter registration and state changes serialized to avoid lost wakeups.
- P2-5: active-to-completed observations retain the same row ID. Cursor continuation no longer repeats a transfer after completion; fresh reads still see its updated final state.

Verified
- make test: all 2,261 tests across 62 reports/five modules pass (10.638s); report test-reports/unit/20261025T192221.058706Z-wu8r1x7w/report.md.
- go vet ./internal/raftengine ./internal/loadsnapshot; pending make lint; git diff --check: pass.
- Focused default-package race: raftengine and loadsnapshot pass (1.487s / 1.488s).
- Focused functional ingress/outbound/receive/finish race: both packages pass (1.454s / 1.331s).
- Microbenchmarks on this Apple M4 Pro, 100ms benchtime: idle epoch refresh 63.03 ns before versus 3.618 ns after, both 0 B/op and 0 allocs/op. This measures only the no-record helper, not end-to-end throughput.
- Idle coordinator wake: 2.265 ns/op, 0 B/op, 0 allocs/op. In-memory 1 MiB duplicate comparison: 18,659 ns/op, 0 B/op, 0 allocs/op. Bench fixtures are generated in memory and not committed data.

Notes
- Initial allocation assertions inside parallel unit tests were rejected by Go 1.26 AllocsPerRun, which changes GOMAXPROCS and disallows parallel tests. Moved measurements to benchmarks, preserving parallel unit isolation.
- The colleague's allocation wording was broader than measured behavior: the old voter-map work was stack-allocated here (already zero heap allocations); the measured improvement is CPU work.
- The remaining P2-3b obligation is the future Task 12 O(1) authorization routing index. RPC teardown and Host stop ordering remain later-task qualification gates.

## Task 9 — source pins, digest sharing and verified progress

Date: 2026-10-26. Commit: this task commit; prior c24dfd2. Three implementation commits after review anchor 251a594; next colleague review after three further commits.

Implemented
- Bounded pure source admission pins the immutable native slice without cloning. Resolve, block-cancellable hash, exact delivery registration and transport run after dispatch. Prepared jobs expire on epoch/total cancellation; active jobs retain ownership until transport and reporting return.
- Two completed digests per live run, singleflight misses and publication-scoped keys retain no payload. Failed hashes are not cached. Capacity retries reuse the digest and release bytes before cooldown.
- Exact receiver-run/offset/final-Ack validation, local verified-progress clocks, duplicate suppression and joined timeout monitor. All verified bytes switch finalization to the original total deadline. Stale external progress callbacks have no effect after transport return.
- Added a private exact-attempt failure port for pre-registration failures and outer-fault retry classification; target/plan record both refinements. Successful Stop clears borrowed ports and digest owner references only after joins.

Verified
- make test: 2,262 tests pass across 62 reports/five modules (11.237s); report test-reports/unit/20261025T195027.628453Z-g0ul_nt9/report.md.
- go vet ./internal/loadsnapshot ./internal/raftengine: pass. SnapshotReports unit race: pass (1.290s).
- loadsnapshot functional race, five repetitions: pass (1.730s), including borrowed old bytes, failed-hash sharing/cancellation, publication change, repeated capacity refusal, stale callbacks, missing/duplicate progress timeout, finalization total timeout, prepared-job expiry and transport/report actual-return joins.
- Pending make lint and git diff --check: pass.

Notes
- Report notification intentionally precedes pin release; the prepared-cancellation test joins exact-owner ownership before asserting release, avoiding a false test race.
- Managed ports, transport network integration and repair policy consumption remain later tasks. No listener, root source, persisted format or large fixture was added. Unrelated .gitignore edit is preserved and excluded.

## Task 10 — bounded gRPC receiving and pinned teardown gate

Date: 2026-10-26. Commit: this task commit; prior 526efec. Four implementation commits after review anchor 251a594; next review after two further commits.

Implemented
- Snapshot RPC on the existing load registration. A private optional service provider preserves ordinary-only doubles until Task 15. Bind copies resolved Node policy; no additional port or listener.
- Separate pre-Open quota max(8, 2*MaxIncoming), min(3s, local policy, RPC deadline) handshake lifetime, authenticated 2*MaxIncoming quota and bounded single Recv/Send pumps. Ordinary call admission is independent.
- Handler return causes actual gRPC stream cancellation; an adapter-owned finalizer joins both pumps and the returned handler before releasing its quota/wait-group owner. Blocking authentication/Node calls retain ownership through actual return and Close retries.
- Exact-generation dispatch, separate checked frame cap, immutable requested chunk size and verified-prefix/total phase clocks. Accepted final Ack remains writable when native cleanup closes its completed SessionDone; target clarification recorded.

Verified
- make test: 2,262 tests across 62 reports/five modules pass (8.116s); report test-reports/unit/20261025T195628.191729Z-k_nf21z2/report.md.
- go vet ./transport/loadgrpc; pending make lint; git diff --check: pass.
- Required real gRPC 1.79.3 functional race gate with stats.Handler, five repetitions: pass (1.659s). Covers empty and admitted half-open Recv, flow-control-blocked Send, attachment replacement, Abort, progress deadline, owner/session stop, adapter Close and exact single RPC completion with joined pumps/quotas.
- Cancellation-ignoring authentication/Write callbacks retain their permits until actual return; Close timeout/retry joins. Saturated pre-Open slots coexist with a progressing authenticated transfer and successful ordinary ReadReplica RPC. Invalid versions, pre-Open data, repeated Open and stale attachment refuse without mutation.
- Full owning-package functional suite passes (0.369s). Transport-local 1024-byte frame test preserves the 4096-byte manifest request and supplies a smaller explicit cap.

Notes
- The pinned pump teardown feasibility gate passes; no architectural replacement or suppressed race was needed. Re-run it on gRPC upgrades.
- Source networking and managed activation remain later tasks. No new root source, port, persistent encoding or large fixture. The unrelated .gitignore change remains excluded.

## Task 11 — independent bulk clients, pipeline and retained resume clocks

Date: 2026-10-26. Commit: this task commit; prior 2168c6e. Five implementation commits after review anchor 251a594; request the next colleague review after Task 12 (sixth commit), counting any additional correction commit.

Implemented
- Snapshot-only bounded outgoing admission and ClientConn cache use the existing caller resolver/load address while remaining independent of ordinary Raft/control connections. Close cancels/joins all accepted send work before closing bulk connections.
- Independent joined chunk sender/Ack receiver enforce the negotiated window, exact session/run/chunk mapping and cumulative prefix bounded by issued chunks. No Data before admission, no Finish before all bytes are verified, no callback after joined return.
- Resume preserves the original manifest and total/progress clocks, accepts tighter policy only, and joins old workers before replacement. Transient stream loss uses 100ms-to-2s capped jittered backoff; permanent/capacity/restart-required refusals end this delivery without unary fallback.
- Header/frame/size validation, exact typed errors and native-proven not_needed remain distinct from accepted full receipt. Effective callbacks/Acks use locally narrowed timings; synchronous timeout checks prevent a late Ack from renewing an expired clock.
- Removed the codec's temporary unused exemption now that both directions have production callers.

Verified
- make test: 2,272 tests across 62 reports/five modules pass (8.503s); report test-reports/unit/20261025T200445.349913Z-oalwwdg9/report.md.
- go vet ./transport/loadgrpc; pending make lint; git diff --check: pass.
- Owning-package unit race passes (1.311s). Functional snapshot RPC race suite, three repetitions, passes (2.843s), including the required server teardown gate.
- Lost Open/chunk/final Ack resume with unchanged manifest; native not_needed sends no payload; unsupported peer and typed capacity/restart refusals do not retry/fallback. Missing/duplicate admission progress expires, total lifetime survives reconnects, finalization after verified bytes uses total lifetime, frame mismatch refuses before Data, and callbacks/workers join on Close.
- An eight-MiB generated transfer blocks on bulk flow control while ordinary ReadReplica succeeds to the identical address over a distinct ClientConn. A three-chunk window test observes exactly three issued chunks while the first verified write is blocked; outgoing admission also remains held during a blocked Begin.
- Pure regressions reject wrong delivery/run/attachment, changed chunk mapping, unaligned/unissued offsets and premature accepted Acks; duplicate and late Acks cannot renew progress.

Notes
- No new listener, root source, persisted encoding or committed payload. Ordinary legacy snapshot routing remains selected until Task 15; managed Host wiring is Task 12. Unrelated .gitignore edit remains excluded.

## Task 12 — managed ownership, exact ports and lifetime joins

Date: 2026-10-26. Commit: this task commit; prior 1bd6373. User steering canceled subsequent colleague reviews; no six-commit request is due and none will be sent.

Implemented
- Host constructs/starts/fences/stops one coordinator from copied Node policy. Node's existing accessor/resolver reaches it; publication remembers the coordinator for observation-only source history after Stop.
- Exact Owner wrappers pin slot/core/run/context, allow unpublished running replicas to catch up and revalidate before native handoff. A refusal before native enqueue completes its owned callback; accepted ownership cannot be undone by a later check.
- Private ports resolve/validate off-loop, register the exact native attempt and original deadline, and retain one terminal result against expiry. Ordinary adapter reports cannot consume job tokens; stale destination failures still end only their original source attempt.
- Replica release requests engine stop before StopOwner, then joins core/application ownership. Host-wide Stop/readmission joins all coordinator reservations before completion/new Host creation; timeout and failed Close retain the old owner.
- Closed P2-3b: load-peer catalog lookup was already indexed; added an applied derived process index to remove the remaining per-chunk member/admission/session/lock scans. No new persisted field or baseline encoding.
- Source selection remains a private lowercase prepareSnapshot bridge, explicitly tested not to satisfy SnapshotJobTransport before Task 15.

Verified
- make test: 2,274 tests across 62 reports/five modules pass (10.504s); report test-reports/unit/20261025T201829.891258Z-j2qnev5s/report.md.
- Owning-package vet, pending make lint and git diff --check pass.
- Focused managed snapshot/shutdown race regressions, five repetitions: pass (4.187s). Full managed functional race at parallel=8 passes (14.615s). Covers blocked source actual return, received native Restore, failed Close/retry, unpublished startup skip, old coordinator/new run resume refusal, original delivery deadline/private CAS and retained permits.
- Node binding race passes (3.048s); expanded Node binding/readmission/factory ownership race selection passes (14.772s).
- loadsnapshot/metaengine unit races pass (1.422s / 3.853s), including source-only observation fallback and index session/lock/revoke/restore equivalence. Existing replacement-admission routing regression remains green.
- Recovery functional suite passes (67.121s). Metaservice functional suite passes at repository parallel=8 (16.416s).
- Process lookup microbenchmark, 100ms runs on Apple M4 Pro: 1/100/10,000 identities take 14.80/14.93/15.38 ns/op, all 0 B/op and 0 allocs/op. This measures lookup only, not metadata rebuild cost or transfer throughput.

Findings and corrections
- The first broad managed race run exposed an existing 20ms pre-admission timing assumption. Replaced it with a stopping-state barrier before canceling; the original resource assertions remain. All subsequent focused/full managed race checks pass.
- A simultaneous unconstrained metadata/recovery/compile run hit several metadata fixture deadlines. Re-running the metadata suite alone at the documented eight-worker limit passes without changing those tests.
- No root file was created, no large fixture/persistent transfer record was added, and the unrelated .gitignore edit remains excluded. Further implementation proceeds without colleague review per the user's latest instruction.

## Task 13 — production fixture transport and capability migration

Date: 2026-10-26. Commit: this task commit; prior fce9876. Further colleague review remains canceled by the user.

Implemented
- Process fixture uses a separately bound production adapter per Node generation and production ordinary Raft/snapshot RPCs on its existing load listener. Both existing service names are registered once; per-RPC implementation capture prevents redirecting old work into a successor. Node joins before adapter Close, which must finish before replacement publication.
- Preserved legacy application/control service and custom protocol wrappers, read-send/receive tracking and authenticated drop_raft/drop_append snapshot refusal. Snapshot drops cannot report successful delivery.
- Fixture-owned chunk/Finish/receipt and actual Ack-write fault seams allow bounded corruption/delay/stream-loss controls without modifying the library. Ack pumps capture the exact old service. Counters distinguish header-only not_needed from accepted full handoff and suppress duplicate accepted replies while retained; installation remains the Node/application observation.
- Managed, root, recovery and API-network loopbacks now call the real service Begin/Write/Finish contract and report cumulative progress. Unused constructor doubles expose a capability that refuses/panics on actual use; no success stub. Ordinary-only adapter Ingress doubles expose an explicit unsupported service; exercised snapshot doubles retain their real protocol implementations.
- Inventory found scheduler/cache examples already use production loadgrpc and examples/cluster plus tests/sim remain meta-only. No simulation dependency or extra root file.

Verified
- make test: 2,275 tests across 62 reports/five modules pass (8.216s); report test-reports/unit/20261025T204709.541309Z-8qlc935k/report.md.
- Pending make lint passes owning vet/revive/unused and compile-only clustertest/example variants. git diff --check passes.
- Full fixture functional race suite at parallel=8 passes (18.021s); new focused production registration/authentication/blocked-Ack-generation cases also passed (4.601s). Coverage proves ordinary production RPC wiring, wrong-credential refusal before transfer admission, adapter ownership after client/Node stop, and exact old recipient refusal after retained restart on the same listener.
- Full transport functional race suite passes (1.861s), including the pinned server/client teardown gates. Managed/recovery functional suites pass (33.975s / 78.124s) during the broader checkpoint; focused recovery race passes (6.561s).
- Process module compiled with clustertest using go test -c in a scoped temporary directory, exit 0. TestMain/Docker and large generators were not executed. Cleanup completed without retained artifacts.

Findings
- A broader concurrent run exposed an existing projection cancellation-accounting assumption; test assertions now retain the full-state/join guarantees while allowing its documented one canceled attempt. Recovery readiness now uses the existing job diagnostic waiter; subsequent full fixture race passes.
- Initial heavily overlapping checks increased unit wall time to 61.817s; the final routine checkpoint is 8.216s. No default large allocation/generation was introduced.
- The unrelated .gitignore edit became staged externally during this task. It remains preserved and is excluded using an explicit-path commit.

## Task 14 — bounded repair catch-up episodes and shared failure evidence

Date: 2026-10-26. Commit: this task commit; prior 797adba. No further colleague review requested per user steering.

Implemented
- Optional PeerProgress.Snapshot publishes the complete planned DTO, with owned pointer clones and omission when absent. Native owner sampling alone exposes progress; worker callbacks cannot refresh Raft stamps, Match, RecentActive or AckSequence.
- One source-owned episode per native peer anchors historical Match and a locally configured TotalTimeout+ProgressTimeout deadline. Bytes, retries, artifacts, source term renewal and receiver engine-only run changes preserve it. Total/progress timeout latches failure; strict native Match advance or a changed process/durable peer permits a new episode. Native peer retirement removes it.
- Added private generic policy and timeout-report inputs omitted from the original sketch, preserving the engine/load package boundary. Immutable owner-core forwarding avoids a Host lock on each progress Ack.
- Relative progress, total, grace and episode durations age at native/meter/transit/cache boundaries. Invalid negative remaining values cannot wrap into fresh protection. Sending requires fresh normal target Raft evidence; bounded handoff grace tolerates Restore while rejecting contrary run/term/stopped evidence.
- Observer suppresses only lag/stall predicates, leaves raw numeric lag visible and records snapshot_catchup/snapshot_transfer_failed reasons. Independent unavailability, learner promotion and quorum checks remain unchanged. Built-in policy and mandatory votercheck consume the same derived evidence.

Verified
- make test: 2,298 tests across 62 reports/five modules pass (7.967s); report test-reports/unit/20261025T211313.995417Z-8iaem78v/report.md.
- Focused owning vet, pending make lint and git diff --check pass.
- Default-package race checks: metrics 1.317s, internal/metrics 1.353s, observe 1.543s, raftengine 1.285s and scheduling 1.217s; all pass.
- Real one-engine snapshot ingress/outbound functional race selection passes (1.483s). New case proves native-only publication, no fabricated Match/RecentActive/AckSequence, timeout latch across receiver-run churn and cleanup at native peer retirement.
- Controlled-clock units cover retry/term/run changes, process replacement, unknown receiver run, native Match reset, bounded grace, expired/stale/zero-byte/contrary target evidence and cached reads. Later fresh progress cannot reset a failed episode's ordinary failure window. Pointer clone, JSON omission, duration arithmetic and shared policy/mandatory-check agreement regressions pass.

Notes
- _meta/standalone use a zero generic snapshot policy and retain legacy send/report behavior. Full four-node repair and process transfer scenarios remain Tasks 17/20; no Docker or large generation was run here.
- Activation is still deferred to Task 15. The unrelated staged .gitignore change remains excluded from feature commits.

## Task 15 — atomic managed cutover and ordinary-path closure

Date: 2026-10-26. Commit: this task commit; prior a1d1412. Further colleague review remains canceled.

Implemented
- Managed engineTransport now implements the actual selecting PrepareSnapshot method. Native Ready selects common source admission/jobs for every managed MsgSnap, preserving generic _meta/standalone transport behavior.
- NewNode performs pure missing/typed-nil snapshot capability refusal. loadgrpc.Ingress requires LoadSnapshots; Bind captures a nonnil borrowed service with already-resolved policy and checked frame feasibility. Removed the optional provider and default-policy bridge.
- Ordinary managed ingress, managed sender and production loadgrpc sender/codec reject MsgSnap. Removed the adapter's old unary snapshot send/report path; snapshot jobs alone own terminal managed reports.
- Removed transitional unused exemptions and renamed the private preparation bridge. Migrated obsolete ordinary-snapshot tests/benchmarks to refusal semantics, retaining ordinary token identity/expiry and snapshot reservation-window regressions.
- Added real native Ready → Node-owned managed coordinator → production gRPC → common receiver ingress → exact native report integration. One actual managed engine adds a protocol-double learner through authorized membership, sends a native snapshot and resumes append probing only after the terminal report. The double reports injected installation failure after accepting ownership, proving accepted transport is not an installed claim. Public Node construction/Bind/Start composition is covered by the prior Node regression; real multi-node process qualification follows in Tasks 17/20.

Verified
- make test: 2,299 tests across 62 reports/five modules pass (17.887s); report test-reports/unit/20261025T212345.398911Z-f3facj7l/report.md.
- Owning pending lint/vet, merged unused analysis, git diff --check and process compile-only clustertest binary pass. No TestMain/Docker run.
- Full managed/adapter functional race checks pass (14.171s / 1.874s), including the native activation path, safe-boundary queue ownership, legacy _meta/standalone reporting and pinned gRPC teardown.
- Full fixture functional race passes (35.179s). Recovery functional suite passes (69.893s). Full public Node functional suite passes on its own (15.098s).
- Constructor tests reject ordinary-only and typed-nil transports without callbacks; Bind tests reject nil/unresolved services. Ordinary mixed batches refuse snapshots before any partial dispatch or fabricated report.

Findings
- The new integration test's first cleanup used t.Context, which Go cancels before cleanup. Switched only cleanup to a fresh bounded background context; the actual protocol run and ownership assertions remain unchanged.
- An overlapping broad run timed out the existing storage-lease test during operation convergence and its intentional first Close failure. It passed alone (4.155s), then the complete public suite passed sequentially without changing its timeout or production storage behavior.
- No runtime rollout flag, giant unary fallback, additional listener, root file or persistent format change. The unrelated staged .gitignore change remains excluded.

## Task 16 — explicit generated state and identity-bound phase controls

Date: 2026-10-26. Commit: this task commit; prior 0c1daca.

Implemented
- Explicit replicated synthetic configuration extends the fixture FSM; ordinary state remains small JSON. Generated binary snapshots preserve KV/dedup state, logical counter, prepared/replay records, deterministic seeded bytes and full integrity digest. Restore validates all bytes and semantics with bounded scratch; cancellable block hashing/generation avoids another full fixture allocation.
- Authenticated group/replica endpoints configure generation through public Group.Propose, force snapshots and return compact state/manifest/progress observations. Generation above the local encoded allowance refuses before materializing data. Requested encoded budgets reserve 64 KiB engine framing headroom; qualification observes actual manifest length/digest/index/term.
- Exact node/process/group/replica controls implement one-shot admission/chunk/final Ack loss at actual wire writes, bounded chunk corruption/delay and before/after Finish/handoff, persistence and Restore gates. Canceled gates join and retain active counts until actual return. Old identities cannot affect successors.
- Snapshot options now flow from process Spec through one JSON flag into Node creation. Borrowed fixture WAL provides persistence gates; Node-owned WAL explicitly refuses them. Existing meta/load listeners and transport services remain unchanged.

Verified
- make test: 2,306 tests across 62 reports/five modules pass (18.933s); report test-reports/unit/20261025T213712.819137Z-6vornj6k/report.md.
- Full fixture functional race suite passes (16.782s); final focused generator/control/authentication/cancellation race checks pass (2.867s).
- Pending owning vet/lint, clustertest/example compile variants, git diff --check and explicit process go test -c compilation pass. No TestMain/Docker or large generator ran.
- Pure cases verify deterministic complete-state restore plus committed tail/dedup, corruption/truncation/forged digest/JSON-only rejection, no publication on failed restore, cancellation and small ordinary snapshots. Functional controls reject absent credentials/stale identities and oversize generation; phase cancellation and one-shot wire loss/corruption preserve ownership and borrowed data.

Notes
- Initial identity capture assumed every focused fixture had a live Node; fixed capture to tolerate protocol-only fixtures while running controls retain exact identity checks. Existing tests remain green.
- No payload/testdata file, implicit storage root, extra listener or new root file. Generated qualification remains opt-in and unexecuted until the process tasks. Unrelated staged .gitignore remains preserved and excluded.
