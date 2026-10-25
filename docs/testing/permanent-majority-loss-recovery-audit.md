# Permanent-majority-loss recovery architectural delta audit

Date: 2026-10-25. Baseline: `26842024d2fb4fe6fd74b15efa5106749a8b0674`.
This is Task16 of the [implementation plan](../plans/20261025-permanent-majority-loss-recovery.md).
The [target](../brainstorms/20261024-permanent-majority-loss-recovery-target.md)
and its approved refinements define the contracts. The
[validation record](permanent-majority-loss-recovery.md) records actual suite,
resource, compatibility and benchmark results. Review discussions are retained
separately; they are not execution authority.

The audit covers gquorum and its independent caller fixture. Meridian's wiring,
deployment, SQL repair and business consistency checks remain outside this plan.
There is no new HTTP server in the library, SQL schema, remote cache, migration,
public executable manifest or rollback API.

## Contract and implementation map

| Target | Actual implementation and verification |
| --- | --- |
| §§1–2, operational boundary | `Recovery` is a resource-free configuration handle; `recovery_run.go`/`recovery_cancel.go` acquire one per-call owner and borrow `recoveryexec.Executor`. Node alone owns runtime application resources. Testapp maintenance jobs persist intent before stopping its Node and remain reachable without quorum. |
| §3, public signatures | `recovery.go`, `recovery_{inspect,approve,run,cancel,status,start_options}.go` implement exactly NewRecovery, Inspect, Approve, Run, Cancel, Status, List and RecoveryStartOptions. DTOs/constants/Refusal are in `recovery` through private model aliases. `load.ReplicaSpec.Recovery` is optional; GroupFactory, storage, lifecycle and transport interfaces are unchanged. `recovery_test.go` checks constructor purity, copies, injected-store refusal and exact helper transformations. |
| §3.3, resource defaults | `recoverymodel.ResolveResources`: 64MiB encoded frame, 1GiB selected snapshot/logical tail, 128MiB logical metadata. Approval latches effective bounds. `recovery_resources_func_test.go` refuses metadata/frame/group overages before approval, without callbacks or source changes. `TestRecoveryExactSourceGroupBoundAllowsDestinationMetadata` checks the inclusive source boundary through actual B startup. |
| §4, remote workflow | The downstream implementation is deferred. Its equivalent authenticated test routes are registered by `tests/cluster/testapp/recovery_http.go`; `recovery_job.go`, `recovery_worker.go`, `recovery_files.go` own durable maintenance, acceptance, joined workers and one node-wide Backup/Undo. No request accepts filesystem paths. Remote job tests and three-process permanent-loss tests exercise these routes. |
| §5.1, roots/ownership | `recoverydisk.Owner` binds canonical paths, root descriptors, stable locks and mount identity. System-only, separate WAL and nested WAL roots are supported; system inside WAL, aliases, private roots, links and replaced/unavailable roots refuse. WAL lease transfers to the Node-owned backend; joined teardown releases it. `owner_func_test.go`, `archive_io_func_test.go` and backend ownership/collection tests cover these boundaries. |
| §5.2, local records | `recoverymodel/journal_types.go` contains Current, LocalManifest, AllocationFloor, SeedSource, ArchiveEntry, ActionAudit, FilesProgress, HookProgress, SeedProgress, SeedMarker and private seed/checkpoint input types. Strict canonical codecs/validators enforce schema1, tags, immutable acceptance, unique inventory, phases, write-once audit/marker, monotonic revisions and attempts. `manifest_validate_test.go` checks exact schema keys, digest and CAS. |
| §5.2, publication/durability | `recoverydisk/approval_publication.go`, `atomic.go`, `journal*.go` publish a synced manifest directory before current.json. Unpublished recognized staging never authorizes; unknown staging refuses. Renames require explicit nonreplacement under exclusive ownership and no-follow parent descriptors, with both parents synced. All private writes remain synchronous independently of ordinary DisableFSync. |
| §§5.2/11.5, status/history | Read-only discovery validates directory ownership and excludes duplicate IDs; valid rows survive unrelated errors. It does not open live WAL or compare the evolving live pin with historical A. History is an owned observation, bounded to256 events,512 UTF-8 message bytes and4096 encoded bytes/event. Approval reserves the complete event budget plus future progress/header growth. `history_test.go`, `history_func_test.go`, `discovery_func_test.go` and `recovery_status_func_test.go` cover this. |
| §§5.3/6, source inventory | `recoveryinspect` opens metadata/authority read-only, runs metadata with nil persister, reads approved Target and independently proven declarations, classifies every WAL and retains staged/replacement allocation floors and tombstones. Missing groups, unknown live replicas, unreadable metadata and ineligible donors block. Reset archives proven ownership without claiming replay. Inventory, approval, unknown-WAL and Docker sparse-hosting tests exercise this. |
| §§6/10.3, strict interpreter | `walbackend/offline*.go`, `strict_{frames,log}.go` validate framing, CRC chain, segments, snapshot/commit/hint/terms before application acquisition. `raftengine.Replay` uses shared restored-core/native/authority interpretation without workers or persister. `CommittedPrefix` excludes uncommitted suffixes without changing A. Offline reader/replay tests assert unchanged bytes and commit-only behavior. |
| §§7.1–7.3, metadata | B is normally founded as v1. `metaengine/recovery*.go` atomically imports approved definitions as one normal v2 approved operation; it imports nonreuse floors/tombstones and retains the full RequestDigest. Old sessions/locks/work/debt remain in A's backup. New host/native effects follow normal approved grants and receipts. Exact import retries resolve lost replies without a second stage/approval. |
| §§7.2/10.4, pending gates | Manifest-owned pre-import gating hands off to the replicated Recovery record. Public mutations, join, repair, leader transfer and ordinary courier proposals cannot cross it. Private commands require the local accepted owner. The committed FSM predicate independently repeats the restriction and returns refusals without halting apply. Real committed-kind trace coverage includes BeginEffect, RecordFence, RecordEffect, transition, consumption and completion. |
| §7.4, retained policy | `recoveryplacement` is a private adapter used by service preparation/count/report paths, planning, FSM validation and repair. It retains original desired policy and allows only improving exact imported deficits. Unrelated ordinary groups remain strict. Core voter/catch-up/domain/pin/native-quorum guards remain. Release requires proven satisfaction or a valid approved policy change and stays released after pruning/restart. Pure safety tests and strict three-AZ Docker regrowth cover singleton→two→three. |
| §§8.1–8.2, seed | The private managed adapter supplies shared administrative replay to `walbackend.InstallRecoverySeed`. It validates accepted backup identity/digest/positions, preserves source application bytes/index/term/request/correlation in(S,C], blanks validated old administrative entries and encodes singleton B authority/configuration at S. B starts with source term, Vote0, commit C and applied hint S. Marker/control distinguish building, prepared, installed, acquired and consumed; seed-state names cannot collide with a group named load. |
| §8.3, application/engine | The ordinary factory transfers successful Application.Close to Node even after waiter cancellation. Private startup Restore/checkpoint uses the owned lifetime, while factory and ordinary startup contracts remain. Recovery protection precedes group acquisition, forces durable writes and checkpoints through the serialized engine writer, supports same-index normalization and persists local consumption before restoring ordinary durability. |
| §§8.4/9/10.5, publication | `recoveryruntime.Coordinator` owns control only; Host owns exact replica reservations and serial reconstruction. Private-ready finishes approved effects with no public handle/Run and rejects forwarded business proposals. After confirmed replicated completion, Host publishes handles for readiness validation, finishes protection at joined boundaries, journals local Complete and releases Run under a final stop/hold check. Failure retains owned state for retry/Stop. |
| §9.4, reset/admission | Reset returns JoinReady to the named destination; the pure helper requires join seeds and clears founding. Early startup marks SuccessorStartedAt before acquisition in both modes. Ordinary exact admission then records JoinedIdentity. Returning A cannot join B through ordinary mismatched pins or stale packets; the three-process test explicitly resets both old members, catches them up and restarts all nodes. |
| §§10/15, composition | Public facades, private ports, replay closure, seed journal callbacks, scheduler adapter and lifecycle handoff are constructed through Node/Recovery. Private implementation is split among cohesive files rather than creating extra public interfaces. The planned node-filtered model.SourceInventory is consolidated into the owned complete metaengine.RecoveryInventory, with filtering in recoveryinspect. The root cancellation-resume wrapper is consolidated into `recoveryexec.Executor.resumeCancellation`; no redundant root owner exists. Checkpoint-only consumption replaces the former RecoveryEvidence read port as explicitly approved. |
| §12, compatibility | Metadata/envelope4, authority2, engine2, WAL identity/applied1 and unrelated bucket/field encodings stay unchanged. Optional replicated Recovery schema1 and separate local files carry provenance. Existing physical/native/scoped fixtures are unchanged; frozen importing/complete schema1 fixtures round-trip. The captured old2684202 decoder rejects the unknown recovery field. No protocol/generated source changed. |
| §13, porting | Meridian supplied scenarios and sequencing only. No commit reconstruction, max(commit,applied), source truncation, manual metadata genesis, old lineage reuse or external application wiring was ported. |
| §§14/16, gates/exclusions | Unit, functional, race, Docker cluster, example, simulation, performance and resource evidence is recorded separately. Unsupported recovery stores and rollback refuse. Full-buffer resource cost is measured; donor merging/dropping, backup purge, debt waiver, streaming/RSS guarantees, downgrade and downstream integration are explicit exclusions with comments at the relevant private boundaries. |

## Crash/resume matrix

These are executable assertions, not a claim that process SIGKILL can reproduce
every filesystem durability cut. Owning-package tests inject faults through
instance dependencies; process tests complement them with actual hard kills.

| Target §11.3 boundary | Existing test and essential assertion |
| --- | --- |
| Inspect/awaiting confirmation | `TestPermanentMajorityLossRemoteCrashResume/prepared`: maintenance and exact effective digest survive a killed process; acceptance remains absent and A does not auto-start. |
| Manifest published, pointer acknowledgement lost | `TestApprovalPublicationFaultsPreserveSourceAndGuard`: all manifest/current sync/rename cuts; a published manifest guards startup and exact retry repairs the pointer without changing acceptance time. |
| Approved before Run | `TestRecoveryResetArchiveAndBeforeBackupCancellation`: no hook/archive before Run; Cancel permits the original runtime without Undo when no attempt ran. |
| Backup attempt/result uncertain | `TestRecoveryCancelledWaiterRetainsCallbackAndRootOwnership` and `TestPermanentMajorityLossRemoteCrashResume/backed_up`: cancellation does not release a live callback owner; resumed Backup is idempotent. |
| Cancelling/Undo interrupted | `TestRecoveryCancelArchivesNoLibraryDataAndResumesOnlyFailedUndo` and `TestPermanentMajorityLossRemoteCancelUndoRetry`: failed Undo retains cancellation/guard; Run resumes only Undo; exact audit survives retries. |
| Cancelled, pointer clearing lost | `TestCancellationHistoryAndMatchingPointerClearing`: stale pointer cannot reactivate work, only a matching pointer clears, cancelled ID remains reserved. |
| Archiving before first rename | `TestPhasePublicationReopenAndConcurrentReaders`: Archiving is durable and cancellation refuses even before a move. |
| Individual archive rename/sync | `TestArchiveMoveFaultsPreserveEvidenceAndRequireJoinedSync`, `TestRecoveryReconcilesMovedBytesWithoutMovedJournalAcknowledgement`: exact intent resolves source-absent/destination-matching, parent-sync retry joins, originals remain preserved. |
| Last move, phase acknowledgement lost | `TestRecoveryPublicArchivePreservesOriginalsAndResumesExactly`: repeating Run verifies accepted backups, records the ready phase and never repeats Backup or archives B. |
| Successor marker sync fails | `TestRecoverySuccessorMarkerPublicationFailuresNeverAcquireDestination`: temp-sync/rename/parent-sync cuts leave no destination acquisition; retry reconciles the write-once marker. |
| Marker durable, acquisition incomplete | `TestRecoveryRestartResumesEachPreFoundingAcquisitionBoundary`: authority-only/fresh-meta/pin-before-v1 resume; foreign/unpinned-nonfresh/acquired-missing metadata refuses. |
| v1 before import | `TestRecoveryNodeClosesPublicGateBeforeImportAndResolvesLostReply` and pre-import service-gate test: ordinary leader exists but join/scheduler/public mutation stay closed until exact import. |
| Import committed, response lost | `TestRecoveryCoordinatorConfirmsLostRepliesBeforeDataAndLocalCompletion` and import FSM retry tests: same v2 operation and immutable request are confirmed, no replacement approval. |
| Seed staging partial | `TestRecoverySeedResumesPublicationBoundariesWithoutClobber`, `TestRecoveryRefusesUnprovenStagingAndForeignLiveDirectory`: only owned unpublished staging can resume; ambiguity preserves/refuses. |
| Seed rename, progress missing | Same publication-boundary test reprobes the exact marker/control; no replacement/empty founding. `TestRecoveryInstalledMissingImageCannotBeReconstructed` refuses installed missing data. |
| Replay/checkpoint interrupted | `TestRecoveryCheckpointSurvivesIntermediateSnapshotAndRetryKeepsSeedGrant`, `TestRecoveryFactoryFailureResumesOneOrdinaryRetryWithOriginalSeed`: retained B opens under exact ancestry and immutable seed provenance. |
| Checkpoint durable, consumption reply lost | `TestRecoveryConfirmsLostConsumptionReplyWithoutAnotherFactory`: exact checkpoint readback resolves one lost reply with one factory; full receipt pair is derived by FSM and retained. |
| Replicated complete, local handoff incomplete | Coordinator late-publication tests plus `TestPermanentMajorityLossRemoteCrashResume/readiness`: validation resumes with no early Run; Stop joins it. |
| Later B WAL lost | `TestRecoveryCompletedRestartUsesPortableBAndNeverRefillsMissingWAL`: exact B is parked before factory; no handle or recreated bytes. Backend consumed/pruned-checkpoint test permits healthy advanced B without consulting A. |

## Other failure and ownership boundaries

| Target §11.4 / Task16 concern | Verification |
| --- | --- |
| Live owners, alias/replaced/unavailable roots, symlinks/hardlinks/mounts | Root owner exclusion/replacement tests, WAL lease adoption, archive artifact traversal and missing-WAL-before-backup tests; typed refusal preserves existing owners and names. Descriptor mount checks have platform-specific Darwin/Linux implementations; Linux is exercised by Docker. |
| Unsupported/corrupt metadata/authority/pin/snapshot/log | Read-only metadata/authority functional fixtures, strict decoder/manifest units, frozen old-format rejection, WAL snapshot CRC/gap/term/hint tests. None initializes missing buckets, repairs/truncates A or renames a bad snapshot .broken. |
| Earlier oversized length, final torn record and future snapshot | `TestOfflineEarlierOversizedLengthCannotBecomeOlderCommit`, `TestOfflineFinalTornRecordNeedsCompleteRetainedBoundaries`, `TestOfflineSnapshotAboveCommitIsReportedWithoutPromotion`, `TestOfflineEarlierCRCWithZeroSectorsIsNotTornTail`. |
| Nil metadata persister/no offline application | `recoveryinspect.readSource` constructs the source FSM without a persister; shared Replay starts no core workers. Offline reader/replay tests compare originals and count application commands. Public Inspect/Approve tests require zero factories/Files callbacks. |
| Unknown WAL/unfinished topology/staged Latest/debt | Pure exact-donor and FSM inventory tests, `TestRecoveryUnknownLiveWALBlocksAndReportsRetainedPositions`, `TestPermanentMajorityLossApprovedInventoryArchivesOldWork`: approved coverage/proven predecessor only; old debt stays unresolved in backup. |
| Changed evidence or acceptance | Stale approval, changed library data in Files hook, corrupted backup-before-seed and exact historical approval tests: digest/audit/resources/destination changes cannot reauthorize or erase B. |
| Both/neither archive paths, foreign/renamed/duplicate namespace | `TestRecoveryArchiveRefusesConflictingBothOrMissingPaths`, disk no-clobber/discovery/staging tests, historical Status with evolving pin/offline WAL. Read-only discovery preserves valid rows plus diagnostic; mutation refuses invalid namespace. |
| Resource bounds/disk I/O failure | One-byte budget refusals, approval future-progress budget validation, instance-owned sync/rename faults and resource qualification. Tests model I/O failures at write boundaries; no automatic cleanup/truncation or unbounded fallback exists. |
| Factory/Restore/Apply/Snapshot failure | Recovery failed-factory retry, failed validation, serial multigroup checkpoint failure, offline interpreter and ordinary shared Host failed-Restore/engine business-rejection tests. Application errors keep the replica incomplete or halted; no Run/handle escapes. |
| Late callbacks, Stop, Close, Run failure | Recovery late-factory/long-Restore/private-checkpoint Stop tests; shared `TestCloseFailureRetainsOwnershipAndRetriesSerially`, `TestSuccessfulFactoryResultAfterCancellationIsStillClosed`, `TestApplicationRunFailureHoldsExactReplicaAndExplicitRetry`. Timeouts retain ownership; callback work joins before replacement or release. |
| Stale packets/reports, wrong reset destination | Three-process complete/rejoin test and existing exact-identity transport/delivery tests; reset startup rejects wrong lineage/bootstrap. Historical Run/Approve cannot rearchive successor state. |
| Later B retirement/recovery | `TestRecoveryProvenanceSurvivesOrdinaryLaterDeletionAndPruning` verifies recovered B cleanup identity and no A authority; `TestRecoveryCompletedLineageCanRecoverAgainWithoutArchivingHistory` performs another public Inspect/Approve/Run/Node-start cycle and retains both histories. |
| Probe authentication/partial coverage | Authenticated old IdentityPing replies including Unavailable block. Credential/lineage rejection is informational; unreachable/unresolved peers never prove fencing. Probe reports attempted X of Y; nil IdentityPing remains side-effect-free. Probe tests repeat checks at Approve without changing accepted digest. |
| Remote reports alongside errors | `TestRemoteRecoveryHistoryDiagnosticsKeepValidReports`: list and selected-job routes retain valid reports plus typed namespace issue, while preserving foreign bytes. |

## Task16 corrections and dispositions

1. User-approved checkpoint-only command: `GroupConsumedProposal` contains the
   exact durable CheckpointProof, not copied receipts. FSM selects and validates
   committed receipt evidence at apply. The retained Consumed schema and frozen
   snapshots do not change. Lost-response confirmation compares identity,
   position and digest; the RecoveryEvidence read port is removed.
2. Review M1: public TransferLeader is guarded and direct GrantTransfer is removed
   from the importing allowlist. Receipt selection uses the highest complete
   creation in the exact attempt chain, blocks a later unsettled creation, and
   survives noncreation floor advancement. A real Node test holds checkpoint
   consumption, attempts transfer and proves recovery subsequently completes.
3. Review M2: native creation receipts must be exactly S/S-term or strictly beyond
   source C in a term above source HardStateTerm, at/below the checkpoint. Units
   reject copied-tail positions, old terms, mixed/incomplete pairs and uncovered
   checkpoints; retry/fenced-successor cases preserve exact same-key evidence.
4. Review L1/L2: partial reachability coverage is visible; a code comment preserves
   the side-effect-free nil-payload IdentityPing contract.
5. The fixture previously hid valid reports when discovery also returned a
   namespace diagnostic. Both HTTP paths now preserve the report and expose the
   typed issue. No library public API changed.
6. Added actual successive recovery and exact source-budget startup tests rather
   than assuming historical ownership or destination encoding behavior.
7. Reconciled obsolete target prose/pseudocode and documented existing private
   helper consolidation. Added exclusion comments; no public stub or force flag.

Repeated full WAL materialization is a measured potential improvement, not an
unimplemented required behavior. The reader/seed/full-buffer algorithms measured
in Task15 are unchanged by Task16's control-payload and authorization corrections.
No streaming or hard RSS claim is made. Approval reserves history capacity before
any hook/archive, and unchanged nil-recovery formats keep ordinary behavior.

## Actual entry-point wiring

1. `NewRecovery` copies source/options/probe configuration; each mutating facade
   acquires `recoverydisk.Owner`, then dispatches to inspection/journal/executor.
   Status/List use bounded read-only system discovery independently of Node.
2. `NewNode` validates/copies. `Start` acquires storage ownership, calls
   `prepareRecoveryStart` before authority/meta/load acquisition, transfers the
   WAL lease, constructs normal metaservice/Host and the private coordinator.
3. Metaservice's local recovery port validates the accepted owner; command
   dispatch, FSM graph preparation, pruning and snapshots all include the optional
   import. Service preparation and scheduler use the same placement adapter.
4. `managed.Host` startup reservations call the private backend adapter with shared
   replay, re-probe the installed WAL and call the ordinary GroupFactory. Engine
   recovery startup/control owns Restore and the serialized durable checkpoint.
5. Coordinator confirms ordinary import/creation/retry/consumption/completion.
   Node readiness calls publication, existing lifecycle validation, completion and
   Run release. Stop joins coordinator, managed callbacks/engines, metadata,
   stores and leases in that order of ownership.
6. Authenticated testapp routes call those public facades through durable workers;
   HTTP cancellation cannot cancel accepted work. Maintenance is loaded before
   ordinary Node startup. Files hook handles only caller-owned application files.
   The process harness uses the owned-WAL mode and these real routes.
7. Normal callers with no accepted manifest use ordinary startup. Nil replicated
   recovery context delegates to ordinary strict topology helpers. Successful
   retained B restarts use portable B data with no A hint. Explicit reset rejoins
   through ordinary admission; completed history grants no reseeding permission.

Audit is complete. Post-audit peer review was explicitly cancelled by the user.
Final unit and lint checks pass; recovery functional/race and every process
recovery case passed. The last full cluster run was105/107; both corrected
ordinary cases subsequently passed the user-authorized isolated rerun2/2,
63.312s. No new complete107-case run is claimed, and no further heavy run was
started. All requested verification is complete. Detailed results remain
in [progress](../progress/20261025-permanent-majority-loss-recovery.md), the
[validation record](permanent-majority-loss-recovery.md) and the plan.
