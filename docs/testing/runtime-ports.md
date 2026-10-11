# Node ownership and cold-boot ports

The original boot and ownership assertions follow their implementation into
`internal/managed` and the public integration tests in `tests/cluster/testapp`.
The historical public signatures and test names are retained in
[api-refactor-baseline.json](api-refactor-baseline.json), captured from `75ba513`. The [test-name migration map](api-refactor-test-map.json)
resolves all 550 baseline test names, including 64 renamed or split tests;
assertion preservation is recorded here and in the source inventory.
Tests that run Raft engines skip short mode. Independent decision, constructor,
ledger and cancellation tests run in parallel under `make test-unit`.

| Source obligation | Current coverage |
| --- | --- |
| Current-format boot decisions | `internal/managed/boot_test.go: TestLoadBootCurrentFormatMatrix` retains every original case: fresh, resume, adoption, reseat, prepared empty learner, established missing/empty store and foreign pin. |
| Durable store before ledger; parking preserves the full record | `TestPublicNodeApprovedLoadParksLostWALRecoversAndRetires` exercises actual WAL loss and replay; `TestDurableLedgerSurvivesParkedRestartAndExactRetirement` additionally compares the entire record across bbolt reopen, including origin and timestamp. |
| Exact move-aside identity protection | `TestHostForeignWALRetainedUntilExactQuarantine` refuses the wrong stored ID, preserves foreign storage and clears parking after exact quarantine and successful startup. |
| Mismatched-store parking survives meta restart | `TestPublicNodeIdentityMismatchParkSurvivesMetaRestart` opens the same meta database under a new process, preserves its member ID, refuses the retained foreign WAL and checks typed/owned diagnostics. |
| Restore original WAL and clear parking | `TestPublicNodeApprovedLoadParksLostWALRecoversAndRetires` restores the original directory and checks committed-tail application replay; `TestHostFailedRestoreParksAndRetriesWithoutLosingStore` checks application restore failure, Close and retry. |
| Same-ID refusal and approved replacement | Boot matrix, `TestEstablishedMissingStoreParksBeforeFactoryAndPreservesEvidence`, exact bbolt CAS and retirement successor tests. |
| Approved identity; Latest stays inert | Public Node recovery test refuses staged-only startup before creating a WAL; host scope/action tests reject foreign identity and stale approval before effects. |
| Empty prepared replicas remain non-serving | Native joining and membership progress tests retain empty-learner versus lost-established distinctions. |
| Local ledger lookup, record, retry and forget | `internal/metastore` ledger tests, `internal/metaservice/replicas_test.go` and managed durable-ledger/retirement tests retain pin stamping, audit fields, cancellation, unavailable stores and exact replacement CAS. |
| Teardown joins admitted effects before closing storage | `TestPublicNodeStopJoinsEffectsBeforeDatabase` holds transport cancellation cleanup, checks retained replica ownership and the actual bbolt lock, then checks joined release and retained borrowed WAL. Node cancellation fences Raft participation immediately; resource ownership lasts through callback return. |
| Duplicate registry ownership and exact stop | `TestHostRegistryAndSharedOwnership` checks idempotent same-handle start, owned sorted IDs, stale stop, canceled stop, new handle on restart, concurrent joined shutdown and borrowed store retention. |
| Constructor/shutdown overlap | `TestHostStopDuringProbeRetainsOwnershipAndJoins`, managed delayed restore and late factory tests, plus `TestPublicNodeCanceledFactoryRemainsOwnedThroughLateReturn`. |
| Construction acquires nothing; admission before start is refused | `TestNodeConstructionAndStopBeforeStartAcquireNothing`, `TestConstructorValidatesWithoutAcquiringDependencies` and public sequential-start tests. Node construction replaces the former separately opened meta and load handles. |
| Parked observations and event identity | `TestParkedLoadOwnedObservationAndTypedError` checks stable timestamps, copied maps, typed cause/readiness errors and exact local event identity; the public restart test verifies facade conversion. |
| Cold WAL inspection | `TestColdProbeReadsRetainedLogWithoutBindingOrWorkers` retains replay evidence without opening writable handles. |

The former `TestRuntime*` tests are removed with Runtime; their assertions are
mapped above. Duplicate `OpenLoad` construction refusal becomes idempotent
`StartLoadGroup` for the same approved identity, as specified by the target.
Caller-selected node/cluster identities for each OpenLoad are no longer an API:
a managed replica inherits its owning Node and its approved assignment.

SQL row migration and unpinned-store adoption do not apply to this WAL backend
and mandatory full identity format. Native engine tests continue to exercise
snapshots, membership and first learner synchronization. Process/fault scenarios
remain in `tests/cluster`; the small example smoke test does not replace them.
See [Node ownership](../architecture/runtime.md) and the
[source-test inventory](../brainstorms/20261010-raft-library-test-port.md).
