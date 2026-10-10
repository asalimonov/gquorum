# Runtime and cold-boot ports

The runtime facade milestone adds 21 test functions, with boot/progress matrix
subcases, to the previously verified engine, meta lifecycle and storage tests.
Independent constructor, ledger and decision tests run in parallel under
`make test-unit`; the three tests that start integrated engines skip short mode.

| Source obligations | Current coverage |
| --- | --- |
| `TestDecideShardBoot_SevenCases` current-format boot decisions | `TestLoadBootCurrentFormatMatrix` checks fresh, resume, adoption, reseating, missing store and foreign store; additional cases distinguish prepared learners from established replicas |
| Fresh store before ledger record; parking preserves ledger | `TestRuntimeApprovedLoadParksLostWALRecoversAndRetires` checks real durable storage and local ledger before/after WAL loss |
| Move-aside identity protection | `TestRuntimeForeignWALRetainedUntilExactCallerQuarantine` requires caller quarantine with the exact stored ID; automatic dropping is replaced by an explicit caller operation |
| Mismatched-store parking survives restart | `TestRuntimeIdentityMismatchParkSurvivesMetaRestart` reopens the same meta database with a new process and refuses the retained foreign WAL |
| Restoring the original store clears parking | The approved-load integration test restores the original WAL and verifies committed-tail replay; failed-restore retry has separate constructor coverage |
| Same identity remains parked; approved replacement clears it | Boot decision cases preserve same-ID missing-store refusal; exact ledger CAS and caller-quarantine tests exercise fresh replacement and clearing |
| Declared identity and Target authorization; Latest staging stays inert | Public scope tests and the real approved-load test refuse staged-only and foreign identities before creating a WAL |
| Empty prepared replicas remain non-serving | Existing native joining tests plus the new progress matrix allow intact empty learner stores while refusing missing previously prepared stores |
| Local replica ledger identity, repeat, lookup and forget | Existing bbolt ledger checks plus public ledger tests cover pin stamping, retained audit fields, cancellation, unavailable stores, authorized cleanup and exact replacement CAS |
| Runtime teardown joins callbacks before group/storage destruction | `TestRuntimeCloseJoinsEffectsBeforeLoadAndDatabase` holds cancellation cleanup open and verifies both resources stay available until it returns |
| Duplicate ownership, canceled stop, constructor/shutdown overlap | Runtime unit tests verify exact stop identity, concurrent idempotent Close and retention of in-flight constructor ownership |
| Cold WAL inspection | `TestColdProbeReadsRetainedLogWithoutBindingOrWorkers` checks retained replay evidence while leaving writable handles and bindings unopened |

SQL row migration and unpinned store adoption do not apply to the selected WAL
backend and mandatory full identity format. Local parked observations and typed
readiness errors replace application/global gauge assertions. Native engine
tests continue to cover snapshots, membership and first learner synchronization.
This milestone does not close the remaining supporting inventory or the
testcontainers process-cluster obligations.

Contracts and ownership are documented in
[runtime architecture](../architecture/runtime.md).
