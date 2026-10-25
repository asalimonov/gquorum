# Gosim improvements for simulation-first gquorum testing

Date: 2026-10-25. Consumer: `github.com/asalimonov/gquorum`, revision `10c78f9`.
Pinned simulator: `github.com/neria-cloud/gosim v0.9.1`; Go toolchain: 1.26.5.

## Recommendation

Prioritize directory-descriptor semantics, real file locking and explicit capability diagnostics. Then qualify the actual bbolt/etcd WAL stack and add precise filesystem fault controls. These changes would let gquorum exercise recovery, ownership and crash/resume through its normal public APIs in deterministic simulation, rather than expanding Docker scenarios or changing production code to accommodate missing hooks.

This is a feature-request report, not an implementation plan or a claim that simulation can replace every native test. The proposed controls and command names below are design suggestions, not released Gosim APIs. No test suite, benchmark or linter was run to prepare this report.

## Evidence and interpretation of the earlier workaround

The earlier statement concerned ordinary startup when no recovery namespace exists. Startup can check that absence while holding its normal leases, without entering the descriptor-based journal reader. If recovery evidence exists, the library still uses its full validation path. This production path also runs in simulation; it is not a simulation-only bypass.

The recorded 21 passing simulation cases were seven tests across three seeds. They were metadata latency/partition/quorum tests, **not 21 disaster-recovery or filesystem-exclusion cases**. The later placement workflow is another simulation scenario. These passes do not establish support for recovery archives, mount boundaries, competing storage owners or WAL crash recovery.

Evidence:

- [Recorded simulator limitation and workaround](../progress/20261025-permanent-majority-loss-recovery.md), Task 4 notes.
- [Simulation fixture and limitations](../../tests/sim/README.md), [fixture](../../tests/sim/fixture_test.go), [tests](../../tests/sim/cluster_test.go).
- [Directory-relative operations](../../internal/recovery/disk/relative.go), [atomic publication](../../internal/recovery/disk/atomic.go), [namespace discovery](../../internal/recovery/disk/discovery.go), [Linux mount identity](../../internal/recovery/disk/mount_linux.go).

The selected dependency was confirmed with `go list -m -json` in `tests/sim`. Its module sum is `h1:sJwT+zAgE0gSYUy0ThqYNmxqT23adbnRDgSTrmj9pfQ=`. Source inspection used the installed v0.9.1 module and upstream references. The [maintainer-supplied main feature matrix](https://raw.githubusercontent.com/neria-cloud/gosim/refs/heads/main/docs/features.md) was also checked. Main is a moving reference; a feature documented there must be verified against the version actually selected by the caller before migration.

## Already available: adopt or extend, do not request again

Gosim already offers isolated machines, crash/restart with fresh epochs, virtual time, deterministic seeds, race instrumentation and debugger step selection. Its filesystem has directory durability, sync fault injection, bounded crash-state enumeration/restore, capacity limits and read-only mode. Networking already supports directional partitions, latency/loss policies and configurable TCP buffers. These are foundations for the requests below, not missing features. See the [feature matrix](https://raw.githubusercontent.com/neria-cloud/gosim/refs/heads/main/docs/features.md), [filesystem controls](https://github.com/neria-cloud/gosim/blob/v0.9.1/fs/control_sim.go) and [recovery controls](https://github.com/neria-cloud/gosim/blob/v0.9.1/fs/recovery_sim.go).

The existing syscall monitor, capability catalogue, build identity/manifest, test-worker protocol, deterministic checks and network/syscall traces should also be reused. In particular, syscall routing/counting is not evidence that all argument combinations or ownership semantics are implemented. The [capability catalogue](https://github.com/neria-cloud/gosim/blob/v0.9.1/internal/fs/capabilities.go) makes that distinction explicit.

## Prioritized requests

| ID | Priority | Request | Main benefit |
| --- | --- | --- | --- |
| GS-01 | P0 | Real directory descriptors and Go `os.Root` compatibility | Run the actual recovery journal and archive paths |
| GS-02 | P0 | File-lock ownership and contention | Make storage-owner assertions meaningful |
| GS-03 | P0/P1 | Link/inode/mount semantics needed by archive guards | Exercise alias, replacement and unsafe-archive refusals |
| GS-04 | P1 | Deterministic operation-scoped filesystem faults and pauses | Reproduce exact crash/error boundaries without application failpoints |
| GS-05 | P1 | bbolt/WAL conformance profile, mmap and directory enumeration improvements | Qualify real retained storage and snapshot/catch-up paths |
| GS-06 | P1 | Network hold/release controls with precise transport semantics | Test delayed acknowledgements, stale requests and isolation |
| GS-07 | P2 | TLS and service-discovery simulation | Move more authenticated transport/admission coverage into simulation |
| GS-08 | P0 | Semantic capability preflight and strict coverage profiles | Prevent unsupported/stubbed behavior from producing misleading passes |
| GS-09 | P1 | Supported CI event/discovery API and portable reproduction bundles | Reduce downstream runner code and improve replay reliability |
| GS-10 | P2 | Fault-script reduction and correlated diagnostic timelines | Turn a failing campaign into a small reviewable regression |

P0 means a correctness blocker for the relevant assertions. P1 expands useful coverage. P2 improves breadth or debugging; it should not delay the first storage-compatible release.

### GS-01 — Real dirfd and `os.Root` operations

**Observed:** v0.9.1 `dirInode` accepts only `AT_FDCWD`. `Openat` also restricts flags, so adding a hook name alone does not enable the library's traversal. This remains visible in the sampled [main implementation](https://github.com/neria-cloud/gosim/blob/main/internal/fs/ops.go); the [pinned implementation](https://github.com/neria-cloud/gosim/blob/v0.9.1/internal/fs/ops.go) is the compatibility baseline.

**Requested contract:**

- Resolve a real directory FD to its inode, independently of the current working directory. Keep the handle attached to that inode after rename.
- Support the combinations used by Go 1.26 `os.Root` and `x/sys/unix`: `Openat`, `Fstatat`, `Mkdirat`, `Renameat` and `Unlinkat`, with their actual descriptor and flag semantics.
- Enforce `O_DIRECTORY`, `O_NOFOLLOW`, `O_EXCL`, `AT_SYMLINK_NOFOLLOW` and directory-removal behavior. Return appropriate errors for bad/closed FDs and non-directory descriptors.
- Cover `os.OpenRoot`, `Root.Open`, `Root.Lstat`, regular files wrapped by `os.NewFile`, and directory `Sync`. Review any additional Go-private hooks required by both supported compiler families.
- Test absolute-path behavior separately from relative-path behavior, and preserve `os.Root` confinement.

**Acceptance:** run gquorum's existing journal publication/archive tests under simulation without replacing descriptor-based operations with path-based operations. A renamed directory handle must still address the original directory; a closed handle must fail; traversal through a symlink must follow the selected syscall contract rather than being silently accepted.

Minimal proposed reproducer, not newly executed for this report:

```go
if err := os.Mkdir("case", 0700); err != nil { t.Fatal(err) }
dir, err := os.Open("case")
if err != nil { t.Fatal(err) }
defer dir.Close()
fd, err := unix.Openat(int(dir.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY, 0)
if err != nil { t.Fatal(err) }
defer unix.Close(fd)
```

Then extend the conformance test through the actual `os.Root` operations and flags. Passing only this minimal example is insufficient.

### GS-02 — Real storage exclusion, not successful lock stubs

**Observed:** the pinned [filesystem catalogue](https://github.com/neria-cloud/gosim/blob/v0.9.1/internal/fs/capabilities.go) identifies `flock` and record-lock behavior as stubs. Their success does not establish ownership or exclusion.

**Requested contract:** implement the lock family used by gquorum and bbolt first: exclusive/nonblocking flock, contention, unlock and release on final owning-descriptor close or crash. Different opens of the same inode must contend even inside one simulated machine. Where descriptor duplication is supported, model its lock lifetime correctly. POSIX/process locks and OFD locks must retain their distinct semantics if implemented; otherwise report them unsupported.

A later shared-volume/process model should permit independent simulated processes to contend for the same filesystem. Cloning a crash snapshot into two private filesystems is not shared-volume contention. This extension must retain normal filesystem isolation by default.

**Acceptance:** a second owner fails before acquiring metadata or invoking a factory; failed application Close retains the first owner's lock; successful joined teardown permits reacquisition; crash releases transient lock ownership without deleting durable data. Use the existing [Node ownership cases](../../node_storage_owner_func_test.go) and [disk owner tests](../../internal/recovery/disk/owner_func_test.go) as acceptance scenarios.

### GS-03 — Alias and mount safety for archival

**Observed:** the model describes files/directories and a single modeled storage volume; it does not provide the complete alias/mount behavior used by gquorum's archive guards. Accepting an lstat flag without modeling symlinks does not prove no-follow behavior. See the [pinned capability catalogue](https://github.com/neria-cloud/gosim/blob/v0.9.1/internal/fs/capabilities.go).

**Requested in two increments:**

1. Hard links, symlinks/readlink, stable inode identity, correct link counts and link-aware removal/rename. Preserve already-open handles to unlinked files. Qualify the link/rename operations actually used for snapshot and collection-marker publication.
2. A small explicit volume/mount model: distinct mount IDs, same-device bind mounts, cross-device rename errors and mount disappearance/replacement. Support the Linux `statx` subset used by the application, including FD-based `AT_EMPTY_PATH` and `STATX_MNT_ID`, with correct masks. Do not return a constant synthetic mount ID as if it proved safety.

Do not silently strengthen ordinary rename into a no-replace operation. gquorum currently performs its no-clobber protocol under exclusive ownership; the model must let that protocol encounter genuine conflicting destinations.

**Acceptance:** unsafe aliases, extra hard links and nested mountpoints refuse before archive mutation; both-path/neither-path conflicts preserve evidence; valid moves retain originals and sync the required parents. Linux mount-ID simulation can cover logical checks; Darwin Fstatfs and real bind-mount behavior still need small native checks. [Current mount qualification](../../internal/recovery/disk/testdata/mount-probe/README.md) remains separate evidence.

### GS-04 — Precise filesystem faults and crash boundaries

Extend existing crash modes, sync injection and snapshot enumeration. Add a small test-control API selecting machine/epoch, operation, virtual path or inode, occurrence and before/after phase. Bound the number of matches and make installation/removal deterministic. Public application APIs must remain unaware of the controls.

Useful actions are error-before-effect, effect-then-error, bounded delay/hold/release, short I/O and explicit crash. Corruption should be a separate deliberate control selecting a byte range, not an undocumented consequence of every I/O error. Preserve the normal capacity/read-only controls instead of duplicating them as another fault subsystem.

**Acceptance matrix:**

- Temp-file write/sync, publication rename, parent sync and pointer update can each fail independently.
- A completed rename followed by an error exercises reconciliation of an uncertain result.
- Source and backup evidence remain unchanged on refusal; no hidden repair or truncation.
- Crash-state campaigns preserve data-sync and directory-sync dependency ordering, and do not manufacture impossible outcomes.
- `ErrEnumerationLimit` remains explicit; an exhausted budget is not exhaustive coverage or normal end-of-enumeration.
- Named phase barriers can coordinate application actions with provider events. Existing Go channels remain sufficient for simple barriers; no public single-step scheduler API is required for the first increment.

This replaces many process hard-kill timing loops with reproducible cuts at the actual persistence boundary. gquorum's [journal fault tests](../../internal/recovery/disk/journal_func_test.go), [archive fault tests](../../internal/recovery/disk/archive_io_func_test.go) and [seed/checkpoint cases](../../internal/walbackend/recovery_func_test.go) supply concrete assertions.

### GS-05 — Qualify the real bbolt/etcd WAL stack

Add a maintained compatibility profile using the consumer's actual bbolt and etcd WAL dependencies. A successful syscall dispatch smoke test is not enough.

The pinned provider has a bounded read-only mmap subset and single-batch directory enumeration. Improve read-only shared mapping/remapping beyond the current 16 MiB ceiling with explicit resource limits and coherent file updates. Writable/private/anonymous mapping is not a prerequisite unless an actual selected dependency needs it. Implement a resumable directory cursor: a small buffer must not silently hide later entries.

Qualify ordinary WAL creation, preallocation flags, rotation, CRC checking, snapshot publication, compaction and reopening. First audit which allocation flags/fallbacks these dependency versions really use; this report does not claim a fresh WAL compatibility probe. Include directory enumeration larger than one buffer, many retained segments and metadata large enough to require remapping.

**Acceptance:** real Node-owned WAL replicates application entries, snapshots, restarts and catches up; replay stops at retained commit, ignores applied hints as authority and never refounds lost established B data. Keep semantic fixtures modest; near-1GiB RSS, disk-throughput and latency qualification remain native benchmarks.

### GS-06 — Delay and release transport work precisely

Directional partition and latency controls already exist. Add bounded hold/release and connection reset/half-close controls, selected by machine/epoch/link/connection and event occurrence, with structured trace IDs.

For gRPC, hold byte ranges/packets while preserving each connection's TCP byte ordering. Delaying an application request or response is a separate operation-boundary control; do not corrupt or reorder stream bytes to imitate RPC reordering. Document whether held traffic is invalidated by reconnect/restart. Keep epoch protections intact.

The current transport's packet loss is not a full TCP retransmission model. Either add the required retransmission semantics or report the narrower loss/reconnect model explicitly; application reconnect tests must not be described as kernel TCP conformance.

**Acceptance:** a stalled load destination does not block healthy peers or metadata; a lost reply after a committed effect confirms the same operation; delayed delivery reports cannot affect a replacement. Test old-lineage application messages by sending them from an intentionally retained sender or over a new valid connection. Gosim's rejection of packets from old machine epochs alone does not prove gquorum rejects stale application identities.

### GS-07 — Broader transport integration, after core storage

TLS/X509 execution and DNS are expansion requests, not prerequisites for plaintext authenticated Raft semantics. Start with deterministic Go TLS handshakes, explicit fixture keys/certificates, virtual-time validity/expiry and simulated socket I/O. Model certificate rejection and connection deadlines. Add a controlled DNS/service-resolution model if hostname-based discovery is part of the test.

Do not obtain entropy, perform DNS queries or contact certificate/network services on the host as a fallback. IPv6 and HTTP-management compatibility can follow measured caller needs. Keep a small native TLS/deployment test even after simulation support exists.

## Tooling improvements

### GS-08 — Capability preflight with assertion-level coverage

Build on the existing syscall monitor and semantic catalogue. Provide a public machine-readable capability view and a preflight command/profile describing operations **and supported argument subsets**: real dirfd, no-follow flags, lock contention, mmap bounds, mounts and durability semantics.

A proposed `doctor` command could check a finite profile such as `durable-raft-storage`, print missing/limited/stubbed requirements with caller locations and fail CI when a required semantic is unavailable. A metadata latency profile may still run while explicitly reporting that storage exclusion was not tested. Do not add a force switch that turns stubs into proof.

Extend diagnostics to distinguish an invalid argument from an unimplemented argument combination. Include operation, feature, machine/epoch, descriptor owner, relevant flags and stack location; redact credentials and payloads. Existing routed-call counts omit some hook-local early refusals, so they cannot alone establish complete coverage. See [syscall monitoring semantics](https://github.com/neria-cloud/gosim/blob/v0.9.1/docs/syscalls.md).

**Acceptance:** the present dirfd reproducer yields an actionable capability explanation; a storage-owner campaign cannot pass while required flock semantics are a stub; CI artifacts state which safety assertions the chosen model supports.

### GS-09 — Supported CI protocol and reproducible artifacts

The downstream [simulation runner](../../scripts/sim_events.py) already verifies manifests/binary hashes, performs a versioned worker handshake, discovers tests and manages isolated workers. It also adapts textual results into Go JSON events. Preserve that validation while providing a supported public interface for discovery, structured test/seed/subtest results and build identity.

Requested improvements:

- Document supported discovery/execution APIs so consumers need not reconstruct identity hashes or depend on worker-internal JSON field order.
- Offer structured events compatible with Go test/JUnit consumers, distinguishing simulated duration from host build/run time, timeout, skip, empty selection and incomplete execution.
- Support a bounded multi-seed/fault campaign with deterministic sharding and an explicit scheduled/completed/failed/unsupported inventory. Reuse prebuilt artifacts and existing identity checks.
- Export a replay bundle containing the manifest, selected source/module/replacement digests, toolchain/platform/features/race settings, replay revision, seed, fault script and failure trace. Seeds alone must never be advertised as portable across changed build identities.
- Diagnose stale worker/cache and local replacement/workspace mismatches before translation or execution.

The downstream README records a workspace caveat, while v0.9.1 already contains workspace-selection code/tests. Treat that as an integration reproduction/documentation task, not proof that Go workspaces are absent. Add a regression with the application root, separate simulation module and local Gosim checkout; do not weaken module fingerprints to make it run.

### GS-10 — Reduce failing scenarios and correlate their evidence

Extend existing tracing and debugger step selection with a versioned timeline correlating machine epoch, connection, filesystem operation/sync, fault action and application-supplied phase markers. A generic marker API must not know gquorum's private schema or claim authority from an observed status event.

Add a reducer for the fault script, operation sequence or selected crash outcome. Each candidate must rerun and reproduce a named invariant violation under its own recorded identity. Do not claim every seed can be shrunk numerically, or that a schedule checksum remains identical after removing operations. Preserve the original failure separately and emit the minimized script plus one-command replay.

**Acceptance:** a long crash/rejoin failure becomes a small sequence such as commit → lose response → crash → restart → confirm original operation, with the relevant source/commit/receipt evidence attached. The existing downstream [offline viewer](../../scripts/sim_visualize.py) can consume a stable upstream event schema instead of reverse-engineering log text.

## What gquorum can move, and when

Simulation support is only half the migration. The consumer must add reusable three-machine fixtures with real Node APIs, real metadata transport and authenticated load transport through the public receiver interfaces. Controller channels may schedule local API calls; they must not carry consensus traffic or bypass authorization. Persist maintenance jobs outside application archival, recreate resources/process identities on reboot, and retain full application snapshot/deduplication assertions.

| Scenario family / current evidence | Migration prerequisites | Required invariant |
| --- | --- | --- |
| Metadata admission and leader replacement: [join cases](../../tests/cluster/join_test.go) | Existing machine/network controls and an expanded simulation fixture; qualify ordinary storage | Learner-first admission, exact identity and retained operation after election |
| Approved document/placement changes: [simulation case](../../tests/sim/cluster_test.go) | Already demonstrated for label edits; extend cases | Real Stage CAS/approval and replicated Target, no unintended ID allocation |
| Snapshot catch-up and retained writes: [growth](../../tests/cluster/growth_test.go), [recovery](../../tests/cluster/recovery_test.go) | Real load transport and GS-05 conformance | Complete snapshot plus committed tail; application state/dedup survive |
| Permanent majority loss and returning nodes: [recovery/rejoin](../../tests/cluster/permanent_majority_loss_test.go) | GS-01/02/03/05 plus real load fixture | One complete survivor, fresh lineage, exact replay bound and explicit reset-for-join |
| Crash during Backup/archive/readiness: [crash cases](../../tests/cluster/permanent_majority_loss_crash_test.go) | Relevant storage prerequisites plus GS-04 | Same accepted operation resumes; no early Run or false completion |
| Incomplete source and unfinished topology: [inventory cases](../../tests/cluster/permanent_majority_loss_inventory_test.go) | Storage-compatible recovery fixture | Approved Target only; missing groups block; old debt remains evidence |
| Stalled destinations and uncertain effects: [runtime cases](../../tests/cluster/runtime_test.go) | Real load fixture and GS-06 or precise operation-boundary controls | Isolation, bounded queues and exact acknowledgement/ownership |
| Competing owners and failed Close: [ownership cases](../../node_storage_owner_func_test.go) | GS-02; shared-volume model only for independent-process contention | No overlap or lock stealing; release follows actual teardown |

These are candidates, not declarations that a particular Docker test can already be deleted. Metadata-only scenarios can advance earlier; they need not wait for TLS or every optional storage feature. Ordinary WAL/restart cases require a focused conformance probe before being classified as supported.

## Delivery and acceptance strategy

1. **Compatibility gate:** minimal real-dirfd and double-open lock reproducers, a semantic capability profile and cross-checks against native Linux behavior. Keep unsupported semantics explicit.
2. **Recovery filesystem gate:** run the real descriptor-based journal/archive flow, including directory sync, links and required mount identity. Keep the application on the normal code path.
3. **Persistent application gate:** qualify bbolt/etcd WAL and three-node application snapshot/tail/catch-up. Use small but real persisted datasets.
4. **Fault campaign gate:** enumerate bounded crash outcomes and inject named error/hold boundaries; assert exact accepted identity, source preservation and publication ordering.
5. **CI migration gate:** promote a scenario family only after seeded repeatability, clear budget/coverage reporting and comparison with its native counterpart. Retain a representative native canary instead of duplicating every seed in Docker.

Do not promise a percentage or wall-time reduction before measuring translation, cached execution and fault exploration separately. More seeds alone do not replace missing semantics.

## Keep a small native/process suite

Simulation should carry most protocol, timing, replay and state-machine interleaving coverage. Retain real OS/process tests for actual lock/syscall behavior, APFS/Linux/bind-mount differences, process signals, container permissions and volume bindings, native TLS integration, deployment packaging, and real RSS/disk/throughput/latency measurements. A simulated crash model is explicit evidence about that model, not universal hardware power-loss behavior.

No Gosim implementation changes, dependency upgrades, new cluster tests or upstream issue submissions are part of this report. Each request above can become a separate maintainer issue with its stated acceptance criteria.
