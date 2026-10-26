# Proposal retries: etcd, TiKV, PD and gquorum

Date: 2026-10-26. Read-only source investigation following the [receipt brainstorm](20261026-write-receipt-contract.md). No reference-project or gquorum implementation was changed; no runtime tests were executed.

Subsequent decision: after this comparison the user approved a simpler [target architecture](20261026-write-receipt-contract-target.md). The current helper retries only proven non-submission, always stops on Unknown, and returns Submission directly. Uncertain-retry opt-in and the aggregate result discussed below are historical proposals, now removed. The timing refinement is accepted: the helper adds no per-attempt deadline. Source findings below remain unchanged.

## 1. Question and conclusion

The user questioned whether proposal retries are necessary given frequent heartbeats and replication, and requested comparison against local etcd, TiKV and PD projects.

The references distinguish **replicating an accepted log entry**, **retrying a client request that may not have reached its intended server**, and **resolving/retrying a logical transaction**. These are different responsibilities. Heartbeats support elections, liveness and replication progress. They do not supply a client response, recreate a never-received command, or determine the business outcome of a lost reply.

Recommendation for gquorum:

1. Keep Group.Propose a single logical submission attempt; let Raft handle replication. Preserve the existing narrow definitive-drop retry unless separately redesigned.
2. Keep attempt receipts independently of whether a retry helper is used. Errors can remain uncertain even with perfect application deduplication.
3. Keep ProposeRetry an explicit optional caller wrapper. Retry proven refusals; stop on uncertainty by default. Ambiguous retry opt-in requires application-level safe repetition.
4. Put route refresh and remote resend policy in #9's client, not inside every exact Group handle. Avoid stacking independent retry loops at application, remote-client and engine layers.
5. Reconsider the proposed automatic three-second attempt timeout. It unnecessarily cuts short the existing 30-second replica proposal wait; see section 7. Three attempts is a configurable policy choice, not a consensus requirement or a number established by these references.

This investigation supports the previously selected safe retry semantics. It does not establish that every application should use the retry helper or that gquorum needs TiKV's transaction machinery.

## 2. Source provenance and limits

| Project | Examined source |
|---|---|
| etcd | Local `/Volumes/M4DATA/Users/speckzzz/reps/neria/etcd`, commit `00a7653ca9dc7d80475565a2279ab4cf741692eb`, 2026-08-15; subject bumps to v3.8.0-alpha.0 |
| TiKV | Local `/Volumes/M4DATA/Users/speckzzz/reps/neria/tikv`, commit `137cbb2593decafebf99905b4d570869230565c2`, 2026-04-09 |
| PD | Supplied `/Volumes/M4DATA/Users/speckzzz/reps/neria/pd` does not exist. Examined upstream archive at `8a6813497b5212a0c9e17ffd7bb09e38c82d73b5`, 2026-04-04 |
| TiKV Go client | Upstream archive at `b7f9a9e9d2abbabe19b8fc27f73f480fe383709d`, 2026-04-01; needed because the TiKV server repository does not contain the external application's transaction retry coordinator |
| PD/client-go version selection | Both versions are pinned by the user's local `neria/tidb/go.mod`, lines 127–128; this keeps those two references tied to a concrete local dependency set |
| Native Raft contract | gquorum's pinned `go.etcd.io/raft/v3 v3.6.0`, inspected in the local module cache |
| gquorum | Current implementation `8e57798`, plus the unimplemented receipt target from this session |

Temporary upstream archives were extracted under the system temporary directory `gquorum-retry-references-km7y0sq6`; permanent links below use pinned GitHub commits. The local etcd development checkout is not a claim that this exact revision is a stable production release. This is a targeted analysis of ordinary proposal/write paths and representative PD operations, not an audit of every admin operation, transport retry, client language or release.

## 3. What heartbeats and replication solve

The native Raft Node.Propose contract explicitly warns that a proposal may be lost without notice and assigns retry responsibility to its caller. In the same Raft implementation, heartbeat responses can trigger another Append when a follower lags the leader's log. These statements are compatible: retransmission of an existing log entry cannot recreate an application request that never made it into that log. Sources: [Node.Propose contract](https://github.com/etcd-io/raft/blob/v3.6.0/node.go#L138-L140), [heartbeat response and Append](https://github.com/etcd-io/raft/blob/v3.6.0/raft.go#L1571-L1589).

| Failure / event | What can make progress | Does it justify a fresh proposal? |
|---|---|---|
| Follower misses Append entries while a live leader retains them | Native Raft retransmission/catch-up | No; let replication work |
| Request reaches a node with stale leadership/region routing and is refused | Client refreshes route and retries a proven refusal | Often yes |
| Local queue refuses before consensus admission | Caller may back off and submit again | Yes, if error is transient and budget remains |
| Follower's initial forwarded proposal never reaches leader | Client/owner must surface uncertainty; native replication has no entry to copy | Only with appropriate retry safety |
| Old leader accepted an uncommitted entry, then lost leadership | Raft may retain/commit it or replace it; caller may not know | No blanket safe-resend rule |
| Entry committed/applied but client response disappeared | Application outcome query, dedup-aware retry, or uncertainty returned | Heartbeats cannot recover the business reply |
| Periodic state report lost | Next newer report may replace it | Often no need to repeat the old report |

The etcd API guarantee explicitly preserves caller uncertainty after timeout/network disruption and possible abort during leader election. [etcd API guarantees](https://etcd.io/docs/v3.7/learning/api_guarantees/#operation-completed).

## 4. etcd

### 4.1 Ordinary server-side write: propose once, wait for Apply

`raftRequest` calls `processInternalRaftRequestOnce`. That function checks admission/size/auth, generates a server request ID, registers an application waiter, invokes `s.r.Propose` once, and waits for its apply result, context expiration or server stop. There is no ordinary KV loop that resubmits the command every heartbeat or after a failed application wait. On timeout it removes the waiter and returns a classified error. Sources: [raftRequest](https://github.com/etcd-io/etcd/blob/00a7653ca9dc7d80475565a2279ab4cf741692eb/server/etcdserver/v3_server.go#L1012-L1032), [one proposal and apply wait](https://github.com/etcd-io/etcd/blob/00a7653ca9dc7d80475565a2279ab4cf741692eb/server/etcdserver/v3_server.go#L1058-L1133).

That request ID correlates a server invocation with its waiter; it is not an exposed durable application idempotency token. The inspected ordinary KV path does not return a generic per-attempt disposition receipt.

### 4.2 Client retry is operation-specific

The client classifies ordinary Put/Delete/Txn as nonRepeatable; Range explicitly selects repeatable. The nonrepeatable safety predicate accepts the specific no-address/no-connection conditions. A general Unavailable, leader-failure timeout or connection-loss timeout does not meet that predicate. Immutable RPCs have broader Unavailable retry behavior. Sources: [retry policy and KV methods](https://github.com/etcd-io/etcd/blob/00a7653ca9dc7d80475565a2279ab4cf741692eb/client/v3/retry.go#L47-L129), [nonrepeatable default](https://github.com/etcd-io/etcd/blob/00a7653ca9dc7d80475565a2279ab4cf741692eb/client/v3/retry_interceptor.go#L370-L376).

The configured unary interceptor has a default cap of 100 loop invocations, despite the reusable interceptor's zero-retry base setting; the client constructor supplies that cap. It has endpoint-aware backoff with a default 25 ms base and 10% jitter. This is not “100 retries of every write.” Eligibility still gates repetition. Sources: [defaults](https://github.com/etcd-io/etcd/blob/00a7653ca9dc7d80475565a2279ab4cf741692eb/client/v3/options.go#L42-L56), [constructor](https://github.com/etcd-io/etcd/blob/00a7653ca9dc7d80475565a2279ab4cf741692eb/client/v3/client.go#L247-L270).

Nuance: the inspected interceptor also has auth-refresh retries, a learner-refusal exception, and a context-status branch that continues if the RPC returns Canceled/DeadlineExceeded while the parent context is still live. Therefore it would be inaccurate to say *every* etcd write retry passes the no-connection predicate. Ordinary etcd server proposal timeout errors are encoded as Unavailable with their own descriptions and are not automatically safe mutable retries. Sources: [interceptor branches](https://github.com/etcd-io/etcd/blob/00a7653ca9dc7d80475565a2279ab4cf741692eb/client/v3/retry_interceptor.go#L43-L101), [learner exception](https://github.com/etcd-io/etcd/blob/00a7653ca9dc7d80475565a2279ab4cf741692eb/client/v3/retry_interceptor.go#L329-L353), [timeout encodings](https://github.com/etcd-io/etcd/blob/00a7653ca9dc7d80475565a2279ab4cf741692eb/api/v3rpc/rpctypes/error.go#L77-L85).

**Lesson for gquorum:** server proposal processing can remain single-attempt while an optional client has constrained retry policy. A generic transient fault classification alone is insufficient evidence to repeat a business write. Explicit receipts improve on relying on specific error descriptions.

## 5. TiKV

### 5.1 Raftstore does not blindly repeat arbitrary commands

The ordinary `propose_normal` path validates state/size, invokes native Raft proposal, and checks whether the log position advanced. A dropped proposal is returned as NotLeader; an accepted proposal registers its callback by index/term. The inspected raftstore-v2 path follows the same ordinary pattern. There are specialized admin/recovery retries elsewhere, but they are not a generic replay loop for arbitrary writes. Sources: [raftstore proposal](https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/raftstore/src/store/peer.rs#L4696-L4789), [raftstore-v2 proposal](https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/raftstore-v2/src/operation/command/mod.rs#L246-L284).

TiKV explicitly models `WriteEvent::Proposed`, `Committed`, and `Finished(Result<()>)`; Finished means aborted or applied. The server wires separate proposed, committed and applied callbacks. These are internal execution milestones and optimization hooks, not the same public API as our proposed receipt. Some specialized transaction paths support earlier responses under their own rules; they must not be treated as proof that every success waits for local Apply. Sources: [write event enum](https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/tikv_kv/src/lib.rs#L297-L311), [callback wiring](https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/src/server/raftkv/mod.rs#L584-L640).

A lost internal applied callback is classified as Undetermined. The transaction scheduler treats that internal invariant failure seriously, including a panic path; that mechanism must not be confused with a normal client network timeout or copied as gquorum's timeout handling. Sources: [callback error conversion](https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/src/server/raftkv/mod.rs#L133-L142), [scheduler handling](https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/src/storage/txn/scheduler.rs#L927-L946).

### 5.2 Client-go retries routes and RPCs

The RegionRequestSender has an explicit retry state machine, updates leader/region information, handles overload, and backs off on send failure. Caller cancellation stops retry. An explicit region UndeterminedResult is returned to the transaction layer instead of retried by that region-error branch. Its SendReq contract exposes retry count and notes possible uncertainty. Sources: [SendReq](https://github.com/tikv/client-go/blob/b7f9a9e9d2abbabe19b8fc27f73f480fe383709d/internal/locate/region_request.go#L452-L462), [send-failure handling](https://github.com/tikv/client-go/blob/b7f9a9e9d2abbabe19b8fc27f73f480fe383709d/internal/locate/region_request.go#L1720-L1805), [unknown/NotLeader handling](https://github.com/tikv/client-go/blob/b7f9a9e9d2abbabe19b8fc27f73f480fe383709d/internal/locate/region_request.go#L1934-L1968).

Thus “TiKV never retries unknown network outcomes” would be wrong: the RPC layer does retry send failures. Correctness depends on the enclosing request/transaction semantics. Findings here concern the transactional path, not a universal guarantee for RawKV compare-and-swap or every command type.

### 5.3 Transaction layer preserves and resolves uncertainty

Commit requests carry the same transaction start timestamp, primary key and key set. When a primary-commit RPC has an uncertain transport failure, the committer records `undeterminedErr`. Later region errors do not automatically clear it. A processed primary-commit response can resolve it, and exhausted failure with retained uncertainty becomes `ErrResultUndetermined`. Sources: [commit identity](https://github.com/tikv/client-go/blob/b7f9a9e9d2abbabe19b8fc27f73f480fe383709d/txnkv/transaction/commit.go#L75-L90), [uncertainty retained across retries](https://github.com/tikv/client-go/blob/b7f9a9e9d2abbabe19b8fc27f73f480fe383709d/txnkv/transaction/commit.go#L124-L181), [final outcome](https://github.com/tikv/client-go/blob/b7f9a9e9d2abbabe19b8fc27f73f480fe383709d/txnkv/transaction/2pc.go#L2029-L2058).

This is backed by durable transaction semantics: TiKV's commit action consults the transaction commit record when its lock is absent; an already committed transaction is recognized as a duplicate commit. It is not just a random token plus generic Raft replication. [Commit-record handling](https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/src/storage/txn/actions/commit.rs#L15-L63).

**Lesson for gquorum:** sticky uncertainty has a concrete production-project analogue. TiKV can clear *logical transaction* uncertainty after a subsequent response because it owns the transaction protocol. gquorum cannot interpret arbitrary Apply results that way. Our Last=Applied plus UnresolvedAttempts>0 is deliberately about physical attempts; an application may separately regard its deduplicated business operation as resolved.

## 6. PD

### 6.1 Persistence uses etcd; PD is not a third independent Raft engine design

PD embeds etcd. The examined PD version pins etcd v3.5.15, not the exact development version in the user's etcd checkout. Its `Save`/`Remove` call one etcd transaction; `SlowLogTxn.Commit` invokes the underlying Commit once, records timing, and returns the result/error. `RunInTxn` also does not install a generic retry loop around its callback/commit. The embedded etcd client has its own behavior; no claim is made that this wrapper suppresses those lower-layer retries. Source: [PD storage wrapper](https://github.com/tikv/pd/blob/8a6813497b5212a0c9e17ffd7bb09e38c82d73b5/pkg/storage/kv/etcd_kv.go#L94-L223).

The ID allocator reads the current allocation boundary, guards a new boundary with CAS and leader identity, then commits once. On error it returns without treating the local range as successfully installed; a later allocation can re-read the persisted boundary. A lost allocation reply can waste IDs, which differs materially from repeating a business debit. Source: [ID allocator](https://github.com/tikv/pd/blob/8a6813497b5212a0c9e17ffd7bb09e38c82d73b5/pkg/id/id.go#L113-L233).

### 6.2 PD APIs choose retry behavior by operation

The Go PD client's GetTS retries classified leader-change errors, with a cap of 20 loop invocations and 100 ms waits after the first immediate retry. Its internal TSO dispatch has another limited retry path for dispatch/service-client availability. Timestamp generation increments an in-memory logical counter under a lock; persistence saves time windows separately, so GetTS retries are not repeated Raft writes for every timestamp. Sources: [GetTS loop](https://github.com/tikv/pd/blob/8a6813497b5212a0c9e17ffd7bb09e38c82d73b5/client/client.go#L553-L608), [dispatch retries](https://github.com/tikv/pd/blob/8a6813497b5212a0c9e17ffd7bb09e38c82d73b5/client/inner_client.go#L381-L410), [timestamp generation](https://github.com/tikv/pd/blob/8a6813497b5212a0c9e17ffd7bb09e38c82d73b5/pkg/tso/tso.go#L143-L157), [timestamp persistence](https://github.com/tikv/pd/blob/8a6813497b5212a0c9e17ffd7bb09e38c82d73b5/pkg/tso/tso.go#L87-L91).

PD's generic unary backoff interceptor invokes once if no backoffer is attached. An explicitly supplied backoffer retries according to its checker; absent a checker it considers errors retryable. That is optional policy, not a server-provided submission guarantee. Sources: [interceptor](https://github.com/tikv/pd/blob/8a6813497b5212a0c9e17ffd7bb09e38c82d73b5/client/pkg/utils/grpcutil/grpcutil.go#L50-L76), [backoffer predicate](https://github.com/tikv/pd/blob/8a6813497b5212a0c9e17ffd7bb09e38c82d73b5/client/pkg/retry/backoff.go#L141-L155).

In TiKV's Rust PD client, selected control RPCs use `LEADER_CHANGE_RETRY=10`, while `store_heartbeat` uses `NO_RETRY=1`. The source explicitly distinguishes periodic requests. TSO and ID allocation have their own retryable operation semantics. These PD reports are not Raft heartbeat messages; they supply a useful example of replacing an obsolete report with the next one. Sources: [policy constants](https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/pd_client/src/client.rs#L327-L329), [store heartbeat](https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/pd_client/src/client.rs#L758-L809), [request retry loop](https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/pd_client/src/util.rs#L443-L520).

**Lesson for gquorum:** retry policy follows the operation. Neither PD's timestamp retries nor its periodic heartbeat behavior establishes a universal rule for an arbitrary application proposal.

## 7. Comparison and changes recommended for our design

| Concern | etcd | TiKV transactional path | PD | gquorum target |
|---|---|---|---|---|
| Ordinary server proposal | One proposal, wait for application | One ordinary Raftstore proposal with callbacks | Delegates durable metadata writes to etcd | One logical attempt; existing one definitive-drop retry |
| Automatic repeat after ambiguous failure | Restricted mutable-RPC policy, with noted interceptor exceptions | RPC retries plus transaction-specific uncertainty handling | Operation-specific; no blanket retry loop in examined KV wrapper | Off by default; explicit application opt-in |
| Routing retries | Client endpoint selection | Region/leader cache and backoff | PD/TSO service discovery and API-specific policy | Future #9 client, not rebinding exact Group |
| Stable identity | Internal server request correlation | Transaction start timestamp/key identity and commit records | Operation-specific CAS/allocator/TSO semantics | Logical RequestID plus distinct AttemptID; no built-in dedup |
| Unknown outcome exposed | Error/timeout uncertainty | ErrResultUndetermined; tracked across retries | Operation-specific errors | Explicit attempt disposition plus optional retry summary |
| Can subsequent success resolve a logical write? | Application can use its own protocol/state | Yes, where transaction semantics establish it | Depends on operation | Application decides; library cannot clear unrelated attempt uncertainty |
| Need to resubmit merely to keep replication going? | No | No | No | No |

The current gquorum helper is more permissive than the proposed safe default: `load.ProposeRetry` delegates to `fault.Retry`, which retries deadline/unavailable/overloaded/not-leader errors and loses earlier values. Heartbeats do not make that safe. The proposed receipts and uncertainty gate improve the existing API even if no additional automatic retry is introduced.

### What should stay

- The receipt contract, because a failed single call still needs accurate outcome information.
- Single-attempt Group/Leadership semantics and ordinary native replication ownership.
- Optional helper that repeats known pre-submission refusals with bounded backoff.
- Stop-on-uncertainty default; explicit application idempotency responsibility.
- Separate aggregate when a caller explicitly opts into uncertain retries. TiKV's retained transaction uncertainty is supporting evidence for retaining earlier information, not a reason to discard it.

### What should be refined before implementation

1. **Do not inherit a three-second attempt timeout implicitly.** The target currently embeds `fault.RetryOptions` defaults. A healthy but slow proposal can still be progressing under its 30-second engine budget; cutting it at three seconds turns it Unknown and the safe helper then stops. Recommended revision: zero helper AttemptTimeout adds no shorter deadline, while an explicit positive value may shorten the parent/engine budget. Backoff defaults remain independent. This is a proposed refinement, not a user-approved change to the target signatures in this investigation.
2. **Do not interpret three attempts as a delivery guarantee.** A default first Unknown ends the helper immediately; it does not send three potentially duplicating commands. All-three-attempt behavior applies to eligible refusals or explicit uncertainty opt-in. Reference counts differ substantially; our number is a bounded convenience policy.
3. **Avoid useless scoped retries.** A Leadership handle's captured term cannot become current again after genuine term loss. A client retry loop must not silently reacquire leadership. The application supervisor handles a new scope; repeated NotLeader from an expired scope does not improve availability.
4. **Use one retry owner per public operation.** A future GroupClient should own route refresh, attempt count and total budget. Applications should not need to wrap an already retrying remote call in another default helper. Preserve the separate, bounded native definitive-drop path without turning it into retry on unknown transport delivery.
5. **Do not add a durable receipt/dedup store to imitate TiKV.** TiKV's transaction protocol resolves logical outcomes using domain-specific durable records. That is a separate capability from the receipt metadata requested by #13.

The default-stop contract already selected by the user is consistent with the conservative side of these designs. The references do not justify removing all retry capability, and they also do not justify introducing transparent repeated submissions into every Group.Propose call.

## 8. Verification and remaining scope

Verified by source reads at the pinned revisions; no test execution, benchmark, fault injection or production experiment. Public documentation corroborates uncertainty; it does not substitute for the inspected branch behavior. No reference repository was edited. Documentation links and `git diff --check` are checked in this worktree.

Not audited exhaustively: TiKV RawKV semantics, all special admin/recovery retry paths, all PD APIs, transport-library transparent retries, or other client languages. They do not alter the established need to separate consensus retransmission from application resubmission; no claim of universal exactly-once behavior is made for any project.
