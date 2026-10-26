# Implementation plan: Write receipts and safe proposal retries

**Plan:** `docs/plans/20261026-write-receipt-contract.md`
**Date:** 2026-10-26
**Project source:** `../..` relative to this plan (repository root `.`)
**Branch:** `feat/receipt-error-contract`
**Implementation baseline inspected:** `8e57798`
**Target architecture:** [write receipt contract](../brainstorms/20261026-write-receipt-contract-target.md)
**Target SHA-256:** `a261597d54d94bdb075bdaeaf82ada2df96ef0e085abe0920ae29ff39b88ebc9`
**Full brainstorm log:** [session and final decisions](../brainstorms/20261026-write-receipt-contract.md)
**Reference comparison:** [etcd, TiKV and PD](../brainstorms/20261026-proposal-retry-reference-comparison.md)
**Peer review:** [thread 1 and user addenda](../review/feat_receipt-error-contract.md)
**Status:** execution started. User directs all cluster/Compose execution to penultimate Task 6 and a colleague review of every implementation commit after final verification.

## Execution contract

The linked target is the design authority. This plan sequences that design; historical aggregate receipts, ambiguous-retry opt-in, atomic header publication and shutdown enqueue gates must not return during implementation. References to target sections below mean that exact file and revision. If the target changes, reconcile this plan and update its fingerprint before execution.

Preserve the agreed contract:

- Change public Group.Propose and Leadership.Propose to `(Submission, error)` in one activation task with their callers. No compatibility aliases, temporary public methods or runtime rollout switches.
- Unknown is the zero disposition. Only successful Apply of the matching committed command establishes Applied. Business rejections remain Result.Code/Data. Nil error implies Applied with a complete result.
- Required caller-owned RequestID is exact valid UTF-8/non-whitespace/no NUL; no normalization or implicit logical ID. Preserve current log encodings and application deduplication responsibility. AttemptID remains a distinct opaque generated token.
- Reuse one existing buffered reply channel and one terminal owner send. Cancellation before observed publication can return Unknown even after Apply returned; an already-buffered terminal reply wins the cancellation/stop race.
- Optional ProposeRetry returns Submission, repeats only proven NotSubmitted with a transient cause, uses CanResubmit, and always stops on Applied/Unknown. Three attempts by default; no additional attempt timeout. Keep the current backoff defaults and document the election gap.
- Expired leadership and non-accepting halted/stopped handles are terminal Stopped with causes retained; Stopped never supplies pre-handoff proof on its own. A waiter attribution mismatch does not halt valid committed application.
- No new or changed in-flight limits: reuse MaxPendingProposals slots, existing queue capacity 256 and nested enqueue budget. No receipt queue, semaphore, cache, header channel, shutdown gate/drain or new persistent state.
- A retained stopped handle may retain queued payloads. Count is bounded but bytes are not; a release guarantee is explicitly deferred. Preserve existing callback joins, map/read cleanup and resource ownership.
- Keep generic fault.Retryable/fault.Retry behavior, private metadata proposals, read/control paths and frozen fixtures unchanged. Public Standalone.Group callers are included in the API migration.
- #9 remote routing and #12 actual size-policy machinery are excluded. Core receipt representation and adapter-level contract tests do not amount to implementing either feature.

The source directory is this worktree; there is no project.txt or separate/symlinked docs repository. Use the existing branch and explicitly add the ignored plan file. Do not create a new repository or switch the source branch. The skill-requested planning commit is `Plan write receipt contract (go-brainstorm)` and contains the new plan only; other pending documentation is not part of that commit. No push is requested.

## Execution steering

The implementation goal supersedes the original gate schedule: all cluster and Compose tests run only in penultimate Task 6, after final unit/functional/race/lint validation. Task 7 performs the architectural delta audit and final wiring verification, then asks the existing Claude peer to review all implementation commits. Fix review findings and revalidate affected non-cluster checks; if a fix needs cluster verification, reopen Task 6 and run its gate there before re-completing the final audit. No cluster execution is moved into another task.

## Sequencing and green checkpoints

The user accepted this sequence: receipt types → engine support → atomic API/caller migration → functional regressions → three-node process coverage → documentation → final validation and architecture audit.

| Task | Deliverable | Dependencies | Expected changed Go/test LOC |
|---|---|---|---|
| 1 | Public value types and the sole write retry predicate | — | 200–400 |
| 2 | Internal engine receipt path using existing queues/replies | 1 | 700–1,200 |
| 3 | Public activation, safe retry helper, all caller migrations | 1–2 | 900–1,500 |
| 4 | Functional evidence, ownership, limit and compatibility regressions | 3 | 700–1,200 |
| 5 | Authenticated fixture receipt endpoint and three-node cases | 3–4 | 500–1,000 |
| 6 | Current architecture, usage and test documentation | 3–5 | Documentation |
| 7 | Final checks, changelog and architectural delta verification | All previous | Verification and necessary gap fixes |

Task 1 adds real standalone value behavior without changing Propose. Task 2 adds the complete internal Submit path while public load still uses legacy methods. Task 3 switches public entry points and migrates every dependent caller in the same checkpoint; it must not leave any module or functional test uncompilable. Avoid temporary stubs entirely. Tests exercise the internal receipt path before public activation.

Each task targets at most about 1,500 non-generated changed lines. If measured scope exceeds that, split into independently green sub-checkpoints and record them here; do not split Task 3 into a broken public API and a later caller repair. Necessary preparatory factoring can precede its atomic switch without exposing new public API.

**Per-task verification:** format changed Go files, run `make test` across all five application modules, run `make lint` while changes are pending, and execute the focused checks listed for the task. Fix all failures in executed checks, including those outside the edited files, and log necessary unrelated fixes under NOT PLANNED CHANGES. A genuine external blocker or unresolved failure belongs in NOT IMPLEMENTED GAPS and prevents marking the task complete. Documentation-only tasks run documentation checks. Full lint, full unit-race, full functional suites and Docker execution are reserved for penultimate Task 6; focused owning-package race tests in Task 4 verify concurrency changes earlier.

Every implementation task ends with its verification, checked boxes, a progress-log entry and a coherent commit. Record actual commands and report directories, not historical results or expected counts. After final verification, request the user-directed Claude colleague review of all implementation commits via paseo-peer, and resolve its findings before completion.

## Design-to-implementation mapping

| Target specification | Plan ownership |
|---|---|
| §1–2 decisions, flow and retry ownership | Tasks 1–3, 6–7 |
| §3.1 Submission/CanResubmit and ID contract | Tasks 1, 3–4 |
| §3.2 Group/Leadership signatures and scope expiry | Tasks 3–4 |
| §3.3–3.5 retry options, return rules, callback shape checks | Tasks 3–4 |
| §4.1–4.4 Submit/reply, cancellation and exact attribution | Tasks 2, 4 |
| §4.5 unchanged capacity and deferred cleanup | Tasks 2, 4, 6–7 |
| §5 durable formats unchanged, volatile state schema | Tasks 2, 4, 7 |
| §6 remote/result-delivery seams and unchanged composition | Tasks 3–7; deferred features stay excluded |
| §7 flows/failure matrix and §11 pseudocode | Tasks 1–5, 7 |
| §8 caller/documentation migration | Tasks 3, 5–7 |
| §9 acceptance/verification | Tasks 1–5, 7 |
| §10 exclusions | Every task; explicit table below |

## Task list

### Task 1: Define public receipt values and retry eligibility

**Goal:** establish the small public contract without activating the breaking method changes yet.
**Files:** create `load/submission.go`, `load/submission_test.go`; update relevant comments in `load/machine.go` only when they describe the new value contract without claiming current methods already use it.
**References:** target §3.1 structures/invariants, §3.5 CanResubmit, §7 failure matrix, §11 CanResubmit pseudocode; no new schema under §5.

```go
type SubmissionDisposition uint8
const (
    SubmissionIndeterminate SubmissionDisposition = iota
    SubmissionNotSubmitted
    SubmissionApplied
)

type Submission struct {
    AttemptID       string
    RequestID       string
    Identity        storage.Identity
    RunID           string
    Disposition     SubmissionDisposition
    Index           uint64
    Term            uint64
    Result          Result
    ResultAvailable bool
}

var ErrResultUnavailable = errors.New("proposal result unavailable")
func (s Submission) CanResubmit(err error) bool
```

- [x] Implement exactly these public values and comments: no Result embedding, duplicate GroupID, aggregate outcome, transferable-proof semantics or wire-format promise. Unknown values are conservative.
- [x] Implement CanResubmit as the sole write-specific classification: require NotSubmitted, reject Stopped/Invalid/Unreadable/IdentityMismatch/Unsupported/context.Canceled anywhere in the chain, then consult unchanged fault.Retryable. It does not inspect caller budgets or authenticate fabricated structs.
- [x] Add parallel pure units (`TestSubmission…`) covering zero/unknown disposition, nil errors, each transient code, terminal-over-transient wrapping, Applied with a transient delivery error and unknown outcomes. Assert the old generic fault predicate remains unchanged even where CanResubmit must reject.
- [x] Keep facade token generation, engine conversion and new Propose/ProposeRetry signatures for Task 3; do not introduce unresolved engine-type dependencies or speculative unused helpers now.

#### Verification

- [x] `make test` and `make lint` pass.
- [x] `go vet ./load ./fault` passes. Tests here are units only; no running engine, network or application fixture.
- [x] Record results and commit this additive value checkpoint.

### Task 2: Implement internal receipts using the existing engine lifecycle

**Goal:** provide the complete internal receipt mechanism while legacy public/private calls remain operational.
**Files:** create `internal/raftengine/submission.go`, `submission_test.go`, `submission_func_test.go`; update `group.go`, `loop.go`, `ready_apply.go`, and shared test literals/helpers. Touch `forwarding.go`/`proposals.go` only where the target requires shared admission handling or contract comments.
**References:** target §2 flow diagram; §4.1–4.5 signatures, single-send algorithm and resource contract; §5 unchanged durable/volatile schemas; §7 admission/apply/stop flows; §11 Submit, stoppedSubmissionError, handleProposal, refuseProposal, applyNormalEntry, answerApplied, waitSubmission and submissionResponse.

Internal Submission mirrors Task 1 but uses raftengine.Result. Extend the existing structures:

```go
type response struct {
    result          Result
    err             error
    disposition     SubmissionDisposition
    index, term     uint64
    resultAvailable bool
}
// Add to existing request: reportSubmission bool.

func (g *Group) Submit(context.Context, Proposal, string, uint64) (Submission, error)
func (g *Group) Identity() storage.Identity
func (g *Group) submissionBase(Proposal, string) Submission
func (g *Group) stoppedSubmissionError() error
func (g *Group) waitSubmission(context.Context, Submission, *request) (Submission, error)
func submissionResponse(Submission, response) (Submission, error)
func (g *Group) refuseProposal(*request, error)
func (g *Group) answerApplied(*request, raftpb.Entry, Result)
```

- [x] Implement internal types, Submit and immutable identity/base construction. Validate both exact IDs and refuse `_meta` on this entry point. Preserve the existing opaque RequestID behavior of private Propose/ProposeInTerm and metadata service callers.
- [x] Reuse g.slots, g.proposals, MaxPendingProposals, the existing engine timeout and nested five-second enqueue budget. Use the existing short g.mu lifecycle read; introduce no new queue, limit, semaphore, shutdown lock or drain. Receipt waiting uses the existing capacity-one request.reply.
- [x] Return explicit NotSubmitted only before a successful send-select handoff or after owner-proven refusal. Wrap non-accepting/halted errors as Stopped with the original cause. Post-handoff stop without published evidence is Unknown plus Stopped, never a new refusal.
- [x] Extend response with keyed literals throughout engine code/tests (`group.go`, `forwarding_func_test.go`, `reads_func_test.go` are known unkeyed sites). Existing native/read/control paths ignore the new fields. Generic answer remains Unknown; no generic error-code inference.
- [x] Route explicit proposal-admission/encoding refusals through refuseProposal. Preserve exactly one owner-loop retry for the pinned ordinary ErrProposalDropped, retaining correlation and rechecking captured term. Unexpected native errors remain Unknown. Use Invalid for known encoded oversize on reportSubmission requests while preserving legacy private Overloaded behavior.
- [x] Reuse the existing entry schema with Correlation=AttemptID and the exact logical RequestID. Do not add fields or tighten the historical persisted decoder. Before attribution, compare the pending command's RequestID/data with the committed envelope. Fail/remove only a mismatched waiter with Unknown/IdentityMismatch, then apply the independently valid command normally; ensure exactly one reply. Existing invalid-entry and actual Apply-error halts remain.
- [x] On successful Apply, copy the result once and send one Applied response with this entry's index/term and ResultAvailable=true. No intermediate header or secondary completion channel. Preserve business Code/Data, including an empty valid result. The response structure can represent Applied/unavailable/error without implementing new size policy.
- [x] On cancellation/stop, perform the target's one nonblocking reply receive after the initial select: an already-published reply wins; otherwise return Unknown. Never close the reply channel, mutate returned results or wait for a second message. Keep late owner sends nonblocking and keep callbacks under engine lifetime ownership.
- [x] Preserve existing finishRun map/read cleanup and joins without adding proposal-queue cleanup. Add a concise `// TODO(not planned): ...` comment documenting deferred guaranteed queued-payload release at the new path's lifecycle boundary; do not emit warning logs during normal stop.
- [x] Instrument the receipt path with existing proposal/result metrics once. Preserve outcome categories and their meaning; calling legacy Propose from Submit must not double count, and observing Status must not supply receipt evidence.
- [x] Add focused initial tests: pure response conversion/publication-priority cases plus a single-running-engine happy path, pre-dispatch refusal and Apply-error halt/next-call Stopped regression. Use existing fixture helpers and `t.Parallel()`; keep deeper races/forwarding in Task 4.

#### Verification

- [x] `make test`, `make lint`, `go vet ./internal/raftengine` pass.
- [x] `make test-function FUNCTIONAL_ARGS='-v -run "TestSubmission|TestLearnerForwardsAndWaitsForLocalAppliedResult|TestScopedProposalChecksTermOnOwnerLoop|TestConsistentReadObservesAppliedStateAndQueryErrorsDoNotHalt"'` passes. Confirm the new receipt tests actually run; legacy neighboring tests protect shared reply compatibility.
- [x] Confirm no persistence/schema/generated/fixture diff and no queue/limit/default changes. Record results and commit before public activation.

### Task 3: Activate the public API and migrate every caller atomically

**Goal:** switch real Group/Leadership entry points, implement safe bounded retry, and leave all application modules and test build variants compiling in the same commit.
**Files:** `load/{submission,group,leadership,retry,machine}.go` and owning tests; `fault/retry.go` comments only; scheduler/cache examples and their tests; `tests/cluster/testapp` callers; affected root/recovery tests. Inventory all call sites again before editing; do not rely only on the list below.
**References:** target §3 public API/ID/helper contracts, §6 composition boundaries, §7 failure matrix, §8 migrations, §11 public/helper pseudocode; brainstorm final simplification and peer-review consolidation.

```go
func (g *Group) Propose(context.Context, Proposal) (Submission, error)
func (l *Leadership) Propose(context.Context, Proposal) (Submission, error)
type ProposeFunc func(context.Context, Proposal) (Submission, error)
type ProposalRetryOptions struct {
    MaxAttempts    int
    InitialBackoff time.Duration
    MaxBackoff     time.Duration
}
func ProposeRetry(context.Context, ProposeFunc, Proposal, ProposalRetryOptions) (Submission, error)

// Private facade helpers specified by target §3:
func newSubmission(*raftengine.Group, storage.Identity, Proposal) Submission
func newSubmissionFromLeadership(*Leadership, Proposal) Submission
func validateRequestID(string) error
func submissionFromEngine(raftengine.Submission) Submission
func resolveProposalRetryOptions(ProposalRetryOptions) (ProposalRetryOptions, error)
func normalizeSubmission(Proposal, Submission, error) (Submission, error)
func waitProposalRetry(context.Context, time.Duration) error
```

- [x] Generate one opaque AttemptID before Group/Leadership validation, preserve supplied RequestID exactly, and reject empty/whitespace/NUL/invalid UTF-8 IDs before dispatch. Capture original handle identity/run even on refusal where available; nil/zero handles have no invented target. Use core.Submit, expectedTerm=0 for Group and captured term for Leadership. No public compatibility method survives this task.
- [x] Preserve joined leadership context linking. Wrap expired-scope errors as Stopped while retaining the meaningful original/caller cause and receipt. Do not overwrite a delivered Applied success because the caller later canceled, follow a successor, or retry/reacquire a new term.
- [x] Replace fault.Retry delegation with the target's bounded loop. Resolve zero MaxAttempts=3, InitialBackoff=100 ms, MaxBackoff=1 s; reject negative settings; raise max backoff to at least initial; use overflow-safe capped jitter/doubling. Pass caller context unchanged, clone input once plus once per callback, and generate no helper attempt ID before any callback runs.
- [x] Return the final receipt, retaining it on cap exhaustion or backoff cancellation. Retry only through CanResubmit and live budget. Always stop on Applied/Unknown, including Applied+retryable error and business rejection. No aggregate, unresolved counter, per-attempt timeout or uncertainty override.
- [x] Implement only cheap callback-shape/RequestID checks from §3.5. Ordinary error plus zero receipt stays Unknown. Bad receipt becomes Unknown with a terminal error. Valid Applied without result and nil error stays Applied with ErrResultUnavailable. Do not maintain cross-attempt target binding or authenticate arbitrary callback output in the helper.
- [x] Add comprehensive parallel helper/value tests with callback counters and cancellation barriers: refused→success; first Unknown stops before a scripted refusal/success; same logical ID/bytes, distinct callback AttemptIDs; mutation isolation; three-attempt/default/custom cap; no new context deadline; cancellation during backoff; nil callback/invalid options/ID preflight with empty AttemptID; unknown enum and malformed receipt; terminal cause precedence; result-unavailable behavior. These are pure units, not running replicas.
- [x] Migrate public tests in `load/group_test.go`, `group_func_test.go`, `leadership_func_test.go`, `progress_func_test.go`: use `.Result`, assert receipts and retain original lifecycle tests with otherwise-valid RequestIDs. Do not let new empty-ID validation mask a stale-handle/expired-scope test. Keep private legacy engine tests' empty-ID behavior.
- [x] Migrate scheduler `scheduler.go`, `executor.go`, `localdb/recovery_func_test.go`. Separate existing read retry options from ProposalRetryOptions; read retries retain their original behavior. Inspect receipts before discarding them or logging commitment. Preserve task CAS/idempotency guards and leadership cancellation. The scheduler may perform a later fresh-read reconciliation, but must not wrap a failed proposal in an error-only replay loop. An executor must not proceed from an unknown start to calculation, or claim finish committed without Applied/result evidence; preserve its existing error/lifecycle handling unless a narrowly necessary change is recorded.
- [x] Keep `replicaService.propose(...) error` by consuming the receipt inside that method before selecting a scheduler action. Change the example-only `submit(ctx context.Context, group *load.Group, kind string, body any)` return to `(load.Submission, error)` so executor.runTask can make its decision before extracting Result. For live, nonterminal refused/uncertain/applied-unavailable outcomes, log the actual disposition and yield to the existing fresh-read reconciliation path; never log successful commitment or advance from an unknown start directly into calculation. If an executor yield would immediately loop, use a context-cancelable backoff before its next Group.Read; do not keep/replay the old command bytes. Actual caller cancellation, Stopped, invalid input or other terminal failures retain error propagation and lifecycle handling. A NotSubmitted transient exhausted by the short helper cap must not alone permanently fail the replica's Application.Run. Test this application-level state re-evaluation explicitly; the library helper itself still never retries Unknown. No new library error type, detached worker or dedup store is needed.
- [x] Migrate cache `server/redis.go`: separate read and proposal options; consume Submission.Result; add write-specific receipt-aware mapping while preserving read error mapping. Keep `replyError(w resp.Writer, err error)` for reads; add `replyProposalError(w resp.Writer, s load.Submission, err error)` for writes. Emit TRYAGAIN only when CanResubmit is true; use distinct `ERR proposal outcome unknown` and `ERR proposal applied; result unavailable` responses for the other states. Do not call the existing DeadlineExceeded-only TRYAGAIN shortcut for writes.
- [x] Add cache mapping units using an in-memory RESP writer, and extend `loadgen/worker_test.go` so the new uncertain/applied-unavailable server replies are not retried. Do not redesign its whole network retry policy or add a durable Redis token protocol; document separate-call/network uncertainty as application scope.
- [x] Migrate fixture callers in `load_transport.go`, `hooks_cluster.go`, `snapshot_controls.go`, `node_generation_func_test.go`, `read_func_test.go`, and audit `hooks_runtime_cluster.go`, pressure/recovery tests and `tests/recovery/resources_bench_func_test.go`. Use `.Result`; preserve fixture response shapes unless intentionally extended in Task 5. When a response claims the submitted command's committed position, use receipt.Index, not `g.Status().Commit`. Preserve business rejection separately from transport disposition.
- [x] Search all modules again for public Propose/ProposeRetry uses and method values. Root `cluster_ensure_func_test.go` and recovery callers that intentionally ignore the receipt may keep that form with valid IDs. Verify `examples/cluster` still compiles. `tests/sim` currently has no proposal caller; inspect again, record the no-change result, and avoid adding simulation work unnecessarily.
- [x] Update fault.Retry/ProposeRetry documentation to forbid error-only inference of write safety; generic fault code and retry behavior stays unchanged. Add focused `TODO(not planned)` comments for no library dedup/resolver, no remote routing and deferred result-size policy at existing relevant seams, following the exclusion table. No stub APIs or routine WARN messages.

#### Verification

- [x] `make test`, `make lint`, `go vet ./load ./fault ./internal/raftengine` pass.
- [x] `make test-function FUNCTIONAL_ARGS='-run ^$$'` compiles all five functional variants without running functional cases. Compile the process variant with `go -C tests/cluster test -c -tags=clustertest -o /tmp/gquorum-write-receipts-cluster.test .`; do not run it or start Docker yet.
- [x] `make test-function FUNCTIONAL_ARGS='-v -run "TestSubmission|TestLeadershipDirectCancellationAndJoinAcrossTerms|TestGroupWatchReportsAppliedIndex|TestProposalPressureBoundsCancellationAndRecovery"'` passes.
- [x] Run `go -C examples/scheduler vet ./...`, `go -C examples/inmemory-cache vet ./...` and `go -C tests/cluster vet ./testapp/...`. Confirm write-side error mappings and helper tests ran; record results and commit the complete cutover.

### Task 4: Verify evidence races, exact ownership and unchanged bounds

**Goal:** exercise the core contract in owning packages using one local participant or protocol doubles, including the cases that cannot be established by pure receipt-value tests.
**Files:** `internal/raftengine/submission_func_test.go`, `load/submission_func_test.go`; extend existing forwarding/leadership/pressure/metrics tests and untagged shared helper files only where needed. No new multi-node functional fixture.
**References:** target §4 single publication/matching/lifecycle, §5 persistence, §7 failure matrix, §9 DISP-01–10 and additional regressions; §11 engine/waiter algorithms.

- [x] Verify nil-error Apply with empty/nonempty Result, deterministic business rejection and exact identity/run/index/term. Copy ownership survives a machine reusing/mutating its response buffer after the call's serialized ownership permits it. Never derive proof from status.
- [x] Make publication-versus-cancellation tests deterministic: prefill the private reply channel when testing published-result priority; leave it empty when testing Unknown; publish afterward and assert the owner send completes once. Use a controlled callback for cancellation before Apply return. Do not depend on allocating a huge result to make a copy slow, wall-clock races, or production test-only callbacks.
- [x] Verify real Apply failure halts the engine; the current handed-off call is Unknown/error, the next exact-handle receipt is NotSubmitted/Stopped with retained cause, and the public retry helper does not repeat it. A known published Applied result survives later bookkeeping/stop where the original contract allows it.
- [x] Drive ordinary follower/learner forwarding with the existing one-engine protocol double: capture the exact correlation/RequestID, assert no early success, deliver a committed entry and verify local applied coordinates. A lost/delayed forwarded outcome times out Unknown without native ambiguous retry.
- [x] Verify exactly one native definitive-drop retry retains AttemptID and RequestID; exhausted proven drops return NotSubmitted. Recheck captured term after the drop wait, refuse without follower forwarding on expiry, and leave unpublished drop-limbo cancellation conservative.
- [x] Exercise a colliding correlation with conflicting RequestID/data: reply Unknown/IdentityMismatch to only that waiter, apply the independently valid committed entry, keep the engine live and do not send a second response. Test wrong/unrelated correlations separately without changing the application dedup semantics.
- [x] Extend snapshot-subsumed waiter regression: no fabricated result/index and no claim that snapshot position proves this attempt applied. Verify old/stopped handles retain exact original identity/run and never redirect on replacement.
- [x] Extend existing proposal-pressure coverage with nondefault MaxPendingProposals; both receipt and legacy paths share slots and the same queue. Confirm saturation/owner refusal is NotSubmitted when observed, canceled waits release caller slots and owner pruning still clears pending maps. No new cap, queue size change, receipt cache or guaranteed stopped-queue drain assertions.
- [x] Assert metrics count each receipt call/result once and preserve existing legacy/read metrics. Do not make new metric names, labels or emission paths just for this feature.
- [x] Use a fake delivery adapter/callback to preserve a known Applied receipt while stripping the complete result and returning an error; verify no retry. This tests the seam for #12, not an implemented result-size limit. Keep this pure when no runtime fixture is required.
- [x] Verify public Standalone and Group/Leadership boundaries, invalid UTF-8 refusal before encode and stable logical IDs across independently invoked retry calls. Demonstrate that same RequestID does not suppress log application by using an application without dedup in a local owning test; any fixture dedup remains explicitly application-owned.
- [x] Run existing physical compatibility tests and preserved-format tests: baseline load rejection, baseline metadata rejection, frozen scoped metadata/authority and event compatibility. Never regenerate immutable files or change storage schema/version to fit the receipt API.

#### Verification

- [x] `make test`, `make lint` and owning-package vet pass.
- [x] `make test-function FUNCTIONAL_ARGS='-v -run "TestSubmission|TestLearnerForwards|TestForwardedWait|TestUnknownLeaderRetry|TestForwardingSnapshot|TestScoped|TestLeadership|TestProposalPressure|TestBaselinePhysical|TestFrozen"'` passes; inspect report names and ensure intended tests were selected.
- [x] `go test -tags=functional -race -count=1 -run 'TestSubmission|TestLearnerForwards|TestForwardedWait|TestForwardingSnapshot|TestScoped|TestLeadership' ./load ./internal/raftengine` passes. This is focused concurrency coverage, not the full unit-race suite.
- [x] Check `git diff --name-only` for unchanged frozen fixtures/proto/storage artifacts. Record results and commit; full suites run in penultimate Task 6.

### Task 5: Expose receipts through the authenticated process fixture and add three-node regressions

**Goal:** verify public load behavior across real replicated processes without implementing the deferred GroupClient or changing production protocols.
**Files:** create `tests/cluster/testapp/submission_controls.go`, `tests/cluster/submission_test.go`; register the fixture helper in existing `hooks_cluster.go`; add fixture helper/tests or `tests/cluster` request decoding where useful. Reuse public Group/Node APIs only.
**References:** target §6.1 deferred remote-client boundary, §7 applied/business/idempotency flows, §8 fixture migration, §9 process acceptance; target §5 explicitly leaves application snapshot/DB schema unchanged.

Fixture-only contract, extending the target's existing authenticated-control integration point:

```go
// tests/cluster/testapp: guarded by the existing test-credential wrapper.
func (a *application) registerSubmissionRoutes(
    mux *http.ServeMux, protect func(http.HandlerFunc) http.HandlerFunc)

type submissionReply struct {
    Submission load.Submission `json:"submission"`
    ErrorCode  string          `json:"error_code,omitempty"`
    Error      string          `json:"error,omitempty"`
}
```

| Fixture route | Input | Output / ownership |
|---|---|---|
| `POST /internal/test/submission/{group}/{replica}` | Existing putCommand JSON, unchanged logical ID and command bytes | Invoke the exact published local Group.Propose once; return submissionReply, preserving receipt on error. This is not a library RPC or receipt-authority service. |

- [x] Register the route through registerClusterRoutes and its existing credential protection. Validate load group/replica selection before invocation; keep pre-route HTTP errors distinct from actual load.Submission. Do not invent a receipt for work that never called Propose. For a completed library invocation, return a structured envelope (including nonnil library error) so the test can inspect both outputs.
- [x] Reuse the existing caller FSM. It stores RequestID fingerprints and returns nil-error Result.Code for deterministic business rejection; do not add a new dedup table/schema or claim library deduplication. Keep the old internal propose route/API responses compatible for existing tests.
- [x] Add `TestSubmissionFollowerAndLearnerAppliedThreeNodes`: acquire a three-node pool lease after t.Parallel; establish approved exact group membership with an intentional learner using existing topology helpers. Submit through a voter follower and through the learner, and assert the receipt identifies the origin applier, has nonzero entry coordinates/RunID, preserves RequestID and returns the original result. Native consensus must still require the voters.
- [x] Add `TestSubmissionBusinessRejectionThreeNodes`: after applying a command, reuse its logical RequestID with different bytes through the fixture's exact route. Assert Applied + ResultAvailable + original VersionConflict business Code with no library transport error, and prove the first business value remains. The rejection must not be relabeled NotSubmitted.
- [x] Add `TestSubmissionConcurrentLogicalIDThreeNodes`: concurrently submit identical logical ID/data through two exact replica hosts. Assert distinct AttemptIDs and log positions, both applied receipts, and the fixture's own dedup keeps one logical state effect. These assertions distinguish repeated log applications from application-level duplicate suppression.
- [x] Reuse public stale-handle controls/coverage where available; no extra retained-result registry, post-return resolver, per-attempt background worker, fourth node or new cluster-wide fault harness is introduced. Lost remote envelopes/spoofed GroupClient replies remain deferred to #9; do not use a new fixture endpoint to claim its transport contract implemented.
- [x] Keep process cases in the clustertest suite, bound deadlines, preserve pool/container/volume cleanup, and use observable membership/application barriers rather than arbitrary sleeps. Place any single-participant fixture tests in functional-tagged files and pure serializers in unit-tagged files.

#### Verification

- [x] `make test`, `make lint`, `go -C tests/cluster vet ./testapp/...` and compile-only `go -C tests/cluster test -c -tags=clustertest -o /tmp/gquorum-write-receipts-cluster.test .` pass.
- [x] Focused fixture functionals pass if changed. Docker execution is deliberately deferred to the penultimate Task 6 checkpoint, after all fixture/source edits and full lint; do not build/replace a shared image tag during another run.
- [x] Record compile/local results and commit the fixture and process scenarios with their runtime gate explicitly pending Task 6.

### Task 6: Update documentation and run final validation

**Goal:** make current documentation describe the activated implementation while keeping the brainstorm/target as design history and preserving all exclusion decisions.
**Files:** `README.md`, `docs/architecture/library.md`, `raft.md`, `runtime.md`, `docs/testing/tests.md`, `examples/scheduler/README.md`, `examples/inmemory-cache/README.md`, `tests/cluster/testapp/README.md`, and owning GoDoc comments. No proposal document or new architecture hierarchy.
**References:** target §1–3 public semantics, §4.3/4.5 publication/retention, §6 deferred integration, §8 migrations, §9 verification; all source statements checked against Tasks 1–5.

- [ ] Explain the one-time breaking API migration for both Node-owned and Standalone groups, required stable RequestID, opaque AttemptID, exact receipt identity and meaningful fields. Show callers consuming both Submission and error; handle business Result.Code only when available.
- [ ] Show CanResubmit as the only write retry predicate. Explain that it does not grant authorization or account for the caller budget; generic fault.Retryable remains useful for non-write transient classification and is insufficient for proposal safety.
- [ ] Document optional three-attempt refusal-only retry, timing defaults, unchanged caller context/engine cap and configurable-election gap. Do not promise a healthy pending command is resent or that the default spans failover. Explain separate-call uncertainty and application dedup responsibility without an exactly-once claim.
- [ ] Replace prior atomic-header/cleanup descriptions with one terminal channel publication and the accepted retention limit. Explicitly state no new/changed MaxPendingProposals, queue capacity, enqueue budget, byte limits or other resource settings.
- [ ] Explain cache uncertain/applied-unavailable responses and request-ID scope; distinguish its demo network retry policy from the library guarantee. Explain scheduler fresh-state reconciliation and why unknown command outcomes do not become confirmed success or automatic identical replay.
- [ ] Document the authenticated fixture receipt route and new process cases as test-app-only. Keep README meta-only example boundaries and normal two-port library integration intact. Preserve old test reports as historical; do not add pass counts until execution supplies them.
- [ ] Describe #9/#12 integration requirements and actual coverage honestly; do not claim end-to-end remote receipt authenticity or configured proposal-result size limits exist. Add/retain only the targeted `TODO(not planned)` comments from the exclusion table, no unused methods or WARN logs for normal operation.

#### Verification

- [ ] Verify API names/signatures, relative links, source paths, commands and example snippets against implemented code; run `git diff --check`.
- [ ] Run final five-module units/race, functionals/race and full lint, then build the dedicated image and execute focused receipt and non-performance cluster suites plus scheduler/cache/meta Compose checks. All cluster execution belongs to this penultimate task only. Inspect reports and cleanup; fix failures and rerun affected checks.
- [ ] Record final gate results and commit; the public changelog is created in the final audit task.

### Task 7: Final validation and architectural delta verification

**Goal:** validate the whole activated feature, repair every target gap, and prove all required paths are used by real callers.
**Files:** all touched components; create `docs/changelogs/20261026-write-receipt-contract.md`; update this plan's progress/gap sections. Target/source documents may be corrected only to reflect an explicit design decision, never to hide an implementation omission.
**References:** ALL target sections, including §5 unchanged durable schema and §6/§10 explicit deferrals; full brainstorm and final peer/user decisions.

#### Verification gates

- [ ] Confirm Task 6's final evidence covers the current implementation. Repair audit gaps and rerun affected non-cluster checks; reopen Task 6 for any necessary cluster rerun rather than running it in the final task.
- [ ] After the final audit/verification commit, ask Claude agent 77d41568-9e29-4cab-a227-ce5be07190c0 to review all implementation commits since eca48ad. Verify and resolve findings, revalidate affected work and record review disposition before declaring completion.

#### Architectural delta audit

- [ ] **Public load API:** compare every Submission field, disposition value, CanResubmit, ErrResultUnavailable and Group/Leadership/ProposeFunc/ProposeRetry/options signature with target §3. No aliases, aggregate result, ambiguous-retry flag or extra API survived. Value/ID/cancellation semantics match docs.
- [ ] **Engine:** compare Submit/Identity/base/stop/wait/response/refusal/applied functions and reportSubmission/response fields against §4/§11. Verify one terminal publication, buffered-result precedence, actual entry coordinates, owner-only proof, mismatch waiter removal without halt, actual Apply-error halt and exactly-once metric accounting per call.
- [ ] **Leadership and lifecycle:** verify captured-term admission after native-drop waits, no follower forwarding, no successor redirection, terminal stopped/expired causes, unchanged callback joins and existing map/read cleanup. No receipt-specific gate, queue drain or cleanup callback exists. Accepted retained queue behavior is documented rather than misreported as guaranteed release.
- [ ] **Capacity and persistence:** compare target §4.5/§5 with source. Reuse the same MaxPendingProposals slots, capacity-256 queue and nested enqueue timeout. No new in-flight limit. No DDL, bbolt buckets, application schema, cache TTL, log/snapshot fields, protocol schema or fixture bytes changed. Verify immutable physical fixture provenance and public entry encoding compatibility regressions.
- [ ] **Retry helper:** inspect the actual loop and tests for unchanged bytes/RequestID, no new context deadline, three-attempt defaults, capped overflow-safe backoff, preserved final receipt, no second call after Unknown/Applied, cheap shape normalization, no cross-attempt target state and no generic fault behavior change.
- [ ] **Examples and caller fixture:** audit scheduler creation/start/finish/reconciliation decisions, cache server/loader response handling, Standalone callers, root/recovery tests and all three separate example modules. Follow receipts through any application helper that still returns only Result/error; it must not erase evidence needed by an outer write retry decision. Existing application dedup remains caller-owned.
- [ ] **Process/adapter scope:** verify the authenticated test endpoint is present only in the caller fixture, uses public exact Group APIs, returns actual receipts and runs under the three-node pool. Map DISP-01–10 to actual tests/results; mark #9/#12 end-to-end checks as intentionally deferred rather than passed. Confirm no accidental production RPC, third port, fake remote verifier or unused integration stub.
- [ ] **Documentation and future extension:** compare README, architecture, GoDoc, fixture/example guides and exclusions with the implementation. Future routing/resolution can be additive without weakening current stop-on-uncertainty semantics. Generic fault docs direct write callers to the receipt predicate.
- [ ] **Gap resolution:** list each discovered mismatch in NOT IMPLEMENTED GAPS with source/target references, fix within the agreed architecture, and revalidate affected behavior. Only explicitly user-approved deferrals may remain as such; otherwise mark an unfixable item `[_]` and leave this task incomplete. Do not lower acceptance tests or rewrite the target to declare a gap solved.
- [ ] **Public changelog:** create `docs/changelogs/20261026-write-receipt-contract.md` after verification. Briefly describe the breaking receipt return, required logical IDs, safer refusal-only retries and clearer stopped/outcome handling. Write for library users/managers, not internal implementers; include no unmeasured performance claims or deferred features as delivered.
- [ ] **Actual wiring — FINAL SUBTASK:** trace Node/Standalone-published Group.Propose → engine Submit → owner admission/native consensus → matching Apply → one reply → public Submission; trace Leadership through captured-term Submit; trace ProposeRetry through CanResubmit; trace cache/scheduler/fixture consumers and registered authenticated fixture route. Verify every required component is constructed/called from a real entry point, all source modules build, and no part is merely present for tests or left behind an unselected adapter. Record the final source revision, evidence, gap dispositions and completed task commit.

## Validation Commands

Run from the repository root unless `go -C` selects a module. These are implementation-time commands, not evidence of tests run during planning. Use the current Makefile runner so reports and failure exit codes are preserved.

**Normal code-task checkpoint:**

```sh
make test
make lint
go vet ./load ./fault ./internal/raftengine
git diff --check
```

Use each task's focused functional/vet commands in addition. Format edited Go files with gofmt before running them. Five-module test flags are selected through `UNIT_ARGS` and `FUNCTIONAL_ARGS`; do not substitute root `go test ./...` for the all-module checkpoint. Test functions proposed in this plan must exist under those prefixes before treating a filter's success as coverage. Make consumes dollar signs in variables: the compile-only regex is written `FUNCTIONAL_ARGS='-run ^$$'` so the executed Go command receives `^$`; using a single dollar sign would accidentally run all functional tests.

**Final gates, executed in penultimate Task 6:**

```sh
make test
make test-race
make test-function
make test-function-race
make lint-full
make test-cluster-build CLUSTER_IMAGE=gquorum-test:write-receipts-final
make test-cluster-run CLUSTER_IMAGE=gquorum-test:write-receipts-final CLUSTER_ARGS='-v -run ^TestSubmission'
make test-cluster-run CLUSTER_IMAGE=gquorum-test:write-receipts-final CLUSTER_ARGS='-v -skip ^TestPerf_'
make test-example
make test-scheduler-example
make test-cache-example
git diff --check
```

The full clustertest command explicitly excludes the repository's TestPerf_* cases; no performance gate, benchmark, large-snapshot qualification, broad simulation or extra chaos campaign is requested. Existing correctness/failure-recovery cases in the ordinary clustertest suite remain part of final regression coverage. The target requires multi-node behavior, so Docker correctness gates are not optional substitutes for functional tests. Do not replace the dedicated image tag while a run uses it; rebuild it after any fixture/application change before rerunning affected process tests.

If tests/sim acquires a changed public caller during execution, compile/check its actual sim build variant using its documented toolchain and record that scope; no sim execution is needed for the currently inspected no-call-site state. No proto source changes are planned, so make generate/proto-check is not a routine gate; investigate any unexpected generated/protocol diff instead of normalizing it away.

For failures read `test-reports/<suite>/latest/report.md`, `timeline.md` and package logs. Record the fresh run directory and actual result. Verify run-owned containers/volumes/workers are cleaned up without deleting unrelated resources. Record skips and unmet environment prerequisites honestly; they are not passing gates.

## Acceptance coverage and dependency gaps

| Issue case | Planned evidence | Completion boundary |
|---|---|---|
| DISP-01 not submitted | Tasks 2–4 validation/admission/stop/oversize cases | Core |
| DISP-02 queued cancellation | Tasks 2/4 buffered publication and real queued wait | Core |
| DISP-03 applied/lost reply | Task 4 no publication observed → Unknown; late send is nonblocking | Actual remote lost-reply/resolution behavior waits for #9 |
| DISP-04 unknown then later refusal | Task 3 scripted callback confirms second call never happens; independent calls retain separate scope | No unsafe aggregation API is introduced |
| DISP-05 business rejection | Tasks 2/4 local Apply result; Task 5 three-node fixture rejection | Core + process |
| DISP-06 result too large | Tasks 3/4 Applied-unavailable adapter representation and stop behavior | Actual size configuration/refusal waits for #12/#9 |
| DISP-07 wrong receipt identity | Tasks 3/4 per-response shape/ID checks and exact local attribution | Remote authenticated correlation/lineage checks wait for #9 |
| DISP-08 generic adapter error | Task 3 zero receipt/error → Unknown and no retry | Core helper; remote compatibility waits for #9 |
| DISP-09 concurrent logical ID | Task 4 non-deduplicating machine; Task 5 distinct attempts with caller FSM dedup | No global exactly-once claim |
| DISP-10 IDs | Tasks 1–4 required logical ID, immutable retries, fresh AttemptIDs; Task 5 exact process receipts | Remote restart replay waits for #9/application ID persistence |

## NOT CONSIDERED & TODOs

These exclusions were already approved in target §10 and the user addenda; this plan does not seek them again or add uncallable NotImplemented APIs. Use short `// TODO(not planned): ...` comments at relevant existing seams where they help prevent accidental scope expansion. No normal-path WARN logs for intentionally unsupported capabilities.

| Tag | Excluded capability | Current behavior / comment placement |
|---|---|---|
| postponed | Guaranteed queued-payload release on retained stopped handles | Existing queue lifetime; comment at receipt enqueue/lifecycle seam, document count versus bytes; no gate/drain |
| potential-improvement | Retry-until-required-deadline mode | Three-attempt default; comment on ProposalRetryOptions/loop if needed; no extra option now |
| potential-improvement | Definite non-submission during native-drop limbo | Unknown unless owner proof is already published; comment near deferred-drop/cancellation boundary |
| postponed | #9 remote GroupClient, ingress/session authorization, wire/reconnect policy | Exact local handles only; comment in proposal integration GoDoc; no production RPC/route/stub |
| postponed | #12 limit discovery/preflight and proposal-result size policy | Represent Applied without result, no new configured result bound; comment near response delivery |
| postponed | Library durable deduplication and result retention | Application-owned RequestID semantics; comment in Proposal/Submission GoDoc |
| potential-improvement | Generic outcome query/resolver across calls/restarts | Unknown remains Unknown without actual application evidence; comment with receipt scope |
| potential-improvement | Persistent external Redis operation IDs / broader client retry redesign | Preserve application-protocol boundary; uncertain replies are not advertised as safe retries; comment in example documentation/code where relevant |

No new tables, ORM models, DDL, bbolt keys, durable receipt records, migrations, TTL caches or storage roots are required. Existing physical stores and schemas are an explicit invariant to verify, not missing implementation tasks.

## NOT IMPLEMENTED GAPS

None identified at planning time within the approved core scope. Intentional #9/#12 and other exclusions are listed above and are not delivered capabilities. During execution add each unexpected gap with its target section, affected file, consequence, owning task, fix and verification. Use `[_]` for an unresolved implementation gap and leave the owning/final task incomplete; do not confuse this marker with an approved deferral.

## NOT PLANNED CHANGES

None at planning time. Record each necessary deviation or unrelated-test repair here with files, rationale, narrow diff/explanation and validation; no silent API/scope expansion.

## Progress log

| Task | Commit | Actual checks / report directories | Result and notes |
|---|---|---|---|
| Planning | Recorded in Git history | Source/target/call-site inspection; document validation only | Seven-task grouping accepted. No implementation task executed. |

## Planning verification record

The target's final reviewed data structures, algorithms, unchanged schema/limits, callback ownership and exclusion decisions were used directly. The only new concrete integration detail is the authenticated **test fixture** receipt route required for process assertions; it adds no library route, protocol field or public service. Current caller inspection also requires separate read/proposal retry options, cache load-generator response classification coverage, explicit propagation/consumption of scheduler executor receipts, and Standalone migration. The scheduler's existing fresh-read/CAS application model supplies the re-evaluation boundary; a short helper refusal must not become a permanent replica failure solely through the old result-only adapter. These elaborate target §8/§9 without changing the target design.

Before the planning commit: verify links and target fingerprint, exactly seven task headings, explicit target references and verification in every task, all implementation checkboxes unchecked, the final audit decomposed by subsystem, and its last checkbox proving actual wiring. The planning session does not execute the implementation gates above or create the future changelog.

Planning checks passed on 2026-10-26: local links and target fingerprint match; all seven tasks contain references and verification; implementation boxes are unchecked; the final task ends with the wiring check; Go snippets parse with gofmt; `git diff --check` passes. A Make dry run verified the compile-only dollar escaping and final process-test exclusion flag. These are document/command checks only, not implementation validation.
