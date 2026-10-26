# Write receipts and uncertain outcomes — brainstorm

Date: 2026-10-26. Examined implementation: `8e57798`.
Status: BRAINSTORM DONE. Target consolidated after user decisions, reference comparison and peer review. No implementation changes. Earlier decisions below are session history; the final consolidation supersedes the aggregate result, ambiguous-retry opt-in, atomic-header publication and shutdown-gate proposals.
Target specification: [write receipt target](20261026-write-receipt-contract-target.md).

## Request and sources

User: `/go-brainstorm design how to resolve https://github.com/asalimonov/gquorum/issues/13`; produce the brainstorm and target architecture.

[Issue #13 / GQ-07](https://github.com/asalimonov/gquorum/issues/13) asks for reliable attempt-level not-submitted, applied, or indeterminate evidence; stable caller-owned logical IDs and distinct attempt IDs; applied business rejections; preservation of applied evidence when results are unavailable; conservative retry aggregation; exact identity checks; and no exactly-once claim. Its candidate API is explicitly open for design. Acceptance cases are DISP-01 through DISP-10.

The live issue and [#9 / remote group client](https://github.com/asalimonov/gquorum/issues/9) and [#12 / payload limits](https://github.com/asalimonov/gquorum/issues/12) were read through GitHub CLI. All three had no comments at inspection. Their public baseline is `14a54c2`; this worktree additionally contains the large-snapshot change `8e57798`.

Read README, relevant library/Raft/runtime architecture, testing guide, compatibility fixture provenance and current sections of the API audit/validation records. Inspected public Group/Leadership/ProposeRetry; engine proposal admission, queueing, RawNode handoff, definitive-drop retry, Apply, snapshot replacement and shutdown; fault retry policy; local ownership identities; existing forwarding/leadership tests; example and process-fixture callers. There is no `project.txt`; this worktree is the project. No nested AGENTS.md was found.

## Current implementation findings

1. `load.Group.Propose` returns `(Result, error)` through `raftengine.Group.Propose`. Followers/learners forward through native Raft and wait for their own exact local replica to apply.
2. `raftstate.Entry` already separates `Correlation` from `RequestID`. Its existing encoding can carry an attempt ID without adding fields or changing schema.
3. Empty RequestID currently becomes the internal correlation. Explicit IDs are checked for non-whitespace content and absence of NUL; there is no distinct identifier byte limit. JSON encoding means new API validation must also reject invalid UTF-8 to preserve the exact string.
4. Before queue handoff, admission failures can prove non-submission. After queue handoff, cancellation alone cannot prove it. `submit` currently chooses among reply, context and engine completion and discards result evidence on the cancellation branch.
5. `handleProposal` checks readiness, bounded work, expected leadership term and encoding before RawNode.Propose. Only the exact local `raft.ErrProposalDropped` permits one owner-loop retry, keeping correlation. The pinned etcd Raft v3.6.0 source was inspected: ordinary proposal drop branches precede append/forward. Generic transport errors do not have that meaning.
6. `applyNormalEntry` has actual committed index/term and the nil-error Apply result, but discards the position when answering. Apply errors halt the replica; business rejection in Result.Code is normal applied work.
7. Installing a snapshot invalidates pending local result waits with an uncertain outcome. A snapshot index alone does not identify individual applied proposals.
8. `fault.Retry` returns zero values on failure and only the last error. `load.ProposeRetry` delegates to it, supplies the same Proposal but does not own an immutable copy across callback mutation, and has no aggregate uncertainty record.
9. `Leadership.Propose` replaces an engine error with caller cancellation; the revised path must preserve receipt evidence while retaining errors.Is behavior.
10. Current load transport carries Raft/control/snapshots, not a library remote application Propose RPC. #9 and #12 remain separate features. Receipts must specify their integration rules without claiming those features already exist.

## Discussion and decisions

### Public API

Options discussed: additive receipt methods, breaking Propose return type, or typed errors only. Initial recommendation was additive methods for compatibility.

User selected: **“Change Propose to return receipts; accept a breaking API change.”**

Resolution: change Group.Propose and Leadership.Propose directly; revise ProposeFunc/ProposeRetry; migrate repository callers. Require caller-owned nonempty RequestID on the public load path. Keep the private metadata proposal adapter's existing contract. No public compatibility alias or parallel ProposeWithReceipt method is needed.

### Retrying uncertain attempts

Initially offered continuing retries with separate retained uncertainty versus stopping on uncertainty. User asked for consequences before choosing.

Explained reservation example: a timed-out “reserve 100” may already have applied. Repeating it can reserve twice unless the state machine deduplicates or the command otherwise tolerates repetition. Stopping avoids an automatic repeat but leaves the application to resolve the result. An immediate not-found read can race with the first attempt committing later. Both policies can retry proven non-submission.

Revised recommendation: stop on uncertainty by default, explicit opt-in for commands safe to repeat.

User selected: **“Stop on uncertainty by default; allow explicit opt-in to continue (Recommended).”**

Resolution: load-specific retry options distinguish error retryability from permission to repeat unknown work. Aggregate evidence remains separate from the last attempt receipt. A later applied attempt never erases earlier unresolved attempts. Bound attempts and keep one parent context.

### Idempotency token

User asked: **“Can we add unique marker like ‘idemoptency token’ or it is not applicable?”**

Explained that RequestID already serves this purpose, but a token alone cannot suppress duplicates. The application can atomically persist `(request ID, command fingerprint, business result)` with its mutation; same token/same command returns the recorded outcome, and conflicting reuse is rejected. Include these records in snapshots/replay. Durable library deduplication would additionally require result retention, expiry and capacity semantics and would change the established application boundary.

User selected: **“Use RequestID; application enforces idempotency (Recommended).”**

### RequestID format

User asked: **“What format of RequestID?”**

Answered: Go string, no mandatory UUID encoding. Examples: `550e8400-e29b-41d4-a716-446655440000` or `tenant-42/reservation/order-987/create`. Current explicit-ID restrictions are non-whitespace and no NUL; encoded proposal limits include the identifier. Proposed ingress rule adds valid UTF-8 and removes implicit generation. Preserve exact spelling, including meaningful surrounding whitespace; reject whitespace-only values rather than trimming them. Application IDs are unique in its deduplication scope, retained across retries/restarts, and different for distinct logical operations. Format details are a design recommendation, not a separately confirmed user constraint.

### Final retry-contract feedback

Presented the concrete draft behavior: three attempts by default, configurable; stop on uncertainty unless explicitly enabled; separate retry summary; a successful retry returns its result while earlier unknown attempts remain visible.

User selected: **“Yes, keep this contract (Recommended).”**

Resolution: the retry limit/default and separate last-attempt versus aggregate semantics are confirmed. No blocking design questions remain for the core scope. Internal implementation choices remain specification recommendations; #9/#12 integration is explicitly accounted for rather than claimed implemented.

## Design resolutions to carry into target

- Three dispositions, with unknown as zero; ResultAvailable is independent of business acceptance.
- No generic fault code is non-submission proof.
- Receipt identity names the exact local applier/target and engine run. Index/Term come from the applied entry, never Status.
- Publish known applied evidence before copying/delivering the result; cancellation snapshots owned evidence without waiting for a blocked Apply.
- Existing encoded correlation becomes AttemptID for the new load path. Native definitive-drop retry retains it; caller/helper retries get fresh IDs.
- Retry summary has constant space: last receipt, attempt count and unresolved count. No receipt history cache, durable lookup endpoint, background resolver or new lifecycle.
- Ordinary receipts are trusted API values, not signed transferable certificates. Future network receivers must authenticate and match their outstanding exact attempt before accepting evidence.
- Empty IDs are invalid before proposal dispatch. Missing/malformed custom-adapter evidence stays unknown.
- Core implementation does not implement #9 routing or #12 size-policy discovery. Specify dependency acceptance separately so those cases cannot be reported as end-to-end passing prematurely.

## NOT CONSIDERED & TODO

- [postponed] #9 remote routing, authorization policy and wire rollout: implement in its own feature, satisfying this receipt contract.
- [postponed] #12 payload-limit configuration/preflight: implement separately; preserve applied receipt whenever a result is stripped.
- [potential-improvement] Durable application outcome lookup and dedup examples: application concern; no generic lookup endpoint or NotImplemented stub in this change.
- [postponed] Library-owned persistent deduplication/result retention: outside scope by user choice.

## Validation record

Documentation-only validation on 2026-10-26: `git diff --check` passed; relative Markdown links, code-fence pairing and referenced existing Go paths checked successfully. Exactly the two brainstorm/target files appear in the diff. The directory is ignored by repository policy, so these files were added with intent-to-add (`git add -N -f`) to make their content reviewable; no commit or staged file content was created.

Local target review clarified a narrow enqueue/shutdown gate so stopped engines cannot retain newly queued receipt references after draining. It also identified that example SET/DEL repetition is not automatically harmless with concurrent intervening writes, so migration must review each opt-in instead of assuming command-name idempotency is sufficient.

No Go tests, race tests, Docker scenarios or implementation changes ran. Historical reports are not new validation. The target's acceptance matrix is a future implementation requirement, including explicit #9/#12 dependency gaps.

## Summary

The target changes public load Propose to return an attempt receipt, requires a stable caller-owned RequestID, and keeps durable idempotency in the application. Following the final simplification below, the optional helper returns the same Submission type, retries only proven non-submission with a transient cause, and always stops on an unknown outcome. Engine-owned evidence preserves actual application independently of result delivery and caller cancellation. Existing log/storage formats and exact-replica/leader-term restrictions are preserved. The companion target includes public/internal signatures, state inventories, lifecycle/data flows, failure tables, caller migration, acceptance ownership and key-method pseudocode for implementation planning.

BRAINSTORM DONE

## Follow-up: comparison with etcd, TiKV and PD

The user requested source research in `/Volumes/M4DATA/Users/speckzzz/reps/neria/{etcd,tikv,pd}` and questioned whether frequent heartbeat/replication eliminates the need for proposal retries.

Completed a [pinned-source comparison](20261026-proposal-retry-reference-comparison.md). The supplied PD path is absent; PD and TiKV client-go were fetched into a temporary directory at versions pinned by the local TiDB go.mod. No reference-project code was changed. The local etcd source is a development revision, so the study does not label that particular checkout as a stable release.

Findings: ordinary etcd and TiKV server proposals are not resubmitted on every heartbeat. Clients have separate operation-specific retries. TiKV's transaction client retains uncertainty across retries and resolves logical outcomes with transaction identity/commit records; gquorum cannot infer equivalent domain evidence from an arbitrary Result. PD delegates metadata durability to etcd and uses separate retry policies for timestamp/control RPCs. TiKV's periodic store-heartbeat reports explicitly avoid repeating the old report. Raft replication catch-up, proposal admission/routing retry and business outcome resolution are distinct.

The prior user-confirmed choices remain: optional helper, three-attempt default, stop on uncertainty unless enabled, and retained aggregate uncertainty. Research recommends refining the unconfirmed timing detail: do not automatically shorten the engine's 30-second proposal wait to the generic helper's three seconds; use the existing caller/engine budget unless a shorter attempt timeout is explicitly configured. This is recorded as a proposed refinement for discussion, not silently adopted as an approved contract change. Scoped leadership retries cannot reacquire a new term, and future routed calls should have one retry owner.

Validation: read-only source inspection; no runtime tests. Reference comparison and cross-links checked with the documentation diff. The source comparison is complete; it does not reopen already agreed receipt semantics.

## Final simplification — approved after reference comparison

The user requested a brief list of changes and stated a preference for a simple, reliable approach that can be extended without breaking APIs. The proposed simplification retained the agreed breaking Propose return change and the receipt fields, while removing uncertain-retry opt-in, RetrySubmission, unresolved counters and the implicit three-second timeout. The helper would retry only proven non-submission with a transient cause, return Submission directly, share the caller budget, and stop on expired leadership. Future remote routing or deduplication-aware repetition would use separately named APIs without weakening existing semantics.

User response: **“I agree, modify the target architecture.”**

This decision supersedes the earlier confirmation of an aggregate retry result and opt-in uncertain retries. The three-attempt default remains a configurable cap for eligible refusals. ProposalRetryOptions now contains only MaxAttempts, InitialBackoff and MaxBackoff. There is no per-attempt timeout setting or shorter implicit wait; the engine retains its existing cap and the caller owns the overall deadline.

Updated the target's API, decision table, flow diagram, retry algorithm, external-client extension contract, state inventory, migration guidance, failure matrix and tests. The helper stops at the first Unknown, so it cannot overwrite that outcome with a subsequent refusal and needs no uncertainty accumulator. A helper-side validation failure before invoking a callback explicitly reports NotSubmitted with an empty AttemptID; otherwise it preserves the actual last attempt receipt.

Expired Leadership errors use existing fault.Stopped while preserving their cause and receipt; retry eligibility treats terminal classifications as dominant over nested transient causes. Deterministic encoded-size rejection on the new receipt path uses fault.Invalid rather than legacy Overloaded to avoid retrying an identical permanently oversized request. Neither change adds a new public error type or storage format.

The target is current; the reference comparison remains historical research with an explicit note of this later decision. No implementation or reference-project code changed. Documentation-only checks cover links, code fences, stale removed API references and `git diff --check`.

BRAINSTORM DONE

## Final consolidation after peer review

The [paseo-peer review](../review/feat_receipt-error-contract.md) completed with verified corrections and optional simplifications. The user explicitly resolved the remaining cleanup disagreement with **“Remove it, its additional API, we can add it in future.”** Then requested **“Now you need to update the target architecture after all decisons and reviews.”** Claude separately relayed the user's decisions **“Keep the current and document the gap”** for the retry default and **“Use the single send”** for result delivery. All are incorporated in the target.

Current implementation specification:

- One terminal publication through the existing capacity-one reply channel. No atomic receipt cell, intermediate Applied header or extra completion channel. A waiter that stops before observing publication can return Unknown even after Apply completed; a delivered known-applied failure preserves Applied with ResultAvailable=false.
- No new shutdown enqueue gate, proposal-queue drain or cleanup API. Retained stopped handles may keep queued payloads reachable; queue count is bounded but bytes are not. Existing map/read/callback/transport cleanup and joined resource ownership remain. Guaranteed queued-payload release is a future internal concern.
- Stopped/halted receipt-path errors are terminal Stopped with the original cause retained. Pre-handoff refusal is NotSubmitted; post-handoff stop without a published outcome stays Unknown.
- A correlation/command attribution mismatch fails only its waiter. The valid committed command still applies; malformed entries and actual Apply failures retain existing halt behavior.
- Public Submission.CanResubmit owns the write retry predicate, including terminal precedence. ProposeRetry and write-specific caller guidance use it. Generic fault.Retry/fault.Retryable behavior is unchanged.
- The helper has no cross-attempt target binding; its trusted callback owns logical targeting. Retain cheap per-response RequestID/shape checks and preserve valid Applied-without-result evidence. Remote adapters retain exact authenticated response checks.
- Keep the three-attempt default and backoff defaults; document the gap relative to configurable elections and the caller's deadline responsibility. No shorter implicit per-attempt timeout. Token redesign and extra definitive-drop precision are deferred.
- Migration explicitly includes Standalone.Group users, cache receipt-aware TRYAGAIN mapping and scheduler distinction between a fresh reconciliation and repeating an unknown write.

Updated signatures, internal structures, flow diagram, state/lifecycle descriptions, failure table, migration inventory, regression requirements and pseudocode together. No implementation, storage formats or reference-project code changed. Documentation checks passed for local links, code fences, whitespace, removal of superseded mechanisms and `git diff --check`. All 20 Go snippets parsed through gofmt; this is syntax validation only, not compilation/type checking or test execution. No Go/runtime tests are claimed.

BRAINSTORM DONE

## B2 addendum: existing concurrency bounds are reused

Claude relayed the user's clarification in the peer thread. On dropping new shutdown cleanup, the user said **“I agree, if it will not introduce/maintain limit on a number on in-flight requests.”** After Claude clarified that existing bounds remain unchanged and the receipt feature adds none, the user confirmed **“I agree, notify the colleague to update brainstorm and the target architecture.”**

The target now states explicitly that receipt proposals reuse the existing MaxPendingProposals admission slots (default 256, configurable), fixed proposal queue capacity 256 and nested five-second enqueue budget. No receipt-specific semaphore, queue, in-flight cap or capacity option is added. Increasing MaxPendingProposals increases the configurable admission-slot allowance; existing queue and other admission constraints still apply. MaxAttempts is a sequential retry bound within one call, not a concurrency limit. Existing bounded queues are not removed.

B2 remains approved: no new shutdown gate/drain, with retained stopped-handle payload lifetime documented and cleanup deferred. This is a documentation clarification within the already-authorized target consolidation; no runtime limits or implementation code changed.
