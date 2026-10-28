# QCM-001: restart after host preparation but before admission Ready

**Status:** blocked automatic continuation reproduced in Quint; Go regression pending. This is a candidate availability defect in the implemented protocol, not a confirmed Go execution failure or a safety violation. Production code is unchanged.

## Scenario and observed result

Three established nodes approve a fourth member. The fourth process receives its join grant, durably pins the assigned member, starts its local metadata engine, and completes the granted host-preparation action. The preparation receipt reaches metadata and the operation advances to AddLearner. Its Ready RPC has neither been sent nor committed. The fourth process then crashes and restarts with the same valid pin/storage and a new incarnation.

The model reproduces two simultaneous refusals:

- Rebinding the Pending admission to the restarted process is refused because the exact member already has a consumed metadata-preparation receipt.
- Adding the learner is disabled because the admission has no Ready acknowledgement or completed native prefix.

The metadata admission still names the old process, while the old process can no longer emit Ready. The restarted process is available and retains the same member ID; the original three-node metadata quorum remains healthy. This does not depend on storage loss, metric degradation, capacity exhaustion or a lost majority.

The desired-success check `retainedPreparationRestartShouldContinueTest` fails at the learner precheck with QNT513. The separate positive witness `preparedRestartBeforeReadyExposesBindingBlockTest` verifies the old admission identity, retained pin, new incarnation, explicit binding refusal and disabled learner admission. The acknowledged-admission restart scenario still completes.

This establishes the gap in the modeled normal restart/bind/Ready/add path. It does **not** prove that every operator-driven repair is impossible, that a remote shell is required, or that no other Go mechanism resolves it. Full progress/backend qualification and Go reproduction remain separate evidence.

## Source correspondence

The Go baseline is `e8e85dbd7879b1c858252cd2a6126a36920f8a8e`; the working branch's Go sources were not changed for this finding.

| Boundary | Source / modeled mechanism |
| --- | --- |
| Pin and start the granted metadata engine | `internal/metaservice/join_client.go:acceptJoinResponse` / `installJoinedEngine`; `admission_steps.pinJoin` / `startJoinedMeta` |
| Host preparation may precede Ready acknowledgement | `internal/metaservice/authority_host.go:VerifyPreparedMeta` checks engine readiness and exact store identity; `node_meta_authority.go:executeMetaAuthority` persists the receipt. `authorityIdentityReply` waits for a pin but does not require an admission Ready record. |
| Ready is a separate RPC | `internal/metaservice/join.go:acknowledgeJoin` and `handleReady`; `announcePrepared`, `acceptPrepared` and a separate metadata command |
| Pending, non-member restart selects instance replacement | `internal/metaservice/join_admission.go:classifyLiveAdmission`; the model keeps the exact prior-process capture |
| Consumed preparation prevents rebinding | `internal/metaengine/scoped_joining_admission.go:bindJoiningAdmission` / `admissionMemberPrepared`; `admission.errors(BindAdmission)` |
| Learner addition requires Prepared admission | `internal/metaservice/effects.go:changeMetaReceipt`; `managed_membership_steps.admissionReady` |
| New process cannot acknowledge the old record | `internal/metaengine/scoped_joining_admission.go:applyReadyAdmission` requires the exact process/member/operation |

The existing `TestMetaPinnedAdmissionRestartsBeforeAndAfterReadyWithoutAllocatingIDs` in `tests/cluster/join_test.go` explicitly checks `MetadataAcquired == false`. It covers the independently pinned, unacquired boundary; it does not cover a consumed host-preparation receipt before Ready. Protocol fixtures in `authority_protocol_helpers_test.go` also send Ready before consuming preparation.

An earlier draft of the Quint path omitted the Prepared-admission gate and incorrectly allowed the unacknowledged restart to continue. That model error is corrected. The desired-success check remains executable outside the positive profiles rather than being removed or labeled successful.

## Reproduction and evidence

Run with Quint 0.33.0 and preinstalled Rust evaluator v0.7.0:

```sh
quint test scripts/quint/instances/admissionRestartExpected.qnt --main admissionRestartExpected --backend rust --seed 1 --max-samples 1 --match retainedPreparationRestartShouldContinueTest
quint test scripts/quint/instances/admission4.qnt --main admission4 --backend rust --seed 1 --max-samples 1 --match preparedRestartBeforeReadyExposesBindingBlockTest
```

The first command is expected to fail; the second verifies the refusal state. Bounds: four potential nodes, three founders, two loads, three requests per process, position 128, incarnation 2 and delivery 512. All stores remain valid; policy/metric assumptions are unchanged.

Local-only logs and source hashes are under `test-reports/quint/task8-admission-gap/`. The retained source instance and named tests are committed with Task 8; local artifacts are gitignored. The failure is a disabled required continuation, not an exhaustive temporal deadlock proof. No Go test or Docker scenario was run for this finding.

## Go regression recipe and possible correction

Extend the four-node admission scenario with an authenticated fixture barrier that withholds Ready while permitting pin acquisition, local engine readiness, IdentityPing and MetaControl. Wait until metadata records the exact preparation receipt and advances the block to AddLearner, then kill the joining process. Restart the normal SDK on the retained volume with a new instance ID. Observe whether binding is refused and AddLearner stays waiting despite healthy connectivity/quorum; require eventual readiness with the original member/attempt to test the desired behavior. Keep multi-node coverage in `tests/cluster`, not a local functional multi-node fixture. A focused metadata-machine test can separately assert the three guard facts above.

Assess either a Ready-before-preparation dependency or a safe retained-instance reconciliation path. Do not simply remove the prepared-member guard: it protects exact ownership and must be considered alongside live-instance fencing, copied identities and admission handover.
