module grants_test {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import grants as G from "../../metaengine/grants"
  import controls as Ctrl from "../../metaengine/controls"
  import authority as A from "../../metaengine/authority"
  import topology_fixtures as F from "../fixtures/topology"
  import authority_fixtures as AF from "../fixtures/authority"
  import grants_fixtures.* from "../fixtures/grants"
  run grantRequiresExactRunningStepTest = and {
    G::wellFormed(grant), G::canIssueStep(running, approved),
    not(G::canIssueStep(AF::change, approved)),
    not(G::canIssueStep(running, { ...approved, operation: { version: 2, attempt: 2 } })),
    not(G::canIssueStep(running, { ...approved, step: { ...planned, seq: planned.seq + 1 } })),
    not(G::canIssueStep(running, { ...approved, step: { ...planned, resultVoters: Set() } })),
  }

  run grantReplayRetainsRevisionTest = and {
    G::issueStep(issued, approved, 7) == issued,
    not(G::currentForIssuance(closed, grant)), G::catalogued(closed, grant),
    not(G::catalogued(issued, { ...grant, targets: Set(replica) })),
  }

  run exactReceiptAndReplayTest = {
    val after = G::recordReceipt(issued, receipt)
    and { G::canRecordReceipt(issued, receipt), G::canRecordReceipt(after, receipt),
      G::recordReceipt(after, receipt) == after,
      not(G::canRecordReceipt(after, { ...receipt, durablePosition: 3 })),
      not(G::canRecordReceipt(issued, { ...receipt, target: Some({ ...replica, replica: replica.replica + 1 }) })),
      not(G::canRecordReceipt(issued, { ...receipt, planStep: Some({ ...planned, seq: planned.seq + 1 }) })),
      not(G::canRecordReceipt(issued, { ...receipt, outcome: TransferStarted })),
    }
  }

  run revocationNeedsPhysicalFenceTest = {
    val pending = closed.revocations.get(grant.key)
    val settled = G::settleRevocations(G::recordFence(closed, fence))
    and { pending.requested, pending.requiredResources == Set(resource), pending.provenResources == Set(),
      not(G::resourcesAvailable(closed, Set(resource))),
      G::resourcesAvailable(closed, Set(GroupResource("b"))),
      G::canRecordFence(closed, fence),
      G::resourcesAvailable(settled, Set(resource)),
      settled.revocations.get(grant.key).evidence == Set(fence),
    }
  }

  run fenceAllowsEarlierHistoricalReceiptTest = {
    val after = G::recordFence(closed, fence)
    and { G::canRecordReceipt(after, receipt),
      not(G::canRecordReceipt(after, { ...receipt, durablePosition: 4 })),
      not(G::canRecordFence(G::recordReceipt(closed, { ...receipt, durablePosition: 4 }), fence)),
      G::canRecordFence(after, { ...fence, receiver: { ...fence.receiver, node: "n1", member: 1 },
        replica: Some({ ...replica, node: "n1", member: 1, replica: 1 }) }),
      not(G::canRecordFence(after, { ...fence, durablePosition: 4 })),
    }
  }

  run consumedOneShotNeedsNoNewFenceTest = {
    val consumed = G::recordReceipt(issued, receipt)
    val after = G::closeGrants(consumed, Set(grant.key), 5)
    and { after.grants == consumed.grants, G::resourcesAvailable(after, Set(resource)),
      after.closedGrants.contains(grant.key) }
  }

  run manualTransferHasDistinctAuthorityTest = and {
    G::wellFormed(manual), manual.planStep == None,
    not(G::wellFormed({ ...manual, purpose: Effect })),
    not(G::wellFormed({ ...grant, purpose: Transfer })),
    Ctrl::manualErrors(F::meta, replica, request) == Set(),
    Ctrl::manualErrors(running, replica, request) == Set(Unauthorized),
  }

  run transferStartDoesNotSettleIntentTest = {
    val meta = G::install(F::meta, manual)
    val started = { ...receipt, grant: manual.key, effect: manual.effect,
      planStep: None, outcome: TransferStarted }
    val after = G::recordReceipt(meta, started)
    val revoked = G::closeGrants(after, Set(manual.key), 5)
    and { G::canRecordReceipt(meta, started), G::resourceConsumed(after, manual, resource),
      not(G::resourceSettled(after, manual, resource)),
      revoked.revocations.get(manual.key).requiredResources == Set(resource),
      not(G::resourcesAvailable(revoked, Set(resource))) }
  }

  run manualTransferReplayKeepsOriginalRequestTest = {
    val meta = Ctrl::beginManual(F::meta, replica, request, 4)
    val other = { ...request, sequence: 2 }
    and { Ctrl::beginManual(meta, replica, request, 5) == meta,
      Ctrl::beginManual(meta, replica, other, 5) == meta,
      Ctrl::manualErrors(meta, { ...replica, replica: 9 }, request) == Set(Conflict) }
  }

  run partialHostReceiptsDoNotSettleBatchTest = {
    val successor = { ...replica, member: 4, replica: 4 }
    val resource = HostResource({ group: "a", node: "n3" })
    val readmit: Grant = { ...grant, purpose: Readmission, effect: None, planStep: None,
      admission: Some({ node: "n3", predecessor: 3, successor: 4 }),
      resources: Set(resource), targets: Set(replica, successor) }
    val first = { ...receipt, effect: None, planStep: None, admission: readmit.admission,
      resource: resource, durableTerm: 0 }
    val meta = G::recordReceipt(G::install(F::meta, readmit), first)
    and { G::wellFormed(readmit), G::canRecordReceipt(meta, first),
      not(G::resourceConsumed(meta, readmit, resource)),
      G::resourceConsumed(G::recordReceipt(meta, { ...first, target: Some(successor), durablePosition: 3 }), readmit, resource) }
  }

  run nativeGrantExcludesUnapprovedHistoryTest = {
    val foreign = { ...replica, replica: 99 }
    val unapproved = { ...running.target, version: 3,
      groups: running.target.groups.setBy("a", g => { ...g,
        nextReplica: 100, replicas: g.replicas.filter(r => r.key.node != foreign.node).union(
          Set({ key: foreign, role: Learner, pinnedLearner: false })) }) }
    val later = { ...running, target: { ...running.target, version: 4 },
      history: running.history.put(3, unapproved) }
    not(G::stepGrant(later, approved, 7).targets.contains(foreign))
  }

  run transferCheckpointRequiresReceiptAndLeaderTest = {
    val staged = AF::stageEdit(F::meta, ChangeReplica({ group: "a", node: "n1", role: Learner }))
    val approvedMeta = A::applyApproval(staged, AF::approvalFor(staged))
    val transferStep = approvedMeta.operations.get(key).plan.blocks.get("a")
      .select(s => s.kind == TransferLeadership).head()
    val actionKey = { operation: key, step: transferStep }
    val started = { ...approvedMeta, operations: approvedMeta.operations.setBy(key, op => { ...op,
      blocks: op.blocks.setBy("a", b => { ...b, phase: OpRunning, step: transferStep.seq }) }) }
    val issuedMeta = G::issueStep(started, actionKey, 4)
    val g = G::actionGrants(issuedMeta, actionKey).getOnlyElement()
    val target = match transferStep.target { | ReplicaTarget(r) => Some(r) | _ => None }
    val r: Receipt = { grant: g.key, effect: g.effect, admission: None, target: target,
      resource: GroupResource("a"), outcome: TransferStarted, planStep: g.planStep,
      durablePosition: 2, durableTerm: 1 }
    val reported = G::recordReceipt(issuedMeta, r)
    and {
      Ctrl::errors(issuedMeta, CompleteStep({ approved: actionKey, observedLeader: target })) == Set(Unauthorized),
      Ctrl::errors(reported, CompleteStep({ approved: actionKey, observedLeader: None })) == Set(Unauthorized),
      Ctrl::errors(reported, CompleteStep({ approved: actionKey, observedLeader: target })) == Set(),
      Ctrl::complete(reported, actionKey).operations.get(key).blocks.get("a").step > transferStep.seq,
      Ctrl::complete(reported, actionKey).operations.get(key).plan == started.operations.get(key).plan,
    }
  }

  run manualTransferPreservesCapturedSourceTest = and {
    Ctrl::errors(F::meta, IssueTransfer({ target: replica, request: request, sourceVersion: 1 })) == Set(),
    Ctrl::errors(F::meta, IssueTransfer({ target: replica, request: request, sourceVersion: 2 })) == Set(Unauthorized),
    Ctrl::errors(G::install(F::meta, manual), IssueTransfer({ target: replica, request: request, sourceVersion: 2 })) == Set(Unauthorized),
  }

  run retryRevokesOnlyFailedGroupEffectsTest = {
    val staged = AF::stageEdit(AF::stageEdit(F::meta, ChangeReplica({ group: "a", node: "n3", role: Learner })),
      ChangeReplica({ group: "b", node: "n3", role: Learner }))
    val meta = A::applyApproval(staged, AF::approvalFor(staged))
    val key: AttemptKey = { version: 3, attempt: 1 }
    val running = { ...meta, operations: meta.operations.setBy(key, op => { ...op,
      blocks: op.blocks.transformValues(b => { ...b, phase: OpRunning }) }) }
    val a = { operation: key, step: running.operations.get(key).plan.blocks.get("a").head() }
    val b = { operation: key, step: running.operations.get(key).plan.blocks.get("b").head() }
    val issued = G::issueStep(G::issueStep(running, a, 4), b, 5)
    val bGrant = G::actionGrants(issued, b).getOnlyElement()
    val bReceipt: Receipt = { grant: bGrant.key, effect: bGrant.effect, admission: None,
      target: match b.step.target { | ReplicaTarget(r) => Some(r) | _ => None },
      resource: GroupResource("b"), outcome: EffectApplied, planStep: bGrant.planStep,
      durablePosition: 2, durableTerm: 1 }
    val consumed = G::recordReceipt(issued, bReceipt)
    val failed = { ...consumed, operations: consumed.operations.setBy(key, op => { ...op,
        blocks: op.blocks.put("a", { ...op.blocks.get("a"), phase: OpFailed })
          .put("b", { ...op.blocks.get("b"), phase: OpSucceeded }) }),
      groups: consumed.groups.put("a", { ...consumed.groups.get("a"), owner: None })
        .put("b", { ...consumed.groups.get("b"), owner: None, sourceVersion: 3,
          proven: Some(consumed.target.groups.get("b")) }) }
    val retry = { previous: key, scopes: Map("a" -> A::scopeAt(failed, "a")) }
    val result = Ctrl::reconcile(failed, A::applyRetry(failed, retry), 6)
    and { A::canRetry(failed, retry),
      result.closedGrants == Set({ cluster: "c1", group: "a", revision: 4 }),
      result.groups.get("b") == failed.groups.get("b"),
      result.operations.get({ version: 3, attempt: 2 }).blocks.get("b").phase == OpSucceeded,
      not(G::resourcesAvailable(result, Set(GroupResource("a")))),
      G::resourcesAvailable(result, Set(GroupResource("b"))),
    }
  }
}
