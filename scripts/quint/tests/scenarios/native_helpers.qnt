module native_helpers {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import lifetime_test.* from "./lifetime_test"
  import grants as WorkflowGrants from "../../metaengine/grants"
  export lifetime_test.*

  // Scenario orchestration only; production transitions stay independent.
  def metaReplica(node: NodeName): ReplicaKey = world.extensions.metadata.target.groups.get("_meta").replicas.filter(r => r.key.node == node).getOnlyElement().key

  action syncMeta(node: NodeName): bool = if (world.system.get(node).volatile.appliedMetaIndex <
    world.extensions.consensus.get("_meta").committedPosition) applyMeta(node) else { world' = world }

  action reportAndCheckpoint(approved: ApprovedAction): bool = {
    val grant = WorkflowGrants::actionGrants(world.extensions.metadata, approved).getOnlyElement()
    nondet receipt = world.system.values().map(n => n.durable.receipts).flatten().union(
      world.extensions.consensus.get(grant.key.group).authorityReceipts).filter(r => r.grant == grant.key).oneOf()
    (match receipt.resource {
      | HostResource(_) => reportHostEffect("n1", receipt)
      | GroupResource(group) => reportNative("n1", group, RecordEffect(receipt))
    }).then(controlCommit(RecordEffect(receipt))).then(applyMeta("n1"))
      .then(checkpointEffect("n1", approved))
      .then(controlCommit(CompleteStep({ approved: approved,
        observedLeader: world.extensions.consensus.get(grant.key.group).leaderAuthority })))
      .then(applyMeta("n1"))
  }

  action runNativeApproved(approved: ApprovedAction): bool = {
    val group = WorkflowGrants::groupOf(approved.step.target)
    commitAndApplyControl(IssueEffect(approved)).then({
      val grant = WorkflowGrants::actionGrants(world.extensions.metadata, approved).getOnlyElement()
      dispatchGrant("n1", grant.key, GroupResource(group))
    }).then(receiveQueuedGrant).then({
      nondet delivery = world.system.get("n1").volatile.waitingEffects.filter(d => d.grant.planStep == Some(approved.step)).oneOf()
      installNativeAuthority(delivery).then(if (approved.step.kind == PromoteLearner)
        catchUp(group, (match approved.step.target { | ReplicaTarget(r) => r | _ => metaReplica("n1") })) else { world' = world })
        .then(precheckNative(delivery)).then(proposeNative(delivery))
    }).then(commitNative(group)).then(reportAndCheckpoint(approved)).expect(safety)
  }

  action runMetaStep(approved: ApprovedAction): bool = match approved.step.kind {
    | PrepareReplica => commitAndApplyControl(IssueEffect(approved)).then({
        val grant = WorkflowGrants::actionGrants(world.extensions.metadata, approved).getOnlyElement()
        nondet resource = grant.resources.oneOf()
        dispatchGrant("n1", grant.key, resource)
      }).then(receiveQueuedGrant).then({
        val target = match approved.step.target { | ReplicaTarget(r) => r.node | _ => "" }
        nondet delivery = world.system.get(target).volatile.waitingEffects.filter(d => d.grant.planStep == Some(approved.step)).oneOf()
        if (delivery.grant.key.group == "_meta") prepareMeta(delivery) else
          syncMeta(target).then(beginLoadPreparation(delivery)).then({
            nondet work = world.system.get(target).volatile.starts.keys().filter(k =>
              world.system.get(target).volatile.starts.get(k).key == delivery.replica).oneOf()
            readStartScope(work).then(reserveOwner(work)).then(startupBody(target, delivery.grant.key.group))
              .then(replyStart(work)).then(finishLoadPreparation(delivery))
          })
      }).then(reportAndCheckpoint(approved))
    | WaitCatchUp => {
        val replica = match approved.step.target { | ReplicaTarget(r) => r | _ => metaReplica("n1") }
        (if (world.extensions.consensus.get(replica.group).catchUp.getOrElse(replica, Behind) == CaughtUp)
          { world' = world } else catchUp(replica.group, replica)).then(checkpointCatchUp("n1", approved))
          .then(controlCommit(CompleteCatchUp(approved))).then(applyMeta("n1"))
      }
    | AddLearner => runNativeApproved(approved)
    | PromoteLearner => runNativeApproved(approved)
    | DemoteVoter => runNativeApproved(approved)
    | RemoveMember => runNativeApproved(approved)
    | _ => all { false, world' = world }
  }

  action executeGroupBlock(version: int, group: GroupId): bool = {
    val key: AttemptKey = { version: version, attempt: 1 }
    val steps = world.extensions.metadata.operations.get(key).plan.blocks.get(group)
    (if (steps.length() == 0) { world' = world } else
      (if (world.extensions.metadata.operations.get(key).blocks.get(group).phase == OpPending)
        commitAndApplyControl(BeginBlock({ operation: key, group: group })) else { world' = world })
        .then(8.reps(i => if (i < steps.length()) runMetaStep({ operation: key, step: steps.nth(i) }) else { world' = world })))
      .expect(steps.length() <= 8)
      .expect(world.extensions.metadata.operations.get(key).blocks.get(group).phase == OpSucceeded)
  }

  action executeMetaBlock(version: int): bool = executeGroupBlock(version, "_meta")

}
