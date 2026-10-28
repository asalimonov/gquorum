module scheduler_test {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import groups_test.* from "./groups_test"
  import metaservice_scheduler_steps as ScheduleRules from "../../metaservice/scheduler_steps"
  import managed_node_steps as ScheduleNode from "../../managed/node_steps"
  export groups_test.*

  val scheduled = world.system.get("n1").volatile.schedulerCapture.unwrap()
  pure val automaticKey: AttemptKey = { version: 2, attempt: 1 }
  pure val standby: ReplicaKey = { cluster: "c1", group: "a", node: "n3", member: 3, replica: 3 }

  action applySchedulerMeta = if (world.system.get("n1").volatile.appliedMetaIndex <
    world.extensions.consensus.get("_meta").committedPosition) applyMeta("n1") else { world' = world }
  action sampleSchedulerInput = 4.reps(_ => {
    val work = world.system.get("n1").volatile.schedulerCapture.unwrap()
    val pending = work.input.unwrap().target.groups.keys().exclude(work.sampled)
    if (pending.size() == 0) { world' = world } else {
      nondet group = pending.oneOf()
      sampleSchedulerGroup("n1", group)
    }
  })
  action schedulerCallback = initScoped.then(startScheduler("n1")).then(captureScheduler("n1"))
    .then(sampleSchedulerInput).then(callScheduler("n1"))
  action schedulerChosen = schedulerCallback.then(returnScheduler("n1"))
    .expect(scheduled.decision == Some({ kind: ReconfigureVoters, group: "a", missing: None, promote: Some(standby), demote: None }))
  action prepareSchedulerPlan = recheckScheduler("n1", false).then(applySchedulerMeta)
    .then(refreshScheduler("n1")).then(observeSchedulerRepair("n1")).then(planSchedulerRepair("n1"))
  action schedulerPlanned = schedulerChosen.then(prepareSchedulerPlan).expect(scheduled.phase == SchedulePlanned)
  action finalSchedulerRead = recheckScheduler("n1", true).then(applySchedulerMeta).then(validateSchedulerRepair("n1"))
  action schedulerValidated = schedulerPlanned.then(finalSchedulerRead).expect(scheduled.phase == ScheduleValidated)
  action commitSchedulerProposal = {
    nondet msg = world.extensions.network.pending.filter(m => match m.payload {
      | MetaProposal(p) => match p.command { | SchedulerProposal(_) => true | _ => false } | _ => false }).oneOf()
    deliver(msg).then(commitMutation(msg))
  }
  action receiveSchedulerAck = {
    val key = world.system.get("n1").volatile.schedulerCapture.unwrap().proposal.unwrap()
    nondet msg = world.extensions.network.pending.filter(m => match m.payload {
      | MetaAck(a) => a.key == key | _ => false }).oneOf()
    deliver(msg).then(receiveSchedulerResult(msg))
  }
  action schedulerCommitted = schedulerValidated.then(submitSchedulerRepair("n1"))
    .then(commitSchedulerProposal).then(receiveSchedulerAck).then(applySchedulerMeta).expect(safety)

  action stageSchedulerOtherGroup = beginMutation(mutationKey, Mutate({ edit: ChangeReplica({ group: "b", node: "n2", role: Learner }), options: options }))
    .then(commitProposal(mutationKey)).then(receiveMutationAck(mutationKey)).then(applyMeta("n2")).then(finishMutation(mutationKey))

  run schedulerCaptureNeedsConfirmedLocalApplyTest = initScoped.then(stageSchedulerOtherGroup).then(startScheduler("n1"))
    .expect(not(ScheduleRules::captureEnabled(world, "n1"))).then(applySchedulerMeta).then(captureScheduler("n1"))
    .expect(scheduled.input.unwrap().target.version == 1).expect(safety)
  run schedulerCallbackDoesNotApproveAnythingTest = schedulerChosen
    .expect(world.extensions.metadata.operations == Map()).expect(safety)
  run schedulerPromotesThroughApprovedNativeStepsTest = schedulerCommitted.then(executeGroupBlock(2, "a"))
    .expect(world.extensions.metadata.groups.get("a").owner == None)
    .expect(world.extensions.consensus.get("a").voters.size() == 3)
    .expect(world.extensions.metadata.operations.get(automaticKey).origin == Reconciler).expect(safety)
  run schedulerDisabledCallbackRemainsOwnedTest = schedulerCallback.then(setSchedulingEnabled("n1", false))
    .expect(not(ScheduleRules::releaseEnabled(world, "n1")))
    .then(returnScheduler("n1")).expect(not(ScheduleRules::readEnabled(world, "n1", false)))
    .then(releaseScheduler("n1")).expect(world.extensions.metadata.operations == Map())
    .then(setSchedulingEnabled("n1", true)).then(startScheduler("n1")).expect(safety)
  run schedulerNodeStopJoinsCallbackTest = schedulerCallback.then(requestNodeStop("n1"))
    .expect(not(ScheduleNode::joinAuthorityWorkersEnabled(world, "n1")))
    .then(returnScheduler("n1")).then(joinAuthorityWorkers("n1"))
    .expect(world.system.get("n1").volatile.schedulerCapture == None).expect(safety)
  run schedulerLeaderLossAndRegainInvalidatesCaptureTest = schedulerCallback
    .then(elect("_meta", metaReplica("n2"))).then(elect("_meta", metaReplica("n1")))
    .then(returnScheduler("n1")).expect(not(ScheduleRules::readEnabled(world, "n1", false)))
    .then(releaseScheduler("n1")).expect(world.extensions.metadata.operations == Map()).expect(safety)
  run schedulerFreshReadRejectsStoppedStandbyTest = schedulerChosen.then(stopOwner("n3", standby, true))
    .then(recheckScheduler("n1", false)).then(refreshScheduler("n1")).then(observeSchedulerRepair("n1"))
    .expect(scheduled.phase == ScheduleDone).expect(scheduled.result.unwrap().errors == Set(Unauthorized)).expect(safety)
  run schedulerNativeLeaderChangeDuringPlanIsRejectedTest = schedulerPlanned
    .then(elect("a", { cluster: "c1", group: "a", node: "n2", member: 2, replica: 2 })).then(finalSchedulerRead)
    .expect(scheduled.phase == ScheduleDone).expect(scheduled.result.unwrap().errors == Set(Conflict)).expect(safety)
  run schedulerLogProgressDoesNotInvalidateMembershipTest = schedulerPlanned.then(commitProgress("a")).then(finalSchedulerRead)
    .expect(scheduled.phase == ScheduleValidated).then(submitSchedulerRepair("n1"))
    .then(commitSchedulerProposal).expect(world.extensions.metadata.target.version == 2).expect(safety)
  run schedulerPreservesIndependentStagedIntentTest = schedulerChosen.then(stageSchedulerOtherGroup)
    .then(prepareSchedulerPlan).then(finalSchedulerRead).then(submitSchedulerRepair("n1"))
    .then(commitSchedulerProposal).then(receiveSchedulerAck)
    .expect(world.extensions.metadata.target.version == 3)
    .expect(world.extensions.metadata.latest.version == 4)
    .expect(world.extensions.metadata.latest.rebasedFrom == Some(2))
    .expect(world.extensions.metadata.target.groups.get("b").replicas.forall(r => r.role == Voter))
    .expect(world.extensions.metadata.latest.groups.get("b").replicas.exists(r => r.key.node == "n2" and r.role == Learner)).expect(safety)
  run schedulerDoesNotStealIndependentUserOperationTest = schedulerChosen
    .then(approveGroupEdit(ChangeReplica({ group: "b", node: "n2", role: Learner }), 1, 2))
    .then(prepareSchedulerPlan).then(finalSchedulerRead).then(submitSchedulerRepair("n1"))
    .then(commitSchedulerProposal)
    .expect(world.extensions.metadata.groups.get("a").owner == Some({ version: 3, attempt: 1 }))
    .expect(world.extensions.metadata.groups.get("b").owner == Some({ version: 2, attempt: 1 })).expect(safety)
  run schedulerCASProtectsIntentStagedAfterSubmissionTest = schedulerValidated.then(submitSchedulerRepair("n1"))
    .then(stageSchedulerOtherGroup).then(commitSchedulerProposal).then(receiveSchedulerAck)
    .expect(scheduled.result.unwrap().errors == Set(Conflict))
    .expect(world.extensions.metadata.target.version == 1).expect(world.extensions.metadata.latest.version == 2).expect(safety)
  run schedulerDisableDoesNotUndoSubmittedCommitTest = schedulerValidated.then(submitSchedulerRepair("n1"))
    .then(setSchedulingEnabled("n1", false)).then(commitSchedulerProposal)
    .expect(world.extensions.metadata.target.version == 2).expect(safety)
  run schedulerOmittedFinalRevalidationHasCounterexampleTest = schedulerPlanned
    .then(stopOwner("n3", standby, true)).then({ world' = ScheduleRules::submitNext(world, "n1") })
    .expect(not(ScheduleRules::submissionsValid(world)))
  action schedulerStageProbe = world' = world
  run schedulerSharesWorldTest = clusterStageInit.then(groupsStageProbe).then(schedulerStageProbe)
}
