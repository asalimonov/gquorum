module authority_test {
  import types.* from "./types"
  import basicSpells.* from "./vendor/spells/basicSpells"
  import topology as T from "./topology"
  import authority as A from "./authority"
  import topology_test as F from "./topology_test"
  import planning_test as PF from "./planning_test"

  pure val request = F::request(DeleteGroup("a"))
  pure val command: StageCommand = { request: request, desired: T::mutateDocument(F::input, request) }
  pure val staged = A::applyStage(F::meta, command)
  pure def approvalFor(meta: MetadataState): ApprovalCommand = {
    val input = { ...A::planningInput(meta, F::input.rules), observations: PF::input.observations }
    val plan = match T::makePlan(input, meta.latest) {
      | Planned(p) => p
      | PlanRejected(_) => { version: meta.latest.version, blocks: Map(), cleanup: Map(), sources: Map(), dependencies: Set() }
    }
    { desired: meta.latest, plan: plan,
      scopes: T::affectedScopes(meta.target, meta.latest).mapBy(g => A::scopeAt(meta, g)),
      captured: input, origin: Operator, audit: meta.latest.audit }
  }
  pure def stageEdit(meta: MetadataState, edit: DocumentEdit): MetadataState = {
    val request = { edit: edit, options: { ...F::options, expectedVersion: meta.latest.version } }
    val input = A::mutationInput(meta, Map(), F::input.rules)
    A::applyStage(meta, { request: request, desired: T::mutateDocument(input, request) })
  }
  pure val stagedChange = stageEdit(F::meta, ChangeReplica({ group: "a", node: "n3", role: Learner }))
  pure val change = A::applyApproval(stagedChange, approvalFor(stagedChange))
  pure val attempt = { version: 2, attempt: 1 }
  // A failed-before-effect fixture. Runtime execution failure/reporting belongs to Task 6.
  pure val failed = { ...change,
    operations: change.operations.setBy(attempt, op => { ...op,
      blocks: op.blocks.setBy("a", b => { ...b, phase: OpFailed }) }),
    groups: change.groups.setBy("a", g => { ...g, owner: None }),
  }
  pure val retry: RetryCommand = { previous: attempt, scopes: Map("a" -> A::scopeAt(failed, "a")) }

  run retryRequiresFailedPredecessorTest = and {
    not(A::canRetry(change, retry)), A::canRetry(failed, retry),
    A::retryErrors(failed, { ...retry, previous: { version: 2, attempt: 9 } }) == Set(NotFound),
  }
  run retryKeepsPlanAndOneImmediateSuccessorTest = {
    val result = A::applyRetry(failed, retry)
    val successor = A::retryKey(result, retry)
    val op = result.operations.get(successor)
    and { successor == { version: 2, attempt: 2 }, op.predecessor == Some(attempt),
      op.plan == failed.operations.get(attempt).plan, op.audit == failed.operations.get(attempt).audit,
      A::canRetry(result, retry), A::applyRetry(result, retry) == result,
      result.firstAttempts.get(2) == attempt, result.groups.get("a").owner == Some(successor),
      result.operations.get(attempt) == failed.operations.get(attempt) }
  }
  run retryRejectsNewConflictingOwnerTest = {
    val staged = stageEdit(failed, DeleteGroup("a"))
    val approved = A::applyApproval(staged, approvalFor(staged))
    A::retryErrors(approved, retry) == Set(Conflict)
  }

  run deletionApprovalRetainsExactDebtTest = {
    val cmd = approvalFor(staged)
    val approved = A::applyApproval(staged, cmd)
    and { A::canApprove(staged, cmd, F::input.rules),
      approved.target == staged.latest,
      approved.operations.get({ version: 2, attempt: 1 }).blocks.get("a").phase == OpSucceeded,
      approved.groups.get("a").owner == None, approved.groups.get("a").proven == None,
      approved.retiredGroupIds == Set("a"), approved.cleanup.keys().size() == 3,
      approved.cleanup.values().forall(debt => and {
        debt.stopAction != None, debt.retireAction != None, debt.stopGrant == None,
        debt.stopped == None, debt.retired == None, debt.collected == None,
      }) }
  }
  run approvalCannotOmitOfflineOwnerTest = {
    val cmd = approvalFor(staged)
    val omitted = { ...cmd, plan: { ...cmd.plan, cleanup: cmd.plan.cleanup.setBy("a", steps =>
      steps.select(s => not(s.kind == StopReplica and s.targets.exists(r => r.node == "n3")))) } }
    A::approvalErrors(staged, omitted, F::input.rules) == Set(Unauthorized)
  }
  run approvalReplayReturnsFirstAttemptTest = {
    val cmd = approvalFor(staged)
    val approved = A::applyApproval(staged, cmd)
    and { A::canApprove(approved, cmd, F::input.rules),
      A::applyApproval(approved, cmd) == approved,
      approved.firstAttempts.get(2) == { version: 2, attempt: 1 } }
  }
  run approvalReplayRejectsChangedPlanTest = {
    val cmd = approvalFor(staged)
    val approved = A::applyApproval(staged, cmd)
    val altered = { ...cmd, plan: { ...cmd.plan, cleanup: Map() } }
    A::approvalErrors(approved, altered, F::input.rules) == Set(Conflict)
  }
  run approvalRejectsChangedScopeCaptureTest = {
    val cmd = approvalFor(staged)
    val changed = { ...staged, groups: staged.groups.setBy("a", g => { ...g, sourceVersion: 2 }) }
    A::approvalErrors(changed, cmd, F::input.rules) == Set(Conflict)
  }
  run disjointApprovalPreservesIndependentOwnerTest = {
    val a = stageEdit(F::meta, ChangeReplica({ group: "a", node: "n3", role: Learner }))
    val ac = approvalFor(a)
    val approvedA = A::applyApproval(a, ac)
    val b = stageEdit(approvedA, ChangeReplica({ group: "b", node: "n3", role: Learner }))
    val bc = approvalFor(b)
    val approvedB = A::applyApproval(b, bc)
    val keyA = { version: 2, attempt: 1 }
    val keyB = { version: 3, attempt: 1 }
    and { A::canApprove(a, ac, F::input.rules), A::canApprove(b, bc, F::input.rules),
      approvedB.groups.get("a").owner == Some(keyA), approvedB.groups.get("b").owner == Some(keyB),
      approvedB.operations.get(keyA) == approvedA.operations.get(keyA) }
  }
  run approvalSupersedesOnlyAffectedBlockTest = {
    val a = stageEdit(F::meta, ChangeReplica({ group: "a", node: "n3", role: Learner }))
    val approvedA = A::applyApproval(a, approvalFor(a))
    val deleted = stageEdit(approvedA, DeleteGroup("a"))
    val cmd = approvalFor(deleted)
    val after = A::applyApproval(deleted, cmd)
    and { A::canApprove(deleted, cmd, F::input.rules),
      after.operations.get({ version: 2, attempt: 1 }).blocks.get("a").phase == Superseded,
      after.groups.get("b") == F::meta.groups.get("b"),
      after.operations.get({ version: 3, attempt: 1 }).blocks.get("a").phase == OpSucceeded }
  }

  run convenienceCasCapturedOnceTest = {
    val supplied = { ...request, options: { ...request.options, expectedVersion: 0,
      audit: { actor: "", comment: "original", forced: None } } }
    val captured = A::capture(supplied, 1, "n2")
    and { captured.options.expectedVersion == 1, captured.options.audit.actor == "n2",
      captured.options.audit.comment == "original", A::capture(captured, 2, "n3") == captured }
  }
  run stageLiteralZeroCasTest = {
    val supplied = { edit: StageDocument(command.desired), options: { ...F::options, expectedVersion: 0 } }
    A::capture(supplied, 1, "n2").options.expectedVersion == 0
  }
  run stageDoesNotAdvanceTargetTest = and {
    A::canStage(F::meta, command, F::input.rules),
    staged.latest.version == 2, staged.target == F::meta.target,
    staged.operations == F::meta.operations, staged.groups == F::meta.groups,
    not(staged.latest.groups.keys().contains("a")), staged.target.groups.keys().contains("a"),
  }
  run exactMutationReplayBeforeRevalidationTest = and {
    // DeleteGroup(a) would now fail validation because a is already absent.
    T::mutationErrors(A::mutationInput(staged, Map(), F::input.rules), request) == Set(Conflict),
    A::canStage(staged, command, F::input.rules),
    A::applyStage(staged, command) == staged,
    A::acceptedStage(staged, command).staged == Some(2),
  }
  run changedAuditCannotReplayTest = {
    val altered = { ...command, request: { ...request, options: { ...request.options,
      audit: { ...request.options.audit, comment: "different" } } } }
    A::stageErrors(staged, altered, F::input.rules) == Set(Conflict)
  }
  run changedCasCannotReplayTest = {
    val altered = { ...command, request: { ...request, options: { ...request.options, expectedVersion: 0 } } }
    A::stageErrors(staged, altered, F::input.rules) == Set(Conflict)
  }
  run replayIgnoresApprovalWaitOptionTest = {
    val altered = { ...command, request: { ...request, options: { ...request.options, approve: true } } }
    A::canStage(staged, altered, F::input.rules) and A::applyStage(staged, altered) == staged
  }
  run rawStageReplayKeepsExactIdentityTest = {
    val raw = { request: F::request(StageDocument(command.desired)), desired: command.desired }
    val after = A::applyStage(F::meta, raw)
    val altered = { ...raw, request: F::request(StageDocument({ ...raw.desired,
      audit: { ...raw.desired.audit, comment: "changed" } })) }
    and { A::canStage(after, raw, F::input.rules),
      A::stageErrors(after, altered, F::input.rules) == Set(Conflict) }
  }
  run stageCannotLowerRetainedAllocationFloorTest = {
    val req = F::request(AddNode({ name: "n4", member: 0, state: Active, nodeType: "worker", labels: Map() }))
    val add = { request: req, desired: T::mutateDocument(F::input, req) }
    val after = A::applyStage(F::meta, add)
    val removed = { ...F::base, version: 3, parent: 2,
      groups: F::base.groups.setBy("_meta", g => { ...g, nextReplica: 5 }) }
    val raw = { request: { edit: StageDocument(removed), options: { ...F::options, expectedVersion: 2 } }, desired: removed }
    and { after.memberAllocationFloor == 5,
      A::canStage(after, raw, F::input.rules),
      A::applyStage(after, raw).memberAllocationFloor == 5,
      A::applyStage(after, raw).replicaAllocationFloors.get("_meta") == 5 }
  }
}
