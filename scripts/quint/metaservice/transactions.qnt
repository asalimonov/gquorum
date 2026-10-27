module transactions {
  import types.* from "../types"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import authority as A from "../metaengine/authority"
  import topology as T from "../topology/topology"
  import membership as M from "../consensus/membership"
  import controls as Ctrl from "../metaengine/controls"
  import repair as Repair from "../metaengine/repair"

  pure def newCall(request: ApiRequest, localVersion: int, caller: NodeName): CallState = {
    request: match request {
      | Mutate(m) => match m.edit {
          | ReseatReplica(_) => if (m.options.preview) Mutate(A::capture(m, localVersion, caller))
              else Mutate(m) // A real reseat captures zero CAS after its confirmed read.
          | _ => Mutate(A::capture(m, localVersion, caller))
        }
      | _ => request
    },
    phase: Captured, result: None, waiterCancelled: false, readIndex: None,
    command: None, proposalSequence: 0, commitIndex: None, commitOutcome: None, stagedVersion: None,
    operationRequest: None,
  }

  pure def canPrepare(owner: LocalNode, key: RequestKey): bool =
    if (not(owner.volatile.pendingCalls.keys().contains(key))) false else {
      val call = owner.volatile.pendingCalls.get(key)
      and {
        M::running(owner), key.caller.incarnation == owner.volatile.incarnation,
        call.phase == PreparingCall, not(call.waiterCancelled),
        match call.request {
          // Reseat uses the separate atomic repair workflow, never Stage.
          | Mutate(m) => match m.edit { | ReseatReplica(_) => m.options.preview | _ => true }
          | _ => false
        },
      }
    }

  pure def replied(call: CallState, outcome: MutationOutcome): CallState =
    { ...call, phase: Replied, stagedVersion: outcome.staged, result: Some(MutationResult(outcome)) }

  pure def prepare(call: CallState, meta: MetadataState, health: NodeName -> HealthClass, rules: Rules): CallState =
    match call.request {
      | Mutate(request) => {
        val input = A::mutationInput(meta, health, rules)
        val rawStage = match request.edit { | StageDocument(_) => true | _ => false }
        if (request.options.preview and request.options.approve) replied(call, A::refused(Set(Invalid)))
        else if (request.options.preview) { ...call, phase: Replied, result: Some(Previewed(T::preview(input, request))) }
        else if (request.options.expectedVersion < meta.latest.version or
          (not(rawStage) and request.options.expectedVersion != meta.latest.version)) {
          // Replay precedes convenience-edit validation, matching mutationBase.
          match A::replayVersion(meta, request) {
            | Some(version) => replied(call, A::stageOutcome(meta, version))
            | None => replied(call, A::refused(Set(Conflict)))
          }
        } else if (rawStage) {
          val desired = match request.edit { | StageDocument(doc) => doc | _ => meta.latest }
          { ...call, phase: PreparedCall, command: Some(StageProposal({ request: request, desired: desired })) }
        } else {
          val errors = T::mutationErrors(input, request)
          if (errors.size() > 0) replied(call, A::refused(errors)) else
            { ...call, phase: PreparedCall, command: Some(StageProposal({ request: request,
              desired: T::mutateDocument(input, request) })) }
        }
      }
      | _ => call
    }

  pure def commandErrors(meta: MetadataState, command: MetaCommand, rules: Rules): Set[Refusal] =
    match command {
      | StageProposal(stage) => A::stageErrors(meta, stage, rules)
      | ApprovalProposal(approval) => A::approvalErrors(meta, approval, rules)
      | RetryProposal(retry) => A::retryErrors(meta, retry)
      | RepairProposal(repair) => Repair::errors(meta, repair, rules)
      | ControlProposal(control) => Ctrl::errors(meta, control)
    }
  pure def applyCommand(meta: MetadataState, command: MetaCommand): MetadataState =
    match command {
      | StageProposal(stage) => A::applyStage(meta, stage)
      | ApprovalProposal(approval) => A::applyApproval(meta, approval)
      | RetryProposal(retry) => A::applyRetry(meta, retry)
      | RepairProposal(repair) => Repair::applyRepair(meta, repair)
      | ControlProposal(_) => meta // Applied with the metadata commit index below.
    }
  pure def applyAt(meta: MetadataState, command: MetaCommand, revision: int): MetadataState =
    match command {
      | ControlProposal(control) => Ctrl::apply(meta, control, revision)
      | _ => Ctrl::reconcile(meta, applyCommand(meta, command), revision)
    }

  pure def commandOutcome(meta: MetadataState, command: MetaCommand): MutationOutcome =
    match command {
      | ControlProposal(_) => A::refused(Set())
      | StageProposal(stage) => A::acceptedStage(meta, stage)
      | ApprovalProposal(approval) => A::stageOutcome(A::applyApproval(meta, approval), approval.desired.version)
      | RetryProposal(retry) => { staged: Some(retry.previous.version), operation: Some(A::retryKey(meta, retry)),
          rebasedTo: None, errors: Set() }
      | RepairProposal(repair) => A::stageOutcome(Repair::applyRepair(meta, repair),
          match A::replayVersion(meta, repair.request) { | Some(v) => v | None => repair.approval.desired.version })
    }

  pure def canFinish(owner: LocalNode, key: RequestKey): bool =
    if (not(owner.volatile.pendingCalls.keys().contains(key))) false else {
      val call = owner.volatile.pendingCalls.get(key)
      and {
        M::running(owner), key.caller.incarnation == owner.volatile.incarnation,
        call.phase == Committed, not(call.waiterCancelled), call.commitOutcome != None,
        match call.commitIndex { | Some(index) => owner.volatile.appliedMetaIndex >= index | None => false },
      }
    }

  pure def finish(call: CallState): CallState = match call.commitOutcome {
    | Some(outcome) => {
      val approve = match call.request { | Mutate(m) => m.options.approve | _ => false }
      if (approve and outcome.errors.size() == 0 and outcome.operation == None)
        { ...call, phase: AwaitingApproval, stagedVersion: outcome.staged }
      else replied(call, outcome)
    }
    | None => call
  }

  pure def operationToSend(call: CallState): Option[OperationRequest] =
    if (call.phase == WaitingOperation) call.operationRequest
    else if (call.phase == AwaitingApproval) match call.stagedVersion {
      | Some(version) => Some(ApproveVersion(version)) | None => None
    } else if (call.phase == PreparingCall) match call.request {
      | Operate(request) => Some(request)
      | Mutate(m) => match m.edit {
          | ReseatReplica(_) => if (call.readIndex != None and not(m.options.preview)) Some(RepairRequest(m)) else None
          | _ => None
        }
      | _ => None
    } else None

  pure def awaitingOperation(call: CallState, request: OperationRequest): CallState = {
    ...call, phase: WaitingOperation, operationRequest: Some(request),
    proposalSequence: if (call.phase == AwaitingApproval) call.proposalSequence + 1 else call.proposalSequence,
  }

  pure def cancelled(call: CallState): CallState = {
    val inFlight = call.phase == WaitingOperation or
      (call.command != None and (call.phase == Submitted or call.phase == Committed))
    val result = match call.stagedVersion {
      | Some(version) => MutationResult({ staged: Some(version), operation: None,
          rebasedTo: None, errors: Set(WaitCancelled) })
      | None => if (inFlight) Indeterminate else Refused(WaitCancelled)
    }
    { ...call, phase: Replied, result: Some(result), waiterCancelled: true }
  }
}
