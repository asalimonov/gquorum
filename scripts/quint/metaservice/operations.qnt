module operations {
  import types.* from "../types"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import authority as A from "../metaengine/authority"
  import remaining as Remaining from "./remaining"
  import topology as T from "../topology/topology"

  pure def newWork(request: OperationRequest): OperationWork = {
    request: request, phase: WorkReading, readIndex: None, captured: None,
    desired: None, input: None, queried: Set(), revalidated: false, readRound: 0, scopes: Map(), command: None,
    commitIndex: None, result: None, replySent: false,
  }
  pure def finished(work: OperationWork, result: MutationOutcome): OperationWork =
    { ...work, phase: WorkFinished, result: Some(result), replySent: false }

  pure def captureDesired(work: OperationWork, meta: MetadataState, desired: Document,
                          groups: Set[GroupId], rules: Rules): OperationWork =
    { ...work, phase: WorkObserving, captured: Some(meta), desired: Some(desired),
      input: Some(A::planningInput(meta, rules)), scopes: groups.mapBy(g => A::scopeAt(meta, g)) }

  pure def capture(work: OperationWork, meta: MetadataState, rules: Rules): OperationWork = match work.request {
    | ApproveVersion(version) =>
      if (version == 0) finished(work, A::refused(Set(Invalid)))
      else if (meta.firstAttempts.keys().contains(version)) finished(work, A::stageOutcome(meta, version))
      else if (version != meta.latest.version or version <= meta.target.version)
        finished(work, { ...A::refused(Set(Conflict)), rebasedTo: A::rebasedSuccessor(meta, version) })
      else captureDesired(work, meta, meta.latest, T::affectedScopes(meta.target, meta.latest), rules)
    | RetryAttempt(previous) =>
      if (meta.successors.keys().contains(previous)) finished(work, {
        staged: Some(previous.version), operation: Some(meta.successors.get(previous)), rebasedTo: None, errors: Set() })
      else if (not(meta.operations.keys().contains(previous))) finished(work, A::refused(Set(NotFound)))
      else {
        val groups = A::failedGroups(meta.operations.get(previous))
        val scopes = groups.mapBy(g => A::scopeAt(meta, g))
        val errors = A::retryErrors(meta, { previous: previous, scopes: scopes })
        if (errors.size() > 0) finished(work, A::refused(errors)) else
          captureDesired(work, meta, meta.history.get(previous.version), groups, rules)
      }
    | RepairRequest(request) => match A::replayVersion(meta, request) {
        | Some(version) => finished(work, A::stageOutcome(meta, version))
        | None => {
          val input = A::mutationInput(meta, Map(), rules)
          val errors = T::mutationErrors(input, request)
          if (errors.size() > 0) finished(work, A::refused(errors)) else {
            val desired = T::mutateDocument(input, request)
            captureDesired(work, meta, desired, T::affectedScopes(meta.target, desired), rules)
          }
        }
      }
  }

  pure def requiredReads(work: OperationWork): Set[GroupId] = match work.input {
    | Some(input) => match work.desired {
        | Some(desired) => work.scopes.keys().intersect(desired.groups.keys()).filter(g =>
            input.approved.groups.keys().contains(g) or input.retained.exists(r => r.group == g))
        | None => Set()
      }
    | None => Set()
  }

  pure def observationsComplete(work: OperationWork): bool =
    requiredReads(work).subseteq(work.queried) and work.input != None and work.desired != None and work.captured != None
      and match work.input { | Some(input) => requiredReads(work).forall(g =>
          if (input.observations.keys().contains(g)) input.observations.get(g).healthKnown else false)
        | None => false }

  pure def canRecapture(work: OperationWork): bool =
    work.phase == WorkObserving and observationsComplete(work) and not(work.revalidated)

  // metaservice.capturePlan rechecks metadata after the native/status I/O.
  // Unrelated groups may advance; the captured scopes and relevant declarations
  // must still match. Native observations remain the original samples.
  pure def recapture(work: OperationWork, meta: MetadataState, rules: Rules): OperationWork = {
    val after = { ...capture(newWork(work.request), meta, rules), readIndex: work.readIndex, readRound: work.readRound }
    if (after.phase == WorkFinished) after else match work.input {
      | Some(beforeInput) => match after.input {
          | Some(afterInput) => match work.desired {
              | Some(beforeDesired) => match after.desired {
                  | Some(afterDesired) => {
                    val changed = T::affectedScopes(beforeInput.approved, afterInput.approved)
                      .union(T::affectedScopes(beforeDesired, afterDesired))
                    if (work.scopes != after.scopes or changed.intersect(work.scopes.keys()).size() > 0)
                      finished(work, A::refused(Set(Conflict)))
                    else { ...after, queried: work.queried, revalidated: true,
                      input: Some({ ...afterInput, observations: beforeInput.observations }) }
                  }
                  | None => finished(work, A::refused(Set(Invalid)))
                }
              | None => finished(work, A::refused(Set(Invalid)))
            }
          | None => finished(work, A::refused(Set(Invalid)))
        }
      | None => finished(work, A::refused(Set(Invalid)))
    }
  }

  pure def canBuild(work: OperationWork): bool =
    work.phase == WorkObserving and observationsComplete(work) and work.revalidated

  pure def build(work: OperationWork): OperationWork = match work.input {
    | Some(input) => match work.desired {
        | Some(desired) => match work.captured {
            | Some(meta) => match work.request {
                | RetryAttempt(previous) => {
                  val valid = requiredReads(work).forall(g => if (not(input.observations.keys().contains(g))) false else {
                    val o = input.observations.get(g)
                    o.healthKnown and o.native.pendingChange == None and
                      o.native.voters.intersect(o.live).size() * 2 > o.native.voters.size()
                  })
                  val suffixErrors = Remaining::errors(meta, previous, input)
                  if (not(valid)) finished(work, A::refused(Set(Unavailable)))
                  else if (suffixErrors.size() > 0) finished(work, A::refused(suffixErrors)) else
                    { ...work, phase: WorkPrepared,
                      command: Some(RetryProposal({ previous: previous, scopes: work.scopes })) }
                }
                | _ => match T::makePlan(input, desired) {
                    | PlanRejected(errors) => finished(work, A::refused(errors))
                    | Planned(plan) => {
                      val approval: ApprovalCommand = { desired: desired, plan: plan,
                        scopes: work.scopes, captured: input, origin: Operator, audit: desired.audit }
                      val command = match work.request {
                        | RepairRequest(request) => RepairProposal({ request: request,
                            expectedTarget: meta.target.version, approval: approval })
                        | _ => ApprovalProposal(approval)
                      }
                      { ...work, phase: WorkPrepared, command: Some(command) }
                    }
                  }
              }
            | None => work
          }
        | None => work
      }
    | None => work
  }
}
