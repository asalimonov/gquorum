module metaservice_scheduler_steps {
  import types.* from "../types"
  import world_types.* from "../runtime/world"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import frames as F from "../runtime/frames"
  import message_effects.* from "../runtime/messages"
  import membership as M from "../consensus/membership"
  import reads as Read from "./reads"
  import authority as A from "../metaengine/authority"
  import scheduler_repair as Repair from "../metaengine/scheduler_repair"
  import scheduling_policy as Policy from "../scheduling/policy"
  import topology as T from "../topology/topology"

  pure def leader(w: World, node: NodeName): bool = w.extensions.consensus.keys().contains("_meta") and
    M::running(w.system.get(node)) and (match w.extensions.consensus.get("_meta").leaderAuthority {
      | Some(key) => key.node == node and M::liveIdentity(w.system, key) | None => false })

  pure def active(w: World, node: NodeName): bool = leader(w, node) and w.system.get(node).volatile.schedulerEnabled and
    (match w.system.get(node).volatile.schedulerCapture {
      | None => true
      | Some(work) => work.process == { node: node, incarnation: w.system.get(node).volatile.incarnation } and
          work.epoch == w.extensions.consensus.get("_meta").leadershipEpoch })

  // ReadRepair on the metadata leader shares the ReadIndex certificate rule.
  // The confirmed position and later local application remain separate phases.
  pure def confirmed(w: World, node: NodeName): bool = {
    val process = { node: node, incarnation: w.system.get(node).volatile.incarnation }
    val ticket: ReadTicket = { owner: process, request: { caller: process, sequence: 0 }, proposal: None, round: 0 }
    Read::canConfirm(w.extensions, w.system, { delivery: 0, source: process, destination: node,
      channel: MetaChannel, payload: ReadProbe(ticket) })
  }

  pure def inputAt(w: World, node: NodeName, meta: MetadataState, observations: GroupId -> NativeObservation): SchedulerInput = {
    target: meta.target, groups: meta.groups, observations: observations,
    healthyNodes: w.system.keys().filter(n => M::running(w.system.get(n)) and
      M::communicates(w.extensions.network.metaLinks, node, n)),
    held: w.system.get(node).volatile.holds,
  }

  pure def scopeSame(input: SchedulerInput, fresh: MetadataState, group: GroupId): bool =
    Policy::readyGroup(input, group) and fresh.groups.keys().contains(group) and fresh.target.groups.keys().contains(group) and
      input.groups.get(group).sourceVersion == fresh.groups.get(group).sourceVersion and
      input.groups.get(group).owner == fresh.groups.get(group).owner and
      input.target.groups.get(group) == fresh.target.groups.get(group) and
      T::affectedScopes(input.target, fresh.target).intersect(Set(group)).size() == 0 and
      fresh.groups.get(group).proven == Some(fresh.target.groups.get(group)) and not(fresh.groups.get(group).deleting)

  pure def observe(w: World, node: NodeName, group: GroupId): Option[SchedulerSample] =
    if (not(w.extensions.consensus.keys().contains(group))) None else {
      val native = w.extensions.consensus.get(group)
      match native.leaderAuthority {
        | None => None
        | Some(key) => if (not(M::hasCommitPath(native, w.system, M::linksFor(w.extensions.network, group))) or
            not(M::communicates(M::linksFor(w.extensions.network, group), node, key.node))) None else Some({
              observation: { native: native, healthKnown: true,
                live: M::observedLive(native, w.system, M::linksFor(w.extensions.network, group)),
                ready: native.voters.union(native.learners).filter(r => M::readableIdentity(w.system, r)) },
              leaderProcess: { node: key.node, incarnation: w.system.get(key.node).volatile.incarnation } })
      }
    }

  pure def sameMembership(first: SchedulerSample, last: SchedulerSample): bool =
    first.leaderProcess == last.leaderProcess and
    first.observation.native.leadershipEpoch == last.observation.native.leadershipEpoch and
    first.observation.native.leaderAuthority == last.observation.native.leaderAuthority and
    first.observation.native.voters == last.observation.native.voters and
    first.observation.native.learners == last.observation.native.learners

  pure def done(work: SchedulerCapture, errors: Set[Refusal]): SchedulerCapture =
    { ...work, phase: ScheduleDone, result: Some(A::refused(errors)) }

  pure def startEnabled(w: World, node: NodeName): bool = active(w, node) and confirmed(w, node) and
    w.system.get(node).volatile.schedulerCapture == None

  pure def startNext(w: World, node: NodeName): World = F::withProcess(w, node, p => { ...p,
    schedulerCapture: Some({ phase: ScheduleApplying, process: { node: node, incarnation: p.incarnation },
      epoch: w.extensions.consensus.get("_meta").leadershipEpoch,
      readIndex: w.extensions.consensus.get("_meta").committedPosition, input: None, sampled: Set(), decision: None,
      fresh: None, sample: None, finalSample: None, freshInput: None, command: None,
      validations: 0, proposal: None, result: None }) })

  pure def at(w: World, node: NodeName, phase: SchedulerPhase): bool =
    w.system.get(node).volatile.schedulerCapture != None and w.system.get(node).volatile.schedulerCapture.unwrap().phase == phase

  pure def captureEnabled(w: World, node: NodeName): bool = active(w, node) and at(w, node, ScheduleApplying) and
    w.system.get(node).volatile.appliedMetaIndex >= w.system.get(node).volatile.schedulerCapture.unwrap().readIndex

  pure def captureNext(w: World, node: NodeName): World = {
    val host = w.system.get(node)
    val meta = w.extensions.metaLog.get(host.volatile.appliedMetaIndex)
    F::withProcess(w, node, p => { ...p, schedulerCapture: Some({ ...p.schedulerCapture.unwrap(),
      phase: ScheduleSampling, input: Some(inputAt(w, node, meta, Map())) }) })
  }

  pure def sampleEnabled(w: World, node: NodeName, group: GroupId): bool = active(w, node) and at(w, node, ScheduleSampling) and {
    val work = w.system.get(node).volatile.schedulerCapture.unwrap()
    work.input.unwrap().target.groups.keys().contains(group) and not(work.sampled.contains(group))
  }

  pure def sampleNext(w: World, node: NodeName, group: GroupId): World = {
    val work = w.system.get(node).volatile.schedulerCapture.unwrap()
    val input = work.input.unwrap()
    val local = w.extensions.metaLog.get(w.system.get(node).volatile.appliedMetaIndex)
    val observed = if (scopeSame(input, local, group)) observe(w, node, group) else None
    val updated = match observed { | None => input | Some(sample) => { ...input,
      observations: input.observations.put(group, sample.observation) } }
    F::withProcess(w, node, p => { ...p, schedulerCapture: Some({ ...work,
      input: Some(updated), sampled: work.sampled.union(Set(group)) }) })
  }

  pure def callEnabled(w: World, node: NodeName): bool = active(w, node) and at(w, node, ScheduleSampling) and {
    val work = w.system.get(node).volatile.schedulerCapture.unwrap()
    work.input.unwrap().target.groups.keys().subseteq(work.sampled)
  }

  pure def callNext(w: World, node: NodeName): World = F::withProcess(w, node, p => { ...p,
    schedulerCapture: Some({ ...p.schedulerCapture.unwrap(), phase: ScheduleCallback }) })

  pure def returnEnabled(w: World, node: NodeName): bool = at(w, node, ScheduleCallback) and
    w.system.get(node).volatile.phase != Stopped

  pure def returnNext(w: World, node: NodeName, decision: Option[RepairDecision]): World = F::withProcess(w, node, p => {
    val work = p.schedulerCapture.unwrap()
    { ...p, schedulerCursor: match decision { | Some(d) => Some(d.group) | None => p.schedulerCursor },
      schedulerCapture: Some(if (decision == None) done(work, Set()) else { ...work, phase: ScheduleReturned, decision: decision }) }
  })

  pure def readEnabled(w: World, node: NodeName, finalRead: bool): bool = active(w, node) and confirmed(w, node) and
    at(w, node, if (finalRead) SchedulePlanned else ScheduleReturned)

  pure def readNext(w: World, node: NodeName, finalRead: bool): World = F::withProcess(w, node, p => { ...p,
    schedulerCapture: Some({ ...p.schedulerCapture.unwrap(), phase: if (finalRead) ScheduleFinalRead else ScheduleRechecking,
      readIndex: w.extensions.consensus.get("_meta").committedPosition }) })

  pure def refreshEnabled(w: World, node: NodeName): bool = active(w, node) and at(w, node, ScheduleRechecking) and
    w.system.get(node).volatile.appliedMetaIndex >= w.system.get(node).volatile.schedulerCapture.unwrap().readIndex

  pure def refreshNext(w: World, node: NodeName): World = {
    val work = w.system.get(node).volatile.schedulerCapture.unwrap()
    val meta = w.extensions.metaLog.get(w.system.get(node).volatile.appliedMetaIndex)
    val valid = scopeSame(work.input.unwrap(), meta, work.decision.unwrap().group)
    F::withProcess(w, node, p => { ...p, schedulerCapture: Some(if (valid)
      { ...work, phase: ScheduleFresh, fresh: Some(meta) } else done(work, Set(Conflict))) })
  }

  pure def observeFreshEnabled(w: World, node: NodeName): bool = active(w, node) and at(w, node, ScheduleFresh)

  pure def observeFreshNext(w: World, node: NodeName): World = {
    val work = w.system.get(node).volatile.schedulerCapture.unwrap()
    val group = work.decision.unwrap().group
    val sample = observe(w, node, group)
    val updated = match sample {
      | None => done(work, Set(Unavailable))
      | Some(value) => {
          val input = inputAt(w, node, work.fresh.unwrap(), Map(group -> value.observation))
          if (Policy::eligible(input, work.decision.unwrap())) { ...work, phase: ScheduleObserved,
            sample: sample, freshInput: Some(input), validations: 1 } else done(work, Set(Unauthorized))
        }
    }
    F::withProcess(w, node, p => { ...p, schedulerCapture: Some(updated) })
  }

  pure def buildEnabled(w: World, node: NodeName): bool = active(w, node) and at(w, node, ScheduleObserved)

  pure def buildNext(w: World, node: NodeName, rules: Rules): World = {
    val work = w.system.get(node).volatile.schedulerCapture.unwrap()
    val meta = work.fresh.unwrap()
    val decision = work.decision.unwrap()
    val desired = Repair::document(meta, decision)
    val captured = { ...A::planningInput(meta, rules), observations: work.freshInput.unwrap().observations }
    val updated = match T::makePlan(captured, desired) {
      | PlanRejected(errors) => done(work, errors)
      | Planned(plan) => {
          val command: SchedulerCommand = { decision: decision, expectedVersion: meta.latest.version, expectedTarget: meta.target.version,
            approval: { desired: desired, plan: plan, captured: captured,
              scopes: Map(decision.group -> A::scopeAt(meta, decision.group)), origin: Reconciler, audit: desired.audit } }
          val errors = Repair::errors(meta, command, rules)
          if (errors.size() == 0) { ...work, phase: SchedulePlanned, command: Some(command) } else done(work, errors)
        }
    }
    F::withProcess(w, node, p => { ...p, schedulerCapture: Some(updated) })
  }

  pure def validateEnabled(w: World, node: NodeName): bool = active(w, node) and at(w, node, ScheduleFinalRead) and
    w.system.get(node).volatile.appliedMetaIndex >= w.system.get(node).volatile.schedulerCapture.unwrap().readIndex

  pure def validateNext(w: World, node: NodeName): World = {
    val work = w.system.get(node).volatile.schedulerCapture.unwrap()
    val meta = w.extensions.metaLog.get(w.system.get(node).volatile.appliedMetaIndex)
    val prior = work.fresh.unwrap()
    val group = work.decision.unwrap().group
    val sample = observe(w, node, group)
    val updated = match sample {
      | None => done(work, Set(Unavailable))
      | Some(value) => {
          val input = inputAt(w, node, meta, Map(group -> value.observation))
          if (meta.latest.version != prior.latest.version or meta.target.version != prior.target.version or
              not(scopeSame(work.input.unwrap(), meta, group)) or not(sameMembership(work.sample.unwrap(), value))) done(work, Set(Conflict))
          else if (not(Policy::eligible(input, work.decision.unwrap()))) done(work, Set(Unauthorized))
          else { ...work, phase: ScheduleValidated, finalSample: Some(value), freshInput: Some(input), validations: 2 }
        }
    }
    F::withProcess(w, node, p => { ...p, schedulerCapture: Some(updated) })
  }

  pure def submitEnabled(w: World, node: NodeName, limit: int): bool = active(w, node) and at(w, node, ScheduleValidated) and
    w.extensions.nextDelivery <= limit

  // Called by the real guarded facade and by one explicit missing-revalidation
  // negative control. No new remote-health read is invented at submission.
  pure def submitNext(w: World, node: NodeName): World = {
    val work = w.system.get(node).volatile.schedulerCapture.unwrap()
    val command = work.command.unwrap()
    val proposal: ProposalKey = { request: { caller: work.process, sequence: -w.extensions.nextDelivery }, sequence: 0 }
    val finalSample = match work.finalSample { | Some(sample) => sample | None => work.sample.unwrap() }
    val evidence = { process: work.process, authorized: active(w, node), command: command, validations: work.validations,
      first: work.sample.unwrap(), last: finalSample, input: work.freshInput.unwrap() }
    val base = F::withProcess(w, node, p => { ...p, schedulerCapture: Some({ ...work, phase: ScheduleSubmitted, proposal: Some(proposal) }) })
    apply_custom_effect({ ...base, extensions: { ...base.extensions,
      schedulerSubmissions: base.extensions.schedulerSubmissions.union(Set(evidence)) } }, Queue({
      delivery: w.extensions.nextDelivery, source: work.process, destination: node, channel: MetaChannel,
      payload: MetaProposal({ key: proposal, coordinator: work.process, forWork: false, command: SchedulerProposal(command) }) }))
  }

  pure def ackEnabled(w: World, msg: Envelope): bool = w.messages.get(msg.destination).contains(msg) and
    at(w, msg.destination, ScheduleSubmitted) and match msg.payload {
      | MetaAck(ack) => ack.coordinator == { node: msg.destination, incarnation: w.system.get(msg.destination).volatile.incarnation } and
          w.system.get(msg.destination).volatile.schedulerCapture.unwrap().proposal == Some(ack.key)
      | _ => false }

  pure def ackNext(w: World, msg: Envelope): World = match msg.payload {
    | MetaAck(ack) => apply_custom_effect(F::withProcess(w, msg.destination, p => { ...p,
        schedulerCapture: Some({ ...p.schedulerCapture.unwrap(), phase: ScheduleDone, result: Some(ack.outcome) }) }), Consume(msg))
    | _ => w
  }

  pure def releaseEnabled(w: World, node: NodeName): bool = w.system.get(node).volatile.schedulerCapture != None and
    not(at(w, node, ScheduleCallback)) and (at(w, node, ScheduleDone) or not(active(w, node)))

  pure def releaseNext(w: World, node: NodeName): World = F::withProcess(w, node, p => { ...p, schedulerCapture: None })
  pure def timeoutEnabled(w: World, node: NodeName): bool = at(w, node, ScheduleSubmitted)
  pure def timeoutNext(w: World, node: NodeName): World = F::withProcess(w, node, p => { ...p,
    schedulerCapture: Some({ ...p.schedulerCapture.unwrap(), phase: ScheduleDone, result: None }) })

  pure def toggleEnabled(w: World, node: NodeName, value: bool): bool = M::running(w.system.get(node)) and
    w.system.get(node).volatile.schedulerEnabled != value
  pure def toggleNext(w: World, node: NodeName, value: bool): World = F::withProcess(w, node, p => { ...p, schedulerEnabled: value })

  pure def submissionsValid(w: World): bool = w.extensions.schedulerSubmissions.forall(s =>
    s.authorized and s.validations == 2 and sameMembership(s.first, s.last) and Policy::eligible(s.input, s.command.decision))
}
