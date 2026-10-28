module managed_membership_steps {
  import admission as Admission from "../metaengine/admission"
  import types.* from "../types"
  import world_types.* from "../runtime/world"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import membership as M from "../consensus/membership"
  import execution as E from "./execution"
  import frames as F from "../runtime/frames"

  pure def isNative(delivery: EffectDelivery): bool = match delivery.grant.effect {
    | Some(effect) => M::nativeKind(effect.kind) and
        delivery.resource == GroupResource(delivery.grant.key.group) and delivery.grant.planStep != None
    | None => false
  }

  pure def capture(w: World, delivery: EffectDelivery): NativeChange = {
    val receiver = w.system.get(delivery.receiver.node)
    val c = w.extensions.consensus.get(delivery.grant.key.group)
    val step = delivery.grant.planStep.unwrap()
    { kind: step.kind, target: match step.target { | ReplicaTarget(r) => r | _ => delivery.replica },
      sourceConfig: c.configRevision,
      evidence: { delivery: delivery, pin: receiver.durable.pin, incarnation: receiver.volatile.incarnation,
        owner: if (receiver.volatile.owners.keys().contains(delivery.grant.key.group))
          Some(receiver.volatile.owners.get(delivery.grant.key.group).key) else None },
      live: M::observedLive(c, w.system, M::linksFor(w.extensions.network, delivery.grant.key.group)) }
  }

  pure def currentLeader(w: World, delivery: EffectDelivery): bool = {
    val c = w.extensions.consensus.get(delivery.grant.key.group)
    c.leaderAuthority == Some(delivery.replica) and E::mayConsume(w, delivery)
  }

  // The native authority floor is persisted by Raft before configuration work.
  // Installing authority is not a membership receipt or a completed effect.
  pure def installNativeEnabled(w: World, delivery: EffectDelivery, maxPosition: int): bool = {
    val c = w.extensions.consensus.get(delivery.grant.key.group)
    isNative(delivery) and w.system.get(delivery.receiver.node).volatile.waitingEffects.contains(delivery) and
      currentLeader(w, delivery) and c.pendingChange == None and
      c.authorityFloor < delivery.grant.key.revision and c.committedPosition < maxPosition and
      M::hasCommitPath(c, w.system, M::linksFor(w.extensions.network, delivery.grant.key.group))
  }

  pure def installNativeNext(w: World, delivery: EffectDelivery): World = {
    val group = delivery.grant.key.group
    val c = w.extensions.consensus.get(group)
    val voters = M::certificate(c, w.system, M::linksFor(w.extensions.network, group), delivery.replica)
    val position = c.committedPosition + 1
    { ...w, system: M::persistCertificate(w.system, voters, position),
      extensions: { ...w.extensions, consensus: w.extensions.consensus.put(group,
        { ...M::committed(c, voters, position), authorityFloor: delivery.grant.key.revision }),
        metaLog: if (group == "_meta") w.extensions.metaLog.put(position, w.extensions.metadata) else w.extensions.metaLog } }
  }

  pure def admissionReady(w: World, delivery: EffectDelivery): bool = {
    val step = delivery.grant.planStep.unwrap()
    if (delivery.grant.key.group != "_meta" or step.kind != AddLearner) true else {
      val local = w.extensions.metaLog.get(w.system.get(delivery.receiver.node).volatile.appliedMetaIndex)
      val key = match step.target { | ReplicaTarget(r) => r | _ => delivery.replica }
      if (not(local.admissions.keys().contains(key.node))) false else {
        val a = local.admissions.get(key.node)
        a.replica == key and a.operation == delivery.grant.effect.unwrap().operation and Admission::preparedProcess(local, a)
      }
    }
  }

  pure def nativeReady(w: World, delivery: EffectDelivery): bool =
    if (not(isNative(delivery))) false else {
      val c = w.extensions.consensus.get(delivery.grant.key.group)
      val candidate = capture(w, delivery)
      w.system.get(delivery.receiver.node).volatile.waitingEffects.contains(delivery) and
        currentLeader(w, delivery) and admissionReady(w, delivery) and not(E::consumed(w, delivery)) and c.pendingChange == None and
        c.authorityFloor == delivery.grant.key.revision and
        match delivery.grant.planStep {
          | Some(step) => M::expectedChange(c, step) and M::safeChange(c, candidate.kind, candidate.target,
              candidate.live, c.catchUp.getOrElse(candidate.target, Behind) == CaughtUp)
          | None => false }
    }

  pure def precheckNativeEnabled(w: World, delivery: EffectDelivery): bool = {
    val checks = w.system.get(delivery.receiver.node).volatile.nativeChecks
    nativeReady(w, delivery) and (if (checks.keys().contains(delivery.grant.key))
      checks.get(delivery.grant.key) != capture(w, delivery) else true)
  }

  pure def precheckNativeNext(w: World, delivery: EffectDelivery): World =
    F::withProcess(w, delivery.receiver.node, process => { ...process,
      nativeChecks: process.nativeChecks.put(delivery.grant.key, capture(w, delivery)) })

  // Re-read native safety after the precheck/wait, as the serialized owner loop
  // does. A successful proposal still has no commit or completion evidence.
  pure def proposeNativeEnabled(w: World, delivery: EffectDelivery): bool = {
    val checks = w.system.get(delivery.receiver.node).volatile.nativeChecks
    if (not(checks.keys().contains(delivery.grant.key))) false else {
      val captured = checks.get(delivery.grant.key)
      nativeReady(w, delivery) and captured.evidence.delivery == delivery and
        captured.sourceConfig == w.extensions.consensus.get(delivery.grant.key.group).configRevision
    }
  }

  pure def proposeNativeNext(w: World, delivery: EffectDelivery): World = {
    val base = F::withProcess(w, delivery.receiver.node, process => { ...process,
      nativeChecks: process.nativeChecks.mapRemove(delivery.grant.key),
      waitingEffects: process.waitingEffects.exclude(Set(delivery)) })
    { ...base, extensions: { ...base.extensions, consensus: base.extensions.consensus.setBy(delivery.grant.key.group,
      c => { ...c, pendingChange: Some(capture(w, delivery)) }) } }
  }

  pure def commitNativeEnabled(w: World, group: GroupId, maxPosition: int): bool = {
    val c = w.extensions.consensus.get(group)
    match c.pendingChange {
      | Some(change) => c.committedPosition < maxPosition and c.configRevision == change.sourceConfig and
          c.authorityFloor == change.evidence.delivery.grant.key.revision and
          M::hasCommitPath(c, w.system, M::linksFor(w.extensions.network, group)) and
          match change.evidence.delivery.grant.planStep { | Some(step) => M::expectedChange(c, step) | None => false }
      | None => false
    }
  }

  // Current-quorum commitment and durable native configuration/receipt apply
  // are one abstract Raft boundary. Reporting/checkpointing remain separate.
  // Do not recheck transient liveness here: SafetyRules ran before proposal.
  pure def commitNativeNext(w: World, group: GroupId, deliveries: int): World = match w.extensions.consensus.get(group).pendingChange {
    | None => w
    | Some(change) => {
        val c = w.extensions.consensus.get(group)
        val delivery = change.evidence.delivery
        val position = c.committedPosition + 1
        val voters = match c.leaderAuthority { | Some(leader) =>
          M::certificate(c, w.system, M::linksFor(w.extensions.network, group), leader) | None => Set() }
        val receipt = { ...E::receipt(delivery, position),
          outcome: if (M::nativeSatisfied(c, change.kind, change.target)) AlreadySatisfied else EffectApplied }
        val committed = M::committed(c, voters, position)
        val result = if (M::nativeSatisfied(c, change.kind, change.target)) { ...committed, pendingChange: None }
          else M::applyChange(committed, change.kind, change.target)
        val applied = { ...w, system: M::persistCertificate(w.system, voters, position),
          extensions: { ...w.extensions, consensus: w.extensions.consensus.put(group,
            { ...result, authorityReceipts: result.authorityReceipts.union(Set(receipt)) }),
            consumptions: w.extensions.consumptions.union(Set(change.evidence)),
            metaLog: if (group == "_meta") w.extensions.metaLog.put(position, w.extensions.metadata) else w.extensions.metaLog } }
        E::returnReceipt(applied, delivery, receipt, deliveries)
      }
  }

  // operator.classifyStepError: temporary native liveness/catch-up failures
  // wait under the same owner; missing predecessors or invalid proof fail.
  pure def nativeStatus(w: World, delivery: EffectDelivery): Option[BlockProgress] =
    if (not(isNative(delivery)) or E::consumed(w, delivery)) None else {
      val c = w.extensions.consensus.get(delivery.grant.key.group)
      val step = delivery.grant.planStep.unwrap()
      val target = match step.target { | ReplicaTarget(r) => r | _ => delivery.replica }
      val failed = Some({ phase: OpFailed, step: step.seq, reason: None })
      val leader = Some({ phase: WaitingLeader, step: step.seq, reason: Some(LeaderRequired) })
      val catchup = Some({ phase: WaitingCatchUp, step: step.seq, reason: Some(CatchUpRequired) })
      if (not(E::mayConsume(w, delivery))) failed
      else if (c.leaderAuthority != Some(delivery.replica) or c.pendingChange != None) leader
      else if (not(admissionReady(w, delivery))) Some({ phase: WaitingReplica, step: step.seq, reason: Some(CallbackPending) })
      else if (c.authorityFloor < delivery.grant.key.revision) None
      else if (not(M::expectedChange(c, step))) failed
      else if (M::nativeSatisfied(c, step.kind, target)) None
      else if ((step.kind == AddLearner and c.voters.contains(target)) or
        (step.kind == DemoteVoter and not(c.voters.contains(target)))) failed
      else if (step.kind == PromoteLearner and not(c.learners.contains(target))) failed
      else if (step.kind == PromoteLearner and c.catchUp.getOrElse(target, Behind) != CaughtUp) catchup
      else if (c.leaderAuthority == Some(target) and step.kind == DemoteVoter and step.transferTarget != None) leader
      else if (M::postVoters(c, step.kind, target).size() == 0 or
        (c.leaderAuthority == Some(target) and step.kind == RemoveMember)) failed
      else if (not(M::safeChange(c, step.kind, target,
        M::observedLive(c, w.system, M::linksFor(w.extensions.network, target.group)), true)))
        if (step.kind == PromoteLearner) catchup else Some({ phase: WaitingReplica, step: step.seq, reason: Some(NoQuorum) })
      else None
    }

  // Uncommitted work may be lost with its leader; durable authority remains.
  pure def discardNativeEnabled(w: World, group: GroupId): bool = match w.extensions.consensus.get(group).pendingChange {
    | None => false
    | Some(change) => w.extensions.consensus.get(group).leaderAuthority != Some(change.evidence.delivery.replica) or
        not(M::liveIdentity(w.system, change.evidence.delivery.replica))
  }

  pure def discardNativeNext(w: World, group: GroupId): World =
    { ...w, extensions: { ...w.extensions, consensus: w.extensions.consensus.setBy(group,
      c => { ...c, pendingChange: None }) } }

  pure def replayNativeEnabled(w: World, delivery: EffectDelivery): bool = isNative(delivery) and
    w.system.get(delivery.receiver.node).volatile.waitingEffects.contains(delivery) and
    E::mayConsume(w, delivery) and E::consumed(w, delivery)

  pure def replayNativeNext(w: World, delivery: EffectDelivery): World = E::forgetWait(w, delivery)
}
