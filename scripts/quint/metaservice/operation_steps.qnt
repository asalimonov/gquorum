module metaservice_operation_steps {
  import frames as Frames from "../runtime/frames"
  import handlers.isReseat from "./handlers"
  import authority as A from "../metaengine/authority"
  import membership as M from "../consensus/membership"
  import operations as O from "./operations"
  import transactions as Tx from "./transactions"
  import types.* from "../types"
  import world_types.* from "../runtime/world"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import message_effects.* from "../runtime/messages"

  pure def dispatchOperationEnabled(w: World, key: RequestKey, MAX_DELIVERIES: int): bool = {
    val owner = w.system.get(key.caller.node)
    val call = owner.volatile.pendingCalls.get(key)
    val leader = if (owner.volatile.observations.keys().contains("_meta"))
      owner.volatile.observations.get("_meta").observedLeader else None
    match Tx::operationToSend(call) {
      | Some(request) => match leader {
          | Some(r) => {
            and {
              M::running(owner),
              key.caller.incarnation == owner.volatile.incarnation,
              not(call.waiterCancelled),
              w.extensions.nextDelivery <= MAX_DELIVERIES
            }
          }
          | None => and {
            false
          }
        }
      | None => and {
        false
      }
    }
  }

  pure def dispatchOperationNext(w: World, key: RequestKey, MAX_DELIVERIES: int): World = {
    val owner = w.system.get(key.caller.node)
    val call = owner.volatile.pendingCalls.get(key)
    val leader = if (owner.volatile.observations.keys().contains("_meta"))
      owner.volatile.observations.get("_meta").observedLeader else None
    match Tx::operationToSend(call) {
      | Some(request) => match leader {
          | Some(r) => {
            val updated = Tx::awaitingOperation(call, request)
            (apply_custom_effect(Frames::withProcess(w, key.caller.node, updatedProcess => { ...updatedProcess, pendingCalls: updatedProcess.pendingCalls.put(key, updated) }),
                Queue({ delivery: w.extensions.nextDelivery, source: key.caller, destination: r.node,
                  channel: MetaChannel, payload: OperationCall({
                    key: { request: key, sequence: updated.proposalSequence }, request: request }) })))
          }
          | None => (w)
        }
      | None => (w)
    }
  }

  pure def transmitOperationReadEnabled(w: World, node: NodeName, key: ProposalKey, retry: bool, MAX_DELIVERIES: int): bool = {
    val owner = w.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    and {
      M::running(owner),
      work.phase == (if (retry) WorkReadQueued else WorkReading),
      w.extensions.nextDelivery <= MAX_DELIVERIES
    }
  }

  pure def transmitOperationReadNext(w: World, node: NodeName, key: ProposalKey, retry: bool, MAX_DELIVERIES: int): World = {
    val owner = w.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    val process = { node: node, incarnation: owner.volatile.incarnation }
    (apply_custom_effect(Frames::withProcess(w, node, updatedProcess => { ...updatedProcess,
        operationWork: updatedProcess.operationWork.put(key, { ...work, phase: WorkReadQueued }) }),
        Queue({ delivery: w.extensions.nextDelivery,
        source: process, destination: node, channel: MetaChannel,
        payload: ReadProbe({ owner: process, request: key.request, proposal: Some(key.sequence), round: work.readRound }) })))
  }

  pure def captureOperationEnabled(w: World, node: NodeName, key: ProposalKey, rules: Rules): bool = {
    val owner = w.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    and {
      M::running(owner),
      work.phase == WorkApplying,
      match work.readIndex { | Some(index) => owner.volatile.appliedMetaIndex >= index | None => false }
    }
  }

  pure def captureOperationNext(w: World, node: NodeName, key: ProposalKey, rules: Rules): World = {
    val owner = w.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    val localMeta = w.extensions.metaLog.get(owner.volatile.appliedMetaIndex)
    (Frames::withProcess(w, node, updatedProcess => { ...updatedProcess,
        operationWork: updatedProcess.operationWork.put(key,
          if (work.captured == None) O::capture(work, localMeta, rules) else O::recapture(work, localMeta, rules)) }))
  }

  pure def observeOperationEnabled(w: World, node: NodeName, key: ProposalKey, group: GroupId): bool = {
    val owner = w.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    and {
      M::running(owner),
      work.phase == WorkObserving,
      O::requiredReads(work).contains(group),
      not(work.queried.contains(group))
    }
  }

  pure def observeOperationNext(w: World, node: NodeName, key: ProposalKey, group: GroupId): World = {
    val owner = w.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    val links = M::linksFor(w.extensions.network, group)
    val observed = if (not(w.extensions.consensus.keys().contains(group))) None else {
      val native = w.extensions.consensus.get(group)
      val responders = native.voters.union(native.learners).filter(r =>
        M::readableIdentity(w.system, r) and M::communicates(links, node, r.node) and
          native.catchUp.getOrElse(r, CatchUpUnknown) == CaughtUp and
          match native.leaderAuthority { | Some(leader) => M::communicates(links, r.node, leader.node) | None => false })
      if (responders.size() == 0 or not(M::hasCommitPath(native, w.system, links))) None
      else Some({ native: native, healthKnown: false, live: Set(), ready: Set() })
    }
    val updated = match work.input {
      | Some(input) => match observed {
          | Some(o) => { ...work, queried: work.queried.union(Set(group)), input: Some({ ...input,
              observations: input.observations.put(group, o) }) }
          | None => O::finished(work, A::refused(Set(Unavailable)))
        }
      | None => work
    }
    (Frames::withProcess(w, node, updatedProcess => { ...updatedProcess,
        operationWork: updatedProcess.operationWork.put(key, updated) }))
  }

  pure def observeOperationHealthEnabled(w: World, node: NodeName, key: ProposalKey, group: GroupId): bool = {
    val owner = w.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    match work.input {
      | Some(input) => if (not(input.observations.keys().contains(group))) and {
false
} else {
          val prior = input.observations.get(group)
          and {
            M::running(owner),
            work.phase == WorkObserving,
            not(prior.healthKnown)
          }
        }
      | None => and {
        false
      }
    }
  }

  pure def observeOperationHealthNext(w: World, node: NodeName, key: ProposalKey, group: GroupId): World = {
    val owner = w.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    match work.input {
      | Some(input) => if (not(input.observations.keys().contains(group))) (w) else {
          val prior = input.observations.get(group)
          val links = M::linksFor(w.extensions.network, group)
          val live = prior.native.voters.union(prior.native.learners).filter(r =>
            M::liveIdentity(w.system, r) and M::communicates(links, node, r.node))
          val observed = { ...prior, healthKnown: true, live: live,
            ready: live.filter(r => if (w.extensions.consensus.keys().contains(group))
              w.extensions.consensus.get(group).catchUp.getOrElse(r, CatchUpUnknown) == CaughtUp else false) }
          (Frames::withProcess(w, node, updatedProcess => { ...updatedProcess,
              operationWork: updatedProcess.operationWork.put(key, { ...work,
                input: Some({ ...input, observations: input.observations.put(group, observed) }) }) }))
        }
      | None => (w)
    }
  }

  pure def beginOperationRecaptureEnabled(w: World, node: NodeName, key: ProposalKey): bool = {
    val owner = w.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    and {
      M::running(owner),
      O::canRecapture(work)
    }
  }

  pure def beginOperationRecaptureNext(w: World, node: NodeName, key: ProposalKey): World = {
    val owner = w.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    (Frames::withProcess(w, node, updatedProcess => { ...updatedProcess,
        operationWork: updatedProcess.operationWork.put(key, { ...work, phase: WorkReading, readIndex: None, readRound: work.readRound + 1 }) }))
  }

  pure def buildOperationEnabled(w: World, node: NodeName, key: ProposalKey): bool = {
    val owner = w.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    and {
      M::running(owner),
      O::canBuild(work)
    }
  }

  pure def buildOperationNext(w: World, node: NodeName, key: ProposalKey): World = {
    val owner = w.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    (Frames::withProcess(w, node, updatedProcess => { ...updatedProcess,
        operationWork: updatedProcess.operationWork.put(key, O::build(work)) }))
  }

  pure def submitOperationEnabled(w: World, node: NodeName, key: ProposalKey, MAX_DELIVERIES: int): bool = {
    val owner = w.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    match work.command {
      | Some(command) => and {
M::running(owner),
work.phase == WorkPrepared or work.phase == WorkSubmitted,
match w.extensions.consensus.get("_meta").leaderAuthority { | Some(r) => r.node == node | None => false },
w.extensions.nextDelivery <= MAX_DELIVERIES
}
      | None => and {
        false
      }
    }
  }

  pure def submitOperationNext(w: World, node: NodeName, key: ProposalKey, MAX_DELIVERIES: int): World = {
    val owner = w.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    match work.command {
      | Some(command) => (apply_custom_effect(Frames::withProcess(w, node, updatedProcess => { ...updatedProcess,
            operationWork: updatedProcess.operationWork.put(key, { ...work, phase: WorkSubmitted }) }),
            Queue({ delivery: w.extensions.nextDelivery, source: { node: node, incarnation: owner.volatile.incarnation },
              destination: node, channel: MetaChannel, payload: MetaProposal({ key: key,
                coordinator: { node: node, incarnation: owner.volatile.incarnation }, forWork: true, command: command }) })))
      | None => (w)
    }
  }

  pure def finishOperationEnabled(w: World, node: NodeName, key: ProposalKey): bool = {
    val owner = w.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    and {
      M::running(owner),
      work.phase == WorkCommitted,
      work.result != None,
      match work.commitIndex { | Some(index) => owner.volatile.appliedMetaIndex >= index | None => false }
    }
  }

  pure def finishOperationNext(w: World, node: NodeName, key: ProposalKey): World = {
    val owner = w.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    (Frames::withProcess(w, node, updatedProcess => { ...updatedProcess,
        operationWork: updatedProcess.operationWork.put(key, { ...work, phase: WorkFinished, replySent: false }) }))
  }

  pure def replyOperationEnabled(w: World, node: NodeName, key: ProposalKey, MAX_DELIVERIES: int): bool = {
    val owner = w.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    match work.result {
      | Some(result) => and {
M::running(owner),
work.phase == WorkFinished,
not(work.replySent),
w.extensions.nextDelivery <= MAX_DELIVERIES
}
      | None => and {
        false
      }
    }
  }

  // An explicit successful leader-observation exchange. Cached routing is not
  // silently refreshed by either an election or a metadata commit.

  pure def replyOperationNext(w: World, node: NodeName, key: ProposalKey, MAX_DELIVERIES: int): World = {
    val owner = w.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    match work.result {
      | Some(result) => (apply_custom_effect(Frames::withProcess(w, node, updatedProcess => { ...updatedProcess,
            operationWork: updatedProcess.operationWork.put(key, { ...work, replySent: true }) }),
            Queue({ delivery: w.extensions.nextDelivery, source: { node: node, incarnation: owner.volatile.incarnation },
              destination: key.request.caller.node, channel: MetaChannel, payload: OperationAck({ key: key, outcome: result }) })))
      | None => (w)
    }
  }

  // An explicit successful leader-observation exchange. Cached routing is not
  // silently refreshed by either an election or a metadata commit.

  pure def observeMetaLeaderEnabled(w: World, node: NodeName): bool = {
    val owner = w.system.get(node)
    val c = w.extensions.consensus.get("_meta")
    match c.leaderAuthority {
      | Some(leader) => {
          val prior = owner.volatile.observations.getOrElse("_meta", {
            group: "_meta", replica: leader, incarnation: owner.volatile.incarnation,
            observedConfig: 0, observedLeader: None, health: Healthy, age: Fresh })
          and {
            M::running(owner),
            M::liveIdentity(w.system, leader),
            M::communicates(w.extensions.network.metaLinks, node, leader.node),
            prior.observedLeader != Some(leader) or prior.observedConfig != c.configRevision
          }
        }
      | None => and {
        false
      }
    }
  }

  pure def observeMetaLeaderNext(w: World, node: NodeName): World = {
    val owner = w.system.get(node)
    val c = w.extensions.consensus.get("_meta")
    match c.leaderAuthority {
      | Some(leader) => {
          val prior = owner.volatile.observations.getOrElse("_meta", {
            group: "_meta", replica: leader, incarnation: owner.volatile.incarnation,
            observedConfig: 0, observedLeader: None, health: Healthy, age: Fresh })
          (Frames::withProcess(w, node, updatedProcess => { ...updatedProcess,
              observations: updatedProcess.observations.put("_meta", { ...prior,
                observedLeader: Some(leader), observedConfig: c.configRevision }) }))
        }
      | None => (w)
    }
  }

  pure def retryReadEnabled(w: World, key: RequestKey, MAX_DELIVERIES: int): bool = {
    val owner = w.system.get(key.caller.node)
    val call = owner.volatile.pendingCalls.get(key)
    val leader = if (owner.volatile.observations.keys().contains("_meta"))
      owner.volatile.observations.get("_meta").observedLeader else None
    match leader {
      | Some(r) => and {
M::running(owner),
key.caller.incarnation == owner.volatile.incarnation,
(call.request == ConfirmedRead or isReseat(call.request)),
call.phase == Submitted,
not(call.waiterCancelled),
w.extensions.nextDelivery <= MAX_DELIVERIES
}
      | None => and {
        false
      }
    }
  }

  // Cancellation affects the waiter; submitted work and proof are not undone.

  pure def retryReadNext(w: World, key: RequestKey, MAX_DELIVERIES: int): World = {
    val owner = w.system.get(key.caller.node)
    val leader = if (owner.volatile.observations.keys().contains("_meta"))
      owner.volatile.observations.get("_meta").observedLeader else None
    match leader {
      | Some(r) => (apply_custom_effect(w, Queue({ delivery: w.extensions.nextDelivery,
            source: key.caller, destination: r.node, channel: MetaChannel, payload: ReadProbe({ owner: key.caller, request: key, proposal: None, round: 0 }) })))
      | None => (w)
    }
  }

  // Cancellation affects the waiter; submitted work and proof are not undone.

  pure def cancelCallEnabled(w: World, key: RequestKey): bool = {
    val owner = w.system.get(key.caller.node)
    val call = owner.volatile.pendingCalls.get(key)
    and {
      M::running(owner),
      key.caller.incarnation == owner.volatile.incarnation,
      call.phase != Replied,
      not(call.waiterCancelled)
    }
  }

  pure def cancelCallNext(w: World, key: RequestKey): World = {
    val owner = w.system.get(key.caller.node)
    val call = owner.volatile.pendingCalls.get(key)
    (Frames::withProcess(w, key.caller.node, updatedProcess => { ...updatedProcess, pendingCalls: updatedProcess.pendingCalls.put(key,
          Tx::cancelled(call)) }))
  }

}
