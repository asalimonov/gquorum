module metaservice_transaction_steps {
  import authority as A from "../metaengine/authority"
  import choreo as C from "../vendor/choreo"
  import membership as M from "../consensus/membership"
  import reads as R from "./reads"
  import transactions as Tx from "./transactions"
  import types.* from "../types"
  import world_types.* from "../runtime/world"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import message_effects.* from "../runtime/messages"
  import handlers.* from "handlers"

  pure def handleCandidates(w: World, node: NodeName, MAX_DELIVERIES: int): Set[Transition] = {
    val ctx = C::convert_context(w, node)

    main_listener(ctx)
  }

  pure def handleEnabled(w: World, node: NodeName, tr: Transition, MAX_DELIVERIES: int): bool = {
    val ctx = C::convert_context(w, node)


    and {
      w.system.get(node).volatile.phase != Stopped,
      withinDeliveryBound(tr, MAX_DELIVERIES)
    }
  }

  pure def handleNext(w: World, node: NodeName, tr: Transition, MAX_DELIVERIES: int): World = {
    val ctx = C::convert_context(w, node)


    (C::apply_effect(w, node, tr, apply_custom_effect))
  }

  pure def confirmReadEnabled(w: World, msg: Envelope, MAX_DELIVERIES: int): bool = match msg.payload {
    | ReadProbe(ticket) => {
      val source = w.system.get(msg.destination)
      val reply = { delivery: w.extensions.nextDelivery,
        source: { node: msg.destination, incarnation: source.volatile.incarnation },
        destination: ticket.owner.node, channel: MetaChannel,
        payload: ReadAck({ ticket: ticket, index: w.extensions.consensus.get("_meta").committedPosition }) }
      and {
        w.messages.get(msg.destination).contains(msg),
        R::canConfirm(w.extensions, w.system, msg),
        w.extensions.nextDelivery <= MAX_DELIVERIES
      }
    }
    | _ => and {
      false
    }
  }

  pure def confirmReadNext(w: World, msg: Envelope, MAX_DELIVERIES: int): World = match msg.payload {
    | ReadProbe(ticket) => {
      val source = w.system.get(msg.destination)
      val reply = { delivery: w.extensions.nextDelivery,
        source: { node: msg.destination, incarnation: source.volatile.incarnation },
        destination: ticket.owner.node, channel: MetaChannel,
        payload: ReadAck({ ticket: ticket, index: w.extensions.consensus.get("_meta").committedPosition }) }
      (apply_custom_effect(apply_custom_effect(w, Consume(msg)), Queue(reply)))
    }
    | _ => (w)
  }

  pure def finishReadEnabled(w: World, key: RequestKey): bool = {
    val owner = w.system.get(key.caller.node)
    and {
      R::canFinish(owner, key)
    }
  }

  pure def finishReadNext(w: World, key: RequestKey): World = {
    val owner = w.system.get(key.caller.node)
    ({ ...w, system: w.system.put(key.caller.node, R::finish(owner, key)) })
  }

  pure def prepareMutationEnabled(w: World, key: RequestKey, rules: Rules): bool = {
    val owner = w.system.get(key.caller.node)
    val localMeta = w.extensions.metaLog.get(owner.volatile.appliedMetaIndex)
    // Remote admission-health observations are collected by the admission
    // workflow. Unknown evidence cannot authorize readmission here.
    val health = Map(key.caller.node -> Healthy)
    and {
      Tx::canPrepare(owner, key)
    }
  }

  pure def prepareMutationNext(w: World, key: RequestKey, rules: Rules): World = {
    val owner = w.system.get(key.caller.node)
    val localMeta = w.extensions.metaLog.get(owner.volatile.appliedMetaIndex)
    // Remote admission-health observations are collected by the admission
    // workflow. Unknown evidence cannot authorize readmission here.
    val health = Map(key.caller.node -> Healthy)
    ({ ...w, system: w.system.put(key.caller.node, { ...owner,
        volatile: { ...owner.volatile, pendingCalls: owner.volatile.pendingCalls.setBy(key,
          call => Tx::prepare(call, localMeta, health, rules)) } }) })
  }

  pure def submitMutationEnabled(w: World, key: RequestKey, MAX_DELIVERIES: int): bool = {
    val owner = w.system.get(key.caller.node)
    val call = owner.volatile.pendingCalls.get(key)
    val leader = if (owner.volatile.observations.keys().contains("_meta"))
      owner.volatile.observations.get("_meta").observedLeader else None
    match leader {
      | Some(r) => match call.command {
        | Some(cmd) => and {
M::running(owner),
key.caller.incarnation == owner.volatile.incarnation,
(call.phase == PreparedCall or call.phase == Submitted),
not(call.waiterCancelled),
w.extensions.nextDelivery <= MAX_DELIVERIES
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

  pure def submitMutationNext(w: World, key: RequestKey, MAX_DELIVERIES: int): World = {
    val owner = w.system.get(key.caller.node)
    val call = owner.volatile.pendingCalls.get(key)
    val leader = if (owner.volatile.observations.keys().contains("_meta"))
      owner.volatile.observations.get("_meta").observedLeader else None
    match leader {
      | Some(r) => match call.command {
        | Some(cmd) => (apply_custom_effect({ ...w, system: w.system.put(key.caller.node, { ...owner,
              volatile: { ...owner.volatile, pendingCalls: owner.volatile.pendingCalls.put(key,
                { ...call, phase: Submitted }) } }) }, Queue({ delivery: w.extensions.nextDelivery,
              source: key.caller, destination: r.node, channel: MetaChannel,
              payload: MetaProposal({ key: { request: key, sequence: call.proposalSequence },
                coordinator: key.caller, forWork: false, command: cmd }) })))
        | None => (w)
      }
      | None => (w)
    }
  }

  pure def commitMutationEnabled(w: World, msg: Envelope, MAX_POSITION: int, MAX_DELIVERIES: int, rules: Rules): bool = match msg.payload {
    | MetaProposal(proposal) => {
      val c = w.extensions.consensus.get("_meta")
      val voters = match c.leaderAuthority {
        | Some(leader) => M::certificate(c, w.system, w.extensions.network.metaLinks, leader)
        | None => Set()
      }
      val errors = Tx::commandErrors(w.extensions.metadata, proposal.command, rules)
      val meta = if (errors.size() == 0) Tx::applyAt(w.extensions.metadata, proposal.command, c.committedPosition + 1)
        else w.extensions.metadata
      val outcome = if (errors.size() == 0) Tx::commandOutcome(w.extensions.metadata, proposal.command)
        else A::refused(errors)
      val position = c.committedPosition + 1
      val source = w.system.get(msg.destination)
      val reply = { delivery: w.extensions.nextDelivery,
        source: { node: msg.destination, incarnation: source.volatile.incarnation },
        destination: proposal.coordinator.node, channel: MetaChannel,
        payload: MetaAck({ key: proposal.key, coordinator: proposal.coordinator,
          forWork: proposal.forWork, index: position, outcome: outcome }) }
      and {
        w.messages.get(msg.destination).contains(msg),
        proposal.coordinator == msg.source,
        match c.leaderAuthority { | Some(r) => r.node == msg.destination | None => false },
        M::hasCommitPath(c, w.system, w.extensions.network.metaLinks),
        position <= MAX_POSITION,
        w.extensions.nextDelivery <= MAX_DELIVERIES
      }
    }
    | _ => and {
      false
    }
  }

  pure def commitMutationNext(w: World, msg: Envelope, MAX_POSITION: int, MAX_DELIVERIES: int, rules: Rules): World = match msg.payload {
    | MetaProposal(proposal) => {
      val c = w.extensions.consensus.get("_meta")
      val voters = match c.leaderAuthority {
        | Some(leader) => M::certificate(c, w.system, w.extensions.network.metaLinks, leader)
        | None => Set()
      }
      val errors = Tx::commandErrors(w.extensions.metadata, proposal.command, rules)
      val meta = if (errors.size() == 0) Tx::applyAt(w.extensions.metadata, proposal.command, c.committedPosition + 1)
        else w.extensions.metadata
      val outcome = if (errors.size() == 0) Tx::commandOutcome(w.extensions.metadata, proposal.command)
        else A::refused(errors)
      val position = c.committedPosition + 1
      val source = w.system.get(msg.destination)
      val reply = { delivery: w.extensions.nextDelivery,
        source: { node: msg.destination, incarnation: source.volatile.incarnation },
        destination: proposal.coordinator.node, channel: MetaChannel,
        payload: MetaAck({ key: proposal.key, coordinator: proposal.coordinator,
          forWork: proposal.forWork, index: position, outcome: outcome }) }
      (apply_custom_effect(apply_custom_effect({ ...w,
          system: M::persistCertificate(w.system, voters, position),
          extensions: { ...w.extensions, metadata: meta,
            consensus: w.extensions.consensus.put("_meta", M::committed(c, voters, position)),
            metaLog: w.extensions.metaLog.put(position, meta) } }, Consume(msg)), Queue(reply)))
    }
    | _ => (w)
  }

  pure def finishMutationEnabled(w: World, key: RequestKey): bool = {
    val owner = w.system.get(key.caller.node)
    and {
      Tx::canFinish(owner, key)
    }
  }

  pure def finishMutationNext(w: World, key: RequestKey): World = {
    val owner = w.system.get(key.caller.node)
    ({ ...w, system: w.system.put(key.caller.node, { ...owner,
        volatile: { ...owner.volatile, pendingCalls: owner.volatile.pendingCalls.setBy(key, call => Tx::finish(call)) } }) })
  }

  pure def beginReseatReadEnabled(w: World, key: RequestKey, MAX_DELIVERIES: int): bool = {
    val owner = w.system.get(key.caller.node)
    val call = owner.volatile.pendingCalls.get(key)
    val leader = if (owner.volatile.observations.keys().contains("_meta"))
      owner.volatile.observations.get("_meta").observedLeader else None
    match leader {
      | Some(r) => and {
M::running(owner),
key.caller.incarnation == owner.volatile.incarnation,
isReseat(call.request),
call.phase == PreparingCall,
call.readIndex == None,
match call.request { | Mutate(m) => not(m.options.preview) | _ => false },
not(call.waiterCancelled),
w.extensions.nextDelivery <= MAX_DELIVERIES
}
      | None => and {
        false
      }
    }
  }

  pure def beginReseatReadNext(w: World, key: RequestKey, MAX_DELIVERIES: int): World = {
    val owner = w.system.get(key.caller.node)
    val call = owner.volatile.pendingCalls.get(key)
    val leader = if (owner.volatile.observations.keys().contains("_meta"))
      owner.volatile.observations.get("_meta").observedLeader else None
    match leader {
      | Some(r) => (apply_custom_effect({ ...w, system: w.system.put(key.caller.node, { ...owner,
            volatile: { ...owner.volatile, pendingCalls: owner.volatile.pendingCalls.put(key, { ...call, phase: Submitted }) } }) },
            Queue({ delivery: w.extensions.nextDelivery, source: key.caller, destination: r.node,
              channel: MetaChannel, payload: ReadProbe({ owner: key.caller, request: key, proposal: None, round: 0 }) })))
      | None => (w)
    }
  }

  pure def finishReseatReadEnabled(w: World, key: RequestKey): bool = {
    val owner = w.system.get(key.caller.node)
    val call = owner.volatile.pendingCalls.get(key)
    and {
      M::running(owner),
      key.caller.incarnation == owner.volatile.incarnation,
      isReseat(call.request),
      call.phase == AwaitingReadApply,
      not(call.waiterCancelled),
      match call.readIndex { | Some(index) => owner.volatile.appliedMetaIndex >= index | None => false }
    }
  }

  pure def finishReseatReadNext(w: World, key: RequestKey): World = {
    val owner = w.system.get(key.caller.node)
    val call = owner.volatile.pendingCalls.get(key)
    ({ ...w, system: w.system.put(key.caller.node, { ...owner, volatile: { ...owner.volatile,
        pendingCalls: owner.volatile.pendingCalls.put(key, { ...call, phase: PreparingCall,
          request: match call.request { | Mutate(m) => Mutate(A::capture(m, owner.volatile.localView.latest.version, key.caller.node))
            | _ => call.request } }) } }) })
  }
}
