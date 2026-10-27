module cluster {
  import initial as I from "./initial"
  import membership as M from "./membership"
  import scope as S from "./scope"
  import reads as R from "./reads"
  import transactions as Tx from "./transactions"
  import authority as A from "./authority"
  import operations as O from "./operations"
  import types.* from "./types"
  import handlers.* from "./handlers"
  import basicSpells.* from "./vendor/spells/basicSpells"
  import choreo as C from "./vendor/choreo"

  const NODE_POOL: Set[NodeName]
  const MAX_REQUESTS: int
  const FOUNDERS: NodeName -> int
  const LOAD_IDS: Set[GroupId]
  const INITIAL_LEADER: NodeName
  const MAX_POSITION: int
  const MAX_INCARNATION: int
  const MAX_DELIVERIES: int
  var world: World
  pure val modelBounds = Map("nodeCount" -> NODE_POOL.size(), "maxRequests" -> MAX_REQUESTS,
    "founderCount" -> FOUNDERS.keys().size(), "loadCount" -> LOAD_IDS.size(),
    "maxPosition" -> MAX_POSITION, "maxIncarnation" -> MAX_INCARNATION,
    "maxDeliveries" -> MAX_DELIVERIES)
  pure val rules: Rules = { maxNodes: NODE_POOL.size(), maxReplicas: 5,
    concurrentGroups: 4, concurrentCleanup: 2,
    nodeOrder: List("n1", "n2", "n3", "n4", "n5").select(n => NODE_POOL.contains(n)),
    groupOrder: List("_meta", "a", "b", "c"),
  }
  pure val generatedOptions: MutationOptions = { expectedVersion: 0,
    audit: { actor: "", comment: "generated", forced: None }, preview: false, approve: false }

  action init = world' = I::worldFor(NODE_POOL, FOUNDERS, LOAD_IDS, INITIAL_LEADER)

  action invoke(node: NodeName, request: ApiRequest): bool = {
    val owner = world.system.get(node)
    val sequence = owner.volatile.pendingCalls.keys().size() + 1
    val caller = { node: node, incarnation: owner.volatile.incarnation }
    val key = { caller: caller, sequence: sequence }
    val call = Tx::newCall(request, owner.volatile.localView.latest.version, node)
    val msg = { delivery: world.extensions.nextDelivery, source: caller,
                destination: node, channel: MetaChannel,
                payload: ApiCall({ key: key, request: call.request }) }
    all {
      S::requestInScope(owner.volatile.localView.latest, request),
      sequence <= MAX_REQUESTS,
      owner.volatile.phase != Stopped and owner.volatile.phase != Failed,
      world.extensions.nextDelivery <= MAX_DELIVERIES,
      world' = { ...world,
        system: world.system.put(node, { ...owner,
          volatile: { ...owner.volatile, pendingCalls: owner.volatile.pendingCalls.put(key,
            call) } }),
        extensions: { ...world.extensions,
          network: { ...world.extensions.network,
            pending: world.extensions.network.pending.union(Set(msg)) },
          nextDelivery: world.extensions.nextDelivery + 1 },
      },
    }
  }

  action deliver(msg: Envelope): bool = all {
    world.extensions.network.pending.contains(msg),
    (if (msg.channel == MetaChannel) world.extensions.network.metaLinks
     else world.extensions.network.loadLinks).contains((msg.source.node, msg.destination)),
    world.system.get(msg.destination).volatile.phase != Stopped,
    world' = { ...world,
      messages: world.messages.setBy(msg.destination, inbox => inbox.union(Set(msg))),
      extensions: { ...world.extensions,
        network: { ...world.extensions.network,
          pending: world.extensions.network.pending.exclude(Set(msg)),
          delivered: world.extensions.network.delivered.union(Set(msg.delivery)) } },
    },
  }

  action handle(node: NodeName): bool = {
    val ctx = C::convert_context(world, node)
    nondet tr = main_listener(ctx).oneOf()
    all {
      world.system.get(node).volatile.phase != Stopped,
      withinDeliveryBound(tr, MAX_DELIVERIES),
      world' = C::apply_effect(world, node, tr, apply_custom_effect),
    }
  }

  action confirmRead(msg: Envelope): bool = match msg.payload {
    | ReadProbe(ticket) => {
      val source = world.system.get(msg.destination)
      val reply = { delivery: world.extensions.nextDelivery,
        source: { node: msg.destination, incarnation: source.volatile.incarnation },
        destination: ticket.owner.node, channel: MetaChannel,
        payload: ReadAck({ ticket: ticket, index: world.extensions.consensus.get("_meta").committedPosition }) }
      all {
        world.messages.get(msg.destination).contains(msg),
        R::canConfirm(world.extensions, world.system, msg),
        world.extensions.nextDelivery <= MAX_DELIVERIES,
        world' = apply_custom_effect(apply_custom_effect(world, Consume(msg)), Queue(reply)),
      }
    }
    | _ => all { false, world' = world }
  }

  action finishRead(key: RequestKey): bool = {
    val owner = world.system.get(key.caller.node)
    all {
      R::canFinish(owner, key),
      world' = { ...world, system: world.system.put(key.caller.node, R::finish(owner, key)) },
    }
  }

  action prepareMutation(key: RequestKey): bool = {
    val owner = world.system.get(key.caller.node)
    val localMeta = world.extensions.metaLog.get(owner.volatile.appliedMetaIndex)
    // Remote admission-health observations are collected by the admission
    // workflow. Unknown evidence cannot authorize readmission here.
    val health = Map(key.caller.node -> Healthy)
    all {
      Tx::canPrepare(owner, key),
      world' = { ...world, system: world.system.put(key.caller.node, { ...owner,
        volatile: { ...owner.volatile, pendingCalls: owner.volatile.pendingCalls.setBy(key,
          call => Tx::prepare(call, localMeta, health, rules)) } }) },
    }
  }

  action submitMutation(key: RequestKey): bool = {
    val owner = world.system.get(key.caller.node)
    val call = owner.volatile.pendingCalls.get(key)
    val leader = if (owner.volatile.observations.keys().contains("_meta"))
      owner.volatile.observations.get("_meta").observedLeader else None
    match leader {
      | Some(r) => match call.command {
        | Some(cmd) => all {
            M::running(owner), key.caller.incarnation == owner.volatile.incarnation,
            (call.phase == PreparedCall or call.phase == Submitted), not(call.waiterCancelled),
            world.extensions.nextDelivery <= MAX_DELIVERIES,
            world' = apply_custom_effect({ ...world, system: world.system.put(key.caller.node, { ...owner,
              volatile: { ...owner.volatile, pendingCalls: owner.volatile.pendingCalls.put(key,
                { ...call, phase: Submitted }) } }) }, Queue({ delivery: world.extensions.nextDelivery,
              source: key.caller, destination: r.node, channel: MetaChannel,
              payload: MetaProposal({ key: { request: key, sequence: call.proposalSequence },
                coordinator: key.caller, forWork: false, command: cmd }) })),
          }
        | None => all { false, world' = world }
      }
      | None => all { false, world' = world }
    }
  }

  action commitMutation(msg: Envelope): bool = match msg.payload {
    | MetaProposal(proposal) => {
      val c = world.extensions.consensus.get("_meta")
      val voters = match c.leaderAuthority {
        | Some(leader) => M::certificate(c, world.system, world.extensions.network.metaLinks, leader)
        | None => Set()
      }
      val errors = Tx::commandErrors(world.extensions.metadata, proposal.command, rules)
      val meta = if (errors.size() == 0) Tx::applyCommand(world.extensions.metadata, proposal.command)
        else world.extensions.metadata
      val outcome = if (errors.size() == 0) Tx::commandOutcome(world.extensions.metadata, proposal.command)
        else A::refused(errors)
      val position = c.committedPosition + 1
      val source = world.system.get(msg.destination)
      val reply = { delivery: world.extensions.nextDelivery,
        source: { node: msg.destination, incarnation: source.volatile.incarnation },
        destination: proposal.coordinator.node, channel: MetaChannel,
        payload: MetaAck({ key: proposal.key, coordinator: proposal.coordinator,
          forWork: proposal.forWork, index: position, outcome: outcome }) }
      all {
        world.messages.get(msg.destination).contains(msg),
        proposal.coordinator == msg.source,
        match c.leaderAuthority { | Some(r) => r.node == msg.destination | None => false },
        M::hasCommitPath(c, world.system, world.extensions.network.metaLinks),
        position <= MAX_POSITION, world.extensions.nextDelivery <= MAX_DELIVERIES,
        world' = apply_custom_effect(apply_custom_effect({ ...world,
          system: M::persistCertificate(world.system, voters, position),
          extensions: { ...world.extensions, metadata: meta,
            consensus: world.extensions.consensus.put("_meta", M::committed(c, voters, position)),
            metaLog: world.extensions.metaLog.put(position, meta) } }, Consume(msg)), Queue(reply)),
      }
    }
    | _ => all { false, world' = world }
  }

  action finishMutation(key: RequestKey): bool = {
    val owner = world.system.get(key.caller.node)
    all {
      Tx::canFinish(owner, key),
      world' = { ...world, system: world.system.put(key.caller.node, { ...owner,
        volatile: { ...owner.volatile, pendingCalls: owner.volatile.pendingCalls.setBy(key, call => Tx::finish(call)) } }) },
    }
  }

  action beginReseatRead(key: RequestKey): bool = {
    val owner = world.system.get(key.caller.node)
    val call = owner.volatile.pendingCalls.get(key)
    val leader = if (owner.volatile.observations.keys().contains("_meta"))
      owner.volatile.observations.get("_meta").observedLeader else None
    match leader {
      | Some(r) => all {
          M::running(owner), key.caller.incarnation == owner.volatile.incarnation,
          isReseat(call.request), call.phase == PreparingCall, call.readIndex == None,
          match call.request { | Mutate(m) => not(m.options.preview) | _ => false },
          not(call.waiterCancelled), world.extensions.nextDelivery <= MAX_DELIVERIES,
          world' = apply_custom_effect({ ...world, system: world.system.put(key.caller.node, { ...owner,
            volatile: { ...owner.volatile, pendingCalls: owner.volatile.pendingCalls.put(key, { ...call, phase: Submitted }) } }) },
            Queue({ delivery: world.extensions.nextDelivery, source: key.caller, destination: r.node,
              channel: MetaChannel, payload: ReadProbe({ owner: key.caller, request: key, proposal: None, round: 0 }) })),
        }
      | None => all { false, world' = world }
    }
  }

  action finishReseatRead(key: RequestKey): bool = {
    val owner = world.system.get(key.caller.node)
    val call = owner.volatile.pendingCalls.get(key)
    all {
      M::running(owner), key.caller.incarnation == owner.volatile.incarnation,
      isReseat(call.request), call.phase == AwaitingReadApply, not(call.waiterCancelled),
      match call.readIndex { | Some(index) => owner.volatile.appliedMetaIndex >= index | None => false },
      world' = { ...world, system: world.system.put(key.caller.node, { ...owner, volatile: { ...owner.volatile,
        pendingCalls: owner.volatile.pendingCalls.put(key, { ...call, phase: PreparingCall,
          request: match call.request { | Mutate(m) => Mutate(A::capture(m, owner.volatile.localView.latest.version, key.caller.node))
            | _ => call.request } }) } }) },
    }
  }

  action dispatchOperation(key: RequestKey): bool = {
    val owner = world.system.get(key.caller.node)
    val call = owner.volatile.pendingCalls.get(key)
    val leader = if (owner.volatile.observations.keys().contains("_meta"))
      owner.volatile.observations.get("_meta").observedLeader else None
    match Tx::operationToSend(call) {
      | Some(request) => match leader {
          | Some(r) => {
            val updated = Tx::awaitingOperation(call, request)
            all {
              M::running(owner), key.caller.incarnation == owner.volatile.incarnation,
              not(call.waiterCancelled), world.extensions.nextDelivery <= MAX_DELIVERIES,
              world' = apply_custom_effect({ ...world, system: world.system.put(key.caller.node, { ...owner,
                volatile: { ...owner.volatile, pendingCalls: owner.volatile.pendingCalls.put(key, updated) } }) },
                Queue({ delivery: world.extensions.nextDelivery, source: key.caller, destination: r.node,
                  channel: MetaChannel, payload: OperationCall({
                    key: { request: key, sequence: updated.proposalSequence }, request: request }) })),
            }
          }
          | None => all { false, world' = world }
        }
      | None => all { false, world' = world }
    }
  }

  action transmitOperationRead(node: NodeName, key: ProposalKey, retry: bool): bool = {
    val owner = world.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    val process = { node: node, incarnation: owner.volatile.incarnation }
    all {
      M::running(owner), work.phase == (if (retry) WorkReadQueued else WorkReading),
      world.extensions.nextDelivery <= MAX_DELIVERIES,
      world' = apply_custom_effect({ ...world, system: world.system.put(node, { ...owner, volatile: { ...owner.volatile,
        operationWork: owner.volatile.operationWork.put(key, { ...work, phase: WorkReadQueued }) } }) },
        Queue({ delivery: world.extensions.nextDelivery,
        source: process, destination: node, channel: MetaChannel,
        payload: ReadProbe({ owner: process, request: key.request, proposal: Some(key.sequence), round: work.readRound }) })),
    }
  }
  action startOperationRead(node: NodeName, key: ProposalKey): bool = transmitOperationRead(node, key, false)
  action retryOperationRead(node: NodeName, key: ProposalKey): bool = transmitOperationRead(node, key, true)

  action captureOperation(node: NodeName, key: ProposalKey): bool = {
    val owner = world.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    val localMeta = world.extensions.metaLog.get(owner.volatile.appliedMetaIndex)
    all {
      M::running(owner), work.phase == WorkApplying,
      match work.readIndex { | Some(index) => owner.volatile.appliedMetaIndex >= index | None => false },
      world' = { ...world, system: world.system.put(node, { ...owner, volatile: { ...owner.volatile,
        operationWork: owner.volatile.operationWork.put(key,
          if (work.captured == None) O::capture(work, localMeta, rules) else O::recapture(work, localMeta, rules)) } }) },
    }
  }

  action observeOperation(node: NodeName, key: ProposalKey, group: GroupId): bool = {
    val owner = world.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    val links = M::linksFor(world.extensions.network, group)
    val observed = if (not(world.extensions.consensus.keys().contains(group))) None else {
      val native = world.extensions.consensus.get(group)
      val responders = native.voters.union(native.learners).filter(r =>
        M::readableIdentity(world.system, r) and M::communicates(links, node, r.node) and
          native.catchUp.getOrElse(r, CatchUpUnknown) == CaughtUp and
          match native.leaderAuthority { | Some(leader) => M::communicates(links, r.node, leader.node) | None => false })
      if (responders.size() == 0 or not(M::hasCommitPath(native, world.system, links))) None
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
    all {
      M::running(owner), work.phase == WorkObserving, O::requiredReads(work).contains(group),
      not(work.queried.contains(group)),
      world' = { ...world, system: world.system.put(node, { ...owner, volatile: { ...owner.volatile,
        operationWork: owner.volatile.operationWork.put(key, updated) } }) },
    }
  }

  action observeOperationHealth(node: NodeName, key: ProposalKey, group: GroupId): bool = {
    val owner = world.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    match work.input {
      | Some(input) => if (not(input.observations.keys().contains(group))) all { false, world' = world } else {
          val prior = input.observations.get(group)
          val links = M::linksFor(world.extensions.network, group)
          val live = prior.native.voters.union(prior.native.learners).filter(r =>
            M::liveIdentity(world.system, r) and M::communicates(links, node, r.node))
          val observed = { ...prior, healthKnown: true, live: live,
            ready: live.filter(r => if (world.extensions.consensus.keys().contains(group))
              world.extensions.consensus.get(group).catchUp.getOrElse(r, CatchUpUnknown) == CaughtUp else false) }
          all {
            M::running(owner), work.phase == WorkObserving, not(prior.healthKnown),
            world' = { ...world, system: world.system.put(node, { ...owner, volatile: { ...owner.volatile,
              operationWork: owner.volatile.operationWork.put(key, { ...work,
                input: Some({ ...input, observations: input.observations.put(group, observed) }) }) } }) },
          }
        }
      | None => all { false, world' = world }
    }
  }

  action beginOperationRecapture(node: NodeName, key: ProposalKey): bool = {
    val owner = world.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    all { M::running(owner), O::canRecapture(work),
      world' = { ...world, system: world.system.put(node, { ...owner, volatile: { ...owner.volatile,
        operationWork: owner.volatile.operationWork.put(key, { ...work, phase: WorkReading, readIndex: None, readRound: work.readRound + 1 }) } }) },
    }
  }

  action buildOperation(node: NodeName, key: ProposalKey): bool = {
    val owner = world.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    all { M::running(owner), O::canBuild(work),
      world' = { ...world, system: world.system.put(node, { ...owner, volatile: { ...owner.volatile,
        operationWork: owner.volatile.operationWork.put(key, O::build(work)) } }) },
    }
  }

  action submitOperation(node: NodeName, key: ProposalKey): bool = {
    val owner = world.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    match work.command {
      | Some(command) => all {
          M::running(owner), work.phase == WorkPrepared or work.phase == WorkSubmitted,
          match world.extensions.consensus.get("_meta").leaderAuthority { | Some(r) => r.node == node | None => false },
          world.extensions.nextDelivery <= MAX_DELIVERIES,
          world' = apply_custom_effect({ ...world, system: world.system.put(node, { ...owner, volatile: { ...owner.volatile,
            operationWork: owner.volatile.operationWork.put(key, { ...work, phase: WorkSubmitted }) } }) },
            Queue({ delivery: world.extensions.nextDelivery, source: { node: node, incarnation: owner.volatile.incarnation },
              destination: node, channel: MetaChannel, payload: MetaProposal({ key: key,
                coordinator: { node: node, incarnation: owner.volatile.incarnation }, forWork: true, command: command }) })),
        }
      | None => all { false, world' = world }
    }
  }

  action finishOperation(node: NodeName, key: ProposalKey): bool = {
    val owner = world.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    all {
      M::running(owner), work.phase == WorkCommitted, work.result != None,
      match work.commitIndex { | Some(index) => owner.volatile.appliedMetaIndex >= index | None => false },
      world' = { ...world, system: world.system.put(node, { ...owner, volatile: { ...owner.volatile,
        operationWork: owner.volatile.operationWork.put(key, { ...work, phase: WorkFinished, replySent: false }) } }) },
    }
  }

  action replyOperation(node: NodeName, key: ProposalKey): bool = {
    val owner = world.system.get(node)
    val work = owner.volatile.operationWork.get(key)
    match work.result {
      | Some(result) => all {
          M::running(owner), work.phase == WorkFinished, not(work.replySent),
          world.extensions.nextDelivery <= MAX_DELIVERIES,
          world' = apply_custom_effect({ ...world, system: world.system.put(node, { ...owner, volatile: { ...owner.volatile,
            operationWork: owner.volatile.operationWork.put(key, { ...work, replySent: true }) } }) },
            Queue({ delivery: world.extensions.nextDelivery, source: { node: node, incarnation: owner.volatile.incarnation },
              destination: key.request.caller.node, channel: MetaChannel, payload: OperationAck({ key: key, outcome: result }) })),
        }
      | None => all { false, world' = world }
    }
  }

  // An explicit successful leader-observation exchange. Cached routing is not
  // silently refreshed by either an election or a metadata commit.
  action observeMetaLeader(node: NodeName): bool = {
    val owner = world.system.get(node)
    val c = world.extensions.consensus.get("_meta")
    match c.leaderAuthority {
      | Some(leader) => all {
          M::running(owner), owner.volatile.observations.keys().contains("_meta"),
          M::liveIdentity(world.system, leader), M::communicates(world.extensions.network.metaLinks, node, leader.node),
          owner.volatile.observations.get("_meta").observedLeader != Some(leader) or
            owner.volatile.observations.get("_meta").observedConfig != c.configRevision,
          world' = { ...world, system: world.system.put(node, { ...owner, volatile: { ...owner.volatile,
            observations: owner.volatile.observations.setBy("_meta", o =>
              { ...o, observedLeader: Some(leader), observedConfig: c.configRevision }) } }) },
        }
      | None => all { false, world' = world }
    }
  }

  action retryRead(key: RequestKey): bool = {
    val owner = world.system.get(key.caller.node)
    val call = owner.volatile.pendingCalls.get(key)
    val leader = if (owner.volatile.observations.keys().contains("_meta"))
      owner.volatile.observations.get("_meta").observedLeader else None
    match leader {
      | Some(r) => all {
          M::running(owner), key.caller.incarnation == owner.volatile.incarnation,
          (call.request == ConfirmedRead or isReseat(call.request)), call.phase == Submitted, not(call.waiterCancelled),
          world.extensions.nextDelivery <= MAX_DELIVERIES,
          world' = apply_custom_effect(world, Queue({ delivery: world.extensions.nextDelivery,
            source: key.caller, destination: r.node, channel: MetaChannel, payload: ReadProbe({ owner: key.caller, request: key, proposal: None, round: 0 }) })),
        }
      | None => all { false, world' = world }
    }
  }

  // Cancellation affects the waiter; submitted work and proof are not undone.
  action cancelCall(key: RequestKey): bool = {
    val owner = world.system.get(key.caller.node)
    val call = owner.volatile.pendingCalls.get(key)
    all {
      M::running(owner), key.caller.incarnation == owner.volatile.incarnation,
      call.phase != Replied, not(call.waiterCancelled),
      world' = { ...world, system: world.system.put(key.caller.node, { ...owner,
        volatile: { ...owner.volatile, pendingCalls: owner.volatile.pendingCalls.put(key,
          Tx::cancelled(call)) } }) },
    }
  }

  action drop(msg: Envelope): bool = all {
    world.extensions.network.pending.contains(msg),
    world' = { ...world, extensions: { ...world.extensions,
      network: { ...world.extensions.network,
        pending: world.extensions.network.pending.exclude(Set(msg)) } } },
  }

  action duplicate(msg: Envelope): bool = all {
    world.extensions.network.pending.contains(msg),
    world.extensions.nextDelivery <= MAX_DELIVERIES,
    world' = { ...world, extensions: { ...world.extensions,
      nextDelivery: world.extensions.nextDelivery + 1,
      network: { ...world.extensions.network, pending: world.extensions.network.pending.union(
        Set({ ...msg, delivery: world.extensions.nextDelivery })) } } },
  }

  action setLink(channel: Channel, source: NodeName, destination: NodeName, up: bool): bool = {
    val net = world.extensions.network
    val links = if (channel == MetaChannel) net.metaLinks else net.loadLinks
    val edge = (source, destination)
    val changed = if (up) links.union(Set(edge)) else links.exclude(Set(edge))
    all {
      source != destination, links.contains(edge) != up,
      world' = { ...world, extensions: { ...world.extensions,
        network: if (channel == MetaChannel) { ...net, metaLinks: changed }
                 else { ...net, loadLinks: changed } } },
    }
  }

  action crash(node: NodeName): bool = {
    val owner = world.system.get(node)
    all {
      M::running(owner),
      world' = { ...world,
        system: world.system.put(node, { ...owner, volatile: { ...owner.volatile,
          phase: Stopped, pendingCalls: Map(), owners: Map(), observations: Map(), operationWork: Map(),
          session: None, schedulerCapture: None, appliedMetaIndex: 0, appliedMetaVersion: 0,
          localView: { latest: I::emptyDocument, target: I::emptyDocument } } }),
        messages: world.messages.put(node, Set()),
      },
    }
  }

  action restart(node: NodeName): bool = {
    val owner = world.system.get(node)
    val view = match owner.durable.metaImage {
      | Some(image) => image
      | None => { latest: I::emptyDocument, target: I::emptyDocument }
    }
    all {
      owner.volatile.phase == Stopped,
      owner.volatile.incarnation < MAX_INCARNATION,
      world' = { ...world, system: world.system.put(node, { ...owner,
        volatile: { ...owner.volatile, phase: Starting,
          incarnation: owner.volatile.incarnation + 1,
          localView: view,
          appliedMetaIndex: owner.durable.metaIndex,
          appliedMetaVersion: view.latest.version } }) },
    }
  }

  // TODO(not planned): application business semantics. A bounded committed
  // position witnesses independent consensus, not a business command result.
  action commitProgress(group: GroupId): bool = {
    val c = world.extensions.consensus.get(group)
    val links = M::linksFor(world.extensions.network, group)
    val voters = match c.leaderAuthority {
      | Some(leader) => M::certificate(c, world.system, links, leader)
      | None => Set()
    }
    val position = c.committedPosition + 1
    all {
      M::hasCommitPath(c, world.system, links), position <= MAX_POSITION,
      world' = { ...world,
        system: M::persistCertificate(world.system, voters, position),
        extensions: { ...world.extensions,
          consensus: world.extensions.consensus.put(group, M::committed(c, voters, position)),
          metaLog: if (group == "_meta") world.extensions.metaLog.put(position, world.extensions.metadata)
                   else world.extensions.metaLog },
      },
    }
  }

  action elect(group: GroupId, candidate: ReplicaKey): bool = {
    val c = world.extensions.consensus.get(group)
    val valid = M::liveIdentity(world.system, candidate)
    val retained = if (valid) match world.system.get(candidate.node).durable.replicaEvidence.get(candidate) {
      | ValidStore(p) => p.commit >= c.committedPosition
      | _ => false
    } else false
    all {
      valid, retained, c.leaderAuthority != Some(candidate),
      M::certificate(c, world.system, M::linksFor(world.extensions.network, group), candidate).size() * 2 > c.voters.size(),
      world' = { ...world, extensions: { ...world.extensions,
        consensus: world.extensions.consensus.put(group, { ...c, leaderAuthority: Some(candidate) }) } },
    }
  }

  action catchUp(group: GroupId, replica: ReplicaKey): bool = {
    val c = world.extensions.consensus.get(group)
    val valid = M::liveIdentity(world.system, replica)
    val source = match c.leaderAuthority {
      | Some(leader) => M::liveIdentity(world.system, leader) and
        M::communicates(M::linksFor(world.extensions.network, group), leader.node, replica.node)
      | None => false
    }
    val node = world.system.get(replica.node)
    val evidence = node.durable.replicaEvidence.getOrElse(replica, MissingStore)
    val lagging = match evidence {
      | ValidStore(p) => p.commit < c.committedPosition
      | _ => false
    }
    all {
      valid, source, lagging, c.voters.union(c.learners).contains(replica),
      world' = { ...world, system: world.system.put(replica.node, { ...node,
        durable: { ...node.durable, replicaEvidence: node.durable.replicaEvidence.put(replica,
          ValidStore({ snapshot: 0, commit: c.committedPosition, tail: c.committedPosition,
                       appliedHint: 0 })) } }),
        extensions: { ...world.extensions,
          consensus: world.extensions.consensus.put(group, { ...c,
            catchUp: c.catchUp.put(replica, CaughtUp) }) },
      },
    }
  }

  action applyMeta(node: NodeName): bool = {
    val owner = world.system.get(node)
    val replicas = world.extensions.consensus.get("_meta").voters.union(
      world.extensions.consensus.get("_meta").learners).filter(r => r.node == node)
    nondet replica = replicas.oneOf()
    val position = match owner.durable.replicaEvidence.getOrElse(replica, MissingStore) {
      | ValidStore(p) => p.commit
      | _ => 0
    }
    val image = world.extensions.metaLog.getOrElse(position, world.extensions.metadata)
    val view = { latest: image.latest, target: image.target }
    all {
      M::liveIdentity(world.system, replica), position > owner.volatile.appliedMetaIndex,
      world.extensions.metaLog.keys().contains(position),
      world' = { ...world, system: world.system.put(node, { ...owner,
        volatile: { ...owner.volatile, appliedMetaIndex: position,
          appliedMetaVersion: image.latest.version, localView: view },
        durable: { ...owner.durable, metaIndex: position, metaImage: Some(view) } }),
        extensions: { ...world.extensions, consensus: world.extensions.consensus.setBy("_meta", c =>
          { ...c, appliedPositions: c.appliedPositions.put(replica, position) }) },
      },
    }
  }

  // This subset is useful for reachability checks under healthy communication.
  // It remains part of the same production step relation, with the same state.
  action requestStep = any {
    nondet node = NODE_POOL.oneOf()
    nondet request = (if (FOUNDERS.keys().size() == 0) Set(ObserveLocal, InvalidRequest)
      else Set(ObserveLocal, InvalidRequest, ConfirmedRead)).oneOf()
    invoke(node, request),
    nondet node = FOUNDERS.keys().oneOf()
    nondet group = LOAD_IDS.oneOf()
    nondet edit = Set(DeleteGroup(group), DrainNode(node)).oneOf()
    nondet approve = Set(false, true).oneOf()
    nondet preview = Set(false, true).oneOf()
    invoke(node, Mutate({ edit: edit, options: { ...generatedOptions, approve: approve, preview: preview } })),
    nondet node = FOUNDERS.keys().oneOf()
    val approved = world.system.get(node).volatile.localView.target
    nondet group = approved.groups.keys().exclude(Set("_meta")).oneOf()
    nondet replica = approved.groups.get(group).replicas.oneOf()
    invoke(node, Mutate({ edit: ReseatReplica(replica.key), options: generatedOptions })),
    nondet node = FOUNDERS.keys().oneOf()
    val latest = world.system.get(node).volatile.localView.latest
    nondet removed = latest.groups.keys().exclude(Set("_meta")).powerset().filter(groups => groups.size() > 0).oneOf()
    invoke(node, Mutate({ edit: StageDocument({ ...latest, version: latest.version + 1, parent: latest.version,
      groups: latest.groups.keys().exclude(removed).mapBy(g => latest.groups.get(g)) }),
      options: { ...generatedOptions, expectedVersion: latest.version } })),
    nondet node = FOUNDERS.keys().oneOf()
    nondet old = world.system.get(node).volatile.pendingCalls.values().filter(c => match c.request {
      | Mutate(m) => m.options.expectedVersion > 0 | _ => false }).oneOf()
    invoke(node, old.request),
    nondet node = FOUNDERS.keys().oneOf()
    invoke(node, Operate(ApproveVersion(world.system.get(node).volatile.localView.latest.version))),
    nondet node = NODE_POOL.filter(n => M::running(world.system.get(n))).oneOf()
    nondet previous = world.extensions.metaLog.get(world.system.get(node).volatile.appliedMetaIndex).operations.keys().oneOf()
    invoke(node, Operate(RetryAttempt(previous))),
  }

  action protocolStep = any {
    nondet msg = world.extensions.network.pending.oneOf()
    deliver(msg),
    nondet node = NODE_POOL.oneOf()
    handle(node),
    nondet node = NODE_POOL.oneOf()
    nondet key = world.system.get(node).volatile.pendingCalls.keys().oneOf()
    prepareMutation(key),
    nondet node = NODE_POOL.oneOf()
    nondet key = world.system.get(node).volatile.pendingCalls.keys().filter(k =>
      world.system.get(node).volatile.pendingCalls.get(k).phase == PreparedCall).oneOf()
    submitMutation(key),
    nondet node = NODE_POOL.oneOf()
    nondet msg = world.messages.get(node).oneOf()
    commitMutation(msg),
    nondet node = NODE_POOL.oneOf()
    nondet key = world.system.get(node).volatile.pendingCalls.keys().oneOf()
    finishMutation(key),
    nondet node = NODE_POOL.oneOf()
    nondet key = world.system.get(node).volatile.pendingCalls.keys().oneOf()
    beginReseatRead(key),
    nondet node = NODE_POOL.oneOf()
    nondet key = world.system.get(node).volatile.pendingCalls.keys().oneOf()
    finishReseatRead(key),
    nondet node = NODE_POOL.oneOf()
    nondet key = world.system.get(node).volatile.pendingCalls.keys().filter(k =>
      world.system.get(node).volatile.pendingCalls.get(k).phase != WaitingOperation).oneOf()
    dispatchOperation(key),
    nondet node = NODE_POOL.oneOf()
    nondet key = world.system.get(node).volatile.operationWork.keys().oneOf()
    startOperationRead(node, key),
    nondet node = NODE_POOL.oneOf()
    nondet key = world.system.get(node).volatile.operationWork.keys().oneOf()
    captureOperation(node, key),
    nondet node = NODE_POOL.oneOf()
    nondet key = world.system.get(node).volatile.operationWork.keys().oneOf()
    nondet group = O::requiredReads(world.system.get(node).volatile.operationWork.get(key)).oneOf()
    observeOperation(node, key, group),
    nondet node = NODE_POOL.oneOf()
    nondet key = world.system.get(node).volatile.operationWork.keys().oneOf()
    nondet group = O::requiredReads(world.system.get(node).volatile.operationWork.get(key)).oneOf()
    observeOperationHealth(node, key, group),
    nondet node = NODE_POOL.oneOf()
    nondet key = world.system.get(node).volatile.operationWork.keys().oneOf()
    beginOperationRecapture(node, key),
    nondet node = NODE_POOL.oneOf()
    nondet key = world.system.get(node).volatile.operationWork.keys().oneOf()
    buildOperation(node, key),
    nondet node = NODE_POOL.oneOf()
    nondet key = world.system.get(node).volatile.operationWork.keys().filter(k =>
      world.system.get(node).volatile.operationWork.get(k).phase == WorkPrepared).oneOf()
    submitOperation(node, key),
    nondet node = NODE_POOL.oneOf()
    nondet key = world.system.get(node).volatile.operationWork.keys().oneOf()
    finishOperation(node, key),
    nondet node = NODE_POOL.oneOf()
    nondet key = world.system.get(node).volatile.operationWork.keys().oneOf()
    replyOperation(node, key),
    nondet node = FOUNDERS.keys().oneOf()
    observeMetaLeader(node),
    nondet node = NODE_POOL.oneOf()
    nondet msg = world.messages.get(node).oneOf()
    confirmRead(msg),
    nondet node = NODE_POOL.oneOf()
    nondet key = world.system.get(node).volatile.pendingCalls.keys().oneOf()
    finishRead(key),
    nondet group = world.extensions.consensus.keys().oneOf()
    nondet replica = world.extensions.consensus.get(group).voters.union(
      world.extensions.consensus.get(group).learners).oneOf()
    catchUp(group, replica),
    nondet node = world.extensions.metadata.target.nodes.keys().oneOf()
    applyMeta(node),
  }

  action faultStep = any {
    nondet node = NODE_POOL.oneOf()
    nondet key = world.system.get(node).volatile.pendingCalls.keys().oneOf()
    retryRead(key),
    nondet node = NODE_POOL.oneOf()
    nondet key = world.system.get(node).volatile.pendingCalls.keys().filter(k =>
      world.system.get(node).volatile.pendingCalls.get(k).phase == Submitted).oneOf()
    submitMutation(key),
    nondet node = NODE_POOL.oneOf()
    nondet key = world.system.get(node).volatile.pendingCalls.keys().filter(k =>
      world.system.get(node).volatile.pendingCalls.get(k).phase == WaitingOperation).oneOf()
    dispatchOperation(key),
    nondet node = NODE_POOL.oneOf()
    nondet key = world.system.get(node).volatile.operationWork.keys().oneOf()
    retryOperationRead(node, key),
    nondet node = NODE_POOL.oneOf()
    nondet key = world.system.get(node).volatile.operationWork.keys().filter(k =>
      world.system.get(node).volatile.operationWork.get(k).phase == WorkSubmitted).oneOf()
    submitOperation(node, key),
    nondet node = NODE_POOL.oneOf()
    nondet key = world.system.get(node).volatile.pendingCalls.keys().oneOf()
    cancelCall(key),
    nondet msg = world.extensions.network.pending.oneOf()
    drop(msg),
    nondet msg = world.extensions.network.pending.oneOf()
    duplicate(msg),
    nondet source = NODE_POOL.oneOf()
    nondet destination = NODE_POOL.oneOf()
    nondet channel = Set(MetaChannel, LoadChannel).oneOf()
    nondet up = Set(true, false).oneOf()
    setLink(channel, source, destination, up),
    nondet node = NODE_POOL.oneOf()
    crash(node),
    nondet node = NODE_POOL.oneOf()
    restart(node),
  }

  action nativeStep = any {
    nondet group = world.extensions.consensus.keys().oneOf()
    commitProgress(group),
    nondet group = world.extensions.consensus.keys().oneOf()
    nondet candidate = world.extensions.consensus.get(group).voters.oneOf()
    elect(group, candidate),
  }

  action managementStep = any { requestStep, protocolStep }
  action step = any { managementStep, faultStep, nativeStep }
}
