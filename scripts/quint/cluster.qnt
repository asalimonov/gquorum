module cluster {
  import grants as G from "./grants"
  import controls as Ctrl from "./controls"
  import execution as E from "./execution"
  import sessions as Sessions from "./sessions"
  import lifecycle as Life from "./lifecycle"
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
      val meta = if (errors.size() == 0) Tx::applyAt(world.extensions.metadata, proposal.command, c.committedPosition + 1)
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
      | Some(leader) => {
          val prior = owner.volatile.observations.getOrElse("_meta", {
            group: "_meta", replica: leader, incarnation: owner.volatile.incarnation,
            observedConfig: 0, observedLeader: None, health: Healthy, age: Fresh })
          all {
            M::running(owner), M::liveIdentity(world.system, leader),
            M::communicates(world.extensions.network.metaLinks, node, leader.node),
            prior.observedLeader != Some(leader) or prior.observedConfig != c.configRevision,
            world' = { ...world, system: world.system.put(node, { ...owner, volatile: { ...owner.volatile,
              observations: owner.volatile.observations.put("_meta", { ...prior,
                observedLeader: Some(leader), observedConfig: c.configRevision }) } }) },
          }
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
          phase: Stopped, pendingCalls: Map(), owners: Map(), observations: Map(), operationWork: Map(), waitingEffects: Set(), authorityReads: Map(), holds: Set(), starts: Map(), fenceWorkers: Set(), metaRunning: false, stopTimedOut: false, ownLease: Revoked, leases: Map(), renewal: None, renewalExpired: false, revokeGate: None,
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
        volatile: { ...owner.volatile, phase: Starting, metaRunning: owner.durable.pin != None, stopTimedOut: false,
          owners: Map(), starts: Map(), holds: Set(), session: None, ownLease: Revoked,
          renewal: None, renewalExpired: false, leases: Map(), revokeGate: None,
          fenceWorkers: Set(), authorityReads: Map(), waitingEffects: Set(), pendingCalls: Map(),
          observations: Map(), operationWork: Map(),
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
      world' = { ...world,
        system: if (group == "_meta") world.system.setBy(candidate.node, node => { ...node, volatile: { ...node.volatile,
          leases: world.extensions.metaLog.get(node.volatile.appliedMetaIndex).sessions.keys().filter(k =>
            world.extensions.metaLog.get(node.volatile.appliedMetaIndex).sessions.get(k).phase == Live).mapBy(_ => Sessions::freshLease) } })
          else world.system,
        extensions: { ...world.extensions,
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

  action beginSession(node: NodeName): bool = {
    val host = world.system.get(node)
    val key: SessionKey = { owner: { node: node, incarnation: host.volatile.incarnation }, generation: 1 }
    all {
      M::running(host), host.volatile.session == None, host.durable.pin != None,
      world' = { ...world, system: world.system.put(node, { ...host,
        volatile: { ...host.volatile, session: Some(key), ownLease: Revoked } }) },
    }
  }
  action proposeSession(node: NodeName): bool = {
    val host = world.system.get(node)
    val local = world.extensions.metaLog.get(host.volatile.appliedMetaIndex)
    match host.volatile.session {
      | Some(key) => match host.durable.pin {
          | Some(pin) => {
              val missing = not(local.sessions.keys().contains(key)) or local.sessions.get(key).phase == Revoked
              val unbound = local.activeSessions.getOrElse(node, { owner: { node: "", incarnation: 0 }, generation: 0 }) != key
              all { key.owner == { node: node, incarnation: host.volatile.incarnation }, missing or unbound,
                submitControl(node, SessionControl(if (missing) GrantInternalSession({ key: key, member: pin.member })
                  else BindInternalSession(key))) }
            }
          | None => all { false, world' = world }
        }
      | None => all { false, world' = world }
    }
  }
  action renewSession(node: NodeName): bool = {
    val host = world.system.get(node)
    val local = world.extensions.metaLog.get(host.volatile.appliedMetaIndex)
    val leader = host.volatile.observations.get("_meta").observedLeader
    match host.volatile.session {
      | Some(key) => match host.durable.pin {
          | Some(pin) => match leader {
              | Some(destination) => {
                  val renewal: Renewal = { key: key, round: world.extensions.nextDelivery, member: pin.member }
                  all {
                    M::running(host), host.volatile.ownLease != Live, host.volatile.renewal == None,
                    local.activeSessions.getOrElse(node, { owner: { node: "", incarnation: 0 }, generation: 0 }) == key,
                    Sessions::renewalMatches(local, renewal, { node: node, incarnation: host.volatile.incarnation }),
                    world.extensions.nextDelivery <= MAX_DELIVERIES,
                    world' = apply_custom_effect({ ...world, system: world.system.put(node, { ...host,
                      volatile: { ...host.volatile, renewal: Some(renewal), renewalExpired: false } }) },
                      Queue({ delivery: world.extensions.nextDelivery, source: key.owner, destination: destination.node,
                        channel: MetaChannel, payload: SessionRenew(renewal) })),
                  }
                }
              | None => all { false, world' = world }
            }
          | None => all { false, world' = world }
        }
      | None => all { false, world' = world }
    }
  }
  pure def locallyLeader(host: LocalNode): bool =
    if (not(host.volatile.observations.keys().contains("_meta"))) false else match host.volatile.observations.get("_meta").observedLeader {
      | Some(leader) => leader.node == host.process_id | None => false }

  action handleSession(msg: Envelope): bool = {
    val host = world.system.get(msg.destination)
    val local = world.extensions.metaLog.get(host.volatile.appliedMetaIndex)
    val source = { node: msg.destination, incarnation: host.volatile.incarnation }
    match msg.payload {
      | SessionRenew(renewal) => {
          val valid = locallyLeader(host) and host.volatile.revokeGate == None and
            Sessions::renewalMatches(local, renewal, msg.source)
          val lease = host.volatile.leases.getOrElse(renewal.key, Sessions::freshLease)
          all {
            world.messages.get(msg.destination).contains(msg), M::running(host),
            world.extensions.nextDelivery <= MAX_DELIVERIES, lease.epoch < MAX_DELIVERIES,
            // KeepAlive is leader-local, not a Raft commit or a quorum proof.
            world' = if (valid) apply_custom_effect(apply_custom_effect({ ...world,
              system: world.system.put(msg.destination, { ...host, volatile: { ...host.volatile,
                leases: host.volatile.leases.put(renewal.key, Sessions::refreshed(lease)) } }) }, Consume(msg)),
              Queue({ delivery: world.extensions.nextDelivery, source: source, destination: renewal.key.owner.node,
                channel: MetaChannel, payload: SessionRenewed(renewal) }))
              else if (host.volatile.revokeGate == None) apply_custom_effect(world, Consume(msg)) else world,
            host.volatile.revokeGate == None,
          }
        }
      | SessionRenewed(renewal) => {
          val exact = host.volatile.session == Some(renewal.key) and renewal.key.owner == source and
            host.volatile.renewal == Some(renewal)
          all {
            world.messages.get(msg.destination).contains(msg), M::running(host),
            world' = apply_custom_effect({ ...world, system: world.system.put(msg.destination,
              if (exact) { ...host, volatile: { ...host.volatile, renewal: None,
                ownLease: if (host.volatile.renewalExpired) ExpiredObserved else Live,
                phase: if (host.volatile.renewalExpired) host.volatile.phase else Ready } } else host) }, Consume(msg)),
          }
        }
      | SessionProbe(probe) => match host.durable.pin {
          | Some(pin) => all {
              world.messages.get(msg.destination).contains(msg), M::running(host),
              world.extensions.nextDelivery <= MAX_DELIVERIES,
              world' = apply_custom_effect(apply_custom_effect(world, Consume(msg)), Queue({ delivery: world.extensions.nextDelivery,
                source: source, destination: probe.leader.node, channel: MetaChannel,
                payload: SessionProbed({ key: probe.key, epoch: probe.epoch, round: probe.round, leader: probe.leader, observed: source, member: pin.member }) })),
            }
          | None => all { false, world' = world }
        }
      | SessionProbed(probe) => {
          val lease = host.volatile.leases.getOrElse(probe.key, Sessions::freshLease)
          val exact = probe.leader == source and lease.phase == ExpiredObserved and lease.epoch == probe.epoch and
            lease.probe == Some(probe.round) and not(lease.failedProbe)
          val same = probe.observed == probe.key.owner and Sessions::eligible(local, probe.observed, probe.member)
          all {
            world.messages.get(msg.destination).contains(msg), M::running(host), lease.epoch < MAX_DELIVERIES,
            world' = apply_custom_effect({ ...world, system: world.system.put(msg.destination, if (exact) { ...host,
              volatile: { ...host.volatile, leases: host.volatile.leases.put(probe.key,
                if (same) Sessions::refreshed(lease) else { ...lease, failedProbe: true }) } } else host) }, Consume(msg)),
          }
        }
      | MetaAck(ack) => match host.volatile.revokeGate {
          | Some(gate) => all {
              world.messages.get(msg.destination).contains(msg), ack.key == gate.proposal, ack.coordinator == source,
              host.volatile.appliedMetaIndex >= ack.index,
              world' = apply_custom_effect({ ...world, system: world.system.put(msg.destination, { ...host,
                volatile: { ...host.volatile, revokeGate: None, leases: host.volatile.leases.mapRemove(gate.key) } }) }, Consume(msg)),
            }
          | None => all { false, world' = world }
        }
      | _ => all { false, world' = world }
    }
  }

  action seedLeases(node: NodeName): bool = {
    val host = world.system.get(node)
    val local = world.extensions.metaLog.get(host.volatile.appliedMetaIndex)
    val missing = local.sessions.keys().filter(k => local.sessions.get(k).phase == Live).exclude(host.volatile.leases.keys())
    all { M::running(host), locallyLeader(host), missing.size() > 0,
      world' = { ...world, system: world.system.put(node, { ...host, volatile: { ...host.volatile,
        leases: missing.fold(host.volatile.leases, (leases, key) => leases.put(key, Sessions::freshLease)) } }) } }
  }
  action forgetRevokedLease(node: NodeName, key: SessionKey): bool = {
    val host = world.system.get(node)
    val local = world.extensions.metaLog.get(host.volatile.appliedMetaIndex)
    all { locallyLeader(host), host.volatile.leases.keys().contains(key), host.volatile.revokeGate == None,
      not(local.sessions.keys().contains(key)) or local.sessions.get(key).phase == Revoked,
      world' = { ...world, system: world.system.put(node, { ...host,
        volatile: { ...host.volatile, leases: host.volatile.leases.mapRemove(key) } }) } }
  }
  action expireLease(node: NodeName, key: SessionKey): bool = {
    val host = world.system.get(node)
    val lease = host.volatile.leases.get(key)
    all { M::running(host), locallyLeader(host), lease.phase == Live, host.volatile.revokeGate == None,
      world' = { ...world, system: world.system.put(node, { ...host, volatile: { ...host.volatile,
        leases: host.volatile.leases.put(key, { ...lease, phase: ExpiredObserved }) } }) } }
  }
  action expireOwnLease(node: NodeName): bool = {
    val host = world.system.get(node)
    all { M::running(host), host.volatile.ownLease == Live or (host.volatile.renewal != None and not(host.volatile.renewalExpired)),
      world' = { ...world, system: world.system.put(node, { ...host, volatile: { ...host.volatile,
        ownLease: ExpiredObserved, renewalExpired: host.volatile.renewal != None } }) } }
  }
  action cancelRenewal(node: NodeName): bool = {
    val host = world.system.get(node)
    all { host.volatile.renewal != None, world' = { ...world, system: world.system.put(node, { ...host,
      volatile: { ...host.volatile, renewal: None } }) } }
  }
  action probeSession(node: NodeName, key: SessionKey): bool = {
    val host = world.system.get(node)
    val lease = host.volatile.leases.get(key)
    val source = { node: node, incarnation: host.volatile.incarnation }
    val round = world.extensions.nextDelivery
    all { M::running(host), locallyLeader(host), lease.phase == ExpiredObserved,
      world.extensions.metaLog.get(host.volatile.appliedMetaIndex).sessions.keys().contains(key),
      world.extensions.metaLog.get(host.volatile.appliedMetaIndex).sessions.get(key).phase == Live,
      lease.probe == None, not(lease.failedProbe), round <= MAX_DELIVERIES,
      world' = if (key.owner.node == node) { ...world, system: world.system.put(node, { ...host,
        volatile: { ...host.volatile, leases: host.volatile.leases.put(key,
          if (key.owner == source and match host.durable.pin { | Some(pin) => Sessions::eligible(
            world.extensions.metaLog.get(host.volatile.appliedMetaIndex), source, pin.member) | None => false })
            Sessions::refreshed(lease) else { ...lease, failedProbe: true }) } }) }
      else apply_custom_effect({ ...world, system: world.system.put(node, { ...host, volatile: { ...host.volatile,
        leases: host.volatile.leases.put(key, { ...lease, probe: Some(round) }) } }) },
        Queue({ delivery: round, source: source, destination: key.owner.node, channel: MetaChannel,
          payload: SessionProbe({ key: key, epoch: lease.epoch, round: round, leader: source }) })),
    }
  }
  action timeoutSessionProbe(node: NodeName, key: SessionKey): bool = {
    val host = world.system.get(node)
    val lease = host.volatile.leases.get(key)
    all { lease.phase == ExpiredObserved, lease.probe != None, not(lease.failedProbe),
      world' = { ...world, system: world.system.put(node, { ...host, volatile: { ...host.volatile,
        leases: host.volatile.leases.put(key, { ...lease, failedProbe: true }) } }) } }
  }
  action revokeExpiredSession(node: NodeName, key: SessionKey): bool = {
    val host = world.system.get(node)
    val lease = host.volatile.leases.get(key)
    all { M::running(host), locallyLeader(host), host.volatile.revokeGate == None,
      lease.phase == ExpiredObserved, lease.failedProbe,
      submitControl(node, SessionControl(RevokeInternalSession(key))) }
  }
  action abandonSessionRevoke(node: NodeName): bool = {
    val host = world.system.get(node)
    all { host.volatile.revokeGate != None, world' = { ...world, system: world.system.put(node, { ...host,
      volatile: { ...host.volatile, revokeGate: None } }) } }
  }
  action sessionStep = any {
    nondet node = NODE_POOL.filter(n => M::running(world.system.get(n)) and
      world.system.get(n).volatile.observations.keys().contains("_meta")).oneOf()
    any { beginSession(node), proposeSession(node), renewSession(node), seedLeases(node) },
    nondet node = NODE_POOL.oneOf()
    nondet msg = world.messages.get(node).oneOf()
    handleSession(msg),
    nondet node = NODE_POOL.oneOf()
    nondet key = world.system.get(node).volatile.leases.keys().oneOf()
    any { probeSession(node, key), revokeExpiredSession(node, key), forgetRevokedLease(node, key) },
  }

  pure def freshMetaAvailable(env: Environment, nodes: NodeStates, node: NodeName): bool = {
    val c = env.consensus.get("_meta")
    M::running(nodes.get(node)) and M::hasCommitPath(c, nodes, env.network.metaLinks) and
      match c.leaderAuthority { | Some(leader) => M::communicates(env.network.metaLinks, node, leader.node)
        | None => false }
  }

  action requestStart(node: NodeName, replica: ReplicaKey, explicit: bool): bool = {
    val owner = world.system.get(node)
    val key = { caller: { node: node, incarnation: owner.volatile.incarnation },
      sequence: owner.volatile.starts.keys().size() + 1 }
    all {
      M::running(owner), replica.node == node, replica.group != "_meta", key.sequence <= MAX_REQUESTS,
      world' = { ...world, system: world.system.put(node, { ...owner, volatile: { ...owner.volatile,
        starts: owner.volatile.starts.put(key, { key: replica, request: key, explicit: explicit,
          floorIndex: None, phase: StartPreparing, error: None }) } }) },
    }
  }

  action readStartScope(request: RequestKey): bool = {
    val node = world.system.get(request.caller.node)
    val work = node.volatile.starts.get(request)
    all {
      request.caller.incarnation == node.volatile.incarnation,
      work.phase == StartPreparing, work.floorIndex == None,
      (Life::canReuse(node, work.key) or freshMetaAvailable(world.extensions, world.system, request.caller.node)),
      world' = { ...world, system: world.system.put(request.caller.node, { ...node,
        volatile: { ...node.volatile, starts: node.volatile.starts.put(request,
          { ...work, floorIndex: Some(if (Life::canReuse(node, work.key)) node.volatile.appliedMetaIndex
            else world.extensions.consensus.get("_meta").committedPosition) }) } }) },
    }
  }

  action reserveOwner(request: RequestKey): bool = {
    val node = world.system.get(request.caller.node)
    val work = node.volatile.starts.get(request)
    match work.floorIndex {
      | Some(index) => {
          val floors = world.extensions.metaLog.get(index)
          val meta = world.extensions.metaLog.get(node.volatile.appliedMetaIndex)
          val staleFloor = floors.authorityFloors.keys().contains(Life::resource(work.key)) and
            Life::floorRevision(node, work.key) > floors.authorityFloors.get(Life::resource(work.key)).revision
          val valid = not(staleFloor) and Life::assigned(meta, work.key) and Life::retainedStore(node, work.key) and
            Life::canReserve(node, work.key, request, work.explicit)
          val imported = Life::canReuse(node, work.key) or Life::importedFloor(node, floors, work.key)
          val accepted = if (valid and imported) Life::reserve(node, work.key, request, work.explicit,
            meta.target.groups.get(work.key.group), Life::floorRevision(node, work.key)) else node
          all {
            M::running(node), request.caller.incarnation == node.volatile.incarnation,
            work.phase == StartPreparing, imported or not(valid),
            world' = { ...world, system: world.system.put(request.caller.node, { ...accepted,
              volatile: { ...accepted.volatile, starts: accepted.volatile.starts.put(request,
                { ...work, phase: if (valid) StartAccepted else StartRefused,
                  error: if (valid) None else Some(Unavailable) }) } }) },
          }
        }
      | None => all { false, world' = world }
    }
  }

  action beginOwnerFactory(node: NodeName, group: GroupId): bool = {
    val owner = world.system.get(node).volatile.owners.get(group)
    all { M::running(world.system.get(node)), owner.phase == Reserved,
      world' = { ...world, system: world.system.setBy(node, n => { ...n,
        volatile: { ...n.volatile, owners: n.volatile.owners.put(group, Life::beginFactory(owner)) } }) } }
  }
  action returnOwnerFactory(node: NodeName, group: GroupId, success: bool): bool = {
    val owner = world.system.get(node).volatile.owners.get(group)
    all { world.system.get(node).volatile.phase != Stopped, owner.factoryOutstanding,
      world' = { ...world, system: world.system.setBy(node, n => { ...n,
        volatile: { ...n.volatile, owners: n.volatile.owners.put(group, Life::factoryReturned(owner, success)) } }) } }
  }
  action expireOwnerFactory(node: NodeName, group: GroupId): bool = {
    val owner = world.system.get(node).volatile.owners.get(group)
    all { owner.factoryOutstanding, not(owner.callbackExpired),
      world' = { ...world, system: world.system.setBy(node, n => { ...n,
        volatile: { ...n.volatile, owners: n.volatile.owners.put(group, Life::expireFactory(owner)) } }) } }
  }
  action finishOwnerReplay(node: NodeName, group: GroupId): bool = {
    val owner = world.system.get(node).volatile.owners.get(group)
    all { owner.replayOutstanding,
      world' = { ...world, system: world.system.setBy(node, n => { ...n,
        volatile: { ...n.volatile, owners: n.volatile.owners.put(group, Life::replayFinished(owner)) } }) } }
  }
  action authorizeOwner(node: NodeName, group: GroupId): bool = {
    val owner = world.system.get(node).volatile.owners.get(group)
    val index = world.system.get(node).volatile.appliedMetaIndex
    val meta = world.extensions.metaLog.get(index)
    val allowed = if (not(Life::assigned(meta, owner.key))) false else true and owner.startDeclaration == Some(meta.target.groups.get(group)) and
      Life::exactPin(world.system.get(node), owner.key) and Life::importedFloor(world.system.get(node), meta, owner.key) and
      owner.authorityRevision == Life::floorRevision(world.system.get(node), owner.key)
    all {
      owner.phase == Replaying, owner.replayReady, not(owner.startupAuthorized), M::running(world.system.get(node)),
      world' = { ...world, system: world.system.setBy(node, n => { ...n, volatile: { ...n.volatile,
        owners: n.volatile.owners.put(group, if (allowed) { ...Life::authorize(owner), scopeIndex: index }
          else Life::requestStop(owner)) } }) },
    }
  }
  action publishOwner(node: NodeName, group: GroupId): bool = {
    val owner = world.system.get(node).volatile.owners.get(group)
    all { Life::publicationAllowed(world.system.get(node), owner),
      world' = { ...world, system: world.system.setBy(node, n => { ...n,
        volatile: { ...n.volatile, owners: n.volatile.owners.put(group, Life::publish(owner)) } }) } }
  }
  action replyStart(request: RequestKey): bool = {
    val node = world.system.get(request.caller.node)
    val work = node.volatile.starts.get(request)
    val owner = node.volatile.owners.get(work.key.group)
    all { work.phase == StartAccepted, owner.key == work.key,
      owner.phase == OwnerReady or owner.phase == OwnerStopped,
      world' = { ...world, system: world.system.setBy(request.caller.node, n => { ...n,
        volatile: { ...n.volatile, starts: n.volatile.starts.put(request,
          { ...work, phase: if (owner.phase == OwnerReady) StartSucceeded else StartRefused,
            error: if (owner.phase == OwnerReady) None else Some(Unavailable) }) } }) } }
  }
  action cancelStart(request: RequestKey): bool = {
    val node = world.system.get(request.caller.node)
    val work = node.volatile.starts.get(request)
    all { work.phase == StartPreparing or work.phase == StartAccepted,
      world' = { ...world, system: world.system.setBy(request.caller.node, n => { ...n,
        volatile: { ...n.volatile,
          starts: n.volatile.starts.put(request, { ...work, phase: StartRefused, error: Some(WaitCancelled) }),
          owners: if (work.phase == StartAccepted) n.volatile.owners.setBy(work.key.group,
            owner => Life::cancelWaiter(owner, request)) else n.volatile.owners } }) } }
  }
  action stopOwner(node: NodeName, replica: ReplicaKey, explicit: bool): bool = {
    val host = world.system.get(node)
    all { replica.node == node, replica.group != "_meta", host.volatile.phase != Stopped,
      if (explicit) not(host.volatile.holds.contains(replica)) else
        host.volatile.owners.keys().contains(replica.group) and
          host.volatile.owners.get(replica.group).key == replica and
          not(Set(OwnerStopping, OwnerStopped).contains(host.volatile.owners.get(replica.group).phase)),
      world' = { ...world, system: world.system.put(node, if (explicit) Life::hold(host, replica) else { ...host,
        volatile: { ...host.volatile, owners: host.volatile.owners.setBy(replica.group, owner => Life::requestStop(owner)) } }) } }
  }
  action joinOwner(node: NodeName, group: GroupId): bool = {
    val owner = world.system.get(node).volatile.owners.get(group)
    all { Life::canJoin(owner), world' = { ...world, system: world.system.setBy(node, n => { ...n,
      volatile: { ...n.volatile, owners: n.volatile.owners.put(group, Life::joined(owner)) } }) } }
  }
  action beginOwnerClose(node: NodeName, group: GroupId): bool = {
    val owner = world.system.get(node).volatile.owners.get(group)
    all { Life::canClose(owner), world' = { ...world, system: world.system.setBy(node, n => { ...n,
      volatile: { ...n.volatile, owners: n.volatile.owners.put(group, Life::beginClose(owner)) } }) } }
  }
  action returnOwnerClose(node: NodeName, group: GroupId, success: bool): bool = {
    val owner = world.system.get(node).volatile.owners.get(group)
    all { owner.applicationClose == Closing, world' = { ...world, system: world.system.setBy(node, n => { ...n,
      volatile: { ...n.volatile, owners: n.volatile.owners.put(group, Life::closeReturned(owner, success)) } }) } }
  }
  action releaseOwner(node: NodeName, group: GroupId): bool = {
    val owner = world.system.get(node).volatile.owners.get(group)
    all { owner.phase == OwnerStopping, Life::released(owner),
      world' = { ...world, system: world.system.setBy(node, n => { ...n,
        volatile: { ...n.volatile, owners: n.volatile.owners.put(group, Life::finishRelease(owner)),
          starts: n.volatile.starts.transformValues(work => if (work.key == owner.key and work.phase == StartAccepted)
            { ...work, phase: StartRefused, error: Some(Unavailable) } else work) } }) } }
  }

  action stopOlderForFence(node: NodeName, resource: ResourceKey): bool = {
    val host = world.system.get(node)
    val pending = host.durable.pendingFences.get(resource)
    val group = pending.delivery.grant.key.group
    val owner = host.volatile.owners.get(group)
    all { host.volatile.fenceWorkers.contains(resource), owner.authorityRevision < pending.delivery.grant.key.revision,
      not(Set(OwnerReady, OwnerStopping, OwnerStopped).contains(owner.phase)),
      world' = { ...world, system: world.system.put(node, { ...host, volatile: { ...host.volatile,
        owners: host.volatile.owners.put(group, Life::requestStop(owner)) } }) } }
  }
  action completeHostFence(node: NodeName, resource: ResourceKey): bool = {
    val host = world.system.get(node)
    val pending = host.durable.pendingFences.get(resource)
    val group = pending.delivery.grant.key.group
    val ownerJoined = if (not(host.volatile.owners.keys().contains(group))) true else {
      val owner = host.volatile.owners.get(group)
      owner.phase == OwnerReady or Life::released(owner)
    }
    val proof = E::fence(pending.delivery, pending.position)
    all { M::running(host), host.volatile.fenceWorkers.contains(resource), ownerJoined,
      E::receiverExact(world, pending.delivery),
      host.durable.authorityFloors.get(resource) == pending.delivery.grant.key.revision,
      world' = { ...world, system: world.system.put(node, { ...host,
        volatile: { ...host.volatile, fenceWorkers: host.volatile.fenceWorkers.exclude(Set(resource)) },
        durable: { ...host.durable, pendingFences: host.durable.pendingFences.mapRemove(resource),
          fences: host.durable.fences.union(Set(proof)) } }) },
    }
  }
  action importStartFloor(request: RequestKey): bool = {
    val host = world.system.get(request.caller.node)
    val work = host.volatile.starts.get(request)
    match work.floorIndex {
      | Some(index) => {
          val meta = world.extensions.metaLog.get(index)
          val resource = Life::resource(work.key)
          all { work.phase == StartPreparing, not(Life::canReuse(host, work.key)),
            meta.authorityFloors.keys().contains(resource), not(Life::importedFloor(host, meta, work.key)),
            dispatchGrantFrom(request.caller.node, meta.authorityFloors.get(resource), resource, index) }
        }
      | None => all { false, world' = world }
    }
  }
  action requestNodeStop(node: NodeName): bool = {
    val host = world.system.get(node)
    val caller = { node: node, incarnation: host.volatile.incarnation }
    val stopping = { ...world, system: world.system.put(node, { ...host, volatile: { ...host.volatile,
      phase: Stopping, ownLease: Revoked, renewal: None,
      owners: host.volatile.owners.transformValues(owner => if (owner.phase == OwnerStopped) owner else Life::requestStop(owner)) } }) }
    val leader = if (host.volatile.observations.keys().contains("_meta")) host.volatile.observations.get("_meta").observedLeader else None
    val nextWorld = match host.volatile.session {
      | Some(key) => match leader {
          | Some(destination) => if (world.extensions.nextDelivery <= MAX_DELIVERIES) apply_custom_effect(stopping,
              Queue({ delivery: world.extensions.nextDelivery, source: caller, destination: destination.node, channel: MetaChannel,
                payload: MetaProposal({ key: { request: { caller: caller, sequence: -world.extensions.nextDelivery }, sequence: 0 },
                  coordinator: caller, forWork: false, command: ControlProposal(SessionControl(RevokeInternalSession(key))) }) }))
            else stopping
          | None => stopping }
      | None => stopping
    }
    all { M::running(host), world' = nextWorld }
  }
  action joinAuthorityWorkers(node: NodeName): bool = {
    val host = world.system.get(node)
    all { host.volatile.phase == Stopping, host.volatile.fenceWorkers.size() > 0 or host.volatile.waitingEffects.size() > 0,
      world' = { ...world, system: world.system.put(node, { ...host, volatile: { ...host.volatile,
        fenceWorkers: Set(), waitingEffects: Set() } }) } }
  }
  action timeoutNodeStop(node: NodeName): bool = {
    val host = world.system.get(node)
    all { host.volatile.phase == Stopping, not(host.volatile.stopTimedOut),
      world' = { ...world, system: world.system.put(node, { ...host,
        volatile: { ...host.volatile, stopTimedOut: true } }) } }
  }
  action expireOwnerClose(node: NodeName, group: GroupId): bool = {
    val owner = world.system.get(node).volatile.owners.get(group)
    all { owner.applicationClose == Closing, not(owner.closeExpired),
      world' = { ...world, system: world.system.setBy(node, host => { ...host, volatile: { ...host.volatile,
        owners: host.volatile.owners.put(group, { ...owner, closeExpired: true }) } }) } }
  }
  action finishNodeStop(node: NodeName): bool = {
    val host = world.system.get(node)
    all { host.volatile.phase == Stopping,
      host.volatile.owners.values().forall(owner => owner.phase == OwnerStopped and Life::released(owner)),
      host.volatile.fenceWorkers.size() == 0, host.volatile.waitingEffects.size() == 0,
      world' = { ...world, system: world.system.put(node, { ...host, volatile: { ...host.volatile,
        phase: Stopped, metaRunning: false, session: None, ownLease: Revoked } }) } }
  }
  action beginNodeStart(node: NodeName): bool = {
    val host = world.system.get(node)
    all { host.volatile.phase == New,
      world' = { ...world, system: world.system.put(node, { ...host,
        volatile: { ...host.volatile, phase: Starting, metaRunning: host.durable.pin != None } }) } }
  }

  action localLifetimeStep = any {
    nondet node = NODE_POOL.oneOf()
    any { beginNodeStart(node), joinAuthorityWorkers(node), finishNodeStop(node) },
    nondet node = NODE_POOL.oneOf()
    nondet resource = world.system.get(node).volatile.fenceWorkers.oneOf()
    any { completeHostFence(node, resource),
      all { world.system.get(node).volatile.owners.keys().contains(world.system.get(node).durable.pendingFences.get(resource).delivery.grant.key.group),
        stopOlderForFence(node, resource) } },
    nondet node = NODE_POOL.oneOf()
    nondet request = world.system.get(node).volatile.starts.keys().oneOf()
    importStartFloor(request),
    nondet node = NODE_POOL.oneOf()
    nondet request = world.system.get(node).volatile.starts.keys().oneOf()
    readStartScope(request),
    nondet node = NODE_POOL.oneOf()
    nondet request = world.system.get(node).volatile.starts.keys().oneOf()
    reserveOwner(request),
    nondet node = NODE_POOL.oneOf()
    nondet request = world.system.get(node).volatile.starts.keys().oneOf()
    replyStart(request),
    nondet node = NODE_POOL.oneOf()
    nondet group = world.system.get(node).volatile.owners.keys().oneOf()
    any { beginOwnerFactory(node, group), returnOwnerFactory(node, group, true), finishOwnerReplay(node, group),
      authorizeOwner(node, group), publishOwner(node, group), joinOwner(node, group), beginOwnerClose(node, group),
      returnOwnerClose(node, group, true), releaseOwner(node, group) },
  }

  // Internal authority mutations share the same Raft commit, durable apply and
  // transport as public mutations. Negative request sequences cannot alias an
  // application call. They identify transport work, not a public API request.
  action submitControl(node: NodeName, command: ControlCommand): bool = {
    val owner = world.system.get(node)
    val local = world.extensions.metaLog.get(owner.volatile.appliedMetaIndex)
    val leader = owner.volatile.observations.get("_meta").observedLeader
    val caller = { node: node, incarnation: owner.volatile.incarnation }
    val proposalKey: ProposalKey = { request: { caller: caller, sequence: -world.extensions.nextDelivery }, sequence: 0 }
    val submitting = match command {
      | SessionControl(sessionCommand) => match sessionCommand {
          | RevokeInternalSession(key) => { ...world, system: world.system.put(node, { ...owner,
              volatile: { ...owner.volatile, revokeGate: Some({ key: key, proposal: proposalKey }) } }) }
          | _ => world }
      | _ => world
    }
    match leader {
      | Some(r) => all {
          M::running(owner), Ctrl::errors(local, command).size() == 0,
          world.extensions.nextDelivery <= MAX_DELIVERIES,
          not(world.extensions.network.pending.union(world.messages.values().flatten()).exists(msg =>
            msg.source == caller and match msg.payload {
              | MetaProposal(p) => p.command == ControlProposal(command) | _ => false })),
          world' = apply_custom_effect(submitting, Queue({ delivery: world.extensions.nextDelivery,
            source: caller, destination: r.node, channel: MetaChannel,
            payload: MetaProposal({ key: proposalKey,
              coordinator: caller, forWork: false, command: ControlProposal(command) }) })),
        }
      | None => all { false, world' = world }
    }
  }

  action observeAuthorityLeader(node: NodeName, group: GroupId): bool = {
    val c = world.extensions.consensus.get(group)
    val owner = world.system.get(node)
    match c.leaderAuthority {
      | Some(leader) => all {
          M::running(owner), M::readableIdentity(world.system, leader),
          M::communicates(M::linksFor(world.extensions.network, group), node, leader.node),
          not(owner.volatile.observations.keys().contains(group) and
            owner.volatile.observations.get(group).observedLeader == Some(leader)),
          world' = { ...world, system: world.system.put(node, { ...owner,
            volatile: { ...owner.volatile, observations: owner.volatile.observations.put(group,
              { group: group, replica: leader, incarnation: world.system.get(leader.node).volatile.incarnation,
                observedConfig: c.configRevision, observedLeader: Some(leader), health: Healthy, age: Fresh }) } }) },
        }
      | None => all { false, world' = world }
    }
  }

  action dispatchGrantFrom(node: NodeName, key: GrantKey, resource: ResourceKey, proofIndex: int): bool = {
    val owner = world.system.get(node)
    val local = world.extensions.metaLog.get(proofIndex)
    val grant = local.grants.get(key)
    val candidates = match resource {
      | HostResource(host) => grant.targets.filter(r => r.node == host.node)
      | GroupResource(group) => if (owner.volatile.observations.keys().contains(group))
          match owner.volatile.observations.get(group).observedLeader { | Some(r) => Set(r) | None => Set() }
          else Set()
    }
    nondet replica = candidates.oneOf()
    val delivery: EffectDelivery = { delivery: world.extensions.nextDelivery,
      sender: { node: node, incarnation: owner.volatile.incarnation },
      senderMember: match owner.durable.pin { | Some(pin) => pin.member | None => 0 }, proofIndex: None,
      receiver: { node: replica.node, incarnation: world.system.get(replica.node).volatile.incarnation },
      replica: replica, grant: grant, resource: resource }
    all {
      M::running(owner), grant.resources.contains(resource), G::currentForIssuance(local, grant),
      E::courierMatches(world.extensions.metadata, delivery),
      M::hasCommitPath(world.extensions.consensus.get("_meta"), world.system, world.extensions.network.metaLinks),
      match world.extensions.consensus.get("_meta").leaderAuthority { | Some(leader) =>
        M::communicates(world.extensions.network.metaLinks, node, leader.node) | None => false },
      not(G::resourceConsumed(local, grant, resource)), E::executable(delivery),
      world.extensions.nextDelivery <= MAX_DELIVERIES,
      world' = apply_custom_effect(world, Queue({ delivery: delivery.delivery, source: delivery.sender,
        destination: replica.node, channel: if (grant.key.group == "_meta") MetaChannel else LoadChannel,
        payload: ExecuteGrant(delivery) })),
    }
  }

  action dispatchGrant(node: NodeName, key: GrantKey, resource: ResourceKey): bool =
    dispatchGrantFrom(node, key, resource, world.system.get(node).volatile.appliedMetaIndex)

  // ValidateAuthorityCourier reads a quorum-confirmed authority proof. Capture
  // that immutable query result separately from acceptance and native waiting.
  action confirmGrant(msg: Envelope): bool = match msg.payload {
    | ExecuteGrant(_) => {
      val node = world.system.get(msg.destination)
      val c = world.extensions.consensus.get("_meta")
      all {
        world.messages.get(msg.destination).contains(msg), M::running(node),
        not(node.volatile.authorityReads.keys().contains(msg.delivery)),
        M::hasCommitPath(c, world.system, world.extensions.network.metaLinks),
        match c.leaderAuthority { | Some(leader) =>
          M::communicates(world.extensions.network.metaLinks, msg.destination, leader.node) | None => false },
        world' = { ...world, system: world.system.put(msg.destination, { ...node,
          volatile: { ...node.volatile, authorityReads: node.volatile.authorityReads.put(msg.delivery, c.committedPosition) } }) },
      }
    }
    | _ => all { false, world' = world }
  }

  action receiveGrant(msg: Envelope): bool = match msg.payload {
    | ExecuteGrant(delivery) => {
      val node = world.system.get(msg.destination)
      val authenticated = { ...delivery, proofIndex: if (node.volatile.authorityReads.keys().contains(msg.delivery))
        Some(node.volatile.authorityReads.get(msg.delivery)) else None }
      val accepted = delivery.sender == msg.source and delivery.receiver.node == msg.destination and
        E::mayReceive(world, authenticated)
      all {
        world.messages.get(msg.destination).contains(msg),
        node.volatile.authorityReads.keys().contains(msg.delivery),
        world' = apply_custom_effect({ ...world, system: world.system.put(msg.destination, { ...node,
          volatile: { ...node.volatile, waitingEffects: if (accepted)
            node.volatile.waitingEffects.union(Set(authenticated)) else node.volatile.waitingEffects,
            authorityReads: node.volatile.authorityReads.mapRemove(msg.delivery) } }) }, Consume(msg)),
      }
    }
    | _ => all { false, world' = world }
  }

  action consumeGrant(delivery: EffectDelivery): bool = {
    val node = world.system.get(delivery.receiver.node)
    val nativeReady = match delivery.resource {
      | HostResource(_) => node.durable.journalPosition < MAX_POSITION
      | GroupResource(group) => {
          val c = world.extensions.consensus.get(group)
          c.leaderAuthority == Some(delivery.replica) and c.committedPosition < MAX_POSITION and
            M::hasCommitPath(c, world.system, M::linksFor(world.extensions.network, group)) and
            (delivery.grant.purpose == Fence or match delivery.grant.effect {
              | Some(e) => match e.target { | ReplicaTarget(r) => c.voters.contains(r) and
                  M::readableIdentity(world.system, r) and c.catchUp.getOrElse(r, Behind) == CaughtUp
                  | _ => false }
              | None => false })
        }
    }
    all {
      node.volatile.waitingEffects.contains(delivery), E::executable(delivery),
      E::mayConsume(world, delivery), E::consumed(world, delivery) or nativeReady,
      world' = E::consume(world, delivery),
    }
  }

  action refuseGrant(delivery: EffectDelivery): bool = all {
    world.system.get(delivery.receiver.node).volatile.waitingEffects.contains(delivery),
    not(E::mayConsume(world, delivery)),
    world' = E::forgetWait(world, delivery),
  }

  action reportHostFence(node: NodeName, fence: FenceEvidence): bool = all {
    world.system.get(node).durable.fences.contains(fence),
    submitControl(node, RecordFence(fence)),
  }

  // A native proof is a fresh quorum-confirmed read of the durable native
  // ledger; reporting it into _meta is a distinct subsequent transaction.
  action reportNative(node: NodeName, group: GroupId, command: ControlCommand): bool = {
    val c = world.extensions.consensus.get(group)
    val sender = world.system.get(node)
    val local = world.extensions.metaLog.get(sender.volatile.appliedMetaIndex)
    val fresh = match c.leaderAuthority { | Some(leader) =>
      M::communicates(M::linksFor(world.extensions.network, group), node, leader.node) and
        M::hasCommitPath(c, world.system, M::linksFor(world.extensions.network, group)) | None => false }
    val present = match command {
      | RecordEffect(r) => c.authorityReceipts.contains(r) and not(local.effectReceipts.contains(r))
      | RecordFence(f) => c.authorityFences.contains(f) and not(local.fenceEvidence.contains(f))
      | _ => false
    }
    all { fresh, present, submitControl(node, command) }
  }

  action finishTransfer(group: GroupId, key: GrantKey): bool = {
    val c = world.extensions.consensus.get(group)
    val receipts = c.authorityReceipts.filter(r => r.grant == key and r.outcome == TransferStarted)
    nondet receipt = receipts.oneOf()
    val target = receipt.target
    match target {
      | Some(replica) => all {
          c.authorityFloor == key.revision,
          // Retained transfer intent is resumable only under its exact floor.
          elect(group, replica),
        }
      | None => all { false, world' = world }
    }
  }

  action checkpointTransfer(node: NodeName, approved: ApprovedAction): bool = {
    val group = G::groupOf(approved.step.target)
    val c = world.extensions.consensus.get(group)
    all {
      approved.step.kind == TransferLeadership,
      M::hasCommitPath(c, world.system, M::linksFor(world.extensions.network, group)),
      match c.leaderAuthority { | Some(leader) =>
        M::communicates(M::linksFor(world.extensions.network, group), node, leader.node) | None => false },
      submitControl(node, CompleteStep({ approved: approved, observedLeader: c.leaderAuthority })),
    }
  }

  action authorityStep = any {
    nondet node = NODE_POOL.filter(n => M::running(world.system.get(n)) and
      world.system.get(n).volatile.observations.keys().contains("_meta")).oneOf()
    val local = world.extensions.metaLog.get(world.system.get(node).volatile.appliedMetaIndex)
    nondet grant = local.grants.values().filter(g => g.purpose == Effect).oneOf()
    match grant.effect { | Some(effect) => match grant.planStep {
      | Some(step) => checkpointTransfer(node, { operation: effect.operation, step: step })
      | None => all { false, world' = world } } | None => all { false, world' = world } },
    nondet node = NODE_POOL.filter(n => M::running(world.system.get(n)) and
      world.system.get(n).volatile.observations.keys().contains("_meta")).oneOf()
    val local = world.extensions.metaLog.get(world.system.get(node).volatile.appliedMetaIndex)
    nondet key = local.operations.keys().oneOf()
    nondet group = local.operations.get(key).blocks.keys().oneOf()
    submitControl(node, BeginBlock({ operation: key, group: group })),
    nondet node = NODE_POOL.filter(n => M::running(world.system.get(n)) and
      world.system.get(n).volatile.observations.keys().contains("_meta")).oneOf()
    val local = world.extensions.metaLog.get(world.system.get(node).volatile.appliedMetaIndex)
    nondet key = local.operations.keys().oneOf()
    nondet group = local.operations.get(key).blocks.keys().oneOf()
    nondet stepIndex = local.operations.get(key).plan.blocks.get(group).indices().oneOf()
    val step = local.operations.get(key).plan.blocks.get(group).nth(stepIndex)
    all { G::actionGrants(local, { operation: key, step: step }).size() == 0,
      submitControl(node, IssueEffect({ operation: key, step: step })) },
    nondet node = NODE_POOL.filter(n => M::running(world.system.get(n))).oneOf()
    val local = world.extensions.metaLog.get(world.system.get(node).volatile.appliedMetaIndex)
    nondet key = local.grants.keys().oneOf()
    nondet resource = local.grants.get(key).resources.oneOf()
    dispatchGrant(node, key, resource),
    nondet node = NODE_POOL.oneOf()
    nondet msg = world.messages.get(node).oneOf()
    receiveGrant(msg),
    nondet node = NODE_POOL.oneOf()
    nondet msg = world.messages.get(node).oneOf()
    confirmGrant(msg),
    nondet node = NODE_POOL.oneOf()
    nondet delivery = world.system.get(node).volatile.waitingEffects.oneOf()
    consumeGrant(delivery),
    nondet node = NODE_POOL.oneOf()
    nondet delivery = world.system.get(node).volatile.waitingEffects.oneOf()
    refuseGrant(delivery),
    nondet node = NODE_POOL.filter(n => M::running(world.system.get(n)) and
      world.system.get(n).volatile.observations.keys().contains("_meta")).oneOf()
    nondet fence = world.system.get(node).durable.fences.oneOf()
    val local = world.extensions.metaLog.get(world.system.get(node).volatile.appliedMetaIndex)
    all { not(local.fenceEvidence.contains(fence)), reportHostFence(node, fence) },
    nondet node = NODE_POOL.filter(n => M::running(world.system.get(n)) and
      world.system.get(n).volatile.observations.keys().contains("_meta")).oneOf()
    nondet group = world.extensions.consensus.keys().oneOf()
    nondet command = world.extensions.consensus.get(group).authorityReceipts.map(r => RecordEffect(r)).union(
      world.extensions.consensus.get(group).authorityFences.map(f => RecordFence(f))).oneOf()
    reportNative(node, group, command),
    nondet node = NODE_POOL.filter(n => M::running(world.system.get(n))).oneOf()
    nondet group = world.extensions.consensus.keys().oneOf()
    observeAuthorityLeader(node, group),
    nondet group = world.extensions.consensus.keys().oneOf()
    nondet receipt = world.extensions.consensus.get(group).authorityReceipts.oneOf()
    finishTransfer(group, receipt.grant),
  }

  // This subset is useful for reachability checks under healthy communication.
  // It remains part of the same production step relation, with the same state.
  action requestStep = any {
    nondet node = NODE_POOL.oneOf()
    requestNodeStop(node),
    nondet node = NODE_POOL.oneOf()
    val local = world.system.get(node).volatile.localView.target
    nondet group = local.groups.keys().exclude(Set("_meta")).oneOf()
    nondet replica = local.groups.get(group).replicas.filter(r => r.key.node == node).oneOf()
    nondet explicit = Set(true, false).oneOf()
    requestStart(node, replica.key, explicit),
    nondet node = NODE_POOL.oneOf()
    nondet group = world.system.get(node).volatile.owners.keys().oneOf()
    stopOwner(node, world.system.get(node).volatile.owners.get(group).key, true),
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
    sessionStep,
    localLifetimeStep,
    authorityStep,
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
    any { expireOwnLease(node), cancelRenewal(node), abandonSessionRevoke(node), timeoutNodeStop(node) },
    nondet node = NODE_POOL.oneOf()
    nondet key = world.system.get(node).volatile.leases.keys().oneOf()
    any { expireLease(node, key), timeoutSessionProbe(node, key) },
    nondet node = NODE_POOL.oneOf()
    nondet request = world.system.get(node).volatile.starts.keys().oneOf()
    cancelStart(request),
    nondet node = NODE_POOL.oneOf()
    nondet group = world.system.get(node).volatile.owners.keys().oneOf()
    any { expireOwnerFactory(node, group), expireOwnerClose(node, group), returnOwnerFactory(node, group, false), returnOwnerClose(node, group, false) },
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
