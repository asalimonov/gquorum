module cluster {
  import harness_environment_steps as Environment from "./harness/environment_steps"
  import metaservice_transaction_steps as Calls from "./metaservice/transaction_steps"
  import metaservice_operation_steps as Operations from "./metaservice/operation_steps"
  import consensus_steps as Native from "./consensus/steps"
  import metaservice_session_steps as SessionSteps from "./metaservice/session_steps"
  import managed_lifetime_steps as Lifetime from "./managed/lifetime_steps"
  import managed_node_steps as NodeSteps from "./managed/node_steps"
  import metaservice_authority_steps as AuthoritySteps from "./metaservice/authority_steps"
  import managed_receiver_steps as Receiver from "./managed/receiver_steps"
  import grants as G from "./metaengine/grants"
  import controls as Ctrl from "./metaengine/controls"
  import execution as E from "./managed/execution"
  import sessions as Sessions from "./metaengine/sessions"
  import lifecycle as Life from "./managed/lifecycle"
  import initial as I from "./harness/initial"
  import membership as M from "./consensus/membership"
  import scope as S from "./harness/scope"
  import reads as R from "./metaservice/reads"
  import transactions as Tx from "./metaservice/transactions"
  import authority as A from "./metaengine/authority"
  import operations as O from "./metaservice/operations"
  import types.* from "./types"
  import handlers.* from "./metaservice/handlers"
  import world_types.* from "./runtime/world"
  import message_effects.* from "./runtime/messages"
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
    if (Environment::invokeEnabled(world, node, request, MAX_REQUESTS, MAX_DELIVERIES)) world' = Environment::invokeNext(world, node, request, MAX_REQUESTS, MAX_DELIVERIES)
    else all { false, world' = world }
  }

  action deliver(msg: Envelope): bool = {
    if (Environment::deliverEnabled(world, msg)) world' = Environment::deliverNext(world, msg)
    else all { false, world' = world }
  }

  action handle(node: NodeName): bool = {
    nondet tr = Calls::handleCandidates(world, node, MAX_DELIVERIES).oneOf()
    if (Calls::handleEnabled(world, node, tr, MAX_DELIVERIES)) world' = Calls::handleNext(world, node, tr, MAX_DELIVERIES)
    else all { false, world' = world }
  }

  action confirmRead(msg: Envelope): bool = {
    if (Calls::confirmReadEnabled(world, msg, MAX_DELIVERIES)) world' = Calls::confirmReadNext(world, msg, MAX_DELIVERIES)
    else all { false, world' = world }
  }

  action finishRead(key: RequestKey): bool = {
    if (Calls::finishReadEnabled(world, key)) world' = Calls::finishReadNext(world, key)
    else all { false, world' = world }
  }

  action prepareMutation(key: RequestKey): bool = {
    if (Calls::prepareMutationEnabled(world, key, rules)) world' = Calls::prepareMutationNext(world, key, rules)
    else all { false, world' = world }
  }

  action submitMutation(key: RequestKey): bool = {
    if (Calls::submitMutationEnabled(world, key, MAX_DELIVERIES)) world' = Calls::submitMutationNext(world, key, MAX_DELIVERIES)
    else all { false, world' = world }
  }

  action commitMutation(msg: Envelope): bool = {
    if (Calls::commitMutationEnabled(world, msg, MAX_POSITION, MAX_DELIVERIES, rules)) world' = Calls::commitMutationNext(world, msg, MAX_POSITION, MAX_DELIVERIES, rules)
    else all { false, world' = world }
  }

  action finishMutation(key: RequestKey): bool = {
    if (Calls::finishMutationEnabled(world, key)) world' = Calls::finishMutationNext(world, key)
    else all { false, world' = world }
  }

  action beginReseatRead(key: RequestKey): bool = {
    if (Calls::beginReseatReadEnabled(world, key, MAX_DELIVERIES)) world' = Calls::beginReseatReadNext(world, key, MAX_DELIVERIES)
    else all { false, world' = world }
  }

  action finishReseatRead(key: RequestKey): bool = {
    if (Calls::finishReseatReadEnabled(world, key)) world' = Calls::finishReseatReadNext(world, key)
    else all { false, world' = world }
  }

  action dispatchOperation(key: RequestKey): bool = {
    if (Operations::dispatchOperationEnabled(world, key, MAX_DELIVERIES)) world' = Operations::dispatchOperationNext(world, key, MAX_DELIVERIES)
    else all { false, world' = world }
  }

  action transmitOperationRead(node: NodeName, key: ProposalKey, retry: bool): bool = {
    if (Operations::transmitOperationReadEnabled(world, node, key, retry, MAX_DELIVERIES)) world' = Operations::transmitOperationReadNext(world, node, key, retry, MAX_DELIVERIES)
    else all { false, world' = world }
  }

  action startOperationRead(node: NodeName, key: ProposalKey): bool = transmitOperationRead(node, key, false)
  action retryOperationRead(node: NodeName, key: ProposalKey): bool = transmitOperationRead(node, key, true)

  action captureOperation(node: NodeName, key: ProposalKey): bool = {
    if (Operations::captureOperationEnabled(world, node, key, rules)) world' = Operations::captureOperationNext(world, node, key, rules)
    else all { false, world' = world }
  }

  action observeOperation(node: NodeName, key: ProposalKey, group: GroupId): bool = {
    if (Operations::observeOperationEnabled(world, node, key, group)) world' = Operations::observeOperationNext(world, node, key, group)
    else all { false, world' = world }
  }

  action observeOperationHealth(node: NodeName, key: ProposalKey, group: GroupId): bool = {
    if (Operations::observeOperationHealthEnabled(world, node, key, group)) world' = Operations::observeOperationHealthNext(world, node, key, group)
    else all { false, world' = world }
  }

  action beginOperationRecapture(node: NodeName, key: ProposalKey): bool = {
    if (Operations::beginOperationRecaptureEnabled(world, node, key)) world' = Operations::beginOperationRecaptureNext(world, node, key)
    else all { false, world' = world }
  }

  action buildOperation(node: NodeName, key: ProposalKey): bool = {
    if (Operations::buildOperationEnabled(world, node, key)) world' = Operations::buildOperationNext(world, node, key)
    else all { false, world' = world }
  }

  action submitOperation(node: NodeName, key: ProposalKey): bool = {
    if (Operations::submitOperationEnabled(world, node, key, MAX_DELIVERIES)) world' = Operations::submitOperationNext(world, node, key, MAX_DELIVERIES)
    else all { false, world' = world }
  }

  action finishOperation(node: NodeName, key: ProposalKey): bool = {
    if (Operations::finishOperationEnabled(world, node, key)) world' = Operations::finishOperationNext(world, node, key)
    else all { false, world' = world }
  }

  action replyOperation(node: NodeName, key: ProposalKey): bool = {
    if (Operations::replyOperationEnabled(world, node, key, MAX_DELIVERIES)) world' = Operations::replyOperationNext(world, node, key, MAX_DELIVERIES)
    else all { false, world' = world }
  }

  action observeMetaLeader(node: NodeName): bool = {
    if (Operations::observeMetaLeaderEnabled(world, node)) world' = Operations::observeMetaLeaderNext(world, node)
    else all { false, world' = world }
  }

  action retryRead(key: RequestKey): bool = {
    if (Operations::retryReadEnabled(world, key, MAX_DELIVERIES)) world' = Operations::retryReadNext(world, key, MAX_DELIVERIES)
    else all { false, world' = world }
  }

  action cancelCall(key: RequestKey): bool = {
    if (Operations::cancelCallEnabled(world, key)) world' = Operations::cancelCallNext(world, key)
    else all { false, world' = world }
  }

  action drop(msg: Envelope): bool = {
    if (Environment::dropEnabled(world, msg)) world' = Environment::dropNext(world, msg)
    else all { false, world' = world }
  }

  action duplicate(msg: Envelope): bool = {
    if (Environment::duplicateEnabled(world, msg, MAX_DELIVERIES)) world' = Environment::duplicateNext(world, msg, MAX_DELIVERIES)
    else all { false, world' = world }
  }

  action setLink(channel: Channel, source: NodeName, destination: NodeName, up: bool): bool = {
    if (Environment::setLinkEnabled(world, channel, source, destination, up)) world' = Environment::setLinkNext(world, channel, source, destination, up)
    else all { false, world' = world }
  }

  action crash(node: NodeName): bool = {
    if (Environment::crashEnabled(world, node)) world' = Environment::crashNext(world, node)
    else all { false, world' = world }
  }

  action restart(node: NodeName): bool = {
    if (Environment::restartEnabled(world, node, MAX_INCARNATION)) world' = Environment::restartNext(world, node, MAX_INCARNATION)
    else all { false, world' = world }
  }

  action commitProgress(group: GroupId): bool = {
    if (Native::commitProgressEnabled(world, group, MAX_POSITION)) world' = Native::commitProgressNext(world, group, MAX_POSITION)
    else all { false, world' = world }
  }

  action elect(group: GroupId, candidate: ReplicaKey): bool = {
    if (Native::electEnabled(world, group, candidate)) world' = Native::electNext(world, group, candidate)
    else all { false, world' = world }
  }

  action catchUp(group: GroupId, replica: ReplicaKey): bool = {
    if (Native::catchUpEnabled(world, group, replica)) world' = Native::catchUpNext(world, group, replica)
    else all { false, world' = world }
  }

  action applyMeta(node: NodeName): bool = {
    nondet replica = Native::applyMetaCandidates(world, node).oneOf()
    if (Native::applyMetaEnabled(world, node, replica)) world' = Native::applyMetaNext(world, node, replica)
    else all { false, world' = world }
  }

  action beginSession(node: NodeName): bool = {
    if (SessionSteps::beginSessionEnabled(world, node)) world' = SessionSteps::beginSessionNext(world, node)
    else all { false, world' = world }
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
    if (SessionSteps::renewSessionEnabled(world, node, MAX_DELIVERIES)) world' = SessionSteps::renewSessionNext(world, node, MAX_DELIVERIES)
    else all { false, world' = world }
  }

  pure def locallyLeader(host: LocalNode): bool = Sessions::locallyLeader(host)

  action handleSession(msg: Envelope): bool = {
    if (SessionSteps::handleSessionEnabled(world, msg, MAX_DELIVERIES)) world' = SessionSteps::handleSessionNext(world, msg, MAX_DELIVERIES)
    else all { false, world' = world }
  }

  action seedLeases(node: NodeName): bool = {
    if (SessionSteps::seedLeasesEnabled(world, node)) world' = SessionSteps::seedLeasesNext(world, node)
    else all { false, world' = world }
  }

  action forgetRevokedLease(node: NodeName, key: SessionKey): bool = {
    if (SessionSteps::forgetRevokedLeaseEnabled(world, node, key)) world' = SessionSteps::forgetRevokedLeaseNext(world, node, key)
    else all { false, world' = world }
  }

  action expireLease(node: NodeName, key: SessionKey): bool = {
    if (SessionSteps::expireLeaseEnabled(world, node, key)) world' = SessionSteps::expireLeaseNext(world, node, key)
    else all { false, world' = world }
  }

  action expireOwnLease(node: NodeName): bool = {
    if (SessionSteps::expireOwnLeaseEnabled(world, node)) world' = SessionSteps::expireOwnLeaseNext(world, node)
    else all { false, world' = world }
  }

  action cancelRenewal(node: NodeName): bool = {
    if (SessionSteps::cancelRenewalEnabled(world, node)) world' = SessionSteps::cancelRenewalNext(world, node)
    else all { false, world' = world }
  }

  action probeSession(node: NodeName, key: SessionKey): bool = {
    if (SessionSteps::probeSessionEnabled(world, node, key, MAX_DELIVERIES)) world' = SessionSteps::probeSessionNext(world, node, key, MAX_DELIVERIES)
    else all { false, world' = world }
  }

  action timeoutSessionProbe(node: NodeName, key: SessionKey): bool = {
    if (SessionSteps::timeoutSessionProbeEnabled(world, node, key)) world' = SessionSteps::timeoutSessionProbeNext(world, node, key)
    else all { false, world' = world }
  }

  action revokeExpiredSession(node: NodeName, key: SessionKey): bool = {
    val host = world.system.get(node)
    val lease = host.volatile.leases.get(key)
    all { M::running(host), locallyLeader(host), host.volatile.revokeGate == None,
      lease.phase == ExpiredObserved, lease.failedProbe,
      submitControl(node, SessionControl(RevokeInternalSession(key))) }
  }
  action abandonSessionRevoke(node: NodeName): bool = {
    if (SessionSteps::abandonSessionRevokeEnabled(world, node)) world' = SessionSteps::abandonSessionRevokeNext(world, node)
    else all { false, world' = world }
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

  pure def freshMetaAvailable(env: Environment, nodes: NodeStates, node: NodeName): bool =
    M::freshMetaAvailable(env, nodes, node)

  action requestStart(node: NodeName, replica: ReplicaKey, explicit: bool): bool = {
    if (Lifetime::requestStartEnabled(world, node, replica, explicit, MAX_REQUESTS)) world' = Lifetime::requestStartNext(world, node, replica, explicit, MAX_REQUESTS)
    else all { false, world' = world }
  }

  action readStartScope(request: RequestKey): bool = {
    if (Lifetime::readStartScopeEnabled(world, request)) world' = Lifetime::readStartScopeNext(world, request)
    else all { false, world' = world }
  }

  action reserveOwner(request: RequestKey): bool = {
    if (Lifetime::reserveOwnerEnabled(world, request)) world' = Lifetime::reserveOwnerNext(world, request)
    else all { false, world' = world }
  }

  action beginOwnerFactory(node: NodeName, group: GroupId): bool = {
    if (Lifetime::beginOwnerFactoryEnabled(world, node, group)) world' = Lifetime::beginOwnerFactoryNext(world, node, group)
    else all { false, world' = world }
  }

  action returnOwnerFactory(node: NodeName, group: GroupId, success: bool): bool = {
    if (Lifetime::returnOwnerFactoryEnabled(world, node, group, success)) world' = Lifetime::returnOwnerFactoryNext(world, node, group, success)
    else all { false, world' = world }
  }

  action expireOwnerFactory(node: NodeName, group: GroupId): bool = {
    if (Lifetime::expireOwnerFactoryEnabled(world, node, group)) world' = Lifetime::expireOwnerFactoryNext(world, node, group)
    else all { false, world' = world }
  }

  action finishOwnerReplay(node: NodeName, group: GroupId): bool = {
    if (Lifetime::finishOwnerReplayEnabled(world, node, group)) world' = Lifetime::finishOwnerReplayNext(world, node, group)
    else all { false, world' = world }
  }

  action authorizeOwner(node: NodeName, group: GroupId): bool = {
    if (Lifetime::authorizeOwnerEnabled(world, node, group)) world' = Lifetime::authorizeOwnerNext(world, node, group)
    else all { false, world' = world }
  }

  action publishOwner(node: NodeName, group: GroupId): bool = {
    if (Lifetime::publishOwnerEnabled(world, node, group)) world' = Lifetime::publishOwnerNext(world, node, group)
    else all { false, world' = world }
  }

  action replyStart(request: RequestKey): bool = {
    if (Lifetime::replyStartEnabled(world, request)) world' = Lifetime::replyStartNext(world, request)
    else all { false, world' = world }
  }

  action cancelStart(request: RequestKey): bool = {
    if (Lifetime::cancelStartEnabled(world, request)) world' = Lifetime::cancelStartNext(world, request)
    else all { false, world' = world }
  }

  action stopOwner(node: NodeName, replica: ReplicaKey, explicit: bool): bool = {
    if (Lifetime::stopOwnerEnabled(world, node, replica, explicit)) world' = Lifetime::stopOwnerNext(world, node, replica, explicit)
    else all { false, world' = world }
  }

  action joinOwner(node: NodeName, group: GroupId): bool = {
    if (Lifetime::joinOwnerEnabled(world, node, group)) world' = Lifetime::joinOwnerNext(world, node, group)
    else all { false, world' = world }
  }

  action beginOwnerClose(node: NodeName, group: GroupId): bool = {
    if (Lifetime::beginOwnerCloseEnabled(world, node, group)) world' = Lifetime::beginOwnerCloseNext(world, node, group)
    else all { false, world' = world }
  }

  action returnOwnerClose(node: NodeName, group: GroupId, success: bool): bool = {
    if (Lifetime::returnOwnerCloseEnabled(world, node, group, success)) world' = Lifetime::returnOwnerCloseNext(world, node, group, success)
    else all { false, world' = world }
  }

  action releaseOwner(node: NodeName, group: GroupId): bool = {
    if (Lifetime::releaseOwnerEnabled(world, node, group)) world' = Lifetime::releaseOwnerNext(world, node, group)
    else all { false, world' = world }
  }

  action stopOlderForFence(node: NodeName, resource: ResourceKey): bool = {
    if (NodeSteps::stopOlderForFenceEnabled(world, node, resource)) world' = NodeSteps::stopOlderForFenceNext(world, node, resource)
    else all { false, world' = world }
  }

  action completeHostFence(node: NodeName, resource: ResourceKey): bool = {
    if (NodeSteps::completeHostFenceEnabled(world, node, resource)) world' = NodeSteps::completeHostFenceNext(world, node, resource)
    else all { false, world' = world }
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
    if (NodeSteps::requestNodeStopEnabled(world, node, MAX_DELIVERIES)) world' = NodeSteps::requestNodeStopNext(world, node, MAX_DELIVERIES)
    else all { false, world' = world }
  }

  action joinAuthorityWorkers(node: NodeName): bool = {
    if (NodeSteps::joinAuthorityWorkersEnabled(world, node)) world' = NodeSteps::joinAuthorityWorkersNext(world, node)
    else all { false, world' = world }
  }

  action timeoutNodeStop(node: NodeName): bool = {
    if (NodeSteps::timeoutNodeStopEnabled(world, node)) world' = NodeSteps::timeoutNodeStopNext(world, node)
    else all { false, world' = world }
  }

  action expireOwnerClose(node: NodeName, group: GroupId): bool = {
    if (NodeSteps::expireOwnerCloseEnabled(world, node, group)) world' = NodeSteps::expireOwnerCloseNext(world, node, group)
    else all { false, world' = world }
  }

  action finishNodeStop(node: NodeName): bool = {
    if (NodeSteps::finishNodeStopEnabled(world, node)) world' = NodeSteps::finishNodeStopNext(world, node)
    else all { false, world' = world }
  }

  action beginNodeStart(node: NodeName): bool = {
    if (NodeSteps::beginNodeStartEnabled(world, node)) world' = NodeSteps::beginNodeStartNext(world, node)
    else all { false, world' = world }
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
    if (AuthoritySteps::submitControlEnabled(world, node, command, MAX_DELIVERIES)) world' = AuthoritySteps::submitControlNext(world, node, command, MAX_DELIVERIES)
    else all { false, world' = world }
  }

  action observeAuthorityLeader(node: NodeName, group: GroupId): bool = {
    if (AuthoritySteps::observeAuthorityLeaderEnabled(world, node, group)) world' = AuthoritySteps::observeAuthorityLeaderNext(world, node, group)
    else all { false, world' = world }
  }

  action dispatchGrantFrom(node: NodeName, key: GrantKey, resource: ResourceKey, proofIndex: int): bool = {
    nondet replica = AuthoritySteps::dispatchGrantFromCandidates(world, node, key, resource, proofIndex, MAX_DELIVERIES).oneOf()
    if (AuthoritySteps::dispatchGrantFromEnabled(world, node, key, resource, proofIndex, replica, MAX_DELIVERIES)) world' = AuthoritySteps::dispatchGrantFromNext(world, node, key, resource, proofIndex, replica, MAX_DELIVERIES)
    else all { false, world' = world }
  }

  action dispatchGrant(node: NodeName, key: GrantKey, resource: ResourceKey): bool =
    dispatchGrantFrom(node, key, resource, world.system.get(node).volatile.appliedMetaIndex)

  // ValidateAuthorityCourier reads a quorum-confirmed authority proof. Capture
  // that immutable query result separately from acceptance and native waiting.
  action confirmGrant(msg: Envelope): bool = {
    if (Receiver::confirmGrantEnabled(world, msg)) world' = Receiver::confirmGrantNext(world, msg)
    else all { false, world' = world }
  }

  action receiveGrant(msg: Envelope): bool = {
    if (Receiver::receiveGrantEnabled(world, msg)) world' = Receiver::receiveGrantNext(world, msg)
    else all { false, world' = world }
  }

  action consumeGrant(delivery: EffectDelivery): bool = {
    if (Receiver::consumeGrantEnabled(world, delivery, MAX_POSITION)) world' = Receiver::consumeGrantNext(world, delivery, MAX_POSITION)
    else all { false, world' = world }
  }

  action refuseGrant(delivery: EffectDelivery): bool = {
    if (Receiver::refuseGrantEnabled(world, delivery)) world' = Receiver::refuseGrantNext(world, delivery)
    else all { false, world' = world }
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
