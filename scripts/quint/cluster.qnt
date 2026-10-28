module cluster {
  import managed_stop_steps as Stops from "./managed/stop_steps"
  import managed_prepare_steps as Preparation from "./managed/prepare_steps"
  import managed_removal_steps as Removal from "./managed/removal_steps"
  import metaservice_admission_steps as AdmissionSteps from "./metaservice/admission_steps"
  import managed_membership_steps as Membership from "./managed/membership_steps"
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

  action guarded(canRun: bool, update: (World) => World): bool =
    if (canRun) world' = update(world) else all { false, world' = world }

  action init = world' = I::worldFor(NODE_POOL, FOUNDERS, LOAD_IDS, INITIAL_LEADER)

  action invoke(node: NodeName, request: ApiRequest): bool = {
    guarded(Environment::invokeEnabled(world, node, request, MAX_REQUESTS, MAX_DELIVERIES), before => Environment::invokeNext(before, node, request, MAX_REQUESTS, MAX_DELIVERIES))
  }

  action deliver(msg: Envelope): bool = {
    guarded(Environment::deliverEnabled(world, msg), before => Environment::deliverNext(before, msg))
  }

  action handle(node: NodeName): bool = {
    nondet tr = Calls::handleCandidates(world, node, MAX_DELIVERIES).oneOf()
    guarded(Calls::handleEnabled(world, node, tr, MAX_DELIVERIES), before => Calls::handleNext(before, node, tr, MAX_DELIVERIES))
  }

  action confirmRead(msg: Envelope): bool = {
    guarded(Calls::confirmReadEnabled(world, msg, MAX_DELIVERIES), before => Calls::confirmReadNext(before, msg, MAX_DELIVERIES))
  }

  action finishRead(key: RequestKey): bool = {
    guarded(Calls::finishReadEnabled(world, key), before => Calls::finishReadNext(before, key))
  }

  action prepareMutation(key: RequestKey): bool = {
    guarded(Calls::prepareMutationEnabled(world, key, rules), before => Calls::prepareMutationNext(before, key, rules))
  }

  action submitMutation(key: RequestKey): bool = {
    guarded(Calls::submitMutationEnabled(world, key, MAX_DELIVERIES), before => Calls::submitMutationNext(before, key, MAX_DELIVERIES))
  }

  action commitMutation(msg: Envelope): bool = {
    guarded(Calls::commitMutationEnabled(world, msg, MAX_POSITION, MAX_DELIVERIES, rules), before => Calls::commitMutationNext(before, msg, MAX_POSITION, MAX_DELIVERIES, rules))
  }

  action finishMutation(key: RequestKey): bool = {
    guarded(Calls::finishMutationEnabled(world, key), before => Calls::finishMutationNext(before, key))
  }

  action beginReseatRead(key: RequestKey): bool = {
    guarded(Calls::beginReseatReadEnabled(world, key, MAX_DELIVERIES), before => Calls::beginReseatReadNext(before, key, MAX_DELIVERIES))
  }

  action finishReseatRead(key: RequestKey): bool = {
    guarded(Calls::finishReseatReadEnabled(world, key), before => Calls::finishReseatReadNext(before, key))
  }

  action dispatchOperation(key: RequestKey): bool = {
    guarded(Operations::dispatchOperationEnabled(world, key, MAX_DELIVERIES), before => Operations::dispatchOperationNext(before, key, MAX_DELIVERIES))
  }

  action transmitOperationRead(node: NodeName, key: ProposalKey, retry: bool): bool = {
    guarded(Operations::transmitOperationReadEnabled(world, node, key, retry, MAX_DELIVERIES), before => Operations::transmitOperationReadNext(before, node, key, retry, MAX_DELIVERIES))
  }

  action startOperationRead(node: NodeName, key: ProposalKey): bool = transmitOperationRead(node, key, false)
  action retryOperationRead(node: NodeName, key: ProposalKey): bool = transmitOperationRead(node, key, true)

  action captureOperation(node: NodeName, key: ProposalKey): bool = {
    guarded(Operations::captureOperationEnabled(world, node, key, rules), before => Operations::captureOperationNext(before, node, key, rules))
  }

  action observeOperation(node: NodeName, key: ProposalKey, group: GroupId): bool = {
    guarded(Operations::observeOperationEnabled(world, node, key, group), before => Operations::observeOperationNext(before, node, key, group))
  }

  action observeOperationHealth(node: NodeName, key: ProposalKey, group: GroupId): bool = {
    guarded(Operations::observeOperationHealthEnabled(world, node, key, group), before => Operations::observeOperationHealthNext(before, node, key, group))
  }

  action beginOperationRecapture(node: NodeName, key: ProposalKey): bool = {
    guarded(Operations::beginOperationRecaptureEnabled(world, node, key), before => Operations::beginOperationRecaptureNext(before, node, key))
  }

  action buildOperation(node: NodeName, key: ProposalKey): bool = {
    guarded(Operations::buildOperationEnabled(world, node, key), before => Operations::buildOperationNext(before, node, key))
  }

  action submitOperation(node: NodeName, key: ProposalKey): bool = {
    guarded(Operations::submitOperationEnabled(world, node, key, MAX_DELIVERIES), before => Operations::submitOperationNext(before, node, key, MAX_DELIVERIES))
  }

  action finishOperation(node: NodeName, key: ProposalKey): bool = {
    guarded(Operations::finishOperationEnabled(world, node, key), before => Operations::finishOperationNext(before, node, key))
  }

  action replyOperation(node: NodeName, key: ProposalKey): bool = {
    guarded(Operations::replyOperationEnabled(world, node, key, MAX_DELIVERIES), before => Operations::replyOperationNext(before, node, key, MAX_DELIVERIES))
  }

  action observeMetaLeader(node: NodeName): bool = {
    guarded(Operations::observeMetaLeaderEnabled(world, node), before => Operations::observeMetaLeaderNext(before, node))
  }

  action retryRead(key: RequestKey): bool = {
    guarded(Operations::retryReadEnabled(world, key, MAX_DELIVERIES), before => Operations::retryReadNext(before, key, MAX_DELIVERIES))
  }

  action cancelCall(key: RequestKey): bool = {
    guarded(Operations::cancelCallEnabled(world, key), before => Operations::cancelCallNext(before, key))
  }

  action drop(msg: Envelope): bool = {
    guarded(Environment::dropEnabled(world, msg), before => Environment::dropNext(before, msg))
  }

  action duplicate(msg: Envelope): bool = {
    guarded(Environment::duplicateEnabled(world, msg, MAX_DELIVERIES), before => Environment::duplicateNext(before, msg, MAX_DELIVERIES))
  }

  action setLink(channel: Channel, source: NodeName, destination: NodeName, up: bool): bool = {
    guarded(Environment::setLinkEnabled(world, channel, source, destination, up), before => Environment::setLinkNext(before, channel, source, destination, up))
  }

  action crash(node: NodeName): bool = {
    guarded(Environment::crashEnabled(world, node), before => Environment::crashNext(before, node))
  }

  action restart(node: NodeName): bool = {
    guarded(Environment::restartEnabled(world, node, MAX_INCARNATION), before => Environment::restartNext(before, node, MAX_INCARNATION))
  }

  action commitProgress(group: GroupId): bool = {
    guarded(Native::commitProgressEnabled(world, group, MAX_POSITION), before => Native::commitProgressNext(before, group, MAX_POSITION))
  }

  action elect(group: GroupId, candidate: ReplicaKey): bool = {
    guarded(Native::electEnabled(world, group, candidate), before => Native::electNext(before, group, candidate))
  }

  action catchUp(group: GroupId, replica: ReplicaKey): bool = {
    guarded(Native::catchUpEnabled(world, group, replica), before => Native::catchUpNext(before, group, replica))
  }

  action applyMeta(node: NodeName): bool = {
    nondet replica = Native::applyMetaCandidates(world, node).oneOf()
    guarded(Native::applyMetaEnabled(world, node, replica), before => Native::applyMetaNext(before, node, replica))
  }

  action beginSession(node: NodeName): bool = {
    guarded(SessionSteps::beginSessionEnabled(world, node), before => SessionSteps::beginSessionNext(before, node))
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
    guarded(SessionSteps::renewSessionEnabled(world, node, MAX_DELIVERIES), before => SessionSteps::renewSessionNext(before, node, MAX_DELIVERIES))
  }

  pure def locallyLeader(host: LocalNode): bool = Sessions::locallyLeader(host)

  action handleSession(msg: Envelope): bool = {
    guarded(SessionSteps::handleSessionEnabled(world, msg, MAX_DELIVERIES), before => SessionSteps::handleSessionNext(before, msg, MAX_DELIVERIES))
  }

  action seedLeases(node: NodeName): bool = {
    guarded(SessionSteps::seedLeasesEnabled(world, node), before => SessionSteps::seedLeasesNext(before, node))
  }

  action forgetRevokedLease(node: NodeName, key: SessionKey): bool = {
    guarded(SessionSteps::forgetRevokedLeaseEnabled(world, node, key), before => SessionSteps::forgetRevokedLeaseNext(before, node, key))
  }

  action expireLease(node: NodeName, key: SessionKey): bool = {
    guarded(SessionSteps::expireLeaseEnabled(world, node, key), before => SessionSteps::expireLeaseNext(before, node, key))
  }

  action expireOwnLease(node: NodeName): bool = {
    guarded(SessionSteps::expireOwnLeaseEnabled(world, node), before => SessionSteps::expireOwnLeaseNext(before, node))
  }

  action cancelRenewal(node: NodeName): bool = {
    guarded(SessionSteps::cancelRenewalEnabled(world, node), before => SessionSteps::cancelRenewalNext(before, node))
  }

  action probeSession(node: NodeName, key: SessionKey): bool = {
    guarded(SessionSteps::probeSessionEnabled(world, node, key, MAX_DELIVERIES), before => SessionSteps::probeSessionNext(before, node, key, MAX_DELIVERIES))
  }

  action timeoutSessionProbe(node: NodeName, key: SessionKey): bool = {
    guarded(SessionSteps::timeoutSessionProbeEnabled(world, node, key), before => SessionSteps::timeoutSessionProbeNext(before, node, key))
  }

  action revokeExpiredSession(node: NodeName, key: SessionKey): bool = {
    val host = world.system.get(node)
    val lease = host.volatile.leases.get(key)
    all { M::running(host), locallyLeader(host), host.volatile.revokeGate == None,
      lease.phase == ExpiredObserved, lease.failedProbe,
      submitControl(node, SessionControl(RevokeInternalSession(key))) }
  }
  action abandonSessionRevoke(node: NodeName): bool = {
    guarded(SessionSteps::abandonSessionRevokeEnabled(world, node), before => SessionSteps::abandonSessionRevokeNext(before, node))
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
    guarded(Lifetime::requestStartEnabled(world, node, replica, explicit, MAX_REQUESTS), before => Lifetime::requestStartNext(before, node, replica, explicit, MAX_REQUESTS))
  }

  action readStartScope(request: RequestKey): bool = {
    guarded(Lifetime::readStartScopeEnabled(world, request), before => Lifetime::readStartScopeNext(before, request))
  }

  action reserveOwner(request: RequestKey): bool = {
    guarded(Lifetime::reserveOwnerEnabled(world, request), before => Lifetime::reserveOwnerNext(before, request))
  }

  action beginOwnerFactory(node: NodeName, group: GroupId): bool = {
    guarded(Lifetime::beginOwnerFactoryEnabled(world, node, group), before => Lifetime::beginOwnerFactoryNext(before, node, group))
  }

  action returnOwnerFactory(node: NodeName, group: GroupId, success: bool): bool = {
    guarded(Lifetime::returnOwnerFactoryEnabled(world, node, group, success), before => Lifetime::returnOwnerFactoryNext(before, node, group, success))
  }

  action expireOwnerFactory(node: NodeName, group: GroupId): bool = {
    guarded(Lifetime::expireOwnerFactoryEnabled(world, node, group), before => Lifetime::expireOwnerFactoryNext(before, node, group))
  }

  action finishOwnerReplay(node: NodeName, group: GroupId): bool = {
    guarded(Lifetime::finishOwnerReplayEnabled(world, node, group), before => Lifetime::finishOwnerReplayNext(before, node, group))
  }

  action authorizeOwner(node: NodeName, group: GroupId): bool = {
    guarded(Lifetime::authorizeOwnerEnabled(world, node, group), before => Lifetime::authorizeOwnerNext(before, node, group))
  }

  action publishOwner(node: NodeName, group: GroupId): bool = {
    guarded(Lifetime::publishOwnerEnabled(world, node, group), before => Lifetime::publishOwnerNext(before, node, group))
  }

  action replyStart(request: RequestKey): bool = {
    guarded(Lifetime::replyStartEnabled(world, request), before => Lifetime::replyStartNext(before, request))
  }

  action cancelStart(request: RequestKey): bool = {
    guarded(Lifetime::cancelStartEnabled(world, request), before => Lifetime::cancelStartNext(before, request))
  }

  action stopOwner(node: NodeName, replica: ReplicaKey, explicit: bool): bool = {
    guarded(Lifetime::stopOwnerEnabled(world, node, replica, explicit), before => Lifetime::stopOwnerNext(before, node, replica, explicit))
  }

  action joinOwner(node: NodeName, group: GroupId): bool = {
    guarded(Lifetime::joinOwnerEnabled(world, node, group), before => Lifetime::joinOwnerNext(before, node, group))
  }

  action beginOwnerClose(node: NodeName, group: GroupId): bool = {
    guarded(Lifetime::beginOwnerCloseEnabled(world, node, group), before => Lifetime::beginOwnerCloseNext(before, node, group))
  }

  action returnOwnerClose(node: NodeName, group: GroupId, success: bool): bool = {
    guarded(Lifetime::returnOwnerCloseEnabled(world, node, group, success), before => Lifetime::returnOwnerCloseNext(before, node, group, success))
  }

  action releaseOwner(node: NodeName, group: GroupId): bool = {
    guarded(Lifetime::releaseOwnerEnabled(world, node, group), before => Lifetime::releaseOwnerNext(before, node, group))
  }

  action stopOlderForFence(node: NodeName, resource: ResourceKey): bool = {
    guarded(NodeSteps::stopOlderForFenceEnabled(world, node, resource), before => NodeSteps::stopOlderForFenceNext(before, node, resource))
  }

  action completeHostFence(node: NodeName, resource: ResourceKey): bool = {
    guarded(NodeSteps::completeHostFenceEnabled(world, node, resource), before => NodeSteps::completeHostFenceNext(before, node, resource))
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
    guarded(NodeSteps::requestNodeStopEnabled(world, node, MAX_DELIVERIES), before => NodeSteps::requestNodeStopNext(before, node, MAX_DELIVERIES))
  }

  action joinAuthorityWorkers(node: NodeName): bool = {
    guarded(NodeSteps::joinAuthorityWorkersEnabled(world, node), before => NodeSteps::joinAuthorityWorkersNext(before, node))
  }

  action timeoutNodeStop(node: NodeName): bool = {
    guarded(NodeSteps::timeoutNodeStopEnabled(world, node), before => NodeSteps::timeoutNodeStopNext(before, node))
  }

  action expireOwnerClose(node: NodeName, group: GroupId): bool = {
    guarded(NodeSteps::expireOwnerCloseEnabled(world, node, group), before => NodeSteps::expireOwnerCloseNext(before, node, group))
  }

  action finishNodeStop(node: NodeName): bool = {
    guarded(NodeSteps::finishNodeStopEnabled(world, node), before => NodeSteps::finishNodeStopNext(before, node))
  }

  action beginNodeStart(node: NodeName): bool = {
    guarded(NodeSteps::beginNodeStartEnabled(world, node), before => NodeSteps::beginNodeStartNext(before, node))
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
    guarded(AuthoritySteps::submitControlEnabled(world, node, command, MAX_DELIVERIES), before => AuthoritySteps::submitControlNext(before, node, command, MAX_DELIVERIES))
  }

  action observeAuthorityLeader(node: NodeName, group: GroupId): bool = {
    guarded(AuthoritySteps::observeAuthorityLeaderEnabled(world, node, group), before => AuthoritySteps::observeAuthorityLeaderNext(before, node, group))
  }

  action dispatchGrantFrom(node: NodeName, key: GrantKey, resource: ResourceKey, proofIndex: int): bool = {
    nondet replica = AuthoritySteps::dispatchGrantFromCandidates(world, node, key, resource, proofIndex, MAX_DELIVERIES).oneOf()
    guarded(AuthoritySteps::dispatchGrantFromEnabled(world, node, key, resource, proofIndex, replica, MAX_DELIVERIES), before => AuthoritySteps::dispatchGrantFromNext(before, node, key, resource, proofIndex, replica, MAX_DELIVERIES))
  }

  action dispatchGrant(node: NodeName, key: GrantKey, resource: ResourceKey): bool =
    dispatchGrantFrom(node, key, resource, world.system.get(node).volatile.appliedMetaIndex)

  // ValidateAuthorityCourier reads a quorum-confirmed authority proof. Capture
  // that immutable query result separately from acceptance and native waiting.
  action confirmGrant(msg: Envelope): bool = {
    guarded(Receiver::confirmGrantEnabled(world, msg), before => Receiver::confirmGrantNext(before, msg))
  }

  action receiveGrant(msg: Envelope): bool = {
    guarded(Receiver::receiveGrantEnabled(world, msg), before => Receiver::receiveGrantNext(before, msg))
  }

  action consumeGrant(delivery: EffectDelivery): bool = {
    guarded(Receiver::consumeGrantEnabled(world, delivery, MAX_POSITION), before => Receiver::consumeGrantNext(before, delivery, MAX_POSITION))
  }

  action refuseGrant(delivery: EffectDelivery): bool = {
    guarded(Receiver::refuseGrantEnabled(world, delivery), before => Receiver::refuseGrantNext(before, delivery))
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

  action checkpointEffect(node: NodeName, approved: ApprovedAction): bool = {
    val group = G::groupOf(approved.step.target)
    val c = world.extensions.consensus.get(group)
    all {
      approved.step.kind != TransferLeadership or (M::hasCommitPath(c, world.system, M::linksFor(world.extensions.network, group)) and
        match c.leaderAuthority { | Some(leader) =>
          M::communicates(M::linksFor(world.extensions.network, group), node, leader.node) | None => false }),
      submitControl(node, CompleteStep({ approved: approved, observedLeader: c.leaderAuthority })),
    }
  }

  action checkpointTransfer(node: NodeName, approved: ApprovedAction): bool = all {
    approved.step.kind == TransferLeadership, checkpointEffect(node, approved),
  }

  action receiveEffectResult(msg: Envelope): bool =
    guarded(Receiver::receiveResultEnabled(world, msg), before => Receiver::receiveResultNext(before, msg))

  action reportCachedEffect(node: NodeName, receipt: Receipt): bool = all {
    world.system.get(node).volatile.effectResults.contains(receipt),
    submitControl(node, RecordEffect(receipt)),
  }

  action authorityStep = any {
    nondet node = NODE_POOL.oneOf()
    nondet msg = world.messages.get(node).oneOf()
    receiveEffectResult(msg),
    nondet node = NODE_POOL.filter(n => M::running(world.system.get(n)) and
      world.system.get(n).volatile.observations.keys().contains("_meta")).oneOf()
    nondet receipt = world.system.get(node).volatile.effectResults.oneOf()
    val local = world.extensions.metaLog.get(world.system.get(node).volatile.appliedMetaIndex)
    all { not(local.effectReceipts.contains(receipt)), reportCachedEffect(node, receipt) },
    nondet node = NODE_POOL.filter(n => M::running(world.system.get(n)) and
      world.system.get(n).volatile.observations.keys().contains("_meta")).oneOf()
    val local = world.extensions.metaLog.get(world.system.get(node).volatile.appliedMetaIndex)
    nondet operation = local.operations.keys().oneOf()
    nondet group = local.operations.get(operation).plan.blocks.keys().oneOf()
    nondet stepIndex = local.operations.get(operation).plan.blocks.get(group).indices().oneOf()
    val step = local.operations.get(operation).plan.blocks.get(group).nth(stepIndex)
    checkpointCatchUp(node, { operation: operation, step: step }),
    nondet node = NODE_POOL.filter(n => M::running(world.system.get(n)) and
      world.system.get(n).volatile.observations.keys().contains("_meta")).oneOf()
    val local = world.extensions.metaLog.get(world.system.get(node).volatile.appliedMetaIndex)
    nondet grant = local.grants.values().filter(g => g.purpose == Effect).oneOf()
    match grant.effect { | Some(effect) => match grant.planStep {
      | Some(step) => checkpointEffect(node, { operation: effect.operation, step: step })
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
    nondet caller = NODE_POOL.filter(n => M::running(world.system.get(n)) and world.system.get(n).durable.pin != None).oneOf()
    nondet node = NODE_POOL.oneOf()
    nondet edit = Set(AddNode({ name: node, member: 0, state: Active, nodeType: "worker", labels: Map() }),
      DrainNode(node), ActivateNode(node), ExcludeNode(node), ForceRetireNode({ name: node, reason: "operator" })).oneOf()
    invoke(caller, Mutate({ edit: edit, options: { ...generatedOptions, approve: true } })),
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
    membershipStep,
    admissionStep,
    cleanupStopStep,
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

  action requestJoin(node: NodeName, seed: NodeName): bool =
    guarded(AdmissionSteps::requestJoinEnabled(world, node, seed, MAX_DELIVERIES),
      before => AdmissionSteps::requestJoinNext(before, node, seed))

  action confirmJoin(msg: Envelope): bool =
    guarded(AdmissionSteps::confirmJoinEnabled(world, msg), before => AdmissionSteps::confirmJoinNext(before, msg))

  action bindJoin(msg: Envelope): bool = match AdmissionSteps::bindCommand(world, msg) {
    | Some(command) => all { AdmissionSteps::joinApplied(world, msg),
        submitControl(msg.destination, AdmissionControl(command)) }
    | None => all { false, world' = world }
  }

  action replyJoin(msg: Envelope): bool =
    guarded(AdmissionSteps::replyJoinEnabled(world, msg, MAX_DELIVERIES), before => AdmissionSteps::replyJoinNext(before, msg))

  action receiveJoin(msg: Envelope): bool =
    guarded(AdmissionSteps::receiveJoinEnabled(world, msg), before => AdmissionSteps::receiveJoinNext(before, msg))

  action pinJoin(node: NodeName): bool =
    guarded(AdmissionSteps::pinJoinEnabled(world, node), before => AdmissionSteps::pinJoinNext(before, node))

  action startJoinedMeta(node: NodeName): bool =
    guarded(AdmissionSteps::startJoinedMetaEnabled(world, node), before => AdmissionSteps::startJoinedMetaNext(before, node))

  action prepareMeta(delivery: EffectDelivery): bool =
    guarded(AdmissionSteps::prepareMetaEnabled(world, delivery, MAX_POSITION), before => AdmissionSteps::prepareMetaNext(before, delivery, MAX_DELIVERIES))

  action reportHostEffect(node: NodeName, receipt: Receipt): bool = match receipt.resource {
    | HostResource(host) => all {
        world.system.get(host.node).durable.receipts.contains(receipt),
        M::running(world.system.get(host.node)),
        M::communicates(world.extensions.network.metaLinks, node, host.node),
        submitControl(node, RecordEffect(receipt)),
      }
    | _ => all { false, world' = world }
  }

  action announcePrepared(node: NodeName, seed: NodeName): bool =
    guarded(AdmissionSteps::announcePreparedEnabled(world, node, seed, MAX_DELIVERIES),
      before => AdmissionSteps::announcePreparedNext(before, node, seed))

  action acceptPrepared(msg: Envelope): bool = match msg.payload {
    | JoinPrepared(a) => {
        val command = AdmissionControl(ReadyAdmission(a))
        guarded(AdmissionSteps::readyMessage(world, msg) and
          AuthoritySteps::submitControlEnabled(world, msg.destination, command, MAX_DELIVERIES),
          before => apply_custom_effect(AuthoritySteps::submitControlNext(before, msg.destination, command, MAX_DELIVERIES), Consume(msg)))
      }
    | _ => all { false, world' = world }
  }

  action checkpointCatchUp(node: NodeName, approved: ApprovedAction): bool = match approved.step.target {
    | ReplicaTarget(replica) => {
        val c = world.extensions.consensus.get(replica.group)
        all { approved.step.kind == WaitCatchUp,
          c.voters.union(c.learners).contains(replica),
          M::liveIdentity(world.system, replica), c.catchUp.getOrElse(replica, Behind) == CaughtUp,
          M::hasCommitPath(c, world.system, M::linksFor(world.extensions.network, replica.group)),
          match c.leaderAuthority { | Some(leader) =>
            M::communicates(M::linksFor(world.extensions.network, replica.group), node, leader.node) | None => false },
          submitControl(node, CompleteCatchUp(approved)) }
      }
    | _ => all { false, world' = world }
  }

  action admissionStep = any {
    nondet node = NODE_POOL.oneOf()
    nondet seed = NODE_POOL.oneOf()
    any { requestJoin(node, seed), announcePrepared(node, seed) },
    nondet node = NODE_POOL.oneOf()
    nondet msg = world.messages.get(node).oneOf()
    any { confirmJoin(msg), bindJoin(msg), replyJoin(msg), receiveJoin(msg), acceptPrepared(msg) },
    nondet node = NODE_POOL.oneOf()
    any { pinJoin(node), startJoinedMeta(node) },
    nondet node = NODE_POOL.oneOf()
    nondet delivery = world.system.get(node).volatile.waitingEffects.oneOf()
    prepareMeta(delivery),
    nondet sender = NODE_POOL.filter(n => M::running(world.system.get(n)) and
      world.system.get(n).volatile.observations.keys().contains("_meta")).oneOf()
    nondet host = NODE_POOL.oneOf()
    nondet receipt = world.system.get(host).durable.receipts.oneOf()
    val local = world.extensions.metaLog.get(world.system.get(sender).volatile.appliedMetaIndex)
    all { not(local.effectReceipts.contains(receipt)), reportHostEffect(sender, receipt) },
  }

  action installNativeAuthority(delivery: EffectDelivery): bool =
    guarded(Membership::installNativeEnabled(world, delivery, MAX_POSITION),
      before => Membership::installNativeNext(before, delivery))

  action precheckNative(delivery: EffectDelivery): bool =
    guarded(Membership::precheckNativeEnabled(world, delivery),
      before => Membership::precheckNativeNext(before, delivery))

  action proposeNative(delivery: EffectDelivery): bool =
    guarded(Membership::proposeNativeEnabled(world, delivery),
      before => Membership::proposeNativeNext(before, delivery))

  action commitNative(group: GroupId): bool =
    guarded(Membership::commitNativeEnabled(world, group, MAX_POSITION),
      before => Membership::commitNativeNext(before, group, MAX_DELIVERIES))

  action discardNative(group: GroupId): bool =
    guarded(Membership::discardNativeEnabled(world, group), before => Membership::discardNativeNext(before, group))

  action replayNative(delivery: EffectDelivery): bool =
    guarded(Membership::replayNativeEnabled(world, delivery),
      before => Membership::replayNativeNext(before, delivery))

  action applyRemovedMeta(node: NodeName, receipt: Receipt): bool =
    guarded(Removal::applyRemovalEnabled(world, node, receipt), before => Removal::applyRemovalNext(before, node, receipt))

  action reportNativeStatus(node: NodeName, delivery: EffectDelivery): bool = match Membership::nativeStatus(world, delivery) {
    | Some(status) => {
        val approved = { operation: delivery.grant.effect.unwrap().operation, step: delivery.grant.planStep.unwrap() }
        val local = world.extensions.metaLog.get(world.system.get(node).volatile.appliedMetaIndex)
        val group = delivery.grant.key.group
        if (not(M::running(world.system.get(node))) or
            not(world.system.get(node).volatile.observations.keys().contains("_meta")) or
            not(local.operations.keys().contains(approved.operation))) all { false, world' = world } else all {
          world.system.get(delivery.receiver.node).volatile.waitingEffects.contains(delivery),
          M::running(world.system.get(delivery.receiver.node)),
          delivery.sender == { node: node, incarnation: world.system.get(node).volatile.incarnation },
          M::communicates(M::linksFor(world.extensions.network, group), node, delivery.receiver.node),
          local.operations.keys().contains(approved.operation),
          local.operations.get(approved.operation).blocks.get(group) != status,
          submitControl(node, RecordStepStatus({ approved: approved, phase: status.phase, reason: status.reason })),
        }
      }
    | None => all { false, world' = world }
  }

  action beginLoadPreparation(delivery: EffectDelivery): bool =
    guarded(Preparation::beginPrepareEnabled(world, delivery, MAX_REQUESTS, MAX_POSITION),
      before => Preparation::beginPrepareNext(before, delivery, MAX_REQUESTS))

  action finishLoadPreparation(delivery: EffectDelivery): bool =
    guarded(Preparation::finishPrepareEnabled(world, delivery, MAX_POSITION),
      before => Preparation::finishPrepareNext(before, delivery, MAX_DELIVERIES))

  action beginGrantedStop(delivery: EffectDelivery): bool =
    guarded(Stops::beginStopEnabled(world, delivery, MAX_POSITION), before => Stops::beginStopNext(before, delivery))

  action finishGrantedStop(delivery: EffectDelivery): bool =
    guarded(Stops::finishStopEnabled(world, delivery, MAX_POSITION), before => Stops::finishStopNext(before, delivery, MAX_DELIVERIES))

  action cleanupStopStep = any {
    nondet node = NODE_POOL.filter(n => M::running(world.system.get(n)) and
      world.system.get(n).volatile.observations.keys().contains("_meta")).oneOf()
    val local = world.extensions.metaLog.get(world.system.get(node).volatile.appliedMetaIndex)
    nondet key = local.cleanup.keys().filter(r => local.cleanup.get(r).stopped == None and local.cleanup.get(r).stopAction != None).oneOf()
    submitControl(node, IssueCleanupStop(local.cleanup.get(key).stopAction.unwrap())),
    nondet node = NODE_POOL.oneOf()
    nondet delivery = world.system.get(node).volatile.waitingEffects.oneOf()
    any { beginGrantedStop(delivery), finishGrantedStop(delivery) },
  }

  action membershipStep = any {
    nondet node = NODE_POOL.oneOf()
    nondet delivery = world.system.get(node).volatile.waitingEffects.oneOf()
    any { beginLoadPreparation(delivery), finishLoadPreparation(delivery) },
    nondet node = NODE_POOL.oneOf()
    nondet receipt = world.extensions.consensus.get("_meta").authorityReceipts.oneOf()
    applyRemovedMeta(node, receipt),
    nondet node = NODE_POOL.oneOf()
    nondet delivery = world.system.get(node).volatile.waitingEffects.filter(d => Membership::isNative(d)).oneOf()
    any { installNativeAuthority(delivery), precheckNative(delivery), proposeNative(delivery), replayNative(delivery),
      reportNativeStatus(delivery.sender.node, delivery) },
    nondet group = world.extensions.consensus.keys().oneOf()
    any { commitNative(group), discardNative(group) },
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
