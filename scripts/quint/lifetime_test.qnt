module lifetime_test {
  import types.* from "./types"
  import basicSpells.* from "./vendor/spells/basicSpells"
  import effects_test.* from "./effects_test"
  import lifecycle as Lifetime from "./lifecycle"
  import sessions as LeaseRules from "./sessions"
  import membership as Native from "./membership"
  export effects_test.*

  pure val localReplica: ReplicaKey = { cluster: "c1", group: "a", node: "n2", member: 2, replica: 2 }
  pure val localStart: RequestKey = { caller: { node: "n2", incarnation: 1 }, sequence: 1 }
  pure val oldSession: SessionKey = { owner: { node: "n2", incarnation: 1 }, generation: 1 }
  pure val newSession: SessionKey = { owner: { node: "n2", incarnation: 2 }, generation: 1 }

  action closeLocal(node: NodeName, group: GroupId): bool = joinOwner(node, group)
    .then(beginOwnerClose(node, group)).then(returnOwnerClose(node, group, true)).then(releaseOwner(node, group))
  action localStopped: bool = init.then(stopOwner("n2", localReplica, true)).then(closeLocal("n2", "a"))
  action localReserved: bool = localStopped.then(requestStart("n2", localReplica, true))
    .then(readStartScope(localStart)).then(reserveOwner(localStart))
  action startupBody(node: NodeName, group: GroupId): bool = beginOwnerFactory(node, group)
    .then(returnOwnerFactory(node, group, true)).then(finishOwnerReplay(node, group))
    .then(authorizeOwner(node, group)).then(publishOwner(node, group))
  action sessionMessage(kind: str, destination: NodeName): bool = {
    nondet msg = world.extensions.network.pending.filter(m => m.destination == destination and match m.payload {
      | SessionRenew(_) => kind == "renew" | SessionRenewed(_) => kind == "renewed"
      | SessionProbe(_) => kind == "probe" | SessionProbed(_) => kind == "probed"
      | _ => false }).oneOf()
    deliver(msg).then(handleSession(msg))
  }

  run retainedLocalRestartReplaysBeforePublicationTest = localReserved
    .expect(not(world.system.get("n2").volatile.owners.get("a").published))
    .then(startupBody("n2", "a")).then(replyStart(localStart))
    .expect(world.system.get("n2").volatile.starts.get(localStart).phase == StartSucceeded)
    .expect(world.system.get("n2").volatile.holds == Set()).expect(safety)

  run concurrentLocalStartsShareFactoryTest = localReserved.then(beginOwnerFactory("n2", "a"))
    .then(requestStart("n2", localReplica, true)).then(readStartScope({ ...localStart, sequence: 2 }))
    .then(reserveOwner({ ...localStart, sequence: 2 }))
    .expect(world.system.get("n2").volatile.owners.get("a").reservation == Some(localStart))
    .expect(world.system.get("n2").volatile.owners.get("a").waiters.size() == 2)
    .then(returnOwnerFactory("n2", "a", true)).then(finishOwnerReplay("n2", "a"))
    .then(authorizeOwner("n2", "a")).then(publishOwner("n2", "a"))
    .then(replyStart(localStart)).then(replyStart({ ...localStart, sequence: 2 }))
    .expect(world.system.get("n2").volatile.starts.values().forall(w => w.phase == StartSucceeded)).expect(safety)

  run cancelledLocalWaiterStillOwnsStartupTest = localReserved.then(beginOwnerFactory("n2", "a"))
    .then(cancelStart(localStart)).then(returnOwnerFactory("n2", "a", true))
    .then(finishOwnerReplay("n2", "a")).then(authorizeOwner("n2", "a")).then(publishOwner("n2", "a"))
    .expect(world.system.get("n2").volatile.starts.get(localStart).error == Some(WaitCancelled))
    .expect(world.system.get("n2").volatile.owners.get("a").published).expect(safety)

  run lateFactoryNeedsCloseDespiteTimeoutTest = localReserved.then(beginOwnerFactory("n2", "a"))
    .then(expireOwnerFactory("n2", "a")).then(returnOwnerFactory("n2", "a", true))
    .expect(world.system.get("n2").volatile.owners.get("a").applicationClose == Acquired)
    .expect(not(Lifetime::released(world.system.get("n2").volatile.owners.get("a"))))
    .then(beginOwnerClose("n2", "a")).then(returnOwnerClose("n2", "a", false))
    .expect(not(Lifetime::released(world.system.get("n2").volatile.owners.get("a"))))
    .then(beginOwnerClose("n2", "a")).then(returnOwnerClose("n2", "a", true)).then(releaseOwner("n2", "a"))
    .expect(world.system.get("n2").volatile.starts.get(localStart).phase == StartRefused).expect(safety)

  run factoryFailureDoesNotCloseUnownedApplicationTest = localReserved.then(beginOwnerFactory("n2", "a"))
    .then(returnOwnerFactory("n2", "a", false)).then(releaseOwner("n2", "a"))
    .expect(world.system.get("n2").volatile.owners.get("a").applicationClose == NotAcquired).expect(safety)

  run localHoldBlocksAutomaticRestartTest = localStopped.then(requestStart("n2", localReplica, false))
    .then(readStartScope(localStart)).then(reserveOwner(localStart))
    .expect(world.system.get("n2").volatile.starts.get(localStart).phase == StartRefused)
    .expect(world.system.get("n2").volatile.holds == Set(localReplica)).expect(safety)

  run nodeStopKeepsMetaUntilApplicationReleaseTest = init.then(requestNodeStop("n2"))
    .then(closeLocal("n2", "a")).then(joinOwner("n2", "b")).then(beginOwnerClose("n2", "b"))
    .then(returnOwnerClose("n2", "b", false))
    .expect(world.system.get("n2").volatile.metaRunning)
    .then(beginOwnerClose("n2", "b")).then(returnOwnerClose("n2", "b", true)).then(releaseOwner("n2", "b"))
    .then(finishNodeStop("n2"))
    .expect(not(world.system.get("n2").volatile.metaRunning))
    .expect(world.system.get("n2").durable.replicaEvidence.keys().contains(localReplica)).expect(safety)

  run retainedProcessRestartReauthorizesLocalOwnerTest = init.then(crash("n2")).then(restart("n2"))
    .then(requestStart("n2", localReplica, false))
    .then(readStartScope({ ...localStart, caller: { node: "n2", incarnation: 2 } }))
    .then(reserveOwner({ ...localStart, caller: { node: "n2", incarnation: 2 } }))
    .then(startupBody("n2", "a"))
    .expect(world.system.get("n2").volatile.owners.get("a").process.incarnation == 2)
    .expect(world.system.get("n2").volatile.ownLease != Live).expect(safety)

  run renewalIsNotQuorumProofTest = init.then(expireOwnLease("n1"))
    .then(setLink(MetaChannel, "n1", "n2", false)).then(setLink(MetaChannel, "n1", "n3", false))
    .then(renewSession("n1")).then(sessionMessage("renew", "n1")).then(sessionMessage("renewed", "n1"))
    .expect(world.system.get("n1").volatile.ownLease == Live)
    .expect(not(Native::hasCommitPath(world.extensions.consensus.get("_meta"), world.system, world.extensions.network.metaLinks)))
    .expect(safety)

  run oldRenewalReplyCannotReadyRestartTest = init.then(expireOwnLease("n2"))
    .then(renewSession("n2")).then(sessionMessage("renew", "n1"))
    .then(crash("n2")).then(restart("n2")).then(sessionMessage("renewed", "n2"))
    .expect(world.system.get("n2").volatile.phase == Starting)
    .expect(world.system.get("n2").volatile.ownLease != Live).expect(safety)

  run expiredRenewalReplyDoesNotExtendDeadlineTest = init.then(expireOwnLease("n2"))
    .then(renewSession("n2")).then(sessionMessage("renew", "n1"))
    .then(expireOwnLease("n2")).then(sessionMessage("renewed", "n2"))
    .expect(world.system.get("n2").volatile.ownLease == ExpiredObserved).expect(safety)

  run expiryProbesExactLivingProcessTest = init.then(seedLeases("n1")).then(expireLease("n1", oldSession))
    .expect(world.extensions.metadata.sessions.get(oldSession).phase == Live)
    .then(probeSession("n1", oldSession)).then(sessionMessage("probe", "n2")).then(sessionMessage("probed", "n1"))
    .expect(world.system.get("n1").volatile.leases.get(oldSession).phase == Live)
    .expect(world.extensions.metadata.sessions.get(oldSession).phase == Live).expect(safety)

  action revokeOldProcess: bool = seedLeases("n1").then(expireLease("n1", oldSession))
    .then(probeSession("n1", oldSession)).then(sessionMessage("probe", "n2")).then(sessionMessage("probed", "n1"))
    .then(revokeExpiredSession("n1", oldSession))
    .then(controlCommit(SessionControl(RevokeInternalSession(oldSession)))).then(applyMeta("n1"))
    .then({ val gate = world.system.get("n1").volatile.revokeGate
      nondet msg = world.extensions.network.pending.filter(m => match m.payload {
        | MetaAck(ack) => match gate { | Some(g) => ack.key == g.proposal | None => false }
        | _ => false }).oneOf()
      deliver(msg).then(handleSession(msg)) })

  run retainedRestartEstablishesExactNewSessionTest = init.then(crash("n2")).then(restart("n2"))
    .then(revokeOldProcess).then(applyMeta("n2")).then(observeMetaLeader("n2"))
    .then(beginSession("n2")).then(proposeSession("n2"))
    .then(controlCommit(SessionControl(GrantInternalSession({ key: newSession, member: 2 })))).then(applyMeta("n2"))
    .then(proposeSession("n2")).then(controlCommit(SessionControl(BindInternalSession(newSession))))
    .then(applyMeta("n2")).then(applyMeta("n1"))
    .then(renewSession("n2")).then(sessionMessage("renew", "n1")).then(sessionMessage("renewed", "n2"))
    .expect(world.extensions.metadata.activeSessions.get("n2") == newSession)
    .expect(world.extensions.metadata.couriers.get("n2").process.incarnation == 2)
    .expect(world.system.get("n2").volatile.phase == Ready).expect(safety)

  pure val thirdReplica: ReplicaKey = { cluster: "c1", group: "a", node: "n3", member: 3, replica: 3 }
  pure val thirdStart: RequestKey = { caller: { node: "n3", incarnation: 1 }, sequence: 1 }
  action fenceDuringFactory: bool = init.then(stopOwner("n3", thirdReplica, true)).then(closeLocal("n3", "a"))
    .then(requestStart("n3", thirdReplica, true)).then(readStartScope(thirdStart)).then(reserveOwner(thirdStart))
    .then(beginOwnerFactory("n3", "a")).then(retainHostFence).then(applyMeta("n3"))
    .then(dispatchGrant("n1", hostFenceKey, hostResource)).then(receiveQueuedGrant)
    .then({ nondet delivery = world.system.get("n3").volatile.waitingEffects.oneOf()
      consumeGrant(delivery) })

  run hostFenceWaitsForLateFactoryAndCloseTest = fenceDuringFactory
    .expect(world.system.get("n3").durable.pendingFences.keys() == Set(hostResource))
    .expect(world.system.get("n3").durable.fences.size() == 0)
    .then(stopOlderForFence("n3", hostResource)).then(returnOwnerFactory("n3", "a", true))
    .then(beginOwnerClose("n3", "a")).then(expireOwnerClose("n3", "a"))
    .expect(world.system.get("n3").durable.fences.size() == 0)
    .then(returnOwnerClose("n3", "a", false)).then(beginOwnerClose("n3", "a"))
    .then(returnOwnerClose("n3", "a", true)).then(releaseOwner("n3", "a"))
    .then(completeHostFence("n3", hostResource))
    .expect(world.extensions.metadata.revocations.get(oldHostKey).provenResources == Set())
    .then({ nondet proof = world.system.get("n3").durable.fences.oneOf()
      reportHostFence("n3", proof).then(controlCommit(RecordFence(proof))) })
    .expect(world.extensions.metadata.revocations.get(oldHostKey).provenResources == Set(hostResource)).expect(safety)

  run interruptedHostFenceResumesSameDurablePositionTest = retainedHostFenceInit.then(applyMeta("n3"))
    .then(dispatchGrant("n1", hostFenceKey, hostResource)).then(receiveQueuedGrant)
    .then({ nondet delivery = world.system.get("n3").volatile.waitingEffects.oneOf()
      consumeGrant(delivery) })
    .then(crash("n3")).then(restart("n3"))
    .expect(world.system.get("n3").durable.pendingFences.keys() == Set(hostResource))
    .expect(world.system.get("n3").volatile.fenceWorkers == Set())
    .then(dispatchGrant("n1", hostFenceKey, hostResource)).then(receiveQueuedGrant)
    .then({ nondet delivery = world.system.get("n3").volatile.waitingEffects.oneOf()
      consumeGrant(delivery) }).then(completeHostFence("n3", hostResource))
    .expect(world.system.get("n3").durable.journalPosition == 1)
    .expect(world.system.get("n3").durable.fences.size() == 1).expect(safety)

  run stopAndCloseTimeoutsDoNotReleaseOwnersTest = init.then(requestNodeStop("n2"))
    .then(joinOwner("n2", "a")).then(beginOwnerClose("n2", "a"))
    .then(timeoutNodeStop("n2")).then(expireOwnerClose("n2", "a"))
    .expect(world.system.get("n2").volatile.stopTimedOut)
    .expect(world.system.get("n2").volatile.owners.get("a").applicationClose == Closing)
    .expect(not(Lifetime::released(world.system.get("n2").volatile.owners.get("a"))))
    .expect(world.system.get("n2").volatile.metaRunning)
    .then(returnOwnerClose("n2", "a", true)).then(releaseOwner("n2", "a"))
    .then(closeLocal("n2", "b")).then(finishNodeStop("n2")).expect(safety)

  run earlyLocalPublicationHasCounterexampleTest = localReserved.then(beginOwnerFactory("n2", "a")).then({
    world' = { ...world, system: world.system.setBy("n2", host => { ...host, volatile: { ...host.volatile,
      owners: host.volatile.owners.setBy("a", owner => Lifetime::publish(owner)) } }) }
  }).expect(not(safety))
  run earlyLocalReleaseHasCounterexampleTest = localReserved.then(beginOwnerFactory("n2", "a"))
    .then(stopOwner("n2", localReplica, false)).then({
      world' = { ...world, system: world.system.setBy("n2", host => { ...host, volatile: { ...host.volatile,
        owners: host.volatile.owners.setBy("a", owner => Lifetime::finishRelease(owner)) } }) }
    }).expect(not(safety))

  run renewalInvalidatesPendingExpiryProbeTest = init.then(seedLeases("n1")).then(expireLease("n1", oldSession))
    .then(probeSession("n1", oldSession)).then(expireOwnLease("n2"))
    .then(renewSession("n2")).then(sessionMessage("renew", "n1"))
    .then(sessionMessage("probe", "n2")).then(sessionMessage("probed", "n1"))
    .expect(world.system.get("n1").volatile.leases.get(oldSession).phase == Live)
    .expect(not(world.system.get("n1").volatile.leases.get(oldSession).failedProbe)).expect(safety)

  val republishedOwner = world.system.values().exists(n => n.volatile.owners.values().exists(owner =>
    owner.phase == OwnerReady and owner.reservation != None))
  val releasedOwner = world.system.values().exists(n => n.volatile.owners.values().exists(owner => owner.phase == OwnerStopped))
}
