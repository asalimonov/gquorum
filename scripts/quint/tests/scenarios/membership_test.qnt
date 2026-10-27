module membership_test {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import membership as M from "../../consensus/membership"
  import initial as I from "../../harness/initial"
  import cluster_test.* from "./cluster_test"
  export cluster_test.*

  val independentLoadProgress = world.extensions.consensus.get("a").committedPosition > 1 and
    not(M::hasCommitPath(world.extensions.consensus.get("_meta"), world.system, world.extensions.network.metaLinks))
  val localApplyLag = world.system.values().exists(n => n.volatile.appliedMetaIndex > 0 and
    n.volatile.appliedMetaIndex < world.extensions.consensus.get("_meta").committedPosition)
  val retainedRestart = world.system.values().exists(n => n.volatile.incarnation > 1)

  run membershipFixtureTest = and {
    Set("n1", "n2", "n3").subseteq(FOUNDERS.keys()),
    LOAD_IDS.contains("a"), MAX_INCARNATION >= 2, MAX_POSITION >= 3,
  }

  run metaFailureDoesNotStopLoadTest = init
    .then(setLink(MetaChannel, "n1", "n2", false))
    .then(setLink(MetaChannel, "n1", "n3", false))
    .then(setLink(MetaChannel, "n2", "n3", false))
    .then(commitProgress("a"))
    .expect(independentLoadProgress)
    .expect(safety)
    .then(commitProgress("_meta").fail())

  run loadFailureDoesNotStopMetaTest = init
    .then(setLink(LoadChannel, "n1", "n2", false))
    .then(setLink(LoadChannel, "n1", "n3", false))
    .then(setLink(LoadChannel, "n2", "n3", false))
    .then(commitProgress("_meta"))
    .expect(world.extensions.consensus.get("_meta").committedPosition == 2)
    .expect(world.extensions.consensus.get("a").committedPosition == 1)
    .expect(safety)
    .then(commitProgress("a").fail())

  run commitBeforeLocalApplyTest = init
    .then(commitProgress("_meta"))
    .expect(world.system.get("n2").volatile.appliedMetaIndex == 1)
    .expect(world.system.get("n2").durable.metaIndex == 1)
    .then(applyMeta("n2"))
    .expect(world.system.get("n2").volatile.appliedMetaIndex == 2)
    .expect(world.system.get("n2").durable.metaIndex == 2)
    .expect(world.system.get("n1").volatile.appliedMetaIndex == 1)
    .expect(safety)

  run majorityLossDespiteHealthyCacheTest = init
    .then(crash("n2"))
    .then(crash("n3"))
    .expect(world.system.get("n1").volatile.observations.get("_meta").health == Healthy)
    .expect(not(M::hasCommitPath(world.extensions.consensus.get("a"), world.system,
      world.extensions.network.loadLinks)))
    .expect(safety)
    .then(commitProgress("_meta").fail())

  run loadMajorityLossTest = init
    .then(crash("n2")).then(crash("n3"))
    .then(commitProgress("a").fail())

  run staleLeaderObservationTest = init
    .then(setLink(MetaChannel, "n1", "n2", false))
    .then(setLink(MetaChannel, "n1", "n3", false))
    .then(elect("_meta", I::replica(FOUNDERS, "_meta", "n2")))
    .then(commitProgress("_meta"))
    .expect(world.system.get("n3").volatile.observations.get("_meta").observedLeader ==
      Some(I::replica(FOUNDERS, "_meta", "n1")))
    .expect(world.extensions.consensus.get("_meta").leaderAuthority ==
      Some(I::replica(FOUNDERS, "_meta", "n2")))
    .expect(safety)

  run retainedRestartCatchUpTest = init
    .then(crash("n2"))
    .then(commitProgress("_meta"))
    .then(restart("n2"))
    .expect(world.system.get("n2").volatile.phase == Starting)
    .expect(world.system.get("n2").volatile.incarnation == 2)
    .expect(world.system.get("n2").durable.pin == Some({ cluster: "c1", node: "n2", member: 2 }))
    .expect(world.system.get("n2").volatile.appliedMetaIndex == 1)
    .then(catchUp("_meta", I::replica(FOUNDERS, "_meta", "n2")))
    .then(applyMeta("n2"))
    .expect(world.system.get("n2").volatile.appliedMetaIndex == 2)
    .expect(not(M::liveIdentity(world.system, I::replica(FOUNDERS, "a", "n2"))))
    .expect(safety)

  run oldIncarnationMessageTest = init
    .then(invoke("n2", ObserveLocal))
    .then(crash("n2"))
    .then(restart("n2"))
    .then({ nondet msg = world.extensions.network.pending.oneOf()
      deliver(msg) })
    .then(handle("n2"))
    .expect(world.system.get("n2").volatile.pendingCalls.keys().size() == 0)
    .expect(world.messages.get("n2").size() == 0)
    .expect(safety)

  run duplicateDeliveryTest = init
    .then(invoke("n2", ObserveLocal))
    .then({ nondet msg = world.extensions.network.pending.oneOf()
      duplicate(msg) })
    .expect(world.extensions.network.pending.size() == 2)
    .then({ nondet msg = world.extensions.network.pending.oneOf()
      deliver(msg) })
    .then(handle("n2"))
    .then({ nondet msg = world.extensions.network.pending.oneOf()
      deliver(msg) })
    .then(handle("n2"))
    .expect(world.system.get("n2").volatile.pendingCalls.keys().size() == 1)
    .expect(world.system.get("n2").volatile.pendingCalls.values().forall(c => c.result == Some(Observed(Ready))))
    .expect(safety)

  run droppedRequestIsNotReplyTest = init
    .then(invoke("n2", ObserveLocal))
    .then({ nondet msg = world.extensions.network.pending.oneOf()
      drop(msg) })
    .expect(world.system.get("n2").volatile.pendingCalls.values().forall(c => c.result == None))
    .then(handle("n2").fail())

  // Pure adversarial input: changing a native voter into a learner removes its vote.
  run learnerDoesNotVoteTest = {
    val w = I::worldFor(NODE_POOL, FOUNDERS, LOAD_IDS, INITIAL_LEADER)
    val c = w.extensions.consensus.get("_meta")
    val leader = I::replica(FOUNDERS, "_meta", "n1")
    val learner = I::replica(FOUNDERS, "_meta", "n2")
    val changed = { ...c, voters: c.voters.exclude(Set(learner)), learners: Set(learner) }
    val links = Set(("n1", "n2"), ("n2", "n1"))
    all {
      M::certificate(changed, w.system, links, leader) == Set(leader),
      not(M::hasCommitPath(changed, w.system, links)),
    }
  }
  action membershipStageProbe = world' = world
  run membershipSharesWorldTest = clusterStageInit.then(clusterStageProbe).then(membershipStageProbe)
    .expect(world.system.keys() == NODE_POOL)
}
