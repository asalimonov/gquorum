module cluster_test {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import cluster.* from "../../cluster"
  export cluster.*
  import properties.* from "../../harness/properties"

  val safety = initialScope(world) and callsWellFormed(world) and metadataWellFormed(world) and
    (if (FOUNDERS.keys().size() == 0) foundationHasNoEffects(world) else consensusWellFormed(world))
  val deliveredRequest = world.system.keys().exists(n =>
    world.system.get(n).volatile.pendingCalls.values().exists(c => c.phase == Replied and not(c.waiterCancelled)))
  pure val scopeOptions: MutationOptions = {
    expectedVersion: 1, audit: { actor: "operator", comment: "scope", forced: None },
    preview: false, approve: false,
  }
  run stageScopeEnforcedAtInvocationTest = {
    nondet node = NODE_POOL.oneOf()
    init.then({
      val doc = world.system.get(node).volatile.localView.latest
      invoke(node, Mutate({ edit: StageDocument({ ...doc,
        placement: { ...doc.placement, selector: Map("az" -> "x") } }), options: scopeOptions })).fail()
    })
  }
  run admissionScopeEnforcedAtInvocationTest = {
    nondet node = NODE_POOL.oneOf()
    init.then(invoke(node, Mutate({ edit: AddNode({ name: "n4", member: 0, state: Active,
      nodeType: "worker", labels: Map("az" -> "x") }), options: scopeOptions })).fail())
  }
  run creationScopeEnforcedAtInvocationTest = {
    nondet node = NODE_POOL.oneOf()
    init.then(invoke(node, Mutate({ edit: CreateGroup({ id: "c", labels: Map("az" -> "x"),
      storage: StorageDefault, placement: None, nodes: None, hosting: None, voterConfig: None }),
      options: scopeOptions })).fail())
  }
  run assumptionsTest = and {
    NODE_POOL.size() > 0, MAX_REQUESTS >= 1, MAX_POSITION >= 1,
    MAX_INCARNATION >= 1, MAX_DELIVERIES >= MAX_REQUESTS,
    FOUNDERS.keys().subseteq(NODE_POOL),
    FOUNDERS.values().size() == FOUNDERS.keys().size(),
    FOUNDERS.values().forall(id => id > 0),
    FOUNDERS.keys().size() == 0 or FOUNDERS.keys().contains(INITIAL_LEADER),
    not(LOAD_IDS.contains("_meta")),
  }
  run deliveredObservationTest = {
    nondet node = NODE_POOL.oneOf()
    init.then(invoke(node, ObserveLocal))
      .expect(world.messages.get(node).size() == 0)
      .then({ nondet msg = world.extensions.network.pending.oneOf()
        deliver(msg) })
      .then(handle(node))
      .expect(world.system.get(node).volatile.pendingCalls.values().forall(c =>
        c.phase == Replied and c.result == Some(Observed(if (FOUNDERS.keys().contains(node)) Ready else New))))
      .expect(safety)
  }

  run invalidRequestRefusedTest = {
    nondet node = NODE_POOL.oneOf()
    init.then(invoke(node, InvalidRequest))
      .expect(world.system.get(node).volatile.pendingCalls.values().forall(c => c.result == None))
      .then({ nondet msg = world.extensions.network.pending.oneOf()
        deliver(msg) })
      .expect(world.system.get(node).volatile.pendingCalls.values().forall(c => c.result == None))
      .then(handle(node))
      .expect(world.system.get(node).volatile.pendingCalls.values().forall(c =>
        c.result == Some(Refused(Invalid))))
      .expect(safety)
  }

  run requestBoundTest = {
    nondet node = NODE_POOL.oneOf()
    init.then(MAX_REQUESTS.reps(_ => invoke(node, ObserveLocal)))
      .expect(world.system.get(node).volatile.pendingCalls.keys().size() == MAX_REQUESTS)
      .then(invoke(node, ObserveLocal).fail())
  }
  action clusterStageInit = init
  action clusterStageProbe = world' = world
  run clusterSharesWorldTest = clusterStageInit.then(clusterStageInit).then(clusterStageProbe)
    .expect(world.system.keys() == NODE_POOL)
}
