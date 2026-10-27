module planning_test {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import topology_fixtures as F from "../fixtures/topology"
  import initial as I from "../../harness/initial"
  import placement as P from "../../topology/placement"
  import topology as T from "../../topology/topology"
  import planning_fixtures.* from "../fixtures/planning"
  run labelsHaveNoNativeEffectsTest = {
    val desired = { ...base, version: 2, groups: base.groups.setBy("a", g => { ...g, labels: Map("desc" -> "changed") }) }
    match T::makePlan(input, desired) {
      | Planned(plan) => and {
          plan.blocks.keys() == Set("a"), plan.blocks.get("a").length() == 0,
          plan.cleanup.get("a").length() == 0, plan.sources.get("a") == 1,
          T::makePlan(input, desired) == Planned(plan),
        }
      | _ => false
    }
  }

  run deletionNeedsNoNativeQuorumPlanTest = {
    val desired = T::mutateDocument(F::input, F::request(DeleteGroup("a")))
    val noObservation = { ...input, observations: input.observations.mapRemove("a") }
    match T::makePlan(noObservation, desired) {
      | Planned(plan) => and {
          plan.blocks.get("a").length() == 0,
          plan.cleanup.get("a").select(s => s.kind == StopReplica).length() == 3,
          plan.cleanup.get("a").select(s => s.kind == DeleteGroupState).length() == 1,
          P::toSet(plan.cleanup.get("a")).forall(s => not(s.resources.contains(GroupResource("a")))),
        }
      | _ => false
    }
  }

  run leaderRemovalTransfersFirstTest = {
    val desired = { ...base, version: 2, groups: base.groups.setBy("a", g =>
      { ...g, replicas: g.replicas.filter(r => r.key.node != "n1") }) }
    match T::makePlan(input, desired) {
      | Planned(plan) => {
          val steps = plan.blocks.get("a")
          and { steps.head().kind == TransferLeadership,
            seqFor(steps, TransferLeadership) < seqFor(steps, RemoveMember),
            steps.nth(1).expectedVoters.size() == 3, steps.nth(1).resultVoters.size() == 2 }
        }
      | _ => false
    }
  }

  run twoToOneIsNotAPlanTest = {
    val desired = { ...base, version: 2, groups: base.groups.setBy("a", g =>
      { ...g, replicas: g.replicas.filter(r => r.key.node == "n1") }) }
    match T::makePlan(input, desired) { | PlanRejected(errors) => errors.contains(UnsafeMembership) | _ => false }
  }

  run sameHostStopsBeforePreparingSuccessorTest = {
    val desired = { ...base, version: 2, groups: base.groups.setBy("a", g => { ...g, nextReplica: 5,
      replicas: g.replicas.map(r => if (r.key.node == "n3") { ...r, key: { ...r.key, replica: 4 } } else r) }) }
    match T::makePlan(input, desired) {
      | Planned(plan) => {
          val steps = plan.blocks.get("a")
          val retirement = plan.cleanup.get("a").head()
          and {
            seqFor(steps, RemoveMember) < seqFor(steps, StopReplica),
            seqFor(steps, StopReplica) < seqFor(steps, PrepareReplica),
            seqFor(steps, AddLearner) < seqFor(steps, WaitCatchUp),
            seqFor(steps, WaitCatchUp) < seqFor(steps, PromoteLearner),
            retirement.kind == RetireReplica,
            plan.dependencies.contains((retirement.seq, seqFor(steps, StopReplica))),
          }
        }
      | _ => false
    }
  }

  run capReplacementWaitsAgainAfterRemovalTest = {
    val joined = T::mutateDocument(F::input, F::request(AddNode({ name: "n4", member: 0,
      nodeType: "worker", state: Active, labels: Map() })))
    val desired = { ...joined, groups: joined.groups.setBy("a", g => { ...g, nextReplica: 5,
      replicas: g.replicas.map(r => if (r.key.node == "n3") { ...r,
        key: { ...r.key, node: "n4", member: 4, replica: 4 } } else r) }) }
    match T::makePlan(input, desired) {
      | Planned(plan) => {
          val steps = plan.blocks.get("a")
          val waits = steps.select(s => s.kind == WaitCatchUp)
          and { waits.length() == 2,
            waits.head().seq < seqFor(steps, RemoveMember),
            waits.nth(1).seq > seqFor(steps, RemoveMember),
            waits.nth(1).seq < seqFor(steps, PromoteLearner) }
        }
      | _ => false
    }
  }

  run missingEstablishedObservationIsNotRefoundingTest = {
    val desired = { ...base, version: 2, groups: base.groups.setBy("a", g => { ...g,
      replicas: g.replicas.map(r => if (r.key.node == "n3") { ...r, role: Learner } else r) }) }
    match T::makePlan({ ...input, observations: input.observations.mapRemove("a") }, desired) {
      | PlanRejected(errors) => errors.contains(Unavailable)
      | _ => false
    }
  }

  run stoppedOwnerDoesNotNeedAnotherStopTest = {
    val desired = T::mutateDocument(F::input, F::request(DeleteGroup("a")))
    val stopped = I::replica(Map("n3" -> 3), "a", "n3")
    match T::makePlan({ ...input, retainedStates: input.retainedStates.put(stopped, RetainedStopped) }, desired) {
      | Planned(plan) => plan.cleanup.get("a").select(s => s.kind == StopReplica).length() == 2
      | _ => false
    }
  }

  run freshGroupHasFoundingAuthorityTest = {
    val spec: GroupSpec = { id: "c", storage: StorageDefault, labels: Map(), placement: None,
      hosting: None, nodes: None, voterConfig: None }
    val desired = T::mutateDocument(F::input, F::request(CreateGroup(spec)))
    match T::makePlan(input, desired) {
      | Planned(plan) => {
          val founding = plan.blocks.get("c").head()
          and { founding.kind == CreateFoundingGroup, founding.target == GroupTarget("c"),
            founding.targets.size() == 3, founding.resources.size() == 4,
            founding.resources.contains(GroupResource("c")), plan.sources.get("c") == 0 }
        }
      | _ => false
    }
  }

  run foreignNativeIdentityRefusesPlanTest = {
    val alien = I::replica(Map("n1" -> 9), "a", "n1")
    val bad = { ...input, observations: input.observations.setBy("a", o =>
      { ...o, native: { ...o.native, voters: o.native.voters.union(Set(alien)) } }) }
    val desired = { ...base, version: 2, groups: base.groups.setBy("a", g => { ...g,
      replicas: g.replicas.map(r => if (r.key.node == "n3") { ...r, role: Learner } else r) }) }
    T::makePlan(bad, desired) == PlanRejected(Set(Unauthorized))
  }
}
