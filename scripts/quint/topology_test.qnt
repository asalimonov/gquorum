module topology_test {
  import types.* from "./types"
  import basicSpells.* from "./vendor/spells/basicSpells"
  import placement_test as F from "./placement_test"
  import initial as I from "./initial"
  import placement as P from "./placement"
  import topology as T from "./topology"

  pure val base = F::base
  pure val meta = I::metadata(base)
  pure val input: MutationInput = {
    view: { latest: base, target: base }, retired: Set(), memberFloor: meta.memberAllocationFloor,
    replicaFloors: meta.replicaAllocationFloors, history: Map(1 -> base),
    groups: meta.groups, nodes: meta.nodes, health: base.nodes.keys().mapBy(_ => Healthy), rules: F::rules,
  }
  pure val options: MutationOptions = {
    expectedVersion: 1, audit: { actor: "operator", comment: "test", forced: None }, preview: false, approve: false,
  }
  pure def request(edit: DocumentEdit): MutationRequest = { edit: edit, options: options }

  run previewIsPureTest = {
    val req = { edit: DeleteGroup("a"), options: { ...options, preview: true } }
    val result = T::preview(input, req)
    all {
      result.errors == Set(), result.plan == None,
      match result.document { | Some(d) => not(d.groups.keys().contains("a")) and d.version == 2 | None => false },
      input.view.latest == base, input.view.target == base,
    }
  }

  run previewAndApproveRefusedTest = T::mutationErrors(input,
    { edit: DeleteGroup("a"), options: { ...options, preview: true, approve: true } }) == Set(Invalid)

  run originalCasIsRequiredTest = T::mutationErrors(input,
    { edit: DeleteGroup("a"), options: { ...options, expectedVersion: 0 } }) == Set(Conflict)

  run addNodeAllocatesFreshMetaTest = {
    val req = request(AddNode({ name: "n4", member: 99, state: Excluded, nodeType: "worker", labels: Map() }))
    val changed = T::mutateDocument(input, req)
    all {
      T::mutationGuard(input, req), changed.nodes.get("n4").member == 4,
      changed.nodes.get("n4").state == Active,
      changed.groups.get("_meta").replicas.exists(r => r.key.node == "n4" and r.key.replica == 4),
      changed.groups.get("a").replicas == base.groups.get("a").replicas,
    }
  }

  run drainPreservesMetaTest = {
    val req = request(DrainNode("n3"))
    val changed = T::mutateDocument(input, req)
    all {
      T::mutationGuard(input, req), changed.nodes.get("n3").state == Draining,
      changed.groups.get("_meta").replicas == base.groups.get("_meta").replicas,
      not(T::hasLoad(changed, "n3")),
    }
  }

  run excludeNeedsProvenDrainTest = T::mutationErrors(input, request(ExcludeNode("n3"))) == Set(Invalid)

  run forceRetirementKeepsAuditTest = {
    val req = request(ForceRetireNode({ name: "n3", reason: "permanent loss" }))
    val changed = T::mutateDocument(input, req)
    all {
      T::mutationGuard(input, req), changed.nodes.get("n3").state == Excluded,
      changed.audit.forced == Some({ node: "n3", reason: "permanent loss" }),
      changed.groups.values().forall(g => not(g.replicas.exists(r => r.key.node == "n3"))),
      not(T::mutationGuard(input, { ...req, options: { ...options, audit: { ...options.audit, actor: "" } } })),
    }
  }

  run readmissionNeedsExactDeadPredecessorTest = {
    val req = request(ReadmitNode({ name: "n3", expectedMember: 3 }))
    val dead = { ...input, health: input.health.put("n3", ConfirmedDead) }
    val changed = T::mutateDocument(dead, req)
    all {
      T::mutationErrors(input, req) == Set(UnsafeMembership),
      T::mutationGuard(dead, req), changed.nodes.get("n3").member == 4,
      changed.groups.values().forall(g => g.replicas.exists(r =>
        r.key.node == "n3" and r.key.member == 4 and r.key.replica == 4)),
      T::mutationErrors(dead, request(ReadmitNode({ name: "n3", expectedMember: 1 }))) == Set(Conflict),
    }
  }

  run adoptionPinsLegacyLearnersTest = {
    val legacy = { ...base, groups: base.groups.setBy("a", g => { ...g,
      replicas: g.replicas.map(r => if (r.key.node == "n3") { ...r, role: Learner } else r) }) }
    val context = { ...input, view: { latest: legacy, target: legacy } }
    val req = request(SetGroupVoterConfig({ group: "a", config: F::config }))
    val changed = T::mutateDocument(context, req)
    all {
      T::mutationGuard(context, req),
      changed.groups.get("a").replicas.exists(r => r.key.node == "n3" and r.pinnedLearner and r.role == Learner),
      changed.groups.get("a").hosting == Some({ mode: ExplicitHosts, selectedNodes: List("n1", "n2", "n3") }),
    }
  }

  run typedHostingSortsButRejectsDuplicatesTest = {
    // A fixed initial preset; this core hosting check does not activate live policy tuning.
    val configured = { ...base, groups: base.groups.setBy("a", g => { ...g,
      voterConfig: Some(T::defaultVoterConfig), hosting: Some({ mode: AllEligible, selectedNodes: List() }) }) }
    val context = { ...input, view: { latest: configured, target: configured }, groups: I::metadata(configured).groups }
    val req = request(SetGroupHosting({ group: "a", hosting: { mode: ExplicitHosts, selectedNodes: List("n2", "n1") } }))
    val changed = T::mutateDocument(context, req)
    all {
      T::mutationGuard(context, req),
      changed.groups.get("a").hosting == Some({ mode: ExplicitHosts, selectedNodes: List("n1", "n2") }),
      T::mutationErrors(context, request(SetGroupHosting({ group: "a",
        hosting: { mode: ExplicitHosts, selectedNodes: List("n1", "n1") } }))) == Set(Invalid),
    }
  }

  run descriptiveLabelDoesNotChangeNativeIntentTest = {
    val changed = { ...base, groups: base.groups.setBy("a", g => { ...g, labels: Map("name" -> "orders") }) }
    all {
      T::affectedScopes(base, changed) == Set("a"),
      T::placementScopes(base, changed) == Set(),
      changed.groups.get("a").replicas == base.groups.get("a").replicas,
    }
  }

  run freshGroupUsesConfiguredHostingTest = {
    val spec: GroupSpec = { id: "c", storage: StorageDefault, labels: Map(), placement: None,
      hosting: None, nodes: None, voterConfig: None }
    val req = request(CreateGroup(spec))
    val changed = T::mutateDocument(input, req)
    all {
      T::mutationGuard(input, req), changed.groups.get("c").replicas.size() == 3,
      changed.groups.get("c").hosting == Some({ mode: AllEligible, selectedNodes: List() }),
      changed.groups.get("c").voterConfig == Some(T::defaultVoterConfig),
      T::mutationErrors(input, request(CreateGroup({ ...spec, nodes: Some(List()),
        hosting: Some({ mode: AllEligible, selectedNodes: List() }) }))) == Set(Invalid),
    }
  }

  run reseatUsesApprovedIntentAndBothFloorsTest = {
    val staged = { ...base, version: 2, groups: base.groups.setBy("b", g => { ...g,
      replicas: g.replicas.map(r => if (r.key.node == "n3") { ...r, role: Learner } else r) }) }
    val context = { ...input, view: { latest: staged, target: base }, replicaFloors: input.replicaFloors.put("a", 7) }
    val key = I::replica(Map("n1" -> 1), "a", "n1")
    val req = { edit: ReseatReplica(key), options: { ...options, expectedVersion: 2 } }
    val changed = T::mutateDocument(context, req)
    all {
      T::mutationGuard(context, req), changed.parent == 1, changed.version == 3,
      changed.groups.get("b") == base.groups.get("b"),
      changed.groups.get("a").replicas.exists(r => r.key.node == "n1" and r.key.replica == 7),
      changed.groups.get("a").nextReplica == 8,
    }
  }

  run stagedRemovalCanRestoreApprovedIdentityTest = {
    val staged = { ...base, version: 2, groups: base.groups.setBy("a", g =>
      { ...g, replicas: g.replicas.filter(r => r.key.node != "n3") }) }
    val context = { ...input, view: { latest: staged, target: base } }
    val req = { edit: StageDocument({ ...base, parent: 2 }), options: { ...options, expectedVersion: 2 } }
    T::mutationGuard(context, req)
  }

  run unpinDoesNotPromoteTest = {
    val configured = T::mutateDocument(input, request(SetGroupVoterConfig({ group: "a", config: F::config })))
    val context = { ...input, view: { latest: { ...configured, version: 1 }, target: configured } }
    val key = I::replica(Map("n3" -> 3), "a", "n3")
    val pinned = T::mutateDocument(context, request(SetReplicaPinnedLearner({ key: key, pinned: true })))
    val unpinContext = { ...context, view: { latest: { ...pinned, version: 1 }, target: pinned } }
    val unpinned = T::mutateDocument(unpinContext, request(SetReplicaPinnedLearner({ key: key, pinned: false })))
    all {
      T::mutationGuard(unpinContext, request(SetReplicaPinnedLearner({ key: key, pinned: false }))),
      unpinned.groups.get("a").replicas.exists(r => r.key == key and not(r.pinnedLearner) and r.role == Learner),
    }
  }

  run metaCannotBeDeletedOrMadeMemoryTest = all {
    T::mutationErrors(input, request(DeleteGroup("_meta"))) == Set(Invalid),
    not(T::mutationGuard(input, request(StageDocument({ ...base, groups: base.groups.setBy("_meta", g =>
      { ...g, storage: StorageMemory }) })))),
  }

  run stageCannotReuseUnapprovedAllocationTest = {
    val context = { ...input, replicaFloors: input.replicaFloors.put("c", 8) }
    val candidate = { ...base, groups: base.groups.put("c", { ...base.groups.get("a"), id: "c", nextReplica: 9,
      replicas: base.groups.get("a").replicas.map(r => { ...r, key: { ...r.key, group: "c" } }) }) }
    not(T::mutationGuard(context, request(StageDocument(candidate))))
  }

  run invalidMutationTargetsAreTotalTest = {
    val edits = Set(DrainNode("missing"), ActivateNode("missing"), ExcludeNode("missing"),
      DeleteGroup("missing"), SetGroupVoterConfig({ group: "missing", config: F::config }),
      SetGroupPlacement({ group: "missing", rule: base.placement }),
      ChangeReplica({ group: "missing", node: "n1", role: Learner }),
      SetReplicaPinnedLearner({ key: I::replica(Map("n1" -> 1), "missing", "n1"), pinned: true }))
    edits.forall(edit => T::mutationErrors(input, request(edit)).contains(NotFound))
  }

  run rollbackKeepsCasAndHistoricalParentSeparateTest = {
    val staged = { ...base, version: 3, groups: base.groups.setBy("a", g => { ...g, labels: Map("x" -> "y") }) }
    val context = { ...input, view: { latest: staged, target: base } }
    val req = { edit: RollbackTo(1), options: { ...options, expectedVersion: 3 } }
    val changed = T::mutateDocument(context, req)
    all { T::mutationGuard(context, req), changed.version == 4, changed.parent == 1,
      changed.groups.get("a").labels == Map(), changed.groups.get("a").nextReplica >= base.groups.get("a").nextReplica }
  }
}
