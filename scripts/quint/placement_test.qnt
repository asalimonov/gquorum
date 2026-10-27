module placement_test {
  import types.* from "./types"
  import basicSpells.* from "./vendor/spells/basicSpells"
  import placement as P from "./placement"
  import initial as I from "./initial"

  pure val rules: Rules = { maxNodes: 5, maxReplicas: 5, concurrentGroups: 4, concurrentCleanup: 2,
    nodeOrder: List("n1", "n2", "n3", "n4", "n5"), groupOrder: List("_meta", "a", "b", "c") }
  pure val base = I::declarations(Map("n1" -> 1, "n2" -> 2, "n3" -> 3), Set("a", "b"))
  pure val config: VoterConfig = {
    target: 3, domain: None, maxSurge: 1, failureWindow: 2, standbyWindow: 2,
    failureLag: 2, standbyLag: 1, standbyApplyBacklog: 1, replaceNoProgress: true,
    noProgressWindow: 2, maxObservationGap: 2, switchCooldown: 2, demotionCooldown: 2,
    allowNewFailureDuringCooldown: true,
  }

  run currentDeclarationValidTest = P::validateDocument(base, rules) == Set()

  run reservedRetainsOnlyExactReplicaTest = {
    val doc = { ...base, nodes: base.nodes.setBy("n1", n => { ...n, labels: Map("gquorum.io/reserved" -> "true") }) }
    val group = doc.groups.get("a")
    val replaced = { ...group, replicas: group.replicas.map(r =>
      if (r.key.node == "n1") { ...r, key: { ...r.key, replica: 4 } } else r), nextReplica: 5 }
    all {
      P::canRetain(base, doc, group, doc.nodes.get("n1")),
      not(P::canAdd(doc, group, doc.nodes.get("n1"))),
      not(P::canRetain(base, doc, replaced, doc.nodes.get("n1"))),
    }
  }

  run learnerOnlyStillHostsMetaTest = {
    val doc = { ...base, nodes: base.nodes.setBy("n1", n => { ...n, labels: Map("gquorum.io/learner-only" -> "true") }) }
    all {
      P::canAdd(doc, doc.groups.get("_meta"), doc.nodes.get("n1")),
      not(P::canVote(base, doc, doc.groups.get("_meta"), doc.nodes.get("n1"))),
      not(P::canVote(base, doc, doc.groups.get("a"), doc.nodes.get("n1"))),
    }
  }

  run missingSelectorIsNotEmptyLabelTest = {
    val rule = { ...base.placement, selector: Map("zone" -> "") }
    all {
      not(P::matchesSelector(rule, base.nodes.get("n1"))),
      P::matchesSelector(rule, { ...base.nodes.get("n1"), labels: Map("zone" -> "") }),
    }
  }

  run duplicateHostingCannotDisappearTest = {
    val bad = { ...base.groups.get("a"), voterConfig: Some(config),
      hosting: Some({ mode: ExplicitHosts, selectedNodes: List("n1", "n1") }) }
    P::validateDocument({ ...base, groups: base.groups.put("a", bad) }, rules).contains(Invalid)
  }

  run configuredHostingRequiredTest = {
    val bad = { ...base.groups.get("a"), voterConfig: Some(config), hosting: None }
    all {
      P::validateDocument(base, rules) == Set(),
      P::validateDocument({ ...base, groups: base.groups.put("a", bad) }, rules).contains(Invalid),
    }
  }

  run storageClassImmutableTest = {
    val changed = { ...base, version: 2, groups: base.groups.setBy("a", g => { ...g, storage: StorageMemory }) }
    P::validateTransition(base, changed, Set(), rules).contains(Invalid)
  }

  run retiredGroupCannotReturnTest = P::validateTransition(base, base, Set("a"), rules).contains(Invalid)

  run strictDomainsRequireFullTargetTest = {
    val doc = { ...base, nodes: base.nodes.transformValues(n => { ...n, labels: Map("zone" -> "same") }) }
    val group = { ...doc.groups.get("a"), voterConfig: Some({ ...config, domain: Some("zone") }),
      hosting: Some({ mode: AllEligible, selectedNodes: List() }) }
    val selected = P::selectSeats(base, doc, group, rules)
    all { P::voters(selected).size() == 1, P::selectionErrors(doc, selected).contains(Invalid) }
  }

  run configuredShortfallWithoutDomainsAllowedTest = {
    val doc = { ...base, nodes: base.nodes.setBy("n3", n => { ...n, labels: Map("gquorum.io/learner-only" -> "true") }) }
    val group = { ...doc.groups.get("a"), voterConfig: Some(config),
      hosting: Some({ mode: AllEligible, selectedNodes: List() }) }
    val selected = P::selectSeats(base, doc, group, rules)
    all { P::voters(selected).size() == 2, P::selectionErrors(doc, selected) == Set() }
  }

  run voterConfigurationValidationTest = all {
    P::validVoterConfig(config, 1, 3),
    not(P::validVoterConfig({ ...config, maxSurge: 2 }, 1, 3)),
    not(P::validVoterConfig({ ...config, failureLag: 1 }, 1, 3)),
    not(P::validVoterConfig({ ...config, noProgressWindow: 0 }, 1, 3)),
    not(P::validVoterConfig({ ...config, target: 4 }, 1, 3)),
  }
}
