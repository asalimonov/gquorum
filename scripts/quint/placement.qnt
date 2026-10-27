module placement {
  import types.* from "./types"
  import basicSpells.* from "./vendor/spells/basicSpells"

  pure def toSet(xs: List[a]): Set[a] = xs.foldl(Set(), (s, x) => s.union(Set(x)))

  pure def voters(group: GroupDecl): Set[ReplicaKey] =
    group.replicas.filter(r => r.role == Voter).map(r => r.key)

  pure def effective(doc: Document, group: GroupDecl): PlacementRule = match group.placement {
    | Some(rule) => rule
    | None => doc.placement
  }

  pure def reserved(node: NodeDecl): bool = node.labels.getOrElse("gquorum.io/reserved", "") == "true"
  pure def learnerOnly(node: NodeDecl): bool = node.labels.getOrElse("gquorum.io/learner-only", "") == "true"
  pure def allows(rule: PlacementRule, node: NodeDecl): bool =
    rule.allowedTypes.size() == 0 or rule.allowedTypes.contains(node.nodeType)

  pure def matchesSelector(rule: PlacementRule, node: NodeDecl): bool =
    rule.selector.keys().forall(k =>
      if (node.labels.keys().contains(k)) node.labels.get(k) == rule.selector.get(k) else false)

  pure def inHosting(group: GroupDecl, node: NodeName): bool = match group.hosting {
    | Some(h) => h.mode == AllEligible or h.selectedNodes.toSet().contains(node)
    | None => true
  }

  pure def hostEligible(doc: Document, group: GroupDecl, node: NodeDecl): bool =
    if (group.id == "_meta") node.state != Excluded else and {
      node.state == Active, allows(effective(doc, group), node),
      matchesSelector(effective(doc, group), node), inHosting(group, node.name),
    }

  pure def exactApproved(approved: Document, group: GroupDecl, node: NodeName): bool =
    if (not(approved.groups.keys().contains(group.id))) false else
      group.replicas.exists(r => r.key.node == node and
        approved.groups.get(group.id).replicas.exists(old => old.key == r.key))

  pure def canRetain(approved: Document, doc: Document, group: GroupDecl, node: NodeDecl): bool =
    hostEligible(doc, group, node) and
      (group.id == "_meta" or not(reserved(node)) or exactApproved(approved, group, node.name))

  pure def canAdd(doc: Document, group: GroupDecl, node: NodeDecl): bool =
    hostEligible(doc, group, node) and (group.id == "_meta" or not(reserved(node)))

  pure def domain(group: GroupDecl, node: NodeDecl): Option[str] = match group.voterConfig {
    | Some(c) => match c.domain {
      | Some(label) => Some(node.labels.getOrElse(label, ""))
      | None => None
    }
    | None => None
  }

  pure def canVote(approved: Document, doc: Document, group: GroupDecl, node: NodeDecl): bool = and {
    canRetain(approved, doc, group, node) or canAdd(doc, group, node),
    not(learnerOnly(node)),
    not(group.replicas.exists(r => r.key.node == node.name and r.pinnedLearner)),
    domain(group, node) != Some(""),
  }

  pure def eligibleHosts(doc: Document, group: GroupDecl): Set[NodeName] =
    doc.nodes.keys().filter(n => canAdd(doc, group, doc.nodes.get(n)))

  pure def validRule(rule: PlacementRule, rules: Rules): bool = and {
    rule.minVoters > 0, rule.maxVoters >= rule.minVoters, rule.maxVoters <= rules.maxReplicas,
    rule.allowedTypes.forall(t => t != ""),
    rule.selector.keys().forall(k => k != "" and k != "gquorum.io/reserved" and k != "gquorum.io/learner-only"),
  }

  pure def validVoterConfig(config: VoterConfig, minimum: int, maximum: int): bool = and {
    config.target >= minimum, config.target <= maximum,
    config.maxSurge >= 0, config.maxSurge <= 1,
    config.failureWindow > 0, config.standbyWindow > 0, config.maxObservationGap > 0,
    config.noProgressWindow >= 0, config.replaceNoProgress implies config.noProgressWindow > 0,
    config.switchCooldown >= 0, config.demotionCooldown >= 0,
    config.standbyLag >= 0, config.failureLag > config.standbyLag, config.standbyApplyBacklog >= 0,
    config.standbyLag <= 1000,
    config.domain != Some(""),
  }

  pure def validHosting(doc: Document, group: GroupDecl, rules: Rules): bool = match group.hosting {
    | None => group.id == "_meta" or group.voterConfig == None
    | Some(h) => and {
      group.id != "_meta", group.voterConfig != None,
      h.selectedNodes.length() == h.selectedNodes.toSet().size(),
      h.selectedNodes.toSet().subseteq(doc.nodes.keys()),
      if (h.mode == AllEligible) h.selectedNodes.length() == 0
      else h.selectedNodes.length() > 0 and
        h.selectedNodes == rules.nodeOrder.select(n => h.selectedNodes.toSet().contains(n)),
    }
  }

  pure def validateDocument(doc: Document, rules: Rules): Set[Refusal] = {
    val admitted = doc.nodes.keys().filter(n => doc.nodes.get(n).state != Excluded)
    val rulesValid = rules.maxNodes > 0 and rules.maxReplicas > 0 and
      rules.concurrentGroups >= 0 and rules.concurrentCleanup >= 0
    val shape = and {
      doc.cluster != "", admitted.size() > 0, admitted.size() <= rules.maxNodes,
      validRule(doc.placement, rules), doc.groups.keys().contains("_meta"),
      doc.nodes.keys().forall(n => {
        val node = doc.nodes.get(n)
        and { n != "", node.name == n, node.nodeType != "", node.member > 0,
          Set("gquorum.io/reserved", "gquorum.io/learner-only").forall(k =>
            if (node.labels.keys().contains(k)) node.labels.get(k) == "true" else true) }
      }),
      doc.groups.keys().forall(g => {
        val group = doc.groups.get(g)
        val rule = effective(doc, group)
        val minimum = if (g == "_meta") 1 else rule.minVoters
        val maximum = if (g == "_meta") rules.maxNodes else rule.maxVoters
        and {
          g != "", group.id == g, group.nextReplica > 0,
          not(group.labels.keys().contains("gquorum.io/reserved")),
          not(group.labels.keys().contains("gquorum.io/learner-only")),
          validHosting(doc, group, rules),
          match group.voterConfig { | Some(c) => validVoterConfig(c, minimum, maximum) | None => true },
          if (g == "_meta") and {
            group.storage == StorageDefault, group.placement == None, voters(group).size() > 0,
            admitted.subseteq(group.replicas.map(r => r.key.node)),
          } else and {
            validRule(rule, rules), group.replicas.size() <= rules.maxReplicas,
            voters(group).size() <= rule.maxVoters,
          },
          group.replicas.map(r => r.key.node).size() == group.replicas.size(),
          group.replicas.map(r => r.key.replica).size() == group.replicas.size(),
          group.replicas.forall(r => if (not(doc.nodes.keys().contains(r.key.node))) false else and {
            r.key.cluster == doc.cluster, r.key.group == g,
            r.key.replica > 0, r.key.replica < group.nextReplica,
            r.key.member == doc.nodes.get(r.key.node).member,
            r.pinnedLearner implies r.role == Learner,
            if (g == "_meta") r.key.replica == r.key.member
            else doc.nodes.get(r.key.node).state != Excluded and allows(rule, doc.nodes.get(r.key.node)),
          }),
        }
      }),
    }
    if (not(rulesValid)) Set(Invalid)
    else if (doc.version == 0 and doc.nodes.keys().size() == 0 and doc.groups.keys().size() == 0) Set()
    else if (shape) Set() else Set(Invalid)
  }

  pure def validateTransition(before: Document, after: Document, retired: Set[GroupId], rules: Rules): Set[Refusal] = {
    val valid = and {
      before.cluster == after.cluster,
      retired.intersect(after.groups.keys()).size() == 0,
      after.nodes.keys().intersect(before.nodes.keys()).forall(n => {
        val old = before.nodes.get(n)
        val updated = after.nodes.get(n)
        (old.state == Excluded or old.nodeType == updated.nodeType) and
          ((old.state == Excluded and updated.state != Excluded) implies old.member != updated.member)
      }),
      after.groups.keys().intersect(before.groups.keys()).forall(g => {
        val old = before.groups.get(g)
        val updated = after.groups.get(g)
        and {
          old.storage == updated.storage, updated.nextReplica >= old.nextReplica,
          updated.replicas.forall(r => r.key.replica >= old.nextReplica or
            old.replicas.exists(p => p.key.replica == r.key.replica and p.key.node == r.key.node)),
          match updated.voterConfig {
            | Some(c) => if (c.target == 1 and voters(old).size() > 1)
                match old.voterConfig { | Some(prior) => prior.target == 1 | None => false }
              else true
            | None => true
          },
        }
      }),
    }
    validateDocument(after, rules).union(if (valid) Set() else Set(Invalid))
  }

  pure def wantedHosts(approved: Document, doc: Document, group: GroupDecl, rules: Rules): List[NodeName] = {
    val names = rules.nodeOrder.select(n => doc.nodes.keys().contains(n))
    val legacyTarget = if (group.placement != None) effective(doc, group).maxVoters
      else group.replicas.size() + max(0, effective(doc, group).maxVoters - voters(group).size())
    val retained = names.select(n => group.replicas.exists(r => r.key.node == n) and
      canRetain(approved, doc, group, doc.nodes.get(n)))
    val newHosts = names.select(n => not(group.replicas.exists(r => r.key.node == n)) and
      canAdd(doc, group, doc.nodes.get(n)))
    if (group.voterConfig != None) names.select(n => retained.toSet().contains(n) or newHosts.toSet().contains(n))
    else retained.concat(newHosts).foldl(List(), (chosen, n) =>
      if (chosen.length() < legacyTarget) chosen.append(n) else chosen)
  }

  pure def materialize(approved: Document, doc: Document, group: GroupDecl, rules: Rules): GroupDecl = {
    val hosts = wantedHosts(approved, doc, group, rules)
    val retained = { ...group, replicas: group.replicas.filter(r => hosts.toSet().contains(r.key.node)) }
    hosts.foldl(retained, (g, n) => if (g.replicas.exists(r => r.key.node == n)) g else {
      ...g, nextReplica: g.nextReplica + 1,
      replicas: g.replicas.union(Set({
        key: { cluster: doc.cluster, group: g.id, node: n, member: doc.nodes.get(n).member, replica: g.nextReplica },
        role: Learner, pinnedLearner: false,
      })),
    })
  }

  pure def selectSeats(before: Document, doc: Document, group: GroupDecl, rules: Rules): GroupDecl = {
    val need = match group.voterConfig { | Some(c) => c.target | None => effective(doc, group).maxVoters }
    val previous = if (before.groups.keys().contains(group.id)) before.groups.get(group.id).replicas else Set()
    val eligible = rules.nodeOrder.select(n => doc.nodes.keys().contains(n) and
      group.replicas.exists(r => r.key.node == n) and canVote(before, doc, group, doc.nodes.get(n)))
    val retainedVoters = eligible.select(n => group.replicas.exists(r => r.key.node == n and
      previous.exists(old => old.key == r.key and old.role == Voter)))
    val retainedLearners = eligible.select(n => group.replicas.exists(r => r.key.node == n and
      previous.exists(old => old.key == r.key and old.role == Learner)))
    val fresh = eligible.select(n => not(retainedVoters.toSet().contains(n)) and not(retainedLearners.toSet().contains(n)))
    val candidates = retainedVoters.concat(if (group.voterConfig == None) List() else retainedLearners).concat(fresh)
    val chosen = candidates.foldl(Set(), (selected, n) => {
      val d = domain(group, doc.nodes.get(n))
      if (selected.size() >= need or (d != None and selected.exists(old => domain(group, doc.nodes.get(old)) == d))) selected
      else selected.union(Set(n))
    })
    { ...group, replicas: group.replicas.map(r => { ...r, role: if (chosen.contains(r.key.node)) Voter else Learner }) }
  }

  pure def selectionErrors(doc: Document, group: GroupDecl): Set[Refusal] = {
    val count = voters(group).size()
    val valid = count >= effective(doc, group).minVoters and match group.voterConfig {
      | Some(c) => c.domain == None or (count == c.target and
        voters(group).map(r => domain(group, doc.nodes.get(r.node))).size() == count)
      | None => true
    }
    if (valid) Set() else Set(Invalid)
  }

  pure def needsSeats(before: Document, doc: Document, group: GroupDecl): bool =
    if (not(before.groups.keys().contains(group.id))) true else {
      val old = before.groups.get(group.id)
      val oldTarget = match old.voterConfig { | Some(c) => c.target | None => effective(before, old).maxVoters }
      val target = match group.voterConfig { | Some(c) => c.target | None => effective(doc, group).maxVoters }
      val oldDomain = match old.voterConfig { | Some(c) => c.domain | None => None }
      val newDomain = match group.voterConfig { | Some(c) => c.domain | None => None }
      or {
        oldTarget != target, oldDomain != newDomain,
        voters(old).exists(r => not(group.replicas.exists(p => p.key == r)) or
          not(canVote(before, doc, group, doc.nodes.get(r.node)))),
        group.voterConfig == None and voters(old).size() < target and
          group.replicas.exists(r => not(old.replicas.exists(p => p.key == r.key))),
      }
    }
}
