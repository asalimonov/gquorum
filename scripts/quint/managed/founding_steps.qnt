module managed_founding_steps {
  import types.* from "../types"
  import world_types.* from "../runtime/world"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import founding as F from "../consensus/founding"
  import membership as M from "../consensus/membership"
  import execution as E from "./execution"
  import grants as G from "../metaengine/grants"

  pure def confirmFoundingEnabled(w: World, d: EffectDelivery, limit: int): bool = {
    val group = d.grant.key.group
    if (not(F::isFounding(d.grant)) or d.resource != GroupResource(group) or
        not(w.extensions.consensus.keys().contains(group))) false else {
      val c = w.extensions.consensus.get(group)
      val local = w.extensions.metaLog.get(w.system.get(d.receiver.node).volatile.appliedMetaIndex)
      w.system.get(d.receiver.node).volatile.waitingEffects.contains(d) and E::mayConsume(w, d) and
        (E::consumed(w, d) or (c.committedPosition < limit and c.pendingChange == None and
          c.leaderAuthority == Some(d.replica) and c.voters == d.grant.targets and c.learners.size() == 0 and
          c.authorityFloor == d.grant.key.revision and
          M::hasCommitPath(c, w.system, M::linksFor(w.extensions.network, group)) and
          d.grant.resources.filter(r => r != GroupResource(group)).forall(r => G::resourceConsumed(local, d.grant, r))))
    }
  }

  pure def confirmFoundingNext(w: World, d: EffectDelivery, deliveries: int): World = {
    val group = d.grant.key.group
    val c = w.extensions.consensus.get(group)
    if (E::consumed(w, d)) E::forgetWait(w, d) else {
      val position = c.committedPosition + 1
      val voters = M::certificate(c, w.system, M::linksFor(w.extensions.network, group), d.replica)
      val receipt = { ...E::receipt(d, position), outcome: EffectApplied }
      val evidence: ConsumptionEvidence = { delivery: d, pin: w.system.get(d.receiver.node).durable.pin,
        incarnation: d.receiver.incarnation, owner: Some(d.replica) }
      val base = E::forgetWait(w, d)
      val applied = { ...base, system: M::persistCertificate(base.system, voters, position),
        extensions: { ...base.extensions, consensus: base.extensions.consensus.put(group,
          { ...M::committed(c, voters, position), authorityReceipts: c.authorityReceipts.union(Set(receipt)) }),
          consumptions: base.extensions.consumptions.union(Set(evidence)) } }
      E::returnReceipt(applied, d, receipt, deliveries)
    }
  }
}
