module frames_test {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import frames as F from "../../runtime/frames"
  import initial as I from "../../harness/initial"

  pure val seed = I::worldFor(Set("n1", "n2", "n3"), Map("n1" -> 1, "n2" -> 2, "n3" -> 3), Set("a", "b"), "n1")

  run ownerUpdatePreservesOtherFramesTest = {
    val changed = F::withOwner(seed, "n1", "a", owner => { ...owner, held: true })
    and {
      changed.system.get("n1").volatile.owners.get("a").held,
      changed.system.get("n1").volatile.owners.get("b") == seed.system.get("n1").volatile.owners.get("b"),
      changed.system.get("n1").durable == seed.system.get("n1").durable,
      changed.system.get("n2") == seed.system.get("n2"), changed.system.get("n3") == seed.system.get("n3"),
      changed.extensions == seed.extensions, changed.messages == seed.messages, changed.events == seed.events,
      { ...changed.system.get("n1").volatile, owners: seed.system.get("n1").volatile.owners } == seed.system.get("n1").volatile,
    }
  }
  run processUpdatePreservesDurableEvidenceTest = {
    val changed = F::withProcess(seed, "n2", process => { ...process, schedulerEnabled: false })
    and { not(changed.system.get("n2").volatile.schedulerEnabled),
      changed.system.get("n2").durable == seed.system.get("n2").durable,
      changed.system.get("n1") == seed.system.get("n1"), changed.system.get("n3") == seed.system.get("n3"),
      changed.extensions == seed.extensions, changed.messages == seed.messages, changed.events == seed.events }
  }
  run nodeUpdatePreservesIdentityAndPeersTest = {
    val changed = F::withNode(seed, "n3", node => { ...node,
      durable: { ...node.durable, journalPosition: node.durable.journalPosition + 1 } })
    and { changed.system.get("n3").durable.journalPosition == seed.system.get("n3").durable.journalPosition + 1,
      changed.system.get("n3").process_id == "n3", changed.system.get("n3").volatile == seed.system.get("n3").volatile,
      changed.system.get("n1") == seed.system.get("n1"), changed.system.get("n2") == seed.system.get("n2"),
      changed.extensions == seed.extensions, changed.messages == seed.messages, changed.events == seed.events }
  }
}
