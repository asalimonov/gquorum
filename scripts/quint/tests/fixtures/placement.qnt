module placement_fixtures {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import placement as P from "../../topology/placement"
  import initial as I from "../../harness/initial"
  pure val rules: Rules = { maxNodes: 5, maxReplicas: 5, concurrentGroups: 4, concurrentCleanup: 2,
    nodeOrder: List("n1", "n2", "n3", "n4", "n5"), groupOrder: List("_meta", "a", "b", "c") }
  pure val base = I::declarations(Map("n1" -> 1, "n2" -> 2, "n3" -> 3), Set("a", "b"))
  pure val config: VoterConfig = {
    target: 3, domain: None, maxSurge: 1, failureWindow: 2, standbyWindow: 2,
    failureLag: 2, standbyLag: 1, standbyApplyBacklog: 1, replaceNoProgress: true,
    noProgressWindow: 2, maxObservationGap: 2, switchCooldown: 2, demotionCooldown: 2,
    allowNewFailureDuringCooldown: true,
  }
}
