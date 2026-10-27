module planning_fixtures {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import topology_fixtures as F from "./topology"
  import initial as I from "../../harness/initial"
  import placement as P from "../../topology/placement"
  import topology as T from "../../topology/topology"
  pure val base = F::base
  pure val native = I::consensus(base, "n1")
  pure val retained = base.groups.values().map(g => g.replicas.map(r => r.key)).flatten()
  pure val input: PlanningInput = {
    approved: base, groups: F::meta.groups, retained: retained,
    retainedStates: retained.mapBy(_ => RetainedHosted), rules: F::input.rules,
    observations: native.keys().mapBy(g => { native: native.get(g), healthKnown: true,
      live: native.get(g).voters, ready: native.get(g).voters }),
  }
  pure def seqFor(steps: List[PlannedStep], kind: ActionKind): int =
    steps.select(s => s.kind == kind).head().seq
}
