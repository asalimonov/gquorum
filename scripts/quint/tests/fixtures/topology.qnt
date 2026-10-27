module topology_fixtures {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import placement_fixtures as F from "./placement"
  import initial as I from "../../harness/initial"
  import placement as P from "../../topology/placement"
  import topology as T from "../../topology/topology"
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
}
