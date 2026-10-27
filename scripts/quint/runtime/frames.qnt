module frames {
  import types.* from "../types"
  import world_types.* from "./world"

  // Callers establish identity, enabledness and key existence. These helpers
  // change only the named frame; they never create a missing owner or grant.
  pure def withNode(w: World, node: NodeName, update: (LocalNode) => LocalNode): World =
    { ...w, system: w.system.setBy(node, update) }

  pure def withProcess(w: World, node: NodeName, update: (ProcessState) => ProcessState): World =
    withNode(w, node, n => { ...n, volatile: update(n.volatile) })

  pure def withOwner(w: World, node: NodeName, group: GroupId, update: (OwnerState) => OwnerState): World =
    withProcess(w, node, process => { ...process, owners: process.owners.setBy(group, update) })
}
