module managed_removal_steps {
  import types.* from "../types"
  import world_types.* from "../runtime/world"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import frames as F from "../runtime/frames"

  // The actual native removal must reach this exact retained member's store.
  // Approved Target, rather than Latest or a cached health result, supplies intent.
  pure def applyRemovalEnabled(w: World, node: NodeName, receipt: Receipt): bool = {
    val host = w.system.get(node)
    if (not(w.extensions.consensus.get("_meta").authorityReceipts.contains(receipt)) or
        not(w.extensions.metaLog.keys().contains(receipt.durablePosition))) false else match receipt.target {
      | None => false
      | Some(key) => {
          val image = w.extensions.metaLog.get(receipt.durablePosition)
          val absent = if (not(image.target.nodes.keys().contains(node))) true else
            image.target.nodes.get(node).state == Excluded or image.target.nodes.get(node).member != key.member
          key.group == "_meta" and key.node == node and host.durable.metaSeen and
            host.durable.metaRemoved == None and host.volatile.metaRunning and
            host.durable.pin == Some({ cluster: key.cluster, node: node, member: key.member }) and absent and
            (match receipt.effect { | Some(e) => e.kind == RemoveMember | None => false }) and
            (match host.durable.replicaEvidence.getOrElse(key, MissingStore) {
              | ValidStore(position) => position.commit >= receipt.durablePosition | _ => false })
        }
    }
  }

  pure def applyRemovalNext(w: World, node: NodeName, receipt: Receipt): World = {
    val image = w.extensions.metaLog.get(receipt.durablePosition)
    val view = { latest: image.latest, target: image.target }
    F::withNode(w, node, host => { ...host,
      durable: { ...host.durable, metaRemoved: receipt.target, metaImage: Some(view), metaIndex: receipt.durablePosition },
      volatile: { ...host.volatile, metaRunning: false, phase: Fenced, ownLease: Revoked, renewal: None,
        appliedMetaIndex: receipt.durablePosition, appliedMetaVersion: image.latest.version, localView: view } })
  }

  pure def removalSafety(w: World): bool = w.system.values().forall(host => match host.durable.metaRemoved {
    | None => true
    | Some(key) => host.durable.metaSeen and not(host.volatile.metaRunning) and
        host.durable.pin == Some({ cluster: key.cluster, node: key.node, member: key.member }) and
        Set(Fenced, Stopping, Stopped).contains(host.volatile.phase)
  })
}
