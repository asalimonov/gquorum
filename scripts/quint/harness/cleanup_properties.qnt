module cleanup_properties {
  import types.* from "../types"
  import world_types.* from "../runtime/world"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import cleanup as Cleanup from "../metaengine/cleanup"
  import readmission_base as Readmit from "../metaengine/readmission_base"
  import managed_collection_steps as C from "../managed/collection_steps"

  pure def safety(w: World): bool = and {
    w.extensions.metadata.readmissionOrigins.keys().forall(key => {
      val origin = w.extensions.metadata.readmissionOrigins.get(key)
      key.predecessor > 0 and key.successor > key.predecessor and
        w.extensions.metadata.operations.keys().contains(origin) and
        w.extensions.metadata.history.keys().contains(origin.version) and
        w.extensions.metadata.history.get(origin.version).nodes.get(key.node).member == key.successor and
        Readmit::batch(w.extensions.metadata, key).exists(g => g.key.group == "_meta")
    }),
    w.extensions.metadata.grants.values().filter(g => g.purpose == Readmission).forall(g => match g.admission {
      | Some(key) => w.extensions.metadata.readmissionOrigins.keys().contains(key)
      | None => false
    }),
    w.extensions.metadata.cleanup.values().forall(debt => match debt.retired {
      | Some(receipt) => match receipt.admission {
          | Some(key) => Readmit::consumed(w.extensions.metadata, key)
          | None => true }
      | None => true
    }),
    w.extensions.metadata.cleanup.values().forall(debt => match debt.collected {
      | None => true
      | Some(receipt) => {
          val host = w.system.get(debt.identity.node)
          receipt.identity == debt.identity and Cleanup::retirementProof(w.extensions.metadata, receipt) and
            host.durable.retiredStores.keys().contains(debt.identity) and
            host.durable.retiredStores.get(debt.identity).token == receipt.token and
            not(host.durable.retiredStores.get(debt.identity).dataPresent)
        }
    }),
    w.system.values().forall(host => and {
      w.extensions.metadata.readmissionOrigins.keys().filter(key => key.node == host.process_id and
        (match host.durable.pin { | Some(pin) => pin.member == key.successor | None => false })).forall(key =>
          Readmit::batch(w.extensions.metadata, key).forall(g => g.targets.forall(target =>
            host.durable.receipts.exists(r => r.grant == g.key and r.target == Some(target)))) and
          host.durable.readmissionBackup.exists(r => r.node == key.node and r.member == key.predecessor and r.group == "_meta") and
          (host.volatile.phase != Ready or Readmit::consumed(w.extensions.metadata, key))),
      host.durable.collectionMarkers.keys().forall(key => {
        val row = host.durable.collectionMarkers.get(key)
        C::rowValid(host, key) and Set(Prepared, CompletionDurable).contains(row.phase) and
          (row.phase != CompletionDurable or not(host.durable.retiredStores.get(key).dataPresent))
      }),
      host.durable.retiredStores.keys().forall(key => {
        val store = host.durable.retiredStores.get(key)
        store.token > 0 and not(host.durable.replicaEvidence.keys().contains(key)) and
          (store.markerPresent or (not(store.dataPresent) and w.extensions.metadata.cleanup.keys().contains(key) and
            w.extensions.metadata.cleanup.get(key).collected != None))
      }),
      host.volatile.collectionAcks.forall(receipt => w.extensions.metadata.cleanup.keys().contains(receipt.identity) and
        w.extensions.metadata.cleanup.get(receipt.identity).collected == Some(receipt)),
    }),
  }
}
