module properties {
  import types.* from "./types"
  import handlers.* from "./handlers"
  import basicSpells.* from "./vendor/spells/basicSpells"

  pure def callsWellFormed(w: World): bool = w.system.keys().forall(n => {
    val owner = w.system.get(n)
    owner.volatile.pendingCalls.keys().forall(key => {
      val call = owner.volatile.pendingCalls.get(key)
      and {
        key.caller.node == n,
        key.sequence > 0,
        (call.phase == Replied) iff (call.result != None),
        match call.result {
          | Some(result) => match result {
            | Refused(reason) => reason == Invalid and call.request == InvalidRequest
            | Observed(_) => call.request == ObserveLocal
            | Indeterminate => false
          }
          | _ => true
        },
      }
    })
  })

  pure def foundationHasNoEffects(w: World): bool = and {
    w.extensions.metadata.latest.version == 0,
    w.extensions.metadata.target.version == 0,
    w.extensions.metadata.grants.keys().size() == 0,
    w.extensions.metadata.cleanup.keys().size() == 0,
    w.system.keys().forall(n => and {
      w.system.get(n).durable.pin == None,
      w.system.get(n).durable.receipts.size() == 0,
      w.system.get(n).volatile.owners.keys().size() == 0,
    }),
  }

  pure def consensusWellFormed(w: World): bool = and {
    w.extensions.consensus.keys().contains("_meta"),
    w.extensions.consensus.keys().forall(g => {
      val c = w.extensions.consensus.get(g)
      and {
        c.voters.size() > 0,
        c.voters.intersect(c.learners).size() == 0,
        c.voters.union(c.learners).forall(r => r.group == g and r.cluster == c.lineage),
        c.appliedPositions.values().forall(index => index <= c.committedPosition),
        match c.leaderAuthority {
          | Some(leader) => c.voters.contains(leader)
          | None => true
        },
      }
    }),
    w.system.keys().forall(n => {
      val node = w.system.get(n)
      and {
        node.volatile.appliedMetaIndex <= w.extensions.consensus.get("_meta").committedPosition,
        node.durable.metaIndex <= w.extensions.consensus.get("_meta").committedPosition,
        node.volatile.incarnation >= 1,
        node.volatile.owners.values().forall(owner => owner.key.node == n),
      }
    }),
  }
}
