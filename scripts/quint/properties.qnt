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
            | Observed(phase) => call.request == ObserveLocal and phase == New
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
}
