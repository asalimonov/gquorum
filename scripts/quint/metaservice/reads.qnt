module reads {
  import types.* from "../types"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import membership as M from "../consensus/membership"

  // A successful abstract ReadIndex round establishes a lower bound, not a
  // frozen snapshot or execution grant. The caller must still apply that index.
  // TODO(not planned): Raft message/term algorithms and read batching/timeouts.
  pure def canConfirm(env: Environment, nodes: NodeStates, msg: Envelope): bool =
    if (not(env.consensus.keys().contains("_meta"))) false else {
      val c = env.consensus.get("_meta")
      and {
        match msg.payload { | ReadProbe(ticket) => ticket.owner == msg.source | _ => false },
        match c.leaderAuthority { | Some(r) => r.node == msg.destination | None => false },
        M::hasCommitPath(c, nodes, env.network.metaLinks),
      }
    }

  pure def canFinish(owner: LocalNode, key: RequestKey): bool =
    if (not(owner.volatile.pendingCalls.keys().contains(key))) false else {
      val call = owner.volatile.pendingCalls.get(key)
      and {
        key.caller == { node: owner.process_id, incarnation: owner.volatile.incarnation },
        M::running(owner), call.request == ConfirmedRead, call.phase == AwaitingReadApply,
        not(call.waiterCancelled),
        match call.readIndex { | Some(index) => owner.volatile.appliedMetaIndex >= index | None => false },
      }
    }

  pure def finish(owner: LocalNode, key: RequestKey): LocalNode = {
    val call = owner.volatile.pendingCalls.get(key)
    val index = match call.readIndex { | Some(value) => value | None => 0 }
    { ...owner, volatile: { ...owner.volatile, pendingCalls: owner.volatile.pendingCalls.put(key,
        { ...call, phase: Replied, result: Some(ConfirmedView({ index: index,
          applied: owner.volatile.appliedMetaIndex, view: owner.volatile.localView })) }) } }
  }
}
