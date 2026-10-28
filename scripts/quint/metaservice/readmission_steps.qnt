module metaservice_readmission_steps {
  import types.* from "../types"
  import world_types.* from "../runtime/world"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import frames as F from "../runtime/frames"
  import message_effects.* from "../runtime/messages"
  import readmission_base as B from "../metaengine/readmission_base"
  import admission as A from "../metaengine/admission"
  import grants as G from "../metaengine/grants"
  import membership as M from "../consensus/membership"
  import metaservice_admission_steps as Join from "./admission_steps"

  // Explicit Start with Join may use the retained control-only store after a
  // durable self-removal. Sending Join never authorizes the old engine to run.
  pure def controlJoinEnabled(w: World, node: NodeName, seed: NodeName, limit: int): bool =
    w.system.get(node).volatile.phase == Fenced and w.system.get(node).durable.pin != None and
      node != seed and w.extensions.nextDelivery <= limit

  pure def healthEnabled(w: World, request: RequestKey, node: NodeName): bool = {
    val caller = w.system.get(request.caller.node)
    caller.volatile.pendingCalls.keys().contains(request) and M::running(caller) and
      caller.volatile.incarnation == request.caller.incarnation and {
        val call = caller.volatile.pendingCalls.get(request)
        call.phase == PreparingCall and match call.request {
          | Mutate(m) => match m.edit { | ReadmitNode(r) => r.name == node | _ => false } | _ => false }
      }
  }

  pure def healthNext(w: World, request: RequestKey, node: NodeName): World = {
    val available = M::running(w.system.get(node)) and
      M::communicates(w.extensions.network.metaLinks, request.caller.node, node)
    F::withProcess(w, request.caller.node, p => { ...p, pendingCalls: p.pendingCalls.setBy(request, c => { ...c,
      health: c.health.put(node, if (available) Healthy else ConfirmedDead) }) })
  }

  pure def command(w: World, msg: Envelope): Option[ControlCommand] = match msg.payload {
    | JoinRequest(j) => {
        val local = w.extensions.metaLog.get(w.system.get(msg.destination).volatile.appliedMetaIndex)
        if (not(local.admissions.keys().contains(j.owner.node))) None else {
          val a = local.admissions.get(j.owner.node)
          if (j.owner != a.owner or j.member <= 0 or j.member >= a.replica.member) None else
            Some(BeginReadmission({ admission: a, predecessor: j.member }))
        }
      }
    | _ => None
  }

  pure def offer(w: World, msg: Envelope): Option[ReadmissionOffer] = match msg.payload {
    | JoinRequest(j) => {
        val host = w.system.get(msg.destination)
        val local = w.extensions.metaLog.get(host.volatile.appliedMetaIndex)
        if (not(local.admissions.keys().contains(j.owner.node))) None else {
          val a = local.admissions.get(j.owner.node)
          val key = { node: j.owner.node, predecessor: j.member, successor: a.replica.member }
          val grants = B::batch(local, key)
          if (j.owner != a.owner or not(A::current(local, a)) or not(grants.exists(g => g.key.group == "_meta")) or
              not(grants.forall(g => G::currentForIssuance(local, g)))) None else Some({
            admission: a, prior: key, grants: grants, proofIndex: host.volatile.appliedMetaIndex,
            sender: { node: msg.destination, incarnation: host.volatile.incarnation },
            senderMember: host.durable.pin.unwrap().member, delivery: w.extensions.nextDelivery })
        }
      }
    | _ => None
  }

  pure def replyEnabled(w: World, msg: Envelope, limit: int): bool = Join::joinApplied(w, msg) and
    offer(w, msg) != None and w.extensions.nextDelivery <= limit

  pure def replyNext(w: World, msg: Envelope): World = {
    val response = offer(w, msg).unwrap()
    val base = F::withProcess(w, msg.destination, p => { ...p, authorityReads: p.authorityReads.mapRemove(msg.delivery) })
    apply_custom_effect(apply_custom_effect(base, Consume(msg)), Queue({ delivery: response.delivery,
      source: response.sender, destination: response.admission.owner.node, channel: MetaChannel,
      payload: ReadmissionAccepted(response) }))
  }
}
