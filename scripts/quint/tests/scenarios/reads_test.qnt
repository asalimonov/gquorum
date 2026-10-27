module reads_test {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import reads as R from "../../metaservice/reads"
  import membership_test.* from "./membership_test"
  export membership_test.*

  pure val reader: RequestKey = { caller: { node: "n2", incarnation: 1 }, sequence: 1 }
  val confirmedRead = world.system.values().exists(n =>
    n.volatile.pendingCalls.values().exists(c => match c.result {
      | Some(result) => match result { | ConfirmedView(_) => true | _ => false }
      | None => false
    }))
  action deliverReadCall: bool = {
    nondet msg = world.extensions.network.pending.filter(m => match m.payload {
      | ApiCall(c) => c.key == reader | _ => false
    }).oneOf()
    deliver(msg)
  }
  action deliverProbe: bool = {
    nondet msg = world.extensions.network.pending.filter(m => match m.payload {
      | ReadProbe(ticket) => ticket.request == reader | _ => false
    }).oneOf()
    deliver(msg)
  }
  action confirmProbe: bool = {
    nondet msg = world.messages.get("n1").filter(m => match m.payload {
      | ReadProbe(ticket) => ticket.request == reader | _ => false
    }).oneOf()
    confirmRead(msg)
  }
  action deliverAck: bool = {
    nondet msg = world.extensions.network.pending.filter(m => match m.payload {
      | ReadAck(ack) => ack.ticket.request == reader | _ => false
    }).oneOf()
    deliver(msg)
  }
  action beginRead: bool = invoke("n2", ConfirmedRead).then(deliverReadCall)
    .then(handle("n2")).then(deliverProbe)
  action receiveAck: bool = deliverAck.then(handle("n2"))

  run confirmedReadWaitsForApplyTest = init.then(commitProgress("_meta"))
    .then(beginRead).then(confirmProbe).then(receiveAck)
    .expect(world.system.get("n2").volatile.pendingCalls.get(reader).readIndex == Some(2))
    .expect(not(R::canFinish(world.system.get("n2"), reader)))
    .then(applyMeta("n2")).then(finishRead(reader)).expect(confirmedRead).expect(safety)

  run confirmedReadNeedsQuorumTest = init.then(beginRead)
    .then(setLink(MetaChannel, "n1", "n2", false))
    .then(setLink(MetaChannel, "n1", "n3", false))
    .expect(world.system.get("n2").volatile.pendingCalls.get(reader).result == None)
    .then(confirmProbe.fail())

  run confirmedReadSurvivesLostReplyTest = init.then(beginRead).then(confirmProbe)
    .then({ nondet msg = world.extensions.network.pending.filter(m => match m.payload {
        | ReadAck(ack) => ack.ticket.request == reader | _ => false
      }).oneOf()
      drop(msg) })
    .expect(world.system.get("n2").volatile.pendingCalls.get(reader).result == None)
    .then(retryRead(reader)).then(deliverProbe).then(confirmProbe).then(receiveAck)
    .then(finishRead(reader)).expect(confirmedRead).expect(safety)

  run cancelledReadDoesNotUndoConfirmationTest = init.then(beginRead).then(confirmProbe)
    .then(cancelCall(reader)).then(receiveAck)
    .expect(world.system.get("n2").volatile.pendingCalls.get(reader).waiterCancelled)
    .expect(world.extensions.metadata.grants.keys().size() == 0)
    .expect(world.system.get("n2").volatile.pendingCalls.get(reader).result == Some(Refused(WaitCancelled)))
    .expect(safety).then(finishRead(reader).fail())

  run oldReadReplyCannotCompleteNewIncarnationTest = init.then(beginRead).then(confirmProbe)
    .then(crash("n2")).then(restart("n2")).then(deliverAck).then(handle("n2"))
    .expect(world.system.get("n2").volatile.pendingCalls.keys().size() == 0)
    .expect(not(confirmedRead)).expect(safety)

  run confirmedReadMayObserveNewerApplyTest = init.then(beginRead).then(confirmProbe)
    .then(commitProgress("_meta")).then(applyMeta("n2"))
    .then(receiveAck).then(finishRead(reader))
    .expect(world.system.get("n2").volatile.pendingCalls.get(reader).result ==
      Some(ConfirmedView({ index: 1, applied: 2, view: world.system.get("n2").volatile.localView })))
    .expect(safety)
  action readsStageProbe = world' = world
  run readsSharesWorldTest = clusterStageInit.then(membershipStageProbe).then(readsStageProbe)
    .expect(world.system.keys() == NODE_POOL)
}
