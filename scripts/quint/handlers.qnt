module handlers {
  import types.* from "./types"
  import basicSpells.* from "./vendor/spells/basicSpells"
  import choreo as C from "./vendor/choreo"
  import operations as O from "./operations"

  type HostEffect = Consume(Envelope) | Queue(Envelope)
  type Context = C::LocalContext[NodeName, NodeState, Envelope, int, Environment]
  type Transition = C::Transition[NodeName, NodeState, Envelope, int, HostEffect]
  type World = C::GlobalContext[NodeName, NodeState, Envelope, int, Environment]
  pure def isReseat(request: ApiRequest): bool = match request {
    | Mutate(m) => match m.edit { | ReseatReplica(_) => true | _ => false }
    | _ => false
  }

  pure def listen_local(ctx: Context): Set[Envelope] =
    ctx.messages.filter(msg => match msg.payload {
      | ApiCall(call) => and {
        msg.destination == ctx.state.process_id,
        call.key.caller.node == ctx.state.process_id,
        call.key.caller.incarnation == ctx.state.volatile.incarnation,
        ctx.state.volatile.pendingCalls.keys().contains(call.key),
        call.request == ObserveLocal or call.request == InvalidRequest,
      }
      | _ => false
    })

  pure def exactCall(ctx: Context, key: RequestKey): bool = and {
    key.caller.node == ctx.state.process_id,
    key.caller.incarnation == ctx.state.volatile.incarnation,
    ctx.state.volatile.pendingCalls.keys().contains(key),
  }

  pure def listen_read(ctx: Context): Set[Envelope] = ctx.messages.filter(msg => match msg.payload {
    | ApiCall(call) => if (not(exactCall(ctx, call.key))) false else {
        val pending = ctx.state.volatile.pendingCalls.get(call.key)
        call.request == ConfirmedRead and pending.request == ConfirmedRead and
          pending.phase == Captured and not(pending.waiterCancelled)
      }
    | ReadAck(ack) => if (not(exactCall(ctx, ack.ticket.request)) or ack.ticket.proposal != None or ack.ticket.round != 0) false else {
        val pending = ctx.state.volatile.pendingCalls.get(ack.ticket.request)
        (pending.request == ConfirmedRead or isReseat(pending.request)) and pending.phase == Submitted and not(pending.waiterCancelled)
      }
    | _ => false
  })

  pure def listen_stale(ctx: Context): Set[Envelope] =
    ctx.messages.exclude(listen_local(ctx)).exclude(listen_read(ctx)).exclude(listen_transaction(ctx))
      .exclude(listen_work(ctx)).filter(msg => match msg.payload {
      // The consensus primitive consumes probes separately from host handling.
      | ReadProbe(_) => false
      | MetaProposal(_) => false
      | _ => true
    })

  pure def act_stale(ctx: Context, msg: Envelope): Transition = {
    post_state: ctx.state, effects: Set(C::CustomEffect(Consume(msg))),
  }

  pure def listen_transaction(ctx: Context): Set[Envelope] = ctx.messages.filter(msg => match msg.payload {
    | ApiCall(call) => if (not(exactCall(ctx, call.key))) false else {
        val pending = ctx.state.volatile.pendingCalls.get(call.key)
        pending.phase == Captured and not(pending.waiterCancelled) and pending.request == call.request and
          match call.request { | Mutate(_) => true | Operate(_) => true | _ => false }
      }
    | MetaAck(ack) => if (not(exactCall(ctx, ack.key.request)) or ack.forWork) false else {
        val pending = ctx.state.volatile.pendingCalls.get(ack.key.request)
        pending.phase == Submitted and pending.proposalSequence == ack.key.sequence and
          not(pending.waiterCancelled)
      }
    | OperationAck(ack) => if (not(exactCall(ctx, ack.key.request))) false else {
        val pending = ctx.state.volatile.pendingCalls.get(ack.key.request)
        pending.phase == WaitingOperation and pending.proposalSequence == ack.key.sequence and not(pending.waiterCancelled)
      }
    | _ => false
  })

  pure def act_transaction(ctx: Context, msg: Envelope): Transition = {
    val pending = ctx.state.volatile.pendingCalls
    val updated = match msg.payload {
      | ApiCall(call) => pending.setBy(call.key, p => { ...p, phase: PreparingCall })
      | MetaAck(ack) => pending.setBy(ack.key.request, p => { ...p, phase: Committed,
          commitIndex: Some(ack.index), commitOutcome: Some(ack.outcome) })
      | OperationAck(ack) => pending.setBy(ack.key.request, p => {
          val outcome = { ...ack.outcome, staged: if (p.stagedVersion != None) p.stagedVersion else ack.outcome.staged }
          { ...p, phase: Replied, stagedVersion: outcome.staged, result: Some(MutationResult(outcome)) }
        })
      | _ => pending
    }
    { post_state: { ...ctx.state, volatile: { ...ctx.state.volatile, pendingCalls: updated } },
      effects: Set(C::CustomEffect(Consume(msg))) }
  }

  pure def listen_work(ctx: Context): Set[Envelope] = ctx.messages.filter(msg => match msg.payload {
    | OperationCall(call) => call.key.request.caller == msg.source
    | ReadAck(ack) => match ack.ticket.proposal {
        | Some(sequence) => {
          val key = { request: ack.ticket.request, sequence: sequence }
          if (ack.ticket.owner != { node: ctx.state.process_id, incarnation: ctx.state.volatile.incarnation } or
              not(ctx.state.volatile.operationWork.keys().contains(key))) false
          else ctx.state.volatile.operationWork.get(key).phase == WorkReadQueued and
            ctx.state.volatile.operationWork.get(key).readRound == ack.ticket.round
        }
        | None => false
      }
    | MetaAck(ack) => if (not(ack.forWork) or ack.coordinator != {
          node: ctx.state.process_id, incarnation: ctx.state.volatile.incarnation } or
          not(ctx.state.volatile.operationWork.keys().contains(ack.key))) false
        else ctx.state.volatile.operationWork.get(ack.key).phase == WorkSubmitted
    | _ => false
  })

  pure def act_work(ctx: Context, msg: Envelope): Transition = {
    val prior = ctx.state.volatile.operationWork
    val updated = match msg.payload {
      | OperationCall(call) => if (prior.keys().contains(call.key)) {
          val old = prior.get(call.key)
          if (old.request == call.request) prior.put(call.key, { ...old, replySent: false }) else prior
        } else prior.put(call.key, O::newWork(call.request))
      | ReadAck(ack) => match ack.ticket.proposal {
          | Some(sequence) => prior.setBy({ request: ack.ticket.request, sequence: sequence }, work =>
              { ...work, phase: WorkApplying, readIndex: Some(ack.index) })
          | None => prior
        }
      | MetaAck(ack) => prior.setBy(ack.key, work => { ...work, phase: WorkCommitted,
          commitIndex: Some(ack.index), result: Some(ack.outcome) })
      | _ => prior
    }
    { post_state: { ...ctx.state, volatile: { ...ctx.state.volatile, operationWork: updated } },
      effects: Set(C::CustomEffect(Consume(msg))) }
  }

  pure def act_local(ctx: Context, msg: Envelope): Transition = match msg.payload {
    | ApiCall(call) => {
      val result = match call.request {
        | ObserveLocal => Observed(ctx.state.volatile.phase)
        | InvalidRequest => Refused(Invalid)
        | _ => Indeterminate // Unreachable: listen_local accepts only the two local calls.
      }
      val prior = ctx.state.volatile.pendingCalls.get(call.key)
      val updated = { ...prior, phase: Replied, result: Some(result) }
      {
        post_state: { ...ctx.state,
          volatile: { ...ctx.state.volatile,
            pendingCalls: ctx.state.volatile.pendingCalls.put(call.key, updated) } },
        effects: Set(C::CustomEffect(Consume(msg))),
      }
    }
    | _ => act_stale(ctx, msg)
  }

  pure def act_read(ctx: Context, msg: Envelope): Transition = match msg.payload {
    | ApiCall(call) => {
      val pending = ctx.state.volatile.pendingCalls.get(call.key)
      val leader = if (ctx.state.volatile.observations.keys().contains("_meta"))
        ctx.state.volatile.observations.get("_meta").observedLeader else None
      val updated = match leader {
        | Some(_) => { ...pending, phase: Submitted }
        | None => { ...pending, phase: Replied, result: Some(Refused(Unavailable)) }
      }
      val outgoing = match leader {
        | Some(r) => Set(C::CustomEffect(Queue({ delivery: ctx.extensions.nextDelivery,
            source: call.key.caller, destination: r.node, channel: MetaChannel, payload: ReadProbe({ owner: call.key.caller, request: call.key, proposal: None, round: 0 }) })))
        | None => Set()
      }
      { post_state: { ...ctx.state, volatile: { ...ctx.state.volatile,
          pendingCalls: ctx.state.volatile.pendingCalls.put(call.key, updated) } },
        effects: outgoing.union(Set(C::CustomEffect(Consume(msg)))) }
    }
    | ReadAck(ack) => {
      val pending = ctx.state.volatile.pendingCalls.get(ack.ticket.request)
      { post_state: { ...ctx.state, volatile: { ...ctx.state.volatile,
          pendingCalls: ctx.state.volatile.pendingCalls.put(ack.ticket.request,
            { ...pending, phase: AwaitingReadApply, readIndex: Some(ack.index) }) } },
        effects: Set(C::CustomEffect(Consume(msg))) }
    }
    | _ => act_stale(ctx, msg)
  }

  pure def main_listener(ctx: Context): Set[Transition] =
    C::cue(ctx, listen_local, act_local).union(C::cue(ctx, listen_read, act_read))
      .union(C::cue(ctx, listen_transaction, act_transaction))
      .union(C::cue(ctx, listen_work, act_work))
      .union(C::cue(ctx, listen_stale, act_stale))

  pure def withinDeliveryBound(tr: Transition, limit: int): bool = tr.effects.forall(e => match e {
    | CustomEffect(custom) => match custom { | Queue(msg) => msg.delivery <= limit | _ => true }
    | _ => true
  })

  pure def apply_custom_effect(env: World, effect: HostEffect): World = match effect {
    | Consume(msg) => { ...env,
      messages: env.messages.setBy(msg.destination, inbox => inbox.exclude(Set(msg))) }
    | Queue(msg) => { ...env, extensions: { ...env.extensions,
        nextDelivery: msg.delivery + 1,
        network: { ...env.extensions.network, pending: env.extensions.network.pending.union(Set(msg)) } } }
  }
}
