module handlers {
  import types.* from "./types"
  import basicSpells.* from "./vendor/spells/basicSpells"
  import choreo as C from "./vendor/choreo"

  type HostEffect = Consume(Envelope)
  type Context = C::LocalContext[NodeName, NodeState, Envelope, int, Environment]
  type Transition = C::Transition[NodeName, NodeState, Envelope, int, HostEffect]
  type World = C::GlobalContext[NodeName, NodeState, Envelope, int, Environment]

  pure def listen_local(ctx: Context): Set[Envelope] =
    ctx.messages.filter(msg => match msg.payload {
      | ApiCall(call) => and {
        msg.destination == ctx.state.process_id,
        call.key.caller.node == ctx.state.process_id,
        call.key.caller.incarnation == ctx.state.volatile.incarnation,
        ctx.state.volatile.pendingCalls.keys().contains(call.key),
        call.request == ObserveLocal or call.request == InvalidRequest,
      }
    })

  pure def listen_stale(ctx: Context): Set[Envelope] =
    ctx.messages.exclude(listen_local(ctx))

  pure def act_stale(ctx: Context, msg: Envelope): Transition = {
    post_state: ctx.state, effects: Set(C::CustomEffect(Consume(msg))),
  }

  pure def act_local(ctx: Context, msg: Envelope): Transition = match msg.payload {
    | ApiCall(call) => {
      val result = match call.request {
        | ObserveLocal => Observed(ctx.state.volatile.phase)
        | InvalidRequest => Refused(Invalid)
        | Mutate(_) => Refused(Unsupported)
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
  }

  pure def main_listener(ctx: Context): Set[Transition] =
    C::cue(ctx, listen_local, act_local).union(C::cue(ctx, listen_stale, act_stale))

  pure def apply_custom_effect(env: World, effect: HostEffect): World = match effect {
    | Consume(msg) => { ...env,
      messages: env.messages.setBy(msg.destination, inbox => inbox.exclude(Set(msg))) }
  }
}
