module message_effects {
  import types.* from "../types"
  import world_types.* from "./world"
  pure def apply_custom_effect(env: World, effect: HostEffect): World = match effect {
    | Consume(msg) => { ...env,
      messages: env.messages.setBy(msg.destination, inbox => inbox.exclude(Set(msg))) }
    | Queue(msg) => { ...env, extensions: { ...env.extensions,
        nextDelivery: msg.delivery + 1,
        network: { ...env.extensions.network, pending: env.extensions.network.pending.union(Set(msg)) } } }
  }
}
