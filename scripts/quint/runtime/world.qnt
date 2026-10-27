module world_types {
  import types.* from "../types"
  import choreo as C from "../vendor/choreo"
  type HostEffect = Consume(Envelope) | Queue(Envelope)
  type Context = C::LocalContext[NodeName, NodeState, Envelope, int, Environment]
  type Transition = C::Transition[NodeName, NodeState, Envelope, int, HostEffect]
  type World = C::GlobalContext[NodeName, NodeState, Envelope, int, Environment]
}
