# Multiplexing load Raft groups

One node can host many load groups through one shared network listener. With
the included `transport/loadgrpc` adapter, groups targeting the same peer address
reuse a cached gRPC connection. Each batch identifies its group and exact
replicas, so the receiver can deliver it to the correct Raft engine.

This document describes the current managed load transport and gRPC adapter.
Custom implementations of `load.Transport` may use another network protocol,
but must preserve the same identity, authentication and delivery contracts.
For consensus behavior, see [the Raft model](raft.md); for application wiring,
see [the library guide](library.md).

## 1. What is shared and what is independent

| Resource | Scope |
| --- | --- |
| Load listener and registered gRPC service | Shared by all load groups on a node in the examples |
| Cached `grpc.ClientConn` | Shared by outgoing calls to the same resolved address within one adapter |
| Outgoing `StreamRaft` stream | One exact sender/recipient replica pair, including group and process identities |
| Raft engine, log, snapshots, membership and leader | Independent for each group; each host owns its local replica |
| State-machine instance | Created separately for each local replica by the application factory |

For example, a node hosting `jobs`, `accounting` and `locks` can receive all their
load traffic on `9001`. Those groups may implement different state machines and
hold unrelated state. Within each group, replicas must apply compatible,
deterministic application logic.

The cache and scheduler examples use `9000` for the separate `_meta` transport
and `9001` for load traffic. These are configurable example ports. Creating a
load group does not create a listener. The application creates the load listener
and gRPC server, registers the adapter once, and supplies that adapter through
`NodeOptions.Loads.Transport`.

“One connection” means connection reuse for a peer endpoint, not one socket for
the entire cluster or a promise that one TCP socket lives forever. Each adapter
caches a gRPC client connection by address; gRPC manages the underlying HTTP/2
connections and reconnection. Other peer addresses have their own connections,
and the reverse direction uses the other node's outgoing transport.

```mermaid
flowchart LR
    subgraph A[Node A]
        AJ[jobs replica] --> ASJ[StreamRaft: jobs A to B]
        AA[accounting replica] --> ASA[StreamRaft: accounting A to B]
        ASJ --> AC[Cached gRPC ClientConn for node-b:9001]
        ASA --> AC
    end
    AC -->|HTTP/2 multiplexed streams and RPCs| BL
    subgraph B[Node B]
        BL[Shared load listener :9001] --> BA[Decode and authenticate]
        BA --> BN[Node.ReceiveLoadRaft]
        BN --> BH[Managed host: validate identities and select GroupID]
        BH --> BJ[jobs Raft engine]
        BH --> BB[accounting Raft engine]
        BJ --> BJS[jobs state machine]
        BB --> BAS[accounting state machine]
    end
```

The arrows show one traffic direction. Raft responses follow the same routing
mechanism in the opposite direction. Committed entries reach the selected
state machine after the engine performs its normal persistence and apply work.

## 2. Routing information in the protocol

The public [load transport types](../../load/transport.go) describe endpoints:

```go
type ReplicaRef struct {
    GroupID   string
    ReplicaID uint64
}

type Peer struct {
    Node    topology.Identity
    Replica ReplicaRef
}

type RaftBatch struct {
    From, To   Peer
    DeliveryID uint64
    Messages   []raftpb.Message
}
```

`topology.Identity` includes `ClusterID`, `NodeName`, `_meta` `MemberID` and
process `InstanceID`. `ReplicaID` identifies a load replica within its group;
it is not globally unique. For example, `jobs` replica 2 and `accounting`
replica 2 are different destinations.

The [protobuf protocol](../../transport/loadgrpc/internal/loadpb/load.proto)
wraps public values in `Envelope{version: 1, body: ...}`. The body is JSON encoded
by the adapter; routing identity is inside that body, not inferred from the
TCP port. Decoding checks the envelope version, size and JSON shape.

| RPC | Purpose |
| --- | --- |
| `StreamRaft` | Ordered, non-snapshot Raft batches on a bidirectional stream |
| `SendRaft` | Unary delivery for snapshot batches and streaming fallback |
| `ReadReplica` | Replica diagnostics or requested quorum-confirmed native evidence |
| `ApplyAction` | An approved action with its exact authority grant and receipts |
| `InstallFence` | A fence request for an exact authority resource |

These methods share the registered load service. `ReadReplica` is a library
control/observation operation; it is not an arbitrary application query RPC.

## 3. Sending a batch

1. A group's Raft engine produces messages for a destination replica. Its managed
   transport adapter adds the group's ID and resolves the replica to a full
   `load.Peer` through topology.
2. The managed host validates the peer and creates a `RaftBatch` with exact
   sender/recipient identities and a delivery ID.
3. `loadgrpc.Transport.SendRaft` checks the route and message identities. It splits
   batches into bounded frames without splitting an individual Raft message or
   changing message order. An oversized individual message is refused.
4. The application's resolver maps the recipient node identity to an address.
   The adapter reuses `clients[address]`, opening a gRPC client connection when
   needed. Resolving an address does not itself authorize the destination.
5. Ordinary batches use a stream keyed by `streamKey{from, to load.Peer}`. Groups
   with different IDs therefore have distinct streams even when their numeric
   replica IDs and destination address match. The streams reuse the address's
   client connection, and gRPC multiplexes them using HTTP/2.

Writes to one stream are serialized to preserve order. There is no global
ordering between different groups and no cross-group transaction implied by
sharing a connection.

Snapshot batches use unary `SendRaft`. The adapter also falls back to unary calls
when a recipient does not implement streaming, retrying streaming after one
minute. Idle streams are retired after two minutes.

## 4. Receiving and selecting the replica

Both streamed and unary batches pass through authentication before Node ingress.
The adapter verifies the declared cluster and exact destination node/process
identity, then calls `Node.ReceiveLoadRaft(batch)`.

The node delegates to the managed host, which:

1. Checks that the peer identities are complete, the delivery ID is nonzero,
   and the batch is nonempty.
2. Requires sender and recipient group IDs to match and every embedded Raft
   message's `From`/`To` to match the declared replica IDs.
3. Validates both peers against approved topology.
4. Looks up the local slot by group ID:

   ```go
   slot := h.slots[batch.To.Replica.GroupID]
   ```

5. Requires the slot's complete local peer identity to equal `batch.To` and
   checks that the engine is running and not stopping. Forwarded proposals are
   refused until the replica is published as ready.
6. Calls that slot's `core.StepBatch(ctx, messages)`.

The group ID chooses the local engine; the remaining identity checks ensure that
it is the intended replica incarnation. A delayed packet for a replaced replica
or previous node process is rejected instead of being redirected to its successor.
A missing/unready replica returns a refusal rather than creating a group from
incoming traffic.

The library does not inspect business-command semantics at this boundary. Raft
orders and commits entries, and that replica's state machine interprets their
application payloads.

## 5. Delivery, resource limits and failures

A successful streamed send means frames were written to the stream, not that
the receiver applied them or a quorum committed them. The receiver steps frames
in order and sends `StreamRefusal` only for refused batches. Normal Raft reply
messages provide consensus progress; they travel through Raft transport separately.

Stream refusals are reported through `Node.ReportLoadDelivery`. A broken stream
reports its last delivery unreachable so Raft can resend. Snapshot delivery reports
success only after all unary frames were accepted; transport success is still
distinct from application-command commitment. Delivery reports retain exact
endpoints and delivery IDs, preventing stale reports from affecting replacements.

The adapter defaults to 64 cached connections, 128 concurrent incoming/outgoing
operations and 16 MiB message frames. Control request bodies are limited to
64 KiB. Idle cached connections may be evicted; exhausted limits return
`Overloaded`. See [adapter configuration](../../transport/loadgrpc/README.md).

Independent group engines and streams still share CPU, network bandwidth and
connection flow control. Multiplexing avoids a listener/connection per group; it
does not provide complete performance isolation. A broken shared connection can
affect several groups communicating with that peer.

The application keeps the transport and server available while `Node.Stop` joins
managed work. It then closes the transport, which cancels and joins its streams
and calls and releases its client connections, before stopping the borrowed server.

## 6. Application request routing

HTTP, Redis or application-specific gRPC requests use the application's own
handler. That handler chooses a local group, for example:

```go
group, ok := node.LoadGroup("jobs")
// Handle an absent local replica, then call group.Propose or group.Read.
```

`Propose` enters this group's consensus path; resulting Raft messages use the
shared load transport described above. `Read` uses a safe read barrier and the
selected replica's query machine. The load adapter does not automatically route
business RPCs between workers. Applications can register their own gRPC services
on the same application-owned server, with their own handlers and authentication.

## Implementation references

| Source | Responsibility |
| --- | --- |
| [Managed transport](../../internal/managed/transport.go) | Build endpoint identities, route batches to slots, validate peers and delivery reports |
| [Node transport entry points](../../node_transport.go) | Delegate authenticated ingress to the managed host |
| [gRPC transport](../../transport/loadgrpc/transport.go) | Node binding, service registration, connection cache, bounds and shutdown |
| [Client](../../transport/loadgrpc/client.go), [streams](../../transport/loadgrpc/stream.go) | Frame splitting, stream selection, unary fallback and delivery reports |
| [Server](../../transport/loadgrpc/server.go), [wire encoding](../../transport/loadgrpc/wire.go) | Authentication, decoding and typed dispatch |
| [Cache runtime](../../examples/inmemory-cache/server/runtime.go) | Shared listener/server setup in an application |
| [Stream tests](../../transport/loadgrpc/stream_func_test.go), [adapter tests](../../transport/loadgrpc/transport_func_test.go), [managed ingress tests](../../internal/managed/transport_func_test.go) | Ordering, refusals, authentication, resource limits and exact-identity checks |
