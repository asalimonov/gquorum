# Load gRPC adapter

`loadgrpc.Transport` implements `load.Transport` for managed replicas. It carries
Raft messages, replica/native reads, approved actions and fences. The application
supplies the server, authentication policy, TLS settings and endpoint resolver.
It carries only library load traffic: gquorum provides no application RPC
channel, and applications hand work between replicas through replicated state.

Construct the adapter before Node, then bind it exactly once:

```go
credential, err := loadgrpc.NewSharedToken(secret, true) // require TLS on clients
if err != nil { return err }
transport, err := loadgrpc.New(loadgrpc.Options{
    Resolve: resolveExactIdentity,
    Authenticate: credential.Authenticate,
    DialOptions: []grpc.DialOption{
        grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)),
        grpc.WithPerRPCCredentials(credential),
    },
})
if err != nil { return err }
options.Loads.Transport = transport
node, err := gquorum.NewNode(options)
if err != nil { return err }
if err := transport.Bind(node); err != nil { return err }
server := grpc.NewServer(
    grpc.Creds(credentials.NewTLS(serverTLS)),
    grpc.MaxRecvMsgSize(16 << 20),
    grpc.MaxSendMsgSize(16 << 20),
)
transport.Register(server)
// Serve the listener before node.Start.
```

`New` performs no I/O. `Resolve(ctx, identity)` receives the full destination
identity on every operation; `gquorum.StaticPeers.ResolveLoad` serves a fixed
roster. Keep routes for retained cleanup destinations;
looking up only the latest topology may lose an endpoint needed by an approved
plan. The receiver's Node verifies exact identities and authority independently.

A shared token proves possession of the cluster secret among trusted members.
It does not prove a unique node identity. Node validates both peers of every
incoming batch against approved topology, so a static roster check in
`Authenticate` is unnecessary. For certificate-bound identities,
provide an `Authenticate` callback that verifies the peer certificate against
the declared sender. The plaintext Compose demo may explicitly pass `false` to
`NewSharedToken` and use `insecure.NewCredentials`; credentials have no implicit
plaintext fallback.

Defaults are 64 cached connections, 128 concurrent incoming/outgoing operations,
16 MiB protobuf messages and a 10-second operation timeout. Only idle connections
are evicted. A full pool or call limit returns `fault.Overloaded`. Control request
bodies are limited to 64 KiB. Configure the server's message limits to match the
adapter; gRPC's default receive limit is smaller than the adapter default.

Raft batches are split into ordered complete-message frames with the original
endpoints and delivery ID. Oversized individual messages are refused.

Batches without a snapshot travel on a long-lived bidirectional `StreamRaft`
stream for each exact sender/recipient replica pair. `SendRaft` returns once the
frames are written, without waiting for the recipient to step them, so a peer
link is not limited to one batch per round trip. The recipient steps frames in
order and answers only refused batches; the sender reports each refusal as an
unreachable delivery through `Node.ReportLoadDelivery`. A broken stream reports
its last delivery unreachable so Raft resends promptly, and the next batch opens
a new stream. Streams idle for two minutes are closed. A recipient without
`StreamRaft` (an older release) gets unary `SendRaft` calls, and streaming is
retried after a minute.

Snapshot batches always use the unary call: success is reported after all
frames are accepted. Failure reports preserve the same exact identity; canceled
sends also use Node's existing send-failure path.
Typed membership refusals, CAS versions, native evidence and effect receipts
survive the wire encoding. The generated protocol is internal to the adapter.

Keep the transport and server available while `Node.Stop` joins managed work.
Then call `transport.Close(ctx)` and stop the borrowed server. Close fences new
calls, cancels and joins accepted work, and releases owned connections. A timeout
does not abandon teardown; call Close again to join. Authentication/resolution
callbacks must honor cancellation and cannot synchronously wait for Close.
