# Load gRPC adapter

`loadgrpc.Transport` implements `load.Transport` for managed replicas. It carries
Raft messages, chunked managed snapshots, replica/native reads, approved actions and fences. The application
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

Raft requests use binary protobuf `RaftFrame` version 2, with complete `RaftPeer`
identities, the original delivery ID and repeated native `raftpb.Message` bytes.
The adapter preserves application command bytes without interpreting them; snapshot data uses the separate transfer protocol below. Administrative RPC
requests and unary acknowledgement/error bodies retain their JSON codecs.

Batches are planned in full, then encoded into ordered complete-message frames.
Each frame is bounded by `MaxMessageBytes` and 4,096 messages; larger batches split
transparently. An individually oversized ordinary message refuses the whole local send before any frames are transmitted. Native receive buffers are owned copies, safe for asynchronous Raft work. Managed `MsgSnap` is rejected on ordinary ingress and sends; it uses `TransferSnapshot`.

Every frame validates its header, authenticates the declared sender, checks the
current local recipient and decodes native messages before Node ingress. Node
then checks current exact peer authorization. Stream lifetime does not cache
that authorization. Malformed frames end the RPC; attributable identity or Node
refusals retain their delivery ID.

Batches without a snapshot travel on a long-lived bidirectional `StreamRaft`
stream for each exact sender/recipient replica pair. `SendRaft` returns once the
frames are written, without waiting for the recipient to step them, so a peer
link is not limited to one batch per round trip. The recipient steps frames in
order and answers only refused batches; the sender reports each refusal as an
unreachable delivery through `Node.ReportLoadDelivery`. A broken stream reports
its last delivery unreachable so Raft resends promptly, and the next batch opens
a new stream. Streams idle for two minutes are closed. When the bound Node exposes its local group lookup, idle routes for stopped/replaced replicas begin retirement within two janitor ticks. Retirement drains already written frames through CloseSend/EOF, with CallTimeout bounding stalled drains and adapter Close joining them. Missing stream methods
fail; there is no unary downgrade or JSON Raft fallback.

`TransferSnapshot` is a version-1 bidirectional RPC registered on the same load server. Its bounded Open/Data/Finish/Abort frames use independent cached bulk connections to the same resolved address. Ordinary Raft workers, streams, connection flow control and short deadlines remain separate; no third port is opened. CPU, memory, NIC bandwidth and the server are still shared.

Node creation configures `Loads.SnapshotTransfer` (`load/snapshot.Options`). Defaults are 256 MiB maximum encoded Snapshot.Data, one incoming and one outgoing transfer across all groups, directional byte budgets of maximum size times concurrency, 1 MiB chunks and eight outstanding chunks. The adapter narrows payloads to fit `MaxMessageBytes`; server send/receive limits must fit the same frames. Total timeout is 30 minutes; verified-progress timeout is one minute, including disconnected time. Snapshot calls use these independent clocks rather than `CallTimeout`. Reconnect never renews them.

The Node-owned job pins immutable data, hashes it once per publication generation, and owns exactly one native result report. `SendSnapshot` sends only after admission, pipelines the bounded chunk window and reports verified cumulative Acks through its supplied callback. Resume requires the exact manifest, delivery, source/destination process and receiver engine run. Process restart or replacement requires a fresh transfer. Lost final Acks can use the bounded terminal cache; eviction safely requires restart.

`accepted` means the native ingress owns the snapshot. It does not prove persistence or Restore. `not_needed` is an owner-confirmed native skip, with no claim to receiving the body. `node.LoadSnapshots().List` separates delivery state from local receiver installed/skipped/failed observations; these remain diagnostics. Errors distinguish size/frame/capability refusal from transient capacity/group_busy.

Custom managed transports must implement both `load.Transport` and `snapshot.Transport`. `NewNode` refuses missing or typed-nil snapshot capability without I/O; Bind requires a nonnil borrowed `LoadSnapshots()` service with resolved options. Authenticate before invoking its ingress and preserve exact session attachments. Do not report managed snapshot delivery through `ReportLoadDelivery`: the common job does so. There is no unary snapshot fallback. `_meta` and `load.NewStandalone` retain their own transport contracts.
Typed membership refusals, CAS versions, native evidence and effect receipts
survive the wire encoding. The generated protocol is internal to the adapter.

Keep the transport and server available while `Node.Stop` joins managed work.
Then call `transport.Close(ctx)` and stop the borrowed server. Close fences new
calls, cancels and joins accepted work, and releases owned connections. A timeout
does not abandon teardown; call Close again to join. Authentication/resolution
callbacks must honor cancellation and cannot synchronously wait for Close.

This is an unreleased breaking wire revision: rebuild and upgrade all participants
using this adapter together. Ordinary Raft frames require version 2; snapshot Open requires version 1. Unsupported versions refuse explicitly. Retained volumes,
WALs, snapshots, metadata and application encodings need no migration. The process fixture uses this production adapter for Raft and snapshots. See the [validation record](../../docs/testing/binary-raft-transport.md)
for measured codec costs, real-example checks and profiling limitations.

Generated qualification passed through 2,000,000,000 encoded bytes, with a sampled maximum of about 13.6 GB RSS across sender/receiver observations. This is not their sum or a memory bound. See [the measured fixture and phase/resource limits](../../tests/cluster/README.md#measured-snapshot-resources). Larger configured sizes remain unmeasured.
