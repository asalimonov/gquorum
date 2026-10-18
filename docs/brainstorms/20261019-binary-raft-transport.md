# Binary Raft transport discussion

Date: 2026-10-19. Status: target proposed; implementation not started.

## Request and decisions

The user requested a target architecture for replacing JSON-encoded Raft batches
in the library's loadgrpc adapter with binary protobuf. The adapter must remain
generic. Application commands and snapshots are opaque bytes whose format is
selected by the application. The user previously discarded the Sonic migration;
this proposal does not restore it.

Compatibility question: should old JSON-speaking nodes remain usable during a
rolling upgrade? User answer: **“No backward compatibility needed, the library
was not released.”** The target therefore replaces the Raft RPC input types,
requires wire version 2 and removes the older-server fallback. No JSON Raft
receiver, protocol negotiation or dual encoding is proposed.

## Evidence and rationale

Reviewed root AGENTS.md/README.md, architecture/library.md and raft.md, testing
contracts, load.Transport, topology.Identity, loadgrpc implementation and tests,
managed ingress, generation rules and retained physical compatibility guidance.
Base commit: 631a606, with unrelated credential/profiling fixes present.

Capture: test-reports/performance/20261018T201051Z-e899dd0a, 30 seconds, three
nodes, one loader worker, 615 keys, GOMAXPROCS=1, load fsync disabled. Node3 was
the load-group leader. Its total sampled CPU was 16.56 s; streaming RPCs accounted
for 4.61 s, including 2.40 s in loadgrpc.decodeJSON. The three named gRPC wrapper
functions contain the same work; inclusive times overlap. Decoder cost is 52%
of the streaming subtree and 14.5% of node3 CPU. The heap profile attributes
approximately 1.35 GiB of sampled cumulative allocations to decodeJSON, including
854 MiB to Decoder.refill; these are lifetime allocations, not resident memory.

Current encoding: load.RaftBatch -> json.Marshal -> protobuf Envelope.Body.
Reception uses a fresh strict JSON decoder for each complete frame. Raft messages
already have generated Marshal/Unmarshal/Size methods. The meta transport uses
binary Raft message bytes already. Current batch splitting repeatedly serializes
candidate message ranges to discover frame boundaries.

Preferred solution: typed protobuf peer/delivery framing and repeated native
Raft protobuf byte strings. This avoids reflection, text scanning, base64 and
stream-decoder buffers without changing application codecs. It also avoids
copying the entire upstream Raft schema into the library's generated API or
mixing its generated Go types with the adapter's protobuf generator.

Alternatives not selected: replacing JSON codecs globally; custom gRPC codecs;
handwritten copies of Raft message schemas; mixed-version negotiation; JSON
fallback. Follow-on optimizations such as routing indexes, pooling and lifecycle
changes are intentionally independent experiments.

The target specification is [20261019-binary-raft-transport-target.md](20261019-binary-raft-transport-target.md).
Only these design documents are created by this task; runtime code is unchanged.
