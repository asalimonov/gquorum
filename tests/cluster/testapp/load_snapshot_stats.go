package main

import (
	"context"
	"sync"

	"github.com/asalimonov/gquorum/load/snapshot"
	"google.golang.org/grpc/stats"
	"google.golang.org/protobuf/proto"
)

type fixtureSnapshotStatsKey struct{}
type snapshotWireCounters struct {
	Frames     uint64 `json:"frames"`
	ChunkPeak  uint64 `json:"chunk_peak"`
	WindowPeak uint64 `json:"window_peak"`
}
type snapshotWireCall struct {
	mu           sync.Mutex
	key          snapshot.Key
	pending      []uint64
	acknowledged uint64
}
type snapshotStatsHandler struct{ app *application }

func (h snapshotStatsHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	if key, ok := ctx.Value(fixtureSnapshotStatsKey{}).(snapshot.Key); ok {
		return context.WithValue(ctx, fixtureSnapshotStatsKey{}, &snapshotWireCall{key: key})
	}
	return ctx
}
func (h snapshotStatsHandler) HandleRPC(ctx context.Context, event stats.RPCStats) {
	call, ok := ctx.Value(fixtureSnapshotStatsKey{}).(*snapshotWireCall)
	if !ok {
		return
	}
	switch value := event.(type) {
	case *stats.OutPayload:
		message, ok := value.Payload.(proto.Message)
		if !ok {
			return
		}
		reflected := message.ProtoReflect()
		field := reflected.Descriptor().Fields().ByName("data")
		if field == nil || !reflected.Has(field) {
			return
		}
		data := reflected.Get(field).Message()
		fields := data.Descriptor().Fields()
		payload, offset := fields.ByName("data"), fields.ByName("offset")
		if payload == nil || offset == nil {
			return
		}
		n := uint64(len(data.Get(payload).Bytes()))
		end := data.Get(offset).Uint() + n
		call.mu.Lock()
		if end > call.acknowledged {
			call.pending = append(call.pending, end)
		}
		outstanding := uint64(len(call.pending))
		call.mu.Unlock()
		h.record(call.key, n, outstanding)
	case *stats.InPayload:
		ack, ok := value.Payload.(interface{ GetNextOffset() uint64 })
		if !ok {
			return
		}
		call.mu.Lock()
		call.acknowledged = max(call.acknowledged, ack.GetNextOffset())
		pending := call.pending[:0]
		for _, end := range call.pending {
			if end > call.acknowledged {
				pending = append(pending, end)
			}
		}
		call.pending = pending
		call.mu.Unlock()
	default:
	}
}
func (h snapshotStatsHandler) record(key snapshot.Key, bytes, outstanding uint64) {
	hooks := h.app.clusterHooks
	if hooks == nil {
		return
	}
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	if control := hooks.snapshotControls[walKey{key.From.Replica.GroupID, key.From.Replica.ReplicaID}]; control != nil && control.request.Identity == key.From.Node {
		control.wire.Frames++
		control.wire.ChunkPeak = max(control.wire.ChunkPeak, bytes)
		control.wire.WindowPeak = max(control.wire.WindowPeak, outstanding)
	}
}
func (snapshotStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (snapshotStatsHandler) HandleConn(context.Context, stats.ConnStats) {}
