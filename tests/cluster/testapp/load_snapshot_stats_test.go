//go:build !functional

package main

import (
	"context"
	"testing"

	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/topology"
	"google.golang.org/grpc/stats"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

type scalarSnapshotAck struct{ offset uint64 }

func (a scalarSnapshotAck) GetNextOffset() uint64 { return a.offset }
func TestSnapshotWireStatsMeasureFramesAndCumulativeWindowWithoutPayloadRetention(t *testing.T) {
	t.Parallel()
	dataDescriptor := &descriptorpb.DescriptorProto{Name: proto.String("Data"), Field: []*descriptorpb.FieldDescriptorProto{
		{Name: proto.String("data"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum()},
		{Name: proto.String("offset"), Number: proto.Int32(2), Type: descriptorpb.FieldDescriptorProto_TYPE_UINT64.Enum()},
	}}
	frameDescriptor := &descriptorpb.DescriptorProto{Name: proto.String("Frame"), Field: []*descriptorpb.FieldDescriptorProto{
		{Name: proto.String("data"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(".fixture.Data")},
	}}
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{Name: proto.String("fixture.proto"), Package: proto.String("fixture"),
		Syntax: proto.String("proto3"), MessageType: []*descriptorpb.DescriptorProto{dataDescriptor, frameDescriptor}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	app := &application{clusterHooks: newClusterHooks()}
	key := snapshot.Key{From: load.Peer{Node: topology.Identity{NodeName: "one", InstanceID: "process"}, Replica: load.ReplicaRef{GroupID: "work", ReplicaID: 1}}}
	control := &snapshotControl{request: snapshotControlRequest{Identity: key.From.Node}}
	app.clusterHooks.snapshotControls[walKey{"work", 1}] = control
	handler := snapshotStatsHandler{app: app}
	ctx := handler.TagRPC(context.WithValue(t.Context(), fixtureSnapshotStatsKey{}, key), &stats.RPCTagInfo{})
	for i := 0; i < 2; i++ {
		data := dynamicpb.NewMessage(file.Messages().ByName("Data"))
		data.Set(data.Descriptor().Fields().ByName("data"), protoreflect.ValueOfBytes(make([]byte, 64)))
		data.Set(data.Descriptor().Fields().ByName("offset"), protoreflect.ValueOfUint64(uint64(i*64)))
		frame := dynamicpb.NewMessage(file.Messages().ByName("Frame"))
		frame.Set(frame.Descriptor().Fields().ByName("data"), protoreflect.ValueOfMessage(data))
		handler.HandleRPC(ctx, &stats.OutPayload{Payload: frame})
	}
	handler.HandleRPC(ctx, &stats.InPayload{Payload: scalarSnapshotAck{offset: 128}})
	call := ctx.Value(fixtureSnapshotStatsKey{}).(*snapshotWireCall)
	if control.wire.Frames != 2 || control.wire.ChunkPeak != 64 || control.wire.WindowPeak != 2 || len(call.pending) != 0 {
		t.Fatal(control.wire, call.pending)
	}
}
