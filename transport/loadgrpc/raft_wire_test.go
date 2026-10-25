//go:build !functional

package loadgrpc

import (
	"context"
	"errors"
	"fmt"
	"testing"

	legacyproto "github.com/golang/protobuf/proto"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
	"github.com/asalimonov/gquorum/transport/loadgrpc/internal/loadpb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func binaryTestBatch(count int) load.RaftBatch {
	from := load.Peer{Node: topology.Identity{ClusterID: "c", NodeName: "a", InstanceID: "a1", MemberID: 11},
		Replica: load.ReplicaRef{GroupID: "g", ReplicaID: 1}}
	to := load.Peer{Node: topology.Identity{ClusterID: "c", NodeName: "b", InstanceID: "b1", MemberID: 22},
		Replica: load.ReplicaRef{GroupID: "g", ReplicaID: 2}}
	b := load.RaftBatch{From: from, To: to, DeliveryID: 37}
	for i := range count {
		b.Messages = append(b.Messages, raftpb.Message{From: 1, To: 2, Type: raftpb.MsgApp,
			Term: 3, Index: uint64(i + 1), Context: []byte{0, 0xff, 0xfe},
			Entries: []raftpb.Entry{{Term: 3, Index: uint64(i + 1), Data: make([]byte, 80)}}})
	}
	return b
}

func TestBinaryFrameRoundTrip(t *testing.T) {
	t.Parallel()
	b := binaryTestBatch(20)
	frames, err := encodeRaftFrames(b, 500)
	if err != nil || len(frames) < 2 {
		t.Fatalf("split: %d, %v", len(frames), err)
	}
	index := 0
	for _, f := range frames {
		if proto.Size(f) > 500 {
			t.Fatal("oversized frame")
		}
		wire, err := proto.Marshal(f)
		if err != nil {
			t.Fatal(err)
		}
		received := new(loadpb.RaftFrame)
		if err := proto.Unmarshal(wire, received); err != nil {
			t.Fatal(err)
		}
		got, err := decodeRaftHeader(received, 500)
		if err != nil {
			t.Fatal(err)
		}
		if err := decodeRaftMessages(context.Background(), received, &got); err != nil {
			t.Fatal(err)
		}
		if got.From != b.From || got.To != b.To || got.DeliveryID != b.DeliveryID {
			t.Fatal("lost exact route or delivery ID")
		}
		for _, msg := range got.Messages {
			if !legacyproto.Equal(&msg, &b.Messages[index]) {
				t.Fatalf("changed message %d", index)
			}
			index++
		}
	}
	if index != len(b.Messages) {
		t.Fatal("lost messages")
	}
	b.Messages[7].Context = make([]byte, 2000)
	if frames, err := encodeRaftFrames(b, 500); frames != nil || !errors.Is(err, fault.ErrOverloaded) {
		t.Fatal("oversized later message did not refuse atomically", err)
	}
}

func TestBinaryHeaderValidation(t *testing.T) {
	t.Parallel()
	frames, err := encodeRaftFrames(binaryTestBatch(1), 4096)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		edit func(*loadpb.RaftFrame)
	}{
		{"version", func(f *loadpb.RaftFrame) { f.Version = 1 }},
		{"delivery", func(f *loadpb.RaftFrame) { f.DeliveryId = 0 }},
		{"empty", func(f *loadpb.RaftFrame) { f.Messages = nil }},
		{"from", func(f *loadpb.RaftFrame) { f.From = nil }},
		{"to", func(f *loadpb.RaftFrame) { f.To = nil }},
		{"incarnation", func(f *loadpb.RaftFrame) { f.From.InstanceId = "" }},
		{"member", func(f *loadpb.RaftFrame) { f.To.MemberId = 0 }},
		{"replica", func(f *loadpb.RaftFrame) { f.From.ReplicaId = 0 }},
		{"group", func(f *loadpb.RaftFrame) { f.To.GroupId = "other" }},
		{"cluster", func(f *loadpb.RaftFrame) { f.To.ClusterId = "other" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := proto.Clone(frames[0]).(*loadpb.RaftFrame)
			tc.edit(f)
			if _, err := decodeRaftHeader(f, 4096); status.Code(err) != codes.InvalidArgument {
				t.Fatal("accepted invalid header", err)
			}
		})
	}
	if _, err := decodeRaftHeader(nil, 4096); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	if _, err := decodeRaftHeader(frames[0], 1); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
}

func TestBinaryNativeValidation(t *testing.T) {
	t.Parallel()
	for _, corrupt := range [][]byte{nil, {0xff}} {
		frames, err := encodeRaftFrames(binaryTestBatch(2), 4096)
		if err != nil {
			t.Fatal(err)
		}
		frame := frames[0]
		batch, err := decodeRaftHeader(frame, 4096)
		if err != nil {
			t.Fatal(err)
		}
		frame.Messages[1] = corrupt
		if err := decodeRaftMessages(context.Background(), frame, &batch); status.Code(err) != codes.InvalidArgument {
			t.Fatal("accepted corrupt message", err)
		}
		if batch.Messages != nil {
			t.Fatal("published prefix of invalid frame")
		}
	}
}

func TestBinaryNativeKindsAndOwnership(t *testing.T) {
	t.Parallel()
	for kind, name := range raftpb.MessageType_name {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			original := binaryTestBatch(1)
			original.From.Replica.ReplicaID = 1<<63 + 17
			original.To.Replica.ReplicaID = 1<<63 + 19
			original.DeliveryID = ^uint64(0)
			msg := &original.Messages[0]
			msg.Type, msg.From, msg.To = raftpb.MessageType(kind), original.From.Replica.ReplicaID, original.To.Replica.ReplicaID
			msg.Reject, msg.RejectHint, msg.LogTerm, msg.Commit = true, 17, 4, 5
			msg.Entries[0].Data = []byte{0, 0xff, 0xfe}
			msg.Snapshot = &raftpb.Snapshot{Data: []byte{0xff, 0, 0xfe}, Metadata: raftpb.SnapshotMetadata{Index: 7, Term: 3,
				ConfState: raftpb.ConfState{Voters: []uint64{msg.From, msg.To}, Learners: []uint64{3}, VotersOutgoing: []uint64{4}, LearnersNext: []uint64{5}, AutoLeave: true}}}
			want := legacyproto.Clone(msg).(*raftpb.Message)
			frames, err := encodeRaftFrames(original, 4096)
			if msg.Type == raftpb.MsgSnap {
				if !errors.Is(err, fault.ErrUnsupported) {
					t.Fatal("ordinary snapshot planning allowed", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			// Encoding owns bytes even if the engine later reuses its input buffers.
			msg.Context[0], msg.Entries[0].Data[0], msg.Snapshot.Data[0] = 42, 42, 42
			got, err := decodeRaftHeader(frames[0], 4096)
			if err != nil {
				t.Fatal(err)
			}
			if err := decodeRaftMessages(context.Background(), frames[0], &got); err != nil {
				t.Fatal(err)
			}
			// Native decoding owns bytes even after the outer frame is discarded/reused.
			for i := range frames[0].Messages[0] {
				frames[0].Messages[0][i] = 0
			}
			if got.From != original.From || got.To != original.To || got.DeliveryID != original.DeliveryID || !legacyproto.Equal(&got.Messages[0], want) {
				t.Fatal("native fields changed or buffers aliased")
			}
		})
	}
}

func TestBinaryFrameByteAndCountBounds(t *testing.T) {
	t.Parallel()
	for _, size := range []int{0, 1, 90, 127, 128, 16383, 16384} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			t.Parallel()
			b := binaryTestBatch(1)
			b.Messages[0].Context = make([]byte, size)
			frames, err := encodeRaftFrames(b, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			exact := proto.Size(frames[0])
			for _, limit := range []int{exact, exact + 1} {
				got, err := encodeRaftFrames(b, limit)
				if err != nil || len(got) != 1 || proto.Size(got[0]) != exact {
					t.Fatal("exact-size planning", limit, err)
				}
			}
			if got, err := encodeRaftFrames(b, exact-1); got != nil || !errors.Is(err, fault.ErrOverloaded) {
				t.Fatal("limit+1 accepted", err)
			}
		})
	}
	b := binaryTestBatch(maxRaftMessagesPerFrame + 1)
	frames, err := encodeRaftFrames(b, 1<<20)
	if err != nil || len(frames) != 2 || len(frames[0].Messages) != maxRaftMessagesPerFrame || len(frames[1].Messages) != 1 {
		t.Fatal("count did not split", err)
	}
	if _, err := decodeRaftHeader(frames[0], 1<<20); err != nil {
		t.Fatal(err)
	}
	frames[0].Messages = append(frames[0].Messages, frames[1].Messages[0])
	if _, err := decodeRaftHeader(frames[0], 1<<20); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("count limit ignored", err)
	}
	for _, limit := range []int{-1, 0, 1} {
		if _, err := planRaftFrames(b, limit); !errors.Is(err, fault.ErrOverloaded) {
			t.Fatal("invalid byte limit", err)
		}
	}
}

func TestBinarySenderRefusesInvalidBatch(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		edit func(*load.RaftBatch)
		want error
	}{
		{"empty", func(b *load.RaftBatch) { b.Messages = nil }, fault.ErrInvalid},
		{"delivery", func(b *load.RaftBatch) { b.DeliveryID = 0 }, fault.ErrInvalid},
		{"cluster-utf8", func(b *load.RaftBatch) { b.From.Node.ClusterID = "\xff" }, fault.ErrInvalid},
		{"node-utf8", func(b *load.RaftBatch) { b.From.Node.NodeName = "\xff" }, fault.ErrInvalid},
		{"instance-utf8", func(b *load.RaftBatch) { b.To.Node.InstanceID = "\xff" }, fault.ErrInvalid},
		{"group-utf8", func(b *load.RaftBatch) { b.From.Replica.GroupID = "\xff" }, fault.ErrInvalid},
		{"member", func(b *load.RaftBatch) { b.From.Node.MemberID = 0 }, fault.ErrInvalid},
		{"group", func(b *load.RaftBatch) { b.From.Replica.GroupID = "other" }, fault.ErrIdentityMismatch},
		{"cluster", func(b *load.RaftBatch) { b.From.Node.ClusterID = "other" }, fault.ErrIdentityMismatch},
		{"native-from", func(b *load.RaftBatch) { b.Messages[1].From = 3 }, fault.ErrIdentityMismatch},
		{"native-to", func(b *load.RaftBatch) { b.Messages[1].To = 3 }, fault.ErrIdentityMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := binaryTestBatch(2)
			tc.edit(&b)
			if frames, err := encodeRaftFrames(b, 4096); frames != nil || !errors.Is(err, tc.want) {
				t.Fatal("invalid send was not atomic", err)
			}
		})
	}
}

func TestBinaryDecodeCancellationAndEndpointRefusal(t *testing.T) {
	t.Parallel()
	frames, err := encodeRaftFrames(binaryTestBatch(2), 4096)
	if err != nil {
		t.Fatal(err)
	}
	frame := frames[0]
	for _, version := range []uint32{0, 1, 3, ^uint32(0)} {
		frame.Version = version
		if _, err := decodeRaftHeader(frame, 4096); status.Code(err) != codes.InvalidArgument {
			t.Fatal("version accepted", err)
		}
	}
	frame.Version = raftWireVersion
	batch, err := decodeRaftHeader(frame, 4096)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := decodeRaftMessages(ctx, frame, &batch); !errors.Is(err, context.Canceled) || batch.Messages != nil {
		t.Fatal("cancellation lost", err)
	}
	wrong := raftpb.Message{From: 1, To: 99}
	frame.Messages[1], err = wrong.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if err := decodeRaftMessages(context.Background(), frame, &batch); !errors.Is(err, fault.ErrIdentityMismatch) || batch.Messages != nil {
		t.Fatal("endpoint refusal lost", err)
	}
}
