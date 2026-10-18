//go:build !functional

package loadgrpc

import (
	"context"
	"errors"
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
