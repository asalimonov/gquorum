package loadgrpc

import (
	"context"
	"unicode/utf8"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
	"github.com/asalimonov/gquorum/transport/loadgrpc/internal/loadpb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const raftWireVersion uint32 = 2
const raftMessagesFieldNumber = 5

// Bound expansion of tiny native messages as well as encoded bytes per frame.
// TODO(not planned): Broader queue/memory accounting remains in the engine.
const maxRaftMessagesPerFrame = 4096

type raftFrameRange struct{ start, end int }

func encodeRaftPeer(p load.Peer) *loadpb.RaftPeer {
	return &loadpb.RaftPeer{ClusterId: p.Node.ClusterID, NodeName: p.Node.NodeName,
		InstanceId: p.Node.InstanceID, MemberId: p.Node.MemberID,
		GroupId: p.Replica.GroupID, ReplicaId: p.Replica.ReplicaID}
}

func validRaftPeer(p load.Peer) bool {
	return p.Replica.ReplicaID != 0 && validIdentity(p.Node) &&
		topology.ValidateLoadID(p.Replica.GroupID) == nil &&
		utf8.ValidString(p.Node.ClusterID) && utf8.ValidString(p.Node.NodeName) &&
		utf8.ValidString(p.Node.InstanceID) && utf8.ValidString(p.Replica.GroupID)
}

func decodeRaftPeer(p *loadpb.RaftPeer) (load.Peer, error) {
	if p == nil {
		return load.Peer{}, status.Error(codes.InvalidArgument, "missing Raft peer")
	}
	peer := load.Peer{Node: topology.Identity{ClusterID: p.ClusterId, NodeName: p.NodeName,
		InstanceID: p.InstanceId, MemberID: p.MemberId},
		Replica: load.ReplicaRef{GroupID: p.GroupId, ReplicaID: p.ReplicaId}}
	if !validRaftPeer(peer) {
		return load.Peer{}, status.Error(codes.InvalidArgument, "invalid Raft peer")
	}
	return peer, nil
}

func validateBatch(b load.RaftBatch) error {
	if len(b.Messages) == 0 || b.DeliveryID == 0 || !validRaftPeer(b.From) || !validRaftPeer(b.To) {
		return fault.ErrInvalid
	}
	if b.From.Node.ClusterID != b.To.Node.ClusterID || b.From.Replica.GroupID != b.To.Replica.GroupID {
		return fault.ErrIdentityMismatch
	}
	for _, msg := range b.Messages {
		if msg.From != b.From.Replica.ReplicaID || msg.To != b.To.Replica.ReplicaID {
			return fault.ErrIdentityMismatch
		}
	}
	return nil
}

func raftFrameHeader(b load.RaftBatch) *loadpb.RaftFrame {
	return &loadpb.RaftFrame{Version: raftWireVersion, From: encodeRaftPeer(b.From),
		To: encodeRaftPeer(b.To), DeliveryId: b.DeliveryID}
}

func planRaftFrames(b load.RaftBatch, limit int) ([]raftFrameRange, error) {
	if err := validateBatch(b); err != nil {
		return nil, err
	}
	headerBytes := proto.Size(raftFrameHeader(b))
	if headerBytes >= limit {
		return nil, fault.ErrOverloaded
	}
	var ranges []raftFrameRange
	start, used := 0, headerBytes
	for i := range b.Messages {
		n := b.Messages[i].Size()
		// Reserve the field tag and length-varint overhead before adding.
		if n <= 0 || n > limit-headerBytes {
			return nil, fault.ErrOverloaded
		}
		overhead := protowire.SizeTag(raftMessagesFieldNumber) + protowire.SizeVarint(uint64(n))
		// Each ordinary native message must fit the configured frame bound.
		if overhead > limit-headerBytes-n {
			return nil, fault.ErrOverloaded
		}
		added := n + overhead
		if added > limit-used || i-start == maxRaftMessagesPerFrame {
			ranges = append(ranges, raftFrameRange{start, i})
			start, used = i, headerBytes
		}
		used += added
	}
	return append(ranges, raftFrameRange{start, len(b.Messages)}), nil
}

func encodeRaftFrames(b load.RaftBatch, limit int) ([]*loadpb.RaftFrame, error) {
	for _, message := range b.Messages {
		if message.Type == raftpb.MsgSnap {
			return nil, fault.ErrUnsupported
		}
	}
	ranges, err := planRaftFrames(b, limit)
	if err != nil {
		return nil, err
	}
	frames := make([]*loadpb.RaftFrame, 0, len(ranges))
	for _, part := range ranges {
		frame := raftFrameHeader(b)
		frame.Messages = make([][]byte, 0, part.end-part.start)
		for i := part.start; i < part.end; i++ {
			data, err := b.Messages[i].Marshal()
			if err != nil {
				return nil, fault.New(fault.Invalid, "encode Raft message", err)
			}
			frame.Messages = append(frame.Messages, data)
		}
		if proto.Size(frame) > limit {
			return nil, fault.ErrOverloaded
		}
		frames = append(frames, frame)
	}
	return frames, nil
}

func decodeRaftHeader(f *loadpb.RaftFrame, limit int) (load.RaftBatch, error) {
	if f == nil || f.Version != raftWireVersion || f.DeliveryId == 0 || len(f.Messages) == 0 {
		return load.RaftBatch{}, status.Error(codes.InvalidArgument, "invalid Raft frame")
	}
	if len(f.Messages) > maxRaftMessagesPerFrame || proto.Size(f) > limit {
		return load.RaftBatch{}, status.Error(codes.ResourceExhausted, "Raft frame exceeds limit")
	}
	from, err := decodeRaftPeer(f.From)
	if err != nil {
		return load.RaftBatch{}, err
	}
	to, err := decodeRaftPeer(f.To)
	if err != nil {
		return load.RaftBatch{}, err
	}
	if from.Node.ClusterID != to.Node.ClusterID || from.Replica.GroupID != to.Replica.GroupID {
		return load.RaftBatch{}, status.Error(codes.InvalidArgument, "inconsistent Raft route")
	}
	return load.RaftBatch{From: from, To: to, DeliveryID: f.DeliveryId}, nil
}

func decodeRaftMessages(ctx context.Context, f *loadpb.RaftFrame, b *load.RaftBatch) error {
	// TODO(not planned): Pooling must not expose recycled data to asynchronous Raft work.
	// Application command bytes stay opaque and caller-formatted.
	messages := make([]raftpb.Message, len(f.Messages))
	for i, data := range f.Messages {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(data) == 0 || messages[i].Unmarshal(data) != nil {
			return status.Error(codes.InvalidArgument, "invalid native Raft message")
		}
		if messages[i].Type == raftpb.MsgSnap {
			return status.Error(codes.Unimplemented, "snapshots require snapshot ingress")
		}
		if messages[i].From != b.From.Replica.ReplicaID || messages[i].To != b.To.Replica.ReplicaID {
			return fault.ErrIdentityMismatch
		}
	}
	b.Messages = messages
	return nil
}
