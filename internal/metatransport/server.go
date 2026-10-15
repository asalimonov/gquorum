// Package metatransport implements the dedicated topology control protocol.
package metatransport

import (
	"bytes"
	"context"
	"crypto/hmac"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/membership"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const credentialKey = "gquorum-cluster-credential"
const snapshotChunkBytes = 256 << 10

// Handler validates cluster and exact peer identities independently of
// credentials.
type Handler interface {
	Step(context.Context, *metapb.Header, raftpb.Message) error
	Call(context.Context, string, *metapb.Header, []byte) *metapb.Reply
}
type server struct {
	metapb.UnimplementedMetaServer
	handler     Handler
	clusterID   string
	credential  []byte
	maxSnapshot int
}

func (s *server) auth(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	values := md.Get(credentialKey)
	if !ok || len(values) != 1 || !hmac.Equal([]byte(values[0]), []byte(hex.EncodeToString(s.credential))) {
		return status.Error(codes.Unauthenticated, "invalid cluster credential")
	}
	return nil
}
func (s *server) header(h *metapb.Header) error {
	if h == nil || h.ClusterId != s.clusterID || h.NodeName == "" {
		return fault.ErrIdentityMismatch
	}
	return nil
}

// Reply encodes classified errors and retained refusal details in a meta
// protocol reply.
func Reply(err error) *metapb.Reply {
	if err == nil {
		return &metapb.Reply{}
	}
	code := fault.Unavailable
	var typed *fault.Error
	if errors.As(err, &typed) {
		code = typed.Code
	}
	details := errorDetails{}
	if typed != nil {
		details.ExpectedVersion, details.CurrentVersion = typed.ExpectedVersion, typed.CurrentVersion
		details.RebasedTo = typed.RebasedTo
	}
	var safety *membership.SafetyRefusal
	if errors.As(err, &safety) {
		code = fault.UnsafeMembership
		details.Membership = safety
	}
	var placement *topology.PlacementError
	if errors.As(err, &placement) {
		code = placement.Code
		report := placement.Report.Clone()
		details.Placement = &report
	}
	data, _ := json.Marshal(details)
	return &metapb.Reply{ErrorCode: string(code), Message: err.Error(), Data: data}
}

// ReplyError reconstructs classified errors and membership or placement details
// from a reply.
func ReplyError(r *metapb.Reply) error {
	if r == nil {
		return fault.ErrUnavailable
	}
	if r.ErrorCode == "" {
		return nil
	}
	var details errorDetails
	if len(r.Data) > 0 {
		if err := json.Unmarshal(r.Data, &details); err != nil {
			return fault.New(fault.Unreadable, "invalid refusal metadata", err)
		}
	}
	if details.Placement != nil {
		return &topology.PlacementError{Code: fault.Code(r.ErrorCode), Report: details.Placement.Clone()}
	}
	result := fault.New(fault.Code(r.ErrorCode), r.Message, nil)
	if result.Code == fault.VersionConflict {
		result.ExpectedVersion, result.CurrentVersion = details.ExpectedVersion, details.CurrentVersion
		result.RebasedTo = details.RebasedTo
	}
	if result.Code == fault.UnsafeMembership && details.Membership != nil {
		result.Cause = details.Membership
	}
	return result
}

func (s *server) StreamMessages(stream grpc.ClientStreamingServer[metapb.RaftMessage, metapb.Reply]) error {
	if err := s.auth(stream.Context()); err != nil {
		return err
	}
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&metapb.Reply{})
		}
		if err != nil {
			return err
		}
		if err = s.header(frame.Header); err != nil {
			return stream.SendAndClose(Reply(err))
		}
		var msg raftpb.Message
		if err = msg.Unmarshal(frame.RaftMessage); err != nil {
			return stream.SendAndClose(Reply(fault.ErrInvalid))
		}
		if msg.Type == raftpb.MsgSnap || frame.ToMemberId != msg.To || frame.Header.MemberId != msg.From {
			return stream.SendAndClose(Reply(fault.ErrIdentityMismatch))
		}
		if err = s.handler.Step(stream.Context(), frame.Header, msg); err != nil {
			return stream.SendAndClose(Reply(err))
		}
	}
}

func (s *server) SendSnapshot(stream grpc.ClientStreamingServer[metapb.SnapshotChunk, metapb.Reply]) error {
	if err := s.auth(stream.Context()); err != nil {
		return err
	}
	var assembled snapshotAssembly
	for {
		frame, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return stream.SendAndClose(Reply(fault.New(fault.Invalid, "snapshot ended without final frame", nil)))
			}
			return err
		}
		if err = s.header(frame.Header); err != nil {
			return stream.SendAndClose(Reply(err))
		}
		if err := assembled.appendFrame(frame, s.maxSnapshot); err != nil {
			return stream.SendAndClose(Reply(err))
		}
		if frame.Final {
			return s.finishSnapshotStream(stream, assembled)
		}
	}
}

type snapshotAssembly struct {
	header *metapb.Header
	msg    raftpb.Message
	data   []byte
	frames int
}

func (a *snapshotAssembly) appendFrame(frame *metapb.SnapshotChunk, maxSnapshot int) error {
	if len(frame.Data) > snapshotChunkBytes || len(a.data) > maxSnapshot-len(frame.Data) {
		return fault.New(fault.Overloaded, "snapshot exceeds transport bound", nil)
	}
	if a.frames == 0 {
		a.header = frame.Header
		if err := a.msg.Unmarshal(frame.RaftMessage); err != nil {
			return fault.ErrInvalid
		}
		if a.msg.Type != raftpb.MsgSnap || a.msg.Snapshot == nil || len(a.msg.Snapshot.Data) != 0 ||
			a.msg.From != a.header.MemberId ||
			a.msg.To != frame.ToMemberId {
			return fault.ErrIdentityMismatch
		}
	} else if len(frame.RaftMessage) != 0 || frame.Header.ClusterId != a.header.ClusterId ||
		frame.Header.MemberId != a.header.MemberId || frame.Header.NodeName != a.header.NodeName ||
		frame.ToMemberId != a.msg.To {
		return fault.ErrIdentityMismatch
	}
	a.frames++
	if len(frame.Data) == 0 || a.frames > maxSnapshot/snapshotChunkBytes+2 {
		return fault.New(fault.Invalid, "invalid snapshot chunk framing", nil)
	}
	a.data = append(a.data, frame.Data...)
	return nil
}

func (s *server) finishSnapshotStream(stream grpc.ClientStreamingServer[metapb.SnapshotChunk, metapb.Reply],
	a snapshotAssembly) error {
	if len(a.data) == 0 {
		return stream.SendAndClose(Reply(fault.ErrInvalid))
	}
	// A final frame must also terminate the client stream.
	if _, err := stream.Recv(); err != io.EOF {
		if err != nil {
			return err
		}
		return stream.SendAndClose(Reply(fault.ErrInvalid))
	}
	a.msg.Snapshot.Data = bytes.Clone(a.data)
	return stream.SendAndClose(Reply(s.handler.Step(stream.Context(), a.header, a.msg)))
}

func (s *server) call(ctx context.Context, method string, req *metapb.MetaRequest) (*metapb.Reply, error) {
	if err := s.auth(ctx); err != nil {
		return nil, err
	}
	if err := s.header(req.Header); err != nil {
		return Reply(err), nil
	}
	return s.handler.Call(ctx, method, req.Header, bytes.Clone(req.Data)), nil
}

func (s *server) Join(ctx context.Context, req *metapb.MetaRequest) (*metapb.Reply, error) {
	return s.call(ctx, "Join", req)
}

func (s *server) JoinReady(ctx context.Context, req *metapb.MetaRequest) (*metapb.Reply, error) {
	return s.call(ctx, "JoinReady", req)
}

func (s *server) KeepAlive(ctx context.Context, req *metapb.MetaRequest) (*metapb.Reply, error) {
	return s.call(ctx, "KeepAlive", req)
}

func (s *server) IdentityPing(ctx context.Context, req *metapb.MetaRequest) (*metapb.Reply, error) {
	return s.call(ctx, "IdentityPing", req)
}

func (s *server) MetaStatus(ctx context.Context, req *metapb.MetaRequest) (*metapb.Reply, error) {
	return s.call(ctx, "MetaStatus", req)
}

func (s *server) MetaReplicaMetrics(ctx context.Context, req *metapb.MetaRequest) (*metapb.Reply, error) {
	return s.call(ctx, "MetaReplicaMetrics", req)
}

func (s *server) MetaControl(ctx context.Context, req *metapb.MetaRequest) (*metapb.Reply, error) {
	return s.call(ctx, "MetaControl", req)
}

func (s *server) ProposeMeta(ctx context.Context, req *metapb.MetaRequest) (*metapb.Reply, error) {
	return s.call(ctx, "ProposeMeta", req)
}

func (s *server) ApplyVersion(ctx context.Context, req *metapb.MetaRequest) (*metapb.Reply, error) {
	return s.call(ctx, "ApplyVersion", req)
}

func (s *server) ParkedRead(ctx context.Context, req *metapb.MetaRequest) (*metapb.Reply, error) {
	return s.call(ctx, "ParkedRead", req)
}

type errorDetails struct {
	RebasedTo       uint64                    `json:"rebased_to,omitempty"`
	ExpectedVersion uint64                    `json:"expected_version,omitempty"`
	CurrentVersion  uint64                    `json:"current_version,omitempty"`
	Membership      *membership.SafetyRefusal `json:"membership,omitempty"`
	Placement       *topology.PlacementReport `json:"placement,omitempty"`
}

func (s *server) InstallFence(ctx context.Context, req *metapb.MetaRequest) (*metapb.Reply, error) {
	return s.call(ctx, "InstallFence", req)
}

func (s *server) AuthorityRead(ctx context.Context, req *metapb.MetaRequest) (*metapb.Reply, error) {
	return s.call(ctx, "AuthorityRead", req)
}
