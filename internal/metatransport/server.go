// Package metatransport implements the dedicated topology control protocol.
package metatransport

import (
	"bytes"
	"context"
	"crypto/hmac"
	"encoding/hex"
	"errors"
	"io"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metapb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const credentialKey = "gquorum-cluster-credential"
const snapshotChunkBytes = 256 << 10

// Handler validates cluster and exact peer identities independently of credentials.
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
func Reply(err error) *metapb.Reply {
	if err == nil {
		return &metapb.Reply{}
	}
	code := fault.Unavailable
	var typed *fault.Error
	if errors.As(err, &typed) {
		code = typed.Code
	}
	return &metapb.Reply{ErrorCode: string(code), Message: err.Error()}
}
func ReplyError(r *metapb.Reply) error {
	if r == nil {
		return fault.ErrUnavailable
	}
	if r.ErrorCode == "" {
		return nil
	}
	return fault.New(fault.Code(r.ErrorCode), r.Message, nil)
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
	var header *metapb.Header
	var msg raftpb.Message
	var data []byte
	frames := 0
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
		if len(frame.Data) > snapshotChunkBytes || len(data) > s.maxSnapshot-len(frame.Data) {
			return stream.SendAndClose(Reply(fault.New(fault.Overloaded, "snapshot exceeds transport bound", nil)))
		}
		if frames == 0 {
			header = frame.Header
			if err = msg.Unmarshal(frame.RaftMessage); err != nil {
				return stream.SendAndClose(Reply(fault.ErrInvalid))
			}
			if msg.Type != raftpb.MsgSnap || msg.Snapshot == nil || len(msg.Snapshot.Data) != 0 || msg.From != header.MemberId || msg.To != frame.ToMemberId {
				return stream.SendAndClose(Reply(fault.ErrIdentityMismatch))
			}
		} else if len(frame.RaftMessage) != 0 || frame.Header.ClusterId != header.ClusterId || frame.Header.MemberId != header.MemberId || frame.Header.NodeName != header.NodeName || frame.ToMemberId != msg.To {
			return stream.SendAndClose(Reply(fault.ErrIdentityMismatch))
		}
		frames++
		if len(frame.Data) == 0 || frames > s.maxSnapshot/snapshotChunkBytes+2 {
			return stream.SendAndClose(Reply(fault.New(fault.Invalid, "invalid snapshot chunk framing", nil)))
		}
		data = append(data, frame.Data...)
		if frame.Final {
			if len(data) == 0 {
				return stream.SendAndClose(Reply(fault.ErrInvalid))
			}
			// A final frame must also terminate the client stream.
			if _, err = stream.Recv(); err != io.EOF {
				if err != nil {
					return err
				}
				return stream.SendAndClose(Reply(fault.ErrInvalid))
			}
			msg.Snapshot.Data = bytes.Clone(data)
			return stream.SendAndClose(Reply(s.handler.Step(stream.Context(), header, msg)))
		}
	}
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
