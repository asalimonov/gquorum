package loadgrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/membership"
	"github.com/asalimonov/gquorum/transport/loadgrpc/internal/loadpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const controlLimit = 64 << 10

// TODO(not planned): Convert administrative and unary reply payloads in a separate change.
func decodeJSON(data []byte, dst any) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fault.New(fault.Invalid, "null load JSON", nil)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return fault.New(fault.Invalid, "invalid load JSON", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fault.New(fault.Invalid, "load JSON must contain one value", err)
	}
	return nil
}
func envelope(value any, bodyLimit, frameLimit int) (*loadpb.Envelope, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, fault.New(fault.Invalid, "encode load request", err)
	}
	request := &loadpb.Envelope{Version: 1, Body: body}
	if len(body) > bodyLimit || proto.Size(request) > frameLimit {
		return nil, fault.New(fault.Overloaded, "load request exceeds message limit", nil)
	}
	return request, nil
}

func decodeEnvelope(request *loadpb.Envelope, dst any, bodyLimit, frameLimit int) error {
	if request == nil || request.Version != 1 || len(request.Body) == 0 {
		return status.Error(codes.InvalidArgument, "unsupported or empty load envelope")
	}
	if len(request.Body) > bodyLimit || proto.Size(request) > frameLimit {
		return status.Error(codes.ResourceExhausted, "load request exceeds message limit")
	}
	if bytes.Equal(bytes.TrimSpace(request.Body), []byte("null")) {
		return status.Error(codes.InvalidArgument, "null load request")
	}
	if err := decodeJSON(request.Body, dst); err != nil {
		return status.Error(codes.InvalidArgument, "invalid load request")
	}
	return nil
}
func encodeReply(value any, err error, limit int) (*loadpb.Reply, error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil, status.FromContextError(err).Err()
	}
	reply := &loadpb.Reply{}
	if err != nil {
		reply.ErrorCode = string(fault.Unavailable)
		reply.ErrorMessage = err.Error()
		var typed *fault.Error
		if errors.As(err, &typed) {
			reply.ErrorCode = string(typed.Code)
			reply.ErrorMessage = typed.Message
			reply.ExpectedVersion = typed.ExpectedVersion
			reply.CurrentVersion = typed.CurrentVersion
		}
		var safety *membership.SafetyRefusal
		if errors.As(err, &safety) {
			reply.ErrorCode = string(fault.UnsafeMembership)
			reply.SafetyRefusal, _ = json.Marshal(safety)
		}
	} else {
		var encodeErr error
		reply.Body, encodeErr = json.Marshal(value)
		if encodeErr != nil {
			return nil, status.Error(codes.Internal, "encode load reply")
		}
	}
	if proto.Size(reply) > limit {
		return nil, status.Error(codes.ResourceExhausted, "load reply exceeds message limit")
	}
	return reply, nil
}
func decodeReply(reply *loadpb.Reply, dst any, limit int) error {
	if reply == nil || proto.Size(reply) > limit {
		return fault.New(fault.Unavailable, "missing or oversized load reply", nil)
	}
	if reply.ErrorCode != "" {
		if len(reply.Body) != 0 {
			return fault.New(fault.Unavailable, "ambiguous load reply", nil)
		}
		typed := &fault.Error{
			Code:            fault.Code(reply.ErrorCode),
			Message:         reply.ErrorMessage,
			ExpectedVersion: reply.ExpectedVersion,
			CurrentVersion:  reply.CurrentVersion,
		}
		if len(reply.SafetyRefusal) > 0 {
			safety := new(membership.SafetyRefusal)
			invalidSafety := typed.Code != fault.UnsafeMembership || decodeJSON(reply.SafetyRefusal, safety) != nil
			if invalidSafety || safety.GroupID == "" || safety.ReplicaID == 0 || safety.Reason == "" {
				return fault.New(fault.Unavailable, "invalid membership refusal", nil)
			}
			typed.Cause = safety
		}
		return typed
	}
	if len(reply.SafetyRefusal) > 0 || reply.ErrorMessage != "" || reply.ExpectedVersion != 0 ||
		reply.CurrentVersion != 0 {
		return fault.New(fault.Unavailable, "ambiguous load success", nil)
	}
	output := dst
	if output == nil {
		output = new(struct{})
	}
	return decodeJSON(reply.Body, output)
}
