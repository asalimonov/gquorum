package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
)

// This opt-in fixture proxy forwards the existing metadata protocol unchanged.
// It owns no library state and needs no internal packages or generated schemas.
// Only authenticated InstallFence calls enter the existing fault barriers;
// Node still performs every identity, courier, grant and resource check.
type metaFaultProxy struct {
	app      *application
	listener net.Listener
	internal net.Listener
	server   *grpc.Server
	conn     *grpc.ClientConn
}

type proxyFrame []byte
type proxyCodec struct{}

type metaRaftControl struct {
	Identity      topology.Identity `json:"identity"`
	DropRaft      bool              `json:"drop_raft"`
	FramesDropped uint64            `json:"frames_dropped"`
}

func (a *application) registerMetaFaultRoutes(mux *http.ServeMux, protect func(http.HandlerFunc) http.HandlerFunc) {
	mux.HandleFunc("PUT /internal/test/meta", protect(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Identity topology.Identity `json:"identity"`
			DropRaft bool              `json:"drop_raft"`
		}
		if err := requestBody(w, r, &body); err != nil {
			httpError(w, err)
			return
		}
		if a.metaProxy == nil || body.Identity != a.node.Identity() {
			httpError(w, fault.ErrIdentityMismatch)
			return
		}
		a.clusterHooks.mu.Lock()
		defer a.clusterHooks.mu.Unlock()
		control := &a.clusterHooks.metaRaft
		control.Identity, control.DropRaft = body.Identity, body.DropRaft
		jsonResponse(w, http.StatusOK, *control)
	}))
	mux.HandleFunc("GET /internal/test/meta", protect(func(w http.ResponseWriter, _ *http.Request) {
		a.clusterHooks.mu.Lock()
		defer a.clusterHooks.mu.Unlock()
		jsonResponse(w, http.StatusOK, a.clusterHooks.metaRaft)
	}))
}

func (a *application) dropMetaRaft(frame []byte) bool {
	id := a.node.Identity()
	a.clusterHooks.mu.Lock()
	defer a.clusterHooks.mu.Unlock()
	control := &a.clusterHooks.metaRaft
	if !control.DropRaft || control.Identity != id {
		return false
	}
	target, err := proxyVarintField(frame, 2)
	if err != nil || target == 0 || target != id.MemberID {
		return false
	}
	control.FramesDropped++
	return true
}

func (proxyCodec) Name() string { return "proto" }
func (proxyCodec) Marshal(v any) ([]byte, error) {
	f, ok := v.(*proxyFrame)
	if !ok {
		return nil, errors.New("invalid proxy frame")
	}
	return *f, nil
}
func (proxyCodec) Unmarshal(data []byte, v any) error {
	f, ok := v.(*proxyFrame)
	if !ok {
		return errors.New("invalid proxy frame")
	}
	*f = bytes.Clone(data)
	return nil
}

func (a *application) newMetaFaultProxy(external net.Listener) (net.Listener, error) {
	internal, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient("passthrough:///"+internal.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.ForceCodec(proxyCodec{}), grpc.MaxCallRecvMsgSize(64<<20), grpc.MaxCallSendMsgSize(64<<20)))
	if err != nil {
		_ = internal.Close()
		return nil, err
	}
	p := &metaFaultProxy{app: a, listener: external, internal: internal, conn: conn}
	p.server = grpc.NewServer(grpc.ForceServerCodec(proxyCodec{}), grpc.MaxRecvMsgSize(64<<20), grpc.MaxSendMsgSize(64<<20), grpc.MaxConcurrentStreams(128), grpc.WaitForHandlers(true), grpc.UnknownServiceHandler(p.forward))
	a.metaProxy = p
	return internal, nil
}

func (p *metaFaultProxy) forward(_ any, downstream grpc.ServerStream) error {
	ctx := downstream.Context()
	method, ok := grpc.MethodFromServerStream(downstream)
	if !ok || !strings.HasPrefix(method, "/gquorum.meta.v1.Meta/") {
		return status.Error(codes.Unimplemented, "unknown metadata route")
	}
	md, ok := metadata.FromIncomingContext(ctx)
	credentials := md.Get("gquorum-cluster-credential")
	if !ok || len(credentials) != 1 || !hmac.Equal([]byte(credentials[0]), []byte(hex.EncodeToString([]byte(p.app.cfg.Credential)))) {
		return status.Error(codes.Unauthenticated, "invalid cluster credential")
	}
	ctx, cancel := context.WithCancel(metadata.NewOutgoingContext(ctx, md.Copy()))
	defer cancel()
	var frame proxyFrame
	if err := downstream.RecvMsg(&frame); err != nil {
		return err
	}
	var fence *load.FenceRequest
	if strings.HasSuffix(method, "/InstallFence") {
		request, err := proxyFenceRequest(frame)
		if err != nil {
			return status.Error(codes.InvalidArgument, "invalid fence frame")
		}
		fence = &request
		if err := p.app.waitAuthorityFence(ctx, request, false); err != nil {
			return status.FromContextError(err).Err()
		}
	}
	upstream, err := p.conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true}, method)
	if err != nil {
		return err
	}
	// Metadata has unary and client-streaming calls, each with one reply. Stream
	// frames immediately, without accumulating snapshots or Raft batches.
	for {
		if (strings.HasSuffix(method, "/StreamMessages") || strings.HasSuffix(method, "/SendSnapshot")) && p.app.dropMetaRaft(frame) {
			// A dropped frame cannot acknowledge persistence or snapshot success.
			return status.Error(codes.Unavailable, "metadata Raft delivery withheld")
		}
		if err := upstream.SendMsg(&frame); err != nil {
			return err
		}
		if err := downstream.RecvMsg(&frame); err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
	}
	if err := upstream.CloseSend(); err != nil {
		return err
	}
	if err := upstream.RecvMsg(&frame); err != nil {
		downstream.SetTrailer(upstream.Trailer())
		return err
	}
	if fence != nil {
		code, err := proxyBytesField(frame, 1)
		if err != nil {
			return status.Error(codes.DataLoss, "invalid fence reply")
		}
		if len(code) == 0 {
			data, err := proxyBytesField(frame, 4)
			var result load.FenceResult
			if err != nil || json.Unmarshal(data, &result) != nil || result.From != fence.To || result.Evidence.Receiver != fence.To || result.Evidence.Resource != fence.Resource || result.Evidence.ReplicaID != fence.ReplicaID || result.Evidence.Validate(fence.Grant) != nil {
				return status.Error(codes.DataLoss, "invalid fence evidence")
			}
			p.app.captureAuthorityFence(result)
			if err := p.app.waitAuthorityFence(ctx, *fence, true); err != nil {
				return status.FromContextError(err).Err()
			}
		}
	}
	if headers, err := upstream.Header(); err == nil {
		if err := downstream.SendHeader(headers); err != nil {
			return err
		}
	}
	downstream.SetTrailer(upstream.Trailer())
	return downstream.SendMsg(&frame)
}

func proxyVarintField(data []byte, field protowire.Number) (uint64, error) {
	var value uint64
	seen := false
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return 0, protowire.ParseError(n)
		}
		data = data[n:]
		if num == field {
			if seen || typ != protowire.VarintType {
				return 0, errors.New("ambiguous proxy field")
			}
			seen = true
			value, n = protowire.ConsumeVarint(data)
		} else {
			n = protowire.ConsumeFieldValue(num, typ, data)
		}
		if n < 0 {
			return 0, protowire.ParseError(n)
		}
		data = data[n:]
	}
	return value, nil
}

func proxyBytesField(data []byte, field protowire.Number) ([]byte, error) {
	var value []byte
	seen := false
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if num == field {
			if seen || typ != protowire.BytesType {
				return nil, errors.New("ambiguous proxy field")
			}
			seen = true
			value, n = protowire.ConsumeBytes(data)
		} else {
			n = protowire.ConsumeFieldValue(num, typ, data)
		}
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
	}
	return value, nil
}

func proxyFenceRequest(frame []byte) (load.FenceRequest, error) {
	var request load.FenceRequest
	data, err := proxyBytesField(frame, 2)
	if err != nil {
		return request, err
	}
	if err := decodeJSON(data, &request); err != nil {
		return request, err
	}
	header, err := proxyBytesField(frame, 1)
	if err != nil {
		return request, err
	}
	member, err := proxyVarintField(header, 3)
	if err != nil || member == 0 || member != request.From.MemberID {
		return request, errors.New("fence envelope member mismatch")
	}
	for field, expected := range map[protowire.Number]string{1: request.From.ClusterID, 2: request.From.NodeName, 6: request.From.InstanceID} {
		value, err := proxyBytesField(header, field)
		if err != nil || expected == "" || string(value) != expected {
			return request, errors.New("fence envelope identity mismatch")
		}
	}
	if request.Grant.Validate() != nil {
		return request, errors.New("invalid load fence scope")
	}
	return request, nil
}
