//go:build !functional

package loadgrpc

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/membership"
	"github.com/asalimonov/gquorum/topology"
	"github.com/asalimonov/gquorum/transport/loadgrpc/internal/loadpb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestStructuredErrors(t *testing.T) {
	t.Parallel()
	refusal := &membership.SafetyRefusal{GroupID: "g", ReplicaID: 3, Operation: "remove", Reason: "quorum", Quorum: 2, LiveVoters: 1, HealthKnown: true, CatchUpLag: 10}
	for _, original := range []error{refusal, fault.New(fault.UnsafeMembership, "refused", refusal), &fault.Error{Code: fault.VersionConflict, Message: "changed", ExpectedVersion: 3, CurrentVersion: 4}, fault.ErrAuthorityFenced} {
		reply, err := encodeReply(nil, original, 1024)
		if err != nil {
			t.Fatal(err)
		}
		restored := decodeReply(reply, nil, 1024)
		var before, after *fault.Error
		if errors.As(original, &before) {
			if !errors.As(restored, &after) || before.Code != after.Code || before.ExpectedVersion != after.ExpectedVersion || before.CurrentVersion != after.CurrentVersion {
				t.Fatalf("lost error details: %v", restored)
			}
		}
		var safety *membership.SafetyRefusal
		if errors.As(original, &safety) {
			var got *membership.SafetyRefusal
			if !errors.As(restored, &got) || !reflect.DeepEqual(safety, got) {
				t.Fatalf("lost refusal: %v", restored)
			}
		}
	}
	reply := &loadpb.Reply{ErrorCode: string(fault.UnsafeMembership), SafetyRefusal: []byte(`{}`)}
	if !errors.Is(decodeReply(reply, nil, 1024), fault.ErrUnavailable) {
		t.Fatal("accepted empty safety refusal")
	}
	reply = &loadpb.Reply{Body: []byte(`{}`), ErrorCode: string(fault.Stopped)}
	if !errors.Is(decodeReply(reply, nil, 1024), fault.ErrUnavailable) {
		t.Fatal("accepted contradictory reply")
	}
}
func TestSplitBatchPreservesOrderAndIdentity(t *testing.T) {
	t.Parallel()
	from := load.Peer{Node: topology.Identity{ClusterID: "c", NodeName: "a", MemberID: 1, InstanceID: "i"}, Replica: load.ReplicaRef{GroupID: "g", ReplicaID: 1}}
	to := from
	to.Node.NodeName = "b"
	to.Node.MemberID = 2
	to.Replica.ReplicaID = 2
	batch := load.RaftBatch{From: from, To: to, DeliveryID: 37}
	for i := range 20 {
		batch.Messages = append(batch.Messages, raftpb.Message{From: 1, To: 2, Type: raftpb.MsgApp, Term: 2, Index: uint64(i + 1), Context: []byte(strings.Repeat("x", 100))})
	}
	frames, err := encodeRaftFrames(batch, 1500)
	if err != nil || len(frames) < 2 {
		t.Fatalf("split: %d %v", len(frames), err)
	}
	var messages []raftpb.Message
	for _, frame := range frames {
		if proto.Size(frame) > 1500 {
			t.Fatal("oversized frame")
		}
		part, err := decodeRaftHeader(frame, 1500)
		if err != nil {
			t.Fatal(err)
		}
		if err := decodeRaftMessages(context.Background(), frame, &part); err != nil {
			t.Fatal(err)
		}
		if part.From != from || part.To != to || part.DeliveryID != 37 {
			t.Fatal("changed exact endpoint")
		}
		messages = append(messages, part.Messages...)
	}
	if !sameRaftMessages(messages, batch.Messages) {
		t.Fatal("lost or reordered messages")
	}
	batch.Messages[3].Context = make([]byte, 2000)
	if frames, err := encodeRaftFrames(batch, 1500); !errors.Is(err, fault.ErrOverloaded) || len(frames) != 0 {
		t.Fatal("oversized message not refused atomically")
	}
}
func TestStrictEnvelopes(t *testing.T) {
	t.Parallel()
	for _, body := range []string{`null`, `{} {}`, `{"foreign":1}`, `[]`, ``} {
		if err := decodeEnvelope(&loadpb.Envelope{Version: 1, Body: []byte(body)}, new(load.ReplicaQuery), 1024, 2048); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("accepted %q: %v", body, err)
		}
	}
	if err := decodeEnvelope(&loadpb.Envelope{Version: 2, Body: []byte(`{}`)}, new(load.ReplicaQuery), 1024, 2048); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	if err := decodeEnvelope(&loadpb.Envelope{Version: 1, Body: []byte(strings.Repeat(" ", 1025))}, new(load.ReplicaQuery), 1024, 2048); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	original := load.ReplicaQuery{Native: true}
	request, _ := envelope(original, 1024, 2048)
	var got load.ReplicaQuery
	if err := decodeEnvelope(request, &got, 1024, 2048); err != nil || got != original {
		t.Fatal(got, err)
	}
}
func TestSharedToken(t *testing.T) {
	t.Parallel()
	token, err := NewSharedToken("test-secret", true)
	if err != nil || !token.RequireTransportSecurity() {
		t.Fatal(err)
	}
	values, err := token.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(values))
	if err := token.Authenticate(ctx, topology.Identity{}); err != nil {
		t.Fatal(err)
	}
	for _, headers := range [][]string{nil, {"Bearer wrong"}, {"Bearer test-secret", "Bearer test-secret"}, {"test-secret"}} {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{"authorization": headers})
		if err := token.Authenticate(ctx, topology.Identity{}); !errors.Is(err, fault.ErrIdentityMismatch) {
			t.Fatal("accepted bad credential", err)
		}
	}
	for _, bad := range []string{"", "x\ny", "x y", strings.Repeat("a", 4097)} {
		if _, err := NewSharedToken(bad, false); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal("accepted invalid token")
		}
	}
	encoded, _ := json.Marshal(token)
	if strings.Contains(string(encoded), "secret") {
		t.Fatal("credential exposed by JSON")
	}
}
func TestNewHasNoCallbacks(t *testing.T) {
	t.Parallel()
	opts := Options{Resolve: func(context.Context, topology.Identity) (string, error) {
		t.Fatal("resolve during construction")
		return "", nil
	}, Authenticate: func(context.Context, topology.Identity) error { t.Fatal("auth during construction"); return nil }, DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}}
	transport, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := transport.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	opts.MaxConnections = -1
	if _, err := New(opts); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("negative connection bound accepted")
	}
}
