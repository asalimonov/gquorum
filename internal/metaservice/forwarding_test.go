package metaservice

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/topology"
	"google.golang.org/protobuf/proto"
)

// The resolver redirects one follower's real leader route to a protocol seam.
// Raft traffic still reaches that leader; only approval replies are controlled.
type approvalResolver struct {
	mu            sync.RWMutex
	name, address string
}

func (r *approvalResolver) Resolve(_ context.Context, name string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if name == r.name && r.address != "" {
		return r.address, nil
	}
	return "", fault.ErrUnavailable
}

type approvalHandler struct {
	*Group
	mu       sync.Mutex
	reply    *metapb.Reply
	requests []uint64
	headers  []*metapb.Header
}

func (h *approvalHandler) Call(ctx context.Context, method string, header *metapb.Header, data []byte) *metapb.Reply {
	if method != "ApplyVersion" {
		return h.Group.Call(ctx, method, header, data)
	}
	var request struct {
		Version uint64 `json:"version"`
	}
	if err := json.Unmarshal(data, &request); err != nil {
		return metatransport.Reply(fault.ErrInvalid)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.requests = append(h.requests, request.Version)
	h.headers = append(h.headers, proto.Clone(header).(*metapb.Header))
	return proto.Clone(h.reply).(*metapb.Reply)
}

func TestFollowerApprovalPreservesExactRequestAndRejectsEmptyReply(t *testing.T) {
	if testing.Short() {
		t.Skip("functional test; run make test")
	}
	resolvers := map[string]*approvalResolver{}
	groups := publicFoundingClusterWith(t, 3, func(o *Options) {
		r := &approvalResolver{}
		resolvers[o.Identity.NodeName] = r
		o.Network.Resolver = r
	})
	// Startup shares the host's filesystem with the full storage race suite.
	// Its budget is separate from the one-second, one-hop approval assertions.
	boot, cancelBoot := context.WithTimeout(t.Context(), 15*time.Second)
	for _, g := range groups {
		if err := g.WaitReady(boot); err != nil {
			cancelBoot()
			t.Fatal("forwarding fixture startup", g.Identity(), g.LocalStatus(), err)
		}
	}
	cancelBoot()
	var leader *Group
	for _, g := range groups {
		if g.IsLeader() {
			leader = g
			break
		}
	}
	if leader == nil {
		t.Fatal("forwarding fixture has no leader")
	}
	var follower *Group
	for _, g := range groups {
		if g != leader {
			follower = g
			break
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	handler := &approvalHandler{Group: leader}
	remote, err := metatransport.New(metatransport.Options{
		Header: leader.transport.Header(), Listener: listener,
		AllowPlaintext: true, Credential: leader.opts.Network.ClusterCredential,
		Handler: handler, Resolve: leader.resolveMember,
		SendTimeout: time.Second, SnapshotTimeout: time.Second,
		RetryInitial: 10 * time.Millisecond, RetryMax: time.Second, MaxSnapshotBytes: 1 << 20,
	})
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	if err := remote.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := remote.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	r := resolvers[follower.Identity().NodeName]
	r.mu.Lock()
	r.name, r.address = leader.Identity().NodeName, listener.Addr().String()
	r.mu.Unlock()
	conflict := &fault.Error{Code: fault.VersionConflict, Message: "stale approved version", ExpectedVersion: 17, CurrentVersion: 19}
	for _, tc := range []struct {
		name  string
		reply *metapb.Reply
		want  topology.OperatorID
		err   error
	}{
		{"success", &metapb.Reply{Data: []byte(`"exec-v17-a2"`)}, "exec-v17-a2", nil},
		{"empty", &metapb.Reply{Data: []byte(`""`)}, "", fault.ErrUnreadable},
		{"null", &metapb.Reply{Data: []byte(`null`)}, "", fault.ErrUnreadable},
		{"malformed", &metapb.Reply{Data: []byte(`{`)}, "", fault.ErrUnreadable},
		{"wrong_type", &metapb.Reply{Data: []byte(`17`)}, "", fault.ErrUnreadable},
		{"domain_conflict", metatransport.Reply(conflict), "", fault.ErrVersionConflict},
		{"not_leader_hint", &metapb.Reply{ErrorCode: string(fault.NotLeader), LeaderId: follower.Identity().MemberID, LeaderAddress: follower.opts.Network.AdvertiseAddress}, "", fault.ErrNotLeader},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler.mu.Lock()
			handler.reply = tc.reply
			before := len(handler.requests)
			handler.mu.Unlock()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			got, err := follower.Apply(ctx, 17)
			if got != tc.want || !errors.Is(err, tc.err) {
				t.Fatal(got, err)
			}
			if tc.name == "domain_conflict" {
				var typed *fault.Error
				if !errors.As(err, &typed) || typed.ExpectedVersion != 17 || typed.CurrentVersion != 19 || typed.Message != conflict.Error() {
					t.Fatal("lost domain refusal", err)
				}
			}
			handler.mu.Lock()
			defer handler.mu.Unlock()
			if len(handler.requests) != before+1 || handler.requests[before] != 17 {
				t.Fatal("approval did not make exactly one hop", handler.requests)
			}
			header := handler.headers[before]
			identity := follower.Identity()
			if header.ClusterId != identity.ClusterID || header.NodeName != identity.NodeName || header.MemberId != identity.MemberID {
				t.Fatal("lost caller identity", header)
			}
			for _, g := range groups {
				if g.state().Latest().Version != 1 || g.state().Target().Version != 1 || g.state().Active().Version != 1 {
					t.Fatal("forwarding seam changed documents")
				}
			}
		})
	}
	// A forwarded RPC received by a follower must return its leader refusal.
	// Its own route still points to the seam, so a re-forward would be recorded.
	handler.mu.Lock()
	before := len(handler.requests)
	handler.mu.Unlock()
	reply := follower.Call(t.Context(), "ApplyVersion", leader.transport.Header(), []byte(`{"version":17}`))
	if err := metatransport.ReplyError(reply); !errors.Is(err, fault.ErrNotLeader) {
		t.Fatal(err)
	}
	handler.mu.Lock()
	defer handler.mu.Unlock()
	if len(handler.requests) != before {
		t.Fatal("received approval was forwarded again")
	}
}

func TestMetaMemberRoutingSeparatesApprovedInboundFromRetainedOutbound(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	g := f.g
	g.addresses = map[string]string{"n1": "meta-one", "n2": "meta-two", "n3": "meta-three", "n4": "meta-four"}
	for _, tc := range []struct {
		id      uint64
		address string
		err     error
	}{
		{1, "meta-one", nil}, {2, "meta-two", nil}, {0, "", fault.ErrIdentityMismatch}, {99, "", fault.ErrIdentityMismatch},
	} {
		got, err := g.resolveMember(t.Context(), tc.id)
		if got != tc.address || !errors.Is(err, tc.err) {
			t.Fatal(tc.id, got, err)
		}
	}
	doc := g.state().Latest()
	doc.Nodes = append(doc.Nodes, topology.Node{Name: "n4", Type: "worker", State: topology.NodeActive})
	doc.MetaGroup().Replicas = append(doc.MetaGroup().Replicas, topology.Replica{NodeName: "n4", ReplicaID: 4, Role: topology.ReplicaVoter})
	doc.MetaGroup().NextReplicaID = 5
	f.commit(t, doc, false)
	if g.memberNode(4) != "" {
		t.Fatal("staged-only identity authorized inbound")
	}
	// History can resolve outbound destinations, including retained staged rows;
	// that route never admits their incoming identity before approval.
	if address, err := g.resolveMember(t.Context(), 4); err != nil || address != "meta-four" {
		t.Fatal("retained outbound route absent", address, err)
	}
	f.command(t, metaengine.BeginOperatorProposal{Version: 2, Plan: topology.Plan{Version: 2}, Actor: "test", Origin: topology.OriginOperator, At: time.Now().UTC()})
	if address, err := g.resolveMember(t.Context(), 4); err != nil || address != "meta-four" {
		t.Fatal("approved Target route absent", address, err)
	}
	delete(g.addresses, "n4")
	if _, err := g.resolveMember(t.Context(), 4); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("unresolved address accepted", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := g.resolveMember(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
