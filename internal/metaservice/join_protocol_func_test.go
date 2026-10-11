//go:build functional

package metaservice

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/topology"
)

// Control replies isolate admission-client policy from election timing. The
// actual join/ready/membership path is covered by join_test.go and process tests.
type joinProtocolHandler struct {
	*Group
	mu       sync.Mutex
	reply    *metapb.Reply
	requests []joinRequest
}

func (h *joinProtocolHandler) Call(_ context.Context, method string, header *metapb.Header, data []byte) *metapb.Reply {
	h.mu.Lock()
	defer h.mu.Unlock()
	var req joinRequest
	if method != "Join" || json.Unmarshal(data, &req) != nil || req.Identity.NodeName != header.NodeName || req.Identity.MemberID != header.MemberId || req.Identity.InstanceID != header.InstanceId || req.Identity.ClusterID != header.ClusterId {
		return metatransport.Reply(fault.ErrInvalid)
	}
	h.requests = append(h.requests, req)
	return h.reply
}

func joinProtocolServer(t *testing.T, h *joinProtocolHandler, credential []byte) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	remote, err := metatransport.New(metatransport.Options{
		Header: &metapb.Header{ClusterId: "cluster", NodeName: "seed"}, Listener: l,
		AllowPlaintext: true, Credential: credential, Handler: h,
		Resolve:     func(context.Context, uint64) (string, error) { return "", fault.ErrUnavailable },
		SendTimeout: time.Second, SnapshotTimeout: time.Second,
		RetryInitial: time.Millisecond, RetryMax: time.Second, MaxSnapshotBytes: 1 << 20,
	})
	if err != nil {
		l.Close()
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
	return l.Addr().String()
}

func joinProtocolClient(t *testing.T, seeds []string) *Group {
	t.Helper()
	o := optionsForTest(t)
	o.Identity.MemberID = 0
	o.Founders = nil
	o.Join = true
	o.JoinSeeds = seeds
	o.Node = topology.Node{Name: o.Identity.NodeName, Type: "worker"}
	o.DBPath = filepath.Join(t.TempDir(), "unopened.bolt")
	g, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.transport.Start(t.Context()); err != nil {
		g.Stop(context.Background())
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := g.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	return g
}

func TestJoinClientRefusalClassificationAndBoundedRedirects(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		reply      *metapb.Reply
		credential string
		want       error
		fatal      bool
		redirect   bool
	}{
		{name: "not ready remains retryable", reply: metatransport.Reply(fault.ErrUnavailable), want: fault.ErrUnavailable},
		{name: "unapproved remains parked", reply: metatransport.Reply(fault.ErrOperatorAuthorization), want: fault.ErrOperatorAuthorization},
		{name: "identity refusal is definitive", reply: metatransport.Reply(fault.New(fault.IdentityMismatch, "arbitrary prose", nil)), want: fault.ErrIdentityMismatch, fatal: true},
		{name: "wrong credential is definitive", reply: &metapb.Reply{}, credential: "foreign", want: fault.ErrIdentityMismatch, fatal: true},
		{name: "malformed success is definitive", reply: &metapb.Reply{Data: []byte("{")}, want: fault.ErrUnreadable, fatal: true},
		{name: "empty grant cannot open storage", reply: &metapb.Reply{}, want: fault.ErrUnreadable, fatal: true},
		{name: "null grant cannot open storage", reply: &metapb.Reply{Data: []byte("null")}, want: fault.ErrIdentityMismatch, fatal: true},
		{name: "redirect exhaustion remains retryable", reply: metatransport.Reply(fault.ErrNotLeader), want: fault.ErrNotLeader, redirect: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			credential := []byte("test-secret")
			if tc.credential != "" {
				credential = []byte(tc.credential)
			}
			h := &joinProtocolHandler{reply: tc.reply}
			seed := joinProtocolServer(t, h, credential)
			if tc.redirect {
				h.reply.LeaderAddress = seed
			}
			g := joinProtocolClient(t, []string{seed, seed})
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			err := g.tryJoin(ctx)
			var fatal *fatalAdmissionError
			if !errors.Is(err, tc.want) || errors.As(err, &fatal) != tc.fatal {
				t.Fatalf("admission result: %v, expected %v, fatal=%v", err, tc.want, tc.fatal)
			}
			h.mu.Lock()
			requests := append([]joinRequest(nil), h.requests...)
			h.mu.Unlock()
			wantCalls := 1
			if tc.credential != "" {
				wantCalls = 0
			}
			if len(requests) != wantCalls {
				t.Fatal("duplicate seeds/redirect retried the same destination", requests)
			}
			if len(requests) > 0 && (requests[0].Identity != g.Identity() || requests[0].Node.Type != "worker" || requests[0].Address != g.opts.Network.AdvertiseAddress || requests[0].LastDocVersion != 0) {
				t.Fatal("admission request lost caller identity", requests)
			}
			if g.engine.Load() != nil || g.Identity().MemberID != 0 || g.joinDone || g.joinReady || len(g.joinMembers) != 0 {
				t.Fatal("refusal mutated local admission")
			}
			if _, err := os.Stat(g.opts.DBPath); !os.IsNotExist(err) {
				t.Fatal("refused admission created storage", err)
			}
		})
	}
}

func TestJoinClientSkipsColdSeedsAndFollowsLeaderHintToExactGrant(t *testing.T) {
	t.Parallel()
	accepted := &joinProtocolHandler{}
	leader := joinProtocolServer(t, accepted, []byte("test-secret"))
	redirect := metatransport.Reply(fault.ErrNotLeader)
	redirect.LeaderAddress = leader
	follower := &joinProtocolHandler{reply: redirect}
	followerAddress := joinProtocolServer(t, follower, []byte("test-secret"))
	cold := &joinProtocolHandler{reply: metatransport.Reply(fault.ErrUnavailable)}
	coldAddress := joinProtocolServer(t, cold, []byte("test-secret"))
	g := joinProtocolClient(t, []string{coldAddress, followerAddress, coldAddress})
	g.ctx = t.Context() // Start only the granted joining engine, without the retry worker.
	doc := topology.Document{
		Version: 1, ClusterID: "cluster", Origin: topology.OriginOperator,
		Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3},
		Nodes:     []topology.Node{{Name: "n1", Type: "worker", State: topology.NodeActive}},
		Groups:    []topology.Group{{ID: topology.MetaGroupID, NextReplicaID: 2, Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter}}}},
	}
	grant := joinResponse{
		Rules: g.rules, MemberID: 1, Granted: doc, LeaderAddress: leader,
		Members:   []joinMember{{Name: "n1", MemberID: 1, Address: g.opts.Network.AdvertiseAddress}},
		Admission: metaengine.AdmissionRecord{NodeName: "n1", InstanceID: g.Identity().InstanceID, MemberID: 1, OperatorID: "exec-v1-a1", State: metaengine.AdmissionPending},
	}
	data, err := json.Marshal(grant)
	if err != nil {
		t.Fatal(err)
	}
	accepted.reply = &metapb.Reply{Data: data}
	// A seed with no service must not mask the later established leader.
	unreachable, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := unreachable.Addr().String()
	unreachable.Close()
	g.opts.JoinSeeds = append([]string{address}, g.opts.JoinSeeds...)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := g.tryJoin(ctx); err != nil {
		t.Fatal(err)
	}
	if !g.joinDone || g.joinReady || g.Identity().MemberID != 1 || g.joinLeader != leader || g.joinAdmission != grant.Admission || g.engine.Load() == nil || !g.engine.Load().Running() {
		t.Fatal("exact pending grant did not prepare the joining engine", g.Identity(), g.joinAdmission)
	}
	for _, h := range []*joinProtocolHandler{cold, follower, accepted} {
		h.mu.Lock()
		requests := append([]joinRequest(nil), h.requests...)
		h.mu.Unlock()
		if len(requests) != 1 || requests[0].Identity.MemberID != 0 || requests[0].Identity.InstanceID != g.Identity().InstanceID {
			t.Fatal("seed walk repeated a destination or changed lineage", requests)
		}
	}
	if len(g.LocalStatus().ConfState.Voters) != 0 || len(g.LocalStatus().ConfState.Learners) != 0 {
		t.Fatal("admission grant invented membership before catch-up", g.LocalStatus())
	}
	if err := g.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	pin, found, err := LoadStoredIdentity(g.opts.DBPath)
	if err != nil || !found || pin.MemberID != 1 || pin.NodeName != "n1" || pin.ClusterID != "cluster" {
		t.Fatal("grant was not durably pinned", pin, found, err)
	}
}
