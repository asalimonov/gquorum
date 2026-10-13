//go:build functional

package metaservice

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/topology"
)

func processOnlyClient(t *testing.T, owner *Group, id topology.Identity) *Group {
	t.Helper()
	client := joinProtocolClient(t, []string{owner.opts.Network.AdvertiseAddress}, func(o *Options) {
		o.Identity = id
		o.Node = topology.Node{Name: id.NodeName, Type: "worker"}
		o.Network.ClusterCredential = owner.opts.Network.ClusterCredential
	})
	client.mu.Lock()
	client.started = true
	client.ctx, client.cancel = context.WithCancel(t.Context())
	client.workCtx, client.workCancel = context.WithCancel(client.ctx)
	client.mu.Unlock()
	return client
}

func TestAdmissionProbeSurvivesMetadataReplacementButNotProcessStop(t *testing.T) {
	t.Parallel()
	g := readyPublic(t, singleMetaFixture(t))
	id := topology.Identity{ClusterID: g.Identity().ClusterID, NodeName: "n2", InstanceID: "pending"}
	client := processOnlyClient(t, g, id)
	g.primeAddress(id.NodeName, client.opts.Network.AdvertiseAddress)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	a := metaengine.AdmissionRecord{NodeName: id.NodeName, InstanceID: id.InstanceID, MemberID: 3}
	for _, member := range []uint64{0, 2, 3} {
		client.mu.Lock()
		client.identity.MemberID = member
		client.mu.Unlock()
		if !g.probeAdmission(ctx, a) {
			t.Fatal("lost exact process during member transition", member)
		}
		wrong := a
		wrong.InstanceID = "copied"
		if g.probeAdmission(ctx, wrong) {
			t.Fatal("different process claimed admission")
		}
		if err := metatransport.ReplyError(client.Call(ctx, "IdentityPing", g.transport.Header(), nil)); !errors.Is(err, fault.ErrUnavailable) {
			t.Fatal("process probe implied participation", err)
		}
	}
	if err := client.FenceParticipation(ctx); err != nil {
		t.Fatal(err)
	}
	if !g.probeAdmission(ctx, a) {
		t.Fatal("participation fence released process ownership")
	}
	for _, data := range []string{
		`{"process":{"node_name":"n2","instance_id":"pending"},"grant":{}}`,
		`{"process":{"node_name":"n2","instance_id":"pending","extra":true}}`,
		`{"process":{"node_name":"n2","instance_id":""}}`,
	} {
		if err := metatransport.ReplyError(client.Call(ctx, "IdentityPing", g.transport.Header(), []byte(data))); err == nil {
			t.Fatal("accepted invalid process scope", data)
		}
	}
	data, _ := json.Marshal(authorityIdentityRequest{Process: &processIdentityRequest{NodeName: id.NodeName, InstanceID: id.InstanceID}})
	foreign := g.transport.Header()
	foreign.ClusterId = "foreign"
	if err := metatransport.ReplyError(client.Call(ctx, "IdentityPing", foreign, data)); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	if err := client.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if g.probeAdmission(ctx, a) {
		t.Fatal("stopped process retained admission")
	}
}
