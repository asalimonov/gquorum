//go:build !functional

package main

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/tests/cluster/testapp/loadpb"
	"github.com/asalimonov/gquorum/topology"
)

type routingCluster struct {
	gquorum.Cluster
	target    topology.Document
	targetErr error
	history   []topology.Document
	approved  map[topology.OperationID]bool
}

func (c routingCluster) Target(context.Context) (topology.Document, error) {
	return c.target.Clone(), c.targetErr
}

func (c routingCluster) History(context.Context, topology.Page) (topology.HistoryResult, error) {
	return topology.HistoryResult{Documents: c.history}, nil
}

func (c routingCluster) Operation(_ context.Context, id topology.OperationID) (topology.OperationStatus, error) {
	if c.approved[id] {
		for _, doc := range c.history {
			if topology.MakeOperationID(doc.Version, 1) == id {
				return topology.OperationStatus{ID: id, Version: doc.Version}, nil
			}
		}
	}
	return topology.OperationStatus{}, fault.ErrHistoryUnavailable
}

func (routingCluster) ReadTopology(context.Context) (topology.ReadView, error) {
	return topology.ReadView{}, fault.ErrUnavailable
}

func TestSeedResolutionSurvivesUnavailableLocalMetadata(t *testing.T) {
	t.Parallel()
	a := &application{cfg: config{Peers: []peer{{Name: "n2", MetaAddress: "seed:9000"}}},
		cluster: routingCluster{targetErr: fault.ErrUnavailable}}
	if got, err := a.address(t.Context(), "n2", metaAddressLabel); err != nil || got != "seed:9000" {
		t.Fatal("seed lost while local metadata unavailable", got, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := a.address(ctx, "n2", metaAddressLabel); !errors.Is(err, context.Canceled) {
		t.Fatal("seed fallback bypassed cancellation", err)
	}
}

func TestLocalRoutesRejectStagedAndReplacedIdentitiesWithoutQuorum(t *testing.T) {
	t.Parallel()
	doc := topology.Document{Version: 2, Nodes: []topology.Node{{Name: "n2"}}, Groups: []topology.Group{
		{ID: topology.MetaGroupID, Replicas: []topology.Replica{{NodeName: "n2", ReplicaID: 2}}},
		{ID: "work", Replicas: []topology.Replica{{NodeName: "n2", ReplicaID: 7}}},
	}}
	staged := doc.Clone()
	staged.Version = 4
	staged.Groups[0].Replicas[0].ReplicaID = 99
	staged.Groups[1].Replicas[0].NodeName = "staged"
	c := routingCluster{target: topology.Document{Version: 3}, history: []topology.Document{staged, doc},
		approved: map[topology.OperationID]bool{"exec-v2-a1": true}}
	a := &application{cfg: config{ClusterID: "cluster"}, cluster: c}
	if owner, err := a.owner(t.Context(), "work", 7); err != nil || owner != "n2" {
		t.Fatal("lost retained owner or used staged owner", owner, err)
	}
	header := &loadpb.Header{ClusterId: "cluster", NodeName: "n2", InstanceId: "process", MemberId: 2}
	if err := a.validateHeader(t.Context(), header); err != nil {
		t.Fatal("retained approved sender required quorum", err)
	}
	c.target = staged
	a.cluster = c
	if err := a.validateHeader(t.Context(), header); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("approved replacement admitted stale sender", err)
	}
}

func TestAddressResolutionDoesNotRequireMetadataQuorum(t *testing.T) {
	t.Parallel()
	for _, founded := range []bool{false, true} {
		a := &application{cfg: config{Peers: []peer{{Name: "n2", MetaAddress: "seed:9000"}}}}
		cluster := routingCluster{}
		want := "seed:9000"
		if founded {
			want = "approved:9000"
			cluster.target = topology.Document{Version: 2, Nodes: []topology.Node{
				{Name: "n2", Labels: map[string]string{metaAddressLabel: want}},
			}}
		}
		a.cluster = cluster
		got, err := (metaResolver{a}).Resolve(t.Context(), "n2")
		if err != nil || got != want {
			t.Fatalf("founded=%v: route=%q, want=%q, err=%v", founded, got, want, err)
		}
	}
}

func TestAddressFreeLoadRoutingBindsApprovedMemberBeforeSeed(t *testing.T) {
	t.Parallel()
	c := routingCluster{target: topology.Document{Version: 1, Nodes: []topology.Node{{Name: "n2"}},
		Groups: []topology.Group{{ID: topology.MetaGroupID,
			Replicas: []topology.Replica{{NodeName: "n2", ReplicaID: 2}}}}}}
	a := &application{cluster: c, cfg: config{Peers: []peer{{Name: "n2", LoadAddress: "seed:9001"}}}}
	if address, err := a.loadAddress(t.Context(), "n2", 2); err != nil || address != "seed:9001" {
		t.Fatal("approved address-free member lost seed route", address, err)
	}
	if _, err := a.loadAddress(t.Context(), "n2", 3); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("seed bypassed exact member binding", err)
	}
}
