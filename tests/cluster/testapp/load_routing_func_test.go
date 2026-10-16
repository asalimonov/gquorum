//go:build functional

package main

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type retainedRouteCluster struct {
	gquorum.Cluster
	documents  []topology.Document
	approved   map[topology.OperationID]topology.OperationStatus
	historyErr error
}

func (*retainedRouteCluster) Target(context.Context) (topology.Document, error) {
	return topology.Document{}, nil
}
func (*retainedRouteCluster) Active(context.Context) (topology.Document, error) {
	return topology.Document{}, nil
}
func (c *retainedRouteCluster) History(context.Context, topology.Page) (topology.HistoryResult, error) {
	return topology.HistoryResult{Documents: c.documents}, c.historyErr
}
func (c *retainedRouteCluster) Operation(_ context.Context, id topology.OperationID) (topology.OperationStatus, error) {
	if op, ok := c.approved[id]; ok {
		return op, nil
	}
	return topology.OperationStatus{}, fault.ErrHistoryUnavailable
}

func TestLoadControlRoutingRetainsExactApprovedEndpointWithoutGrantingAuthority(t *testing.T) {
	t.Parallel()
	doc := func(version, member uint64, address string) topology.Document {
		return topology.Document{Version: version,
			Nodes:  []topology.Node{{Name: "n4", Labels: map[string]string{loadAddressLabel: address}}},
			Groups: []topology.Group{{ID: topology.MetaGroupID, Replicas: []topology.Replica{{NodeName: "n4", ReplicaID: member}}}},
		}
	}
	cluster := &retainedRouteCluster{
		documents: []topology.Document{doc(3, 4, "staged.invalid:9001"), doc(2, 4, "127.0.0.1:19001"), doc(1, 99, "foreign.invalid:9001")},
		approved:  map[topology.OperationID]topology.OperationStatus{"exec-v2-a1": {ID: "exec-v2-a1", Version: 2}},
	}
	a := &application{cluster: cluster, clients: map[string]*loadClient{}}
	client, err := a.client(t.Context(), "n4", 4)
	if err != nil || client.address != "127.0.0.1:19001" || client.member != 4 {
		t.Fatal("lost historical endpoint or used staged/foreign routing", client, err)
	}
	t.Cleanup(func() { _ = client.conn.Close() })
	cluster.historyErr = fault.ErrUnavailable
	if cached, err := a.client(t.Context(), "n4", 4); err != nil || cached != client {
		t.Fatal("exact owned endpoint lost after history disappeared", cached, err)
	}
	if _, err := a.client(t.Context(), "n4", 5); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("former endpoint was rebound to a different member", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := a.client(ctx, "n4", 4); !errors.Is(err, context.Canceled) {
		t.Fatal("cache bypassed request cancellation", err)
	}
}
