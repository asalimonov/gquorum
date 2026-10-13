//go:build functional

package main

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/tests/cluster/testapp/loadpb"
	"github.com/asalimonov/gquorum/topology"
	"google.golang.org/grpc/metadata"
)

type fenceProtocolNode struct {
	gquorum.Node
	requests []load.FenceRequest
}

func (n *fenceProtocolNode) InstallLoadFence(_ context.Context, request load.FenceRequest) (load.FenceResult, error) {
	n.requests = append(n.requests, request)
	return load.FenceResult{From: request.To, Evidence: topology.FenceEvidence{Key: request.Grant.Key, Resource: request.Resource, Receiver: request.To, LocalSequence: 2}}, nil
}

func TestLoadFenceProtocolAuthenticatesAndPreservesExactRequest(t *testing.T) {
	t.Parallel()
	node := &fenceProtocolNode{}
	a := &application{node: node, workCtx: t.Context(), closing: make(chan struct{}), cfg: config{Credential: "credential"}}
	id := topology.Identity{ClusterID: "cluster", NodeName: "n1", MemberID: 1, InstanceID: "process"}
	resource := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: "work", NodeName: id.NodeName}
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: id.ClusterID, GroupID: "work", Revision: 9007199254740993}, Purpose: topology.AuthorityFence, Resources: []topology.AuthorityResource{resource}, Targets: []topology.AuthorityTarget{{NodeName: id.NodeName, MemberID: id.MemberID, ReplicaID: 7}}}
	want := load.FenceRequest{From: id, To: id, Resource: resource, Grant: grant}
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	request := &loadpb.Request{Header: &loadpb.Header{ClusterId: id.ClusterID, NodeName: id.NodeName, MemberId: id.MemberID, InstanceId: id.InstanceID}, Method: "Fence", GroupId: "work", Data: data}
	reply, err := a.Call(t.Context(), request)
	if err != nil || !errors.Is(replyError(reply), fault.ErrIdentityMismatch) || len(node.requests) != 0 {
		t.Fatal("unauthenticated fence reached Node", reply, err)
	}
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(credentialHeader, a.cfg.Credential))
	request.Header.InstanceId = "stale"
	reply, err = a.Call(ctx, request)
	if err != nil || !errors.Is(replyError(reply), fault.ErrIdentityMismatch) || len(node.requests) != 0 {
		t.Fatal("changed sender reached Node", reply, err)
	}
	request.Header.InstanceId = id.InstanceID
	request.ReplicaId = 7
	reply, err = a.Call(ctx, request)
	if err != nil || !errors.Is(replyError(reply), fault.ErrIdentityMismatch) || len(node.requests) != 0 {
		t.Fatal("host fence accepted a replica route", reply, err)
	}
	request.ReplicaId = 0
	reply, err = a.Call(ctx, request)
	if err != nil || replyError(reply) != nil || len(node.requests) != 1 || !reflect.DeepEqual(node.requests[0], want) {
		t.Fatal(reply, err, node.requests)
	}
	var result load.FenceResult
	if err := decodeJSON(reply.Data, &result); err != nil || result.Evidence.Validate(grant) != nil || result.From != id {
		t.Fatal(result, err)
	}
}
