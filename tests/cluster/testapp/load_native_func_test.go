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
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/metadata"
)

type nativeProtocolNode struct {
	gquorum.Node
	id       topology.Identity
	requests []load.ReplicaQuery
}

func (n *nativeProtocolNode) Identity() topology.Identity { return n.id }
func (n *nativeProtocolNode) ReadLoadReplica(_ context.Context, query load.ReplicaQuery) (load.ReplicaStatus, error) {
	n.requests = append(n.requests, query)
	result := load.ReplicaStatus{Peer: query.To, Found: true, LocalState: "ready"}
	if query.Native {
		result.Native = &topology.NativeState{GroupID: query.To.Replica.GroupID, ReplicaID: query.To.Replica.ReplicaID, Applied: 9007199254740993, ConfState: raftpb.ConfState{Voters: []uint64{7}, Learners: []uint64{8}}}
	}
	return result, nil
}

func TestLoadNativeProtocolPreservesAuthenticatedScopeAndAppliedEvidence(t *testing.T) {
	t.Parallel()
	id := topology.Identity{ClusterID: "cluster", NodeName: "one", MemberID: 1, InstanceID: "process"}
	node := &nativeProtocolNode{id: id}
	a := &application{node: node, workCtx: t.Context(), closing: make(chan struct{}), cfg: config{NodeName: id.NodeName, Credential: "credential"}}
	want := load.ReplicaQuery{From: id, To: load.Peer{Node: id, Replica: load.ReplicaRef{GroupID: "work", ReplicaID: 7}}, Native: true}
	data, _ := json.Marshal(want)
	request := &loadpb.Request{Header: a.header(), Method: "ReadReplica", GroupId: "work", ReplicaId: 7, Data: data}
	if reply, err := a.Call(t.Context(), request); err != nil || !errors.Is(replyError(reply), fault.ErrIdentityMismatch) || len(node.requests) != 0 {
		t.Fatal("unauthenticated native read reached Node", reply, err)
	}
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(credentialHeader, a.cfg.Credential))
	request.Header.InstanceId = "stale"
	if reply, err := a.Call(ctx, request); err != nil || !errors.Is(replyError(reply), fault.ErrIdentityMismatch) || len(node.requests) != 0 {
		t.Fatal("wrong sender reached Node", reply, err)
	}
	request.Header.InstanceId = id.InstanceID
	request.ReplicaId = 8
	if reply, err := a.Call(ctx, request); err != nil || !errors.Is(replyError(reply), fault.ErrIdentityMismatch) || len(node.requests) != 0 {
		t.Fatal("wrong replica route reached Node", reply, err)
	}
	transport := loadTransport{app: a}
	result, err := transport.ReadReplica(t.Context(), want)
	if err != nil || len(node.requests) != 1 || !reflect.DeepEqual(node.requests[0], want) || result.Peer != want.To || !result.Found || result.Native == nil || result.Native.Applied != 9007199254740993 || !reflect.DeepEqual(result.Native.ConfState, raftpb.ConfState{Voters: []uint64{7}, Learners: []uint64{8}}) {
		t.Fatal("transport lost native request or applied evidence", result, err, node.requests)
	}
	want.Native = false
	result, err = transport.ReadReplica(t.Context(), want)
	if err != nil || result.Native != nil || len(node.requests) != 2 || node.requests[1].Native {
		t.Fatal("diagnostic read was promoted into native proof", result, err, node.requests)
	}
}
