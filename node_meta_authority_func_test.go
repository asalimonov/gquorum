//go:build functional

package gquorum

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// The private callback tests supply verified-grant protocol inputs. The service
// suite separately exercises authenticated quorum proof before invoking them.
func metaHostGrant(id topology.Identity, revision uint64, kind topology.StepKind) topology.AuthorityGrant {
	action := topology.AuthorizedStep{OperatorID: topology.MakeOperatorID(1, 1), Version: 1, Attempt: 1, StepSeq: 1, Kind: kind, GroupID: topology.MetaGroupID, NodeName: id.NodeName, ReplicaID: id.MemberID}
	return topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: id.ClusterID, GroupID: topology.MetaGroupID, Revision: revision}, Purpose: topology.AuthorityEffect, OperatorID: action.OperatorID, Action: &action, Resources: []topology.AuthorityResource{{Kind: topology.AuthorityHost, GroupID: topology.MetaGroupID, NodeName: id.NodeName}}, Targets: []topology.AuthorityTarget{{NodeName: id.NodeName, MemberID: id.MemberID, ReplicaID: id.MemberID}}}
}
func metaHostRequest(id topology.Identity, grant topology.AuthorityGrant) load.ActionRequest {
	return load.ActionRequest{From: id, To: load.Peer{Node: id, Replica: load.ReplicaRef{GroupID: topology.MetaGroupID, ReplicaID: id.MemberID}}, Grant: grant, Authorization: *grant.Action, LocalTarget: storage.Identity{ClusterID: id.ClusterID, NodeName: id.NodeName, GroupID: topology.MetaGroupID, ReplicaID: id.MemberID}}
}
func TestNodeMetaHostPrepareStopAndRetainedReceipt(t *testing.T) {
	t.Parallel()
	options := authorityNodeOptions(t, t.TempDir(), "process")
	public, err := NewNode(options)
	if err != nil {
		t.Fatal(err)
	}
	n := public.(*node)
	t.Cleanup(func() { stopAuthorityNode(t, n) })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err = n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = n.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	id := n.Identity()
	prepare := metaHostGrant(id, 100, topology.StepPrepareReplica)
	if _, err = n.applyMetaHostAction(ctx, metaHostRequest(id, prepare)); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal("effect ran before host floor", err)
	}
	install := func(grant topology.AuthorityGrant) topology.FenceEvidence {
		t.Helper()
		result, err := n.installAuthorityFence(ctx, load.FenceRequest{From: id, To: id, Grant: grant, Resource: grant.Resources[0]})
		if err != nil || result.Evidence.Validate(grant) != nil {
			t.Fatal(result, err)
		}
		return result.Evidence
	}
	proof := install(prepare)
	result, err := n.applyMetaHostAction(ctx, metaHostRequest(id, prepare))
	if err != nil || len(result.Receipts) != 1 || result.Receipts[0].Validate(prepare) != nil || result.Receipts[0].LocalSequence != proof.LocalSequence {
		t.Fatal(result, err)
	}
	again, err := n.applyMetaHostAction(ctx, metaHostRequest(id, prepare))
	if err != nil || !reflect.DeepEqual(again, result) {
		t.Fatal("prepare receipt was not idempotent", again, err)
	}
	if err = n.meta.VerifyPreparedMeta(ctx, id); err != nil {
		t.Fatal("prepare stopped participation", err)
	}
	stopped := metaHostGrant(id, 101, topology.StepStopReplica)
	stopProof := install(stopped)
	if stopProof.LocalSequence <= proof.LocalSequence {
		t.Fatal("host floor did not advance")
	}
	if _, err = n.applyMetaHostAction(ctx, metaHostRequest(id, prepare)); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal("completed fence admitted older prepare", err)
	}
	result, err = n.applyMetaHostAction(ctx, metaHostRequest(id, stopped))
	if err != nil || len(result.Receipts) != 1 || result.Receipts[0].Validate(stopped) != nil {
		t.Fatal(result, err)
	}
	if n.meta.IsLeader() {
		t.Fatal("stopped metadata advertised leadership")
	}
	if err = n.meta.VerifyPreparedMeta(ctx, id); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("stop receipt preceded participation fencing", err)
	}
	if _, err = n.meta.ReadTopology(ctx); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("stopped participant served ordinary reads", err)
	}
	if err = n.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	store, err := authoritystore.Open(options.authorityDBPath(), id, false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	receipt, found, err := store.Receipt(ctx, stopped, stopped.Targets[0])
	if err != nil || !found || !reflect.DeepEqual(receipt, result.Receipts[0]) {
		t.Fatal("stop receipt did not survive owner teardown", receipt, found, err)
	}
	if _, err = n.applyMetaHostAction(ctx, metaHostRequest(id, stopped)); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("stopped node reopened authority callback", err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewNode(authorityNodeOptions(t, filepath.Dir(options.authorityDBPath()), "replacement-process"))
	if err != nil {
		t.Fatal(err)
	}
	defer stopAuthorityNode(t, restarted)
	if err = restarted.Start(t.Context()); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal("process restart bypassed durable meta stop", err)
	}
	stopAuthorityNode(t, restarted)
	joiningOptions := authorityNodeOptions(t, filepath.Dir(options.authorityDBPath()), "readmission-process")
	joiningOptions.Node = topology.Node{Name: "n1", Type: "worker"}
	joiningOptions.Bootstrap = BootstrapOptions{Join: true, JoinSeeds: []string{options.Meta.Network.AdvertiseAddress}}
	joining, err := NewNode(joiningOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer stopAuthorityNode(t, joining)
	if err = joining.Start(t.Context()); err != nil {
		t.Fatal("explicit readmission could not retain its control owner", err)
	}
	local := joining.(*node)
	if _, err = local.meta.ReadTopology(ctx); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("readmission restarted stopped metadata participation", err)
	}
	if local.meta.IsLeader() || joining.Identity().MemberID != id.MemberID {
		t.Fatal("control startup changed former identity", joining.Identity())
	}
}
