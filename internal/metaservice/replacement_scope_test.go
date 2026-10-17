//go:build !functional

package metaservice

import (
	"errors"
	"strings"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func transferScopeFixture() metaengine.PlanRead {
	group := topology.Group{ID: "work", Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter}, {NodeName: "n2", ReplicaID: 2, Role: topology.ReplicaVoter}}}
	meta := topology.Group{ID: topology.MetaGroupID, Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 11, Role: topology.ReplicaVoter}, {NodeName: "n2", ReplicaID: 12, Role: topology.ReplicaVoter}}}
	doc := topology.Document{Version: 9, Nodes: []topology.Node{{Name: "n1", State: topology.NodeActive}, {Name: "n2", State: topology.NodeActive}}, Groups: []topology.Group{meta, group}}
	input := topology.PlanInput{Approved: doc.Clone(), Groups: map[string]topology.GroupProgress{
		"work":               {GroupID: "work", ConvergedVersion: 3, Converged: &group, Observation: &topology.GroupStatus{GroupID: "work", Found: true, LeaderID: 1}},
		topology.MetaGroupID: {GroupID: topology.MetaGroupID, ConvergedVersion: 5, Converged: &meta},
		"sibling":            {GroupID: "sibling", OperationID: "exec-v8-a1"},
	}, Nodes: map[string]topology.NodeProgress{"n1": {NodeName: "n1", MemberID: 11, ConvergedVersion: 3, Converged: doc.Node("n1")}},
		Native: map[string]topology.NativeState{"work": {GroupID: "work", ReplicaID: 1, Applied: 20, ConfState: raftpb.ConfState{Voters: []uint64{1, 2}}}}}
	return metaengine.PlanRead{Input: input, Target: doc.Clone(), Scopes: []metaengine.ScopeExpectation{{GroupID: "work", SourceVersion: 3}}}
}

func TestReplacementScopesUseExactProofAndNameBusyOwner(t *testing.T) {
	t.Parallel()
	read := transferScopeFixture()
	key := topology.ReplicaKey{NodeName: "n1", ReplicaID: 1}
	if err := checkReseatScope(read.Input, "work", key); err != nil {
		t.Fatal(err)
	}
	if err := metaengine.ValidateReadmissionScope(read.Input, "n1", 11); err != nil {
		t.Fatal(err)
	}
	key.ReplicaID = 7
	if err := checkReseatScope(read.Input, "work", key); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal(err)
	}
	if err := metaengine.ValidateReadmissionScope(read.Input, "n1", 7); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal(err)
	}
	row := read.Input.Groups["work"]
	row.OperationID = "exec-v10-a2"
	read.Input.Groups["work"] = row
	if _, err := unownedProvenGroup(read.Input, "work"); !errors.Is(err, fault.ErrUnavailable) || !strings.Contains(err.Error(), string(row.OperationID)) {
		t.Fatal(err)
	}
	if err := metaengine.ValidateReadmissionScope(read.Input, "n1", 11); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal(err)
	}
	row.OperationID, row.ConvergedVersion = "", 0
	read.Input.Groups["work"] = row
	if _, err := unownedProvenGroup(read.Input, "work"); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal(err)
	}
}

func TestDirectTransferUsesGroupSourceNativeMembershipAndGrantBinding(t *testing.T) {
	t.Parallel()
	read := transferScopeFixture()
	request := topology.AuthorizedStep{Kind: topology.StepTransferLeader, GroupID: "work", NodeName: "n2"}
	action, err := transferAction(read, request)
	if err != nil || action.Version != 3 || action.ReplicaID != 2 {
		t.Fatal(action, err)
	}
	if err := validateTransferNative(read, action); err != nil {
		t.Fatal(err)
	}
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{GroupID: "work"}, Action: &action,
		Targets: []topology.AuthorityTarget{{NodeName: "n1", ReplicaID: 1, MemberID: 11}, {NodeName: "n2", ReplicaID: 2, MemberID: 12}}}
	// A lagging node row and a newer metadata member must not rewrite the grant.
	read.Input.Nodes["n1"] = topology.NodeProgress{MemberID: 4}
	read.Target.Group(topology.MetaGroupID).Replicas[0].ReplicaID = 40
	target, err := transferRecipient(read, grant)
	if err != nil || target != grant.Targets[0] {
		t.Fatal(target, err)
	}
	read.Input.Groups["work"].Observation.LeaderID = 2
	target, err = transferRecipient(read, grant)
	if err != nil || target != grant.Targets[1] {
		t.Fatal(target, err)
	}
	native := read.Input.Native["work"]
	native.ConfState.Voters = []uint64{1}
	read.Input.Native["work"] = native
	if err := validateTransferNative(read, action); !errors.Is(err, fault.ErrUnsafeMembership) {
		t.Fatal(err)
	}
	action.Version++
	if _, err := transferAction(read, action); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal(err)
	}
}

func TestTransferCaptureIgnoresUnrelatedVersionButRechecksAffectedBindings(t *testing.T) {
	t.Parallel()
	before, after := transferScopeFixture(), transferScopeFixture()
	after.Target.Version++
	after.Input.Approved.Version++
	if err := comparePlanCapture(before, after); err != nil {
		t.Fatal(err)
	}
	after.Target.Group("work").Replicas[0].ReplicaID++
	if err := comparePlanCapture(before, after); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal(err)
	}
}
