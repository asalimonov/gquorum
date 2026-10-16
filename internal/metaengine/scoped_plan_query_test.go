//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestScopedPlanningCaptureApprovesWithoutSiblingConvergence(t *testing.T) {
	t.Parallel()
	s, proposal := approvalFixture(t, topology.MetaGroupID)
	query := QueryRequest{Kind: "approval_plan", Version: proposal.Version}
	read, err := queryScopedPlan(&s, query)
	if err != nil || !reflect.DeepEqual(read.Scopes, proposal.Scopes) || read.Input.Groups["shard0"].OperationID == "" {
		t.Fatal(read, err)
	}
	query.Native = map[string]topology.NativeState{topology.MetaGroupID: {GroupID: topology.MetaGroupID, ReplicaID: 1, Applied: 10, ConfState: raftpb.ConfState{Voters: []uint64{1}}}}
	read, err = queryScopedPlan(&s, query)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := topology.BuildPlanPreferred(read.Input, read.Target, topology.PlanOptions{Rules: s.Rules})
	if err != nil || len(plan.Blocks) != 1 || plan.Blocks[0].GroupID != topology.MetaGroupID {
		t.Fatal(plan, err)
	}
	proposal.Plan, proposal.Scopes = plan, read.Scopes
	accepted, err := s.applyBeginOperation(s.AppliedIndex+1, proposal)
	if err != nil || !accepted.changed {
		t.Fatal(accepted, err)
	}
	before, _ := json.Marshal(s)
	repeated, err := queryScopedPlan(&s, query)
	after, _ := json.Marshal(s)
	if err != nil || repeated.Accepted != topology.OperationID(accepted.result.OperatorID) || len(repeated.Scopes) != 0 || !bytes.Equal(before, after) {
		t.Fatal("lost approval reply was replanned", repeated, err)
	}
}

func TestScopedRetryReadKeepsOriginalPlanEvidenceAndSuccessor(t *testing.T) {
	t.Parallel()
	s := failedScopedAttempt(t)
	old := s.Operations[0].Clone()
	query := QueryRequest{Kind: "retry_plan", OperationID: old.Status.ID}
	read, err := queryScopedPlan(&s, query)
	if err != nil || len(read.Scopes) != 1 || read.Scopes[0].GroupID != "shard0" || !read.Operation.Plan.Equal(old.Plan) {
		t.Fatal(read, err)
	}
	issued := applyScopedRetryFixture(t, &s, scopedRetryProposal(&s, &old))
	read, err = queryScopedPlan(&s, query)
	if err != nil || read.Accepted != topology.OperationID(issued.result.OperatorID) {
		t.Fatal("retry replay lost immediate edge", read, err)
	}
	next := s.indexes.operations[read.Accepted]
	read, err = queryScopedPlan(&s, QueryRequest{Kind: "operation_plan", OperationID: next.Status.ID, Key: "shard0"})
	if err != nil || len(read.Previous) != 1 || read.Previous[0].Status.ID != old.Status.ID {
		t.Fatal("ancestry missing", read, err)
	}
	for _, grant := range read.Grants {
		if grant.OperatorID != topology.OperatorID(old.Status.ID) {
			t.Fatal("original grant relabelled", grant)
		}
	}
	read.Previous[0].Plan.Steps[0].NodeName = "caller"
	if s.Operations[0].Plan.Steps[0].NodeName == "caller" {
		t.Fatal("borrowed operation plan")
	}
}

func TestScopedPlanningQueryRefusesMixedFieldsAndUncapturedNative(t *testing.T) {
	t.Parallel()
	s, p := approvalFixture(t, "shard0")
	for _, q := range []QueryRequest{
		{Kind: "planning"}, {Kind: "approval_plan"}, {Kind: "retry_plan", OperationID: "op", Key: "work"},
		{Kind: "operation_plan", OperationID: "op"}, {Kind: "authority", OperationID: "op", Version: 1},
		{Kind: "approval_plan", Version: p.Version, Progress: &topology.ProgressQuery{}},
		{Kind: "approval_plan", Version: p.Version, Native: map[string]topology.NativeState{topology.MetaGroupID: {GroupID: topology.MetaGroupID, ReplicaID: 1, Applied: 1}}},
	} {
		if _, err := queryScopedPlan(&s, q); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal(q, err)
		}
	}
	body, _ := json.Marshal(QueryRequest{Kind: "approval_plan", Version: p.Version})
	if _, err := NewMachine(testRules(), nil).Query(t.Context(), body); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal("schema3 accepted scoped planning", err)
	}
}
