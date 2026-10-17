//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestPrepareWaitsForSameHostRetirementBeforeTakingFloor(t *testing.T) {
	t.Parallel()
	s := pendingScopedRetirement(t)
	op := &s.Operations[0]
	op.Plan.Steps[0].Kind = topology.StepPrepareReplica
	op.Status.State, op.Status.Blocks[0].State = topology.OperationRunning, topology.OperationRunning
	s.Revocations = nil
	for i := range s.Groups {
		if s.Groups[i].GroupID == "a" {
			s.Groups[i].Owner = op.Status.ID
		}
	}
	prepareScopedFixture(t, &s)
	action := scopedPlanAction(op, op.Plan.Steps[0])
	debt := s.Cleanup[0].clone()
	before, _ := json.Marshal(s)
	if result, err := s.applyBeginEffect(s.AppliedIndex+1, BeginEffectProposal{Action: action}); !errors.Is(err, fault.ErrUnavailable) || result.changed {
		t.Fatal("prepare bypassed predecessor retirement", result, err)
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("pending preparation changed the host floor")
	}
	if blocked := s.requiredBlocker(s.indexes.operations[op.Status.ID], 0); blocked == nil ||
		blocked.Code != topology.BlockedDependency {
		t.Fatal("pending host retirement was not a scoped dependency", blocked)
	}
	retire := issueScopedCleanup(t, &s, BeginCleanupEffectProposal{Identity: debt.Identity, Action: *debt.RetireAction})
	recordScopedEffectFixture(t, &s, RecordEffectProposal{Receipt: fixtureEffectReceipts(*retire.result.Grant)[0],
		At: debt.UpdatedAt.Add(time.Second)})
	if s.indexes.cleanup[debt.Identity].Retired == nil || s.indexes.cleanup[debt.Identity].Collection != nil {
		t.Fatal("fixture must prove retirement without physical collection")
	}
	result, err := s.applyBeginEffect(s.AppliedIndex+1, BeginEffectProposal{Action: action})
	if err != nil || !result.changed || result.result.Grant == nil {
		t.Fatal("retired uncollected data blocked successor", result, err)
	}
	grant := result.result.Grant
	host := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: "a", NodeName: "n"}
	if s.authorityCatalog().floors[host] != grant.Key || grant.Targets[0].ReplicaID != 8 {
		t.Fatal("successor did not own the host floor", grant)
	}
	prepareScopedFixture(t, &s)
}

func TestSameHostPrepareBeforeStopIsCommittedRefusal(t *testing.T) {
	t.Parallel()
	doc := multiNodeDoc()
	group := doc.Group("shard0")
	group.Replicas = slices.Clone(doc.MetaGroup().Replicas)
	group.NextReplicaID = 6
	s := convergedScopedFixture(t, doc)
	s.Latest = s.Target.Clone()
	s.Latest.Version, s.Latest.ParentVersion = 2, 1
	s.Latest.Group("shard0").Replicas[2].ReplicaID = 6
	s.Latest.Group("shard0").NextReplicaID = 7
	for i := range s.Allocations {
		if s.Allocations[i].GroupID == "shard0" {
			s.Allocations[i].NextReplicaID = 7
		}
	}
	m := restoreMachineFixture(t, s)
	input := testPlanningInput(t, m, s.Latest, assumedReadmissionStatuses(s.Target, ""))
	plan, err := topology.BuildPlan(input, s.Latest, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(plan.Steps, func(step topology.Step) bool { return step.Kind == topology.StepPrepareReplica })
	steps := append([]topology.Step(nil), plan.Steps[i:i+2]...)
	steps = append(steps, plan.Steps[:i]...)
	steps = append(steps, plan.Steps[i+2:]...)
	plan.Steps, plan.Blocks[0].Steps, plan.Blocks[0].Cleanup = steps, nil, nil
	for i := range plan.Steps {
		step := &plan.Steps[i]
		step.Seq = i + 1
		if step.Kind == topology.StepAddLearner {
			step.ExpectedVotersDigest = topology.VoterSetDigest(plan.VoterBases[0].Voters)
			step.ResultVotersDigest = step.ExpectedVotersDigest
		}
		if step.Kind == topology.StepCleanupReplicaState {
			plan.Blocks[0].Cleanup = append(plan.Blocks[0].Cleanup, step.Seq)
		} else {
			plan.Blocks[0].Steps = append(plan.Blocks[0].Steps, step.Seq)
		}
	}
	result := applyMachine(t, m, BeginOperationProposal{Version: 2, Plan: plan,
		Scopes: []ScopeExpectation{{GroupID: "shard0", SourceVersion: 1}},
		Actor:  "admin", Origin: topology.OriginAPI, At: s.FullyConverged.At.Add(time.Second), ProposalID: "bad-order"})
	if result.Accepted || string(result.Code) != string(fault.UnsafeMembership) ||
		!strings.Contains(result.Reason, "lacks required stop") || m.Views().Target.Version != 1 {
		t.Fatal("prepare before predecessor stop was not a committed safety refusal", result)
	}
}

func TestReadmissionOfInactiveNodeIsCommittedRefusal(t *testing.T) {
	t.Parallel()
	doc := multiNodeDoc()
	doc.Node("node3").State = topology.NodeDraining
	s := convergedScopedFixture(t, doc)
	s.Activated = []string{"node3"}
	m := restoreMachineFixture(t, s)
	result := applyMachine(t, m, CommitReadmissionProposal{
		Admission:       AdmissionRecord{NodeName: "node3", InstanceID: "returning", MemberID: 6},
		ExpectedVersion: 1, ExpectedTarget: 1, At: s.FullyConverged.At.Add(time.Second), ProposalID: "inactive"})
	if result.Accepted || string(result.Code) != string(fault.Invalid) || m.Views().Target.Version != 1 {
		t.Fatal("inactive readmission did not refuse atomically", result)
	}
}
