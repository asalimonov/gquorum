//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/recovery/model"
	recoveryplacement "github.com/asalimonov/gquorum/internal/recovery/placement"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func importedRecoveryWithPolicy(t *testing.T, strict bool) (scopedState, model.ImportProposal) {
	t.Helper()
	s := recoveryFoundingFixture(t)
	p := recoveryImportFixture()
	p.Sources[0].Identity.ClusterID = p.Source.ClusterID
	if strict {
		p.ApprovedSource.Placement.MinReplicas = 3
		p.ApprovedSource.Nodes[0].Labels = map[string]string{"az": "a"}
		for i := range p.ApprovedSource.Groups {
			policy := topology.DefaultVoterConfig()
			policy.DistinctVoterLabel = "az"
			p.ApprovedSource.Groups[i].VoterConfig = &policy
			if p.ApprovedSource.Groups[i].ID != topology.MetaGroupID {
				p.ApprovedSource.Groups[i].Hosting = &topology.HostingRule{Mode: topology.HostingAllEligible}
			}
		}
	}

	if _, err := foundingCommand(t, &s, p, raftpb.ConfState{Voters: []uint64{5}}); err != nil {
		t.Fatal(err)
	}
	return s, p
}

func consumedRecoveryFixture(t *testing.T) (scopedState, model.GroupConsumedProposal) {
	t.Helper()
	return consumedRecoveryWithPolicy(t, false)
}

func consumedRecoveryWithPolicy(t *testing.T, strict bool) (scopedState, model.GroupConsumedProposal) {
	t.Helper()
	s, p := importedRecoveryWithPolicy(t, strict)
	op := s.indexes.operations[s.indexes.first[2]]
	var action topology.AuthorizedStep
	for _, step := range op.Plan.Steps {
		if step.Kind == topology.StepCreateFoundingGroup {
			action = scopedPlanAction(op, step)
		}
	}
	if action.StepSeq == 0 {
		t.Fatal("ordinary creation step absent")
	}
	applyScopedTransitionFixture(t, &s, scopedTransitionFor(op.Clone(), "shard0", action.StepSeq))
	result, err := s.applyBeginEffect(s.AppliedIndex+1, BeginEffectProposal{Action: action, ProposalID: "create"})
	if err != nil || result.result.Grant == nil {
		t.Fatal(result, err)
	}
	grant := *result.result.Grant
	proposal := model.GroupConsumedProposal{RecoveryID: p.RecoveryID, PlanDigest: p.PlanDigest, GroupID: "shard0", ProposalID: "consumed",
		Checkpoint: model.CheckpointProof{Identity: s.Recovery.Groups[0].Destination, Position: model.Position{Index: 2, Term: 1}, Digest: strings.Repeat("b", 64)}}
	for _, receipt := range fixtureEffectReceipts(grant) {
		if receipt.Resource.Kind == topology.AuthorityGroup {
			receipt.AppliedIndex, receipt.AppliedTerm = 1, 1
		}
		if result, err := s.applyRecordEffect(s.AppliedIndex+1, RecordEffectProposal{Receipt: receipt, At: p.At.Add(time.Second), ProposalID: "receipt"}); err != nil || !result.result.Accepted {
			t.Fatal(result, err)
		}
	}
	return s, proposal
}

func TestRecoveryConsumptionRequiresExactCreationReceiptsAndCheckpoint(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*model.GroupConsumedProposal){
		"identity":      func(p *model.GroupConsumedProposal) { p.Checkpoint.Identity.ReplicaID++ },
		"checkpoint":    func(p *model.GroupConsumedProposal) { p.Checkpoint.Position.Index = 1 },
		"snapshot hash": func(p *model.GroupConsumedProposal) { p.Checkpoint.Digest = "not-a-hash" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s, p := consumedRecoveryFixture(t)
			mutate(&p)
			before, _ := json.Marshal(s)
			if _, err := s.applyRecoveryGroupConsumed(s.AppliedIndex+1, p); err == nil {
				t.Fatal("forged consumption accepted")
			}
			after, _ := json.Marshal(s)
			if !bytes.Equal(before, after) {
				t.Fatal("refused consumption changed recovery state")
			}
		})
	}
	s, p := consumedRecoveryFixture(t)
	if _, err := s.applyCompleteRecoveryImport(s.AppliedIndex+1, model.CompleteImportProposal{RecoveryID: p.RecoveryID, PlanDigest: p.PlanDigest, ProposalID: "complete", At: time.Unix(120, 0)}); err == nil {
		t.Fatal("incomplete recovery completed")
	}
	if result, err := s.applyRecoveryGroupConsumed(s.AppliedIndex+1, p); err != nil || !result.changed {
		t.Fatal(result, err)
	}
	if result, err := s.applyRecoveryGroupConsumed(s.AppliedIndex+1, p); err != nil || result.changed {
		t.Fatal("consumption retry rewrote evidence", result, err)
	}
	changed := p
	changed.Checkpoint.Position.Index++
	before, _ := json.Marshal(s)
	if _, err := s.applyRecoveryGroupConsumed(s.AppliedIndex+1, changed); err == nil {
		t.Fatal("changed checkpoint replaced retained consumption")
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("changed checkpoint mutated retained consumption")
	}
	op := s.indexes.operations[s.indexes.first[2]].Clone()
	transition := scopedTransitionFor(op, "shard0", op.Plan.Steps[len(op.Plan.Steps)-1].Seq)
	transition.State = topology.OperationSucceeded
	applyScopedTransitionFixture(t, &s, transition)
	complete := model.CompleteImportProposal{RecoveryID: p.RecoveryID, PlanDigest: p.PlanDigest, ProposalID: "complete", At: time.Unix(120, 0).UTC()}
	if result, err := s.applyCompleteRecoveryImport(s.AppliedIndex+1, complete); err != nil || !result.changed {
		t.Fatal(result, err)
	}
	if s.Recovery.Phase != "complete" || len(s.Recovery.Policies) != 2 {
		t.Fatal("data completion discarded policy provenance", s.Recovery)
	}
	if err := s.pruneRetainedProofs(); err != nil {
		t.Fatal("consumed lineage failed pruning", err)
	}
	encoded, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var restored scopedState
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal("consumed recovery failed restart", err)
	}
	if _, err := restored.applyCompleteRecoveryImport(restored.AppliedIndex+1, complete); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryDataCompletionDoesNotReleaseThreeDomainDeficit(t *testing.T) {
	t.Parallel()
	s, p := consumedRecoveryWithPolicy(t, true)
	if _, err := s.applyRecoveryGroupConsumed(s.AppliedIndex+1, p); err != nil {
		t.Fatal(err)
	}
	op := s.indexes.operations[s.indexes.first[2]].Clone()
	transition := scopedTransitionFor(op, "shard0", op.Plan.Steps[len(op.Plan.Steps)-1].Seq)
	transition.State = topology.OperationSucceeded
	applyScopedTransitionFixture(t, &s, transition)
	complete := model.CompleteImportProposal{RecoveryID: p.RecoveryID, PlanDigest: p.PlanDigest, ProposalID: "complete", At: time.Unix(120, 0).UTC()}
	if _, err := s.applyCompleteRecoveryImport(s.AppliedIndex+1, complete); err != nil {
		t.Fatal(err)
	}
	for _, policy := range s.Recovery.Policies {
		if policy.PolicyReleased {
			t.Fatal("data completion released unsatisfied policy", policy)
		}
	}
	encoded, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var restored scopedState
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	if err := restored.pruneRetainedProofs(); err != nil {
		t.Fatal(err)
	}
	for _, policy := range restored.Recovery.Policies {
		if policy.PolicyReleased {
			t.Fatal("restart/pruning released unsatisfied policy")
		}
	}
}

func completedRecoveryFixture(t *testing.T, strict bool) scopedState {
	t.Helper()
	s, p := consumedRecoveryWithPolicy(t, strict)
	body, _ := json.Marshal(p)
	if result, err := applyScopedProposal(&s, s.AppliedIndex+1, KindRecoveryGroupConsumed, body); err != nil || !result.changed {
		t.Fatal(result, err)
	}
	op := s.indexes.operations[s.indexes.first[2]].Clone()
	transition := scopedTransitionFor(op, "shard0", op.Plan.Steps[len(op.Plan.Steps)-1].Seq)
	transition.State = topology.OperationSucceeded
	applyScopedTransitionFixture(t, &s, transition)
	completion := model.CompleteImportProposal{RecoveryID: p.RecoveryID, PlanDigest: p.PlanDigest,
		ProposalID: "complete", At: time.Unix(120, 0).UTC()}
	body, _ = json.Marshal(completion)
	if result, err := applyScopedProposal(&s, s.AppliedIndex+1, KindCompleteRecoveryImport, body); err != nil || !result.changed {
		t.Fatal(result, err)
	}
	return s
}

func TestRecoveryPoliciesReleaseOnlyAfterOrdinaryApprovedConvergence(t *testing.T) {
	t.Parallel()
	s := completedRecoveryFixture(t, true)
	desired := s.Latest.Clone()
	desired.ChangedBy, desired.Comment = "operator", "explicit policy adjustment"
	desired.Placement.MinReplicas = 1
	for i := range desired.Groups {
		group := &desired.Groups[i]
		policy := topology.DefaultVoterConfig()
		policy.TargetVoters = 1
		policy.DistinctVoterLabel = ""
		group.VoterConfig = &policy
	}
	body, _ := json.Marshal(ProposeDocument{Doc: desired, ExpectedVersion: s.Latest.Version})
	if result, err := applyScopedProposal(&s, s.AppliedIndex+1, KindProposeDocument, body); err != nil || !result.changed {
		t.Fatal(result, err)
	}
	for _, policy := range s.Recovery.Policies {
		if policy.PolicyReleased {
			t.Fatal("staging released a retained exception")
		}
	}
	context, err := s.recoveryPlacementContext()
	if err != nil {
		t.Fatal(err)
	}
	input := s.planningDeclarations()
	input.Native = map[string]topology.NativeState{
		topology.MetaGroupID: {GroupID: topology.MetaGroupID, ReplicaID: 5, Applied: s.AppliedIndex, ConfState: raftpb.ConfState{Voters: []uint64{5}}},
		"shard0":             {GroupID: "shard0", ReplicaID: 7, Applied: 2, ConfState: raftpb.ConfState{Voters: []uint64{7}}},
	}
	plan, err := recoveryplacement.BuildPlan(context, input, s.Latest, topology.PlanOptions{Rules: s.Rules})
	if err != nil {
		t.Fatal(err)
	}
	approval := BeginOperationProposal{Version: s.Latest.Version, Plan: plan, Actor: "operator",
		Origin: topology.OriginOperator, At: time.Unix(130, 0).UTC(), ProposalID: "policy-repair",
		Scopes: s.capturePlanScopes(topology.AffectedGroups(s.Target, s.Latest))}
	body, _ = json.Marshal(approval)
	if result, err := applyScopedProposal(&s, s.AppliedIndex+1, KindBeginOperation, body); err != nil || !result.changed {
		t.Fatal(result, err)
	}
	for _, policy := range s.Recovery.Policies {
		if !policy.PolicyReleased {
			t.Fatal("converged ordinary policy did not release exception", policy)
		}
	}
	if err := s.pruneRetainedProofs(); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var restored scopedState
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	for _, policy := range restored.Recovery.Policies {
		if !policy.PolicyReleased {
			t.Fatal("restart resurrected policy exception")
		}
	}
	if _, err := restored.applyRecoveryImport(restored.AppliedIndex+1, recoveryImportFixture()); err == nil {
		t.Fatal("altered historical import was accepted after policy release")
	}
}
