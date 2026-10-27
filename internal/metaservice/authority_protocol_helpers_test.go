//go:build functional

package metaservice

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

// Protocol doubles model durable host and group evidence independently of meta.
func protocolEffectReceipts(grant topology.AuthorityGrant) []topology.EffectReceipt {
	var result []topology.EffectReceipt
	for _, resource := range grant.Resources {
		if resource.Kind == topology.AuthorityHost {
			for _, target := range grant.Targets {
				if target.NodeName == resource.NodeName {
					result = append(result, topology.EffectReceipt{Key: grant.Key, Resource: resource, Action: grant.Action, AdmissionID: grant.AdmissionID, Target: target, Outcome: topology.EffectApplied, LocalSequence: grant.Key.Revision*10 + 1})
				}
			}
			continue
		}
		receipt := topology.EffectReceipt{Key: grant.Key, Resource: resource, Action: grant.Action, Outcome: topology.EffectApplied, AppliedIndex: grant.Key.Revision*10 + 1, AppliedTerm: 1}
		if grant.Action.Kind == topology.StepTransferLeader {
			receipt.Outcome = topology.EffectTransferStarted
		}
		for _, target := range grant.Targets {
			if target.NodeName == grant.Action.NodeName && target.ReplicaID == grant.Action.ReplicaID {
				receipt.Target = target
			}
		}
		result = append(result, receipt)
	}
	return result
}

func (c *verifiedLoadController) installFence(ctx context.Context, request load.FenceRequest) (load.FenceResult, error) {
	if _, err := c.authorized(ctx, request.Grant); err != nil {
		return load.FenceResult{}, err
	}
	proof := topology.FenceEvidence{Key: request.Grant.Key, Resource: request.Resource, Receiver: request.To, ReplicaID: request.ReplicaID}
	if request.Resource.Kind == topology.AuthorityHost {
		proof.LocalSequence = request.Grant.Key.Revision * 10
	} else {
		proof.AppliedIndex, proof.AppliedTerm = request.Grant.Key.Revision*10, 1
	}
	return load.FenceResult{From: request.To, Evidence: proof}, nil
}

func (f *mutationFixture) consumeStep(t *testing.T, action topology.AuthorizedStep) {
	t.Helper()
	if action.GroupID != topology.MetaGroupID && action.Kind == topology.StepPrepareReplica {
		f.retireFormerHost(t, action)
	}
	f.bindFixtureAdmission(t, action)
	grant := f.command(t, metaengine.BeginEffectProposal{Action: action, ProposalID: newProposalID()}).Grant
	for _, receipt := range protocolEffectReceipts(*grant) {
		f.command(t, metaengine.RecordEffectProposal{Receipt: receipt, At: time.Now().UTC(), ProposalID: newProposalID()})
	}
}

func (f *mutationFixture) bindFixtureAdmission(t *testing.T, action topology.AuthorizedStep) {
	t.Helper()
	if action.GroupID == topology.MetaGroupID && action.Kind == topology.StepPrepareReplica {
		instance := "fixture-" + action.NodeName
		if _, exists := f.g.machine.Admission(action.NodeName, instance); !exists {
			admission := metaengine.AdmissionRecord{NodeName: action.NodeName, InstanceID: instance,
				MemberID: action.ReplicaID, OperationID: action.OperationID}
			f.command(t, metaengine.BindAdmissionProposal{Admission: admission, At: time.Now().UTC(), ProposalID: newProposalID()})
			f.command(t, metaengine.ReadyAdmissionProposal{NodeName: admission.NodeName, InstanceID: instance,
				MemberID: action.ReplicaID, OperationID: action.OperationID, ProposalID: newProposalID()})
		}
	}
}

func (f *mutationFixture) fixturePlanRead(t *testing.T, request metaengine.QueryRequest) metaengine.PlanRead {
	t.Helper()
	data, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.g.machine.Query(t.Context(), data)
	if err != nil {
		t.Fatal(err)
	}
	var read metaengine.PlanRead
	if err := json.Unmarshal(result.Data, &read); err != nil {
		t.Fatal(err)
	}
	return read
}

func (f *mutationFixture) fixtureApproval(t *testing.T, doc topology.Document) metaengine.BeginOperationProposal {
	t.Helper()
	request := metaengine.QueryRequest{Kind: "approval_plan", Version: doc.Version}
	read := f.fixturePlanRead(t, request)
	request.Native = make(map[string]topology.NativeState)
	for _, scope := range read.Scopes {
		if native, found := f.native[scope.GroupID]; found && doc.Group(scope.GroupID) != nil {
			request.Native[scope.GroupID] = native
		}
	}
	read = f.fixturePlanRead(t, request)
	for group, native := range request.Native {
		live := make(map[uint64]bool)
		for _, id := range native.ConfState.Voters {
			live[id] = true
		}
		row := read.Input.Groups[group]
		row.Observation = &topology.GroupStatus{GroupID: group, Found: true, LeaderID: native.ReplicaID,
			Commit: native.Applied, ConfState: native.ConfState, HealthKnown: true, LiveVoters: live}
		read.Input.Groups[group] = row
	}
	plan, err := topology.BuildPlanPreferred(read.Input, doc, topology.PlanOptions{Rules: f.g.rules})
	if err != nil {
		t.Fatal(err)
	}
	return metaengine.BeginOperationProposal{Version: doc.Version, Plan: plan, Scopes: read.Scopes,
		Actor: "test", Origin: doc.Origin, At: time.Now().UTC(), ProposalID: newProposalID(),
		DeletionSemantics: metaengine.DeletionSemanticsDetached}
}

func (f *mutationFixture) approveFixture(t *testing.T, doc topology.Document) {
	t.Helper()
	proposal := f.fixtureApproval(t, doc)
	plan := proposal.Plan
	begin := f.command(t, proposal)
	for _, step := range plan.Steps {
		f.bindFixtureAdmission(t, topology.AuthorizedStep{OperationID: begin.OperationID,
			GroupID: step.GroupID, Kind: step.Kind, NodeName: step.NodeName, ReplicaID: step.ReplicaID})
	}
	for turn := 0; turn <= len(plan.Steps)+len(plan.Blocks); turn++ {
		status, err := f.g.machine.Operation(begin.OperationID)
		if err != nil {
			t.Fatal(err)
		}
		if status.State == topology.OperationSucceeded {
			return
		}
		work, err := f.g.machine.ReadyBlocks("", 1)
		if err != nil || len(work) == 0 {
			t.Fatal("fixture operation cannot progress", status, err)
		}
		f.completeFixtureWork(t, work[0].Operation, work[0].GroupID, doc)
	}
	t.Fatal("fixture operation did not finish", plan)
}

func (f *mutationFixture) completeFixtureWork(t *testing.T, op topology.Operation, group string,
	doc topology.Document) {
	t.Helper()
	position := slices.IndexFunc(op.Plan.Blocks, func(block topology.PlanBlock) bool { return block.GroupID == group })
	block := op.Plan.Blocks[position]
	transition := topology.OperationTransition{OperationID: op.Status.ID, Version: op.Status.Version,
		Attempt: op.Status.Attempt, GroupID: group, State: topology.OperationRunning}
	for _, seq := range block.Steps {
		step := op.Plan.Steps[seq-1]
		transition.StepSeq, transition.At = seq, time.Now().UTC()
		f.command(t, metaengine.TransitionOperationProposal{Transition: transition, ProposalID: newProposalID()})
		if step.Kind == topology.StepWaitCaughtUp {
			continue
		}
		f.consumeStep(t, topology.AuthorizedStep{OperationID: topology.OperationID(op.Status.ID),
			Version: op.Status.Version, Attempt: op.Status.Attempt, StepSeq: seq, Kind: step.Kind,
			GroupID: group, NodeName: step.NodeName, ReplicaID: step.ReplicaID,
			TransferNodeName: step.TransferNodeName, TransferReplicaID: step.TransferReplicaID,
			ExpectedVotersDigest: step.ExpectedVotersDigest, ResultVotersDigest: step.ResultVotersDigest})
	}
	transition.State, transition.At = topology.OperationSucceeded, time.Now().UTC()
	f.command(t, metaengine.TransitionOperationProposal{Transition: transition, ProposalID: newProposalID()})
	if desired := doc.Group(group); desired != nil {
		f.native[group] = fixtureNative(*desired)
	} else {
		delete(f.native, group)
	}
}

// Detached retirement owns its own grant. A required same-host Prepare waits
// for that exact predecessor's retirement, without waiting for collection.
func (f *mutationFixture) retireFormerHost(t *testing.T, action topology.AuthorizedStep) {
	t.Helper()
	for after := ""; ; {
		page, err := f.g.machine.CleanupPage(after, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, debt := range page.Records {
			id := debt.Identity
			if id.GroupID != action.GroupID || id.NodeName != action.NodeName ||
				id.ReplicaID == action.ReplicaID || debt.Retired != nil {
				continue
			}
			if debt.Stopped == nil || debt.RetireAction == nil {
				t.Fatal("predecessor lacks required stop or retirement action", debt)
			}
			grant := f.command(t, metaengine.BeginCleanupEffectProposal{Identity: id,
				Action: *debt.RetireAction, ProposalID: newProposalID()}).Grant
			for _, receipt := range protocolEffectReceipts(*grant) {
				f.command(t, metaengine.RecordEffectProposal{Receipt: receipt, At: time.Now().UTC(),
					ProposalID: newProposalID()})
			}
		}
		if page.Next == "" {
			return
		}
		after = page.Next
	}
}
