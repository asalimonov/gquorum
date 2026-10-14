package raftengine

import (
	"context"
	"encoding/json"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// ObserveAuthorizedEffect records deterministic satisfaction of an exact native
// membership predicate. It does not attribute earlier work to this grant.
func (g *Group) ObserveAuthorizedEffect(ctx context.Context,
	grant topology.AuthorityGrant) (topology.EffectReceipt, error) {
	var receipt topology.EffectReceipt
	if err := g.validateAuthorityRequest(grant); err != nil {
		return receipt, err
	}
	if grant.Action == nil || grant.Action.Kind == topology.StepTransferLeader {
		return receipt, fault.ErrUnsupported
	}
	result, err := g.submitAuthority(ctx, authoritySubmission{kind: raftstate.ObserveEffect, grant: grant})
	if err == nil {
		err = json.Unmarshal(result.Data, &receipt)
	}
	return receipt, err
}

// TransferAuthorized commits initiation intent. Its caller must separately wait
// for observed target leadership; an acknowledged intent cannot promise an
// election.
func (g *Group) TransferAuthorized(ctx context.Context, grant topology.AuthorityGrant) error {
	if err := g.validateAuthorityRequest(grant); err != nil {
		return err
	}
	if grant.Action == nil || grant.Action.Kind != topology.StepTransferLeader {
		return fault.ErrInvalid
	}
	_, err := g.submitAuthority(ctx, authoritySubmission{kind: raftstate.TransferIntent, grant: grant})
	return err
}

func (g *Group) effectSatisfied(grant topology.AuthorityGrant) bool {
	if grant.Action == nil {
		return false
	}
	a := grant.Action
	voter, learner := slices.Contains(g.cs.Voters, a.ReplicaID), slices.Contains(g.cs.Learners, a.ReplicaID)
	switch a.Kind {
	case topology.StepAddLearner, topology.StepDemoteVoter:
		return learner && !voter
	case topology.StepPromoteVoter:
		return voter && !learner
	case topology.StepRemoveReplica:
		return !voter && !learner
	case topology.StepCreateFoundingGroup:
		var expected raftpb.ConfState
		for _, target := range grant.Targets {
			expected.Voters = append(expected.Voters, target.ReplicaID)
		}
		return sameConfiguration(expected, g.cs)
	default:
		return false
	}
}

func (g *Group) handleAuthorizedEffect(req *request) {
	if err := g.currentEffect(*req.admin.Grant); err != nil {
		answer(req, Result{}, err)
		return
	}
	if g.authority.Receipt != nil {
		if req.admin.Kind == raftstate.TransferIntent &&
			g.authority.Receipt.Outcome == topology.EffectTransferStarted && g.rn.BasicStatus().LeadTransferee == 0 {
			g.transferIssuedTerm = 0 // an exact retry can resume an aborted native attempt
		}
		result, err := authorityResult(g.authority.Receipt)
		answer(req, result, err)
		return
	}
	if g.pendingAdministrativeEntry() {
		answer(req, Result{}, fault.ErrAuthorityPending)
		return
	}
	if err := g.validateEffectIntent(*req.admin); err != nil {
		answer(req, Result{}, err)
		return
	}
	data, err := raftstate.EncodeEntry(*req.admin)
	if err == nil {
		err = g.rn.Propose(data)
	}
	if err != nil {
		answer(req, Result{}, err)
		return
	}
	g.changes[req.correlation] = req
}

func (g *Group) applyAuthorizedEffect(entry raftpb.Entry, e raftstate.Entry) error {
	if err := g.currentEffect(*e.Grant); err != nil || g.authority.Receipt != nil {
		g.notifyAuthority(e, nil, fault.ErrAuthorityFenced)
		return nil
	}
	if err := g.validateEffectIntent(e); err != nil {
		g.notifyAuthority(e, nil, err)
		return nil
	}
	outcome := topology.EffectAlreadySatisfied
	if e.Kind == raftstate.TransferIntent {
		if e.Action.Kind == topology.StepDemoteVoter {
			g.applyDemotionTransfer(entry, e)
			return nil // a prerequisite is never a demotion completion receipt
		}
		outcome = topology.EffectTransferStarted
	}
	receipt, err := g.effectReceipt(entry, *e.Grant, outcome)
	if err != nil {
		return err
	}
	g.authority.Receipt = &receipt
	g.notifyAuthority(e, receipt, nil)
	return nil
}

// Resume once per leadership term after the durable/applied current-term
// barrier.
// A new leader can resume a retained intent; a newer applied floor removes it.
// Quorum confirmation and installed-log order govern the intent, not cached
// meta.
func (g *Group) resumeAuthorizedTransfer() {
	if !g.opts.ManagedAuthority || g.pending != nil || g.rn.HasReady() {
		return
	}
	target := g.activeTransferTarget()
	if target == 0 {
		return
	}
	status := g.rn.BasicStatus()
	if status.RaftState != raft.StateLeader || g.transferIssuedTerm == status.Term || g.authorityReady() != nil ||
		g.pendingAdministrativeEntry() {
		return
	}
	if !slices.Contains(g.cs.Voters, target) {
		return
	}
	if grant := g.authority.Grant; grant != nil && grant.Action != nil &&
		grant.Action.Kind == topology.StepDemoteVoter {
		if validateAuthorizedVoterSet(grant.Action, g.cs.Voters, approvedVoterSet) != nil {
			return
		}
	}
	g.transferIssuedTerm = status.Term
	if target != g.opts.ReplicaID {
		g.rn.TransferLeader(target)
	}
}

func (g *Group) activeTransferTarget() uint64 {
	grant, receipt := g.authority.Grant, g.authority.Receipt
	if grant == nil || grant.Action == nil {
		return 0
	}
	if receipt != nil && receipt.Outcome == topology.EffectTransferStarted {
		return grant.Action.ReplicaID
	}
	if receipt == nil && g.authority.DemotionTransferIndex != 0 && grant.Action.ReplicaID == g.opts.ReplicaID {
		return grant.Action.TransferReplicaID
	}
	return 0
}

func (g *Group) validateEffectIntent(e raftstate.Entry) error {
	if e.Kind == raftstate.TransferIntent {
		if e.Action.Kind == topology.StepDemoteVoter {
			if err := validateAuthorizedVoterSet(e.Action, g.cs.Voters, approvedVoterSet); err != nil {
				return err
			}
		}
		target := e.Action.ReplicaID
		if e.Action.Kind == topology.StepDemoteVoter {
			target = e.Action.TransferReplicaID
		}
		if !slices.Contains(g.cs.Voters, target) {
			return fault.ErrUnsafeMembership
		}
	} else {
		if !g.effectSatisfied(*e.Grant) {
			return fault.ErrUnavailable
		}
		if err := validateAuthorizedVoterSet(e.Action, g.cs.Voters, resultingVoterSet); err != nil {
			return err
		}
	}
	return nil
}

func (g *Group) applyDemotionTransfer(entry raftpb.Entry, e raftstate.Entry) {
	if !slices.Contains(g.cs.Voters, e.Action.ReplicaID) {
		g.notifyAuthority(e, nil, fault.ErrUnsafeMembership)
		return
	}
	if g.authority.DemotionTransferIndex == 0 {
		g.authority.DemotionTransferIndex, g.authority.DemotionTransferTerm = entry.Index, entry.Term
		g.transferIssuedTerm = 0
	}
	g.notifyAuthority(e, nil, fault.ErrNotLeader)
}
