package raftengine

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/membership"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// ProposeAuthorizedConfChange applies an exact granted membership operation
// against expected native state.
func (g *Group) ProposeAuthorizedConfChange(ctx context.Context, grant topology.AuthorityGrant, cc raftpb.ConfChangeV2,
	expected raftpb.ConfState) (topology.EffectReceipt, error) {
	var receipt topology.EffectReceipt
	if err := g.validateAuthorityRequest(grant); err != nil {
		return receipt, err
	}
	if err := exactConfiguration(cc, grant.Action); err != nil {
		return receipt, err
	}
	cc.Changes, cc.Context = slices.Clone(cc.Changes), nil
	cloned := copyConf(expected)
	submission := authoritySubmission{kind: raftstate.ManagedConfiguration, grant: grant, change: &cc,
		expected: &cloned}
	result, err := g.submitAuthority(ctx, submission)
	if err == nil {
		err = json.Unmarshal(result.Data, &receipt)
	}
	return receipt, err
}

func exactConfiguration(cc raftpb.ConfChangeV2, a *topology.AuthorizedStep) error {
	if a == nil || cc.Transition != raftpb.ConfChangeTransitionAuto || len(cc.Changes) != 1 ||
		cc.Changes[0].NodeID != a.ReplicaID || a.ReplicaID == 0 {
		return fault.ErrUnsafeMembership
	}
	var typ raftpb.ConfChangeType
	switch a.Kind {
	case topology.StepAddLearner, topology.StepDemoteVoter:
		typ = raftpb.ConfChangeAddLearnerNode
	case topology.StepPromoteVoter:
		typ = raftpb.ConfChangeAddNode
	case topology.StepRemoveReplica:
		typ = raftpb.ConfChangeRemoveNode
	default:
		return fault.ErrUnsafeMembership
	}
	if cc.Changes[0].Type != typ {
		return fault.ErrUnsafeMembership
	}
	return nil
}

func sameConfiguration(a, b raftpb.ConfState) bool {
	if a.AutoLeave != b.AutoLeave {
		return false
	}
	for _, pair := range [][2][]uint64{{a.Voters, b.Voters}, {a.Learners, b.Learners},
		{a.VotersOutgoing, b.VotersOutgoing}, {a.LearnersNext, b.LearnersNext}} {
		x, y := slices.Clone(pair[0]), slices.Clone(pair[1])
		slices.Sort(x)
		slices.Sort(y)
		if !slices.Equal(x, y) {
			return false
		}
	}
	return true
}

type voterSetPhase uint8

const (
	approvedVoterSet voterSetPhase = iota
	resultingVoterSet
)

// validateAuthorizedVoterSet binds a granted membership action to the native
// voter set. Receipt reuse happens before this check: a completed effect need
// not reconstruct a predecessor that no longer exists.
func validateAuthorizedVoterSet(a *topology.AuthorizedStep, voters []uint64, phase voterSetPhase) error {
	if a == nil {
		return fault.ErrOperatorAuthorization
	}
	want := a.ExpectedVotersDigest
	if phase == resultingVoterSet {
		want = a.ResultVotersDigest
	}
	if want == "" || topology.VoterSetDigest(voters) != want {
		return fault.New(fault.UnsafeMembership, "native voter set differs from approved proof; replan_required", nil)
	}
	if phase == resultingVoterSet {
		return nil
	}
	projected := slices.Clone(voters)
	switch a.Kind {
	case topology.StepAddLearner:
	case topology.StepPromoteVoter:
		if !slices.Contains(projected, a.ReplicaID) {
			projected = append(projected, a.ReplicaID)
		}
	case topology.StepDemoteVoter, topology.StepRemoveReplica:
		projected = slices.DeleteFunc(projected, func(id uint64) bool { return id == a.ReplicaID })
	default:
		return fault.ErrOperatorAuthorization
	}
	if a.ResultVotersDigest == "" || topology.VoterSetDigest(projected) != a.ResultVotersDigest {
		return fault.New(fault.UnsafeMembership, fmt.Sprintf("approved %s has an invalid voter-set projection",
			a.Kind), nil)
	}
	return nil
}

// currentEffect is owner-loop-only. A receipt may be reused only for its exact
// immutable grant; a new floor cannot authorize an old effect's repetition.
func (g *Group) currentEffect(grant topology.AuthorityGrant) error {
	if g.authority.Grant == nil || !grant.Equal(*g.authority.Grant) {
		return fault.ErrAuthorityFenced
	}
	return nil
}

func (g *Group) handleAuthorizedConfiguration(req *request) {
	err := g.currentEffect(*req.admin.Grant)
	if err == nil {
		err = exactConfiguration(*req.change, req.admin.Action)
	}
	if err != nil {
		answer(req, Result{}, err)
		return
	}
	if g.authority.Receipt != nil {
		result, err := authorityResult(g.authority.Receipt)
		answer(req, result, err)
		return
	}
	if req.expected == nil || !sameConfiguration(*req.expected, g.cs) {
		answer(req, Result{}, fault.ErrUnavailable)
		return
	}
	phase := approvedVoterSet
	if g.effectSatisfied(*req.admin.Grant) {
		phase = resultingVoterSet
	}
	if err := validateAuthorizedVoterSet(req.admin.Action, g.cs.Voters, phase); err != nil {
		answer(req, Result{}, err)
		return
	}
	if g.pendingAdministrativeEntry() {
		answer(req, Result{}, fault.ErrAuthorityPending)
		return
	}
	if phase == resultingVoterSet {
		req.admin.Kind = raftstate.ObserveEffect
		g.handleAuthorizedEffect(req)
		return
	}
	g.proposeAuthorizedConfiguration(req)
}

func (g *Group) proposeAuthorizedConfiguration(req *request) {
	a := req.admin.Action
	// Add and demote use the same Raft opcode, but never authorize each other.
	if a.Kind == topology.StepAddLearner &&
		slices.Contains(g.cs.Voters, a.ReplicaID) || a.Kind == topology.StepDemoteVoter &&
		!slices.Contains(g.cs.Voters, a.ReplicaID) {
		answer(req, Result{}, fault.ErrUnsafeMembership)
		return
	}
	status := g.rn.Status()
	live := nativeLive{g.opts.ReplicaID: true}
	for id, p := range status.Progress {
		// Raft initializes a newly added peer as RecentActive. A zero Match
		// is not evidence of a live replica and cannot support promotion.
		if p.RecentActive && p.Match != 0 {
			live[id] = true
		}
	}
	if g.prepareDemotionTransfer(req, live) {
		return
	}
	checkErr := (membership.SafetyRules{}).Check(nativeMembershipView{membershipView{
		g}, status}, live, *req.change)
	if checkErr != nil {
		answer(req, Result{}, checkErr)
		return
	}
	cc := *req.change
	encoded, err := raftstate.EncodeEntry(*req.admin)
	cc.Context = encoded
	if err == nil {
		err = g.rn.ProposeConfChange(cc)
	}
	if err != nil {
		answer(req, Result{}, err)
		return
	}
	g.changes[req.correlation] = req
}

type nativeMembershipView struct {
	membershipView
	status raft.Status
}

func (v nativeMembershipView) Status() raft.Status { return v.status }

func (v nativeMembershipView) IsLeader() bool { return v.status.RaftState == raft.StateLeader }

type nativeLive map[uint64]bool

func (v nativeLive) Live(string) (map[uint64]bool, bool) { return v, true }

func (g *Group) effectReceipt(entry raftpb.Entry, grant topology.AuthorityGrant,
	outcome topology.EffectOutcome) (topology.EffectReceipt, error) {
	receipt := topology.EffectReceipt{
		Key:          grant.Key,
		Resource:     topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: g.opts.GroupID},
		Action:       grant.Action,
		Outcome:      outcome,
		AppliedIndex: entry.Index,
		AppliedTerm:  entry.Term,
	}
	if grant.Action != nil && grant.Action.ReplicaID != 0 {
		for _, target := range grant.Targets {
			if target.NodeName == grant.Action.NodeName && target.ReplicaID == grant.Action.ReplicaID {
				receipt.Target = target
				break
			}
		}
	}
	return receipt.Clone(), receipt.Validate(grant)
}

func (g *Group) applyAuthorizedConfChange(entry raftpb.Entry, cc raftpb.ConfChangeV2, e raftstate.Entry) error {
	if err := g.currentEffect(*e.Grant); err != nil || g.authority.Receipt != nil {
		g.notifyAuthority(e, nil, fault.ErrAuthorityFenced)
		return nil // Advance still releases Raft's pendingConfIndex.
	}
	if err := exactConfiguration(cc, e.Action); err != nil {
		return fault.New(fault.Unreadable, "committed membership differs from its approved action", err)
	}
	phase := approvedVoterSet
	if g.effectSatisfied(*e.Grant) {
		phase = resultingVoterSet
	}
	if err := validateAuthorizedVoterSet(e.Action, g.cs.Voters, phase); err != nil {
		return fault.New(fault.Unreadable, "committed membership has an invalid voter predecessor", err)
	}
	receipt, err := g.effectReceipt(entry, *e.Grant, topology.EffectApplied)
	if err != nil {
		return err
	}
	if change := cc.Changes[0]; change.Type == raftpb.ConfChangeRemoveNode && change.NodeID != g.opts.ReplicaID {
		g.queueRetirement(entry, change.NodeID)
	}
	g.cs = copyConf(*g.rn.ApplyConfChange(cc))
	if err := validateAuthorizedVoterSet(e.Action, g.cs.Voters, resultingVoterSet); err != nil {
		return fault.New(fault.Unreadable, "committed membership has an invalid voter result", err)
	}
	g.membershipChanged = true
	g.authority.Receipt = &receipt
	g.notifyAuthority(e, receipt, nil)
	return nil
}

func (g *Group) prepareDemotionTransfer(req *request, live nativeLive) bool {
	a := req.admin.Action
	if a.Kind == topology.StepDemoteVoter && a.ReplicaID == g.opts.ReplicaID && a.TransferReplicaID != 0 {
		if a.TransferReplicaID == a.ReplicaID || !live[a.TransferReplicaID] ||
			!slices.Contains(g.cs.Voters, a.TransferReplicaID) {
			answer(req, Result{}, fault.New(fault.Unavailable, "demotion transfer target is unavailable", nil))
			return true
		}
		if g.authority.DemotionTransferIndex != 0 {
			if g.rn.BasicStatus().LeadTransferee == 0 {
				g.transferIssuedTerm = 0
			}
			answer(req, Result{}, fault.ErrNotLeader)
			return true // the retained prerequisite resumes on the owner loop
		}
		req.admin.Kind = raftstate.TransferIntent
		g.handleAuthorizedEffect(req)
		return true
	}
	return false
}
