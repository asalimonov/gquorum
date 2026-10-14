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

type adminReply struct {
	req    *request
	result Result
	err    error
}

// InstallAuthority requires its caller to verify the retained committed grant.
// Only this group's durable application establishes the resource fence
// boundary.
func (g *Group) InstallAuthority(ctx context.Context, grant topology.AuthorityGrant) (topology.FenceEvidence, error) {
	var evidence topology.FenceEvidence
	if err := g.validateAuthorityRequest(grant); err != nil {
		return evidence, err
	}
	local := topology.AuthorityTarget{NodeName: g.opts.NodeName, MemberID: g.opts.MemberID, ReplicaID: g.opts.ReplicaID}
	if !slices.Contains(grant.Targets, local) {
		return evidence, fault.ErrIdentityMismatch
	}
	result, err := g.submitAuthority(ctx, authoritySubmission{kind: raftstate.InstallAuthority, grant: grant})
	if err == nil {
		err = json.Unmarshal(result.Data, &evidence)
	}
	return evidence, err
}

// AuthorityReceipt reads the exact installed grant's consumption after a safe
// read barrier on this replica. Advancing the floor never substitutes a
// receipt.
func (g *Group) AuthorityReceipt(parent context.Context,
	key topology.AuthorityKey) (topology.EffectReceiptLookup, error) {
	if !g.opts.ManagedAuthority {
		return topology.EffectReceiptLookup{}, fault.ErrUnsupported
	}
	if key.ClusterID != g.opts.ClusterID || key.GroupID != g.opts.GroupID || key.Revision == 0 {
		return topology.EffectReceiptLookup{}, fault.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(parent, g.opts.Reads.Timeout)
	defer cancel()
	result, err := g.waitRead(&readRequest{ctx: ctx, authorityKey: &key, reply: make(chan response, 1)})
	var receipt *topology.EffectReceipt
	if err == nil {
		err = json.Unmarshal(result.Data, &receipt)
	}
	if receipt == nil || err != nil {
		return topology.EffectReceiptLookup{}, err
	}
	return topology.EffectReceiptLookup{Receipt: receipt.Clone(), Found: true}, nil
}

func (g *Group) readAuthorityReceipt(key topology.AuthorityKey) (Result, error) {
	if g.authority.Grant != nil && key != g.authority.Grant.Key {
		return Result{}, fault.ErrAuthorityFenced
	}
	return authorityResult(g.authority.Receipt)
}
func authorityResult(value any) (Result, error) {
	data, err := json.Marshal(value)
	return Result{Data: data}, err
}
func (g *Group) validateAuthorityRequest(grant topology.AuthorityGrant) error {
	if !g.opts.ManagedAuthority {
		return fault.ErrUnsupported
	}
	return validateGroupGrant(g.opts, grant)
}

type authoritySubmission struct {
	kind     raftstate.EntryKind
	grant    topology.AuthorityGrant
	change   *raftpb.ConfChangeV2
	expected *raftpb.ConfState
}

func (g *Group) submitAuthority(parent context.Context, submission authoritySubmission) (Result, error) {
	ctx, cancel := context.WithTimeout(parent, g.opts.ProposalTimeout)
	defer cancel()
	cloned := submission.grant.Clone()
	e := &raftstate.Entry{Schema: raftstate.Schema, Kind: submission.kind, Correlation: g.newID(), Grant: &cloned}
	if submission.kind != raftstate.InstallAuthority {
		e.Action = cloned.Action
	}
	return g.submit(ctx, g.controls, &request{
		ctx:         ctx,
		correlation: e.Correlation,
		admin:       e,
		change:      submission.change,
		expected:    submission.expected,
		reply:       make(chan response, 1),
	})
}
func (g *Group) authorityReady() error {
	if !g.opts.ManagedAuthority {
		return fault.ErrUnsupported
	}
	if g.rn.BasicStatus().RaftState != raft.StateLeader {
		return fault.ErrNotLeader
	}
	select {
	case <-g.ready:
	default:
		return fault.ErrReplicaNotReady
	}
	term, err := g.ms.Term(g.applied)
	if g.pending != nil || err != nil || term != g.rn.BasicStatus().Term || !g.currentTermCommitted() {
		return fault.ErrAuthorityPending
	}
	return nil
}
func (g *Group) handleAuthority(req *request) {
	err := req.ctx.Err()
	if err == nil {
		err = g.authorityReady()
	}
	if err == nil {
		err = g.validateAuthorityRequest(*req.admin.Grant)
	}
	if err != nil {
		answer(req, Result{}, err)
		return
	}
	e := req.admin
	if e.Kind == raftstate.ObserveEffect || e.Kind == raftstate.TransferIntent {
		g.handleAuthorizedEffect(req)
		return
	}
	if e.Kind == raftstate.ManagedConfiguration {
		g.handleAuthorizedConfiguration(req)
		return
	}
	if e.Kind != raftstate.InstallAuthority {
		answer(req, Result{}, fault.ErrUnsupported)
		return
	}
	g.handleAuthorityInstall(req)
}

func (g *Group) handleAuthorityInstall(req *request) {
	e := req.admin
	if old := g.authority.Grant; old != nil {
		if e.Grant.Key.Revision < old.Key.Revision || e.Grant.Key == old.Key && !e.Grant.Equal(*old) {
			answer(req, Result{}, fault.ErrAuthorityFenced)
			return
		}
		if e.Grant.Key == old.Key {
			result, err := authorityResult(g.fenceEvidence())
			answer(req, result, err)
			return
		}
	}
	if g.pendingAdministrativeEntry() {
		answer(req, Result{}, fault.ErrAuthorityPending)
		return
	}
	// Raft drops proposals during native leader transfer. Stop that transient
	// attempt so a verified newer floor can enter the log; the old durable
	// intent remains installed until this entry applies. Pending admin work
	// prevents its
	// resumption while the new floor commits.
	if g.activeTransferTarget() != 0 && g.rn.BasicStatus().LeadTransferee != 0 {
		g.rn.TransferLeader(g.opts.ReplicaID)
	}
	data, err := raftstate.EncodeEntry(*e)
	if err == nil {
		err = g.rn.Propose(data)
	}
	if err != nil {
		answer(req, Result{}, err)
		return
	}
	g.changes[req.correlation] = req
}

// Include retained uncommitted work, not just this leader's in-memory waiters.
func (g *Group) pendingAdministrativeEntry() bool {
	if len(g.changes) != 0 {
		return true
	}
	last, err := g.ms.LastIndex()
	if err != nil {
		return true
	}
	if last <= g.applied {
		return false
	}
	entries, err := g.ms.Entries(g.applied+1, last+1, ^uint64(0))
	if err != nil {
		return true
	}
	for _, entry := range entries {
		if entry.Type != raftpb.EntryNormal {
			return true
		}
		if len(entry.Data) == 0 {
			continue
		}
		e, err := raftstate.DecodeEntry(entry.Data)
		if err != nil || e.Kind != raftstate.Application {
			return true
		}
	}
	return false
}

func (g *Group) fenceEvidence() topology.FenceEvidence {
	return topology.FenceEvidence{
		Key:      g.authority.Grant.Key,
		Resource: topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: g.opts.GroupID},
		Receiver: topology.Identity{
			ClusterID:  g.opts.ClusterID,
			NodeName:   g.opts.NodeName,
			MemberID:   g.opts.MemberID,
			InstanceID: g.opts.InstanceID,
		},
		ReplicaID:    g.opts.ReplicaID,
		AppliedIndex: g.authority.InstallIndex,
		AppliedTerm:  g.authority.InstallTerm,
	}
}
func (g *Group) notifyAuthority(e raftstate.Entry, value any, err error) {
	req := g.changes[e.Correlation]
	if req == nil {
		return
	}
	delete(g.changes, e.Correlation)
	var result Result
	failure := err
	if failure == nil {
		result, failure = authorityResult(value)
	}
	g.adminNotify = append(g.adminNotify, adminReply{req: req, result: result, err: failure})
}

func (g *Group) applyAuthority(entry raftpb.Entry, e raftstate.Entry) error {
	if err := g.validateAuthorityRequest(*e.Grant); err != nil {
		return err
	}
	if e.Kind == raftstate.ObserveEffect || e.Kind == raftstate.TransferIntent {
		return g.applyAuthorizedEffect(entry, e)
	}
	if e.Kind != raftstate.InstallAuthority {
		return fault.ErrUnsupported
	}
	if old := g.authority.Grant; old != nil {
		if e.Grant.Key.Revision < old.Key.Revision {
			g.notifyAuthority(e, nil, fault.ErrAuthorityFenced)
			return nil
		}
		if e.Grant.Key == old.Key {
			if !e.Grant.Equal(*old) {
				return fault.New(fault.Unreadable, "committed authority contradicts the installed grant", nil)
			}
			g.notifyAuthority(e, g.fenceEvidence(), nil)
			return nil
		}
	}
	// Abort Raft's catch-up phase of an earlier transfer. TimeoutNow messages
	// already emitted can still cause an election; a fence cannot retract them.
	if g.activeTransferTarget() != 0 && g.rn.BasicStatus().RaftState == raft.StateLeader {
		g.rn.TransferLeader(g.opts.ReplicaID)
	}
	grant := e.Grant.Clone()
	g.authority = raftstate.AuthorityState{Grant: &grant, InstallIndex: entry.Index, InstallTerm: entry.Term}
	g.transferIssuedTerm = 0
	g.notifyAuthority(e, g.fenceEvidence(), nil)
	return nil
}
