package metaservice

import (
	"context"
	"encoding/json"
	"net"
	"reflect"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/topology"
)

func decodeJoinRequest(h *metapb.Header, data []byte) (joinRequest, error) {
	var req joinRequest
	decodeErr := json.Unmarshal(data, &req)
	if decodeErr != nil || req.Identity.NodeName != h.NodeName || req.Identity.ClusterID != h.ClusterId ||
		req.Identity.InstanceID == "" || req.Identity.MemberID != h.MemberId ||
		req.Identity.InstanceID != h.InstanceId ||
		req.Identity.MemberID != 0 && req.AuthorityMemberID < req.Identity.MemberID ||
		req.Node.Name != h.NodeName || req.Node.Type == "" || strings.TrimSpace(req.Identity.NodeName) == "" ||
		strings.ContainsRune(req.Identity.NodeName, 0) || strings.ContainsRune(req.Identity.InstanceID, 0) {
		return joinRequest{}, fault.ErrInvalid
	}
	if _, _, err := net.SplitHostPort(req.Address); err != nil {
		return joinRequest{}, fault.ErrInvalid
	}
	return req, nil
}

type joinClassification struct {
	req     joinRequest
	views   topology.ReadView
	machine *metaengine.Machine
	node    *topology.Node
	member  uint64
	replace bool
}

func (g *Group) admitJoin(ctx context.Context, req joinRequest, machine *metaengine.Machine) *metapb.Reply {
	views := machine.Views()
	target := views.Target
	if target.Version == 0 {
		return metatransport.Reply(fault.ErrUnavailable)
	}
	node := target.Node(req.Node.Name)
	if node == nil || !node.State.Places() {
		kind := parkedUnapproved
		if node != nil && node.State == topology.NodeExcluded {
			kind = parkedExcluded
		}
		return g.refuseJoin(req, kind,
			fault.New(fault.OperatorAuthorization, "node needs an approved topology admission", nil))
	}
	if node.Type != req.Node.Type {
		return metatransport.Reply(fault.New(fault.IdentityMismatch, "node type differs from approved topology", nil))
	}
	if reply := g.revokeConflictingSession(ctx, req, machine); reply != nil {
		return reply
	}
	if admission, ok := machine.Admission(req.Node.Name, req.Identity.InstanceID); ok {
		if admission.State == metaengine.AdmissionSuperseded || admission.State == metaengine.AdmissionRetired {
			return g.refuseJoin(req, parkedSuperseded, fault.New(fault.OperatorAuthorization,
				"process admission was superseded or retired", nil))
		}
		// The exact admitted process may need predecessor host fences before
		// retirement grants can be issued. Its control endpoint must be
		// reachable
		// even when joinReply returns AuthorityPending.
		g.primeAddress(req.Identity.NodeName, req.Address)
		return g.joinReply(ctx, admission, req.AuthorityMemberID)
	}
	state := joinClassification{req: req, views: views, machine: machine, node: node,
		member: target.MemberID(req.Node.Name)}
	if reply := g.classifyLiveAdmission(ctx, &state); reply != nil {
		return reply
	}
	return g.joinWithTopology(ctx, state)
}

func (g *Group) refuseJoin(req joinRequest, kind string, err error) *metapb.Reply {
	g.parked.record(req, kind, err.Error())
	return metatransport.Reply(err)
}

func (g *Group) revokeConflictingSession(ctx context.Context, req joinRequest,
	machine *metaengine.Machine) *metapb.Reply {
	// Probe the previous cached endpoint before replacing it with this
	// request's
	// address. A copied disk does not bypass the live process's name lock.
	if holder, held := machine.LockSession("node/" + req.Node.Name); held &&
		holder.InstanceID != req.Identity.InstanceID {
		if g.probeSession(ctx, holder) {
			return g.refuseJoin(req, parkedNameConflict, fault.New(fault.IdentityMismatch,
				"node name is held by another live process", nil))
		}
		g.leaseGate.Lock()
		_, err := g.propose(ctx, metaengine.RevokeSession{SessionID: holder.SessionID,
			Reason: "stale process during admission"})
		g.leaseGate.Unlock()
		if err != nil {
			return g.leaderReply(err)
		}
	}
	return nil
}

func (g *Group) classifyLiveAdmission(ctx context.Context, state *joinClassification) *metapb.Reply {
	req, machine, member := state.req, state.machine, state.member
	if other, ok := machine.LiveAdmission(req.Node.Name, req.Identity.InstanceID); ok {
		if g.probeAdmission(ctx, other) {
			return g.refuseJoin(req, parkedNameConflict,
				fault.New(fault.IdentityMismatch, "node name has another live admission", nil))
		}
		// An unprepared crash needs a new process-bound ready acknowledgement;
		// rebinding pending admission preserves the exact allocated member ID.
		if other.State == metaengine.AdmissionPending &&
			!containsMember(g.engine.Load().Status().ConfState, other.MemberID) {
			state.replace = true
		} else if req.Identity.MemberID != member {
			return g.refuseJoin(req, parkedUnapproved, fault.New(fault.OperatorAuthorization,
				"entered admission requires fresh identity repair", nil))
		}
	}
	return nil
}

func (g *Group) joinWithTopology(ctx context.Context, state joinClassification) *metapb.Reply {
	req, machine, node, member, replace := state.req, state.machine, state.node, state.member, state.replace
	latest, target, active := state.views.Latest, state.views.Target, state.views.Active

	if member != 0 && req.Identity.MemberID == member && !replace {
		// Ordinary retained-disk restart: no version or membership change.
		response := g.membershipReply(ctx)
		response.MemberID = member
		response.Existing = true
		encoded, _ := json.Marshal(response)
		return &metapb.Reply{Data: encoded}
	}
	if node.State != topology.NodeActive && req.Identity.MemberID != member {
		return g.refuseJoin(req, parkedUnapproved,
			fault.New(fault.OperatorAuthorization, "draining nodes may only restart their retained identity", nil))
	}
	if ex, ok := machine.CurrentOperator(); ok && ex.Status.Version == target.Version && !ex.Status.State.Terminal() &&
		hasAdmissionSteps(ex.Plan, target, req.Node.Name, member) {
		return g.bindJoinAdmission(ctx, req, metaengine.BindAdmissionProposal{
			Admission: metaengine.AdmissionRecord{
				NodeName:   req.Node.Name,
				InstanceID: req.Identity.InstanceID,
				MemberID:   member,
				OperatorID: ex.Status.ID,
			},
			ReplaceInstance: replace,
			At:              time.Now().UTC(),
		})
	}
	if machine.HasActivated(req.Node.Name) && reflect.DeepEqual(latest, target) && reflect.DeepEqual(target, active) {
		return g.commitJoinReadmission(ctx, state)
	}
	return g.joinFoundingNode(ctx, state)
}

func (g *Group) commitJoinReadmission(ctx context.Context, state joinClassification) *metapb.Reply {
	req, latest, active := state.req, state.views.Latest, state.views.Active
	at := time.Now().UTC()
	fresh, err := metaengine.ReadmissionDocument(active, req.Node.Name, at)
	if err != nil {
		return metatransport.Reply(err)
	}
	plan, err := g.PlanFor(ctx, fresh)
	if err != nil {
		return g.leaderReply(err)
	}
	result, err := g.propose(ctx, metaengine.CommitReadmissionProposal{
		Admission: metaengine.AdmissionRecord{
			NodeName:   req.Node.Name,
			InstanceID: req.Identity.InstanceID,
			MemberID:   fresh.MemberID(req.Node.Name),
		},
		Doc:             fresh,
		ExpectedVersion: latest.Version,
		Plan:            plan,
		At:              at,
	})
	if err != nil {
		return g.leaderReply(err)
	}
	g.controller.Signal()
	g.primeAddress(req.Identity.NodeName, req.Address)
	return g.joinReply(ctx, result.Admission, req.AuthorityMemberID)
}

func (g *Group) joinFoundingNode(ctx context.Context, state joinClassification) *metapb.Reply {
	req, machine, member, replace := state.req, state.machine, state.member, state.replace
	identity := req.Identity
	identity.MemberID = member
	founding, ready := machine.AdmissionBasis(identity)
	activated := machine.HasActivated(req.Node.Name)
	if !activated && ready && founding != "" &&
		containsMember(g.engine.Load().Status().ConfState, member) {
		return g.bindJoinAdmission(ctx, req, metaengine.BindAdmissionProposal{
			Admission: metaengine.AdmissionRecord{
				NodeName:   req.Node.Name,
				InstanceID: req.Identity.InstanceID,
				MemberID:   member,
				OperatorID: topology.OperatorID(founding),
			},
			Founding:        true,
			ReplaceInstance: replace,
			At:              time.Now().UTC(),
		})
	}
	return g.refuseJoin(req, parkedUnapproved, fault.New(fault.OperatorAuthorization,
		"node has no current approved learner-first admission", nil))
}

func (g *Group) bindJoinAdmission(ctx context.Context, req joinRequest,
	proposal metaengine.BindAdmissionProposal) *metapb.Reply {
	result, err := g.propose(ctx, proposal)
	if err != nil {
		return g.leaderReply(err)
	}
	g.primeAddress(req.Identity.NodeName, req.Address)
	return g.joinReply(ctx, result.Admission, req.AuthorityMemberID)
}

func (g *Group) checkJoinFormer(ctx context.Context, response *joinResponse, former uint64,
	initial []topology.AuthorityGrant) ([]topology.AuthorityGrant, *metapb.Reply) {
	a, grants := response.Admission, initial
	if former != a.MemberID && (former != 0 || g.state().ReadmissionRequired(a.NodeName, a.MemberID)) {
		if former == 0 {
			return nil, metatransport.Reply(fault.New(fault.ReadmissionRequired,
				"returning node lacks retained authority identity", nil))
		}
		if len(grants) == 0 {
			result, err := g.propose(ctx, metaengine.BeginReadmissionAuthority{
				NodeName:         a.NodeName,
				InstanceID:       a.InstanceID,
				MemberID:         a.MemberID,
				PreviousMemberID: former,
				OperatorID:       a.OperatorID,
			})
			if err != nil {
				return nil, g.leaderReply(err)
			}
			grants = result.Grants
		}
		if len(grants) == 0 || grants[0].Targets[0].MemberID != former {
			return nil, metatransport.Reply(fault.ErrIdentityMismatch)
		}
		response.Readmitted = true
	}
	return grants, nil
}
