package metaservice

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/topology"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

type joinIdentityProof struct {
	validation      topology.Identity
	authorityMember uint64
}

func (g *Group) joinIdentityProof(identity topology.Identity) (joinIdentityProof, error) {
	authorityMember := identity.MemberID
	validationIdentity := identity
	if g.opts.ReadmissionIdentity != nil {
		retained := g.opts.ReadmissionIdentity()
		if retained.ClusterID != identity.ClusterID || retained.NodeName != identity.NodeName ||
			retained.InstanceID != identity.InstanceID {
			return joinIdentityProof{}, &fatalAdmissionError{fault.ErrIdentityMismatch}
		}
		if identity.MemberID != 0 && retained.MemberID != identity.MemberID {
			// Retirement/rebind may finish just before the request deadline.
			// Only this still-owned exact grant can explain the temporary
			// split;
			// no old engine or live metadata pin may remain.
			g.mu.RLock()
			pending := g.readmissionGrant
			resume := pending != nil && pending.Previous == identity && pending.Granted == retained && g.store == nil
			g.mu.RUnlock()
			fresh, err := metastore.StoreIsFresh(g.opts.DBPath)
			if err != nil {
				return joinIdentityProof{}, err
			}
			if !resume || !fresh || g.engine.Load() != nil {
				return joinIdentityProof{}, &fatalAdmissionError{fault.ErrIdentityMismatch}
			}
			validationIdentity.MemberID = 0
		}
		authorityMember = retained.MemberID
	}
	return joinIdentityProof{validation: validationIdentity, authorityMember: authorityMember}, nil
}

func (g *Group) requestJoin(ctx context.Context, address string, data []byte) (*metapb.Reply, error) {
	attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
	reply, err := g.transport.CallAddress(attempt, address, "Join", data)
	cancel()
	if err != nil {
		if grpcstatus.Code(err) == codes.PermissionDenied || grpcstatus.Code(err) == codes.Unauthenticated {
			return nil, &fatalAdmissionError{fault.New(fault.IdentityMismatch, "meta admission authentication refused",
				err)}
		}
		return nil, err
	}
	return reply, nil
}

func (g *Group) joinRedirects(ctx context.Context, reply *metapb.Reply, err error) []string {
	var addresses []string
	notLeader := errors.Is(err, fault.ErrNotLeader)
	if notLeader && reply.LeaderAddress != "" {
		addresses = append(addresses, reply.LeaderAddress)
	}
	notLeader = errors.Is(err, fault.ErrNotLeader)
	if notLeader && reply.LeaderId != 0 {
		if leader, resolveErr := g.resolveMember(ctx, reply.LeaderId); resolveErr == nil {
			addresses = append(addresses, leader)
		}
	}
	return addresses
}

func (g *Group) decodeGrantedJoin(data []byte, proof joinIdentityProof) (joinResponse, error) {
	var result joinResponse
	if err := json.Unmarshal(data, &result); err != nil {
		return joinResponse{}, &fatalAdmissionError{fault.New(fault.Unreadable, "invalid admission response", err)}
	}
	if result.Readmitted && result.PreviousMemberID != proof.authorityMember || proof.authorityMember != 0 &&
		proof.authorityMember != result.MemberID &&
		!result.Readmitted {
		return joinResponse{}, &fatalAdmissionError{fault.ErrIdentityMismatch}
	}
	if err := validateJoinGrant(proof.validation, g.rules, result); err != nil {
		return joinResponse{}, &fatalAdmissionError{err}
	}
	return result, nil
}

func (g *Group) acceptJoinResponse(ctx context.Context, identity topology.Identity, result joinResponse) error {
	for _, member := range result.Members {
		g.mu.Lock()
		g.joinMembers[member.MemberID] = member.Name
		g.mu.Unlock()
		if member.Name != identity.NodeName {
			g.primeAddress(member.Name, member.Address)
		}
	}
	g.joinLeader = result.LeaderAddress
	// A retry already pinned to the granted identity resumes that store.
	// Its admission may still describe the earlier readmission, but
	// quarantining
	// the replacement itself would destroy the lineage we just authorized.
	if result.Readmitted {
		if err := g.prepareReadmission(ctx, identity, result); err != nil {
			return err
		}
	}
	if err := g.collectJoinReceipts(ctx, identity, result); err != nil {
		return err
	}
	granted := identity
	granted.MemberID = result.MemberID
	if err := g.installJoinedEngine(ctx, granted); err != nil {
		return err
	}
	g.joinAdmission = result.Admission
	g.joinReady = result.Existing || result.Admission.State == metaengine.AdmissionCompleted
	g.joinDone = true
	g.mu.Lock()
	g.readmissionGrant = nil
	g.mu.Unlock()
	return nil
}

func (g *Group) collectJoinReceipts(ctx context.Context, identity topology.Identity, result joinResponse) error {
	if len(result.Grants) == 0 {
		return nil
	}
	if g.opts.ReadmissionReceipts == nil {
		return fault.ErrUnsupported
	}
	payload := g.readmissionPayload(identity, result, nil)
	receipts, err := g.opts.ReadmissionReceipts(ctx, payload)
	if err != nil {
		return err
	}
	if err := validateReadmissionReceipts(result.Grants, receipts); err != nil {
		return err
	}
	g.joinReceipts = receipts
	return nil
}

func (g *Group) installJoinedEngine(ctx context.Context, granted topology.Identity) error {
	g.mu.RLock()
	stoppedMember := g.controlOnly && granted.MemberID == g.identity.MemberID
	g.mu.RUnlock()
	if stoppedMember {
		return fault.ErrAuthorityPending // only a fresh approved member may participate
	}
	if g.engine.Load() == nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := g.openEngine(granted, raftengine.Joining, nil); err != nil {
			return &fatalAdmissionError{err}
		}
	}
	if engine := g.engine.Load(); engine != nil && !engine.Running() {
		if err := engine.Start(g.ctx); err != nil {
			return err
		}
	}
	if g.currentIdentity().MemberID != granted.MemberID {
		return &fatalAdmissionError{fault.ErrIdentityMismatch}
	}
	return nil
}

func (g *Group) joinReadyAddresses(ctx context.Context, engine *raftengine.Group) []string {
	addresses := append([]string{g.joinLeader}, g.opts.JoinSeeds...)
	if leader := engine.RaftStatus().Lead; leader != 0 {
		if address, err := g.resolveMember(ctx, leader); err == nil {
			addresses = append([]string{address}, addresses...)
		}
	}
	return addresses
}
