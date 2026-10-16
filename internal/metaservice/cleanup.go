package metaservice

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/managed"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// BeginCleanupEffect issues only the exact action already bound to retained debt.
func (g *Group) BeginCleanupEffect(ctx context.Context, id storage.Identity,
	action topology.AuthorizedStep) (topology.AuthorityGrant, error) {
	result, err := g.propose(ctx, metaengine.BeginCleanupEffectProposal{Identity: id,
		Action: action, ProposalID: rand.Text()})
	if err != nil {
		return topology.AuthorityGrant{}, err
	}
	if result.Grant == nil || result.Grant.Key.ClusterID != id.ClusterID || result.Grant.Key.GroupID != id.GroupID ||
		!slices.ContainsFunc(result.Grant.Targets, func(target topology.AuthorityTarget) bool {
			return target.NodeName == id.NodeName && target.ReplicaID == id.ReplicaID
		}) {
		return topology.AuthorityGrant{}, fault.ErrUnreadable
	}
	return g.issuedEffect(ctx, result, action)
}

// ReportCollection remains usable after participation fencing. It reports only
// an already-collected journal receipt; even a settled reply authorizes no effect.
func (g *Group) ReportCollection(parent context.Context,
	receipt authoritystore.CollectionReceipt) (authoritystore.CollectionResult, error) {
	owned, err := g.beginRPC(parent)
	if err != nil {
		return authoritystore.CollectionResult{}, err
	}
	defer owned.Release()
	id, err := g.authorityIdentity()
	if err != nil {
		return authoritystore.CollectionResult{}, err
	}
	if err := validateCollectionSender(receipt, id); err != nil {
		return authoritystore.CollectionResult{}, err
	}
	data, err := g.callCleanupControl(owned.Context, collectionRequest{
		Kind: collectionReportKind, Receipt: receipt, ReporterMemberID: id.MemberID})
	if err != nil {
		return authoritystore.CollectionResult{}, err
	}
	var result authoritystore.CollectionResult
	if err := decodeCleanupRequest(data, &result); err != nil {
		return authoritystore.CollectionResult{}, fault.ErrUnreadable
	}
	if result.AppliedIndex == 0 || !result.Receipt.Equal(receipt) {
		return authoritystore.CollectionResult{}, fault.ErrUnreadable
	}
	return result, nil
}

// ReportCleanupStatus sends advisory retention diagnostics through the same restricted route.
func (g *Group) ReportCleanupStatus(parent context.Context, status managed.CleanupStatus) error {
	owned, err := g.beginRPC(parent)
	if err != nil {
		return err
	}
	defer owned.Release()
	id, err := g.authorityIdentity()
	if err != nil {
		return err
	}
	if status.Identity.ClusterID != id.ClusterID || status.Identity.NodeName != id.NodeName {
		return fault.ErrIdentityMismatch
	}
	message := cleanupStatusRequest{Kind: cleanupStatusKind, ReporterMemberID: id.MemberID,
		Identity: status.Identity, MemberID: status.MemberID, RetirementKey: status.RetirementKey,
		NotBefore: status.NotBefore, Blocked: status.Blocked}
	_, err = g.callCleanupControl(owned.Context, message)
	return err
}

func (g *Group) callCleanupControl(ctx context.Context, request any) ([]byte, error) {
	if g.transport == nil {
		return nil, fault.ErrUnavailable
	}
	data, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	if g.IsLeader() {
		reply := g.metaControlReply(ctx, g.transport.Header(), data)
		return reply.Data, metatransport.ReplyError(reply)
	}
	return g.callAuthorityRoutes(ctx, "MetaControl", data, g.authorityReadRoutes(ctx, g.Identity()))
}
