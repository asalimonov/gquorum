package metaservice

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/topology"
)

func (g *Group) cleanupControlReply(ctx context.Context, header *metapb.Header,
	data []byte, kind string) *metapb.Reply {
	if kind != collectionReportKind && kind != cleanupStatusKind {
		return metatransport.Reply(fault.ErrUnsupported)
	}
	if !g.IsLeader() {
		return g.leaderReply(fault.ErrNotLeader)
	}
	if kind == cleanupStatusKind {
		return g.cleanupStatusReply(ctx, header, data)
	}
	result, err := g.collectionAccess().receiveCollection(ctx, header, data)
	if err != nil {
		return g.leaderReply(err)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return metatransport.Reply(err)
	}
	return &metapb.Reply{Data: encoded}
}

func (a collectionReportAccess) receiveCollection(ctx context.Context, header *metapb.Header,
	data []byte) (authoritystore.CollectionResult, error) {
	var request collectionRequest
	if err := decodeCleanupRequest(data, &request); err != nil {
		return authoritystore.CollectionResult{}, err
	}
	if request.Kind != collectionReportKind {
		return authoritystore.CollectionResult{}, fault.ErrInvalid
	}
	courier, err := cleanupCourier(header, request.ReporterMemberID)
	if err != nil {
		return authoritystore.CollectionResult{}, err
	}
	if err := validateCollectionSender(request.Receipt, courier); err != nil {
		return authoritystore.CollectionResult{}, err
	}
	return a.accept(ctx, metaengine.RecordCollectionProposal{Receipt: request.Receipt, Courier: courier,
		ReadmissionKey: request.ReadmissionKey, At: time.Now().UTC(), ProposalID: rand.Text()})
}

// A retained journal may already be rebound while the participation header is
// zero or older. Match AuthorityRead's monotonic member hint; header cluster,
// node and process remain fixed. Retained-chain validation is still mandatory
// for acceptance. A safe absence reply admits nobody and authorizes no effect.
func cleanupCourier(header *metapb.Header, hint uint64) (topology.Identity, error) {
	if header == nil {
		return topology.Identity{}, fault.ErrIdentityMismatch
	}
	if hint != 0 && hint < header.MemberId || strings.TrimSpace(header.ClusterId) == "" ||
		strings.TrimSpace(header.NodeName) == "" || strings.TrimSpace(header.InstanceId) == "" ||
		strings.ContainsRune(header.InstanceId, 0) {
		return topology.Identity{}, fault.ErrIdentityMismatch
	}
	member := max(header.MemberId, hint)
	if member == 0 {
		return topology.Identity{}, fault.ErrIdentityMismatch
	}
	return topology.Identity{ClusterID: header.ClusterId, NodeName: header.NodeName,
		MemberID: member, InstanceID: header.InstanceId}, nil
}

func validateCollectionSender(receipt authoritystore.CollectionReceipt, courier topology.Identity) error {
	if receipt.Identity.ClusterID != courier.ClusterID || receipt.Identity.NodeName != courier.NodeName ||
		courier.MemberID == 0 {
		return fault.ErrIdentityMismatch
	}
	return receipt.Validate()
}

func (g *Group) cleanupStatusReply(ctx context.Context, header *metapb.Header, data []byte) *metapb.Reply {
	return g.leaderReply(g.collectionAccess().receiveStatus(ctx, header, data))
}

func (a collectionReportAccess) receiveStatus(ctx context.Context, header *metapb.Header, data []byte) error {
	var request cleanupStatusRequest
	if err := decodeCleanupRequest(data, &request); err != nil {
		return err
	}
	if request.Kind != cleanupStatusKind {
		return fault.ErrInvalid
	}
	courier, err := cleanupCourier(header, request.ReporterMemberID)
	if err != nil {
		return err
	}
	if request.Identity.ClusterID != courier.ClusterID || request.Identity.NodeName != courier.NodeName ||
		request.MemberID == 0 || request.Identity.ReplicaID == 0 || request.RetirementKey.Revision == 0 ||
		request.RetirementKey.ClusterID != request.Identity.ClusterID ||
		request.RetirementKey.GroupID != request.Identity.GroupID ||
		(topology.ProgressQuery{GroupID: request.Identity.GroupID,
			NodeName: request.Identity.NodeName}).Validate() != nil ||
		request.NotBefore != nil && request.NotBefore.IsZero() ||
		request.Blocked != nil && strings.TrimSpace(request.Blocked.Code) == "" {
		return fault.ErrIdentityMismatch
	}
	check, err := a.check(ctx, request.Identity)
	if err != nil {
		return err
	}
	if check.Untracked {
		return nil // No diagnostic row remains to update.
	}
	if err := a.courier(request.Identity, request.MemberID, courier); err != nil {
		return err
	}
	_, err = a.propose(ctx, metaengine.RecordCleanupStatusProposal{Identity: request.Identity,
		MemberID: request.MemberID, RetirementKey: request.RetirementKey, NotBefore: request.NotBefore,
		Blocked: request.Blocked, At: time.Now().UTC(), ProposalID: rand.Text()})
	return err
}
