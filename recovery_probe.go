package gquorum

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/topology"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const recoveryProbeTimeout = 2 * time.Second
const recoveryProbeWorkers = 4

type recoveryProbeOptions struct {
	resolver       topology.AddressResolver
	credential     []byte
	clientTLS      *tls.Config
	allowPlaintext bool
	advertise      string
}

func copyRecoveryProbe(options MetaNetworkOptions) recoveryProbeOptions {
	result := recoveryProbeOptions{resolver: options.Resolver, credential: bytes.Clone(options.ClusterCredential),
		allowPlaintext: options.AllowPlaintext, advertise: options.AdvertiseAddress}
	if options.ClientTLS != nil {
		result.clientTLS = options.ClientTLS.Clone()
	}
	return result
}

func (r *Recovery) probeAccepted(ctx context.Context, accepted *model.AcceptedPlan) error {
	if accepted.Request.Mode != model.Rebuild {
		return nil
	}
	peers := recoveryProbePeers(accepted)
	if len(peers) == 0 {
		return ctx.Err()
	}
	if r.probe.resolver == nil || len(r.probe.credential) == 0 || r.probe.clientTLS == nil && !r.probe.allowPlaintext {
		accepted.Issues = append(accepted.Issues, model.Issue{Code: "peer_probe_unavailable",
			GroupID: topology.MetaGroupID,
			Message: fmt.Sprintf("Old-member probe attempted 0 of %d peers; "+
				"configure resolver, credential and client settings", len(peers)),
			Remedy: model.RemedyInvestigate})
		return ctx.Err()
	}
	observations, err := r.probeMembers(ctx, accepted.Source, peers)
	if err != nil {
		return err
	}
	blocked := false
	unresolved := 0
	for _, issue := range observations {
		if issue != nil {
			accepted.Issues = append(accepted.Issues, *issue)
			if issue.Code == "peer_probe_address_unavailable" {
				unresolved++
			}
			blocked = blocked || issue.Blocking
		}
	}
	if unresolved > 0 {
		accepted.Issues = append(accepted.Issues, model.Issue{Code: "peer_probe_incomplete",
			GroupID: topology.MetaGroupID,
			Message: fmt.Sprintf("Old-member probe attempted %d of %d approved peers; "+
				"unresolved members were not checked", len(peers)-unresolved, len(peers)),
			Remedy: model.RemedyInvestigate})
	}
	if blocked {
		return fault.New(fault.OperationAuthorization, "old_member_reachable", nil)
	}
	return nil
}

func recoveryProbePeers(accepted *model.AcceptedPlan) []topology.Replica {
	meta := accepted.Approved.MetaGroup()
	if meta == nil {
		return nil
	}
	var peers []topology.Replica
	for _, replica := range meta.Replicas {
		if replica.NodeName != accepted.Source.NodeName {
			peers = append(peers, replica)
		}
	}
	return peers
}
func (r *Recovery) probeMembers(ctx context.Context, source topology.Identity,
	peers []topology.Replica) ([]*model.Issue, error) {
	observations := make([]*model.Issue, len(peers))
	next := make(chan int, len(peers))
	for i := range peers {
		next <- i
	}
	close(next)
	var workers sync.WaitGroup
	for range min(recoveryProbeWorkers, len(peers)) {
		workers.Go(func() {
			for i := range next {
				if ctx.Err() != nil {
					return
				}
				observations[i] = r.probeOldMember(ctx, source, peers[i])
			}
		})
	}
	workers.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return observations, nil
}

func (r *Recovery) probeOldMember(parent context.Context, source topology.Identity,
	peer topology.Replica) *model.Issue {
	ctx, cancel := context.WithTimeout(parent, recoveryProbeTimeout)
	defer cancel()
	address, err := r.probe.resolver.Resolve(ctx, peer.NodeName)
	if err != nil || address == "" {
		return probeInformation("peer_probe_address_unavailable", peer.NodeName, "")
	}
	reply, err := metatransport.ProbeIdentity(ctx, metatransport.ProbeOptions{
		Header: &metapb.Header{ClusterId: source.ClusterID, NodeName: source.NodeName, MemberId: source.MemberID,
			InstanceId: "recovery-inspection", AdvertiseAddress: r.probe.advertise}, Address: address,
		ClientTLS:      r.probe.clientTLS,
		AllowPlaintext: r.probe.allowPlaintext, Credential: r.probe.credential})
	if err != nil {
		if status.Code(err) == codes.Unauthenticated || status.Code(err) == codes.PermissionDenied {
			return probeInformation("peer_probe_authentication_rejected", peer.NodeName, address)
		}
		return nil // Transport failures provide no authenticated reachability evidence.
	}
	if reply == nil {
		return nil
	}
	replyErr := metatransport.ReplyError(reply)
	if errors.Is(replyErr, fault.ErrIdentityMismatch) {
		return probeInformation("peer_probe_identity_mismatch", peer.NodeName, address)
	}
	if len(reply.Data) > 0 && replyErr == nil {
		var identity topology.Identity
		if err := json.Unmarshal(reply.Data, &identity); err == nil && identity.ClusterID != source.ClusterID {
			return probeInformation("peer_probe_identity_mismatch", peer.NodeName, address)
		}
	}
	return &model.Issue{Code: "old_member_reachable", GroupID: topology.MetaGroupID, Blocking: true,
		Message: model.CleanDiagnostic(fmt.Sprintf(
			"Old-cluster member %s answered at %s; stop and fence the old deployment",
			peer.NodeName, address), model.HistoryMessageBytes),
		Remedy: model.RemedyInvestigate}
}
func probeInformation(code, node, address string) *model.Issue {
	return &model.Issue{Code: code, GroupID: topology.MetaGroupID,
		Message: model.CleanDiagnostic(fmt.Sprintf("Old-member probe %s for %s at %s", code, node, address),
			model.HistoryMessageBytes), Remedy: model.RemedyInvestigate}
}
