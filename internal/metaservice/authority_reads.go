package metaservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/topology"
)

type authorityReadRequest struct {
	Key            topology.AuthorityKey `json:"key"`
	CourierNode    string                `json:"courier_node,omitempty"`
	ReaderMemberID uint64                `json:"reader_member_id,omitempty"`
}

// This route is a read-only proof lookup, not a topology or application query.
// A retained grant target may still request it after native removal or before
// its joining replica has received metadata. Authentication remains transport-owned.
func (g *Group) authorityReadReply(ctx context.Context, header *metapb.Header, data []byte) *metapb.Reply {
	if header.NodeName == "" || header.InstanceId == "" {
		return metatransport.Reply(fault.ErrIdentityMismatch)
	}
	var request authorityReadRequest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil || decoder.Decode(new(any)) != io.EOF {
		return metatransport.Reply(fault.ErrInvalid)
	}
	reader := header.MemberId
	if request.ReaderMemberID != 0 {
		if request.ReaderMemberID < reader {
			return metatransport.Reply(fault.ErrIdentityMismatch)
		}
		reader = request.ReaderMemberID
	}
	if reader == 0 {
		return metatransport.Reply(fault.ErrIdentityMismatch)
	}
	if !g.IsLeader() {
		return g.leaderReply(fault.ErrNotLeader)
	}
	var proof metaengine.AuthorityProof
	if err := g.readControl(ctx, metaengine.QueryRequest{Kind: "authority_proof", AuthorityKey: &request.Key, Key: request.CourierNode}, &proof); err != nil {
		return g.leaderReply(err)
	}
	if g.memberNode(reader) != header.NodeName && (proof.Grant == nil || !slices.ContainsFunc(proof.Grant.Targets, func(target topology.AuthorityTarget) bool {
		return target.NodeName == header.NodeName && target.MemberID == reader
	})) {
		return metatransport.Reply(fault.ErrIdentityMismatch)
	}
	encoded, err := json.Marshal(proof)
	if err != nil {
		return metatransport.Reply(err)
	}
	return &metapb.Reply{Data: encoded}
}

// readAuthorityProof remains usable for this exact protocol owner after its
// participation fence. It cannot reopen a stopped owner or redirect an ordinary
// public read. The remote leader performs one safe query for grant and courier.
func (g *Group) readAuthorityProof(ctx context.Context, key topology.AuthorityKey, courier string) (metaengine.AuthorityProof, error) {
	ctx, release, err := g.beginRPC(ctx)
	if err != nil {
		return metaengine.AuthorityProof{}, err
	}
	defer release()
	if key.ClusterID != g.Identity().ClusterID || key.Revision == 0 || topology.ValidateID(key.GroupID) != nil {
		return metaengine.AuthorityProof{}, fault.ErrIdentityMismatch
	}
	var proof metaengine.AuthorityProof
	if g.IsLeader() {
		err = g.readControl(ctx, metaengine.QueryRequest{Kind: "authority_proof", AuthorityKey: &key, Key: courier}, &proof)
	} else {
		var encoded []byte
		encoded, err = g.callAuthorityReader(ctx, authorityReadRequest{Key: key, CourierNode: courier})
		if err == nil && json.Unmarshal(encoded, &proof) != nil {
			err = fault.ErrUnreadable
		}
	}
	if err != nil {
		return metaengine.AuthorityProof{}, err
	}
	if proof.Grant != nil {
		if proof.Grant.Key != key || proof.Grant.Validate() != nil {
			return metaengine.AuthorityProof{}, fault.ErrUnreadable
		}
		for _, receipt := range proof.Receipts {
			if receipt.Validate(*proof.Grant) != nil {
				return metaengine.AuthorityProof{}, fault.ErrUnreadable
			}
		}
		seen := make(map[topology.AuthorityTarget]bool)
		for _, target := range proof.CleanupTargets {
			a := proof.Grant.Action
			if a == nil || topology.ValidateLoadID(key.GroupID) != nil || seen[target] || !slices.Contains(proof.Grant.Targets, target) ||
				!slices.Contains(proof.Grant.Resources, topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: key.GroupID, NodeName: target.NodeName}) ||
				a.Kind != topology.StepDeleteGroupState && (a.Kind != topology.StepCleanupReplicaState || a.NodeName != target.NodeName || a.ReplicaID != target.ReplicaID) {
				return metaengine.AuthorityProof{}, fault.ErrUnreadable
			}
			seen[target] = true
		}
		clear(seen)
		for _, target := range proof.ReleasedHosts {
			a := proof.Grant.Action
			if key.GroupID != topology.MetaGroupID || a == nil || a.Kind != topology.StepStopReplica || target.ReplicaID != target.MemberID || seen[target] || !slices.Contains(proof.Grant.Targets, target) ||
				!slices.ContainsFunc(proof.Receipts, func(r topology.EffectReceipt) bool {
					return r.Target == target && r.Resource.Kind == topology.AuthorityHost
				}) {
				return metaengine.AuthorityProof{}, fault.ErrUnreadable
			}
			seen[target] = true
		}
	} else if len(proof.Receipts) != 0 || len(proof.CleanupTargets) != 0 || len(proof.ReleasedHosts) != 0 {
		return metaengine.AuthorityProof{}, fault.ErrUnreadable
	}
	return proof, nil
}

func (g *Group) callAuthorityReader(ctx context.Context, request authorityReadRequest) ([]byte, error) {
	if g.transport == nil {
		return nil, fault.ErrUnavailable
	}
	id, err := g.authorityIdentity()
	if err != nil {
		return nil, err
	}
	request.ReaderMemberID = id.MemberID
	g.mu.RLock()
	addresses := slices.Clone(g.opts.JoinSeeds)
	for name, address := range g.addresses {
		if name != g.identity.NodeName {
			addresses = append(addresses, address)
		}
	}
	g.mu.RUnlock()
	// A retained removed replica may have no leader hint or joining seeds, and
	// no longer receives Raft traffic that would refresh either. Its approved
	// local catalog still supplies candidate names for the caller's explicit
	// resolver. These are routes only; the remote leader validates the proof.
	owners, _ := g.state().MemberOwners()
	var names []string
	for _, name := range owners {
		if name != id.NodeName {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	for _, name := range slices.Compact(names) {
		if address, err := g.resolveNode(ctx, name); err == nil && address != "" {
			addresses = append(addresses, address)
		}
	}
	slices.Sort(addresses)
	addresses = slices.Compact(addresses)
	if engine := g.engine.Load(); engine != nil {
		if leader := engine.Status().LeaderID; leader != 0 && leader != g.Identity().MemberID {
			if address, err := g.resolveMember(ctx, leader); err == nil {
				addresses = append([]string{address}, addresses...)
			}
		}
	}
	data, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	// One redirect budget per initial route bounds stale or malicious routing hints.
	limit := 2 * len(addresses)
	last := error(fault.ErrUnavailable)
	for i := 0; i < len(addresses) && i < limit; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		address := addresses[i]
		if address == "" || seen[address] {
			continue
		}
		seen[address] = true
		attempt, cancel := context.WithTimeout(ctx, g.opts.Timing.DialTimeout)
		reply, err := g.transport.CallAddress(attempt, address, "AuthorityRead", data)
		cancel()
		if err != nil {
			last = err
			continue
		}
		if err = metatransport.ReplyError(reply); err == nil {
			return reply.Data, nil
		}
		last = err
		if errors.Is(err, fault.ErrNotLeader) && reply.LeaderAddress != "" {
			addresses = append(addresses, reply.LeaderAddress)
		}
	}
	return nil, last
}

// ValidateAuthorityCourier binds immutable authority and current courier identity
// in one quorum-confirmed read, including after local participation has ended.
func (g *Group) ValidateAuthorityCourier(ctx context.Context, grant topology.AuthorityGrant, courier topology.Identity) error {
	if courier.ClusterID != g.Identity().ClusterID || courier.NodeName == "" || courier.MemberID == 0 || courier.InstanceID == "" {
		return fault.ErrIdentityMismatch
	}
	if err := grant.Validate(); err != nil {
		return err
	}
	proof, err := g.readAuthorityProof(ctx, grant.Key, courier.NodeName)
	if err != nil {
		return err
	}
	if proof.Grant == nil {
		return fault.ErrOperatorAuthorization
	}
	if !proof.Grant.Equal(grant) || proof.Courier != courier {
		return fault.ErrIdentityMismatch
	}
	return nil
}

// An authority-scoped identity probe keeps exact host routing available after
// ordinary session deactivation. It never treats native absence as effect proof.
type authorityIdentityRequest struct {
	Grant   *topology.AuthorityGrant `json:"grant,omitempty"`
	Target  topology.AuthorityTarget `json:"target,omitempty"`
	Process *processIdentityRequest  `json:"process,omitempty"`
}

func (g *Group) authorityIdentityReply(ctx context.Context, header *metapb.Header, data []byte) *metapb.Reply {
	var request authorityIdentityRequest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil || decoder.Decode(new(any)) != io.EOF {
		return metatransport.Reply(fault.ErrInvalid)
	}
	if request.Process != nil {
		if request.Grant != nil || request.Target != (topology.AuthorityTarget{}) {
			return metatransport.Reply(fault.ErrInvalid)
		}
		return g.processIdentityReply(*request.Process)
	}
	if request.Grant == nil {
		return metatransport.Reply(fault.ErrInvalid)
	}
	id, err := g.authorityIdentity()
	if err != nil {
		return metatransport.Reply(err)
	}
	if request.Grant.Validate() != nil || request.Target.NodeName != id.NodeName || !slices.Contains(request.Grant.Targets, request.Target) {
		return metatransport.Reply(fault.ErrIdentityMismatch)
	}
	// The authenticated endpoint can be listening before Join has installed
	// its first durable member pin. This is not a conflicting retained owner;
	// prepare must wait for acquisition rather than fail the approved plan.
	if id.MemberID == 0 {
		return metatransport.Reply(fault.ErrReplicaNotReady)
	}
	if request.Target.MemberID != id.MemberID {
		return metatransport.Reply(fault.ErrIdentityMismatch)
	}
	courier := topology.Identity{ClusterID: header.ClusterId, NodeName: header.NodeName, MemberID: header.MemberId, InstanceID: header.InstanceId}
	if err := g.ValidateAuthorityCourier(ctx, *request.Grant, courier); err != nil {
		return metatransport.Reply(err)
	}
	encoded, err := json.Marshal(id)
	if err != nil {
		return metatransport.Reply(err)
	}
	return &metapb.Reply{Data: encoded}
}

// Only grant-scoped control uses the retained authority pin. Ordinary Raft and
// admission headers continue to name the actual metadata identity, possibly zero.
func (g *Group) authorityIdentity() (topology.Identity, error) {
	id := g.Identity()
	if g.opts.ReadmissionIdentity == nil {
		return id, nil
	}
	retained := g.opts.ReadmissionIdentity()
	if retained.ClusterID != id.ClusterID || retained.NodeName != id.NodeName || retained.InstanceID != id.InstanceID || retained.MemberID < id.MemberID {
		return topology.Identity{}, fault.ErrIdentityMismatch
	}
	return retained, nil
}

func (g *Group) authorityPeerIdentity(ctx context.Context, grant topology.AuthorityGrant, target topology.AuthorityTarget) (topology.Identity, error) {
	address, err := g.resolveNode(ctx, target.NodeName)
	if err != nil {
		return topology.Identity{}, err
	}
	data, err := json.Marshal(authorityIdentityRequest{Grant: &grant, Target: target})
	if err != nil {
		return topology.Identity{}, err
	}
	reply, err := g.transport.CallAddress(ctx, address, "IdentityPing", data)
	if err != nil {
		return topology.Identity{}, err
	}
	if err = metatransport.ReplyError(reply); err != nil {
		return topology.Identity{}, err
	}
	var id topology.Identity
	if json.Unmarshal(reply.Data, &id) != nil {
		return topology.Identity{}, fault.ErrUnreadable
	}
	return id, nil
}
