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
// its joining replica has received metadata. Authentication remains
// transport-owned.
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
	readControlErr := g.readControl(ctx, metaengine.QueryRequest{Kind: "authority_proof", AuthorityKey: &request.Key,
		Key: request.CourierNode}, &proof)
	if readControlErr != nil {
		return g.leaderReply(readControlErr)
	}
	if g.memberNode(reader) != header.NodeName &&
		(proof.Grant == nil || !slices.ContainsFunc(proof.Grant.Targets,
			func(target topology.AuthorityTarget) bool {
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
func (g *Group) readAuthorityProof(parent context.Context, key topology.AuthorityKey,
	courier string) (metaengine.AuthorityProof, error) {
	ctxHandle, err := g.beginRPC(parent)
	ctx, release := ctxHandle.Context, ctxHandle.Release
	if err != nil {
		return metaengine.AuthorityProof{}, err
	}
	defer release()
	identity := g.Identity()
	if key.ClusterID != identity.ClusterID || key.Revision == 0 || topology.ValidateID(key.GroupID) != nil {
		return metaengine.AuthorityProof{}, fault.ErrIdentityMismatch
	}
	var proof metaengine.AuthorityProof
	if g.IsLeader() {
		err = g.readControl(ctx, metaengine.QueryRequest{Kind: "authority_proof", AuthorityKey: &key,
			Key: courier}, &proof)
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
	if err := validateAuthorityProof(key, proof); err != nil {
		return metaengine.AuthorityProof{}, err
	}
	return proof, nil
}

func validateAuthorityProof(key topology.AuthorityKey, proof metaengine.AuthorityProof) error {
	if proof.Grant == nil {
		if len(proof.Receipts) != 0 || len(proof.CleanupTargets) != 0 || len(proof.ReleasedHosts) != 0 {
			return fault.ErrUnreadable
		}
		return nil
	}
	if proof.Grant.Key != key || proof.Grant.Validate() != nil {
		return fault.ErrUnreadable
	}
	for _, receipt := range proof.Receipts {
		if receipt.Validate(*proof.Grant) != nil {
			return fault.ErrUnreadable
		}
	}
	if err := validateProofCleanupTargets(key, proof); err != nil {
		return err
	}
	return validateProofReleasedHosts(key, proof)
}

func validateProofCleanupTargets(key topology.AuthorityKey, proof metaengine.AuthorityProof) error {
	seen := make(map[topology.AuthorityTarget]bool)
	for _, target := range proof.CleanupTargets {
		a := proof.Grant.Action
		if a == nil || seen[target] ||
			a.Kind != topology.StepDeleteGroupState && (a.Kind != topology.StepCleanupReplicaState ||
				a.NodeName != target.NodeName || a.ReplicaID != target.ReplicaID) ||
			topology.ValidateLoadID(key.GroupID) != nil || !slices.Contains(proof.Grant.Targets, target) ||
			!slices.Contains(proof.Grant.Resources, topology.AuthorityResource{Kind: topology.AuthorityHost,
				GroupID: key.GroupID, NodeName: target.NodeName}) {
			return fault.ErrUnreadable
		}
		seen[target] = true
	}
	return nil
}

func validateProofReleasedHosts(key topology.AuthorityKey, proof metaengine.AuthorityProof) error {
	seen := make(map[topology.AuthorityTarget]bool)
	for _, target := range proof.ReleasedHosts {
		a := proof.Grant.Action
		if key.GroupID != topology.MetaGroupID || a == nil || a.Kind != topology.StepStopReplica ||
			target.ReplicaID != target.MemberID || seen[target] || !slices.Contains(proof.Grant.Targets, target) ||
			!slices.ContainsFunc(proof.Receipts, func(r topology.EffectReceipt) bool {
				return r.Target == target && r.Resource.Kind == topology.AuthorityHost
			}) {
			return fault.ErrUnreadable
		}
		seen[target] = true
	}
	return nil
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
	addresses := g.authorityReadRoutes(ctx, id)
	data, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	return g.callAuthorityRoutes(ctx, "AuthorityRead", data, addresses)
}

func (g *Group) authorityReadRoutes(ctx context.Context, id topology.Identity) []string {
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
	return g.prependAuthorityLeader(ctx, addresses)
}

func (g *Group) prependAuthorityLeader(ctx context.Context, candidates []string) []string {
	addresses := candidates
	if engine := g.engine.Load(); engine != nil {
		if leader := engine.Status().LeaderID; leader != 0 && leader != g.Identity().MemberID {
			if address, err := g.resolveMember(ctx, leader); err == nil {
				addresses = append([]string{address}, addresses...)
			}
		}
	}
	return addresses
}

func (g *Group) callAuthorityRoutes(ctx context.Context, method string,
	data []byte, candidates []string) ([]byte, error) {
	addresses := candidates
	seen := map[string]bool{}
	// One redirect budget per initial route bounds stale or malicious routing
	// hints.
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
		reply, err := g.transport.CallAddress(attempt, address, method, data)
		cancel()
		if err == nil {
			err = metatransport.ReplyError(reply)
		}
		if err == nil {
			return reply.Data, nil
		}
		if !retryAuthorityRoute(err) {
			return nil, err
		}
		last = err
		if reply != nil && reply.LeaderAddress != "" && errors.Is(err, fault.ErrNotLeader) {
			addresses = append(addresses, reply.LeaderAddress)
		}
	}
	return nil, last
}

// Route failures may try another authenticated endpoint. A refused identity,
// conflicting proof or unsupported payload is never hidden by another route.
func retryAuthorityRoute(err error) bool {
	return errors.Is(err, fault.ErrNotLeader) || errors.Is(err, fault.ErrUnavailable) ||
		errors.Is(err, fault.ErrReplicaNotReady) || errors.Is(err, context.DeadlineExceeded)
}

// ValidateAuthorityCourier binds immutable authority and current courier
// identity
// in one quorum-confirmed read, including after local participation has ended.
func (g *Group) ValidateAuthorityCourier(ctx context.Context, grant topology.AuthorityGrant,
	courier topology.Identity) error {
	identity := g.Identity()
	if courier.ClusterID != identity.ClusterID || courier.NodeName == "" || courier.MemberID == 0 ||
		courier.InstanceID == "" {
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
	if proof.Courier != courier || !proof.Grant.Equal(grant) {
		return fault.ErrIdentityMismatch
	}
	return nil
}

// An authority-scoped identity probe keeps exact host routing available after
// ordinary session deactivation. It never treats native absence as effect
// proof.
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
	if request.Target.NodeName != id.NodeName || request.Grant.Validate() != nil ||
		!slices.Contains(request.Grant.Targets, request.Target) {
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
	courier := topology.Identity{
		ClusterID:  header.ClusterId,
		NodeName:   header.NodeName,
		MemberID:   header.MemberId,
		InstanceID: header.InstanceId,
	}
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
// admission headers continue to name the actual metadata identity, possibly
// zero.
func (g *Group) authorityIdentity() (topology.Identity, error) {
	id := g.Identity()
	if g.opts.ReadmissionIdentity == nil {
		return id, nil
	}
	retained := g.opts.ReadmissionIdentity()
	if retained.ClusterID != id.ClusterID || retained.NodeName != id.NodeName || retained.InstanceID != id.InstanceID ||
		retained.MemberID < id.MemberID {
		return topology.Identity{}, fault.ErrIdentityMismatch
	}
	return retained, nil
}

func (g *Group) authorityPeerIdentity(ctx context.Context, grant topology.AuthorityGrant,
	target topology.AuthorityTarget) (topology.Identity, error) {
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
