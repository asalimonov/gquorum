package metaservice

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/collection"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
)

type joinRequest struct {
	Identity          topology.Identity `json:"identity"`
	Node              topology.Node     `json:"node"`
	Address           string            `json:"address"`
	AuthorityMemberID uint64            `json:"authority_member_id"`
	LastDocVersion    uint64            `json:"last_doc_version"`
}
type joinMember struct {
	Name, Address string
	MemberID      uint64
}
type joinResponse struct {
	Rules             topology.Rules             `json:"rules"`
	MemberID          uint64                     `json:"member_id"`
	Existing          bool                       `json:"existing"`
	Admission         metaengine.AdmissionRecord `json:"admission"`
	Members           []joinMember               `json:"members"`
	LeaderAddress     string                     `json:"leader_address"`
	Readmitted        bool                       `json:"readmitted"`
	Previous, Granted topology.Document
	PreviousGroups    []topology.Group          `json:"previous_groups,omitempty"`
	PreviousMemberID  uint64                    `json:"previous_member_id,omitempty"`
	Grants            []topology.AuthorityGrant `json:"grants,omitempty"`
}
type readyRequest struct {
	NodeName, InstanceID, Address string
	MemberID                      uint64
	OperatorID                    topology.OperatorID      `json:"ExecutionID"`
	Receipts                      []topology.EffectReceipt `json:"receipts,omitempty"`
}

func (g *Group) primeAddress(name, address string) {
	if name == "" || address == "" {
		return
	}
	if _, _, err := net.SplitHostPort(address); err != nil {
		return
	}
	g.mu.Lock()
	g.addresses[name] = address
	g.mu.Unlock()
	g.refreshAddressHints()
	if primer, ok := g.opts.Network.Resolver.(interface{ Prime(string, string) }); ok {
		primer.Prime(name, address)
	}
}

func (g *Group) handleJoin(ctx context.Context, h *metapb.Header, data []byte) (reply *metapb.Reply) {
	req, err := decodeJoinRequest(h, data)
	if err != nil {
		return metatransport.Reply(err)
	}
	defer func() {
		// A refused copied identity must never replace the healthy holder's
		// route.
		if reply != nil && reply.ErrorCode == "" {
			g.primeAddress(req.Identity.NodeName, req.Address)
			g.parked.clear(req.Identity.NodeName, req.Identity.InstanceID)
		}
	}()
	if !g.IsLeader() {
		return g.leaderReply(fault.ErrNotLeader)
	}
	select {
	case g.admissionToken <- struct{}{}:
		defer func() { <-g.admissionToken }()
	case <-ctx.Done():
		return metatransport.Reply(ctx.Err())
	}
	if !g.IsLeader() {
		return g.leaderReply(fault.ErrNotLeader)
	}
	// Admission decisions must use all committed state, including the current
	// term's leader entry, before classifying an identity or superseding a
	// disk.
	status := g.engine.Load().RaftStatus()
	machine := g.state()
	if !joinClassificationReady(status, machine) {
		return metatransport.Reply(fault.ErrUnavailable)
	}
	return g.admitJoin(ctx, req, machine)
}

// Equal indexes can still belong to a previous term just after election. Wait
// until this leader's committed no-op has reached the durable meta machine.
func joinClassificationReady(status raft.Status, machine *metaengine.Machine) bool {
	index, term := machine.AppliedState()
	return status.RaftState == raft.StateLeader && status.Term > 0 && status.Commit > 0 &&
		status.Applied >= status.Commit && index >= status.Commit && term >= status.Term
}

func hasAdmissionSteps(plan topology.Plan, desired topology.Document, name string, id uint64) bool {
	if meta := desired.MetaGroup(); meta != nil {
		for _, r := range meta.Replicas {
			if r.NodeName == name && r.ReplicaID == id {
				return topology.ValidAdmissionSequence(plan.Steps, name, id, r.Role)
			}
		}
	}
	return false
}

func (g *Group) membershipReply(ctx context.Context) joinResponse {
	response := joinResponse{Rules: g.rules, LeaderAddress: g.opts.Network.AdvertiseAddress}
	views := g.state().Views()
	target, active := views.Target, views.Active
	response.Granted = target
	response.Previous = active
	owners := map[uint64]string{}
	for _, doc := range []topology.Document{active, target} {
		if group := doc.MetaGroup(); group != nil {
			for _, r := range group.Replicas {
				owners[r.ReplicaID] = r.NodeName
			}
		}
	}
	for id, name := range owners {
		address, _ := g.resolveNode(ctx, name)
		response.Members = append(response.Members, joinMember{Name: name, MemberID: id, Address: address})
	}
	slices.SortFunc(response.Members, func(a, b joinMember) int { return cmp.Compare(a.MemberID, b.MemberID) })
	return response
}

func (g *Group) joinReply(ctx context.Context, a metaengine.AdmissionRecord, former uint64) *metapb.Reply {
	response := g.membershipReply(ctx)
	response.Admission = a
	response.MemberID = a.MemberID
	grants, err := g.state().ReadmissionGrants(ctx, a)
	if err != nil {
		return metatransport.Reply(err)
	}
	grants, refusal := g.checkJoinFormer(ctx, &response, former, grants)
	if refusal != nil {
		return refusal
	}
	if len(grants) != 0 {
		response.Grants = grants
		response.PreviousMemberID = grants[0].Targets[0].MemberID
		catalog, err := g.state().RecoveryCatalog(ctx)
		if err != nil {
			return metatransport.Reply(err)
		}
		response.PreviousGroups = readmissionGroups(grants, catalog)
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return metatransport.Reply(err)
	}
	return &metapb.Reply{Data: encoded}
}

func (g *Group) handleReady(ctx context.Context, h *metapb.Header, data []byte) *metapb.Reply {
	var request readyRequest
	decodeErr := json.Unmarshal(data, &request)
	if decodeErr != nil || request.NodeName != h.NodeName || request.MemberID != h.MemberId ||
		request.InstanceID == "" || request.InstanceID != h.InstanceId || request.OperatorID == "" {
		return metatransport.Reply(fault.ErrInvalid)
	}
	if _, _, err := net.SplitHostPort(request.Address); err != nil {
		return metatransport.Reply(fault.ErrInvalid)
	}
	if len(request.Receipts) != 0 {
		_, err := g.propose(ctx, metaengine.RecordReadmissionReceiptsProposal{
			Admission: metaengine.AdmissionRecord{
				NodeName:   request.NodeName,
				InstanceID: request.InstanceID,
				MemberID:   request.MemberID,
				OperatorID: request.OperatorID,
			},
			Receipts: request.Receipts,
			At:       time.Now().UTC(),
		})
		if err != nil {
			return g.leaderReply(err)
		}
	}
	_, err := g.propose(ctx, metaengine.ReadyAdmissionProposal{
		NodeName:   request.NodeName,
		InstanceID: request.InstanceID,
		MemberID:   request.MemberID,
		OperatorID: request.OperatorID,
	})
	if err == nil {
		g.primeAddress(request.NodeName, request.Address)
		g.controller.Signal()
	}
	return g.leaderReply(err)
}

func (g *Group) tryJoin(ctx context.Context) error {
	identity := g.currentIdentity()
	lastVersion := g.state().Latest().Version
	g.mu.RLock()
	lastVersion = max(lastVersion, g.recoveryVersion)
	g.mu.RUnlock()
	proof, err := g.joinIdentityProof(identity)
	if err != nil {
		return err
	}
	data, _ := json.Marshal(joinRequest{
		Identity:          identity,
		AuthorityMemberID: proof.authorityMember,
		Node:              g.opts.Node,
		Address:           g.opts.Network.AdvertiseAddress,
		LastDocVersion:    lastVersion,
	})
	addresses := append([]string(nil), g.opts.JoinSeeds...)
	if g.joinLeader != "" {
		addresses = append([]string{g.joinLeader}, addresses...)
	}
	seen := map[string]bool{}
	var last error = fault.ErrUnavailable
	for i := 0; i < len(addresses); i++ {
		address := addresses[i]
		if seen[address] {
			continue
		}
		seen[address] = true
		reply, err := g.requestJoin(ctx, address, data)
		var fatal *fatalAdmissionError
		if errors.As(err, &fatal) {
			return err
		}
		if err != nil {
			last = err
			continue
		}
		if err = metatransport.ReplyError(reply); err != nil {
			// A live name fence, incompatible type or other immutable identity
			// refusal cannot become valid by retrying this process. The leader
			// keeps its exact-process warning; admission/availability refusals
			// still park.
			if errors.Is(err, fault.ErrIdentityMismatch) {
				return &fatalAdmissionError{err}
			}
			last = err
			addresses = append(addresses, g.joinRedirects(ctx, reply, err)...)
			continue
		}
		result, err := g.decodeGrantedJoin(reply.Data, proof)
		if err != nil {
			return err
		}
		return g.acceptJoinResponse(ctx, identity, result)
	}
	return last
}

func (g *Group) prepareReadmission(ctx context.Context, previous topology.Identity, result joinResponse) error {
	if engine := g.engine.Load(); engine != nil {
		if err := engine.Stop(ctx); err != nil {
			return err
		}
		g.engine.Store(nil)
	}
	// Include replay completed since openEngine inspected the materialized
	// state.
	var appliedCatalog []topology.Group
	if machine := g.state(); machine != nil {
		var err error
		appliedCatalog, err = machine.RecoveryCatalog(ctx)
		if err != nil {
			return err
		}
	}
	g.mu.Lock()
	store := g.store
	g.mu.Unlock()
	if store != nil {
		if err := store.Close(); err != nil {
			return err
		}
		g.mu.Lock()
		if g.store == store {
			g.store = nil
		}
		g.mu.Unlock()
	}
	payload := g.readmissionPayload(previous, result, appliedCatalog)
	owned := cloneReadmission(payload)
	g.mu.Lock()
	g.readmissionGrant = &owned
	g.mu.Unlock()
	if g.opts.OnReadmission == nil {
		return &ReadmissionRequiredError{Readmission: payload}
	}
	// Node's managed host owns resource retirement. Keep the old database
	// closed and retain the authenticated grant across retries until the new
	// pin opens.
	if err := g.opts.OnReadmission(ctx, cloneReadmission(payload)); err != nil {
		return err // owned retirement failures may resume the same durable intent
	}
	if fresh, err := metastore.StoreIsFresh(g.opts.DBPath); err != nil || !fresh {
		return fault.New(fault.ReadmissionRequired,
			"caller readmission hook did not quarantine the previous meta lineage", err)
	}
	return nil
}

func cloneReadmission(in Readmission) Readmission {
	in.PreviousGroups = (topology.Document{Groups: in.PreviousGroups}).Clone().Groups
	in.GrantedGroups = (topology.Document{Groups: in.GrantedGroups}).Clone().Groups
	in.Grants = slices.Clone(in.Grants)
	for i := range in.Grants {
		in.Grants[i] = in.Grants[i].Clone()
	}
	return in
}

// AuthorizeReadmission checks the exact grant delivered by the authenticated
// Join protocol. It is a private Node composition boundary, never an RPC.
func (g *Group) AuthorizeReadmission(ctx context.Context, grant Readmission) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.readmissionGrant == nil || !reflect.DeepEqual(*g.readmissionGrant, grant) {
		return fault.ErrOperatorAuthorization
	}
	return nil
}
func nodeGroups(doc topology.Document, name string) []topology.Group {
	owned := doc.Clone()
	var result []topology.Group
	for _, group := range owned.Groups {
		var replicas []topology.Replica
		for _, r := range group.Replicas {
			if r.NodeName == name {
				replicas = append(replicas, r)
			}
		}
		if len(replicas) > 0 {
			group.Replicas = replicas
			result = append(result, group)
		}
	}
	return result
}

func previousNodeGroups(docs []topology.Document, granted topology.Document, name string) []topology.Group {
	grouped := map[string]topology.Group{}
	for _, doc := range docs {
		for _, group := range nodeGroups(doc, name) {
			previous := mergePreviousGroup(grouped[group.ID], group, granted.Group(group.ID))
			if len(previous.Replicas) > 0 {
				grouped[group.ID] = previous
			}
		}
	}
	result := collection.Slice[topology.Group](len(grouped))
	for _, group := range grouped {
		result = append(result, group)
	}
	slices.SortFunc(result, func(a, b topology.Group) int { return cmp.Compare(a.ID, b.ID) })
	return result
}

func (g *Group) acknowledgeJoin(ctx context.Context) error {
	if g.joinReady {
		return nil
	}
	engine := g.engine.Load()
	if engine == nil {
		return fault.ErrUnavailable
	}
	if err := engine.WaitReady(ctx); err != nil {
		return err
	}
	a := g.joinAdmission
	data, _ := json.Marshal(readyRequest{
		NodeName:   a.NodeName,
		InstanceID: a.InstanceID,
		MemberID:   a.MemberID,
		OperatorID: a.OperatorID,
		Address:    g.opts.Network.AdvertiseAddress,
		Receipts:   g.joinReceipts,
	})
	addresses := g.joinReadyAddresses(ctx, engine)
	seen := map[string]bool{}
	var last error = fault.ErrUnavailable
	for i := 0; i < len(addresses); i++ {
		address := addresses[i]
		if address == "" || seen[address] {
			continue
		}
		seen[address] = true
		attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
		reply, err := g.transport.CallAddress(attempt, address, "JoinReady", data)
		cancel()
		if err != nil {
			last = err
			continue
		}
		if err = metatransport.ReplyError(reply); err == nil {
			g.joinReady = true
			g.joinLeader = address
			return nil
		}
		last = err
		notLeader := errors.Is(err, fault.ErrNotLeader)
		if notLeader && reply.LeaderAddress != "" {
			addresses = append(addresses, reply.LeaderAddress)
		}
	}
	return last
}

func (g *Group) refreshAddressHints() {
	if g.transport == nil {
		return
	}
	g.mu.RLock()
	addresses := make(map[string]string, len(g.addresses))
	for name, address := range g.addresses {
		addresses[name] = address
	}
	owners := make(map[uint64]string, len(g.joinMembers))
	for id, name := range g.joinMembers {
		owners[id] = name
	}
	g.mu.RUnlock()
	approved, established := g.state().MemberOwners()
	if established {
		owners = approved
	} else {
		for _, founder := range g.opts.Founders {
			owners[founder.MemberID] = founder.Node.Name
		}
	}
	peers := make([]*metapb.PeerAddress, 0, len(owners))
	for id, name := range owners {
		if address := addresses[name]; address != "" {
			peers = append(peers, &metapb.PeerAddress{NodeName: name, MemberId: id, Address: address})
		}
	}
	slices.SortFunc(peers, func(a, b *metapb.PeerAddress) int { return cmp.Compare(a.MemberId, b.MemberId) })
	g.transport.SetPeerAddresses(peers)
}
func (g *Group) acceptAddressHints(h *metapb.Header) {
	if h == nil || h.MemberId == 0 || g.memberNode(h.MemberId) != h.NodeName {
		return
	}
	own := g.currentIdentity().NodeName
	if h.NodeName != own {
		g.primeAddress(h.NodeName, h.AdvertiseAddress)
	}
	// Only the observed Raft leader relays peer routes. Followers may carry
	// stale
	// caches after a restart; allowing those to overwrite leader-accepted joins
	// would revert a healthy endpoint indefinitely. Direct sender routes above
	// remain useful on every replica, while topology supplies all identities.
	engine := g.engine.Load()
	if engine == nil || engine.RaftStatus().Lead != h.MemberId || g.currentIdentity().MemberID == h.MemberId {
		return
	}
	for _, peer := range h.Peers {
		if peer != nil && peer.NodeName != own && peer.MemberId != 0 && g.memberNode(peer.MemberId) == peer.NodeName {
			g.primeAddress(peer.NodeName, peer.Address)
		}
	}
}

type fatalAdmissionError struct{ error }

func (e *fatalAdmissionError) Unwrap() error { return e.error }
