package meta

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type joinRequest struct {
	Identity       Identity      `json:"identity"`
	Node           topology.Node `json:"node"`
	Address        string        `json:"address"`
	LastDocVersion uint64        `json:"last_doc_version"`
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
}
type readyRequest struct {
	NodeName, InstanceID, Address string
	MemberID                      uint64
	ExecutionID                   topology.ExecutionID
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
	var req joinRequest
	if json.Unmarshal(data, &req) != nil || strings.TrimSpace(req.Identity.NodeName) == "" || strings.ContainsRune(req.Identity.NodeName, 0) || strings.ContainsRune(req.Identity.InstanceID, 0) || req.Identity.NodeName != h.NodeName || req.Identity.ClusterID != h.ClusterId || req.Identity.InstanceID == "" || req.Identity.MemberID != h.MemberId || req.Node.Name != h.NodeName || req.Node.Type == "" {
		return metatransport.Reply(fault.ErrInvalid)
	}
	if _, _, err := net.SplitHostPort(req.Address); err != nil {
		return metatransport.Reply(fault.ErrInvalid)
	}
	defer func() {
		// A refused copied identity must never replace the healthy holder's route.
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
	// term's leader entry, before classifying an identity or superseding a disk.
	status := g.engine.Load().RaftStatus()
	machine := g.state()
	if !joinClassificationReady(status, machine) {
		return metatransport.Reply(fault.ErrUnavailable)
	}
	latest, target, active := machine.Views()
	if target.Version == 0 {
		return metatransport.Reply(fault.ErrUnavailable)
	}
	refuse := func(kind string, err error) *metapb.Reply {
		g.parked.record(req, kind, err.Error())
		return metatransport.Reply(err)
	}
	node := target.Node(req.Node.Name)
	if node == nil || !node.State.Places() {
		kind := parkedUnapproved
		if node != nil && node.State == topology.NodeExcluded {
			kind = parkedExcluded
		}
		return refuse(kind, fault.New(fault.ExecutionAuthorization, "node needs an approved topology admission", nil))
	}
	if node.Type != req.Node.Type {
		return metatransport.Reply(fault.New(fault.IdentityMismatch, "node type differs from approved topology", nil))
	}
	// Probe the previous cached endpoint before replacing it with this request's
	// address. A copied disk does not bypass the live process's name lock.
	if holder, held := machine.LockSession("node/" + req.Node.Name); held && holder.InstanceID != req.Identity.InstanceID {
		if g.probeSession(ctx, holder) {
			return refuse(parkedNameConflict, fault.New(fault.IdentityMismatch, "node name is held by another live process", nil))
		}
		g.leaseGate.Lock()
		_, err := g.propose(ctx, metaengine.RevokeSession{SessionID: holder.SessionID, Reason: "stale process during admission"})
		g.leaseGate.Unlock()
		if err != nil {
			return g.leaderReply(err)
		}
	}
	if admission, ok := machine.Admission(req.Node.Name, req.Identity.InstanceID); ok {
		if admission.State == metaengine.AdmissionSuperseded || admission.State == metaengine.AdmissionRetired {
			return refuse(parkedSuperseded, fault.New(fault.ExecutionAuthorization, "process admission was superseded or retired", nil))
		}
		return g.joinReply(ctx, admission, false)
	}
	member := target.MemberID(req.Node.Name)
	var replace bool
	if other, ok := machine.LiveAdmission(req.Node.Name, req.Identity.InstanceID); ok {
		if g.probeMember(ctx, other.NodeName, other.MemberID, other.InstanceID) {
			return refuse(parkedNameConflict, fault.New(fault.IdentityMismatch, "node name has another live admission", nil))
		}
		// An unprepared crash needs a new process-bound ready acknowledgement;
		// rebinding pending admission preserves the exact allocated member ID.
		if other.State == metaengine.AdmissionPending && !containsMember(g.engine.Load().Status().ConfState, other.MemberID) {
			replace = true
		} else if req.Identity.MemberID != member {
			return refuse(parkedUnapproved, fault.New(fault.ExecutionAuthorization, "entered admission requires fresh identity repair", nil))
		}
	}

	if member != 0 && req.Identity.MemberID == member && !replace {
		// Ordinary retained-disk restart: no version or membership change.
		response := g.membershipReply(ctx)
		response.MemberID = member
		response.Existing = true
		encoded, _ := json.Marshal(response)
		return &metapb.Reply{Data: encoded}
	}
	if node.State != topology.NodeActive && req.Identity.MemberID != member {
		return refuse(parkedUnapproved, fault.New(fault.ExecutionAuthorization, "draining nodes may only restart their retained identity", nil))
	}
	if ex, ok := machine.CurrentExecution(); ok && !ex.Status.State.Terminal() && ex.Status.Version == target.Version && hasAdmissionSteps(ex.Plan, req.Node.Name, member) {
		result, err := g.propose(ctx, metaengine.BindAdmissionProposal{Admission: metaengine.AdmissionRecord{NodeName: req.Node.Name, InstanceID: req.Identity.InstanceID, MemberID: member, ExecutionID: ex.Status.ID}, ReplaceInstance: replace, At: time.Now().UTC()})
		if err != nil {
			return g.leaderReply(err)
		}
		return g.joinReply(ctx, result.Admission, false)
	}
	if machine.HasActivated(req.Node.Name) && reflect.DeepEqual(latest, target) && reflect.DeepEqual(target, active) {
		at := time.Now().UTC()
		fresh, err := metaengine.ReadmissionDocument(active, req.Node.Name, at)
		if err != nil {
			return metatransport.Reply(err)
		}
		plan, err := g.PlanFor(ctx, fresh)
		if err != nil {
			return g.leaderReply(err)
		}
		result, err := g.propose(ctx, metaengine.CommitReadmissionProposal{Admission: metaengine.AdmissionRecord{NodeName: req.Node.Name, InstanceID: req.Identity.InstanceID, MemberID: fresh.MemberID(req.Node.Name)}, Doc: fresh, ExpectedVersion: latest.Version, Plan: plan, At: at})
		if err != nil {
			return g.leaderReply(err)
		}
		g.convergence.Signal()
		return g.joinReply(ctx, result.Admission, true)
	}
	if !machine.HasActivated(req.Node.Name) && reflect.DeepEqual(target, active) && target.Origin == topology.OriginFounding && containsMember(g.engine.Load().Status().ConfState, member) {
		ex, ok := machine.CurrentExecution()
		if !ok || ex.Status.State != topology.ExecutionSucceeded {
			return metatransport.Reply(fault.ErrUnavailable)
		}
		result, err := g.propose(ctx, metaengine.BindAdmissionProposal{Admission: metaengine.AdmissionRecord{NodeName: req.Node.Name, InstanceID: req.Identity.InstanceID, MemberID: member, ExecutionID: ex.Status.ID}, Founding: true, ReplaceInstance: replace, At: time.Now().UTC()})
		if err != nil {
			return g.leaderReply(err)
		}
		return g.joinReply(ctx, result.Admission, false)
	}
	return refuse(parkedUnapproved, fault.New(fault.ExecutionAuthorization, "node has no current approved learner-first admission", nil))
}

// Equal indexes can still belong to a previous term just after election. Wait
// until this leader's committed no-op has reached the durable meta machine.
func joinClassificationReady(status raft.Status, machine *metaengine.Machine) bool {
	index, term := machine.AppliedState()
	return status.RaftState == raft.StateLeader && status.Term > 0 && status.Commit > 0 &&
		status.Applied >= status.Commit && index >= status.Commit && term >= status.Term
}
func hasAdmissionSteps(plan topology.Plan, name string, id uint64) bool {
	wanted := []topology.StepKind{topology.StepPrepareReplica, topology.StepAddLearner, topology.StepWaitCaughtUp, topology.StepPromoteVoter}
	for i := 0; i+len(wanted) <= len(plan.Steps); i++ {
		match := true
		for j, kind := range wanted {
			s := plan.Steps[i+j]
			match = match && s.Kind == kind && s.GroupID == topology.MetaGroupID && s.NodeName == name && s.ReplicaID == id
		}
		if match {
			return true
		}
	}
	return false
}
func (g *Group) membershipReply(ctx context.Context) joinResponse {
	response := joinResponse{Rules: g.rules, LeaderAddress: g.opts.Network.AdvertiseAddress}
	_, target, active := g.state().Views()
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
	sort.Slice(response.Members, func(i, j int) bool { return response.Members[i].MemberID < response.Members[j].MemberID })
	return response
}
func (g *Group) joinReply(ctx context.Context, a metaengine.AdmissionRecord, readmitted bool) *metapb.Reply {
	response := g.membershipReply(ctx)
	response.Admission = a
	response.MemberID = a.MemberID
	response.Readmitted = readmitted
	if ex, ok := g.state().ExecutionRecord(a.ExecutionID); ok {
		for _, step := range ex.Plan.Steps {
			if step.Kind == topology.StepRemoveReplica && step.GroupID == topology.MetaGroupID && step.NodeName == a.NodeName && step.ReplicaID != a.MemberID {
				response.Readmitted = true
				if old, ok := g.state().Get(ex.Status.Version - 1); ok {
					response.Previous = old
				}
				break
			}
		}
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return metatransport.Reply(err)
	}
	return &metapb.Reply{Data: encoded}
}
func (g *Group) handleReady(ctx context.Context, h *metapb.Header, data []byte) *metapb.Reply {
	var request readyRequest
	if json.Unmarshal(data, &request) != nil || request.NodeName != h.NodeName || request.MemberID != h.MemberId || request.InstanceID == "" || request.ExecutionID == "" {
		return metatransport.Reply(fault.ErrInvalid)
	}
	if _, _, err := net.SplitHostPort(request.Address); err != nil {
		return metatransport.Reply(fault.ErrInvalid)
	}
	_, err := g.propose(ctx, metaengine.ReadyAdmissionProposal{NodeName: request.NodeName, InstanceID: request.InstanceID, MemberID: request.MemberID, ExecutionID: request.ExecutionID})
	if err == nil {
		g.primeAddress(request.NodeName, request.Address)
		g.convergence.Signal()
	}
	return g.leaderReply(err)
}

func (g *Group) tryJoin(ctx context.Context) error {
	identity := g.currentIdentity()
	lastVersion := g.state().Latest().Version
	g.mu.RLock()
	for _, doc := range g.recoveryDocuments {
		lastVersion = max(lastVersion, doc.Version)
	}
	g.mu.RUnlock()
	data, _ := json.Marshal(joinRequest{Identity: identity, Node: g.opts.Node, Address: g.opts.Network.AdvertiseAddress, LastDocVersion: lastVersion})
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
		attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
		reply, err := g.transport.CallAddress(attempt, address, "Join", data)
		cancel()
		if err != nil {
			if status.Code(err) == codes.PermissionDenied || status.Code(err) == codes.Unauthenticated {
				return &fatalAdmissionError{fault.New(fault.IdentityMismatch, "meta admission authentication refused", err)}
			}
			last = err
			continue
		}
		if err = metatransport.ReplyError(reply); err != nil {
			// A live name fence, incompatible type or other immutable identity
			// refusal cannot become valid by retrying this process. The leader keeps
			// its exact-process warning; admission/availability refusals still park.
			if errors.Is(err, fault.ErrIdentityMismatch) {
				return &fatalAdmissionError{err}
			}
			last = err
			if errors.Is(err, fault.ErrNotLeader) && reply.LeaderAddress != "" {
				addresses = append(addresses, reply.LeaderAddress)
			}
			if errors.Is(err, fault.ErrNotLeader) && reply.LeaderId != 0 {
				if leader, err := g.resolveMember(ctx, reply.LeaderId); err == nil {
					addresses = append(addresses, leader)
				}
			}
			continue
		}
		var result joinResponse
		if err = json.Unmarshal(reply.Data, &result); err != nil {
			return &fatalAdmissionError{fault.New(fault.Unreadable, "invalid admission response", err)}
		}
		if err = validateJoinGrant(identity, g.rules, result); err != nil {
			return &fatalAdmissionError{err}
		}
		for _, member := range result.Members {
			g.mu.Lock()
			g.joinMembers[member.MemberID] = member.Name
			g.mu.Unlock()
			if member.Name != identity.NodeName {
				g.primeAddress(member.Name, member.Address)
			}
		}
		g.joinLeader = result.LeaderAddress
		// A retry already pinned to the granted identity resumes that store. Its
		// admission may still describe the earlier readmission, but quarantining
		// the replacement itself would destroy the lineage we just authorized.
		if identity.MemberID != result.MemberID && (result.Readmitted || identity.MemberID != 0) {
			if err = g.prepareReadmission(ctx, identity, result); err != nil {
				return err
			}
		}
		granted := identity
		granted.MemberID = result.MemberID
		if g.engine.Load() == nil {
			if err = ctx.Err(); err != nil {
				return err
			}
			if err = g.openEngine(granted, raftengine.Joining, nil); err != nil {
				return &fatalAdmissionError{err}
			}
		}
		if engine := g.engine.Load(); engine != nil && !engine.Running() {
			if err = engine.Start(g.ctx); err != nil {
				return err
			}
		}
		if g.currentIdentity().MemberID != result.MemberID {
			return &fatalAdmissionError{fault.ErrIdentityMismatch}
		}
		g.joinAdmission = result.Admission
		g.joinReady = result.Existing || result.Admission.State == metaengine.AdmissionCompleted
		g.joinDone = true
		return nil
	}
	return last
}
func (g *Group) prepareReadmission(ctx context.Context, previous Identity, result joinResponse) error {
	if engine := g.engine.Load(); engine != nil {
		if err := engine.Stop(ctx); err != nil {
			return err
		}
		g.engine.Store(nil)
	}
	g.mu.Lock()
	store := g.store
	g.store = nil
	g.mu.Unlock()
	if store != nil {
		if err := store.Close(); err != nil {
			return err
		}
	}
	granted := previous
	granted.MemberID = result.MemberID
	if previous.MemberID == 0 {
		previous.MemberID = result.Previous.MemberID(previous.NodeName)
	}
	g.mu.RLock()
	oldDocuments := append([]topology.Document{result.Previous}, g.recoveryDocuments...)
	g.mu.RUnlock()
	payload := Readmission{Previous: previous, Granted: granted, AdmissionID: string(result.Admission.ExecutionID) + "/" + result.Admission.InstanceID, MetaDBPath: g.opts.DBPath, PreviousGroups: previousNodeGroups(oldDocuments, result.Granted, previous.NodeName), GrantedGroups: nodeGroups(result.Granted, previous.NodeName)}
	if g.opts.OnReadmission == nil {
		return &ReadmissionRequiredError{Readmission: payload}
	}
	// Caller code owns exact storage/application quarantine. The old database is
	// closed before invoking this idempotent hook; the library never wipes it.
	if err := g.opts.OnReadmission(ctx, payload); err != nil {
		return fault.New(fault.ReadmissionRequired, "caller readmission hook failed", err)
	}
	if fresh, err := metastore.StoreIsFresh(g.opts.DBPath); err != nil || !fresh {
		return fault.New(fault.ReadmissionRequired, "caller readmission hook did not quarantine the previous meta lineage", err)
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
			previous := grouped[group.ID]
			if previous.ID == "" {
				previous = group
				previous.Replicas = nil
			}
			for _, r := range group.Replicas {
				skip := false
				if next := granted.Group(group.ID); next != nil {
					for _, fresh := range next.Replicas {
						if fresh.NodeName == r.NodeName && fresh.ReplicaID == r.ReplicaID {
							skip = true
						}
					}
				}
				for _, old := range previous.Replicas {
					skip = skip || old.ReplicaID == r.ReplicaID
				}
				if !skip {
					previous.Replicas = append(previous.Replicas, r)
				}
			}
			if len(previous.Replicas) > 0 {
				grouped[group.ID] = previous
			}
		}
	}
	var result []topology.Group
	for _, group := range grouped {
		result = append(result, group)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
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
	data, _ := json.Marshal(readyRequest{NodeName: a.NodeName, InstanceID: a.InstanceID, MemberID: a.MemberID, ExecutionID: a.ExecutionID, Address: g.opts.Network.AdvertiseAddress})
	addresses := append([]string{g.joinLeader}, g.opts.JoinSeeds...)
	if leader := engine.RaftStatus().Lead; leader != 0 {
		if address, err := g.resolveMember(ctx, leader); err == nil {
			addresses = append([]string{address}, addresses...)
		}
	}
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
		if errors.Is(err, fault.ErrNotLeader) && reply.LeaderAddress != "" {
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
	sort.Slice(peers, func(i, j int) bool { return peers[i].MemberId < peers[j].MemberId })
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
	// Only the observed Raft leader relays peer routes. Followers may carry stale
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

// validateJoinGrant runs before any resolver update or durable-store creation.
func validateJoinGrant(identity Identity, rules topology.Rules, result joinResponse) error {
	if !reflect.DeepEqual(result.Rules, rules) || result.MemberID == 0 || result.Granted.ClusterID != identity.ClusterID {
		return fault.ErrIdentityMismatch
	}
	if err := result.Granted.Validate(rules); err != nil {
		return fault.New(fault.Invalid, "invalid admitted topology", err)
	}
	if result.MemberID != result.Granted.MemberID(identity.NodeName) {
		return fault.ErrIdentityMismatch
	}
	if !result.Existing && (result.Admission.NodeName != identity.NodeName || result.Admission.InstanceID != identity.InstanceID || result.Admission.MemberID != result.MemberID || result.Admission.ExecutionID == "") {
		return fault.ErrIdentityMismatch
	}
	if result.Previous.Version > 0 {
		if result.Previous.ClusterID != identity.ClusterID {
			return fault.ErrIdentityMismatch
		}
		if err := result.Previous.Validate(rules); err != nil {
			return fault.New(fault.Invalid, "invalid previous topology", err)
		}
	}
	owners := map[uint64]string{}
	for _, doc := range []topology.Document{result.Previous, result.Granted} {
		if group := doc.MetaGroup(); group != nil {
			for _, r := range group.Replicas {
				if old := owners[r.ReplicaID]; old != "" && old != r.NodeName {
					return fault.ErrIdentityMismatch
				}
				owners[r.ReplicaID] = r.NodeName
			}
		}
	}
	seen := map[uint64]bool{}
	for _, member := range result.Members {
		if member.MemberID == 0 || member.Name == "" || owners[member.MemberID] != member.Name || seen[member.MemberID] {
			return fault.ErrIdentityMismatch
		}
		seen[member.MemberID] = true
		if member.Address != "" {
			if _, _, err := net.SplitHostPort(member.Address); err != nil {
				return fault.ErrInvalid
			}
		}
	}
	if _, _, err := net.SplitHostPort(result.LeaderAddress); err != nil {
		return fault.ErrInvalid
	}
	return nil
}
