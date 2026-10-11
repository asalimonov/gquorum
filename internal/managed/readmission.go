package managed

import (
	"cmp"
	"context"
	"path/filepath"
	"slices"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// Readmission is supplied only by the authenticated private meta service. Node
// installs a new managed host after this host's lifetime has been retired.
type Readmission struct {
	Previous, Granted             topology.Identity
	AdmissionID, MetaDBPath       string
	PreviousGroups, GrantedGroups []topology.Group
}

func (g Readmission) Clone() Readmission {
	g.PreviousGroups = (topology.Document{Groups: g.PreviousGroups}).Clone().Groups
	g.GrantedGroups = (topology.Document{Groups: g.GrantedGroups}).Clone().Groups
	return g
}

func (g Readmission) Validate(own topology.Identity, path string) error {
	if !completeIdentity(g.Previous) || !completeIdentity(g.Granted) || strings.TrimSpace(path) == "" || strings.TrimSpace(g.AdmissionID) == "" || strings.ContainsRune(g.AdmissionID, 0) || filepath.Clean(g.MetaDBPath) != filepath.Clean(path) {
		return fault.ErrIdentityMismatch
	}
	if own.NodeName != g.Previous.NodeName || own.ClusterID != g.Previous.ClusterID || own.InstanceID != g.Previous.InstanceID || own.MemberID != 0 && own.MemberID != g.Previous.MemberID {
		return fault.ErrIdentityMismatch
	}
	if g.Granted.NodeName != g.Previous.NodeName || g.Granted.ClusterID != g.Previous.ClusterID || g.Granted.InstanceID != g.Previous.InstanceID || g.Granted.MemberID == g.Previous.MemberID {
		return fault.ErrIdentityMismatch
	}
	prior, err := readmissionIDs(g.PreviousGroups, g.Previous)
	if err != nil {
		return err
	}
	granted, err := readmissionIDs(g.GrantedGroups, g.Granted)
	if err != nil {
		return err
	}
	for id := range prior {
		if granted[id] {
			return fault.ErrIdentityMismatch
		}
	}
	return nil
}

func readmissionIDs(groups []topology.Group, node topology.Identity) (map[storage.Identity]bool, error) {
	ids := make(map[storage.Identity]bool)
	seenGroups := make(map[string]bool)
	for _, group := range groups {
		if topology.ValidateID(group.ID) != nil || seenGroups[group.ID] || len(group.Replicas) == 0 {
			return nil, fault.ErrIdentityMismatch
		}
		seenGroups[group.ID] = true
		for _, r := range group.Replicas {
			id := storage.Identity{NodeName: node.NodeName, ClusterID: node.ClusterID, GroupID: group.ID, ReplicaID: r.ReplicaID}
			if r.NodeName != node.NodeName || r.ReplicaID == 0 || ids[id] {
				return nil, fault.ErrIdentityMismatch
			}
			ids[id] = true
		}
	}
	return ids, nil
}

// Readmit joins the former host before retiring application and consensus
// state. Its callbacks remain owned until they return, even after cancellation.
// A retry uses the same grant and verifies already completed work again.
func (h *Host) Readmit(parent context.Context, grant Readmission) error {
	grant = grant.Clone()
	if err := grant.Validate(h.cfg.Identity(), h.cfg.MetaDBPath); err != nil {
		return err
	}
	if h.cfg.AuthorizeReadmission == nil {
		return fault.ErrUnsupported
	}
	if err := h.cfg.AuthorizeReadmission(parent, grant.Clone()); err != nil {
		return err
	}
	select {
	case h.readmissionGate <- struct{}{}:
		defer func() { <-h.readmissionGate }()
	case <-parent.Done():
		return parent.Err()
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	h.mu.Lock()
	if h.shutdownRequested {
		h.mu.Unlock()
		return fault.ErrStopped
	}
	h.readmissionCancel = cancel
	if h.readmissionOwned == nil {
		h.readmissionOwned = make(map[storage.Identity]bool)
	}
	for _, slot := range h.slots {
		if !slot.released {
			h.readmissionOwned[slot.identity] = true
		}
	}
	var owned []storage.Identity
	for id := range h.readmissionOwned {
		owned = append(owned, id)
	}
	h.mu.Unlock()
	defer func() { h.mu.Lock(); h.readmissionCancel = nil; h.mu.Unlock() }()
	if err := h.stopReplicas(ctx); err != nil {
		return err
	}
	prior, _ := readmissionIDs(grant.PreviousGroups, grant.Previous)
	for _, id := range owned {
		if !prior[id] {
			return fault.ErrIdentityMismatch
		}
	}
	check := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := grant.Validate(h.cfg.Identity(), h.cfg.MetaDBPath); err != nil {
			return err
		}
		return h.cfg.AuthorizeReadmission(ctx, grant.Clone())
	}
	if err := check(); err != nil {
		return err
	}
	meta, err := inspectReadmissionMeta(ctx, grant, h.cfg.Identity().MemberID == 0)
	if err != nil {
		return err
	}
	for group, record := range meta.records {
		if !prior[storage.Identity{NodeName: record.NodeName, ClusterID: record.ClusterID, GroupID: group, ReplicaID: record.ReplicaID}] {
			return fault.ErrIdentityMismatch
		}
	}
	var ids []storage.Identity
	groups := make(map[string]bool)
	for id := range prior {
		if id.GroupID == topology.MetaGroupID {
			continue
		}
		ids = append(ids, id)
		groups[id.GroupID] = true
	}
	slices.SortFunc(ids, compareReadmissionIdentity)
	// Validate every actual live identity before invoking any application hook.
	for group := range groups {
		probe, err := h.cfg.Store.Probe(ctx, group)
		if err != nil {
			return err
		}
		if probe.Exists && !prior[probe.Identity] {
			return fault.ErrIdentityMismatch
		}
	}
	for _, id := range ids {
		if err := check(); err != nil {
			return err
		}
		req := load.RetirementRequest{Identity: id, Reason: load.RetirementReadmission, AdmissionID: grant.AdmissionID, PreviousMemberID: grant.Previous.MemberID, GrantedMemberID: grant.Granted.MemberID}
		call, done := context.WithTimeout(ctx, h.cfg.EffectTimeout)
		err := h.retireReadmissionReplica(call, req, prior, check)
		done()
		if err != nil {
			return err
		}
	}
	if err := check(); err != nil {
		return err
	}
	return retireReadmissionMeta(ctx, grant, h.cfg.Identity().MemberID == 0)
}

func compareReadmissionIdentity(a, b storage.Identity) int {
	if order := cmp.Compare(a.GroupID, b.GroupID); order != 0 {
		return order
	}
	return cmp.Compare(a.ReplicaID, b.ReplicaID)
}

func (h *Host) retireReadmissionReplica(ctx context.Context, req load.RetirementRequest, prior map[storage.Identity]bool, check func() error) error {
	// The meta database is closed and its ledger is preserved in the backup.
	// Never manufacture a topology cleanup step to forget readmission evidence.
	result, err := h.callRetireApplication(ctx, req)
	if err != nil {
		return err
	}
	if !matchingRetirement(req, result) {
		return fault.ErrOperatorAuthorization
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := check(); err != nil {
		return err
	}
	probe, err := h.cfg.Store.Probe(ctx, req.Identity.GroupID)
	if err != nil {
		return err
	}
	if probe.Exists && !prior[probe.Identity] {
		return fault.ErrIdentityMismatch
	}
	if probe.Exists && probe.Identity == req.Identity {
		_, err = h.cfg.Store.Quarantine(ctx, req.Identity.GroupID, req.Identity.ReplicaID)
	}
	return err
}
