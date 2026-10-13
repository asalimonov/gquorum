package managed

import (
	"context"
	"errors"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// A readmission owns the entire former host after its admission reservation.
// Persist every scope's floor before joining callbacks; only then may retirement
// start. The caller must also join Node's metadata authority worker.
type readmissionAuthority struct {
	store  *authoritystore.Store
	grants map[string]topology.AuthorityGrant
	floors []authoritystore.FenceRecord
}

func beginReadmissionAuthority(ctx context.Context, s *authoritystore.Store, grant Readmission) (*readmissionAuthority, error) {
	if s == nil || s.Identity() != grant.Previous || len(grant.Grants) == 0 {
		return nil, fault.ErrOperatorAuthorization
	}
	prior, err := readmissionIDs(grant.PreviousGroups, grant.Previous)
	if err != nil {
		return nil, err
	}
	prior[storage.Identity{ClusterID: grant.Previous.ClusterID, NodeName: grant.Previous.NodeName, GroupID: topology.MetaGroupID, ReplicaID: grant.Previous.MemberID}] = true
	granted, err := readmissionIDs(grant.GrantedGroups, grant.Granted)
	if err != nil {
		return nil, err
	}
	a := &readmissionAuthority{store: s, grants: make(map[string]topology.AuthorityGrant)}
	var revision uint64
	for _, g := range grant.Grants {
		want := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: g.Key.GroupID, NodeName: grant.Previous.NodeName}
		if g.Validate() != nil || g.Purpose != topology.AuthorityReadmit || g.AdmissionID != grant.AdmissionID || g.Key.ClusterID != grant.Previous.ClusterID || len(g.Resources) != 1 || g.Resources[0] != want || a.grants[g.Key.GroupID].Key.Revision != 0 || revision != 0 && revision != g.Key.Revision {
			return nil, fault.ErrOperatorAuthorization
		}
		revision = g.Key.Revision
		for _, target := range g.Targets {
			id := storage.Identity{ClusterID: g.Key.ClusterID, NodeName: target.NodeName, GroupID: g.Key.GroupID, ReplicaID: target.ReplicaID}
			if target.MemberID != grant.Previous.MemberID || target.NodeName != grant.Previous.NodeName || !prior[id] || granted[id] || g.Key.GroupID == topology.MetaGroupID && target.ReplicaID != grant.Previous.MemberID {
				return nil, fault.ErrIdentityMismatch
			}
			delete(prior, id)
		}
		a.grants[g.Key.GroupID] = g.Clone()
	}
	if len(prior) != 0 {
		return nil, fault.ErrOperatorAuthorization
	}
	// Complete validation precedes the first durable write. Partial installation
	// after an I/O failure remains pending and is safe to resume with this grant.
	for _, g := range grant.Grants {
		floor, _, err := s.Fence(ctx, g)
		if err != nil {
			return nil, err
		}
		a.floors = append(a.floors, floor)
	}
	return a, nil
}

func (a *readmissionAuthority) joined(ctx context.Context) error {
	for _, floor := range a.floors {
		running, err := a.store.Running(ctx, floor.Grant.Key.GroupID)
		if err != nil {
			return err
		}
		for _, old := range running {
			if old.Grant.Equal(floor.Grant) {
				continue // retry of this readmission's own interrupted retirement
			}
			if err := a.store.FenceEffect(ctx, old, floor.Grant.Key); err != nil {
				return err
			}
		}
		if _, err := a.store.CompleteFence(ctx, floor.Grant.Key, floor.Sequence); err != nil {
			return err
		}
	}
	return nil
}

func (a *readmissionAuthority) effect(ctx context.Context, group string, replica uint64, run func() error) error {
	grant, ok := a.grants[group]
	if !ok {
		return fault.ErrOperatorAuthorization
	}
	target := topology.AuthorityTarget{NodeName: a.store.Identity().NodeName, MemberID: a.store.Identity().MemberID, ReplicaID: replica}
	receipt, err := a.store.BeginEffect(ctx, grant, target)
	if err == nil && receipt != nil {
		return nil
	}
	if err != nil && !errors.Is(err, fault.ErrAuthorityPending) {
		return err
	}
	running, err := a.store.Running(ctx, group)
	if err != nil {
		return err
	}
	var sequence uint64
	for _, r := range running {
		if r.Grant.Equal(grant) && r.Target == target {
			sequence = r.Sequence
		}
	}
	if sequence == 0 {
		return fault.ErrAuthorityPending
	}
	if err := run(); err != nil {
		return err
	}
	return a.store.FinishEffect(ctx, topology.EffectReceipt{Key: grant.Key, Resource: grant.Resources[0], AdmissionID: grant.AdmissionID, Target: target, Outcome: topology.EffectApplied, LocalSequence: sequence})
}

func (a *readmissionAuthority) retireMeta(ctx context.Context, grant Readmission, allowMissing bool) error {
	if err := a.effect(ctx, topology.MetaGroupID, grant.Previous.MemberID, func() error {
		return retireReadmissionMeta(ctx, grant, allowMissing)
	}); err != nil {
		return err
	}
	// A historical receipt cannot hide storage restored into the live namespace.
	fresh, err := metastore.StoreIsFresh(grant.MetaDBPath)
	if err != nil {
		return err
	}
	if !fresh {
		return fault.ErrIdentityMismatch
	}
	return ctx.Err()
}
