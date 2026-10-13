package raftengine

import (
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func validateAuthorityOptions(o Options) error {
	if !o.ManagedAuthority {
		if o.InitialAuthority != nil {
			return fault.ErrInvalid
		}
		return nil
	}
	if o.MemberID == 0 || o.InstanceID == "" {
		return fault.New(fault.Invalid, "managed authority requires the exact host identity", nil)
	}
	if o.InitialAuthority == nil {
		return nil
	}
	g := *o.InitialAuthority
	if o.Mode != Founding || o.GroupID == topology.MetaGroupID || g.Action == nil || g.Action.Kind != topology.StepCreateFoundingGroup {
		return fault.New(fault.Invalid, "initial authority is restricted to approved load founding", nil)
	}
	if err := validateGroupGrant(o, g); err != nil {
		return err
	}
	if !slices.Contains(g.Targets, topology.AuthorityTarget{NodeName: o.NodeName, MemberID: o.MemberID, ReplicaID: o.ReplicaID}) {
		return fault.ErrIdentityMismatch
	}
	return nil
}

func validateGroupGrant(o Options, g topology.AuthorityGrant) error {
	if err := g.Validate(); err != nil {
		return err
	}
	if g.Key.ClusterID != o.ClusterID || g.Key.GroupID != o.GroupID {
		return fault.ErrIdentityMismatch
	}
	if !slices.Contains(g.Resources, topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: o.GroupID}) {
		return fault.ErrInvalid
	}
	return nil
}

func validateAuthoritySnapshot(o Options, s raftstate.Snapshot) error {
	if !o.ManagedAuthority && s.Authority.Grant != nil {
		return fault.New(fault.Unsupported, "standalone snapshot contains managed authority", nil)
	}
	if o.ManagedAuthority && o.GroupID != topology.MetaGroupID && s.Authority.Grant == nil {
		return fault.New(fault.Unreadable, "managed load snapshot lacks authority", nil)
	}
	return nil
}

func validateAuthorityRecovery(o Options, r storage.Recovery, s raftstate.Snapshot) error {
	fresh := raft.IsEmptySnap(r.Snapshot) && raft.IsEmptyHardState(r.HardState) && len(r.Entries) == 0
	if o.InitialAuthority != nil && !fresh {
		return fault.New(fault.Invalid, "initial authority cannot override retained state", nil)
	}
	if fresh && o.ManagedAuthority && o.GroupID != topology.MetaGroupID && o.Mode == Founding && o.InitialAuthority == nil {
		return fault.New(fault.Invalid, "managed load founding requires approved authority", nil)
	}
	if !raft.IsEmptySnap(r.Snapshot) {
		if err := validateAuthoritySnapshot(o, s); err != nil {
			return err
		}
	}
	for _, e := range r.Entries {
		if err := validateEntryMode(o, e); err != nil {
			return err
		}
	}
	return nil
}

func validateEntryMode(o Options, entry raftpb.Entry) error {
	if err := validateEntryFormat(entry); err != nil {
		return err
	}
	if entry.Type == raftpb.EntryNormal && len(entry.Data) == 0 {
		return nil
	}
	var e raftstate.Entry
	if entry.Type == raftpb.EntryConfChangeV2 {
		_, e, _ = decodeConfiguration(entry)
	} else {
		e, _ = raftstate.DecodeEntry(entry.Data)
	}
	if e.Kind == raftstate.Application {
		return nil
	}
	if e.Kind == raftstate.StandaloneConfiguration {
		if o.ManagedAuthority {
			return fault.ErrUnsupported
		}
		return nil
	}
	if !o.ManagedAuthority {
		return fault.ErrUnsupported
	}
	return validateGroupGrant(o, *e.Grant)
}
