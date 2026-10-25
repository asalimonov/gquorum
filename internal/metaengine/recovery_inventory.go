package metaengine

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// RecoveryInventory separates approved requirements from exact proven donors.
// Latest contributes non-reuse floors and archived summaries, never donor intent.
type RecoveryInventory struct {
	Rules              topology.Rules
	Approved           topology.Document
	Replicas           []model.ReplicaEvidence
	Floors             []model.AllocationFloor
	Retired            []string
	Archived           model.Summary
	ReadmissionBackups map[string]topology.Identity
}

// RecoveryInventory returns an owned, validated source projection after replay.
func (m *Machine) RecoveryInventory(ctx context.Context) (RecoveryInventory, error) {
	if err := ctx.Err(); err != nil {
		return RecoveryInventory{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.scoped == nil || m.scoped.Target.Version == 0 {
		return RecoveryInventory{}, fault.ErrUnavailable
	}
	if _, err := prepareScopedGraph(m.scoped, m.scoped.Rules); err != nil {
		return RecoveryInventory{}, err
	}
	return recoveryInventory(m.scoped), nil
}

func recoveryInventory(s *scopedState) RecoveryInventory {
	out := RecoveryInventory{Rules: s.Rules, Approved: s.Target.Clone(), Archived: model.Summary{
		LatestVersion: s.Latest.Version, ApprovedVersion: s.Target.Version,
		SessionsNotRestored: uint64(len(s.Sessions)), LocksNotRestored: uint64(len(s.Locks)),
	}}
	for _, floor := range s.Allocations {
		out.Floors = append(out.Floors, model.AllocationFloor{GroupID: floor.GroupID, NextID: floor.NextReplicaID})
	}
	retired := make(map[string]bool)
	for _, group := range s.RetiredGroups {
		retired[group.ID] = true
	}
	addRecoveryDocument(&out.Replicas, s.Target, "approved")
	addRecoveryProgress(s, &out, retired)
	addRecoveryCleanup(s, &out)
	addRecoveryReadmissions(s, &out)
	for _, operation := range s.Operations {
		out.Archived.OperationsNotResumed = append(out.Archived.OperationsNotResumed, operation.Status.ID)
	}
	for group := range retired {
		out.Retired = append(out.Retired, group)
	}
	slices.Sort(out.Retired)
	out.Archived.RetiredGroupIDs = slices.Clone(out.Retired)
	slices.Sort(out.Archived.OperationsNotResumed)
	canonicalRecoveryReplicas(&out)
	return out
}

func addRecoveryDocument(out *[]model.ReplicaEvidence, doc topology.Document, class string) {
	for _, group := range doc.Groups {
		addRecoveryGroup(out, doc, group, class)
	}
}
func addRecoveryGroup(out *[]model.ReplicaEvidence, doc topology.Document, group topology.Group, class string) {
	meta := doc.MetaGroup()
	if meta == nil {
		return
	}
	for _, replica := range group.Replicas {
		for _, member := range meta.Replicas {
			if member.NodeName == replica.NodeName {
				*out = append(*out,
					model.ReplicaEvidence{Identity: storage.Identity{ClusterID: doc.ClusterID,
						NodeName: replica.NodeName, GroupID: group.ID, ReplicaID: replica.ReplicaID},
						MemberID: member.ReplicaID, SourceVersion: doc.Version, Classification: class})
				break
			}
		}
	}
}

func addRecoveryCleanup(s *scopedState, out *RecoveryInventory) {
	for _, debt := range s.Cleanup {
		class := "pending_predecessor"
		if debt.Retired != nil {
			class = "retired"
		}
		if s.Target.Group(debt.Identity.GroupID) == nil {
			class = "approved_deleted"
		}
		version := uint64(0)
		if debt.StopAction != nil {
			version = debt.StopAction.Version
		}
		if debt.RetireAction != nil {
			version = max(version, debt.RetireAction.Version)
		}
		if version == 0 && debt.ReadmissionKey != nil {
			// Readmission keys carry Raft revision, not document version. The
			// retained exact debt is proven at this reconstructed Target.
			version = s.Target.Version
		}
		out.Replicas = append(out.Replicas, model.ReplicaEvidence{Identity: debt.Identity,
			MemberID: debt.MemberID, SourceVersion: version, Classification: class})
		if !cleanupComplete(debt) {
			out.Archived.UnresolvedCleanup = append(out.Archived.UnresolvedCleanup, debt.Identity)
		}
	}
}

func canonicalRecoveryReplicas(out *RecoveryInventory) {
	slices.SortFunc(out.Replicas, func(a, b model.ReplicaEvidence) int {
		return cmp.Or(cmp.Compare(a.Identity.GroupID, b.Identity.GroupID), cmp.Compare(a.Identity.NodeName,
			b.Identity.NodeName), cmp.Compare(a.Identity.ReplicaID, b.Identity.ReplicaID),
			cmp.Compare(a.MemberID, b.MemberID), cmp.Compare(a.Classification, b.Classification),
			cmp.Compare(a.SourceVersion, b.SourceVersion))
	})
	out.Replicas = slices.Compact(out.Replicas)
	slices.SortFunc(out.Archived.UnresolvedCleanup, func(a, b storage.Identity) int {
		return cmp.Or(cmp.Compare(a.GroupID, b.GroupID), cmp.Compare(a.NodeName, b.NodeName),
			cmp.Compare(a.ReplicaID, b.ReplicaID))
	})
}

func addRecoveryProgress(s *scopedState, out *RecoveryInventory, retired map[string]bool) {
	for _, row := range s.Groups {
		if (row.Deleting || row.SourceVersion > 0 && row.Declaration == nil) && s.Target.Group(row.GroupID) == nil {
			retired[row.GroupID] = true
		}
		if row.Declaration == nil {
			continue
		}
		if doc, found := checkpointDocument(s, s.indexes, row); found {
			addRecoveryGroup(&out.Replicas, doc, *row.Declaration, "proven")
		}
	}
}

func addRecoveryReadmissions(s *scopedState, out *RecoveryInventory) {
	out.ReadmissionBackups = make(map[string]topology.Identity)
	for _, record := range s.AuthorityRecords {
		grant := record.Grant
		if grant.Purpose != topology.AuthorityReadmit || grant.AdmissionID == "" {
			continue
		}
		for _, target := range grant.Targets {
			nonce := sha256.Sum256([]byte(grant.AdmissionID))
			name := fmt.Sprintf("_meta.bolt.retired-%d-", target.MemberID) + hex.EncodeToString(nonce[:8])
			out.ReadmissionBackups[name] = topology.Identity{ClusterID: grant.Key.ClusterID,
				NodeName: target.NodeName, MemberID: target.MemberID}
		}
	}
}
