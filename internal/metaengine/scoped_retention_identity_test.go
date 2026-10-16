//go:build !functional

package metaengine

import (
	"testing"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestScopedCollectionReferencesEveryRetainedProofSource(t *testing.T) {
	t.Parallel()
	full := scopedCodecFixture()
	debt := full.Cleanup[0]
	id := debt.Identity
	declaration := full.History[1]
	cases := map[string]func(*scopedState){
		"latest":  func(s *scopedState) { s.Latest = declaration },
		"target":  func(s *scopedState) { s.Target = declaration },
		"history": func(s *scopedState) { s.History = []topology.Document{declaration} },
		"checkpoint": func(s *scopedState) {
			s.Groups = []groupConvergence{{GroupID: id.GroupID, SourceVersion: 1, Declaration: declaration.Group(id.GroupID)}}
		},
		"failed attempt": func(s *scopedState) {
			s.Operations = []topology.Operation{{Status: topology.OperationStatus{State: topology.OperationFailed},
				Plan: topology.Plan{Steps: []topology.Step{{Kind: topology.StepPrepareReplica,
					GroupID: id.GroupID, NodeName: id.NodeName, ReplicaID: id.ReplicaID}}}}}
		},
		"settled grant": func(s *scopedState) {
			for _, grant := range full.AuthorityRecords {
				if grant.Grant.Key == debt.Retired.Key {
					s.AuthorityRecords = append(s.AuthorityRecords, grant)
				}
			}
		},
	}
	for name, retain := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := scopedState{Target: topology.Document{ClusterID: id.ClusterID}}
			retain(&s)
			if !s.identityReferencedLocked(id) {
				t.Fatal("retained proof was treated as absence")
			}
			for _, foreign := range []storage.Identity{
				{ClusterID: "other", GroupID: id.GroupID, NodeName: id.NodeName, ReplicaID: id.ReplicaID},
				{ClusterID: id.ClusterID, GroupID: id.GroupID, NodeName: "other", ReplicaID: id.ReplicaID},
				{ClusterID: id.ClusterID, GroupID: id.GroupID, NodeName: id.NodeName, ReplicaID: id.ReplicaID + 100},
			} {
				if s.identityReferencedLocked(foreign) {
					t.Fatal("reference matched a different identity", foreign)
				}
			}
		})
	}
	// A completed row and its embedded receipts cannot keep their own proof
	// closure alive after every independent reference has been pruned.
	s := scopedState{Target: topology.Document{ClusterID: id.ClusterID}, Cleanup: []cleanupRecord{debt}}
	if s.identityReferencedLocked(id) {
		t.Fatal("cleanup retained itself")
	}
}

func TestScopedMetadataReferencesIncludeReadmissionAndNodeCheckpoint(t *testing.T) {
	t.Parallel()
	id := storage.Identity{ClusterID: "c", GroupID: topology.MetaGroupID, NodeName: "n", ReplicaID: 3}
	for _, mode := range []string{"admission", "node checkpoint", "load readmission"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s := scopedState{Target: topology.Document{ClusterID: "c"}}
			switch mode {
			case "admission":
				s.Admissions = []AdmissionRecord{{NodeName: "n", MemberID: 3, State: AdmissionSuperseded}}
			case "node checkpoint":
				s.Nodes = []nodeConvergence{{NodeName: "n", MemberID: 3}}
			case "load readmission":
				s.AuthorityRecords = []scopedAuthorityRecord{{authorityRecord: authorityRecord{Closed: true,
					Grant: topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: "c", GroupID: "load"},
						Purpose: topology.AuthorityReadmit,
						Targets: []topology.AuthorityTarget{{NodeName: "n", MemberID: 3, ReplicaID: 17}}}}}}
			}
			if !s.identityReferencedLocked(id) {
				t.Fatal("metadata lineage disappeared from collection references")
			}
			wrong := id
			wrong.ReplicaID++
			if s.identityReferencedLocked(wrong) {
				t.Fatal("former metadata proof named the successor")
			}
		})
	}
}
