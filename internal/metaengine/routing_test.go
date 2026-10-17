//go:build !functional

package metaengine

import (
	"testing"

	"github.com/asalimonov/gquorum/topology"
)

func TestProcessRoutingSeparatesReplacementAdmissionFromFormerSession(t *testing.T) {
	t.Parallel()
	s, replacement := scopedReadmissionCommitFixture(t)
	// Isolate explicit replacement approval from the automatic repair's rebase.
	s.Latest = s.Target.Clone()
	for i := range s.Allocations {
		s.Allocations[i].NextReplicaID = s.Target.Group(s.Allocations[i].GroupID).NextReplicaID
	}
	replacement.Doc.Version, replacement.Plan.Version = 2, 2
	s.Sessions = []sessionState{{SessionID: "old-session", NodeName: "n3", InstanceID: "old-process", TTLSeconds: 30}}
	s.Locks = []lockEntry{{Key: "node/n3", SessionID: "old-session"}}
	m := restoreMachineFixture(t, s)
	old := topology.Identity{ClusterID: s.Target.ClusterID, NodeName: "n3", MemberID: 3, InstanceID: "old-process"}
	if got, ok := m.ProcessIdentity("n3"); !ok || got != old {
		t.Fatal(got, ok)
	}
	staged := applyMachine(t, m, ProposeDocument{Doc: replacement.Doc, ExpectedVersion: s.Latest.Version})
	if !staged.Accepted {
		t.Fatal(staged)
	}
	approved := applyMachine(t, m, BeginOperationProposal{Version: staged.Doc.Version, Plan: replacement.Plan,
		Scopes: replacement.Scopes, At: replacement.At, Origin: replacement.Doc.Origin, ProposalID: "approve"})
	if !approved.Accepted {
		t.Fatal(approved)
	}
	if got := m.EligibleMemberID("n3"); got != replacement.Admission.MemberID {
		t.Fatal("approved replacement did not fence the old member", got)
	}
	if got, ok := m.ProcessIdentity("n3"); ok || got.InstanceID != "" {
		t.Fatal("former session attached to newly approved member before admission", got)
	}
	admission := replacement.Admission
	admission.OperationID = topology.OperationID(approved.OperationID)
	bound := applyMachine(t, m, BindAdmissionProposal{Admission: admission, At: replacement.At})
	if !bound.Accepted {
		t.Fatal(bound)
	}
	if got, ok := m.ProcessIdentity("n3"); !ok || got.MemberID != admission.MemberID || got.InstanceID != admission.InstanceID {
		t.Fatal("admission cannot route before session activation", got, ok)
	}
	// The retained old session must not be overwritten to route the newcomer.
	if record, ok := m.Session("old-session"); !ok || record.InstanceID != "old-process" {
		t.Fatal("routing changed former process evidence", record, ok)
	}
}
