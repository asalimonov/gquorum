//go:build !functional

package metaengine

import (
	"strconv"
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

func assertIndexedProcessRouting(t *testing.T, m *Machine) {
	t.Helper()
	names := map[string]bool{"unknown": true}
	for _, node := range m.scoped.Target.Nodes {
		names[node.Name] = true
	}
	for _, node := range m.scoped.Nodes {
		names[node.NodeName] = true
	}
	for _, a := range m.scoped.Admissions {
		names[a.NodeName] = true
	}
	for _, session := range m.scoped.Sessions {
		names[session.NodeName] = true
	}
	for name := range names {
		want, valid := scopedProcessIdentity(m.scoped, name)
		got, ok := m.ProcessIdentity(name)
		if got != want || ok != valid {
			t.Fatalf("indexed route %s differs: %+v/%v, want %+v/%v", name, got, ok, want, valid)
		}
	}
}
func TestProcessRoutingIndexTracksSessionsLocksAndRestore(t *testing.T) {
	t.Parallel()
	m := restoreMachineFixture(t, scopedCodecFixture())
	assertIndexedProcessRouting(t, m)
	result := applyMachine(t, m, GrantSession{ProposalID: "route-session", SessionID: "route", NodeName: "n", InstanceID: "fresh", TTLSeconds: 30})
	if !result.Accepted {
		t.Fatal(result)
	}
	assertIndexedProcessRouting(t, m)
	result = applyMachine(t, m, AcquireLock{ProposalID: "route-lock", Key: "node/n", SessionID: "route"})
	if !result.Accepted {
		t.Fatal(result)
	}
	assertIndexedProcessRouting(t, m)
	if id, ok := m.ProcessIdentity("n"); !ok || id.InstanceID != "fresh" {
		t.Fatal(id, ok)
	}
	restored := restoreMachineFixture(t, *m.scoped)
	assertIndexedProcessRouting(t, restored)
	result = applyMachine(t, restored, RevokeSession{ProposalID: "route-revoke", SessionID: "route"})
	if !result.Accepted {
		t.Fatal(result)
	}
	assertIndexedProcessRouting(t, restored)
	if id, ok := restored.ProcessIdentity("n"); ok || id.InstanceID != "" {
		t.Fatal("revoked process still routed", id, ok)
	}
}

func BenchmarkProcessIdentityIndex(b *testing.B) {
	for _, count := range []int{1, 100, 10000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			m := &Machine{processIdentities: make(map[string]topology.Identity, count)}
			for i := range count {
				name := strconv.Itoa(i)
				m.processIdentities[name] = topology.Identity{NodeName: name, ClusterID: "cluster", InstanceID: "process", MemberID: uint64(i + 1)}
			}
			name := strconv.Itoa(count - 1)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				id, ok := m.ProcessIdentity(name)
				if !ok || id.MemberID != uint64(count) {
					b.Fatal(id, ok)
				}
			}
		})
	}
}
