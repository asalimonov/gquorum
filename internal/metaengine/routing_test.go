package metaengine

import (
	"github.com/asalimonov/gquorum/topology"
	"testing"
)

func TestProcessRoutingSeparatesReplacementAdmissionFromFormerSession(t *testing.T) {
	t.Parallel()
	m := NewMachine(topology.Rules{SchemaVersion: 1, Limits: topology.Limits{}.WithDefaults()}, nil)
	m.active = topology.Document{Version: 1, ClusterID: "cluster", Groups: []topology.Group{{ID: topology.MetaGroupID, Replicas: []topology.Replica{{NodeName: "node", ReplicaID: 1}}}}}
	m.target = m.active.Clone()
	m.locks["node/node"] = "old-session"
	m.sessions["old-session"] = &sessionState{SessionID: "old-session", NodeName: "node", InstanceID: "old-process"}
	old := topology.Identity{ClusterID: "cluster", NodeName: "node", MemberID: 1, InstanceID: "old-process"}
	if got, ok := m.ProcessIdentity("node"); !ok || got != old {
		t.Fatal(got, ok)
	}
	m.target.Groups[0].Replicas[0].ReplicaID = 2
	if got, ok := m.ProcessIdentity("node"); ok || got.InstanceID != "" {
		t.Fatal("old session attached to replacement member", got)
	}
	grant := &AdmissionRecord{NodeName: "node", MemberID: 2, InstanceID: "new-process", State: AdmissionReady}
	m.admissions[admissionKey("node", "new-process")] = grant
	if got, ok := m.ProcessIdentity("node"); !ok || got.MemberID != 2 || got.InstanceID != "new-process" {
		t.Fatal("admission cannot route before session activation", got, ok)
	}
	grant.State = AdmissionSuperseded
	if got, ok := m.ProcessIdentity("node"); ok {
		t.Fatal("superseded process routed", got)
	}
	m.active = m.target.Clone()
	delete(m.sessions, "old-session")
	if got, ok := m.ProcessIdentity("node"); ok {
		t.Fatal("missing activation fabricated process", got)
	}
	m.sessions["new-session"] = &sessionState{SessionID: "new-session", NodeName: "node", InstanceID: "new-process"}
	m.locks["node/node"] = "new-session"
	if got, ok := m.ProcessIdentity("node"); !ok || got.MemberID != 2 || got.InstanceID != "new-process" {
		t.Fatal(got, ok)
	}
}
