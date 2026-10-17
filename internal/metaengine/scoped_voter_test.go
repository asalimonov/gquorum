//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func scopedArchivedVoterFixture(at time.Time) []topology.VoterSwitchState {
	var groups []topology.VoterSwitchState
	for i, group := range []string{"live", "a"} {
		id := uint64(1)
		if group == "a" {
			id = 8
		}
		r := topology.RoleChangeRecord{OperationID: "exec-v5-a1", Version: 5, Attempt: 1, StepSeq: i + 1,
			GroupID: group, NodeName: "n", ReplicaID: id, From: topology.ReplicaLearner,
			To: topology.ReplicaVoter, At: at, Index: uint64(25 + i)}
		groups = append(groups, topology.VoterSwitchState{GroupID: group, Last: r,
			Replicas: []topology.RoleChangeRecord{r}})
	}
	return groups
}

func scopedVoterFixture() scopedState {
	s := scopedGraphFixture()
	for _, doc := range []*topology.Document{&s.History[0], &s.Target} {
		doc.Nodes = append(doc.Nodes, topology.Node{Name: "n2", Type: "worker", State: topology.NodeActive})
		role := topology.ReplicaLearner
		if doc.Version == 2 {
			role = topology.ReplicaVoter
		}
		doc.MetaGroup().Replicas = append(doc.MetaGroup().Replicas,
			topology.Replica{NodeName: "n2", ReplicaID: 2, Role: role})
		doc.MetaGroup().NextReplicaID = 3
	}
	s.Latest = s.Target.Clone()
	s.Groups[0].Declaration = s.Target.MetaGroup()
	s.Nodes = append(s.Nodes, nodeConvergence{NodeName: "n2", MemberID: 2, SourceVersion: 1,
		Declaration: s.History[0].Node("n2"), CompletedAt: s.Nodes[0].CompletedAt})
	s.Allocations[0].NextReplicaID = 3
	for i := range s.Operations {
		step := &s.Operations[i].Plan.Steps[0]
		step.Kind, step.NodeName, step.ReplicaID = topology.StepPromoteVoter, "n2", 2
		step.ExpectedVotersDigest, step.ResultVotersDigest = topology.VoterSetDigest([]uint64{1}),
			topology.VoterSetDigest([]uint64{1, 2})
		s.Operations[i].Plan.VoterBases = []topology.PlanVoterBase{{GroupID: topology.MetaGroupID, Voters: []uint64{1}}}
	}
	s.AuthorityRecords[0].Grant = s.AuthorityRecords[0].Grant.Clone()
	grant := &s.AuthorityRecords[0].Grant
	step := s.Operations[0].Plan.Steps[0]
	grant.Action.Kind, grant.Action.NodeName, grant.Action.ReplicaID = step.Kind, step.NodeName, step.ReplicaID
	grant.Action.ExpectedVotersDigest, grant.Action.ResultVotersDigest = step.ExpectedVotersDigest, step.ResultVotersDigest
	grant.Targets[0] = topology.AuthorityTarget{NodeName: "n2", MemberID: 2, ReplicaID: 2}
	s.AuthorityRecords[0].Effects = fixtureEffectReceipts(*grant)
	s.AuthorityRecords[0].Effects[0].AppliedIndex = 3
	r := topology.RoleChangeRecord{OperationID: "exec-v2-a1", Version: 2, Attempt: 1, StepSeq: 1,
		GroupID: topology.MetaGroupID, NodeName: "n2", ReplicaID: 2, From: topology.ReplicaLearner,
		To: topology.ReplicaVoter, At: s.Operations[0].UpdatedAt, Index: 5}
	s.VoterSwitches = []topology.VoterSwitchState{{GroupID: r.GroupID, Last: r, Replicas: []topology.RoleChangeRecord{r}}}
	return s
}

func TestScopedVoterHistorySurvivesRetryAndPrunedOperation(t *testing.T) {
	t.Parallel()
	s := scopedVoterFixture()
	s.AuthorityRecords[0].Closed = true // Settled historical consumption stays valid after revocation.
	data, _ := json.Marshal(s)
	var restored scopedState
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(s.VoterSwitches, restored.VoterSwitches) ||
		restored.indexes.retries["exec-v2-a1"] != "exec-v2-a2" {
		t.Fatal("retry reset cooldown or lost its exact proof link")
	}
	s.Operations, s.AuthorityRecords, s.AuthorityFloors, s.Revocations, s.History = nil, nil, nil, nil, nil
	s.Groups[1].Owner, s.FullyConverged = "", topology.FullyConverged{}
	pruned, _ := json.Marshal(s)
	if err := json.Unmarshal(pruned, &restored); err != nil {
		t.Fatal("pruned operation erased cooldown", err)
	}
	if !reflect.DeepEqual(s.VoterSwitches, restored.VoterSwitches) || len(restored.indexes.operations) != 0 {
		t.Fatal("archive changed or retained an obsolete operation")
	}
}

func TestMachineVoterSwitchesPreservesCooldownAndOwnsRecords(t *testing.T) {
	t.Parallel()
	s := scopedVoterFixture()
	m := restoreMachineFixture(t, s)
	got := m.VoterSwitches()
	if !reflect.DeepEqual(got, s.VoterSwitches) {
		t.Fatal("scheduler lost restored role-change cooldown", got)
	}
	got[0].Replicas[0].At = got[0].Replicas[0].At.Add(time.Hour)
	got[0].Last.NodeName = "caller mutation"
	if !reflect.DeepEqual(m.VoterSwitches(), s.VoterSwitches) {
		t.Fatal("scheduler input aliases retained cooldown proof")
	}
}

func TestScopedVoterHistoryRejectsCorruptionAtomically(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*topology.RoleChangeRecord){
		"foreign node":       func(r *topology.RoleChangeRecord) { r.NodeName = "other" },
		"unallocated":        func(r *topology.RoleChangeRecord) { r.ReplicaID = 3 },
		"zero index":         func(r *topology.RoleChangeRecord) { r.Index = 0 },
		"before grant":       func(r *topology.RoleChangeRecord) { r.Index = 3 },
		"future index":       func(r *topology.RoleChangeRecord) { r.Index = 11 },
		"future version":     func(r *topology.RoleChangeRecord) { r.Version, r.OperationID = 3, "exec-v3-a1" },
		"unretained attempt": func(r *topology.RoleChangeRecord) { r.Attempt, r.OperationID = 3, "exec-v2-a3" },
		"relabeled retry":    func(r *topology.RoleChangeRecord) { r.Attempt, r.OperationID = 2, "exec-v2-a2" },
		"wrong step":         func(r *topology.RoleChangeRecord) { r.StepSeq = 2 },
		"wrong role":         func(r *topology.RoleChangeRecord) { r.From, r.To = r.To, r.From },
		"same role":          func(r *topology.RoleChangeRecord) { r.To = r.From },
		"zero time":          func(r *topology.RoleChangeRecord) { r.At = time.Time{} },
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := scopedVoterFixture()
			good, _ := json.Marshal(s)
			var restored scopedState
			if err := json.Unmarshal(good, &restored); err != nil {
				t.Fatal(err)
			}
			index := restored.indexes
			corrupt(&s.VoterSwitches[0].Last)
			s.VoterSwitches[0].Replicas[0] = s.VoterSwitches[0].Last
			bad, _ := json.Marshal(s)
			if err := json.Unmarshal(bad, &restored); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("invalid voter history installed", err)
			}
			after, _ := json.Marshal(restored)
			if !bytes.Equal(good, after) || restored.indexes != index {
				t.Fatal("refused voter history replaced state or indexes")
			}
		})
	}
}
