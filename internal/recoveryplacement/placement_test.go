//go:build !functional

package recoveryplacement

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func placementFixture(t *testing.T) (topology.Document, topology.Rules, *model.ImportState) {
	t.Helper()
	policy := topology.DefaultVoterConfig()
	policy.DistinctVoterLabel = "az"
	loadPolicy := policy
	doc := topology.Document{Version: 2, ParentVersion: 1, ClusterID: "recovered", Origin: topology.OriginOperator,
		ChangedBy: "operator", CreatedAt: time.Unix(100, 0).UTC(), Placement: topology.PlacementRule{MinReplicas: 3, MaxReplicas: 5},
		Nodes: []topology.Node{{Name: "n1", Type: "worker", State: topology.NodeActive, Labels: map[string]string{"az": "a"}}},
		Groups: []topology.Group{
			{ID: topology.MetaGroupID, NextReplicaID: 6, VoterConfig: &policy, Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 5, Role: topology.ReplicaVoter}}},
			{ID: "work", NextReplicaID: 6, VoterConfig: &loadPolicy, Hosting: &topology.HostingRule{Mode: topology.HostingAllEligible}, Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 5, Role: topology.ReplicaVoter}}},
		}}
	rules := topology.Rules{SchemaVersion: 1, Limits: (topology.Limits{}).WithDefaults()}
	state := &model.ImportState{Schema: model.SchemaVersion, RecoveryID: "recovery", PlanDigest: strings.Repeat("a", 64),
		Source:   topology.Identity{NodeName: "n1", ClusterID: "old", MemberID: 1},
		Survivor: storage.Identity{NodeName: "n1", ClusterID: doc.ClusterID, GroupID: topology.MetaGroupID, ReplicaID: 5}, Version: 2, Phase: "complete",
		Groups: []model.ImportGroup{{GroupID: "work", Destination: storage.Identity{NodeName: "n1", ClusterID: doc.ClusterID, GroupID: "work", ReplicaID: 5}}}}
	for _, group := range doc.Groups {
		digest, err := PolicyDigest(doc, group.ID)
		if err != nil {
			t.Fatal(err)
		}
		state.Policies = append(state.Policies, model.PolicyState{GroupID: group.ID, PolicyDigest: digest})
	}
	return doc, rules, state
}

func TestRecoveryPreparationGrowsSingletonWithoutChangingDesiredPolicy(t *testing.T) {
	t.Parallel()
	doc, rules, state := placementFixture(t)
	ctx, err := FromImport(state, doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(Context{}, doc, doc, doc, rules); err == nil {
		t.Fatal("ordinary strict validation allowed the recovery deficit")
	}
	if err := Validate(ctx, doc, doc, doc, rules); err != nil {
		t.Fatal("unchanged recovered assignment refused", err)
	}
	original, _ := json.Marshal(doc)
	for _, node := range []topology.Node{{Name: "n2", Type: "worker", State: topology.NodeActive, Labels: map[string]string{"az": "b"}}, {Name: "n3", Type: "worker", State: topology.NodeActive, Labels: map[string]string{"az": "c"}}} {
		edited := doc.Clone()
		edited.Nodes = append(edited.Nodes, node)
		meta := edited.MetaGroup()
		meta.Replicas = append(meta.Replicas, topology.Replica{NodeName: node.Name, ReplicaID: meta.NextReplicaID, Role: topology.ReplicaLearner})
		meta.NextReplicaID++
		prepared, err := Prepare(ctx, topology.ReadView{Latest: doc, Target: doc}, edited, rules, time.Unix(101, 0).UTC())
		if err != nil {
			t.Fatal(node.Name, err)
		}
		for _, group := range prepared.Document.Groups {
			if group.VoterConfig.TargetVoters != 3 || prepared.Document.GroupPlacement(group.ID).MinReplicas != 3 {
				t.Fatal("normalized count escaped into desired intent", group)
			}
		}
		if len(prepared.Document.Group("work").Replicas) != len(prepared.Document.Nodes) {
			t.Fatal("admission did not add load hosting", prepared.Document)
		}
		// Declarative admission remains learner-first. Model the following
		// approved role edit; native catch-up/quorum guards stay in BuildPlan.
		promoted := prepared.Document.Clone()
		promoted.Version++
		promoted.ParentVersion = prepared.Document.Version
		for i := range promoted.Groups {
			for j := range promoted.Groups[i].Replicas {
				promoted.Groups[i].Replicas[j].Role = topology.ReplicaVoter
			}
		}
		if err := Validate(ctx, prepared.Document, prepared.Document, promoted, rules); err != nil {
			t.Fatal("progressive promotion refused", err)
		}
		doc = promoted
	}
	if err := Validate(Context{}, doc, doc, doc, rules); err != nil {
		t.Fatal("fully repaired policy is not normally valid", err)
	}
	base, _, _ := placementFixture(t)
	if !reflect.DeepEqual(original, mustJSON(t, base)) {
		t.Fatal("input fixture changed")
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestRecoveryPolicyScopesRejectForgeryAndNeverReactivateReleasedPolicy(t *testing.T) {
	t.Parallel()
	doc, rules, state := placementFixture(t)
	for _, change := range []func(*model.ImportState){
		func(s *model.ImportState) { s.PlanDigest = "not-a-digest" },
		func(s *model.ImportState) { s.Survivor.ClusterID = "foreign" },
		func(s *model.ImportState) { s.Policies = append(s.Policies, s.Policies[0]) },
	} {
		invalid := model.CloneImport(*state)
		change(&invalid)
		if _, err := FromImport(&invalid, doc); err == nil {
			t.Fatal("forged scope accepted")
		}
	}
	for i := range state.Policies {
		state.Policies[i].PolicyReleased = true
	}
	ctx, err := FromImport(state, doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(ctx, doc, doc, doc, rules); err == nil {
		t.Fatal("released exception reactivated")
	}
}
