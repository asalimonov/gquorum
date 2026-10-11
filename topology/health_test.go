//go:build !functional

package topology

import (
	"fmt"
	"testing"
)

func healthDocument() Document {
	doc := testDocument()
	doc.Groups[1] = doc.Groups[0]
	doc.Groups[1].ID = "load"
	doc.Groups[1].Replicas = append([]Replica(nil), doc.Groups[0].Replicas...)
	return doc
}

// Ports the source's declared-voter health cases, including independence of
// placement policy and nullable observations before health is known.
func TestEvaluateGroupHealth(t *testing.T) {
	t.Parallel()
	for alive := 0; alive <= 3; alive++ {
		t.Run(fmt.Sprint(alive), func(t *testing.T) {
			doc := healthDocument()
			health := map[string]Health{}
			for i, n := range doc.Nodes {
				health[n.Name] = HealthDead
				if i < alive {
					health[n.Name] = HealthAlive
				}
			}
			want := GroupQuorumLost
			if alive == 2 {
				want = GroupDegraded
			}
			if alive == 3 {
				want = GroupHealthy
			}
			for _, id := range []string{MetaGroupID, "load"} {
				e := EvaluateGroupHealth(doc, id, health)
				if e.Voters != 3 || e.Quorum != 2 || e.ObservedHealth.State != want || e.ObservedHealth.Source != "observed" || e.ObservedHealth.HealthyVoters == nil || *e.ObservedHealth.HealthyVoters != alive || e.ObservedHealth.FailureTolerance == nil || *e.ObservedHealth.FailureTolerance != max(alive-2, 0) {
					t.Fatalf("evaluation %+v", e)
				}
				if id == MetaGroupID && e.ObservedHealth.BelowMinReplicas != nil || id == "load" && e.ObservedHealth.BelowMinReplicas == nil {
					t.Fatal("placement applied to wrong group", e)
				}
			}
		})
	}
}
func TestEvaluateGroupHealthBelowMinIndependent(t *testing.T) {
	t.Parallel()
	for lo := 1; lo <= 3; lo++ {
		doc := healthDocument()
		doc.Groups[1].Placement = &PlacementRule{MinReplicas: lo, MaxReplicas: 3}
		e := EvaluateGroupHealth(doc, "load", map[string]Health{"a": HealthDead, "b": HealthAlive, "c": HealthAlive})
		if e.ObservedHealth.State != GroupDegraded || *e.ObservedHealth.BelowMinReplicas != (lo > 2) {
			t.Fatal(e)
		}
	}
}
func TestEvaluateGroupHealthUnknown(t *testing.T) {
	t.Parallel()
	for _, health := range []map[string]Health{nil, {}, {"a": HealthAlive, "b": HealthUnknown, "c": HealthDead}, {"a": HealthAlive, "c": HealthDead}} {
		e := EvaluateGroupHealth(healthDocument(), "load", health)
		if e.Voters != 3 || e.Quorum != 2 || e.ObservedHealth.State != GroupUnknown || e.ObservedHealth.HealthyVoters != nil || e.ObservedHealth.FailureTolerance != nil || e.ObservedHealth.BelowMinReplicas != nil {
			t.Fatal("unknown looks measured", e)
		}
		want := "observed"
		if health == nil {
			want = "none"
		}
		if e.ObservedHealth.Source != want {
			t.Fatal(e)
		}
	}
}
func TestEvaluateDataAndAllGroupHealth(t *testing.T) {
	t.Parallel()
	doc := healthDocument()
	data := EvaluateDataGroupHealth(doc, nil)
	all := EvaluateAllGroupHealth(doc, nil)
	if len(data) != 1 || data[0].GroupID != "load" || len(all) != 2 || all[0].GroupID != MetaGroupID || all[1].GroupID != "load" {
		t.Fatal(data, all)
	}
	if len(EvaluateAllGroupHealth(Document{}, nil)) != 0 {
		t.Fatal("empty topology invented groups")
	}
}
