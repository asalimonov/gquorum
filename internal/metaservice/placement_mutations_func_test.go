//go:build functional

package metaservice

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestTypedNodeAdmissionHonorsSelectorReservationAndLearnerOnly(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	doc := f.g.state().Latest()
	for i := range doc.Nodes {
		doc.Nodes[i].Labels = map[string]string{"network": "public_ip"}
	}
	doc.Placement.MaxReplicas = 4
	doc.Placement.NodeSelector = map[string]string{"network": "public_ip"}
	f.commit(t, doc, true)
	for _, tc := range []struct {
		name   string
		labels map[string]string
	}{
		{"selector-mismatch", map[string]string{"network": "private"}},
		{"reserved", map[string]string{"network": "public_ip", topology.LabelReserved: "true"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := f.g.AddNode(t.Context(), topology.Node{Name: "n4", Type: "worker", Labels: tc.labels}, preview())
			if err != nil {
				t.Fatal(err)
			}
			if r.Document.MemberID("n4") == 0 || len(r.Document.Group("work").Replicas) != 3 {
				t.Fatal("ineligible node gained load placement or missed meta admission", r.Document)
			}
		})
	}
	r, err := f.g.AddNode(t.Context(), topology.Node{Name: "n4", Type: "worker", Labels: map[string]string{"network": "public_ip", topology.LabelLearnerOnly: "true"}}, preview())
	if err != nil {
		t.Fatal(err)
	}
	meta := r.Document.MetaGroup()
	for _, replica := range meta.Replicas {
		if replica.NodeName == "n4" && replica.Role != topology.ReplicaLearner {
			t.Fatal("learner-only node became a meta voter", replica)
		}
	}
	if len(r.Document.VotersOf("work")) != 3 || len(r.Document.Group("work").Replicas) != 4 {
		t.Fatal("learner-only host was promoted or omitted", r.Document.Group("work").Replicas)
	}
}

func TestTypedGroupPlacementPreviewMatchesPurePreparation(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	views := f.g.state().Views()
	latest, target := views.Latest, views.Target
	rule := topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3, NodeSelector: map[string]string{"network": "public_ip"}}
	edited := latest.Clone()
	edited.Node("n2").Labels = map[string]string{"network": "public_ip"}
	edited.Node("n3").Labels = map[string]string{"network": "public_ip"}
	// A direct label edit first makes the selector feasible and is committed as
	// a complete document; the typed rule then uses those same approved labels.
	f.commit(t, edited, true)
	views = f.g.state().Views()
	latest, target = views.Latest, views.Target
	edited = latest.Clone()
	edited.Group("work").Placement = &rule
	edited.ChangedBy = "n1"
	preparation, err := topology.PreparePlacement(topology.ReadView{Latest: latest, Target: target}, edited, f.g.rules, time.Now().UTC())
	pure := preparation.Document
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.g.SetGroupPlacement(t.Context(), "work", rule, preview())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r.Document.Group("work").Replicas, pure.Group("work").Replicas) {
		// Timestamps differ, but identities, roles and pins must match.
		got, want := r.Document.Group("work").Replicas, pure.Group("work").Replicas
		if len(got) != len(want) {
			t.Fatal("typed and pure host counts differ", got, want)
		}
		for i := range got {
			if got[i].NodeName != want[i].NodeName || got[i].ReplicaID != want[i].ReplicaID || got[i].Role != want[i].Role || got[i].PinnedLearner != want[i].PinnedLearner {
				t.Fatal("typed preview diverged from pure preparation", got, want)
			}
		}
	}
}

func TestReservedIdentityCannotBeReseatedOrReadmittedInPlace(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	doc := f.g.state().Latest()
	doc.Node("n3").Labels = map[string]string{topology.LabelReserved: "true"}
	f.commit(t, doc, true)
	before := f.g.state().Latest()
	if _, err := f.g.ReseatReplica(t.Context(), "work", topology.ReplicaKey{NodeName: "n3", ReplicaID: 3}, preview()); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("reseat minted a new load identity on reserved node", err)
	}
	f.g.opts.Health = mutationHealth{"n3": topology.HealthDead}
	if _, err := f.g.ReadmitNode(t.Context(), "n3", 3, preview()); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("readmission minted a new load identity on reserved node", err)
	}
	after := f.g.state().Latest()
	if !reflect.DeepEqual(before, after) {
		t.Fatal("failed reserved mutation changed topology")
	}
}
