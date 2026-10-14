//go:build functional

package metaservice

import (
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestVoterConfigAdoptionPreservesIntentAndExactPins(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	old := f.g.state().Latest()
	old.Group("work").Replicas[2].Role = topology.ReplicaLearner
	f.commit(t, old, true)
	p := topology.DefaultVoterConfig()
	r, err := f.g.SetGroupVoterConfig(t.Context(), "work", p, preview())
	d := mutationDoc(t, r, err)
	g := d.Group("work")
	if !g.Replicas[2].PinnedLearner || g.Replicas[2].Role != topology.ReplicaLearner || len(d.VotersOf("work")) != 2 || g.Hosting.Mode != topology.HostingExplicit || !slices.Equal(g.Hosting.Nodes, []string{"n1", "n2", "n3"}) {
		t.Fatal("adoption changed intent", g)
	}
	f.commit(t, d, true)
	before := f.g.state().Latest()
	if _, err := f.g.SetReplicaPinnedLearner(t.Context(), "work", topology.LearnerPin{Replica: topology.ReplicaKey{NodeName: "n3", ReplicaID: 99}}, preview()); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("successor pin accepted", err)
	}
	r, err = f.g.SetReplicaPinnedLearner(t.Context(), "work", topology.LearnerPin{Replica: topology.ReplicaKey{NodeName: "n3", ReplicaID: 3}}, preview())
	d = mutationDoc(t, r, err)
	if d.Group("work").Replicas[2].PinnedLearner || d.Group("work").Replicas[2].Role != topology.ReplicaLearner {
		t.Fatal("unpin promoted learner")
	}
	if !reflect.DeepEqual(before, f.g.state().Latest()) {
		t.Fatal("preview staged intent")
	}
	f.commit(t, d, true)
	r, err = f.g.ChangeReplica(t.Context(), "work", topology.ReplicaRoleChange{NodeName: "n3", Role: topology.ReplicaLearner}, preview())
	d = mutationDoc(t, r, err)
	if !d.Group("work").Replicas[2].PinnedLearner {
		t.Fatal("manual learner not pinned")
	}
}

func TestVoterConfigAdmissionAddsLearnersWithoutExpandingExplicitHosts(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	p := topology.DefaultVoterConfig()
	r, err := f.g.SetGroupVoterConfig(t.Context(), topology.MetaGroupID, p, preview())
	f.commit(t, mutationDoc(t, r, err), true)
	r, err = f.g.CreateLoadGroup(t.Context(), GroupSpec{ID: "all"}, preview())
	f.commit(t, mutationDoc(t, r, err), true)
	r, err = f.g.CreateLoadGroup(t.Context(), GroupSpec{ID: "subset", Nodes: []string{"n2"}}, preview())
	f.commit(t, mutationDoc(t, r, err), true)
	r, err = f.g.AddNode(t.Context(), topology.Node{Name: "n4", Type: "worker"}, preview())
	d := mutationDoc(t, r, err)
	for _, id := range []string{topology.MetaGroupID, "all"} {
		group := d.Group(id)
		if len(group.Replicas) != 4 || len(d.VotersOf(id)) != 3 || group.Replicas[3].Role != topology.ReplicaLearner || group.Replicas[3].PinnedLearner {
			t.Fatal("admission occupied voter seat", group)
		}
	}
	if len(d.Group("subset").Replicas) != 1 || d.Group("subset").Hosting.Mode != topology.HostingExplicit {
		t.Fatal("explicit hosting expanded")
	}
	f.commit(t, d, true)
	r, err = f.g.CreateLoadGroup(t.Context(), GroupSpec{ID: "fresh"}, preview())
	d = mutationDoc(t, r, err)
	if len(d.Group("fresh").Replicas) != 4 || len(d.VotersOf("fresh")) != 3 {
		t.Fatal("fresh hosting/voters", d.Group("fresh"))
	}
}

func TestAllHostOverflowRefusesWholeMutation(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	f.g.rules.Limits.MaxReplicas = 3
	r, err := f.g.CreateLoadGroup(t.Context(), GroupSpec{ID: "all"}, preview())
	f.commit(t, mutationDoc(t, r, err), true)
	before := f.g.state().Latest()
	if _, err := f.g.AddNode(t.Context(), topology.Node{Name: "n4", Type: "worker"}, preview()); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("overflow accepted", err)
	}
	if !reflect.DeepEqual(before, f.g.state().Latest()) {
		t.Fatal("overflow allocated identities or partially staged")
	}
}
