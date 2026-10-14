//go:build functional

package metaservice

import (
	"errors"
	"os"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestFreshMetaFoundingPreflightRejectsStrictZoneShortageBeforeStorage(t *testing.T) {
	t.Parallel()
	o := optionsForTest(t)
	o.Founders[0].Node.Labels = map[string]string{"zone": "a"}
	o.Founders = append(o.Founders, FoundingMember{Node: topology.Node{Name: "n2", Type: "worker", State: topology.NodeActive, Labels: map[string]string{"zone": "a"}}, MemberID: 2, Address: "127.0.0.1:2222"})
	voterConfig := topology.DefaultVoterConfig()
	voterConfig.TargetVoters, voterConfig.DistinctVoterLabel = 2, "zone"
	o.VoterConfig = &voterConfig
	_, err := New(o)
	if !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("strict meta founding accepted one availability zone", err)
	}
	var placement *topology.PlacementError
	if !errors.As(err, &placement) || len(placement.Report.Groups) == 0 || len(placement.Report.Groups[0].Issues) == 0 {
		t.Fatal("missing unsatisfied-placement explanation", err)
	}
	if _, err := os.Stat(o.DBPath); !os.IsNotExist(err) {
		t.Fatal("rejected founding changed meta storage", err)
	}
}

func TestFreshMetaFoundingLearnerOnlyIsUsedByPeerSelection(t *testing.T) {
	t.Parallel()
	o := optionsForTest(t)
	o.Founders[0].Node.Labels = map[string]string{topology.LabelLearnerOnly: "true"}
	o.Founders = append(o.Founders, FoundingMember{Node: topology.Node{Name: "n2", Type: "worker", State: topology.NodeActive}, MemberID: 2, Address: "127.0.0.1:2222"})
	roles, err := o.defaults().selectFoundingRoles()
	if err != nil {
		t.Fatal(err)
	}
	if roles["n1"] != topology.ReplicaLearner || roles["n2"] != topology.ReplicaVoter {
		t.Fatal("learner-only founder was selected to vote", roles)
	}
}

func TestFreshMetaFoundingRejectsOnlyLearnerBeforeStorage(t *testing.T) {
	t.Parallel()
	o := optionsForTest(t)
	o.Founders[0].Node.Labels = map[string]string{topology.LabelLearnerOnly: "true"}
	_, err := New(o)
	if !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("learner-only meta cluster was founded", err)
	}
	var placement *topology.PlacementError
	if !errors.As(err, &placement) || len(placement.Report.Groups) == 0 {
		t.Fatal("missing founding voter explanation", err)
	}
	if _, err := os.Stat(o.DBPath); !os.IsNotExist(err) {
		t.Fatal("rejected founding changed meta storage", err)
	}
}

func TestLoadFoundingStrictPreflightAndReservedHosts(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	doc := f.g.state().Latest()
	for i := range doc.Nodes {
		doc.Nodes[i].Labels = map[string]string{"zone": "a"}
	}
	f.commit(t, doc, true)
	voterConfig := topology.DefaultVoterConfig()
	voterConfig.TargetVoters, voterConfig.DistinctVoterLabel = 2, "zone"
	spec := GroupSpec{ID: "strict", VoterConfig: &voterConfig}
	rejected, err := f.g.CreateLoadGroup(t.Context(), spec, preview())
	if !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("strict load founding accepted one zone", err)
	}
	if rejected.Placement == nil || len(rejected.Placement.Groups) == 0 || len(rejected.Placement.Groups[0].Issues) == 0 {
		t.Fatal("preview dropped unsatisfied placement report", rejected, err)
	}
	latest := f.g.state().Latest()
	if latest.Group("strict") != nil {
		t.Fatal("failed preflight staged an incomplete group")
	}
	doc = f.g.state().Latest()
	doc.Nodes[1].Labels["zone"] = "b"
	doc.Nodes[2].Labels[topology.LabelReserved] = "true"
	f.commit(t, doc, true)
	r, err := f.g.CreateLoadGroup(t.Context(), spec, preview())
	if err != nil {
		t.Fatal(err)
	}
	if r.Placement == nil || r.Placement.Version != r.Document.Version || len(r.Placement.Groups) == 0 {
		t.Fatal("successful preview omitted placement report", r)
	}
	group := r.Document.Group("strict")
	if len(group.Replicas) != 2 || group.Replicas[0].NodeName != "n1" || group.Replicas[1].NodeName != "n2" || group.Replicas[0].Role != topology.ReplicaVoter || group.Replicas[1].Role != topology.ReplicaVoter {
		t.Fatal("fresh load placement included reserved node or wrong voters", group.Replicas)
	}
}
