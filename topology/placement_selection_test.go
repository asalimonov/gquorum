//go:build !functional

package topology

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
)

func TestAffectedPlacementGroupsIgnoresDescriptiveLabels(t *testing.T) {
	t.Parallel()
	before := testDocument()
	next := before.Clone()
	if err := next.Group("load").SetLabels(map[string]string{"purpose": "accounting"}); err != nil {
		t.Fatal(err)
	}
	if got := AffectedPlacementGroups(before, next); len(got) != 0 {
		t.Fatalf("descriptive labels moved groups: %v", got)
	}
	next.Node("a").Labels[LabelLearnerOnly] = "true"
	if got := AffectedPlacementGroups(before, next); !reflect.DeepEqual(got, []string{MetaGroupID, "load"}) {
		t.Fatalf("learner-only did not affect both groups: %v", got)
	}
	next = before.Clone()
	next.Group("load").Placement.NodeSelector = map[string]string{"network": "public_ip"}
	if got := AffectedPlacementGroups(before, next); !reflect.DeepEqual(got, []string{"load"}) {
		t.Fatalf("selector edit did not affect load group: %v", got)
	}
}

func TestRequiredLoadHostsForPreservesOnlyApprovedReservation(t *testing.T) {
	t.Parallel()
	approved := testDocument()
	desired := approved.Clone()
	p := DefaultVoterConfig()
	p.TargetVoters = 1
	desired.Group("load").VoterConfig = &p
	desired.Group("load").Hosting = &HostingRule{Mode: HostingAllEligible}
	desired.Node("a").Labels[LabelReserved] = "true"
	hosts, err := RequiredLoadHostsFor(approved, desired, "load")
	if err != nil || !reflect.DeepEqual(hosts, []string{"a", "c"}) {
		t.Fatalf("approved exact reserved host lost: %v %v", hosts, err)
	}
	desired.Group("load").Replicas[0].ReplicaID = 9
	hosts, err = RequiredLoadHostsFor(approved, desired, "load")
	if err != nil || !reflect.DeepEqual(hosts, []string{"c"}) {
		t.Fatalf("fresh reserved identity accepted: %v %v", hosts, err)
	}
	desired.Node("a").Labels = map[string]string{"rack": "1"}
	hosts, err = RequiredLoadHostsFor(approved, desired, "load")
	if err != nil || !reflect.DeepEqual(hosts, []string{"a", "c"}) {
		t.Fatalf("reservation release did not reopen host: %v %v", hosts, err)
	}
	rules := testRules()
	rules.Limits.MaxReplicas = 1
	if err := ValidateHostingCapacityFor(approved, desired, rules); !errors.Is(err, fault.ErrInvalid) {
		t.Fatalf("oversized hosting set silently truncated: %v", err)
	}
}

func TestStrictFoundingRequiresCompleteDistinctVoterTarget(t *testing.T) {
	t.Parallel()
	doc := testDocument()
	doc.Group("load").Placement = nil
	doc.Placement.MaxReplicas = 3
	for i := range doc.Nodes {
		doc.Nodes[i].Type = "worker"
	}
	doc.Node("a").Labels["az"] = "one"
	doc.Node("b").Labels = map[string]string{"az": "two"}
	doc.Node("c").Labels = map[string]string{"az": "two"}
	p := DefaultVoterConfig()
	p.DistinctVoterLabel = "az"
	doc.Group("load").Replicas = nil
	doc.Group("load").VoterConfig = &p
	doc.Group("load").Hosting = &HostingRule{Mode: HostingAllEligible}
	if _, err := SelectFoundingRoles(doc, "load", nil); !errors.Is(err, fault.ErrInvalid) {
		t.Fatalf("two AZs founded strict three-voter group: %v", err)
	} else {
		var placement *PlacementError
		if !errors.As(err, &placement) || len(placement.Report.Groups) != 1 {
			t.Fatalf("missing unsatisfied placement report: %v", err)
		}
	}
	doc.Node("c").Labels["az"] = "three"
	roles, err := SelectFoundingRoles(doc, "load", nil)
	if err != nil || len(roles) != 3 {
		t.Fatalf("three AZs rejected: %v %v", roles, err)
	}
	for _, role := range roles {
		if role != ReplicaVoter {
			t.Fatalf("strict founding created learner despite three AZs: %v", roles)
		}
	}
	if _, err := SelectFoundingRoles(doc, "load", map[string]bool{"b": true}); !errors.Is(err, fault.ErrInvalid) {
		t.Fatalf("pin bypassed strict target: %v", err)
	}
	doc.Group("load").VoterConfig.DistinctVoterLabel = ""
	roles, err = SelectFoundingRoles(doc, "load", map[string]bool{"b": true})
	if err != nil || roles["b"] != ReplicaLearner {
		t.Fatalf("non-strict pin not preserved: %v %v", roles, err)
	}
}

func TestVoterReconciliationPrefersRetainedThenExistingLearner(t *testing.T) {
	t.Parallel()
	before := testDocument()
	before.Node("b").Type = "worker"
	next := before.Clone()
	p := DefaultVoterConfig()
	p.TargetVoters = 1
	next.Group("load").VoterConfig = &p
	next.Group("load").Hosting = &HostingRule{Mode: HostingAllEligible}
	next.Group("load").Replicas = append(next.Group("load").Replicas, Replica{NodeName: "b", ReplicaID: 3, Role: ReplicaLearner})
	next.Group("load").NextReplicaID = 4
	if err := reconcileGroupVoters(before, &next, "load", time.Now()); err != nil {
		t.Fatal(err)
	}
	if next.Group("load").Replicas[0].Role != ReplicaVoter {
		t.Fatal("healthy retained voter was replaced")
	}
	next.Node("a").Labels[LabelLearnerOnly] = "true"
	if err := reconcileGroupVoters(before, &next, "load", time.Now()); err != nil {
		t.Fatal(err)
	}
	if next.Group("load").Replicas[0].Role != ReplicaLearner || next.Group("load").Replicas[1].Role != ReplicaVoter || next.Group("load").Replicas[2].Role != ReplicaLearner {
		t.Fatalf("did not prefer existing learner: %#v", next.Group("load").Replicas)
	}
	next.Group("load").Replicas[1].PinnedLearner = true
	if err := reconcileGroupVoters(before, &next, "load", time.Now()); err != nil {
		t.Fatal(err)
	}
	if next.Group("load").Replicas[2].Role != ReplicaVoter || next.Group("load").Replicas[1].Role != ReplicaLearner {
		t.Fatal("pin did not force new host selection")
	}
}

func TestLegacyLearnerIsNotPromotedByRestriction(t *testing.T) {
	t.Parallel()
	before := testDocument()
	next := before.Clone()
	next.Node("a").Labels[LabelLearnerOnly] = "true"
	if err := reconcileGroupVoters(before, &next, "load", time.Now()); !errors.Is(err, fault.ErrInvalid) {
		t.Fatalf("legacy learner silently promoted: %v", err)
	}
	if next.Group("load").Replicas[1].Role != ReplicaLearner {
		t.Fatal("failed reconciliation changed legacy learner")
	}
}

func TestReservationReleaseAddsLearnerWithoutMovingVoters(t *testing.T) {
	t.Parallel()
	approved := testDocument()
	approved.Node("b").Type = "worker"
	p := DefaultVoterConfig()
	p.TargetVoters = 1
	approved.Group("load").VoterConfig = &p
	approved.Group("load").Hosting = &HostingRule{Mode: HostingAllEligible}
	desired := approved.Clone()
	desired.Node("b").Labels = map[string]string{LabelReserved: "true"}
	if err := materializeHosting(approved, &desired, desired.Group("load"), time.Now()); err != nil {
		t.Fatal(err)
	}
	if len(desired.Group("load").Replicas) != 2 {
		t.Fatalf("reserved node was assigned: %#v", desired.Group("load").Replicas)
	}
	delete(desired.Node("b").Labels, LabelReserved)
	if err := materializeHosting(approved, &desired, desired.Group("load"), time.Now()); err != nil {
		t.Fatal(err)
	}
	replicas := desired.Group("load").Replicas
	if len(replicas) != 3 || replicas[0].NodeName != "a" || replicas[0].ReplicaID != 1 || replicas[0].Role != ReplicaVoter ||
		replicas[1].NodeName != "b" || replicas[1].Role != ReplicaLearner || replicas[1].ReplicaID < 3 ||
		replicas[2].NodeName != "c" || replicas[2].ReplicaID != 2 {
		t.Fatalf("release changed retained seats or skipped learner: %#v", replicas)
	}
}
