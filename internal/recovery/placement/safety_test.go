//go:build !functional

package placement

import (
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/recovery/model"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func twoNodeFixture(t *testing.T) (topology.Document, topology.Rules, Context) {
	t.Helper()
	doc, rules, state := placementFixture(t)
	doc.Nodes = append(doc.Nodes, topology.Node{Name: "n2", Type: "worker", State: topology.NodeActive, Labels: map[string]string{"az": "b"}})
	for i := range doc.Groups {
		doc.Groups[i].Replicas = append(doc.Groups[i].Replicas, topology.Replica{NodeName: "n2", ReplicaID: 6, Role: topology.ReplicaLearner})
		doc.Groups[i].NextReplicaID = 7
	}
	ctx, err := FromImport(state, doc)
	if err != nil {
		t.Fatal(err)
	}
	return doc, rules, ctx
}

func TestRecoveryInitialImportAndLegacyPolicyHaveNarrowCountAllowance(t *testing.T) {
	t.Parallel()
	desired, rules, state := placementFixture(t)
	state.Phase = "importing"
	base := desired.Clone()
	base.Version, base.ParentVersion, base.Origin = 1, 0, topology.OriginFounding
	base.Groups = base.Groups[:1]
	base.Groups[0].VoterConfig.TargetVoters = 1
	base.Groups[0].VoterConfig.DistinctVoterLabel = ""
	base.Placement.MinReplicas = 1
	ctx, err := FromImport(state, desired)
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(ctx, base, base, desired, rules); err != nil {
		t.Fatal("exact v1 singleton import refused", err)
	}
	if _, err := BuildPlan(ctx, nativeInput(base), desired, topology.PlanOptions{Rules: rules}); err != nil {
		t.Fatal("ordinary initial group founding plan refused", err)
	}
	foreign := base.Clone()
	foreign.MetaGroup().Replicas[0].ReplicaID++
	if err := Validate(ctx, foreign, foreign, desired, rules); err == nil {
		t.Fatal("initial exception borrowed a different founder")
	}
	legacy, _, legacyState := placementFixture(t)
	legacy.Group("work").VoterConfig = nil
	legacy.Group("work").Hosting = nil
	for i := range legacyState.Policies {
		digest, err := PolicyDigest(legacy, legacyState.Policies[i].GroupID)
		if err != nil {
			t.Fatal(err)
		}
		legacyState.Policies[i].PolicyDigest = digest
	}
	legacyCtx, err := FromImport(legacyState, legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckLoadBounds(legacyCtx, legacy, legacy); err != nil {
		t.Fatal("legacy deficit refused", err)
	}
	if _, err := Prepare(legacyCtx, topology.ReadView{Target: legacy, Latest: legacy}, legacy, rules, time.Unix(101, 0).UTC()); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryUnrelatedEditAndExplicitPolicyEditKeepTheirScopes(t *testing.T) {
	t.Parallel()
	base, rules, ctx := twoNodeFixture(t)
	unchanged := base.Clone()
	unchanged.Version++
	unchanged.ParentVersion = base.Version
	unchanged.Group("work").Labels = map[string]string{"description": "unrelated"}
	if err := Validate(ctx, base, base, unchanged, rules); err != nil {
		t.Fatal("unrelated edit borrowed global convergence", err)
	}
	edited := base.Clone()
	edited.Version++
	edited.ParentVersion = base.Version
	edited.Group("work").Placement = &topology.PlacementRule{MinReplicas: 1, MaxReplicas: 5}
	edited.Group("work").VoterConfig.TargetVoters = 1
	if err := Validate(ctx, base, base, edited, rules); err != nil {
		t.Fatal("normally valid policy edit refused by unrelated deficit", err)
	}
	if ctx.applies(edited, "work") {
		t.Fatal("changed policy retained its old allowance")
	}
	progress := topology.GroupProgress{GroupID: "work", Converged: edited.Group("work"), ConvergedVersion: edited.Version}
	if !CanRelease(ctx, progress, edited) {
		t.Fatal("normally valid converged policy edit did not close exception")
	}
}

func nativeInput(doc topology.Document) topology.PlanInput {
	in := topology.PlanInput{Approved: doc.Clone(), Groups: make(map[string]topology.GroupProgress), Nodes: make(map[string]topology.NodeProgress), Native: make(map[string]topology.NativeState)}
	for i := range doc.Groups {
		group := doc.Groups[i]
		conf := raftpb.ConfState{}
		live := make(map[uint64]bool)
		for _, replica := range group.Replicas {
			live[replica.ReplicaID] = true
			if replica.Role == topology.ReplicaVoter {
				conf.Voters = append(conf.Voters, replica.ReplicaID)
			} else {
				conf.Learners = append(conf.Learners, replica.ReplicaID)
			}
		}
		in.Groups[group.ID] = topology.GroupProgress{GroupID: group.ID, Converged: &group, ConvergedVersion: doc.Version,
			Observation: &topology.GroupStatus{GroupID: group.ID, Found: true, HealthKnown: true, LeaderID: 5, Term: 2, Commit: 10, Applied: 10, ConfState: conf, LiveVoters: live}}
		in.Native[group.ID] = topology.NativeState{GroupID: group.ID, ReplicaID: 5, Applied: 10, ConfState: conf}
	}
	for i := range doc.Nodes {
		node := doc.Nodes[i]
		in.Nodes[node.Name] = topology.NodeProgress{NodeName: node.Name, Converged: &node, ConvergedVersion: doc.Version, MemberID: doc.MemberID(node.Name)}
	}
	return in
}

func TestRecoveryPlanRetainsCatchupAndNativeQuorumGuards(t *testing.T) {
	t.Parallel()
	base, rules, ctx := twoNodeFixture(t)
	desired := base.Clone()
	desired.Version++
	desired.ParentVersion = base.Version
	desired.Group("work").Replicas[1].Role = topology.ReplicaVoter
	input := nativeInput(base)
	before := mustJSON(t, input)
	plan, err := BuildPlan(ctx, input, desired, topology.PlanOptions{Rules: rules})
	if err != nil {
		t.Fatal(err)
	}
	var kinds []topology.StepKind
	for _, step := range plan.Steps {
		if step.GroupID == "work" {
			kinds = append(kinds, step.Kind)
		}
	}
	if !reflect.DeepEqual(kinds, []topology.StepKind{topology.StepWaitCaughtUp, topology.StepPromoteVoter}) {
		t.Fatal("normal promotion guards changed", plan)
	}
	if err := ValidatePlan(ctx, input, desired, plan); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, mustJSON(t, input)) {
		t.Fatal("normalized analysis changed owned input")
	}
	input.Groups["work"].Observation.LiveVoters[5] = false
	if _, err := BuildPlan(ctx, input, desired, topology.PlanOptions{Rules: rules}); err == nil {
		t.Fatal("recovery bypassed lost native quorum")
	}
}

func TestRecoveryDeficitCannotWaivePinsDomainsOrLastVoter(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"last voter", "worsened deficit", "same domain", "pinned learner", "learner only", "selector", "forged policy"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			base, rules, ctx := twoNodeFixture(t)
			desired := base.Clone()
			desired.Version++
			desired.ParentVersion = base.Version
			switch scenario {
			case "last voter":
				desired.Group("work").Replicas[0].Role = topology.ReplicaLearner
			case "worsened deficit":
				base.Group("work").Replicas[1].Role = topology.ReplicaVoter
			case "same domain":
				desired.Node("n2").Labels["az"] = "a"
				desired.Group("work").Replicas[1].Role = topology.ReplicaVoter
			case "pinned learner":
				desired.Group("work").Replicas[1].PinnedLearner = true
				desired.Group("work").Replicas[1].Role = topology.ReplicaVoter
			case "learner only":
				desired.Node("n2").Labels[topology.LabelLearnerOnly] = "true"
				desired.Group("work").Replicas[1].Role = topology.ReplicaVoter
			case "selector":
				desired.Node("n1").Type = "excluded-type"
				desired.Placement.AllowedNodeTypes = []topology.NodeType{"worker"}
			case "forged policy":
				ctx.policies["work"] = model.PolicyState{GroupID: "work", PolicyDigest: "foreign"}
			}
			if err := Validate(ctx, base, base, desired, rules); err == nil {
				t.Fatal("invalid recovery assignment accepted", scenario)
			}
		})
	}
}

func TestRecoveryReleaseRequiresProvenPolicySatisfaction(t *testing.T) {
	t.Parallel()
	doc, _, state := placementFixture(t)
	ctx, err := FromImport(state, doc)
	if err != nil {
		t.Fatal(err)
	}
	progress := topology.GroupProgress{GroupID: "work", Converged: doc.Group("work"), ConvergedVersion: doc.Version}
	if CanRelease(ctx, progress, doc) {
		t.Fatal("data import completion released an unmet policy")
	}
	for _, node := range []topology.Node{{Name: "n2", Type: "worker", State: topology.NodeActive, Labels: map[string]string{"az": "b"}}, {Name: "n3", Type: "worker", State: topology.NodeActive, Labels: map[string]string{"az": "c"}}} {
		doc.Nodes = append(doc.Nodes, node)
		for i := range doc.Groups {
			group := &doc.Groups[i]
			group.Replicas = append(group.Replicas, topology.Replica{NodeName: node.Name, ReplicaID: group.NextReplicaID, Role: topology.ReplicaVoter})
			group.NextReplicaID++
		}
	}
	progress.Converged = doc.Group("work")
	if !CanRelease(ctx, progress, doc) {
		t.Fatal("proven original policy did not release")
	}
	progress.Converged = nil
	progress.ConvergedVersion = 0
	if CanRelease(ctx, progress, doc) {
		t.Fatal("unknown declaration released policy")
	}
}

func TestZeroContextMatchesOrdinaryPlacementIncludingEmptyFoundingReference(t *testing.T) {
	t.Parallel()
	desired, rules, _ := placementFixture(t)
	for _, base := range []topology.Document{{}, desired} {
		reference := base.Clone()
		if reference.ClusterID == "" {
			reference.ClusterID = desired.ClusterID
		}
		expected := topology.ValidateHostingCapacityFor(reference, desired, rules)
		if expected == nil {
			expected = topology.ValidatePlacementTransition(reference, reference, desired)
		}
		actual := Validate(Context{}, base, base, desired, rules)
		if (expected == nil) != (actual == nil) {
			t.Fatal("ordinary placement result changed", expected, actual)
		}
		if expected != nil && expected.Error() != actual.Error() {
			t.Fatal("ordinary placement refusal changed", expected, actual)
		}
	}
}
