//go:build !functional

package metaengine

import (
	"encoding/json"
	"slices"
	"testing"
	"time"

	recoveryplacement "github.com/asalimonov/gquorum/internal/recovery/placement"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestRecoveryProvenanceSurvivesOrdinaryLaterDeletionAndPruning(t *testing.T) {
	t.Parallel()
	s := completedRecoveryFixture(t, false)
	consumed := *s.Recovery.Groups[0].Consumed
	desired := s.Latest.Clone()
	desired.Groups = slices.DeleteFunc(desired.Groups, func(group topology.Group) bool { return group.ID == "shard0" })
	body, _ := json.Marshal(ProposeDocument{Doc: desired, ExpectedVersion: s.Latest.Version})
	if result, err := applyScopedProposal(&s, s.AppliedIndex+1, KindProposeDocument, body); err != nil || !result.changed {
		t.Fatal(result, err)
	}
	context, err := s.recoveryPlacementContext()
	if err != nil {
		t.Fatal(err)
	}
	collector := s.planningCollector()
	input, err := collector.collect(s.planningDeclarations(), s.Latest)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := recoveryplacement.BuildPlan(context, input, s.Latest, topology.PlanOptions{Rules: s.Rules})
	if err != nil {
		t.Fatal(err)
	}
	approval := BeginOperationProposal{Version: s.Latest.Version, Plan: plan, DeletionSemantics: DeletionSemanticsDetached,
		Actor: "operator", Origin: topology.OriginOperator, At: time.Unix(130, 0).UTC(), ProposalID: "delete-after-recovery",
		Scopes: s.capturePlanScopes(topology.AffectedGroups(s.Target, s.Latest))}
	body, _ = json.Marshal(approval)
	if result, err := applyScopedProposal(&s, s.AppliedIndex+1, KindBeginOperation, body); err != nil || !result.changed {
		t.Fatal(result, err)
	}
	if s.Target.Group("shard0") != nil || len(s.Cleanup) != 1 || s.Cleanup[0].Identity != consumed.Identity {
		t.Fatal("ordinary deletion lost exact recovered B ownership", s.Cleanup)
	}
	if err := s.pruneRetainedProofs(); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var restored scopedState
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	if restored.Recovery.Phase != "complete" || restored.Recovery.Groups[0].Consumed.Identity != consumed.Identity {
		t.Fatal("later deletion resurrected or discarded recovery provenance")
	}
	source := storage.Identity{ClusterID: restored.Recovery.Source.ClusterID, NodeName: "n1", GroupID: "shard0", ReplicaID: 1}
	if restored.identityReferencedLocked(source) {
		t.Fatal("new lineage acquired authority over old A identity")
	}
}
