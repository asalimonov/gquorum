//go:build functional

package metaservice

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

func TestControllerFailureApprovalReplayKeepsOriginalAttempt(t *testing.T) {
	t.Parallel()
	g := readyPublic(t, singleMetaFixtureWith(t, func(o *Options) { o.Limits.MaxNodes = 3 }))
	g.controller.Stop()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	doc := fixtureConvergedDocument(t, g.state())
	doc.Nodes = append(doc.Nodes, topology.Node{Name: "n2", Type: "worker", State: topology.NodeActive})
	doc.MetaGroup().Replicas = append(doc.MetaGroup().Replicas, topology.Replica{NodeName: "n2", ReplicaID: 2, Role: topology.ReplicaLearner})
	doc.MetaGroup().NextReplicaID = 3
	staged, err := g.Stage(ctx, doc, doc.Version)
	if err != nil {
		t.Fatal(err)
	}
	// Model an approved plan whose assumptions no longer cover native state.
	// The real single-participant meta engine still has only voter 1.
	result, err := g.propose(ctx, metaengine.BeginOperationProposal{Version: staged.Version,
		Plan:   topology.Plan{Version: staged.Version, Blocks: []topology.PlanBlock{{GroupID: topology.MetaGroupID, SourceVersion: 1}}},
		Scopes: []metaengine.ScopeExpectation{{GroupID: topology.MetaGroupID, SourceVersion: 1}}, At: time.Now().UTC(), ProposalID: newProposalID()})
	if err != nil {
		t.Fatal(err)
	}
	g.controller.Start(g.workCtx)
	failed, err := g.WaitOperation(ctx, result.OperationID)
	g.controller.Stop()
	if err != nil || failed.State != topology.OperationFailed || len(failed.Blocks) != 1 || !strings.Contains(failed.Blocks[0].Error, "replan_required") || g.state().Views().FullyConverged.Version != 1 {
		t.Fatal(failed, err)
	}
	retry, err := g.Approve(ctx, staged.Version)
	if err != nil {
		t.Fatal(err)
	}
	ex, readErr := g.state().Operation(retry)
	if readErr != nil || retry != result.OperationID || ex.ID != retry || ex.Attempt != 1 || len(ex.Blocks) != 1 || ex.Blocks[0].TotalSteps != 0 {
		t.Fatal("approval replay created a retry or rebuilt actions", ex, err)
	}
	failed, err = g.Operation(ctx, retry)
	if err != nil || failed.State != topology.OperationFailed || len(failed.Blocks) != 1 || !strings.Contains(failed.Blocks[0].Error, "replan_required") {
		t.Fatal(failed, err)
	}
	// A changed metadata scope may explicitly authorize the missing prepare/add sequence.
	staged.Node("n1").Labels = map[string]string{"repair": "new-plan"}
	staged, err = g.Stage(ctx, staged, staged.Version)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := g.PlanFor(ctx, staged)
	if err != nil || len(plan.Steps) != 2 || plan.Steps[0].Kind != topology.StepPrepareReplica || plan.Steps[1].Kind != topology.StepAddLearner {
		t.Fatal(plan, err)
	}
}
