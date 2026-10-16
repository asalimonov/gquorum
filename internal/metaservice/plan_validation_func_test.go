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
	doc := g.state().Active()
	doc.Nodes = append(doc.Nodes, topology.Node{Name: "n2", Type: "worker", State: topology.NodeActive})
	doc.MetaGroup().Replicas = append(doc.MetaGroup().Replicas, topology.Replica{NodeName: "n2", ReplicaID: 2, Role: topology.ReplicaLearner})
	doc.MetaGroup().NextReplicaID = 3
	staged, err := g.Stage(ctx, doc, doc.Version)
	if err != nil {
		t.Fatal(err)
	}
	// Model an approved plan whose assumptions no longer cover native state.
	// The real single-participant meta engine still has only voter 1.
	result, err := g.propose(ctx, metaengine.BeginOperatorProposal{Version: staged.Version, Plan: topology.Plan{Version: staged.Version}, At: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	g.controller.Tick(ctx)
	failed, err := g.Operator(ctx, result.OperatorID)
	if err != nil || failed.State != topology.OperatorFailed || !strings.Contains(failed.Error, "replan_required") || g.state().Active().Version != 1 {
		t.Fatal(failed, err)
	}
	retry, err := g.Apply(ctx, staged.Version)
	if err != nil {
		t.Fatal(err)
	}
	ex, ok := g.state().CurrentOperator()
	if !ok || retry != result.OperatorID || ex.Status.ID != retry || ex.Status.Attempt != 1 || len(ex.Plan.Steps) != 0 {
		t.Fatal("approval replay created a retry or rebuilt actions", ex, err)
	}
	g.controller.Tick(ctx)
	failed, err = g.Operator(ctx, retry)
	if err != nil || failed.State != topology.OperatorFailed || !strings.Contains(failed.Error, "replan_required") {
		t.Fatal(failed, err)
	}
	// A new version may explicitly authorize the missing prepare/add sequence.
	staged, err = g.Stage(ctx, staged, staged.Version)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := g.PlanFor(ctx, staged)
	if err != nil || len(plan.Steps) != 2 || plan.Steps[0].Kind != topology.StepPrepareReplica || plan.Steps[1].Kind != topology.StepAddLearner {
		t.Fatal(plan, err)
	}
}
