//go:build functional

package metaservice

import (
	"context"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func TestFoundingPlanUsesOnlyNativeSeedAndEmptyProvenInput(t *testing.T) {
	t.Parallel()
	g := readyPublic(t, singleMetaFixture(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	doc := g.state().Target()
	load := *doc.MetaGroup()
	load.ID, load.Storage, load.VoterConfig = "failed-load", topology.StorageDefault, nil
	doc.Groups = append(doc.Groups, load)
	// No load controller exists. Bootstrap must still produce its founding work
	// rather than trying to observe it or treating its declaration as proven.
	plan, err := g.foundingPlan(ctx, doc)
	if err != nil || len(plan.Steps) != 1 || plan.Steps[0].GroupID != load.ID ||
		plan.Steps[0].Kind != topology.StepCreateFoundingGroup {
		t.Fatal(plan, err)
	}
	wrong := doc.Clone()
	wrong.MetaGroup().Replicas[0].ReplicaID = 8
	wrong.MetaGroup().NextReplicaID = 9
	if _, err := g.foundingPlan(ctx, wrong); err == nil {
		t.Fatal("accepted a different seed member")
	}
	identity := g.currentIdentity()
	identity.MemberID++
	if _, ready := g.state().AdmissionBasis(identity); ready {
		t.Fatal("admission accepted a foreign member")
	}
}
