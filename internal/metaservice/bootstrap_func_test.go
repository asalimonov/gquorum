//go:build functional

package metaservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
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

func TestFirstSessionUsesCompletedMemberAcrossRetainedProcessRestart(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	doc := f.g.machine.Latest()
	doc.Comment, doc.Origin = "unrelated approval before the late founder", topology.OriginAPI
	f.commit(t, doc, true)
	identity := topology.Identity{ClusterID: doc.ClusterID, NodeName: "n1", MemberID: 1, InstanceID: "first"}
	founding, err := sessionAdmissionBasis(f.g.machine, identity)
	if err != nil || founding != "exec-v1-a1" {
		t.Fatal("late founder lost original metadata proof", founding, err)
	}
	f.command(t, metaengine.BindAdmissionProposal{Founding: true, At: time.Now().UTC(), Admission: metaengine.AdmissionRecord{
		NodeName: identity.NodeName, MemberID: identity.MemberID, InstanceID: identity.InstanceID,
		OperatorID: topology.OperatorID(founding)}})
	// Crash after binding, before any session. A retained restart supplies a new
	// process ID and must neither rebind the seed nor wait on the old instance.
	identity.InstanceID = "retained-restart"
	if f.g.machine.HasActivated(identity.NodeName) {
		t.Fatal("fixture already activated a session")
	}
	if id, err := sessionAdmissionBasis(f.g.machine, identity); err != nil || id != "" {
		t.Fatal("completed member required another founding bind", id, err)
	}
	// Completed admission does not bypass the process lock or exact member check.
	f.command(t, metaengine.GrantSession{NodeName: "n1", InstanceID: "another-live-process", SessionID: "held", TTLSeconds: 30})
	f.command(t, metaengine.AcquireLock{Key: "node/n1", SessionID: "held"})
	if err := selectSessionIdentity(&sessionActivation{identity: identity, machine: f.g.machine}); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("retained membership bypassed process ownership", err)
	}
	identity.MemberID++
	if _, err := sessionAdmissionBasis(f.g.machine, identity); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("prior completion admitted a different member", err)
	}
}
