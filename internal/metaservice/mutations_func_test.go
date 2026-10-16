//go:build functional

package metaservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/topology"
)

// These source mutation ports use the real deterministic FSM as their document
// store. Native membership/transport are verified separately by runtime tests.
type mutationFixture struct {
	g     *Group
	index uint64
}

func newMutationFixture(t *testing.T) *mutationFixture {
	t.Helper()
	rules := topology.Rules{SchemaVersion: 1, Limits: topology.Limits{MaxNodes: 5, MaxReplicas: 5}}
	g := &Group{rules: rules, identity: topology.Identity{NodeName: "n1", ClusterID: "cluster"}, machine: metaengine.NewMachine(rules, nil)}
	f := &mutationFixture{g: g}
	doc := topology.Document{ClusterID: "cluster", Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3}, Origin: topology.OriginFounding, Groups: []topology.Group{{ID: topology.MetaGroupID, NextReplicaID: 4}, {ID: "work", NextReplicaID: 4}}}
	for i := 1; i <= 3; i++ {
		name := fmt.Sprintf("n%d", i)
		doc.Nodes = append(doc.Nodes, topology.Node{Name: name, Type: "worker", State: topology.NodeActive})
		for j := range doc.Groups {
			doc.Groups[j].Replicas = append(doc.Groups[j].Replicas, topology.Replica{NodeName: name, ReplicaID: uint64(i), Role: topology.ReplicaVoter})
		}
	}
	f.commit(t, doc, true)
	return f
}
func (f *mutationFixture) command(t *testing.T, proposal any) metaengine.ApplyResult {
	t.Helper()
	data, err := metaengine.Encode(proposal)
	if err != nil {
		t.Fatal(err)
	}
	f.index++
	result, err := f.g.machine.Apply(context.Background(), raftengine.Command{Index: f.index, Term: 1, Data: data})
	if err != nil {
		t.Fatal(err)
	}
	var applied metaengine.ApplyResult
	if err = json.Unmarshal(result.Data, &applied); err != nil {
		t.Fatal(err)
	}
	if !applied.Accepted {
		t.Fatalf("FSM refused %T: %s", proposal, applied.Reason)
	}
	return applied
}
func (f *mutationFixture) commit(t *testing.T, doc topology.Document, activate bool) {
	t.Helper()
	current := f.g.machine.Latest().Version
	result := f.command(t, metaengine.ProposeDocument{Doc: doc, ExpectedVersion: current})
	if !activate {
		return
	}
	at := time.Now().UTC()
	version := result.Doc.Version
	plan := topology.Plan{Version: version}
	active := f.g.machine.Active()
	for _, group := range active.Groups {
		if group.ID == topology.MetaGroupID || result.Doc.Group(group.ID) != nil {
			continue
		}
		for _, replica := range group.Replicas {
			plan.Steps = append(plan.Steps, topology.Step{Seq: len(plan.Steps) + 1, Kind: topology.StepStopReplica, GroupID: group.ID, NodeName: replica.NodeName, ReplicaID: replica.ReplicaID})
		}
		plan.Steps = append(plan.Steps, topology.Step{Seq: len(plan.Steps) + 1, Kind: topology.StepDeleteGroupState, GroupID: group.ID})
	}
	begin := f.command(t, metaengine.BeginOperatorProposal{Version: version, Plan: plan, Actor: "test", Origin: result.Doc.Origin, At: at})
	transition := topology.OperatorTransition{OperatorID: begin.OperatorID, Version: version, Attempt: 1, At: at.Add(time.Millisecond)}
	for _, step := range plan.Steps {
		transition.StepSeq, transition.GroupID, transition.NodeName, transition.State = step.Seq, step.GroupID, step.NodeName, topology.OperatorRunning
		f.command(t, metaengine.TransitionOperatorProposal{Transition: transition})
		f.consumeStep(t, topology.AuthorizedStep{OperatorID: begin.OperatorID, Version: version, Attempt: 1, StepSeq: step.Seq, Kind: step.Kind, GroupID: step.GroupID, NodeName: step.NodeName, ReplicaID: step.ReplicaID})
	}
	transition.State = topology.OperatorSucceeded
	f.command(t, metaengine.TransitionOperatorProposal{Transition: transition})
	f.command(t, metaengine.ActivateTargetProposal{OperatorID: begin.OperatorID, Version: version, Attempt: 1})
}
func preview() MutationOptions { return MutationOptions{Preview: true} }
func mutationDoc(t *testing.T, r MutationResult, err error) topology.Document {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	return r.Document
}
func TestAddNodeHappyPath(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	r, err := f.g.AddNode(context.Background(), topology.Node{Name: "n4", Type: "worker", Region: "west", Labels: map[string]string{"rack": "a"}}, preview())
	doc := mutationDoc(t, r, err)
	if doc.MemberID("n4") != 4 || doc.Node("n4").Region != "west" || doc.Node("n4").State != topology.NodeActive {
		t.Fatal(doc)
	}
	if len(doc.Group("work").Replicas) != 3 {
		t.Fatal("placement cap ignored")
	}
	if f.g.machine.Latest().Version != 1 {
		t.Fatal("preview proposed")
	}
	doc.Node("n1").State = topology.NodeExcluded
	original := f.g.machine.Latest()
	if original.Node("n1").State != topology.NodeActive {
		t.Fatal("preview aliases state")
	}
}
func TestAddNodeReplacesExcludedNodeWithFreshIdentity(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	ctx := context.Background()
	r, err := f.g.DrainNode(ctx, "n3", preview())
	doc := mutationDoc(t, r, err)
	f.commit(t, doc, true)
	r, err = f.g.ExcludeNode(ctx, "n3", preview())
	doc = mutationDoc(t, r, err)
	f.commit(t, doc, true)
	r, err = f.g.AddNode(ctx, topology.Node{Name: "n3", Type: "coordinator", Region: "new"}, preview())
	doc = mutationDoc(t, r, err)
	if doc.MemberID("n3") != 4 || doc.Group("work").NextReplicaID != 5 || doc.Node("n3").Type != "coordinator" {
		t.Fatal("retired identity reused", doc)
	}
}
func TestDrainNodeHappyPath(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	r, err := f.g.DrainNode(context.Background(), "n3", preview())
	doc := mutationDoc(t, r, err)
	if doc.MemberID("n3") != 3 || len(doc.Group("work").Replicas) != 2 || doc.Node("n3").State != topology.NodeDraining {
		t.Fatal("drain removed meta vote", doc)
	}
	if doc.Group("work").UpdatedAt.IsZero() || !doc.MetaGroup().UpdatedAt.IsZero() {
		t.Fatal("group stamps wrong", doc.Groups)
	}
}
func TestActivateNodeReversesDrain(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	ctx := context.Background()
	r, err := f.g.DrainNode(ctx, "n3", preview())
	doc := mutationDoc(t, r, err)
	f.commit(t, doc, true)
	r, err = f.g.ActivateNode(ctx, "n3", preview())
	doc = mutationDoc(t, r, err)
	if doc.MemberID("n3") != 3 || len(doc.Group("work").Replicas) != 3 || doc.Group("work").Replicas[2].ReplicaID != 4 {
		t.Fatal("activation failed to allocate fresh load identity", doc)
	}
}
func TestDrainNodeBelowMinRefused(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	doc := f.g.machine.Latest()
	doc.Placement.MinReplicas = 3
	f.commit(t, doc, true)
	if _, err := f.g.DrainNode(context.Background(), "n3", preview()); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("unsafe bounds admitted", err)
	}
}
func TestDrainNodeRefusalIsActionable(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	r, err := f.g.DrainNode(context.Background(), "n3", preview())
	doc := mutationDoc(t, r, err)
	f.commit(t, doc, false)
	_, err = f.g.DrainNode(context.Background(), "n3", preview())
	if err == nil || !strings.Contains(err.Error(), "v2") || !strings.Contains(err.Error(), "apply") {
		t.Fatal("staged drain not identified", err)
	}
	f.commit(t, doc, true)
	_, err = f.g.DrainNode(context.Background(), "n3", preview())
	if err == nil || !strings.Contains(err.Error(), "activate") {
		t.Fatal("converged drain not identified", err)
	}
}
func TestRemoveNodeRefusals(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	if _, err := f.g.ExcludeNode(context.Background(), "n3", preview()); err == nil {
		t.Fatal("undrained retirement admitted")
	}
	r, err := f.g.DrainNode(context.Background(), "n3", preview())
	doc := mutationDoc(t, r, err)
	f.commit(t, doc, false)
	if _, err = f.g.ExcludeNode(context.Background(), "n3", preview()); err == nil {
		t.Fatal("unconverged drain retired")
	}
}
func TestRemoveNodeCancelsStagedOnlyAdmission(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	initial := f.g.machine.Latest()
	initial.Placement.MaxReplicas = 4
	f.commit(t, initial, true)
	ctx := context.Background()
	r, err := f.g.AddNode(ctx, topology.Node{Name: "n4", Type: "worker"}, preview())
	doc := mutationDoc(t, r, err)
	f.commit(t, doc, false)
	r, err = f.g.ExcludeNode(ctx, "n4", preview())
	doc = mutationDoc(t, r, err)
	if doc.MemberID("n4") != 0 || doc.Node("n4").State != topology.NodeExcluded {
		t.Fatal(doc)
	}
}
func TestChangeReplicaDemote(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	r, err := f.g.ChangeReplica(context.Background(), "work", topology.ReplicaRoleChange{NodeName: "n3", Role: topology.ReplicaLearner}, preview())
	doc := mutationDoc(t, r, err)
	if doc.Group("work").Replicas[2].Role != topology.ReplicaLearner || doc.Group("work").Replicas[2].ReplicaID != 3 || doc.Group("work").UpdatedAt.IsZero() {
		t.Fatal(doc)
	}
}

type mutationHealth map[string]topology.Health

func (h mutationHealth) Snapshot(context.Context) (map[string]topology.Health, error) { return h, nil }
func TestChangeReplicaDemoteQuorumSafety(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	f.g.opts.Health = mutationHealth{"n1": topology.HealthAlive, "n2": topology.HealthDead, "n3": topology.HealthAlive}
	if _, err := f.g.ChangeReplica(context.Background(), "work", topology.ReplicaRoleChange{NodeName: "n3", Role: topology.ReplicaLearner}, preview()); !errors.Is(err, fault.ErrUnsafeMembership) {
		t.Fatal("post-demotion majority lost", err)
	}
}
func TestPlacementInvariantClampsOnVotersNotReplicas(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	doc := f.g.machine.Latest()
	doc.Group("work").Replicas[2].Role = topology.ReplicaLearner
	f.commit(t, doc, true)
	r, err := f.g.AddNode(context.Background(), topology.Node{Name: "n4", Type: "worker"}, preview())
	doc = mutationDoc(t, r, err)
	if len(doc.VotersOf("work")) != 3 || len(doc.Group("work").Replicas) != 4 {
		t.Fatal("intentional learner counted as placement voter", doc)
	}
}
func TestForceRetireNodeRequiresReasonAndActor(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	for _, o := range []RetireOptions{{}, {Reason: "lost"}, {Mutation: MutationOptions{Actor: "operator"}}} {
		if _, err := f.g.ForceRetireNode(context.Background(), "n3", o); err == nil {
			t.Fatal("unaudited retirement admitted")
		}
	}
}
func TestForceRetireNodeAuditsAndAllocatesFreshReplacement(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	ctx := context.Background()
	r, err := f.g.AddNode(ctx, topology.Node{Name: "n4", Type: "worker"}, preview())
	doc := mutationDoc(t, r, err)
	f.commit(t, doc, true)
	r, err = f.g.ForceRetireNode(ctx, "n3", RetireOptions{Reason: "disk lost", Mutation: MutationOptions{Actor: "operator", Preview: true}})
	doc = mutationDoc(t, r, err)
	if doc.Audit == nil || !doc.Audit.Forced || doc.Audit.StagedBy != "operator" || len(doc.Group("work").Replicas) != 3 || doc.Group("work").Replicas[2].ReplicaID != 4 || doc.MemberID("n3") != 0 {
		t.Fatal(doc)
	}
}
func TestCreateLoadGroupHonorsCallerTypesAndExplicitSubset(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	var doc topology.Document
	r, err := f.g.AddNode(context.Background(), topology.Node{Name: "n4", Type: "coordinator"}, preview())
	doc = mutationDoc(t, r, err)
	f.commit(t, doc, true)
	rule := topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3, AllowedNodeTypes: []topology.NodeType{"coordinator"}}
	r, err = f.g.CreateLoadGroup(context.Background(), GroupSpec{ID: "coord", Placement: &rule}, preview())
	doc = mutationDoc(t, r, err)
	if len(doc.Group("coord").Replicas) != 1 || doc.Group("coord").Replicas[0].NodeName != "n4" {
		t.Fatal("type restriction lost", doc)
	}
	r, err = f.g.CreateLoadGroup(context.Background(), GroupSpec{ID: "subset", Nodes: []string{"n2"}}, preview())
	doc = mutationDoc(t, r, err)
	if len(doc.Group("subset").Replicas) != 1 || doc.Group("subset").Replicas[0].NodeName != "n2" {
		t.Fatal("subset expanded", doc)
	}
}
func TestCreateLoadGroupRefusals(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	for _, id := range []string{"", topology.MetaGroupID, "../escape", "old.quarantine-1", "work"} {
		if _, err := f.g.CreateLoadGroup(context.Background(), GroupSpec{ID: id}, preview()); err == nil {
			t.Fatal("invalid ID minted", id)
		}
	}
	if _, err := f.g.CreateLoadGroup(context.Background(), GroupSpec{ID: "new", Nodes: []string{"n1", "n1"}}, preview()); err == nil {
		t.Fatal("duplicate host admitted")
	}
}
func TestSetPlacementRefusals(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	if _, err := f.g.SetPlacement(context.Background(), topology.PlacementRule{MinReplicas: 1, MaxReplicas: 2}, preview()); err == nil {
		t.Fatal("placement silently shrank group")
	}
}
func TestRollbackLineage(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	r, err := f.g.SetPlacement(context.Background(), topology.PlacementRule{MinReplicas: 2, MaxReplicas: 3}, preview())
	doc := mutationDoc(t, r, err)
	f.commit(t, doc, true)
	r, err = f.g.RollbackTo(context.Background(), 1, preview())
	doc = mutationDoc(t, r, err)
	if doc.ParentVersion != 1 || doc.Version != 3 || doc.Placement.MinReplicas != 1 {
		t.Fatal(doc)
	}
}
func TestRollbackStagedRemovalKeepsApprovedIdentityUntilApproval(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	r, err := f.g.DrainNode(context.Background(), "n3", preview())
	doc := mutationDoc(t, r, err)
	f.commit(t, doc, false)
	r, err = f.g.RollbackTo(context.Background(), 1, preview())
	doc = mutationDoc(t, r, err)
	if doc.Group("work").Replicas[2].ReplicaID != 3 {
		t.Fatal("unapproved removal burned an established identity")
	}
	f.commit(t, doc, false)
	r, err = f.g.DeleteLoadGroup(context.Background(), "work", preview())
	doc = mutationDoc(t, r, err)
	f.commit(t, doc, false)
	if _, err = f.g.RollbackTo(context.Background(), 1, preview()); err != nil {
		t.Fatal("unapproved deletion retired group", err)
	}
}
func TestRollbackRefusesResurrectedIdentity(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	r, err := f.g.DrainNode(context.Background(), "n3", preview())
	doc := mutationDoc(t, r, err)
	f.commit(t, doc, true)
	if _, err = f.g.RollbackTo(context.Background(), 1, preview()); err == nil {
		t.Fatal("approved retired identity resurrected")
	}
}
func TestMutationExpectedVersionAndPreviewAreExact(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	before := f.g.machine.Latest()
	o := preview()
	o.ExpectedVersion = 99
	if _, err := f.g.ActivateNode(context.Background(), "n1", o); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal(err)
	}
	o = preview()
	o.Apply = true
	if _, err := f.g.ActivateNode(context.Background(), "n1", o); err == nil {
		t.Fatal("preview attempted approval")
	}
	if !reflect.DeepEqual(before, f.g.machine.Latest()) {
		t.Fatal("refusal mutated FSM")
	}
}

func TestReseatReplicaAllocatesFreshIDAndRefusesUnstableViews(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	r, err := f.g.ReseatReplica(context.Background(), "work", topology.ReplicaKey{NodeName: "n2", ReplicaID: 2}, preview())
	doc := mutationDoc(t, r, err)
	if doc.Group("work").Replicas[1].ReplicaID != 4 || doc.Group("work").NextReplicaID != 5 || doc.Group("work").Replicas[1].Role != topology.ReplicaVoter {
		t.Fatal(doc)
	}
	r, err = f.g.DrainNode(context.Background(), "n3", preview())
	doc = mutationDoc(t, r, err)
	f.commit(t, doc, false)
	if _, err = f.g.ReseatReplica(context.Background(), "work", topology.ReplicaKey{NodeName: "n2", ReplicaID: 2}, preview()); !errors.Is(err, fault.ErrOperatorAuthorization) {
		t.Fatal("repair extended staged operator intent", err)
	}
}
func TestReseatReplicaRetainsAuditAndRejectsMetaLearnersAndUnknown(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	doc := f.g.machine.Latest()
	doc.Audit = &topology.ChangeAudit{Operation: topology.AuditOperationForceRetireNode, TargetNode: "old", Forced: true, Reason: "lost", StagedBy: "operator", StagedAt: time.Now().UTC()}
	f.commit(t, doc, true)
	r, err := f.g.ReseatReplica(context.Background(), "work", topology.ReplicaKey{NodeName: "n2", ReplicaID: 2}, preview())
	repaired := mutationDoc(t, r, err)
	if !reflect.DeepEqual(doc.Audit, repaired.Audit) {
		t.Fatal("repair changed prior audit")
	}
	for _, pair := range [][2]string{{topology.MetaGroupID, "n2"}, {"missing", "n2"}, {"work", "unknown"}} {
		if _, err = f.g.ReseatReplica(context.Background(), pair[0], topology.ReplicaKey{NodeName: pair[1], ReplicaID: 2}, preview()); err == nil {
			t.Fatal("invalid reseat", pair)
		}
	}
	doc = f.g.machine.Latest()
	doc.Group("work").Replicas[1].Role = topology.ReplicaLearner
	f.commit(t, doc, true)
	if _, err = f.g.ReseatReplica(context.Background(), "work", topology.ReplicaKey{NodeName: "n2", ReplicaID: 2}, preview()); err == nil {
		t.Fatal("learner reseated")
	}
}

func TestSetPlacementQuorumSafetyAndHealthyGrow(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	doc := f.g.machine.Latest()
	doc.Placement.MaxReplicas = 2
	doc.Group("work").Replicas = doc.Group("work").Replicas[:2]
	f.commit(t, doc, true)
	f.g.opts.Health = mutationHealth{"n1": topology.HealthAlive, "n2": topology.HealthDead, "n3": topology.HealthDead}
	if _, err := f.g.SetPlacement(context.Background(), topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3}, preview()); !errors.Is(err, fault.ErrUnsafeMembership) {
		t.Fatal("unsafe growth admitted", err)
	}
	f.g.opts.Health = mutationHealth{"n1": topology.HealthAlive, "n2": topology.HealthAlive, "n3": topology.HealthAlive}
	r, err := f.g.SetPlacement(context.Background(), topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3}, preview())
	grown := mutationDoc(t, r, err)
	if len(grown.VotersOf("work")) != 3 || grown.Group("work").Replicas[2].ReplicaID != 4 {
		t.Fatal("growth reused removed identity", grown)
	}
}
func TestAddNodeAllowsDeclareBeforeBoot(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	f.g.opts.Health = mutationHealth{"n1": topology.HealthAlive, "n2": topology.HealthAlive, "n3": topology.HealthAlive, "n4": topology.HealthDead}
	if _, err := f.g.AddNode(context.Background(), topology.Node{Name: "n4", Type: "worker"}, preview()); err != nil {
		t.Fatal("admission required running undeclared node", err)
	}
}
func TestCreateLoadGroupQuorumSafety(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	f.g.opts.Health = mutationHealth{"n1": topology.HealthAlive, "n2": topology.HealthDead, "n3": topology.HealthDead}
	if _, err := f.g.CreateLoadGroup(context.Background(), GroupSpec{ID: "new"}, preview()); !errors.Is(err, fault.ErrUnsafeMembership) {
		t.Fatal("group born below majority", err)
	}
	if _, err := f.g.CreateLoadGroup(context.Background(), GroupSpec{ID: "new", Nodes: []string{"n1"}}, preview()); err != nil {
		t.Fatal("healthy subset refused", err)
	}
}
func TestRollbackApprovedGroupDeletionIsPermanentlyRetired(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	r, err := f.g.DeleteLoadGroup(context.Background(), "work", preview())
	doc := mutationDoc(t, r, err)
	f.commit(t, doc, true)
	if _, err = f.g.RollbackTo(context.Background(), 1, preview()); !errors.Is(err, fault.ErrGroupRetired) {
		t.Fatal("rollback resurrected group", err)
	}
}

func TestMutationConcurrentCommitReturnsTypedCASConflict(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	_, err := f.g.computeMutation(context.Background(), preview(), mutationPolicy{Comment: "edit"}, func(_ *topology.Document, _ time.Time) error {
		concurrent := f.g.machine.Latest()
		concurrent.Comment = "concurrent edit"
		f.commit(t, concurrent, false)
		return nil
	})
	var conflict *fault.Error
	if !errors.As(err, &conflict) || conflict.Code != fault.VersionConflict || conflict.ExpectedVersion != 1 || conflict.CurrentVersion != 2 {
		t.Fatal("interleaved edit did not preserve CAS", err)
	}
}

func TestTypedMutationPinsZeroVersionAndChecksReplayBeforeEdit(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	policy := mutationPolicy{Kind: "activate_node", Input: mutationInput{NodeName: "n1"}, Comment: "activate"}
	docs, err := f.g.computeMutation(t.Context(), MutationOptions{Preview: true}, policy, func(_ *topology.Document, _ time.Time) error { return nil })
	if err != nil || docs.Intent == nil || docs.Intent.ExpectedVersion != 1 || docs.Intent.Version != 2 {
		t.Fatal("zero expected was not captured", docs, err)
	}
	concurrent := f.g.machine.Latest()
	concurrent.Comment = "another caller"
	f.commit(t, concurrent, false)
	before := f.g.machine.Latest()
	calls := 0
	_, err = f.g.computeMutation(t.Context(), MutationOptions{ExpectedVersion: 1}, policy, func(_ *topology.Document, _ time.Time) error { calls++; return nil })
	if !errors.Is(err, fault.ErrVersionConflict) || calls != 0 || !reflect.DeepEqual(before, f.g.machine.Latest()) {
		t.Fatal("stale intent invoked allocating edit or changed CAS", calls, err)
	}
	// Raw acceptance is still inspectable in the schema3 transition adapter. Typed
	// provenance is deliberately not enabled until the complete format cutover.
	accepted := f.g.machine.Latest()
	replay, err := f.g.machine.ReplayMutation(metaengine.ProposeDocument{ExpectedVersion: 1, Doc: accepted})
	if err != nil || !replay.Accepted || !reflect.DeepEqual(replay.Doc, accepted) {
		t.Fatal("lost raw Stage response was not recoverable", replay, err)
	}
}

func TestStageLostResponseReturnsHistoricalDocumentWithoutProposal(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	candidate := f.g.machine.Latest()
	candidate.Comment = "original acceptance"
	f.commit(t, candidate, false)
	accepted := f.g.machine.Latest()
	candidate = f.g.machine.Latest()
	candidate.Comment = "later unrelated edit"
	f.commit(t, candidate, false)
	before := f.g.machine.Latest()
	// This fixture has no running service. Success proves the stale path returns
	// retained evidence without trying to submit another Raft proposal.
	result, err := f.g.Stage(t.Context(), accepted, 1)
	if err != nil || !reflect.DeepEqual(result, accepted) || !reflect.DeepEqual(f.g.machine.Latest(), before) {
		t.Fatal(result, err)
	}
	accepted.Comment = "different purpose"
	if _, err := f.g.Stage(t.Context(), accepted, 1); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal(err)
	}
}
