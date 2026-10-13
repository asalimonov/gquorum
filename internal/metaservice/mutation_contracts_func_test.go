//go:build functional

package metaservice

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/operator"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestActivateNodeIdempotentAndRefusals(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	before := f.g.state().Latest()
	r, err := f.g.ActivateNode(t.Context(), "n3", preview())
	got := mutationDoc(t, r, err)
	if !reflect.DeepEqual(got.Nodes, before.Nodes) || !reflect.DeepEqual(got.Groups, before.Groups) {
		t.Fatal("already active node changed identities or item stamps")
	}
	if _, err := f.g.ActivateNode(t.Context(), "missing", preview()); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	r, err = f.g.DrainNode(t.Context(), "n3", preview())
	f.commit(t, mutationDoc(t, r, err), true)
	r, err = f.g.ExcludeNode(t.Context(), "n3", preview())
	f.commit(t, mutationDoc(t, r, err), true)
	if _, err := f.g.ActivateNode(t.Context(), "n3", preview()); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("excluded identity reactivated", err)
	}
}

func TestNodeLifecycleStamps(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	doc := f.g.state().Latest()
	doc.Placement.MaxReplicas = 4
	f.commit(t, doc, true)
	r, err := f.g.AddNode(t.Context(), topology.Node{Name: "n4", Type: "worker"}, preview())
	added := mutationDoc(t, r, err)
	node := *added.Node("n4")
	if node.CreatedAt.IsZero() || !node.CreatedAt.Equal(node.UpdatedAt) {
		t.Fatal(node)
	}
	for _, group := range added.Groups {
		for _, replica := range group.Replicas {
			if replica.NodeName == "n4" && (replica.CreatedAt.IsZero() || !replica.CreatedAt.Equal(replica.UpdatedAt)) {
				t.Fatal(replica)
			}
		}
	}
	f.commit(t, added, true)
	r, err = f.g.DrainNode(t.Context(), "n4", preview())
	drained := mutationDoc(t, r, err)
	if !drained.Node("n4").CreatedAt.Equal(node.CreatedAt) || !drained.Node("n4").UpdatedAt.After(node.UpdatedAt) || !reflect.DeepEqual(drained.Node("n1"), added.Node("n1")) {
		t.Fatal("drain restamped an unchanged item or regressed audit")
	}
	if drained.Group("work").UpdatedAt.Before(added.Group("work").UpdatedAt) || !reflect.DeepEqual(drained.MetaGroup(), added.MetaGroup()) {
		t.Fatal("drain group stamps or meta identity changed")
	}
	f.commit(t, drained, true)
	r, err = f.g.ActivateNode(t.Context(), "n4", preview())
	activated := mutationDoc(t, r, err)
	if !activated.Node("n4").CreatedAt.Equal(node.CreatedAt) || !activated.Node("n4").UpdatedAt.After(drained.Node("n4").UpdatedAt) || !reflect.DeepEqual(activated.Node("n1"), added.Node("n1")) {
		t.Fatal("activation changed unrelated item audit")
	}
	last := activated.Group("work").Replicas[len(activated.Group("work").Replicas)-1]
	if last.NodeName != "n4" || last.ReplicaID != 5 || last.CreatedAt.IsZero() {
		t.Fatal("activation did not stamp fresh replica", last)
	}
}

type transferRecorder struct {
	operator.GroupEffects
	calls []topology.AuthorizedStep
	err   error
}

func (r *transferRecorder) TransferLeader(_ context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	r.calls = append(r.calls, *grant.Action)
	return protocolEffectReceipts(grant), r.err
}

func TestTransferLeaderUsesExactActiveScopeAndRefusesIneligibleTargets(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, group, node string
		setup             func(*mutationFixture)
		health            mutationHealth
		err               error
	}{
		{name: "dead", group: "work", node: "n2", health: mutationHealth{"n2": topology.HealthDead}, err: fault.ErrUnsafeMembership},
		{name: "unknown_group", group: "missing", node: "n2", err: fault.ErrInvalid},
		{name: "unknown_node", group: "work", node: "missing", err: fault.ErrInvalid},
		{name: "learner", group: "work", node: "n2", err: fault.ErrInvalid, setup: func(f *mutationFixture) {
			doc := f.g.state().Latest()
			doc.Group("work").Replicas[1].Role = topology.ReplicaLearner
			f.commit(t, doc, true)
		}},
		{name: "draining", group: "work", node: "n2", err: fault.ErrInvalid, setup: func(f *mutationFixture) {
			doc := f.g.state().Latest()
			doc.Node("n2").State = topology.NodeDraining
			f.commit(t, doc, true)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newMutationFixture(t)
			if tc.setup != nil {
				tc.setup(f)
			}
			f.g.started = true
			f.g.ctx = t.Context()
			f.g.workCtx = t.Context()
			f.g.opts.Health = tc.health
			controller := &transferRecorder{}
			f.g.opts.LoadGroups = controller
			before := f.g.state().Active()
			// A newer staged label must not authorize a different transfer version.
			staged := before.Clone()
			staged.Node("n1").Labels = map[string]string{"staged": "intent"}
			f.commit(t, staged, false)
			latest := f.g.state().Latest()
			err := f.g.TransferLeader(t.Context(), tc.group, tc.node)
			if !errors.Is(err, tc.err) {
				t.Fatal(err)
			}
			if len(controller.calls) != 0 {
				t.Fatal("refused transfer reached caller", controller.calls)
			}

			if !reflect.DeepEqual(f.g.state().Active(), before) || !reflect.DeepEqual(f.g.state().Latest(), latest) {
				t.Fatal("transfer changed document")
			}
		})
	}
}

func TestDrainNodeQuorumSafetyAllowsStagingButLeavesApprovedViews(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		health mutationHealth
		drain  string
	}{
		{"unsafe_live_removal", mutationHealth{"n1": topology.HealthAlive, "n2": topology.HealthAlive, "n3": topology.HealthDead}, "n2"},
		{"dead_voter_removal", mutationHealth{"n1": topology.HealthAlive, "n2": topology.HealthAlive, "n3": topology.HealthDead}, "n3"},
		{"unknown", nil, "n2"},
		{"healthy", mutationHealth{"n1": topology.HealthAlive, "n2": topology.HealthAlive, "n3": topology.HealthAlive}, "n2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newMutationFixture(t)
			f.g.opts.Health = tc.health
			before := f.g.state().Active()
			r, err := f.g.DrainNode(t.Context(), tc.drain, preview())
			doc := mutationDoc(t, r, err)
			if len(doc.VotersOf("work")) != 2 || len(doc.VotersOf(topology.MetaGroupID)) != 3 || doc.Node(tc.drain).State != topology.NodeDraining {
				t.Fatal(doc)
			}
			if !reflect.DeepEqual(f.g.state().Active(), before) || !reflect.DeepEqual(f.g.state().Target(), before) || f.g.state().Latest().Version != before.Version {
				t.Fatal("declarative preview approved unsafe intent")
			}
		})
	}
}

func TestDrainNodeStateRefusals(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	if _, err := f.g.DrainNode(t.Context(), "missing", preview()); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	r, err := f.g.DrainNode(t.Context(), "n3", preview())
	f.commit(t, mutationDoc(t, r, err), true)
	if _, err := f.g.DrainNode(t.Context(), "n3", preview()); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("drained node accepted a second drain", err)
	}
}

func TestChangeReplicaPromoteIsDeclarativeAndPlansCatchUpBeforePromotion(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	active := f.g.state().Latest()
	active.Group("work").Replicas[2].Role = topology.ReplicaLearner
	f.commit(t, active, true)
	active = f.g.state().Active()
	r, err := f.g.ChangeReplica(t.Context(), "work", "n3", topology.ReplicaVoter, preview())
	desired := mutationDoc(t, r, err)
	if desired.Group("work").Replicas[2].ReplicaID != 3 || desired.Group("work").Replicas[2].Role != topology.ReplicaVoter || !reflect.DeepEqual(f.g.state().Active(), active) {
		t.Fatal("promotion changed approved membership or identity")
	}
	observed := map[string]topology.GroupStatus{}
	for _, group := range active.Groups {
		s := topology.GroupStatus{GroupID: group.ID, Found: true, LeaderID: 1, Commit: 10, Progress: map[uint64]topology.ReplicaProgress{}, ConfState: raftpb.ConfState{}}
		for _, r := range group.Replicas {
			if r.Role == topology.ReplicaVoter {
				s.ConfState.Voters = append(s.ConfState.Voters, r.ReplicaID)
			} else {
				s.ConfState.Learners = append(s.ConfState.Learners, r.ReplicaID)
			}
			s.Progress[r.ReplicaID] = topology.ReplicaProgress{Match: 10}
		}
		observed[group.ID] = s
	}
	plan, err := topology.BuildPlan(active, desired, observed, f.g.rules)
	if err != nil || len(plan.Steps) != 2 || plan.Steps[0].Kind != topology.StepWaitCaughtUp || plan.Steps[1].Kind != topology.StepPromoteVoter {
		t.Fatal(plan, err)
	}
	for _, s := range plan.Steps {
		if s.GroupID != "work" || s.NodeName != "n3" || s.ReplicaID != 3 {
			t.Fatal(s)
		}
	}
	f.commit(t, desired, false)
	r, err = f.g.ChangeReplica(t.Context(), "work", "n3", topology.ReplicaVoter, preview())
	if got := mutationDoc(t, r, err); !reflect.DeepEqual(got.Group("work"), desired.Group("work")) {
		t.Fatal("repeat promotion changed item state")
	}
}

func TestChangeReplicaErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, group, node string
		role              topology.ReplicaRole
	}{
		{"unknown_group", "missing", "n1", topology.ReplicaVoter},
		{"unknown_node", "work", "missing", topology.ReplicaVoter},
		{"invalid_role", "work", "n3", "reader"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newMutationFixture(t)
			before := f.g.state().Latest()
			if _, err := f.g.ChangeReplica(t.Context(), tc.group, tc.node, tc.role, preview()); !errors.Is(err, fault.ErrInvalid) {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(f.g.state().Latest(), before) {
				t.Fatal("refused edit changed state")
			}
		})
	}
}

func TestRollbackRefusalsAndAllocationFloors(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	if _, err := f.g.RollbackTo(t.Context(), 1, preview()); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("current version rolled back", err)
	}
	if _, err := f.g.RollbackTo(t.Context(), 99, preview()); !errors.Is(err, fault.ErrHistoryUnavailable) {
		t.Fatal("missing version rolled back", err)
	}
	r, err := f.g.AddNode(t.Context(), topology.Node{Name: "n4", Type: "worker"}, preview())
	added := mutationDoc(t, r, err)
	f.commit(t, added, false)
	r, err = f.g.RollbackTo(t.Context(), 1, preview())
	restored := mutationDoc(t, r, err)
	if restored.Node("n4") != nil || restored.Version != 3 || restored.ParentVersion != 1 {
		t.Fatal("rollback lineage", restored)
	}
	for _, group := range added.Groups {
		if restored.Group(group.ID).NextReplicaID < group.NextReplicaID {
			t.Fatal("rollback regressed allocation floor", group.ID)
		}
	}
	f.commit(t, restored, false)
	r, err = f.g.AddNode(t.Context(), topology.Node{Name: "n4", Type: "worker"}, preview())
	if got := mutationDoc(t, r, err); got.MemberID("n4") != 5 {
		t.Fatal("rollback reused burned member", got)
	}
}

func TestOperatorMalformedIDsDoNotAliasRetainedAttempt(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	id := topology.MakeOperatorID(1, 1)
	for _, value := range []topology.OperatorID{
		"exec-v1xyz", "exec-v1 ", "exec-v1.5", "exec-v", "exec-v-1",
		"exec-v+1", "prefix-v1", "exec-vv1", " exec-v1", "exec-v0x1",
		id + "xyz", id + " ", " " + id, "exec-v1-a+1", "exec-v1-a0",
	} {
		if _, err := f.g.Operator(t.Context(), value); !errors.Is(err, fault.ErrHistoryUnavailable) {
			t.Fatal("malformed handle aliased a retained execution", value, err)
		}
	}
	status, err := f.g.Operator(t.Context(), id)
	if err != nil || status.ID != id || status.Version != 1 || status.Attempt != 1 || status.State != topology.OperatorSucceeded {
		t.Fatal("valid retained execution did not resolve", status, err)
	}
}

func TestTransferLeaderCommitsGrantForActiveScope(t *testing.T) {
	t.Parallel()
	for _, known := range []bool{false, true} {
		t.Run(fmt.Sprint(known), func(t *testing.T) {
			t.Parallel()
			owner := &verifiedLoadController{groups: map[string]topology.Group{}, hosts: map[string]map[string]uint64{}}
			recorder := &transferRecorder{GroupEffects: owner}
			groups := singleMetaFixtureWith(t, func(o *Options) {
				o.LoadGroups = recorder
				if known {
					o.Health = mutationHealth{"n1": topology.HealthAlive}
				}
			})
			g := readyPublic(t, groups)
			owner.mu.Lock()
			owner.meta = groups
			owner.mu.Unlock()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			doc, err := g.Stage(ctx, loadDocument(t, g), 1)
			if err != nil {
				t.Fatal(err)
			}
			id, err := g.Apply(ctx, doc.Version)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = g.WaitOperator(ctx, id); err != nil {
				t.Fatal(err)
			}
			for g.state().Active().Version != doc.Version {
				select {
				case <-ctx.Done():
					t.Fatal("target was not activated", ctx.Err())
				case <-time.After(time.Millisecond):
				}
			}
			before := g.state().Active()
			staged := before.Clone()
			staged.Node("n1").Labels = map[string]string{"staged": "intent"}
			latest, err := g.Stage(ctx, staged, before.Version)
			if err != nil {
				t.Fatal(err)
			}
			if err = g.TransferLeader(ctx, "load-one", "n1"); err != nil {
				t.Fatal(err)
			}
			want := topology.AuthorizedStep{Version: before.Version, Kind: topology.StepTransferLeader, GroupID: "load-one", NodeName: "n1", ReplicaID: 1}
			if len(recorder.calls) != 1 || recorder.calls[0] != want {
				t.Fatal("lost Active scope", recorder.calls)
			}
			if !reflect.DeepEqual(g.state().Active(), before) || !reflect.DeepEqual(g.state().Latest(), latest) {
				t.Fatal("transfer changed topology")
			}
		})
	}
}
