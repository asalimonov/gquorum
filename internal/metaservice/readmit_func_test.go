//go:build functional

package metaservice

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestReadmitNodeRotatesOnlyExactNodeIdentities(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	f.g.opts.Health = mutationHealth{"n1": topology.HealthAlive, "n2": topology.HealthAlive, "n3": topology.HealthDead}
	before := f.g.machine.Latest()
	result, err := f.g.ReadmitNode(t.Context(), "n3", 3, MutationOptions{ExpectedVersion: before.Version, Actor: "operator", Preview: true})
	if err != nil {
		t.Fatal(err)
	}
	got := result.Document
	if got.Version != before.Version+1 || got.ParentVersion != before.Version || got.Origin != topology.OriginOperator || got.ChangedBy != "operator" || got.Node("n3").Type != before.Node("n3").Type || got.Node("n3").State != topology.NodeActive {
		t.Fatal("wrong readmission scope/audit", got)
	}
	for _, group := range before.Groups {
		fresh := got.Group(group.ID)
		if fresh == nil || len(fresh.Replicas) != len(group.Replicas) || fresh.NextReplicaID != group.NextReplicaID+1 {
			t.Fatal("readmission changed placement or allocation", fresh)
		}
		for i, replica := range group.Replicas {
			replacement := fresh.Replicas[i]
			if replica.NodeName == "n3" {
				if replacement.NodeName != replica.NodeName || replacement.Role != replica.Role || replacement.ReplicaID != group.NextReplicaID || replacement.CreatedAt != replica.CreatedAt {
					t.Fatal("wrong rotated identity", replica, replacement)
				}
			} else if !reflect.DeepEqual(replica, replacement) {
				t.Fatal("readmission rotated another host", replica, replacement)
			}
		}
	}
	if !reflect.DeepEqual(f.g.machine.Latest(), before) || result.OperationID != "" {
		t.Fatal("preview committed or approved readmission")
	}
}

func TestReadmitNodeRefusesUnverifiedAndPendingIdentities(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		code error
		edit func(*testing.T, *mutationFixture) (string, MutationOptions, context.Context)
	}{
		{"alive", fault.ErrUnsafeMembership, func(t *testing.T, f *mutationFixture) (string, MutationOptions, context.Context) {
			f.g.opts.Health = mutationHealth{"n3": topology.HealthAlive}
			return "n3", preview(), t.Context()
		}},
		{"unknown", fault.ErrUnavailable, func(t *testing.T, f *mutationFixture) (string, MutationOptions, context.Context) {
			f.g.opts.Health = mutationHealth{"n3": topology.HealthUnknown}
			return "n3", preview(), t.Context()
		}},
		{"missing_health", fault.ErrUnavailable, func(t *testing.T, f *mutationFixture) (string, MutationOptions, context.Context) {
			f.g.opts.Health = mutationHealth{}
			return "n3", preview(), t.Context()
		}},
		{"unknown_node", fault.ErrInvalid, func(t *testing.T, _ *mutationFixture) (string, MutationOptions, context.Context) {
			return "missing", preview(), t.Context()
		}},
		{"stale_version", fault.ErrVersionConflict, func(t *testing.T, _ *mutationFixture) (string, MutationOptions, context.Context) {
			return "n3", MutationOptions{ExpectedVersion: 2, Preview: true}, t.Context()
		}},
		{"owned_affected_group", fault.ErrUnavailable, func(t *testing.T, f *mutationFixture) (string, MutationOptions, context.Context) {
			next := f.g.machine.Latest()
			for i := range next.Group("work").Replicas {
				if next.Group("work").Replicas[i].NodeName == "n3" {
					next.Group("work").Replicas[i].Role = topology.ReplicaLearner
				}
			}
			f.commit(t, next, false)
			f.command(t, f.fixtureApproval(t, f.g.machine.Latest()))
			f.g.opts.Health = mutationHealth{"n3": topology.HealthDead}
			return "n3", preview(), t.Context()
		}},
		{"cancelled", context.Canceled, func(t *testing.T, _ *mutationFixture) (string, MutationOptions, context.Context) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			return "n3", preview(), ctx
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newMutationFixture(t)
			name, opts, ctx := tc.edit(t, f)
			before := f.g.machine.Latest()
			if result, err := f.g.ReadmitNode(ctx, name, 3, opts); !errors.Is(err, tc.code) || result.Document.Version != 0 {
				t.Fatal("readmission accepted an unverified identity", result, err)
			}
			if !reflect.DeepEqual(f.g.machine.Latest(), before) {
				t.Fatal("refusal changed allocation or topology")
			}
		})
	}
}

func TestReplacementRequiresExactPredecessorBeforeAllocation(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	f.g.opts.Health = mutationHealth{"n3": topology.HealthDead}
	before := f.g.machine.Latest()
	for _, id := range []uint64{0, 2, 4} {
		want := fault.ErrVersionConflict
		if id == 0 {
			want = fault.ErrInvalid
		}
		if _, err := f.g.ReadmitNode(t.Context(), "n3", id, preview()); !errors.Is(err, want) {
			t.Fatal("readmit accepted arbitrary predecessor", id, err)
		}
		if _, err := f.g.ReseatReplica(t.Context(), "work", topology.ReplicaKey{NodeName: "n3", ReplicaID: id}, preview()); !errors.Is(err, want) {
			t.Fatal("reseat accepted arbitrary predecessor", id, err)
		}
	}
	if !reflect.DeepEqual(before, f.g.machine.Latest()) {
		t.Fatal("refusal changed allocation")
	}
	rotated, err := f.g.ReadmitNode(t.Context(), "n3", 3, preview())
	if err != nil {
		t.Fatal(err)
	}
	f.commit(t, rotated.Document, false)
	if _, err := f.g.ReadmitNode(t.Context(), "n3", 3, preview()); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal("old member matched reused node name", err)
	}
}
