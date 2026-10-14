//go:build !functional

package load_test

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestManagedHandleHasNoOwnerEscapeHatch(t *testing.T) {
	t.Parallel()
	typ := reflect.TypeOf((*load.Group)(nil))
	var methods []string
	for i := 0; i < typ.NumMethod(); i++ {
		methods = append(methods, typ.Method(i).Name)
	}
	want := []string{"Error", "GroupID", "Identity", "IsLeader", "Metrics", "MetricsHistory", "ProposalStats", "Propose", "Read", "ReadBarrier", "ReplicaID", "RunWhileLeader", "Running", "Snapshot", "Status", "WALStall", "WaitReady", "Watch"}
	if !slices.Equal(methods, want) {
		t.Fatalf("managed handle method set: %v", methods)
	}
	for _, f := range reflect.VisibleFields(typ.Elem()) {
		if f.IsExported() || f.Anonymous {
			t.Fatalf("raw representation exposed by field %s", f.Name)
		}
	}
}

func TestUninitializedHandlesRejectOperations(t *testing.T) {
	t.Parallel()
	for _, g := range []*load.Group{nil, {}} {
		if g.Identity() != (storage.Identity{}) || g.Running() || g.IsLeader() || !errors.Is(g.Error(), fault.ErrInvalid) {
			t.Fatal("zero handle appears usable")
		}
		_, err := g.Propose(t.Context(), load.Proposal{Data: []byte("x")})
		_, readErr := g.Read(t.Context(), nil)
		_, watchErr := g.Watch(t.Context())
		leaderErr := g.RunWhileLeader(t.Context(), func(context.Context, *load.Leadership) error { return nil })
		for _, err := range []error{err, readErr, watchErr, leaderErr, g.ReadBarrier(t.Context()), g.WaitReady(t.Context()), g.Snapshot(t.Context())} {
			if !errors.Is(err, fault.ErrInvalid) {
				t.Fatal(err)
			}
		}
	}
	for _, owner := range []*load.Standalone{nil, {}} {
		for _, err := range []error{owner.Start(t.Context()), owner.Stop(t.Context()), owner.Step(t.Context(), raftpb.Message{}), owner.ChangeMembership(t.Context(), raftpb.ConfChangeV2{}, nil), owner.TransferLeader(t.Context(), 1)} {
			if !errors.Is(err, fault.ErrInvalid) {
				t.Fatal(err)
			}
		}
		owner.ReportUnreachable(1)
		owner.ReportSnapshot(1, raft.SnapshotFailure)
	}
}

func TestUninitializedLeadershipRejectsProposals(t *testing.T) {
	t.Parallel()
	for _, leader := range []*load.Leadership{nil, {}} {
		if leader.Term() != 0 {
			t.Fatal("zero handle has a term")
		}
		if _, err := leader.Propose(t.Context(), load.Proposal{}); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal(err)
		}
	}
}
