//go:build functional

package metaservice

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func replicaFixture(t *testing.T) *mutationFixture {
	t.Helper()
	f := newMutationFixture(t)
	f.g.identity.MemberID = 1
	f.g.identity.InstanceID = "process"
	f.g.started = true
	f.g.ctx = context.Background()
	s, err := metastore.Open(filepath.Join(t.TempDir(), "meta.bolt"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureIdentity("n1", "cluster", 1); err != nil {
		t.Fatal(err)
	}
	f.g.store = s
	t.Cleanup(func() { _ = s.Close() })
	return f
}
func fixtureLoadIdentity() storage.Identity {
	return storage.Identity{NodeName: "n1", ClusterID: "cluster", GroupID: "work", ReplicaID: 1}
}

func TestLoadReplicaScopeUsesApprovedIdentityAndOwnedViews(t *testing.T) {
	t.Parallel()
	f := replicaFixture(t)
	ctx := context.Background()
	scope, err := f.g.LoadReplicaScope(ctx, fixtureLoadIdentity())
	if err != nil || !scope.Started || !scope.Established {
		t.Fatal(scope, err)
	}
	scope.Group.Replicas[0].ReplicaID = 99
	if scope, _ = f.g.LoadReplicaScope(ctx, fixtureLoadIdentity()); scope.Group.Replicas[0].ReplicaID != 1 {
		t.Fatal("scope aliased FSM")
	}
	doc := f.g.machine.Latest()
	doc.Group("work").Replicas[0].ReplicaID = 4
	doc.Group("work").NextReplicaID = 5
	f.commit(t, doc, false)
	newID := fixtureLoadIdentity()
	newID.ReplicaID = 4
	if _, err = f.g.LoadReplicaScope(ctx, newID); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("Latest authorized unstaged replica", err)
	}
	if _, err = f.g.LoadReplicaScope(ctx, fixtureLoadIdentity()); err != nil {
		t.Fatal("staging revoked approved identity", err)
	}
	for _, mutate := range []func(*storage.Identity){func(id *storage.Identity) { id.ClusterID = "other" }, func(id *storage.Identity) { id.NodeName = "n2" }, func(id *storage.Identity) { id.ReplicaID = 0 }, func(id *storage.Identity) { id.GroupID = "other" }} {
		id := fixtureLoadIdentity()
		mutate(&id)
		if _, err := f.g.LoadReplicaScope(ctx, id); !errors.Is(err, fault.ErrIdentityMismatch) {
			t.Fatal(id, err)
		}
	}
	if f.g.Identity().InstanceID != "process" {
		t.Fatal("identity not preserved")
	}
}

func TestPublicLoadLedgerIdempotenceAndApprovedReseat(t *testing.T) {
	t.Parallel()
	f := replicaFixture(t)
	ctx := context.Background()
	record := ReplicaRecord{ReplicaID: 1, Origin: "fresh"}
	if err := f.g.RecordLoadReplica(ctx, "work", record, 0); err != nil {
		t.Fatal(err)
	}
	before, found, err := f.g.LookupLoadReplica(ctx, "work")
	if err != nil || !found || before.NodeName != "n1" || before.ClusterID != "cluster" || before.CreatedAt.IsZero() {
		t.Fatal(before, found, err)
	}
	record.Origin = "resume"
	if err := f.g.RecordLoadReplica(ctx, "work", record, 0); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := f.g.LookupLoadReplica(ctx, "work"); got != before {
		t.Fatal("repeat rewrote audit", got)
	}
	doc := f.g.machine.Latest()
	doc.Group("work").Replicas[0].ReplicaID = 4
	doc.Group("work").NextReplicaID = 5
	f.commit(t, doc, true)
	if err := f.g.RecordLoadReplica(ctx, "work", record, 0); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("retired replica recorded", err)
	}
	record.ReplicaID = 4
	if err := f.g.RecordLoadReplica(ctx, "work", record, 2); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("wrong prior ID replaced", err)
	}
	if err := f.g.RecordLoadReplica(ctx, "work", record, 1); err != nil {
		t.Fatal(err)
	}
	if got, _, err := f.g.LookupLoadReplica(ctx, "work"); err != nil || got.ReplicaID != 4 {
		t.Fatal(got, err)
	}
	record.NodeName = "n2"
	if err := f.g.RecordLoadReplica(ctx, "work", record, 1); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("foreign record accepted", err)
	}
}

func TestPublicLoadLedgerReadinessCancellationAndQuiescedRead(t *testing.T) {
	t.Parallel()
	f := replicaFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := f.g.LookupLoadReplica(ctx, "work"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := f.g.RecordLoadReplica(ctx, "work", ReplicaRecord{ReplicaID: 1}, 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	f.g.quiescing = true
	if _, _, err := f.g.LookupLoadReplica(context.Background(), "work"); err != nil {
		t.Fatal("quiesce closed read ledger", err)
	}
	if err := f.g.RecordLoadReplica(context.Background(), "work", ReplicaRecord{ReplicaID: 1}, 0); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
	if _, _, err := (&Group{}).LookupLoadReplica(context.Background(), "work"); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal(err)
	}
}

func cleanupLedgerFixture(t *testing.T) (*mutationFixture, topology.AuthorizedStep) {
	t.Helper()
	f := replicaFixture(t)
	ctx := context.Background()
	if err := f.g.RecordLoadReplica(ctx, "work", ReplicaRecord{ReplicaID: 1}, 0); err != nil {
		t.Fatal(err)
	}
	doc := f.g.machine.Latest()
	doc.Groups = doc.Groups[:1]
	f.commit(t, doc, false)
	at := time.Now().UTC()
	steps := []topology.Step{{Seq: 1, Kind: topology.StepStopReplica, GroupID: "work", NodeName: "n1", ReplicaID: 1}, {Seq: 2, Kind: topology.StepStopReplica, GroupID: "work", NodeName: "n2", ReplicaID: 2}, {Seq: 3, Kind: topology.StepStopReplica, GroupID: "work", NodeName: "n3", ReplicaID: 3}, {Seq: 4, Kind: topology.StepDeleteGroupState, GroupID: "work"}}
	b := f.command(t, metaengine.BeginOperatorProposal{Version: 2, Plan: topology.Plan{Version: 2, Steps: steps}, At: at})
	for _, step := range steps {
		at = at.Add(time.Millisecond)
		f.command(t, metaengine.TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: b.OperatorID, Version: 2, Attempt: 1, StepSeq: step.Seq, State: topology.OperatorRunning, GroupID: step.GroupID, NodeName: step.NodeName, At: at}})
		if step.Kind != topology.StepDeleteGroupState {
			f.consumeStep(t, topology.AuthorizedStep{OperatorID: b.OperatorID, Version: 2, Attempt: 1, StepSeq: step.Seq, Kind: step.Kind, GroupID: step.GroupID, NodeName: step.NodeName, ReplicaID: step.ReplicaID})
		}
	}
	a := topology.AuthorizedStep{OperatorID: b.OperatorID, Version: 2, Attempt: 1, StepSeq: 4, Kind: topology.StepDeleteGroupState, GroupID: "work"}
	return f, a
}

func TestGrantedLoadLedgerCleanupRequiresExactAuthorization(t *testing.T) {
	t.Parallel()
	f, a := cleanupLedgerFixture(t)
	grant := f.command(t, metaengine.BeginEffectProposal{Action: a}).Grant
	if grant == nil {
		t.Fatal("missing cleanup grant")
	}
	ctx := t.Context()
	wrong := grant.Clone()
	wrong.Action.Attempt++
	wrong.OperatorID = topology.MakeOperatorID(wrong.Action.Version, wrong.Action.Attempt)
	wrong.Action.OperatorID = wrong.OperatorID
	if err := f.g.ForgetGrantedLoadReplica(ctx, wrong, 1); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("stale cleanup accepted", err)
	}
	if err := f.g.ForgetGrantedLoadReplica(ctx, *grant, 2); !errors.Is(err, fault.ErrOperatorAuthorization) {
		t.Fatal("wrong ledger ID erased", err)
	}
	if err := f.g.ForgetGrantedLoadReplica(ctx, *grant, 1); err != nil {
		t.Fatal(err)
	}
	if err := f.g.ForgetGrantedLoadReplica(ctx, *grant, 1); err != nil {
		t.Fatal("cleanup repeat not idempotent", err)
	}
	if _, found, err := f.g.LookupLoadReplica(ctx, "work"); err != nil || found {
		t.Fatal(found, err)
	}
	if _, err := f.g.LoadReplicaScope(ctx, fixtureLoadIdentity()); !errors.Is(err, fault.ErrGroupRetired) {
		t.Fatal(err)
	}
}

func TestGrantedLoadLedgerCleanupAfterSupersessionPreservesSuccessors(t *testing.T) {
	for _, mode := range []string{"closed_grant", "successor_replica", "successor_member", "different_grant"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f, action := cleanupLedgerFixture(t)
			grant := f.command(t, metaengine.BeginEffectProposal{Action: action}).Grant
			if grant == nil {
				t.Fatal("missing cleanup grant")
			}
			f.command(t, metaengine.TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: action.OperatorID, Version: action.Version, Attempt: action.Attempt, StepSeq: action.StepSeq, GroupID: action.GroupID, State: topology.OperatorSuperseded, At: time.Now().UTC().Add(time.Minute)}})
			if err := f.g.Authorize(t.Context(), action); err == nil {
				t.Fatal("cleanup still has current operator authority")
			}
			if mode == "successor_replica" {
				if err := f.g.store.RecordReplicaCAS("work", ReplicaRecord{NodeName: "n1", ClusterID: "cluster", ReplicaID: 4}, 1); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "successor_member" {
				f.g.identity.MemberID = 4
			}
			if mode == "different_grant" {
				grant.Action.Attempt++
			}
			err := f.g.ForgetGrantedLoadReplica(t.Context(), *grant, 1)
			if mode == "closed_grant" {
				if err != nil {
					t.Fatal("owned old cleanup could not finish before its new resource fence", err)
				}
				if err := f.g.ForgetGrantedLoadReplica(t.Context(), *grant, 1); err != nil {
					t.Fatal("exact retry was not idempotent", err)
				}
				if _, found, err := f.g.LookupLoadReplica(t.Context(), "work"); err != nil || found {
					t.Fatal(found, err)
				}
				return
			}
			if err == nil {
				t.Fatal("mismatched authority erased the ledger")
			}
			if record, found, err := f.g.LookupLoadReplica(t.Context(), "work"); err != nil || !found || mode == "successor_replica" && record.ReplicaID != 4 {
				t.Fatal("successor evidence was lost", record, found, err)
			}
		})
	}
}
