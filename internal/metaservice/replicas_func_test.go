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
	before, err := f.g.LookupLoadReplica(ctx, "work")
	if err != nil || !before.Found || before.Record.NodeName != "n1" || before.Record.ClusterID != "cluster" || before.Record.CreatedAt.IsZero() {
		t.Fatal(before.Record, before.Found, err)
	}
	record.Origin = "resume"
	if err := f.g.RecordLoadReplica(ctx, "work", record, 0); err != nil {
		t.Fatal(err)
	}
	if got, _ := f.g.LookupLoadReplica(ctx, "work"); got.Record != before.Record {
		t.Fatal("repeat rewrote audit", got.Record)
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
	if got, err := f.g.LookupLoadReplica(ctx, "work"); err != nil || got.Record.ReplicaID != 4 {
		t.Fatal(got.Record, err)
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
	if _, err := f.g.LookupLoadReplica(ctx, "work"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := f.g.RecordLoadReplica(ctx, "work", ReplicaRecord{ReplicaID: 1}, 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	f.g.quiescing = true
	if _, err := f.g.LookupLoadReplica(context.Background(), "work"); err != nil {
		t.Fatal("quiesce closed read ledger", err)
	}
	if err := f.g.RecordLoadReplica(context.Background(), "work", ReplicaRecord{ReplicaID: 1}, 0); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
	if _, err := (&Group{}).LookupLoadReplica(context.Background(), "work"); !errors.Is(err, fault.ErrReplicaNotReady) {
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
	approval := f.fixtureApproval(t, f.g.machine.Latest())
	begin := f.command(t, approval)
	status, err := f.g.machine.Operation(begin.OperationID)
	if err != nil || status.State != topology.OperationSucceeded {
		t.Fatal(status, err)
	}
	block := approval.Plan.Blocks[0]
	if len(block.Steps) != 0 || len(block.Cleanup) != 4 {
		t.Fatal(approval.Plan)
	}
	work, err := f.g.machine.ReadyCleanup(storage.Identity{}, 10)
	if err != nil || len(work) != 3 {
		t.Fatal(work, err)
	}
	for _, item := range work {
		if item.Action.Kind != topology.StepStopReplica {
			t.Fatal("retirement issued before stop", item)
		}
		grant := f.command(t, metaengine.BeginCleanupEffectProposal{Identity: item.Identity,
			Action: item.Action, ProposalID: newProposalID()}).Grant
		for _, receipt := range protocolEffectReceipts(*grant) {
			f.command(t, metaengine.RecordEffectProposal{Receipt: receipt, At: time.Now().UTC(),
				ProposalID: newProposalID()})
		}
	}
	step := approval.Plan.Steps[block.Cleanup[len(block.Cleanup)-1]-1]
	if step.Kind != topology.StepDeleteGroupState {
		t.Fatal(step)
	}
	a := topology.AuthorizedStep{OperationID: begin.OperationID, Version: approval.Version, Attempt: 1, StepSeq: step.Seq, Kind: step.Kind, GroupID: step.GroupID}
	return f, a
}

func TestGrantedLoadLedgerCleanupRequiresExactAuthorization(t *testing.T) {
	t.Parallel()
	f, a := cleanupLedgerFixture(t)
	grant := f.command(t, metaengine.BeginCleanupEffectProposal{Identity: fixtureLoadIdentity(), Action: a, ProposalID: newProposalID()}).Grant
	if grant == nil {
		t.Fatal("missing cleanup grant")
	}
	ctx := t.Context()
	wrong := grant.Clone()
	wrong.Action.Attempt++
	wrong.OperationID = topology.MakeOperationID(wrong.Action.Version, wrong.Action.Attempt)
	wrong.Action.OperationID = wrong.OperationID
	if err := f.g.ForgetGrantedLoadReplica(ctx, wrong, 1); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("stale cleanup accepted", err)
	}
	if err := f.g.ForgetGrantedLoadReplica(ctx, *grant, 2); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("wrong ledger ID erased", err)
	}
	if err := f.g.ForgetGrantedLoadReplica(ctx, *grant, 1); err != nil {
		t.Fatal(err)
	}
	if err := f.g.ForgetGrantedLoadReplica(ctx, *grant, 1); err != nil {
		t.Fatal("cleanup repeat not idempotent", err)
	}
	if found, err := f.g.LookupLoadReplica(ctx, "work"); err != nil || found.Found {
		t.Fatal(found.Found, err)
	}
	if _, err := f.g.LoadReplicaScope(ctx, fixtureLoadIdentity()); !errors.Is(err, fault.ErrGroupRetired) {
		t.Fatal(err)
	}
}

func TestGrantedLoadLedgerCleanupAfterCompletionPreservesSuccessors(t *testing.T) {
	for _, mode := range []string{"completed_parent", "successor_replica", "successor_member", "different_grant"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f, action := cleanupLedgerFixture(t)
			grant := f.command(t, metaengine.BeginCleanupEffectProposal{Identity: fixtureLoadIdentity(), Action: action, ProposalID: newProposalID()}).Grant
			if grant == nil {
				t.Fatal("missing cleanup grant")
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
			if mode == "completed_parent" {
				if err != nil {
					t.Fatal("detached cleanup could not finish after its required parent", err)
				}
				if err := f.g.ForgetGrantedLoadReplica(t.Context(), *grant, 1); err != nil {
					t.Fatal("exact retry was not idempotent", err)
				}
				if found, err := f.g.LookupLoadReplica(t.Context(), "work"); err != nil || found.Found {
					t.Fatal(found.Found, err)
				}
				return
			}
			if err == nil {
				t.Fatal("mismatched authority erased the ledger")
			}
			if record, err := f.g.LookupLoadReplica(t.Context(), "work"); err != nil || !record.Found || mode == "successor_replica" && record.Record.ReplicaID != 4 {
				t.Fatal("successor evidence was lost", record.Record, record.Found, err)
			}
		})
	}
}
