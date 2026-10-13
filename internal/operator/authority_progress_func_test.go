//go:build functional

package operator

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type receiptController struct {
	GroupEffects
	alter func([]topology.EffectReceipt) []topology.EffectReceipt
}

func (g receiptController) PrepareReplica(ctx context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	receipts, err := g.GroupEffects.PrepareReplica(ctx, grant)
	return g.alter(receipts), err
}

func TestControllerRequiresRecordedExactReceipts(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		alter     func([]topology.EffectReceipt) []topology.EffectReceipt
		recordErr error
	}{
		{name: "missing", alter: func([]topology.EffectReceipt) []topology.EffectReceipt { return nil }},
		{name: "wrong target", alter: func(rs []topology.EffectReceipt) []topology.EffectReceipt { rs[0].Target.ReplicaID++; return rs }},
		{name: "record unavailable", alter: func(rs []topology.EffectReceipt) []topology.EffectReceipt { return rs }, recordErr: fault.ErrUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			active, target, replica := growDocuments()
			step := Step{Seq: 1, Kind: StepPrepareReplica, GroupID: "shard0", NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
			f := newControllerFixture(active, target, []Step{step, {Seq: 2, Kind: StepAddLearner, GroupID: step.GroupID, NodeName: step.NodeName, ReplicaID: step.ReplicaID}}, 0, OperatorPending)
			f.groups.statuses[step.GroupID] = statusFor(step.GroupID, []uint64{1}, nil, 1)
			f.engine.groups = receiptController{GroupEffects: f.groups, alter: tc.alter}
			f.coord.receiptErr = tc.recordErr
			f.engine.Tick(t.Context())
			ex, _ := f.coord.CurrentOperator(t.Context())
			if ex.CurrentStep != 1 || ex.State == OperatorSucceeded {
				t.Fatal("advanced without exact recorded receipt", ex)
			}
			for _, rs := range f.coord.receipts {
				if len(rs) > 0 {
					t.Fatal("invalid or unrecorded receipt became proof", rs)
				}
			}
			if tc.name == "wrong target" {
				if ex.State != OperatorFailed {
					t.Fatal("malformed proof was retryable", ex)
				}
				return
			}
			f.coord.receiptErr = nil
			f.engine.groups = f.groups
			f.engine.Tick(t.Context())
			ex, _ = f.coord.CurrentOperator(t.Context())
			if ex.CurrentStep != 2 {
				t.Fatal("valid receipt did not unblock progress", ex)
			}
		})
	}
}

func TestControllerReconcilesFencesBeforeOperatorSelection(t *testing.T) {
	t.Parallel()
	for _, state := range []OperatorState{OperatorPending, OperatorSucceeded} {
		t.Run(string(state), func(t *testing.T) {
			t.Parallel()
			active, target, _ := growDocuments()
			f := newControllerFixture(active, target, nil, 0, state)
			f.coord.reconcileErr = fault.ErrAuthorityPending
			if got := f.engine.tick(t.Context()); got != controllerWaiting {
				t.Fatal("pending fence skipped", got)
			}
			if f.store.activationRuns != 0 || len(f.coord.transitions) != 0 {
				t.Fatal("pending revocation allowed progress")
			}
			f.coord.hasOperator = false
			if got := f.engine.tick(t.Context()); got != controllerWaiting {
				t.Fatal("revocation lost with absent operator", got)
			}
			f.coord.reconcileErr = nil
			if got := f.engine.tick(t.Context()); got != controllerIdle {
				t.Fatal(got)
			}
		})
	}
}

func TestControllerValidatesAfterFencingBeforeEffectsOrEmptyPlanCompletion(t *testing.T) {
	t.Parallel()
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprint(empty), func(t *testing.T) {
			t.Parallel()
			active, target, r := growDocuments()
			steps := []Step{{Seq: 1, GroupID: "shard0", Kind: StepPrepareReplica, NodeName: r.NodeName, ReplicaID: r.ReplicaID}}
			if empty {
				steps = nil
			}
			f := newControllerFixture(active, target, steps, 0, OperatorPending)
			calls := 0
			f.engine.planValidator = func(context.Context, Record) error { calls++; return fault.ErrReplanRequired }
			f.coord.reconcileErr = fault.ErrAuthorityPending
			f.engine.Tick(t.Context())
			if calls != 0 {
				t.Fatal("validated before inherited fences completed")
			}
			f.coord.reconcileErr = nil
			f.engine.Tick(t.Context())
			ex, _ := f.coord.CurrentOperator(t.Context())
			if calls != 1 || ex.State != OperatorFailed || !strings.Contains(ex.Error, "replan_required") || len(f.groups.calls) != 0 || f.store.activationRuns != 0 {
				t.Fatal("unreachable plan progressed", ex, calls)
			}
		})
	}
}
