//go:build functional

package operator

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

type scopedExecutionTest struct {
	AuthorityCoordinator
	GroupEffects
	grant          topology.AuthorityGrant
	receipts       []topology.EffectReceipt
	requests       []effectDispatch
	status         GroupStatus
	native         topology.NativeState
	beginErr       error
	recordErr      error
	nativeErr      error
	alterReceipt   func(topology.EffectReceipt) topology.EffectReceipt
	discardOnError bool
	onDispatch     func(effectDispatch) error
}

func (f *scopedExecutionTest) BeginEffect(_ context.Context, action topology.AuthorizedStep) (topology.AuthorityGrant, error) {
	if f.grant.Action == nil || *f.grant.Action != action {
		return topology.AuthorityGrant{}, fault.ErrOperationAuthorization
	}
	return f.grant.Clone(), f.beginErr
}
func (f *scopedExecutionTest) EffectReceipts(context.Context, topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	return append([]topology.EffectReceipt(nil), f.receipts...), nil
}
func (f *scopedExecutionTest) RecordEffect(_ context.Context, receipt topology.EffectReceipt) error {
	if f.recordErr != nil {
		return f.recordErr
	}
	f.receipts = append(f.receipts, receipt.Clone())
	return nil
}
func (f *scopedExecutionTest) DispatchEffect(_ context.Context, grant topology.AuthorityGrant, resource topology.AuthorityResource, target topology.AuthorityTarget) (topology.EffectReceipt, error) {
	r := effectDispatch{grant: grant.Clone(), resource: resource, target: target}
	f.requests = append(f.requests, r)
	var err error
	if f.onDispatch != nil {
		err = f.onDispatch(r)
	}
	if err != nil && f.discardOnError {
		return topology.EffectReceipt{}, err
	}
	receipt := dispatchTestReceipt(grant, resource, target)
	if f.alterReceipt != nil {
		receipt = f.alterReceipt(receipt)
	}
	return receipt, err
}
func (f *scopedExecutionTest) Status(context.Context, string) (GroupStatus, error) {
	return f.status, nil
}
func (f *scopedExecutionTest) ReadNative(context.Context, string, ...storage.Identity) (topology.NativeState, error) {
	return f.native, f.nativeErr
}

func scopedExecutionFixture(t *testing.T) (*Controller, *scopedExecutionTest, *scopedCoordinatorTest, WorkItem) {
	t.Helper()
	grant := dispatchTestGrant(t, "work")
	step := topology.Step{Seq: 1, Kind: topology.StepCreateFoundingGroup, GroupID: "work"}
	work := dispatchFixtureWork("work")[0]
	work.Operation.Plan.Steps = []topology.Step{step}
	work.Operation.Plan.Blocks[0].Steps = []int{1}
	work.Operation.Status.Blocks[0].TotalSteps = 1
	work.Approved = &topology.Group{ID: "work", Replicas: []topology.Replica{
		{NodeName: "a", ReplicaID: 1, Role: topology.ReplicaVoter}, {NodeName: "b", ReplicaID: 2, Role: topology.ReplicaVoter}}}
	cs := raftpb.ConfState{Voters: []uint64{1, 2}}
	f := &scopedExecutionTest{grant: grant, status: GroupStatus{GroupID: "work", Found: true, LeaderID: 1, ConfState: cs}, native: topology.NativeState{GroupID: "work", ConfState: cs}}
	c := &scopedCoordinatorTest{leader: true}
	cfg := scopedControllerConfig(c)
	cfg.Authority, cfg.Groups = f, f
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.run = &controllerRun{ctx: t.Context(), dispatch: &controllerDispatch{}}
	t.Cleanup(func() { e.run.dispatch.close(); e.run.dispatch.workers.Wait() })
	return e, f, c, work
}

func TestScopedExecutionFoundingRequiresRecordedHostsBeforeNative(t *testing.T) {
	t.Parallel()
	e, f, c, work := scopedExecutionFixture(t)
	for i := range 3 {
		outcome := e.runScopedWork(t.Context(), work)
		if i < 2 && outcome != controllerWaiting || i == 2 && outcome != controllerProgressed {
			t.Fatal(i, outcome, c.transitions)
		}
		if len(f.requests) != i+1 {
			t.Fatal(f.requests)
		}
		if i < 2 && f.requests[i].resource.Kind != topology.AuthorityHost {
			t.Fatal("native before hosts", f.requests)
		}
	}
	if f.requests[0].target.NodeName != "a" || f.requests[1].target.NodeName != "b" || f.requests[2].resource.Kind != topology.AuthorityGroup {
		t.Fatal(f.requests)
	}
	for _, request := range f.requests {
		if !request.grant.Equal(f.grant) {
			t.Fatal("trimmed grant", request)
		}
	}
	last := c.transitions[len(c.transitions)-1]
	if last.State != topology.OperationSucceeded || last.StepSeq != 1 {
		t.Fatal(last)
	}
}

func TestScopedExecutionLostReplyKeepsProofAndDoesNotRepeatEffect(t *testing.T) {
	t.Parallel()
	e, f, c, work := scopedExecutionFixture(t)
	// The final native request completes durably, but its RPC response reports loss.
	// Select receipts by resource instead of depending on their fixture order.
	for _, r := range controllerReceipts(f.grant) {
		if r.Resource.Kind == topology.AuthorityHost {
			f.receipts = append(f.receipts, r)
		}
	}
	f.onDispatch = func(effectDispatch) error { return fault.ErrUnavailable }
	if outcome := e.runScopedWork(t.Context(), work); outcome != controllerWaiting {
		t.Fatal(outcome)
	}
	if len(f.requests) != 1 {
		t.Fatal(f.requests)
	}
	f.onDispatch = nil
	if outcome := e.runScopedWork(t.Context(), work); outcome != controllerProgressed {
		t.Fatal(outcome, c.transitions)
	}
	if len(f.requests) != 1 || c.transitions[len(c.transitions)-1].State != topology.OperationSucceeded {
		t.Fatal("repeated consumed effect", f.requests, c.transitions)
	}
}

func TestScopedExecutionConsumesOriginalProofAndUsesGlobalBlockSequence(t *testing.T) {
	t.Parallel()
	e, f, c, work := scopedExecutionFixture(t)
	work.Operation.Plan.Steps = []topology.Step{
		{Seq: 1, Kind: topology.StepPrepareReplica, GroupID: "sibling"},
		{Seq: 2, Kind: topology.StepPrepareReplica, GroupID: "work", NodeName: "b", ReplicaID: 2},
		{Seq: 3, Kind: topology.StepCleanupReplicaState, GroupID: "work", NodeName: "old", ReplicaID: 3},
		{Seq: 4, Kind: topology.StepStopReplica, GroupID: "work", NodeName: "old", ReplicaID: 3},
	}
	work.Operation.Plan.Blocks[0].Steps, work.Operation.Plan.Blocks[0].Cleanup = []int{2, 4}, []int{3}
	work.Operation.Status.ID, work.Operation.Status.Attempt = "exec-v2-a2", 2
	work.Operation.Previous, work.Consumed = "exec-v2-a1", true
	before := f.grant.Clone()
	if outcome := e.runScopedWork(t.Context(), work); outcome != controllerProgressed {
		t.Fatal(outcome)
	}
	if len(f.requests) != 0 || !reflect.DeepEqual(before, f.grant) {
		t.Fatal("reissued/relabelled consumed predecessor")
	}
	last := c.transitions[len(c.transitions)-1]
	if last.StepSeq != 4 || last.OperationID != "exec-v2-a2" || last.State != topology.OperationRunning {
		t.Fatal(last)
	}
}

func TestScopedExecutionRefusesFencesStaleScopeAndWrongNativeResult(t *testing.T) {
	for _, scenario := range []string{"pending fence", "scope changed", "native mismatch", "wrong leader"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			e, f, c, work := scopedExecutionFixture(t)
			switch scenario {
			case "pending fence":
				f.beginErr = fault.ErrAuthorityPending
			case "scope changed":
				f.onDispatch = func(effectDispatch) error {
					e.scopedPlan = func(context.Context, topology.Operation, string) error { return fault.ErrOperationAuthorization }
					return nil
				}
			case "native mismatch":
				f.receipts = controllerReceipts(f.grant)
				f.native.ConfState.Voters = []uint64{1}
			case "wrong leader":
				for _, r := range controllerReceipts(f.grant) {
					if r.Resource.Kind == topology.AuthorityHost {
						f.receipts = append(f.receipts, r)
					}
				}
				f.status.LeaderID = 99
			}
			e.runScopedWork(t.Context(), work)
			for _, transition := range c.transitions {
				if transition.State == topology.OperationSucceeded {
					t.Fatal(scenario, transition)
				}
			}
			if scenario == "pending fence" && len(f.requests) != 0 {
				t.Fatal("native state bypassed fence")
			}
			if scenario == "scope changed" && len(f.receipts) != 1 {
				t.Fatal("lost exact late proof", f.receipts)
			}
			if scenario == "wrong leader" && len(f.requests) != 0 {
				t.Fatal("routed outside grant")
			}
		})
	}
}

func TestScopedExecutionRotatesFoundingHostAfterFailure(t *testing.T) {
	t.Parallel()
	e, f, _, work := scopedExecutionFixture(t)
	e.rules.Limits.MaxConcurrentGroups, e.rules.Limits.MaxConcurrentCleanup = 1, 1
	clock := newFakeControllerClock()
	e.clock = clock
	f.discardOnError = true
	f.onDispatch = func(request effectDispatch) error {
		if request.target.NodeName == "a" {
			return fault.ErrUnavailable
		}
		return nil
	}
	for range 2 {
		// Exercise the complete worker, including both RPC and group backoff.
		e.run.dispatch.workers.Add(1)
		e.runDispatchWork(t.Context(), e.run.dispatch, work)
		clock.mu.Lock()
		clock.now = clock.now.Add(2 * time.Second)
		clock.mu.Unlock()
	}
	if len(f.requests) != 2 || f.requests[0].target.NodeName != "a" || f.requests[1].target.NodeName != "b" {
		t.Fatal("dead first host monopolized capacity one", f.requests)
	}
	if len(f.receipts) != 1 || f.receipts[0].Target.NodeName != "b" {
		t.Fatal(f.receipts)
	}
	work.Operation.Plan.Steps[0].Seq = 2
	if _, err := requiredWorkBlock(work); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("malformed global sequence", err)
	}
}

func TestScopedExecutionWaitAndDeletionUseExactRequiredSuffix(t *testing.T) {
	for _, kind := range []topology.StepKind{topology.StepWaitCaughtUp, topology.StepStopReplica} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			e, f, c, work := scopedExecutionFixture(t)
			work.Operation.Plan.Steps = []topology.Step{{Seq: 1, GroupID: "sibling", Kind: topology.StepStopReplica}, {Seq: 2, GroupID: "work", Kind: kind, NodeName: "b", ReplicaID: 2}}
			work.Operation.Plan.Blocks[0].Steps = []int{2}
			work.Approved = nil
			if kind == topology.StepWaitCaughtUp {
				work.Operation.Plan.Steps = append(work.Operation.Plan.Steps,
					topology.Step{Seq: 3, GroupID: "sibling", Kind: topology.StepStopReplica},
					topology.Step{Seq: 4, GroupID: "work", Kind: topology.StepPromoteVoter, NodeName: "b", ReplicaID: 2})
				work.Operation.Plan.Blocks[0].Steps = []int{2, 4}
				f.status.ConfState = raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}
				f.status.Commit = 100
				f.status.Progress = map[uint64]ReplicaProgress{2: {Match: 100}}
			} else {
				work.Consumed = true
			}
			if e.runScopedWork(t.Context(), work) != controllerProgressed {
				t.Fatal(c.transitions)
			}
			last := c.transitions[len(c.transitions)-1]
			wantSeq, wantState := 2, topology.OperationSucceeded
			if kind == topology.StepWaitCaughtUp {
				wantSeq, wantState = 4, topology.OperationRunning
			}
			if last.StepSeq != wantSeq || last.State != wantState || len(f.requests) != 0 {
				t.Fatal(last, f.requests)
			}
		})
	}
}

func TestScopedExecutionRechecksPreparatoryLearnerBeforeDestruction(t *testing.T) {
	t.Parallel()
	e, f, c, work := scopedExecutionFixture(t)
	work.Operation.Plan.Steps = []topology.Step{
		{Seq: 1, GroupID: "sibling", Kind: topology.StepPrepareReplica},
		{Seq: 2, GroupID: "work", Kind: topology.StepWaitCaughtUp, ReplicaID: 3},
		{Seq: 3, GroupID: "sibling", Kind: topology.StepStopReplica},
		{Seq: 4, GroupID: "work", Kind: topology.StepTransferLeader, ReplicaID: 2},
		{Seq: 5, GroupID: "sibling", Kind: topology.StepCleanupReplicaState},
		{Seq: 6, GroupID: "work", Kind: topology.StepRemoveReplica, ReplicaID: 1},
	}
	work.Operation.Plan.Blocks[0].Steps = []int{2, 4, 6}
	work.Operation.Status.Blocks[0].CurrentStep = 6
	f.status.ConfState.Learners = []uint64{3}
	f.status.Commit = 100
	f.status.Progress = map[uint64]ReplicaProgress{3: {Match: 1}}
	e.runScopedWork(t.Context(), work)
	last := c.transitions[len(c.transitions)-1]
	if !last.State.Waiting() || last.StepSeq != 6 || len(f.requests) != 0 {
		t.Fatal("destruction skipped lagging preparation", last, f.requests)
	}
}

func TestScopedExecutionRequiresExactRecordedReceipts(t *testing.T) {
	for _, mode := range []string{"missing", "wrong target", "record unavailable"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			e, f, c, work := scopedExecutionFixture(t)
			clock := newFakeControllerClock()
			e.clock = clock
			switch mode {
			case "missing":
				f.alterReceipt = func(topology.EffectReceipt) topology.EffectReceipt { return topology.EffectReceipt{} }
			case "wrong target":
				f.alterReceipt = func(r topology.EffectReceipt) topology.EffectReceipt { r.Target.ReplicaID++; return r }
			case "record unavailable":
				f.recordErr = fault.ErrUnavailable
			}
			e.runScopedWork(t.Context(), work)
			last := c.transitions[len(c.transitions)-1]
			if len(f.receipts) != 0 || last.StepSeq != 1 || last.State == topology.OperationSucceeded {
				t.Fatal("invalid or unrecorded proof advanced work", last, f.receipts)
			}
			if mode != "record unavailable" && last.State != topology.OperationFailed {
				t.Fatal("malformed proof was retryable", last)
			}
			if mode == "record unavailable" {
				f.recordErr = nil
				clock.mu.Lock()
				clock.now = clock.now.Add(2 * time.Second)
				clock.mu.Unlock()
				// Founding records its two host phases and then the native phase.
				// Retrying the lost record keeps the original exact grant.
				for range 2 {
					if outcome := e.runScopedWork(t.Context(), work); outcome != controllerWaiting {
						t.Fatal("founding skipped a receipt phase after record recovery", outcome)
					}
				}
				if outcome := e.runScopedWork(t.Context(), work); outcome != controllerProgressed || len(f.receipts) != 3 {
					t.Fatal("record recovery did not resume original work", outcome, f.receipts)
				}
			}
		})
	}
}

func TestScopedFinalValidationRequiresNativeProofAndSafety(t *testing.T) {
	for _, mode := range []string{"unavailable", "foreign group", "membership mismatch", "joint", "final refusal"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			e, f, c, work := scopedExecutionFixture(t)
			f.receipts = controllerReceipts(f.grant)
			valid := f.native.Clone()
			switch mode {
			case "unavailable":
				f.nativeErr = fault.ErrUnavailable
			case "foreign group":
				f.native.GroupID = "foreign"
			case "membership mismatch":
				f.native.ConfState.Voters = []uint64{1}
			case "joint":
				f.native.ConfState.VotersOutgoing = []uint64{1}
			case "final refusal":
				e.scopedFinal = func(context.Context, topology.Operation, string) error { return fault.ErrReplanRequired }
			}
			e.runScopedWork(t.Context(), work)
			last := c.transitions[len(c.transitions)-1]
			if last.State == topology.OperationSucceeded || len(f.requests) != 0 {
				t.Fatal("diagnostic membership bypassed final validation", last, f.requests)
			}
			if mode == "final refusal" {
				if last.State != topology.OperationFailed {
					t.Fatal(last)
				}
				return
			}
			f.native, f.nativeErr = valid, nil
			if e.runScopedWork(t.Context(), work) != controllerProgressed || len(f.requests) != 0 {
				t.Fatal("confirmed membership did not resume consumed work", c.transitions, f.requests)
			}
		})
	}
}

func TestScopedMembershipReceiptNeedsObservedResult(t *testing.T) {
	for _, kind := range []topology.StepKind{topology.StepAddLearner, topology.StepPromoteVoter, topology.StepDemoteVoter, topology.StepRemoveReplica, topology.StepTransferLeader} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			e, f, c, work := scopedExecutionFixture(t)
			step := topology.Step{Seq: 1, Kind: kind, GroupID: "work", NodeName: "b", ReplicaID: 2}
			work.Operation.Plan.Steps = []topology.Step{step, {Seq: 2, Kind: topology.StepWaitCaughtUp, GroupID: "work", NodeName: "b", ReplicaID: 2}}
			work.Operation.Plan.Blocks[0].Steps = []int{1, 2}
			action := scopedAction(work, step)
			f.grant.Action = &action
			f.grant.Resources = []topology.AuthorityResource{{Kind: topology.AuthorityGroup, GroupID: "work"}}
			f.status.ConfState = raftpb.ConfState{Voters: []uint64{1, 2}}
			if kind == topology.StepAddLearner {
				f.status.ConfState.Voters = []uint64{1}
			}
			if kind == topology.StepPromoteVoter {
				f.status.ConfState = raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}
			}
			if err := f.grant.Validate(); err != nil {
				t.Fatal(err)
			}
			e.runScopedWork(t.Context(), work)
			last := c.transitions[len(c.transitions)-1]
			if last.StepSeq != 1 || !last.State.Waiting() || len(f.receipts) != 1 {
				t.Fatal("acknowledgement bypassed observed result", last, f.receipts)
			}
			switch kind {
			case topology.StepAddLearner, topology.StepDemoteVoter:
				f.status.ConfState = raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}
			case topology.StepPromoteVoter:
				f.status.ConfState = raftpb.ConfState{Voters: []uint64{1, 2}}
			case topology.StepRemoveReplica:
				f.status.ConfState = raftpb.ConfState{Voters: []uint64{1}}
			case topology.StepTransferLeader:
				f.status.LeaderID = 2
			}
			if e.runScopedWork(t.Context(), work) != controllerProgressed {
				t.Fatal(c.transitions)
			}
			last = c.transitions[len(c.transitions)-1]
			if last.StepSeq != 2 || last.State != topology.OperationRunning {
				t.Fatal(last)
			}
			want := 1
			if kind == topology.StepTransferLeader {
				want = 2
			} // Re-observe the transfer using the same grant.
			if len(f.requests) != want {
				t.Fatal("consumed membership repeated", f.requests)
			}
		})
	}
}

func TestScopedFoundingSeedsVotersBeforeIntentionalLearner(t *testing.T) {
	t.Parallel()
	e, f, c, work := scopedExecutionFixture(t)
	work.Approved.Replicas = append(work.Approved.Replicas, topology.Replica{NodeName: "c", ReplicaID: 3, Role: topology.ReplicaLearner})
	work.Operation.Plan.Steps = append(work.Operation.Plan.Steps,
		topology.Step{Seq: 2, Kind: topology.StepPrepareReplica, GroupID: "work", NodeName: "c", ReplicaID: 3},
		topology.Step{Seq: 3, Kind: topology.StepAddLearner, GroupID: "work", NodeName: "c", ReplicaID: 3})
	work.Operation.Plan.Blocks[0].Steps = []int{1, 2, 3}
	for range 3 {
		e.runScopedWork(t.Context(), work)
	}
	last := c.transitions[len(c.transitions)-1]
	if last.State != topology.OperationRunning || last.StepSeq != 2 {
		t.Fatal("voter seed did not advance to learner preparation", last)
	}
	for _, request := range f.requests {
		if request.target.NodeName == "c" {
			t.Fatal("intentional learner was founded", request)
		}
	}
}
