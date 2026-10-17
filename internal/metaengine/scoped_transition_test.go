//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func scopedTransitionFor(op topology.Operation, group string, seq int) topology.OperationTransition {
	return topology.OperationTransition{OperationID: op.Status.ID, Version: op.Status.Version,
		Attempt: op.Status.Attempt, GroupID: group, StepSeq: seq, State: topology.OperationRunning,
		At: op.UpdatedAt.Add(time.Second)}
}

func applyScopedTransitionFixture(t *testing.T, s *scopedState, transition topology.OperationTransition) {
	t.Helper()
	body, _ := json.Marshal(TransitionOperationProposal{Transition: transition, ProposalID: "progress"})
	result, err := applyScopedProposal(s, s.AppliedIndex+1, "transition_operation", body)
	if err != nil || !result.changed || !result.result.Accepted {
		t.Fatal("transition refused", result, err)
	}
	prepareScopedFixture(t, s)
}

func TestScopedTransitionsKeepIndependentCursorsAfterSiblingFailure(t *testing.T) {
	t.Parallel()
	s := loadAdmissionFixture(t)
	op := s.Operations[0].Clone()
	failed := scopedTransitionFor(op, "shard0", 3)
	failed.State, failed.ErrorCode, failed.Error = topology.OperationFailed, "unavailable", "host unavailable"
	applyScopedTransitionFixture(t, &s, failed)
	if s.Groups[1].Owner != "" || s.Operations[0].Status.State.Terminal() ||
		!reflect.DeepEqual(op.Status.Blocks[0], s.Operations[0].Status.Blocks[0]) {
		t.Fatal("failure released or changed independent metadata work")
	}
	running := scopedTransitionFor(s.Operations[0], topology.MetaGroupID, 1)
	applyScopedTransitionFixture(t, &s, running)
	waiting := scopedTransitionFor(s.Operations[0], topology.MetaGroupID, 1)
	waiting.State = topology.OperationWaitingForReplica
	waiting.Blocked = &topology.BlockedReason{Code: "admission", NodeName: "n2", ReplicaID: 2}
	applyScopedTransitionFixture(t, &s, waiting)
	waiting.Blocked.Message = "caller-owned"
	if s.Operations[0].Status.Blocks[0].Blocked.Message != "" || s.Groups[0].Owner != op.Status.ID {
		t.Fatal("waiting diagnostic aliased the request or lost ownership")
	}
	// A lost failure response remains idempotent after another block advanced the clock.
	before, _ := json.Marshal(s)
	result, err := s.applyTransitionOperation(s.AppliedIndex+1, TransitionOperationProposal{Transition: failed})
	after, _ := json.Marshal(s)
	if err != nil || result.changed || !bytes.Equal(before, after) {
		t.Fatal("terminal block replay rewrote newer sibling progress", result, err)
	}
}

func TestScopedTransitionRefusesForgedScopeSkippedReceiptAndStaleClock(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*topology.OperationTransition){
		"operation":         func(t *topology.OperationTransition) { t.OperationID = "exec-v2-a9" },
		"attempt":           func(t *topology.OperationTransition) { t.Attempt++ },
		"version":           func(t *topology.OperationTransition) { t.Version++ },
		"group":             func(t *topology.OperationTransition) { t.GroupID = "foreign" },
		"foreign step":      func(t *topology.OperationTransition) { t.StepSeq = 3 },
		"pending":           func(t *topology.OperationTransition) { t.State = topology.OperationPending },
		"stale clock":       func(t *topology.OperationTransition) { t.At = t.At.Add(-2 * time.Second) },
		"missing error":     func(t *topology.OperationTransition) { t.State = topology.OperationFailed },
		"missing blocker":   func(t *topology.OperationTransition) { t.State = topology.OperationWaitingForReplica },
		"premature success": func(t *topology.OperationTransition) { t.State = topology.OperationSucceeded },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := loadAdmissionFixture(t)
			transition := scopedTransitionFor(s.Operations[0], topology.MetaGroupID, 1)
			mutate(&transition)
			before, _ := json.Marshal(s)
			if _, err := s.applyTransitionOperation(11, TransitionOperationProposal{Transition: transition}); !errors.Is(err, fault.ErrOperationAuthorization) {
				t.Fatal("forged transition accepted", err)
			}
			after, _ := json.Marshal(s)
			if !bytes.Equal(before, after) {
				t.Fatal("refused transition published partial progress")
			}
		})
	}
	s := loadAdmissionFixture(t)
	transition := scopedTransitionFor(s.Operations[0], topology.MetaGroupID, 2)
	if _, err := s.applyTransitionOperation(11, TransitionOperationProposal{Transition: transition}); !errors.Is(err, fault.ErrAuthorityPending) {
		t.Fatal("cursor skipped unconsumed preparation", err)
	}
	admissionPrefixReceipt(&s, 0, 1)
	s.Admissions[0].State = AdmissionReady
	prepareScopedFixture(t, &s)
	applyScopedTransitionFixture(t, &s, transition)
	if s.Operations[0].Status.Blocks[0].CurrentStep != 2 || s.Operations[0].Status.Blocks[1].CurrentStep != 3 {
		t.Fatal("block-local progress changed its sibling cursor")
	}
}

func TestScopedAggregatePrecedenceUsesEveryBlock(t *testing.T) {
	t.Parallel()
	states := []topology.OperationState{topology.OperationSucceeded, topology.OperationSuperseded, topology.OperationFailed,
		topology.OperationPending, topology.OperationWaitingForReplica, topology.OperationRunning}
	for _, first := range states {
		for _, second := range states {
			blocks := []topology.BlockStatus{{GroupID: "a", State: first}, {GroupID: "b", State: second}}
			result := aggregateBlockStates(blocks)
			if !first.Terminal() || !second.Terminal() {
				if result.Terminal() {
					t.Fatalf("active sibling hidden by aggregate: %v => %s", blocks, result)
				}
				continue
			}
			want := topology.OperationSucceeded
			if first == topology.OperationSuperseded || second == topology.OperationSuperseded {
				want = topology.OperationSuperseded
			}
			if first == topology.OperationFailed || second == topology.OperationFailed {
				want = topology.OperationFailed
			}
			if result != want {
				t.Fatalf("wrong aggregate precedence: %v => %s", blocks, result)
			}
		}
	}
}

func TestScopedTransitionLateRefusalPreservesPendingDeletion(t *testing.T) {
	t.Parallel()
	s := retryScopedDeletion()
	prepareScopedFixture(t, &s)
	op := s.Operations[len(s.Operations)-1]
	transition := scopedTransitionFor(op, "b", op.Status.Blocks[1].CurrentStep)
	transition.State = topology.OperationSuperseded
	before, _ := json.Marshal(s)
	if _, err := s.applyTransitionOperation(s.AppliedIndex+1, TransitionOperationProposal{Transition: transition}); err == nil {
		t.Fatal("supersession abandoned pending required stop")
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("late graph refusal changed ownership, revocation or deletion proof")
	}
}

func TestScopedTransitionRecordsOriginalConsumedRoleOnce(t *testing.T) {
	t.Parallel()
	s := scopedVoterFixture()
	s.VoterSwitches = nil
	s.AuthorityRecords[0].Closed = true
	for i := range s.Operations {
		op := &s.Operations[i]
		last := op.Plan.Steps[1]
		last.Seq = 3
		op.Plan.Steps = []topology.Step{op.Plan.Steps[0], {Seq: 2, Kind: topology.StepWaitCaughtUp,
			GroupID: topology.MetaGroupID, NodeName: "n2", ReplicaID: 2}, last}
		op.Plan.Blocks[0].Steps, op.Plan.Blocks[1].Steps = []int{1, 2}, []int{3}
		op.Status.Blocks[0].TotalSteps, op.Status.Blocks[1].CurrentStep = 2, 3
		op.Status.Blocks[0].State = topology.OperationFailed
	}
	s.Operations[1].Status.Blocks[0].State = topology.OperationPending
	s.Groups[0].SourceVersion, s.Groups[0].Declaration = 1, s.History[0].MetaGroup()
	s.Groups[0].Owner = s.Operations[1].Status.ID
	prepareScopedFixture(t, &s)
	transition := scopedTransitionFor(s.Operations[1], topology.MetaGroupID, 2)
	applyScopedTransitionFixture(t, &s, transition)
	r := s.VoterSwitches[0].Last
	if r.OperationID != "exec-v2-a1" || r.Attempt != 1 || r.StepSeq != 1 || r.Index != 11 ||
		r.From != topology.ReplicaLearner || r.To != topology.ReplicaVoter {
		t.Fatal("retry relabeled inherited role proof", r)
	}
	transition.At = transition.At.Add(time.Hour)
	result, err := s.applyTransitionOperation(12, TransitionOperationProposal{Transition: transition})
	if err != nil || result.changed || s.VoterSwitches[0].Last != r {
		t.Fatal("duplicate completion reset cooldown", result, err)
	}
}
