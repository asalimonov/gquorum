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

func failedScopedAttempt(t *testing.T) scopedState {
	t.Helper()
	s := scopedGraphFixture()
	s.Operations = s.Operations[:1]
	s.Groups[1].Owner = ""
	prepareScopedFixture(t, &s)
	return s
}

func scopedRetryProposal(s *scopedState, op *topology.Operation) RetryOperationProposal {
	p := RetryOperationProposal{Previous: op.Status.ID, At: op.UpdatedAt.Add(time.Second), ProposalID: "retry"}
	for _, ref := range failedBlockRefs(op) {
		row := s.indexes.groups[ref.GroupID]
		p.Scopes = append(p.Scopes, ScopeExpectation{GroupID: ref.GroupID, SourceVersion: row.SourceVersion, Owner: row.Owner})
	}
	return p
}

func applyScopedRetryFixture(t *testing.T, s *scopedState, p RetryOperationProposal) proposalApplication {
	t.Helper()
	body, _ := json.Marshal(p)
	result, err := applyScopedProposal(s, s.AppliedIndex+1, "retry_operation", body)
	if err != nil || !result.result.Accepted {
		t.Fatal("retry refused", result, err)
	}
	encoded, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var restored scopedState
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal("retry cannot restore", err)
	}
	return result
}

func TestScopedRetryPreservesPlanSiblingsAndImmediateSuccessor(t *testing.T) {
	t.Parallel()
	for _, sibling := range []topology.OperationState{topology.OperationSucceeded, topology.OperationSuperseded} {
		s := failedScopedAttempt(t)
		s.Operations[0].Status.Blocks[0].State = sibling
		prepareScopedFixture(t, &s)
		previous := s.Operations[0].Clone()
		p := scopedRetryProposal(&s, &previous)
		issued := applyScopedRetryFixture(t, &s, p)
		retry := s.indexes.operations[topology.OperationID(issued.result.OperatorID)]
		if !issued.changed || retry.Previous != previous.Status.ID || !reflect.DeepEqual(retry.Plan, previous.Plan) ||
			!reflect.DeepEqual(retry.Status.Blocks[0], previous.Status.Blocks[0]) ||
			s.indexes.groups[topology.MetaGroupID].Owner != "" || s.indexes.groups["shard0"].Owner != retry.Status.ID {
			t.Fatal("retry changed immutable plan or inherited sibling", retry)
		}
		transition := scopedTransitionFor(*retry, "shard0", 2)
		transition.State, transition.Error = topology.OperationFailed, "offline"
		if _, err := s.applyTransitionOperation(s.AppliedIndex+1, TransitionOperationProposal{Transition: transition}); err != nil {
			t.Fatal(err)
		}
		before, _ := json.Marshal(s)
		p.ProposalID, p.At, p.Scopes = "concurrent/lost-reply", time.Time{}, []ScopeExpectation{}
		repeat := applyScopedRetryFixture(t, &s, p)
		after, _ := json.Marshal(s)
		if repeat.changed || repeat.result.OperatorID != issued.result.OperatorID || !bytes.Equal(before, after) {
			t.Fatal("repeat retry selected a different attempt or wrote state")
		}
		latest := s.indexes.operations[topology.OperationID(issued.result.OperatorID)]
		third := applyScopedRetryFixture(t, &s, scopedRetryProposal(&s, latest))
		if third.result.OperatorID != "exec-v2-a3" {
			t.Fatal("explicit retry of second failure did not extend the chain", third)
		}
		original, err := queryOperationStatus(&s, previous.Status.ID)
		if err != nil || !reflect.DeepEqual(original, previous.Status) {
			t.Fatal("historical lookup followed a later retry", original, err)
		}
	}
}

func TestScopedRetryRejectsMissingStaleAndNonterminalScopesAtomically(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*scopedState, *RetryOperationProposal){
		"missing": func(_ *scopedState, p *RetryOperationProposal) { p.Previous = "missing" },
		"capture": func(_ *scopedState, p *RetryOperationProposal) { p.Scopes[0].SourceVersion++ },
		"owner":   func(_ *scopedState, p *RetryOperationProposal) { p.Scopes[0].Owner = "other" },
		"clock":   func(_ *scopedState, p *RetryOperationProposal) { p.At = time.Time{} },
		"running sibling": func(s *scopedState, _ *RetryOperationProposal) {
			s.Operations[0].Status.State, s.Operations[0].Status.Blocks[0].State = topology.OperationRunning, topology.OperationRunning
			s.Groups[0].Owner = s.Operations[0].Status.ID
			s.Groups[0].SourceVersion, s.Groups[0].Declaration = 1, s.History[0].MetaGroup()
		},
		"new desired scope": func(s *scopedState, _ *RetryOperationProposal) {
			s.History = append(s.History, s.Target.Clone())
			s.Target.Version, s.Target.ParentVersion = 3, 2
			s.Target.Group("shard0").Labels = map[string]string{"owner": "new"}
			s.Latest = s.Target.Clone()
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := failedScopedAttempt(t)
			p := scopedRetryProposal(&s, &s.Operations[0])
			mutate(&s, &p)
			prepareScopedFixture(t, &s)
			before, _ := json.Marshal(s)
			_, err := s.applyRetryOperation(s.AppliedIndex+1, p)
			if err == nil || name == "missing" && !errors.Is(err, fault.ErrHistoryUnavailable) {
				t.Fatal("invalid retry accepted or history classified incorrectly", err)
			}
			after, _ := json.Marshal(s)
			if !bytes.Equal(before, after) {
				t.Fatal("refused retry changed state")
			}
		})
	}
}

func TestScopedRetryAllowsUnrelatedTargetAndKeepsPendingStopOrigin(t *testing.T) {
	t.Parallel()
	s := pendingScopedDeletion()
	// The original failed A scope was superseded; only B is eligible for retry.
	s.Operations[2].Status.Blocks[0].State = topology.OperationSuperseded
	prepareScopedFixture(t, &s)
	previous := s.indexes.operations["exec-v2-a1"]
	debt := s.Cleanup[1].clone()
	result := applyScopedRetryFixture(t, &s, scopedRetryProposal(&s, previous))
	retry := s.indexes.operations[topology.OperationID(result.result.OperatorID)]
	got := s.indexes.cleanup[debt.Identity]
	if got.StopAction.OperatorID != result.result.OperatorID || got.OperationID != debt.OperationID ||
		!reflect.DeepEqual(got.RetireAction, debt.RetireAction) ||
		retry.Status.Blocks[0].State != topology.OperationSuperseded || s.Target.Version != 5 {
		t.Fatal("retry lost required stop binding or reclaimed newer independent scope")
	}
}

func TestScopedRetryDoesNotRebindConsumedStopOrFenceDetachedRetirement(t *testing.T) {
	t.Parallel()
	s := scopedCodecFixture()
	s.Operations[2].Status.Blocks[0].State = topology.OperationSuperseded
	prepareScopedFixture(t, &s)
	old := s.indexes.operations["exec-v2-a1"]
	debt := s.Cleanup[1].clone()
	retirement := *s.indexes.grants[debt.Retired.Key]
	applyScopedRetryFixture(t, &s, scopedRetryProposal(&s, old))
	if !reflect.DeepEqual(s.indexes.cleanup[debt.Identity], &debt) ||
		!reflect.DeepEqual(*s.indexes.grants[debt.Retired.Key], retirement) {
		t.Fatal("retry reclaimed already consumed stop or unrelated deferred work")
	}
}
