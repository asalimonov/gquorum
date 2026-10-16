//go:build !functional

package metaengine

import (
	"errors"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func pendingScopedDeletion() scopedState {
	s := scopedCodecFixture()
	d := &s.Cleanup[1]
	d.Stopped, d.StoppedAt, d.Retired, d.RetiredAt = nil, nil, nil, nil
	s.AuthorityRecords = slices.DeleteFunc(s.AuthorityRecords, func(r scopedAuthorityRecord) bool {
		return r.Grant.Key.GroupID == "b"
	})
	s.AuthorityFloors = slices.DeleteFunc(s.AuthorityFloors, func(r resourceFloor) bool { return r.Resource.GroupID == "b" })
	return s
}

func retryScopedDeletion() scopedState {
	s := pendingScopedDeletion()
	retry := s.Operations[2].Clone()
	retry.Previous, retry.Status.ID, retry.Status.Attempt = retry.Status.ID, "exec-v2-a2", 2
	retry.Status.State, retry.Status.Blocks[1].State = topology.OperationPending, topology.OperationPending
	s.Operations = append(s.Operations, retry)
	s.Groups[len(s.Groups)-1].Owner = retry.Status.ID
	action := *s.Cleanup[1].StopAction
	action.OperatorID, action.Attempt = topology.OperatorID(retry.Status.ID), 2
	s.Cleanup[1].StopAction = &action
	return s
}

func TestScopedCleanupFailedAndRetriedDeletionRemainRestorable(t *testing.T) {
	t.Parallel()
	for _, s := range []scopedState{pendingScopedDeletion(), retryScopedDeletion()} {
		if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
			t.Fatal("lost pending deletion or exact retry action", err)
		}
	}
	s := retryScopedDeletion()
	original := scopedCodecFixture()
	oldStop := original.AuthorityRecords[len(original.AuthorityRecords)-1]
	s.AuthorityRecords = append(s.AuthorityRecords, oldStop)
	s.AuthorityFloors = append(s.AuthorityFloors, resourceFloor{Resource: oldStop.Grant.Resources[0], Key: oldStop.Grant.Key})
	s.Cleanup[1].Stopped, s.Cleanup[1].StoppedAt = original.Cleanup[1].Stopped, original.Cleanup[1].StoppedAt
	if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
		t.Fatal("late consumed predecessor receipt lost its original milestone", err)
	}
	// The exact tip may subsequently record its own idempotent stop.
	grant := oldStop.Grant.Clone()
	grant.Action, grant.OperatorID, grant.Key.Revision = s.Cleanup[1].StopAction, "exec-v2-a2", 10
	receipt := fixtureEffectReceipts(grant)[0]
	s.AuthorityRecords = append(s.AuthorityRecords, scopedAuthorityRecord{Class: "execution",
		authorityRecord: authorityRecord{Grant: grant, Effects: []topology.EffectReceipt{receipt}}})
	s.AuthorityFloors[len(s.AuthorityFloors)-1].Key = grant.Key
	if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
		t.Fatal("successor receipt rewrote the first stop proof", err)
	}
}

func TestScopedCleanupRejectsLostProgressAndPrematureRetirement(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*scopedState){
		"receipt without milestones": func(s *scopedState) {
			s.Cleanup[1].Stopped, s.Cleanup[1].StoppedAt, s.Cleanup[1].Retired, s.Cleanup[1].RetiredAt = nil, nil, nil, nil
		},
		"retirement issued before stop": func(s *scopedState) {
			r := s.AuthorityRecords[1]
			*s = pendingScopedDeletion()
			r.Effects = nil
			s.AuthorityRecords = append(s.AuthorityRecords, r)
			s.AuthorityFloors = append(s.AuthorityFloors, resourceFloor{Resource: r.Grant.Resources[0], Key: r.Grant.Key})
		},
		"lost origin": func(s *scopedState) { s.Operations = s.Operations[:2] },
		"premature tombstone": func(s *scopedState) {
			*s = pendingScopedDeletion()
			r := &s.Groups[len(s.Groups)-1]
			r.Declaration, r.Deleting, r.SourceVersion = nil, false, 2
		},
		"superseded pending stop": func(s *scopedState) {
			*s = pendingScopedDeletion()
			s.Operations[2].Status.Blocks[1].State = topology.OperationSuperseded
		},
		"tip action not rebound": func(s *scopedState) {
			*s = retryScopedDeletion()
			s.Cleanup[1].StopAction.OperatorID, s.Cleanup[1].StopAction.Attempt = "exec-v2-a1", 1
		},
		"retire rebound": func(s *scopedState) {
			*s = retryScopedDeletion()
			s.Cleanup[1].RetireAction.OperatorID, s.Cleanup[1].RetireAction.Attempt = "exec-v2-a2", 2
		},
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := scopedCodecFixture()
			corrupt(&s)
			if _, err := prepareScopedGraph(&s, s.Rules); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("contradictory cleanup progress accepted", err)
			}
		})
	}
}

func TestScopedCleanupContextSurvivesUnrelatedConvergence(t *testing.T) {
	t.Parallel()
	s := scopedCodecFixture()
	for i := range s.Groups {
		r := &s.Groups[i]
		if r.Declaration != nil && !r.Deleting {
			r.SourceVersion, r.Declaration = s.Target.Version, s.Target.Group(r.GroupID)
		}
	}
	s.Nodes[1].SourceVersion, s.Nodes[1].Declaration = s.Target.Version, s.Target.Node("n")
	s.FullyConverged.Version = s.Target.Version
	if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
		t.Fatal("unrelated progress invalidated pinned cleanup context", err)
	}
}

func TestScopedCleanupCombinedStopRetireRequiresBothMilestones(t *testing.T) {
	t.Parallel()
	s := scopedCodecFixture()
	debt, record := s.Cleanup[0], s.AuthorityRecords[2].authorityRecord
	debt.RetireAction, debt.Retired = debt.StopAction, nil
	target := record.Grant.Targets[0]
	if cleanupGrantProgress(&debt, &record, target) {
		t.Fatal("combined stop receipt lost retirement milestone")
	}
	debt.Retired = debt.Stopped
	if !cleanupGrantProgress(&debt, &record, target) {
		t.Fatal("combined stop and retirement receipt refused")
	}
	debt.Stopped, debt.Retired, record.Effects = nil, nil, nil
	if !cleanupGrantProgress(&debt, &record, target) {
		t.Fatal("combined stop action required its own prior consumption")
	}
}
