//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func TestScopedCleanupUpsertPreservesCompletedDebtAndOwnsProof(t *testing.T) {
	t.Parallel()
	s := scopedCodecFixture()
	before, _ := json.Marshal(s)
	incoming := s.Cleanup[0].clone()
	incoming.Removed, incoming.Stopped, incoming.Retired, incoming.Collection = nil, nil, nil, nil
	incoming.RemovedAt, incoming.StoppedAt, incoming.RetiredAt = nil, nil, nil
	incoming.UpdatedAt = incoming.UpdatedAt.Add(time.Hour)
	if err := s.upsertCleanupLocked(incoming); err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("repeated removal resurrected completed debt or rewrote timestamps")
	}
	copy := s.Cleanup[0].clone()
	copy.StopAction.NodeName, copy.RetireAction.NodeName = "foreign", "foreign"
	copy.Removed.Action.NodeName, copy.Stopped.Action.NodeName, copy.Retired.Action.NodeName = "foreign", "foreign", "foreign"
	copy.Collection.Token = "caller"
	*copy.RemovedAt = copy.RemovedAt.Add(time.Hour)
	after, _ = json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("owned cleanup record shared mutable proof")
	}
}

func TestScopedCleanupUpsertAddsExactMilestonesWithoutMovingTheirOrigin(t *testing.T) {
	t.Parallel()
	s := pendingReplacementCleanup()
	original := s.Cleanup[0].clone()
	completed := scopedCodecFixture().Cleanup[0].clone()
	completed.CreatedAt, completed.UpdatedAt = original.CreatedAt.Add(time.Minute), original.CreatedAt.Add(time.Minute)
	completed.RemovedAt, completed.StoppedAt, completed.RetiredAt = &completed.CreatedAt, &completed.CreatedAt, &completed.CreatedAt
	if err := s.upsertCleanupLocked(completed); err != nil {
		t.Fatal(err)
	}
	got := s.Cleanup[0]
	if got.CreatedAt != original.CreatedAt || !reflect.DeepEqual(got.Removed, original.Removed) ||
		*got.RemovedAt != *original.RemovedAt || got.Stopped == nil || got.Retired == nil || got.Collection == nil {
		t.Fatal("monotonic merge lost the original removal boundary", got)
	}
	completed.Stopped.Action.NodeName = "caller"
	if got.Stopped.Action.NodeName == "caller" {
		t.Fatal("new milestone aliases the caller")
	}
	before, _ := json.Marshal(s)
	replay := got.clone()
	later := got.UpdatedAt.Add(time.Hour)
	replay.UpdatedAt, replay.StoppedAt, replay.RetiredAt = later, &later, &later
	if err := s.upsertCleanupLocked(replay); err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("receipt replay moved recorded milestone clocks")
	}
}

func TestScopedCleanupUpsertRejectsConflictingProofWithoutMutation(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*cleanupRecord){
		"member":         func(r *cleanupRecord) { r.MemberID++ },
		"storage":        func(r *cleanupRecord) { r.Storage = topology.StorageMemory; r.Collection = nil },
		"origin":         func(r *cleanupRecord) { r.OperationID = "exec-v3-a1" },
		"stop binding":   func(r *cleanupRecord) { r.StopAction.OperatorID = "exec-v3-a1" },
		"retire binding": func(r *cleanupRecord) { r.RetireAction.StepSeq++ },
		"removal":        func(r *cleanupRecord) { r.Removed.AppliedIndex++ },
		"stopped":        func(r *cleanupRecord) { r.Stopped.LocalSequence++ },
		"retired":        func(r *cleanupRecord) { r.Retired.LocalSequence++ },
		"collection":     func(r *cleanupRecord) { r.Collection.Token = "foreign" },
		"missing time":   func(r *cleanupRecord) { r.StoppedAt = nil },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := scopedCodecFixture()
			before, _ := json.Marshal(s)
			incoming := s.Cleanup[0].clone()
			mutate(&incoming)
			if err := s.upsertCleanupLocked(incoming); err == nil {
				t.Fatal("conflicting cleanup proof accepted")
			}
			after, _ := json.Marshal(s)
			if !bytes.Equal(before, after) {
				t.Fatal("refused proof changed retained debt")
			}
		})
	}
}
