//go:build !functional

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

func configuredSyntheticMachine(t *testing.T) *kvMachine {
	t.Helper()
	m := newMachine()
	configure, _ := json.Marshal(putCommand{RequestID: "configure", Synthetic: &syntheticSpec{Version: 1, Seed: 42, EncodedBytes: 256 << 10}})
	result, err := m.Apply(t.Context(), load.Command{RequestID: "configure", Index: 1, Data: configure})
	if err != nil || result.Code != "ok" {
		t.Fatal(result, err)
	}
	data, _ := json.Marshal(putCommand{RequestID: "put", Key: "key", Value: "value"})
	result, err = m.Apply(t.Context(), load.Command{RequestID: "put", Index: 2, Data: data})
	if err != nil || result.Code != "ok" {
		t.Fatal(result, err)
	}
	return m
}
func TestSyntheticSnapshotDeterministicCompleteStateAndTail(t *testing.T) {
	t.Parallel()
	m := configuredSyntheticMachine(t)
	data, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	again, err := m.Snapshot(t.Context())
	if err != nil || !bytes.Equal(data, again) {
		t.Fatal("same state generated different bytes", err)
	}
	restored := newMachine()
	if err = restored.Restore(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m.state, restored.state) || restored.syntheticView().Restores != 1 || restored.syntheticView().Counter != 1 {
		t.Fatal("incomplete synthetic restore", restored.state)
	}
	next, _ := json.Marshal(putCommand{RequestID: "tail", Key: "tail", Value: "committed"})
	result, err := restored.Apply(t.Context(), load.Command{RequestID: "tail", Index: 3, Data: next})
	if err != nil || result.Code != "ok" {
		t.Fatal(result, err)
	}
	result, err = restored.Apply(t.Context(), load.Command{RequestID: "tail", Index: 4, Data: next})
	if err != nil || result.Code != "ok" {
		t.Fatal(result, err)
	}
	if restored.syntheticView().Counter != 2 || restored.values()["tail"] != "committed" {
		t.Fatal("tail/dedup semantics changed")
	}
	final, err := restored.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(data, final) {
		t.Fatal("tail state absent from generated snapshot")
	}
}
func TestSyntheticSnapshotCorruptionTruncationAndMissingPayloadRefuse(t *testing.T) {
	t.Parallel()
	m := configuredSyntheticMachine(t)
	data, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"corrupt", "truncated", "forged-digest", "json-only"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			value := bytes.Clone(data)
			switch kind {
			case "corrupt":
				value[len(value)/2] ^= 1
			case "truncated":
				value = value[:len(value)-1]
			case "forged-digest":
				value[len(value)/2] ^= 1
				sum := sha256.Sum256(value[:len(value)-sha256.Size])
				copy(value[len(value)-sha256.Size:], sum[:])
			case "json-only":
				value, _ = json.Marshal(m.state)
			}
			restored := newMachine()
			before := restored.state
			if err := restored.Restore(t.Context(), value); err == nil {
				t.Fatal("partial/forged state accepted")
			}
			if !reflect.DeepEqual(before, restored.state) {
				t.Fatal("failed restore published state")
			}
		})
	}
}
func TestSyntheticGenerationIsExplicitAndCancelable(t *testing.T) {
	t.Parallel()
	normal := newMachine()
	small, err := normal.Snapshot(t.Context())
	if err != nil || len(small) > 1024 || bytes.HasPrefix(small, []byte(syntheticMagic)) {
		t.Fatal("default allocated synthetic data", len(small), err)
	}
	m := configuredSyntheticMachine(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err = m.Snapshot(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err = newMachine().Restore(ctx, []byte(syntheticMagic)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	spec := syntheticSpec{Version: 2, EncodedBytes: 256 << 10}
	if err = spec.validate(); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
}

func TestSnapshotRestoreFailureBudgetIsExactAndDoesNotReachSuccessor(t *testing.T) {
	t.Parallel()
	a := &application{clusterHooks: newClusterHooks()}
	key := walKey{"work", 2}
	id := topology.Identity{NodeName: "node", InstanceID: "original"}
	a.clusterHooks.snapshotControls[key] = &snapshotControl{request: snapshotControlRequest{Identity: id, FailRestoreCount: 3}, hits: make(map[string]uint64)}
	replacement := id
	replacement.InstanceID = "successor"
	if err := a.snapshotRestoreFailure(replacement, key); err != nil {
		t.Fatal("old fault reached successor", err)
	}
	for range 3 {
		if err := a.snapshotRestoreFailure(id, key); !errors.Is(err, fault.ErrUnavailable) {
			t.Fatal(err)
		}
	}
	if err := a.snapshotRestoreFailure(id, key); err != nil {
		t.Fatal("restore failure budget did not expire", err)
	}
}

func TestSyntheticSnapshotCalibratedEnvelopeBudget(t *testing.T) {
	t.Parallel()
	m := configuredSyntheticMachine(t)
	m.state.Synthetic.Spec.FramingBytes = 123
	data, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	encoded := uint64(123) + 4*((uint64(len(data))+2)/3)
	ceiling := m.state.Synthetic.Spec.EncodedBytes
	if encoded > ceiling || ceiling-encoded >= 4 {
		t.Fatal("calibrated size exceeded ceiling/granularity", encoded, ceiling)
	}
	restored := newMachine()
	if err = restored.Restore(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restored.state, m.state) {
		t.Fatal("calibrated state changed on restore")
	}
}
