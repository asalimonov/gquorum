package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftengine"
)

func TestMachineRetryAndSnapshotPreserveRequestIdentity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	machine := newMachine()
	command := func(id, value string, index uint64) raftengine.Command {
		data, err := json.Marshal(putCommand{RequestID: id, Key: "key", Value: value})
		if err != nil {
			t.Fatal(err)
		}
		return raftengine.Command{RequestID: id, Data: data, Index: index}
	}
	if result, err := machine.Apply(ctx, command("one", "first", 1)); err != nil || result.Code != "ok" {
		t.Fatal(result, err)
	}
	if result, err := machine.Apply(ctx, command("two", "second", 2)); err != nil || result.Code != "ok" {
		t.Fatal(result, err)
	}
	snapshot, err := machine.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	restored := newMachine()
	if err := restored.Restore(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if result, err := restored.Apply(ctx, command("one", "first", 3)); err != nil || result.Code != "ok" {
		t.Fatal(result, err)
	}
	if value, found, index := restored.get("key"); !found || value != "second" || index != 3 {
		t.Fatal("retry overwrote newer value", value, found, index)
	}
	if result, err := restored.Apply(ctx, command("one", "different", 4)); err != nil || result.Code != string(fault.VersionConflict) {
		t.Fatal(result, err)
	}
	if value, _, _ := restored.get("key"); value != "second" {
		t.Fatal(value)
	}
	before, _ := restored.Snapshot(ctx)
	if err := restored.Restore(ctx, []byte(`{"schema_version":2,"values":{},"requests":{}}`)); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal(err)
	}
	after, _ := restored.Snapshot(ctx)
	if !bytes.Equal(before, after) {
		t.Fatal("failed restore mutated machine")
	}
}
func TestInvalidLoadCommandsAreBusinessResults(t *testing.T) {
	t.Parallel()
	m := newMachine()
	for _, cmd := range []raftengine.Command{{RequestID: "one", Data: []byte(`{"request_id":"other","key":"k","value":"v"}`)}, {Data: []byte(`null`)}, {RequestID: "one", Data: []byte(`{"request_id":"one","key":"k","value":"v","unexpected":1}`)}} {
		result, err := m.Apply(t.Context(), cmd)
		if err != nil || result.Code != string(fault.Invalid) {
			t.Fatal(result, err)
		}
	}
	if len(m.values()) != 0 {
		t.Fatal("invalid command mutated values")
	}
}
