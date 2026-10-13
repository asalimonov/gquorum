//go:build !functional

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
)

func TestMachineRetryAndSnapshotPreserveRequestIdentity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	machine := newMachine()
	command := func(id, value string, index uint64) load.Command {
		data, err := json.Marshal(putCommand{RequestID: id, Key: "key", Value: value})
		if err != nil {
			t.Fatal(err)
		}
		return load.Command{RequestID: id, Data: data, Index: index}
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
	for _, cmd := range []load.Command{{RequestID: "one", Data: []byte(`{"request_id":"other","key":"k","value":"v"}`)}, {Data: []byte(`null`)}, {RequestID: "one", Data: []byte(`{"request_id":"one","key":"k","value":"v","unexpected":1}`)}} {
		result, err := m.Apply(t.Context(), cmd)
		if err != nil || result.Code != string(fault.Invalid) {
			t.Fatal(result, err)
		}
	}
	if len(m.values()) != 0 {
		t.Fatal("invalid command mutated values")
	}
}

func TestQueryReturnsRestoredStateWithoutMutatingIt(t *testing.T) {
	t.Parallel()
	m := newMachine()
	data, _ := json.Marshal(putCommand{RequestID: "one", Key: "key", Value: "value"})
	if result, err := m.Apply(t.Context(), load.Command{RequestID: "one", Index: 7, Data: data}); err != nil || result.Code != "ok" {
		t.Fatal(result, err)
	}
	snapshot, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	m = newMachine()
	if err := m.Restore(t.Context(), snapshot); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"key", "missing"} {
		query, _ := json.Marshal(getQuery{Key: key})
		result, err := m.Query(t.Context(), query)
		var got getResult
		if err != nil || result.Code != "ok" || decodeJSON(result.Data, &got) != nil || got.Key != key || got.AppliedIndex != 7 || got.Found != (key == "key") || key == "key" && got.Value != "value" {
			t.Fatal(result, got, err)
		}
	}
	for _, query := range []string{`null`, `{}`, `{"key":"key","unknown":1}`, `{"key":"key"} {}`} {
		if _, err := m.Query(t.Context(), []byte(query)); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal(query, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := m.Query(ctx, []byte(`{"key":"key"}`)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	after, err := m.Snapshot(t.Context())
	if err != nil || !bytes.Equal(after, snapshot) {
		t.Fatal("read or rejected query changed application state", err)
	}
}
