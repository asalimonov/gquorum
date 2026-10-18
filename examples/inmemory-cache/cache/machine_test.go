//go:build !functional

package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/asalimonov/gquorum/load"
)

func apply(t *testing.T, m *Machine, cmd Command) load.Result {
	t.Helper()
	data, _ := json.Marshal(cmd)
	out, err := m.Apply(context.Background(), load.Command{Data: data})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func get(t *testing.T, m *Machine, key string) (string, bool) {
	t.Helper()
	data, _ := json.Marshal(Query{Op: "get", Key: key})
	out, err := m.Query(context.Background(), data)
	if err != nil {
		t.Fatal(err)
	}
	return string(out.Data), out.Code == "ok"
}

func key(i int) string { return fmt.Sprintf("key:%04d", i) }

func TestRetainsKeysBeyondFormerCapacity(t *testing.T) {
	t.Parallel()
	m := NewMachine()
	const count = 2048
	for i := range count {
		apply(t, m, Command{Op: "set", Key: key(i), Value: fmt.Sprint(i)})
	}
	apply(t, m, Command{Op: "set", Key: key(0), Value: "updated"})
	for i := range count {
		want := fmt.Sprint(i)
		if i == 0 {
			want = "updated"
		}
		if value, ok := get(t, m, key(i)); !ok || value != want {
			t.Fatalf("key %d lost or changed: %q, present=%v", i, value, ok)
		}
	}
	data, _ := json.Marshal(Query{Op: "dbsize"})
	if out, _ := m.Query(context.Background(), data); string(out.Data) != `{"n":2048}` {
		t.Fatal("incorrect size", string(out.Data))
	}
}

func TestDeleteExistsAndInvalidCommands(t *testing.T) {
	t.Parallel()
	m := NewMachine()
	apply(t, m, Command{Op: "set", Key: "a", Value: "1"})
	apply(t, m, Command{Op: "set", Key: "b", Value: "2"})
	data, _ := json.Marshal(Query{Op: "exists", Keys: []string{"a", "b", "c"}})
	if out, _ := m.Query(context.Background(), data); string(out.Data) != `{"n":2}` {
		t.Fatal(string(out.Data))
	}
	if out := apply(t, m, Command{Op: "del", Keys: []string{"a", "c"}}); string(out.Data) != `{"n":1}` {
		t.Fatal(string(out.Data))
	}
	for _, bad := range []Command{{Op: "set"}, {Op: "incr", Key: "a"}, {Op: "set", Key: "k", Value: string(make([]byte, MaxValueBytes+1))}} {
		if out := apply(t, m, bad); out.Code != "invalid" {
			t.Fatal("accepted", bad.Op, out.Code)
		}
	}
	if out, err := m.Apply(context.Background(), load.Command{Data: []byte("{")}); err != nil || out.Code != "invalid" {
		t.Fatal(out, err)
	}
}

func TestSnapshotPreservesAllValues(t *testing.T) {
	t.Parallel()
	m := NewMachine()
	for i := range 1024 {
		apply(t, m, Command{Op: "set", Key: key(i), Value: fmt.Sprint(i)})
	}
	apply(t, m, Command{Op: "set", Key: key(0), Value: "updated"})
	data, err := m.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	restored := NewMachine()
	if err := restored.Restore(context.Background(), data); err != nil {
		t.Fatal(err)
	}
	again, _ := restored.Snapshot(context.Background())
	if string(again) != string(data) {
		t.Fatal("snapshot changed", string(again))
	}
	for i := range 1024 {
		want, _ := get(t, m, key(i))
		if got, ok := get(t, restored, key(i)); !ok || got != want {
			t.Fatalf("restored key %d: %q, present=%v, want %q", i, got, ok, want)
		}
	}
	if err := restored.Restore(context.Background(), nil); err != nil || len(restored.values) != 0 {
		t.Fatal("Restore(nil) kept entries", err)
	}
	if err := restored.Restore(context.Background(), []byte(`{"format":1,"entries":[{"Key":"a"},{"Key":"a"}]}`)); err == nil {
		t.Fatal("duplicate snapshot entries accepted")
	}
}

func TestRestoreLegacySnapshot(t *testing.T) {
	t.Parallel()
	m := NewMachine()
	// The old cache stored entries in write order, with the same format.
	data := []byte(`{"format":1,"entries":[{"Key":"z","Value":"last"},{"Key":"a","Value":""}]}`)
	if err := m.Restore(context.Background(), data); err != nil {
		t.Fatal(err)
	}
	if value, ok := get(t, m, "z"); !ok || value != "last" {
		t.Fatal("lost legacy value", value, ok)
	}
	if value, ok := get(t, m, "a"); !ok || value != "" {
		t.Fatal("lost empty value", value, ok)
	}
	if err := m.Restore(context.Background(), []byte(`{"format":1,"entries":[{"Key":"a"},{"Key":"a"}]}`)); err == nil {
		t.Fatal("accepted duplicate empty values")
	}
	if value, ok := get(t, m, "z"); !ok || value != "last" {
		t.Fatal("failed restore changed existing state", value, ok)
	}
}
