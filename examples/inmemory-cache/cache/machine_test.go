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

func TestEvictsOldestWriteBeyondCapacity(t *testing.T) {
	t.Parallel()
	m := NewMachine()
	for i := range Capacity {
		apply(t, m, Command{Op: "set", Key: key(i), Value: "v"})
	}
	// Rewriting key 0 makes it the newest, so key 1 is now the oldest.
	apply(t, m, Command{Op: "set", Key: key(0), Value: "v2"})
	out := apply(t, m, Command{Op: "set", Key: key(Capacity), Value: "v"})
	var count Count
	if err := json.Unmarshal(out.Data, &count); err != nil || count.Evicted != 1 {
		t.Fatal(out, err)
	}
	if _, ok := get(t, m, key(1)); ok {
		t.Fatal("oldest key survived")
	}
	if v, ok := get(t, m, key(0)); !ok || v != "v2" {
		t.Fatal("rewritten key evicted", v, ok)
	}
	if m.order.Len() != Capacity || len(m.index) != Capacity {
		t.Fatal("capacity exceeded", m.order.Len())
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

func TestSnapshotPreservesEvictionOrder(t *testing.T) {
	t.Parallel()
	m := NewMachine()
	for i := range 5 {
		apply(t, m, Command{Op: "set", Key: key(i), Value: fmt.Sprint(i)})
	}
	apply(t, m, Command{Op: "set", Key: key(0), Value: "newest"})
	data, err := m.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	restored := NewMachine()
	if err := restored.Restore(context.Background(), data); err != nil {
		t.Fatal(err)
	}
	again, _ := restored.Snapshot(context.Background())
	if string(again) != string(data) || restored.order.Front().Value.(*entry).Key != key(1) {
		t.Fatal("order changed", string(again))
	}
	if err := restored.Restore(context.Background(), nil); err != nil || restored.order.Len() != 0 {
		t.Fatal("Restore(nil) kept entries", err)
	}
	if err := restored.Restore(context.Background(), []byte(`{"format":1,"entries":[{"Key":"a"},{"Key":"a"}]}`)); err == nil {
		t.Fatal("duplicate snapshot entries accepted")
	}
}
