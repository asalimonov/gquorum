// Package cache is the replicated state machine of the cache example: an
// in-memory map bounded to Capacity entries that evicts the entry written
// longest ago. Every replica applies the same commands in the same order, so
// eviction is identical everywhere. Nothing is written to disk by the
// application; the library restores state from its Raft snapshot and log.
package cache

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"

	"github.com/asalimonov/gquorum/load"
)

// Capacity is the maximum number of keys; writing a new key beyond it evicts
// the oldest one. A SET of an existing key makes it the newest.
const Capacity = 512

const (
	codeInvalid = "invalid"
	codeOK      = "ok"
)

// Bounds for keys and values keep complete snapshots small.
const (
	MaxKeyBytes   = 1 << 10
	MaxValueBytes = 4 << 10
)

// Command is one replicated write.
type Command struct {
	Op    string   `json:"op"` // set or del
	Key   string   `json:"key,omitempty"`
	Value string   `json:"value,omitempty"`
	Keys  []string `json:"keys,omitempty"`
}

// Query is one read served after a quorum read barrier.
type Query struct {
	Op   string   `json:"op"` // get, exists or dbsize
	Key  string   `json:"key,omitempty"`
	Keys []string `json:"keys,omitempty"`
}

type entry struct{ Key, Value string }

// Machine implements load.StateMachine and load.QueryMachine. The library
// serializes Apply, Query, Snapshot and Restore, so it needs no lock.
type Machine struct {
	index map[string]*list.Element
	order *list.List // front is the oldest write
}

var _ load.StateMachine = (*Machine)(nil)
var _ load.QueryMachine = (*Machine)(nil)

// NewMachine creates an empty cache state machine with deterministic insertion
// order.
func NewMachine() *Machine { return &Machine{index: map[string]*list.Element{}, order: list.New()} }

func reply(code string, value any) load.Result {
	data, _ := json.Marshal(value)
	return load.Result{Code: code, Data: data}
}

// Apply never fails for bad input: an invalid command is a deterministic
// refusal, so every replica stays in step.
func (m *Machine) Apply(_ context.Context, c load.Command) (load.Result, error) {
	var cmd Command
	if json.Unmarshal(c.Data, &cmd) != nil {
		return load.Result{Code: codeInvalid}, nil
	}
	switch cmd.Op {
	case "set":
		keyValid := validKey(cmd.Key)
		if !keyValid || len(cmd.Value) > MaxValueBytes {
			return load.Result{Code: codeInvalid}, nil
		}
		return reply(codeOK, Count{N: 1, Evicted: m.set(cmd.Key, cmd.Value)}), nil
	case "del":
		removed := 0
		for _, key := range cmd.Keys {
			if e, ok := m.index[key]; ok {
				m.order.Remove(e)
				delete(m.index, key)
				removed++
			}
		}
		return reply(codeOK, Count{N: removed}), nil
	default:
		return load.Result{Code: codeInvalid}, nil
	}
}

func (m *Machine) set(key, value string) (evicted int) {
	if e, ok := m.index[key]; ok {
		e.Value.(*entry).Value = value
		m.order.MoveToBack(e)
		return 0
	}
	m.index[key] = m.order.PushBack(&entry{Key: key, Value: value})
	for m.order.Len() > Capacity {
		oldest := m.order.Front()
		m.order.Remove(oldest)
		delete(m.index, oldest.Value.(*entry).Key)
		evicted++
	}
	return evicted
}

// Query returns a read-only cache result for a validated query payload.
func (m *Machine) Query(_ context.Context, data []byte) (load.Result, error) {
	var q Query
	if json.Unmarshal(data, &q) != nil {
		return load.Result{Code: codeInvalid}, nil
	}
	switch q.Op {
	case "get":
		if e, ok := m.index[q.Key]; ok {
			return load.Result{Code: codeOK, Data: []byte(e.Value.(*entry).Value)}, nil
		}
		return load.Result{Code: "nil"}, nil
	case "exists":
		found := 0
		for _, key := range q.Keys {
			if _, ok := m.index[key]; ok {
				found++
			}
		}
		return reply(codeOK, Count{N: found}), nil
	case "dbsize":
		return reply(codeOK, Count{N: m.order.Len()}), nil
	default:
		return load.Result{Code: codeInvalid}, nil
	}
}

type snapshot struct {
	Format  int     `json:"format"`
	Entries []entry `json:"entries"` // oldest first
}

// Snapshot encodes every entry in eviction order.
func (m *Machine) Snapshot(context.Context) ([]byte, error) {
	s := snapshot{Format: 1, Entries: make([]entry, 0, m.order.Len())}
	for e := m.order.Front(); e != nil; e = e.Next() {
		s.Entries = append(s.Entries, *e.Value.(*entry))
	}
	return json.Marshal(s)
}

// Restore replaces all state; Restore(nil) is the empty cache.
func (m *Machine) Restore(_ context.Context, data []byte) error {
	restored := NewMachine()
	if len(data) > 0 {
		var s snapshot
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		if s.Format != 1 || len(s.Entries) > Capacity {
			return errors.New("invalid cache snapshot")
		}
		for _, e := range s.Entries {
			keyValid := validKey(e.Key)
			if !keyValid || len(e.Value) > MaxValueBytes || restored.index[e.Key] != nil {
				return errors.New("invalid cache snapshot entry")
			}
			restored.set(e.Key, e.Value)
		}
	}
	*m = *restored
	return nil
}

func validKey(key string) bool { return key != "" && len(key) <= MaxKeyBytes }
