// Package cache is the replicated state machine of the cache example: an
// in-memory key/value map without automatic expiration or eviction. Nothing
// is written to disk by the application; the library restores state from its
// Raft snapshot and log.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"github.com/asalimonov/gquorum/load"
)

const (
	codeInvalid = "invalid"
	codeOK      = "ok"
)

// Bounds for individual keys and values limit command size.
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
	values map[string]string
}

var _ load.StateMachine = (*Machine)(nil)
var _ load.QueryMachine = (*Machine)(nil)

// NewMachine creates an empty cache state machine.
func NewMachine() *Machine { return &Machine{values: make(map[string]string)} }

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
		m.values[cmd.Key] = cmd.Value
		return reply(codeOK, Count{N: 1}), nil
	case "del":
		removed := 0
		for _, key := range cmd.Keys {
			if _, ok := m.values[key]; ok {
				delete(m.values, key)
				removed++
			}
		}
		return reply(codeOK, Count{N: removed}), nil
	default:
		return load.Result{Code: codeInvalid}, nil
	}
}

// Query returns a read-only cache result for a validated query payload.
func (m *Machine) Query(_ context.Context, data []byte) (load.Result, error) {
	var q Query
	if json.Unmarshal(data, &q) != nil {
		return load.Result{Code: codeInvalid}, nil
	}
	switch q.Op {
	case "get":
		if value, ok := m.values[q.Key]; ok {
			return load.Result{Code: codeOK, Data: []byte(value)}, nil
		}
		return load.Result{Code: "nil"}, nil
	case "exists":
		found := 0
		for _, key := range q.Keys {
			if _, ok := m.values[key]; ok {
				found++
			}
		}
		return reply(codeOK, Count{N: found}), nil
	case "dbsize":
		return reply(codeOK, Count{N: len(m.values)}), nil
	default:
		return load.Result{Code: codeInvalid}, nil
	}
}

type snapshot struct {
	Format  int     `json:"format"`
	Entries []entry `json:"entries"`
}

// Snapshot encodes every entry in key order for deterministic output.
// The entry format remains compatible with snapshots from the bounded cache.
func (m *Machine) Snapshot(context.Context) ([]byte, error) {
	s := snapshot{Format: 1, Entries: make([]entry, 0, len(m.values))}
	for key, value := range m.values {
		s.Entries = append(s.Entries, entry{Key: key, Value: value})
	}
	slices.SortFunc(s.Entries, func(a, b entry) int { return strings.Compare(a.Key, b.Key) })
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
		if s.Format != 1 {
			return errors.New("invalid cache snapshot")
		}
		for _, e := range s.Entries {
			_, duplicate := restored.values[e.Key]
			if duplicate || len(e.Value) > MaxValueBytes || !validKey(e.Key) {
				return errors.New("invalid cache snapshot entry")
			}
			restored.values[e.Key] = e.Value
		}
	}
	*m = *restored
	return nil
}

func validKey(key string) bool { return key != "" && len(key) <= MaxKeyBytes }
