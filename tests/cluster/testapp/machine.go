package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"sync"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
)

type putCommand struct {
	RequestID string `json:"request_id"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}
type getQuery struct {
	Key string `json:"key"`
}
type getResult struct {
	Key          string `json:"key"`
	Value        string `json:"value"`
	Found        bool   `json:"found"`
	AppliedIndex uint64 `json:"applied_index"`
}

type machineState struct {
	SchemaVersion int               `json:"schema_version"`
	Values        map[string]string `json:"values"`
	Requests      map[string]string `json:"requests"`
	AppliedIndex  uint64            `json:"applied_index"`
}
type kvMachine struct {
	mu    sync.RWMutex
	state machineState
}

func newMachine() *kvMachine {
	return &kvMachine{state: machineState{SchemaVersion: 1, Values: map[string]string{}, Requests: map[string]string{}}}
}
func (m *kvMachine) Apply(ctx context.Context, cmd load.Command) (load.Result, error) {
	if err := ctx.Err(); err != nil {
		return load.Result{}, err
	}
	var put putCommand
	if err := decodeJSON(cmd.Data, &put); err != nil || put.RequestID == "" || len(put.RequestID) > 128 || put.RequestID != cmd.RequestID || put.Key == "" || len(put.Key) > 256 || len(put.Value) > 1<<16 {
		return load.Result{Code: string(fault.Invalid)}, nil
	}
	hash := sha256.Sum256(cmd.Data)
	fingerprint := hex.EncodeToString(hash[:])
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.AppliedIndex = cmd.Index
	if old, found := m.state.Requests[put.RequestID]; found {
		if old != fingerprint {
			return load.Result{Code: string(fault.VersionConflict)}, nil
		}
		return load.Result{Code: "ok"}, nil
	}
	m.state.Values[put.Key], m.state.Requests[put.RequestID] = put.Value, fingerprint
	return load.Result{Code: "ok"}, nil
}

// Query executes on the library's machine owner after quorum confirmation and
// local application. get remains the explicit local diagnostic accessor.
func (m *kvMachine) Query(ctx context.Context, data []byte) (load.Result, error) {
	if err := ctx.Err(); err != nil {
		return load.Result{}, err
	}
	var query getQuery
	if err := decodeJSON(data, &query); err != nil {
		return load.Result{}, err
	}
	if query.Key == "" || len(query.Key) > 256 {
		return load.Result{}, fault.ErrInvalid
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, found := m.state.Values[query.Key]
	encoded, err := json.Marshal(getResult{Key: query.Key, Value: value, Found: found, AppliedIndex: m.state.AppliedIndex})
	return load.Result{Code: "ok", Data: encoded}, err
}

func (m *kvMachine) Snapshot(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return json.Marshal(m.state)
}
func (m *kvMachine) Restore(ctx context.Context, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	state := machineState{SchemaVersion: 1, Values: map[string]string{}, Requests: map[string]string{}}
	if len(data) != 0 {
		if err := decodeJSON(data, &state); err != nil {
			return fault.New(fault.Unreadable, "invalid example snapshot", err)
		}
		if state.SchemaVersion != 1 || state.Values == nil || state.Requests == nil {
			return fault.New(fault.Unreadable, "incomplete example snapshot", nil)
		}
		for key, value := range state.Values {
			if key == "" || len(key) > 256 || len(value) > 1<<16 {
				return fault.New(fault.Unreadable, "invalid example value", nil)
			}
		}
		for id, fingerprint := range state.Requests {
			decoded, err := hex.DecodeString(fingerprint)
			if id == "" || len(id) > 128 || err != nil || len(decoded) != sha256.Size {
				return fault.New(fault.Unreadable, "invalid request fingerprint", err)
			}
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = state
	return nil
}
func (m *kvMachine) get(key string) (string, bool, uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, found := m.state.Values[key]
	return value, found, m.state.AppliedIndex
}
func (m *kvMachine) values() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return maps.Clone(m.state.Values)
}
func decodeJSON(data []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return fault.New(fault.Invalid, "invalid JSON", err)
	}
	var extra any
	if err := d.Decode(&extra); err == nil {
		return fault.New(fault.Invalid, "trailing JSON", nil)
	} else if !errors.Is(err, io.EOF) {
		return fault.New(fault.Invalid, "invalid trailing JSON", err)
	}
	return nil
}
