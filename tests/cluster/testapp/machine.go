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
	Synthetic *syntheticSpec `json:"synthetic,omitempty"`
	RequestID string         `json:"request_id"`
	Key       string         `json:"key"`
	Value     string         `json:"value"`
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
	Synthetic     *syntheticState   `json:"synthetic,omitempty"`
	SchemaVersion int               `json:"schema_version"`
	Values        map[string]string `json:"values"`
	Requests      map[string]string `json:"requests"`
	AppliedIndex  uint64            `json:"applied_index"`
}
type kvMachine struct {
	synthetic syntheticObservation
	mu        sync.RWMutex
	state     machineState
}

func newMachine() *kvMachine {
	return &kvMachine{state: machineState{SchemaVersion: 1, Values: map[string]string{}, Requests: map[string]string{}}}
}
func (m *kvMachine) Apply(ctx context.Context, cmd load.Command) (load.Result, error) {
	if err := ctx.Err(); err != nil {
		return load.Result{}, err
	}
	var put putCommand
	if err := decodeJSON(cmd.Data, &put); err != nil {
		return load.Result{Code: string(fault.Invalid)}, nil
	}
	if put.Synthetic != nil {
		return m.configureSynthetic(ctx, cmd, put)
	}
	if put.RequestID == "" || len(put.RequestID) > 128 || put.RequestID != cmd.RequestID || put.Key == "" || len(put.Key) > 256 || len(put.Value) > 1<<16 {
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
	if m.state.Synthetic != nil {
		m.state.Synthetic.Counter++
	}
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
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state.Synthetic == nil {
		return json.Marshal(m.state)
	}
	data, err := syntheticSnapshot(ctx, m.state)
	if err != nil {
		return nil, err
	}
	sum, err := hashSynthetic(ctx, data)
	if err != nil {
		return nil, err
	}
	m.synthetic.Spec = m.state.Synthetic.Spec
	m.synthetic.Counter = m.state.Synthetic.Counter
	m.synthetic.ApplicationBytes = uint64(len(data))
	m.synthetic.ApplicationSHA256 = hex.EncodeToString(sum[:])
	return data, nil
}

func (m *kvMachine) Restore(ctx context.Context, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if bytes.HasPrefix(data, []byte(syntheticMagic)) {
		state, err := decodeSynthetic(ctx, data)
		if err != nil {
			return err
		}
		sum, err := hashSynthetic(ctx, data)
		if err != nil {
			return err
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		m.state = state
		m.synthetic = syntheticObservation{Spec: state.Synthetic.Spec, Counter: state.Synthetic.Counter,
			ApplicationBytes: uint64(len(data)), ApplicationSHA256: hex.EncodeToString(sum[:]), Restores: m.synthetic.Restores + 1, AppliedIndex: state.AppliedIndex}
		return nil
	}
	state := machineState{SchemaVersion: 1, Values: map[string]string{}, Requests: map[string]string{}}
	if len(data) != 0 {
		if err := decodeJSON(data, &state); err != nil {
			return fault.New(fault.Unreadable, "invalid example snapshot", err)
		}
		if state.Synthetic != nil {
			return fault.New(fault.Unreadable, "synthetic snapshot lacks generated payload", nil)
		}
		if err := validateMachineState(state); err != nil {
			return err
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

func (m *kvMachine) configureSynthetic(_ context.Context, cmd load.Command, put putCommand) (load.Result, error) {
	if put.RequestID == "" || put.RequestID != cmd.RequestID || len(put.RequestID) > 128 || put.Synthetic.validate() != nil {
		return load.Result{Code: string(fault.Invalid)}, nil
	}
	sum := sha256.Sum256(cmd.Data)
	fingerprint := hex.EncodeToString(sum[:])
	m.mu.Lock()
	defer m.mu.Unlock()
	if old, found := m.state.Requests[put.RequestID]; found {
		if old != fingerprint {
			return load.Result{Code: string(fault.VersionConflict)}, nil
		}
		return load.Result{Code: "ok"}, nil
	}
	m.state.AppliedIndex = cmd.Index
	m.state.Requests[put.RequestID] = fingerprint
	m.state.Synthetic = newSyntheticState(*put.Synthetic)
	return load.Result{Code: "ok"}, nil
}
func (m *kvMachine) syntheticView() syntheticObservation {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := m.synthetic
	if m.state.Synthetic != nil {
		result.Spec = m.state.Synthetic.Spec
		result.Counter = m.state.Synthetic.Counter
	}
	result.AppliedIndex = m.state.AppliedIndex
	return result
}

func validateMachineState(state machineState) error {
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
			return fault.New(fault.Unreadable, "invalid request fingerprint", nil)
		}
	}
	return nil
}
