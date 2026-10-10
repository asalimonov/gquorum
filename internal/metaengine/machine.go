package metaengine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

// Machine adapts the deterministic topology FSM to the shared Raft loop.
type Machine struct{ *topologySM }

func NewMachine(rules topology.Rules, persister statePersister) *Machine {
	return &Machine{newTopologySM(persister, rules)}
}
func (m *Machine) Apply(ctx context.Context, c raftengine.Command) (raftengine.Result, error) {
	if err := ctx.Err(); err != nil {
		return raftengine.Result{}, err
	}
	result, err := m.topologySM.Apply(c.Index, c.Term, c.Data)
	if err != nil {
		return raftengine.Result{}, err
	}
	data, err := json.Marshal(result)
	return raftengine.Result{Code: string(result.Code), Data: data}, err
}
func (m *Machine) Snapshot(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return m.topologySM.Snapshot()
}
func (m *Machine) Restore(ctx context.Context, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(data) == 0 {
		fresh := newTopologySM(nil, m.rules)
		var err error
		data, err = fresh.Snapshot()
		if err != nil {
			return err
		}
	}
	return m.topologySM.Restore(bytes.Clone(data))
}

var _ raftengine.Machine = (*Machine)(nil)

func (m *Machine) AfterApply(ctx context.Context, index, term uint64, cs raftpb.ConfState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.appliedIndex, m.appliedTerm = index, term
	data, err := m.marshalLocked()
	if err != nil {
		return err
	}
	if store, ok := m.persister.(interface {
		SaveApplication(uint64, raftpb.ConfState, []byte) error
	}); ok {
		if err := store.SaveApplication(index, cs, data); err != nil {
			return fmt.Errorf("%w: %w", errDurablePersist, err)
		}
	}
	return nil
}
