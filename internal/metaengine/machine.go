package metaengine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

// Machine adapts the deterministic topology FSM to the shared Raft loop.
type Machine struct {
	*topologySM
	// OnApplied is installed before the engine is constructed. It runs after
	// durable application persistence, outside the FSM lock, and must be bounded.
	OnApplied func(uint64, raftpb.ConfState)
}

func NewMachine(rules topology.Rules, persister statePersister) *Machine {
	return &Machine{topologySM: newTopologySM(persister, rules)}
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

type SessionRecord = sessionState

func (m *Machine) Session(id string) (SessionRecord, bool) { return m.topologySM.session(id) }
func (m *Machine) LockSession(key string) (SessionRecord, bool) {
	_, s, ok := m.topologySM.lockHolderSession(key)
	return s, ok && s.SessionID != ""
}

func (m *Machine) AfterApply(ctx context.Context, index, term uint64, cs raftpb.ConfState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	m.appliedIndex, m.appliedTerm = index, term
	data, err := m.marshalLocked()
	if err != nil {
		m.mu.Unlock()
		return err
	}
	if store, ok := m.persister.(interface {
		SaveApplication(uint64, raftpb.ConfState, []byte) error
	}); ok {
		if err := store.SaveApplication(index, cs, data); err != nil {
			m.mu.Unlock()
			return fmt.Errorf("%w: %w", errDurablePersist, err)
		}
	}
	m.mu.Unlock()
	if m.OnApplied != nil {
		m.OnApplied(index, cs)
	}
	return nil
}

// Views captures the locally applied documents at one FSM boundary.
func (m *Machine) Views() (topology.Document, topology.Document, topology.Document) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.latest.Clone(), m.target.Clone(), m.active.Clone()
}

func (m *Machine) AdmissionForOperator(id topology.OperatorID, name string, member uint64) (AdmissionRecord, bool) {
	return m.admissionForOperator(id, name, member)
}

func (m *Machine) LiveAdmission(name, except string) (AdmissionRecord, bool) {
	return m.liveAdmissionForNode(name, except)
}
func ReadmissionDocument(base topology.Document, name string, at time.Time) (topology.Document, error) {
	return buildReadmissionDocument(base, name, at)
}

// MemberOwner reads the approved routing identities without copying load-group
// documents. Founding manifests remain authoritative until Target is installed.
func (m *Machine) MemberOwner(id uint64) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.metaOwners == nil {
		return m.metaMemberOwnersLocked()[id], m.target.Version > 0
	}
	return m.metaOwners[id], m.target.Version > 0
}
func (m *Machine) MemberOwners() (map[uint64]string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.metaOwners == nil {
		return m.metaMemberOwnersLocked(), m.target.Version > 0
	}
	return maps.Clone(m.metaOwners), m.target.Version > 0
}
func (m *Machine) ActiveMemberID(name string) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.active.MemberID(name)
}
func (m *Machine) ActiveVersion() uint64 { m.mu.RLock(); defer m.mu.RUnlock(); return m.active.Version }

// ValidateDesired performs the same historical-identity preflight as a committed
// document. CAS and validation are repeated by the replicated proposal.
func (m *Machine) ValidateDesired(doc topology.Document) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if doc.Version != m.latest.Version+1 {
		var expected uint64
		if doc.Version > 0 {
			expected = doc.Version - 1
		}
		return &fault.Error{Code: fault.VersionConflict, ExpectedVersion: expected, CurrentVersion: m.latest.Version}
	}
	return m.validateDocument(doc)
}

// AllocationFloors returns owned historical counters, including removed groups.
func (m *Machine) AllocationFloors() map[string]uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	floors := make(map[string]uint64, len(m.allocations))
	for id, next := range m.allocations {
		floors[id] = next
	}
	return floors
}
