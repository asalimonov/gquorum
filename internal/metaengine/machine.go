package metaengine

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

// Machine adapts the deterministic topology FSM to the shared Raft loop.
type Machine struct {
	mu        sync.RWMutex
	persister statePersister
	rules     topology.Rules
	scoped    *scopedState
	// OnApplied is installed before the engine is constructed. It runs after
	// durable application persistence, outside the FSM lock, and must be
	// bounded.
	OnApplied         func(uint64, raftpb.ConfState)
	OnRecoveryCommand func(ProposalKind, error) // instance-owned diagnostics, installed before engine startup
}

// NewMachine validates replicated rules and constructs an empty schema-4
// topology machine without I/O or application callbacks.
func NewMachine(rules topology.Rules, persister statePersister) (*Machine, error) {
	if rules.SchemaVersion != 1 {
		return nil, fault.New(fault.Invalid, "unsupported cluster rules schema", nil)
	}
	if err := rules.Limits.Validate(); err != nil {
		return nil, fault.New(fault.Invalid, "invalid cluster limits", err)
	}
	state := scopedState{SchemaVersion: scopedStateSchemaVersion, Rules: rules}
	indexes, err := prepareScopedGraph(&state, rules)
	if err != nil {
		return nil, err
	}
	state.indexes = indexes
	return &Machine{rules: rules, persister: persister, scoped: &state}, nil
}

// Apply applies a committed metadata command and encodes its deterministic
// result.
func (m *Machine) Apply(ctx context.Context, c raftengine.Command) (raftengine.Result, error) {
	if err := ctx.Err(); err != nil {
		return raftengine.Result{}, err
	}
	trace := m.recoveryCommandTrace(c.Data)
	result, err := m.applyScoped(c.Index, c.Term, c.Data, c.ConfState)
	if err != nil {
		return raftengine.Result{}, err
	}
	if trace != nil {
		trace()
	}
	data, err := json.Marshal(result)
	return raftengine.Result{Code: string(result.Code), Data: data}, err
}

func (m *Machine) applyScoped(index, term uint64, data []byte, conf raftpb.ConfState) (ApplyResult, error) {
	decoded, err := decodeEnvelope(data)
	if err != nil {
		return ApplyResult{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.scoped
	if state == nil {
		return ApplyResult{}, fault.ErrUnreadable
	}
	if index <= state.AppliedIndex {
		return ApplyResult{}, nil
	}
	// Handlers must prepare owned candidates before changing maps or slices.
	// This shallow checkpoint restores scalar apply context on refusal; it is
	// deliberately not a rollback mechanism for in-place payload mutation.
	previous := *state
	state.nativeConf = &conf
	state.AppliedTerm = term
	application, err := applyScopedProposal(state, index, decoded.Kind, decoded.Body)
	if err != nil {
		*state = previous
		application.result, err = scopedProposalRefusal(decoded.Body, err)
		if err != nil {
			return ApplyResult{}, err
		}
	}
	state.nativeConf = nil
	if state.AppliedIndex < index {
		state.AppliedIndex = index
	}
	state.AppliedTerm = term
	if application.result.CurrentVersion == 0 {
		application.result.CurrentVersion = state.Latest.Version
	}
	if m.persister != nil {
		// TODO(not planned): incremental persistence. The 10k-debt benchmark
		// measures full-state cost; never discard unresolved proof to cap it.
		encoded, err := json.Marshal(state)
		if err != nil {
			return ApplyResult{}, err
		}
		if err := m.persister.SaveState(encoded); err != nil {
			return ApplyResult{}, fmt.Errorf("%w: %w", errDurablePersist, err)
		}
	}
	return application.result, nil
}

// Snapshot returns the complete replicated topology machine state.
func (m *Machine) Snapshot(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.scoped == nil {
		return nil, fault.ErrUnreadable
	}
	return json.Marshal(m.scoped)
}

// Restore validates and owns a complete schema-4 checkpoint before replacing
// live state. Empty input initializes an empty current-format machine.
func (m *Machine) Restore(ctx context.Context, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	next := scopedState{SchemaVersion: scopedStateSchemaVersion, Rules: m.rules}
	if len(data) != 0 {
		var header struct {
			SchemaVersion uint32 `json:"schema_version"`
		}
		if err := json.Unmarshal(data, &header); err != nil {
			return fault.New(fault.Unreadable, "invalid metadata snapshot", err)
		}
		if header.SchemaVersion != scopedStateSchemaVersion {
			// TODO(not planned): negotiated migrations and coordinated disaster
			// restore. Reject old evidence intact rather than interpreting it.
			return fault.ErrUnsupported
		}
		if err := json.Unmarshal(data, &next); err != nil {
			return err
		}
		if !reflect.DeepEqual(next.Rules, m.rules) {
			return fault.ErrIdentityMismatch
		}
	} else {
		var err error
		next.indexes, err = prepareScopedGraph(&next, m.rules)
		if err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.scoped = &next
	return nil
}

var _ raftengine.Machine = (*Machine)(nil)

// SessionRecord exposes the machine's retained session state to private service
// composition.
type SessionRecord = sessionState

// Session returns the retained record for a session ID.
func (m *Machine) Session(id string) (SessionRecord, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sessionLocked(id)
}

func (m *Machine) sessionLocked(id string) (SessionRecord, bool) {
	for _, record := range m.scoped.Sessions {
		if record.SessionID == id {
			return record, true
		}
	}
	return SessionRecord{}, false
}

// LockSession returns the retained session associated with a lock holder, if
// present.
func (m *Machine) LockSession(key string) (SessionRecord, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, lock := range m.scoped.Locks {
		if lock.Key == key {
			return m.sessionLocked(lock.SessionID)
		}
	}
	return SessionRecord{}, false
}

type applicationPersister interface {
	SaveApplication(uint64, raftpb.ConfState, []byte) error
}

// AfterApply persists materialized state with its applied boundary before
// notifying the service.
func (m *Machine) AfterApply(ctx context.Context, index, term uint64, cs raftpb.ConfState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	if m.scoped == nil {
		m.mu.Unlock()
		return fault.ErrUnreadable
	}
	m.scoped.AppliedIndex, m.scoped.AppliedTerm = index, term
	data, err := json.Marshal(m.scoped)
	if err != nil {
		m.mu.Unlock()
		return err
	}
	if store, ok := m.persister.(applicationPersister); ok {
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
func (m *Machine) Views() topology.ReadView {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return topology.ReadView{AppliedIndex: m.scoped.AppliedIndex, Latest: m.scoped.Latest.Clone(),
		Target: m.scoped.Target.Clone(), FullyConverged: m.scoped.FullyConverged}
}

// FullyConvergedDocument returns the retained historical document named by
// the scoped completion marker. It is suitable only for algorithms that need
// a complete pinned baseline; it is not a current topology view.
func (m *Machine) FullyConvergedDocument() (topology.Document, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	version := m.scoped.FullyConverged.Version
	if version == 0 {
		return topology.Document{}, fault.ErrUnavailable
	}
	doc, ok := m.scoped.indexes.documents[version]
	if !ok {
		return topology.Document{}, fault.ErrHistoryUnavailable
	}
	return doc.Clone(), nil
}

// AdmissionForOperator finds the exact admission bound to an operator, node and
// member.
func (m *Machine) AdmissionForOperator(id topology.OperationID, name string,
	member uint64) (AdmissionRecord, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, record := range m.scoped.Admissions {
		if record.OperationID == id && record.NodeName == name && record.MemberID == member &&
			record.State != AdmissionRetired && record.State != AdmissionSuperseded {
			return record, true
		}
	}
	return AdmissionRecord{}, false
}

// LiveAdmission finds a nonterminal admission for a node, excluding the
// supplied incarnation.
func (m *Machine) LiveAdmission(name, except string) (AdmissionRecord, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if record := scopedLiveAdmission(m.scoped, name, except); record != nil {
		return *record, true
	}
	return AdmissionRecord{}, false
}

// ReadmissionDocument builds an owned replacement topology declaration for a
// named node.
func ReadmissionDocument(base topology.Document, name string, at time.Time) (topology.Document, error) {
	return buildReadmissionDocument(base, name, at)
}

// MemberOwner reads the approved routing identities without copying load-group
// documents. Founding manifests remain authoritative until Target is installed.
func (m *Machine) MemberOwner(id uint64) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	owners, err := scopedMetaMemberOwners(m.scoped)
	if err != nil {
		return "", false
	}
	return owners[id], m.scoped.Target.Version > 0
}

// MemberOwners returns an owned approved meta-routing map and whether Target is
// established.
func (m *Machine) MemberOwners() (map[uint64]string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	owners, err := scopedMetaMemberOwners(m.scoped)
	return owners, err == nil && m.scoped.Target.Version > 0
}

// EligibleMemberID resolves the member allowed to renew a node session at the
// current approval boundary. A pending removal remains eligible while its
// proven metadata membership exists; a replacement in Target fences the old
// member before that group converges.
func (m *Machine) EligibleMemberID(name string) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return scopedMemberID(m.scoped, name)
}

// ValidateDesired performs the same historical-identity preflight as a
// committed
// document. CAS and validation are repeated by the replicated proposal.
func (m *Machine) ValidateDesired(doc topology.Document) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if doc.Version != m.scoped.Latest.Version+1 {
		var expected uint64
		if doc.Version > 0 {
			expected = doc.Version - 1
		}
		return &fault.Error{Code: fault.VersionConflict, ExpectedVersion: expected,
			CurrentVersion: m.scoped.Latest.Version}
	}
	return m.scoped.validateStagedDocument(doc)
}

// AllocationFloors returns owned historical counters, including removed groups.
func (m *Machine) AllocationFloors() map[string]uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	floors := make(map[string]uint64, len(m.scoped.Allocations))
	for _, row := range m.scoped.Allocations {
		floors[row.GroupID] = row.NextReplicaID
	}
	return floors
}
