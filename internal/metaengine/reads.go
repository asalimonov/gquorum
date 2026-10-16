package metaengine

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// QueryRequest is the private protocol between the meta service and its
// machine.
type QueryRequest struct {
	Progress           *topology.ProgressQuery `json:"progress,omitempty"`
	CollectionIdentity *storage.Identity       `json:"collection_identity,omitempty"`
	OperationID        topology.OperationID    `json:"operation_id,omitempty"`
	AuthorityKey       *topology.AuthorityKey  `json:"authority_key,omitempty"`
	OperatorID         topology.OperatorID     `json:"operator_id,omitempty"`
	Kind               string                  `json:"kind"`
	Key                string                  `json:"key,omitempty"`
}

// LockRead returns a lock-holder session and whether the lock is held.
type LockRead = topology.LockHolder

// PlanRead captures an operator, its topology boundaries and retained authority
// evidence.
type PlanRead struct {
	Operator topology.Operator         `json:"operator"`
	Input    topology.PlanInput        `json:"input"`
	Target   topology.Document         `json:"target"`
	Grants   []topology.AuthorityGrant `json:"grants,omitempty"`
	Receipts []topology.EffectReceipt  `json:"receipts,omitempty"`
}

// SupportsQuery reports that the metadata machine implements serialized engine
// queries.
func (*Machine) SupportsQuery() bool { return true }

// Query validates and executes the private metadata read protocol under the
// machine lock.
func (m *Machine) Query(ctx context.Context, data []byte) (raftengine.Result, error) {
	if err := ctx.Err(); err != nil {
		return raftengine.Result{}, err
	}
	var q QueryRequest
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&q); err != nil {
		return raftengine.Result{}, fault.ErrInvalid
	}
	if d.Decode(new(any)) != io.EOF {
		return raftengine.Result{}, fault.ErrInvalid
	}
	if err := q.validateScopedFields(); err != nil {
		return raftengine.Result{}, err
	}
	if q.AuthorityKey != nil && q.Kind != "authority_grant" && q.Kind != "authority_proof" {
		return raftengine.Result{}, fault.ErrInvalid
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, err := m.queryValueLocked(q)
	if err != nil {
		return raftengine.Result{}, err
	}
	// Marshal under the same lock, owning all returned bytes before releasing
	// it.
	encoded, err := json.Marshal(value)
	return raftengine.Result{Data: encoded}, err
}

// SessionValue validates the replicated identity and configured TTL. It does
// not derive remaining lifetime from a process-local expiry observation.
func SessionValue(s SessionRecord) (topology.Session, error) {
	if s.SessionID == "" || s.NodeName == "" || s.InstanceID == "" || s.TTLSeconds <= 0 ||
		int64(s.TTLSeconds) > math.MaxInt64/int64(time.Second) {
		return topology.Session{}, fault.New(fault.Unreadable, "invalid replicated session identity or TTL", nil)
	}
	return topology.Session{
		ID:         s.SessionID,
		NodeName:   s.NodeName,
		InstanceID: s.InstanceID,
		TTL:        time.Duration(s.TTLSeconds) * time.Second,
	}, nil
}

func (m *Machine) queryValueLocked(q QueryRequest) (any, error) {
	if q.OperationID != "" {
		return queryScopedAuthority(nil, q) // Scoped state replaces this adapter at Task 78.
	}
	switch q.Kind {
	case "plan_validation":
		return m.queryPlanLocked(q)
	case "topology":
		if q.Key != "" || q.OperatorID != "" {
			return nil, fault.ErrInvalid
		}
		return topology.ReadView{AppliedIndex: m.appliedIndex, Latest: m.latest, Target: m.target,
			Active: m.active}, nil
	case "lock_holder":
		return m.queryLockHolderLocked(q)
	case "authority":
		if q.Key != "" || q.OperatorID == "" {
			return nil, fault.ErrInvalid
		}
		return m.authorityStatusLocked(q.OperatorID)
	case "authority_floors":
		if q.Key == "" || q.OperatorID != "" {
			return nil, fault.ErrInvalid
		}
		return m.authorityFloorsLocked(q.Key), nil
	case "authority_proof":
		return m.queryAuthorityProofLocked(q)
	case "authority_grant":
		if q.Key != "" || q.OperatorID != "" || q.AuthorityKey == nil ||
			q.AuthorityKey.ClusterID != m.latest.ClusterID || q.AuthorityKey.Revision == 0 ||
			topology.ValidateID(q.AuthorityKey.GroupID) != nil {
			return nil, fault.ErrInvalid
		}
		var grant *topology.AuthorityGrant
		if record := m.authority.records[*q.AuthorityKey]; record != nil {
			grant = &record.Grant
		}
		return grant, nil
	default:
		return nil, fault.ErrUnsupported
	}
}

func (m *Machine) queryPlanLocked(q QueryRequest) (PlanRead, error) {
	if q.Key != "" || q.OperatorID == "" {
		return PlanRead{}, fault.ErrInvalid
	}
	op := m.operators[q.OperatorID]
	if op == nil || op.Version != m.target.Version || op.State.Terminal() {
		return PlanRead{}, fault.ErrOperatorAuthorization
	}
	for _, revocation := range m.authority.revocations {
		if !revokedComplete(revocation) {
			return PlanRead{}, fault.ErrAuthorityPending
		}
	}
	input, err := m.planningInputLocked(m.target, nil)
	if err != nil {
		return PlanRead{}, err
	}
	read := PlanRead{Operator: operatorRecordView(*op), Input: input, Target: m.target}
	if err := m.collectPlanEvidenceLocked(op, &read); err != nil {
		return PlanRead{}, err
	}
	slices.SortFunc(read.Grants, func(a, b topology.AuthorityGrant) int {
		return compareAuthorityKey(a.Key, b.Key)
	})
	slices.SortFunc(read.Receipts, func(a, b topology.EffectReceipt) int {
		if byKey := compareAuthorityKey(a.Key, b.Key); byKey != 0 {
			return byKey
		}
		if byResource := a.Resource.Compare(b.Resource); byResource != 0 {
			return byResource
		}
		return a.Target.Compare(b.Target)
	})
	return read, nil
}

func (m *Machine) queryLockHolderLocked(q QueryRequest) (LockRead, error) {
	if q.Key == "" || q.OperatorID != "" {
		return LockRead{}, fault.ErrInvalid
	}
	result := LockRead{}
	if id, held := m.locks[q.Key]; held {
		s, exists := m.sessions[id]
		if !exists || s == nil {
			return LockRead{}, fault.ErrUnreadable
		}
		var err error
		result.Session, err = SessionValue(*s)
		if err != nil {
			return LockRead{}, err
		}
		result.Held = true
	}
	return result, nil
}

func (m *Machine) queryAuthorityProofLocked(q QueryRequest) (AuthorityProof, error) {
	if q.OperatorID != "" || q.AuthorityKey == nil || q.AuthorityKey.ClusterID != m.latest.ClusterID ||
		q.AuthorityKey.Revision == 0 || topology.ValidateID(q.AuthorityKey.GroupID) != nil {
		return AuthorityProof{}, fault.ErrInvalid
	}
	proof := AuthorityProof{}
	if record := m.authority.records[*q.AuthorityKey]; record != nil {
		proof.Grant = &record.Grant
		proof.Receipts = record.Effects
		for _, target := range record.Grant.Targets {
			if m.hostReleasedLocked(record, target) {
				proof.ReleasedHosts = append(proof.ReleasedHosts, target)
			}
			if topology.ValidateLoadID(record.Grant.Key.GroupID) == nil &&
				m.grantedLoadCleanupLocked(record.Grant, target) == nil {
				proof.CleanupTargets = append(proof.CleanupTargets, target)
			}
		}
	}
	if q.Key != "" {
		proof.Courier, _ = m.processIdentityLocked(q.Key)
	}
	return proof, nil
}

func (m *Machine) collectPlanEvidenceLocked(op *operatorRecord, read *PlanRead) error {
	for _, record := range m.authority.records {
		grant := record.Grant
		if grant.Purpose != topology.AuthorityEffect || grant.Action == nil || grant.OperatorID != op.ID {
			continue
		}
		seq := grant.Action.StepSeq
		if seq < 1 || seq > len(op.Steps) || !grant.Action.Matches(op.ID, op.Attempt, op.Version, op.Steps[seq-1]) {
			return fault.ErrUnreadable
		}
		read.Grants = append(read.Grants, grant.Clone())
		for _, receipt := range record.Effects {
			read.Receipts = append(read.Receipts, receipt.Clone())
		}
	}
	return nil
}
