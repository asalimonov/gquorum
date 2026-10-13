package metaengine

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/topology"
)

// QueryRequest is the private protocol between the meta service and its machine.
type QueryRequest struct {
	AuthorityKey *topology.AuthorityKey `json:"authority_key,omitempty"`
	OperatorID   topology.OperatorID    `json:"operator_id,omitempty"`
	Kind         string                 `json:"kind"`
	Key          string                 `json:"key,omitempty"`
}
type LockRead struct {
	Session topology.Session `json:"session"`
	Held    bool             `json:"held"`
}

type PlanRead struct {
	Operator topology.Operator `json:"operator"`
	Base     topology.Document `json:"base"`
	Target   topology.Document `json:"target"`
}

func (*Machine) SupportsQuery() bool { return true }
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
	if q.AuthorityKey != nil && q.Kind != "authority_grant" && q.Kind != "authority_proof" {
		return raftengine.Result{}, fault.ErrInvalid
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var value any
	switch q.Kind {
	case "plan_validation":
		if q.Key != "" || q.OperatorID == "" {
			return raftengine.Result{}, fault.ErrInvalid
		}
		op := m.operators[q.OperatorID]
		if op == nil || op.State.Terminal() || op.Version != m.target.Version {
			return raftengine.Result{}, fault.ErrOperatorAuthorization
		}
		for _, revocation := range m.authority.revocations {
			if !revokedComplete(revocation) {
				return raftengine.Result{}, fault.ErrAuthorityPending
			}
		}
		base, err := m.operatorBaseLocked(op)
		if err != nil {
			return raftengine.Result{}, err
		}
		value = PlanRead{Operator: operatorRecordView(*op), Base: base, Target: m.target}
	case "topology":
		if q.Key != "" || q.OperatorID != "" {
			return raftengine.Result{}, fault.ErrInvalid
		}
		value = topology.ReadView{AppliedIndex: m.appliedIndex, Latest: m.latest, Target: m.target, Active: m.active}
	case "lock_holder":
		if q.Key == "" || q.OperatorID != "" {
			return raftengine.Result{}, fault.ErrInvalid
		}
		result := LockRead{}
		if id, held := m.locks[q.Key]; held {
			s, exists := m.sessions[id]
			if !exists || s == nil {
				return raftengine.Result{}, fault.ErrUnreadable
			}
			var err error
			result.Session, err = SessionValue(*s)
			if err != nil {
				return raftengine.Result{}, err
			}
			result.Held = true
		}
		value = result
	case "authority":
		if q.Key != "" || q.OperatorID == "" {
			return raftengine.Result{}, fault.ErrInvalid
		}
		var err error
		value, err = m.authorityStatusLocked(q.OperatorID)
		if err != nil {
			return raftengine.Result{}, err
		}
	case "authority_floors":
		if q.Key == "" || q.OperatorID != "" {
			return raftengine.Result{}, fault.ErrInvalid
		}
		value = m.authorityFloorsLocked(q.Key)
	case "authority_proof":
		if q.OperatorID != "" || q.AuthorityKey == nil || q.AuthorityKey.ClusterID != m.latest.ClusterID || q.AuthorityKey.Revision == 0 || topology.ValidateID(q.AuthorityKey.GroupID) != nil {
			return raftengine.Result{}, fault.ErrInvalid
		}
		proof := AuthorityProof{}
		if record := m.authority.records[*q.AuthorityKey]; record != nil {
			proof.Grant = &record.Grant
			proof.Receipts = record.Effects
			for _, target := range record.Grant.Targets {
				if m.hostReleasedLocked(record, target) {
					proof.ReleasedHosts = append(proof.ReleasedHosts, target)
				}
				if topology.ValidateLoadID(record.Grant.Key.GroupID) == nil && m.grantedLoadCleanupLocked(record.Grant, target) == nil {
					proof.CleanupTargets = append(proof.CleanupTargets, target)
				}
			}
		}
		if q.Key != "" {
			proof.Courier, _ = m.processIdentityLocked(q.Key)
		}
		value = proof
	case "authority_grant":
		if q.Key != "" || q.OperatorID != "" || q.AuthorityKey == nil || q.AuthorityKey.ClusterID != m.latest.ClusterID || q.AuthorityKey.Revision == 0 || topology.ValidateID(q.AuthorityKey.GroupID) != nil {
			return raftengine.Result{}, fault.ErrInvalid
		}
		var grant *topology.AuthorityGrant
		if record := m.authority.records[*q.AuthorityKey]; record != nil {
			grant = &record.Grant
		}
		value = grant
	default:
		return raftengine.Result{}, fault.ErrUnsupported
	}
	// Marshal under the same lock, owning all returned bytes before releasing it.
	encoded, err := json.Marshal(value)
	return raftengine.Result{Data: encoded}, err
}

// SessionValue validates the replicated identity and configured TTL. It does
// not derive remaining lifetime from a process-local expiry observation.
func SessionValue(s SessionRecord) (topology.Session, error) {
	if s.SessionID == "" || s.NodeName == "" || s.InstanceID == "" || s.TTLSeconds <= 0 || int64(s.TTLSeconds) > math.MaxInt64/int64(time.Second) {
		return topology.Session{}, fault.New(fault.Unreadable, "invalid replicated session identity or TTL", nil)
	}
	return topology.Session{ID: s.SessionID, NodeName: s.NodeName, InstanceID: s.InstanceID, TTL: time.Duration(s.TTLSeconds) * time.Second}, nil
}
