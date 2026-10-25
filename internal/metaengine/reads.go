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
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// QueryRequest is the private protocol between the meta service and its
// machine.
type QueryRequest struct {
	Desired            *topology.Document              `json:"desired,omitempty"`
	Native             map[string]topology.NativeState `json:"native,omitempty"`
	Version            uint64                          `json:"version,omitempty"`
	Progress           *topology.ProgressQuery         `json:"progress,omitempty"`
	CollectionIdentity *storage.Identity               `json:"collection_identity,omitempty"`
	OperationID        topology.OperationID            `json:"operation_id,omitempty"`
	AuthorityKey       *topology.AuthorityKey          `json:"authority_key,omitempty"`
	Kind               string                          `json:"kind"`
	Key                string                          `json:"key,omitempty"`
}

// LockRead returns a lock-holder session and whether the lock is held.
type LockRead = topology.LockHolder

// PlanRead captures an operation, its topology boundaries and retained authority
// evidence.
type PlanRead struct {
	RecoveryFoundingSeeds []string                  `json:"recovery_founding_seeds,omitempty"`
	Recovery              *model.ImportState        `json:"recovery,omitempty"`
	Latest                topology.Document         `json:"latest"`
	Operation             topology.Operation        `json:"operation"`
	Previous              []topology.Operation      `json:"previous,omitempty"`
	Scopes                []ScopeExpectation        `json:"scopes"`
	Consumed              []int                     `json:"consumed,omitempty"`
	Accepted              topology.OperationID      `json:"accepted,omitempty"`
	Input                 topology.PlanInput        `json:"input"`
	Target                topology.Document         `json:"target"`
	Grants                []topology.AuthorityGrant `json:"grants,omitempty"`
	Receipts              []topology.EffectReceipt  `json:"receipts,omitempty"`
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
	if m.scoped == nil {
		return nil, fault.ErrUnreadable
	}
	if scopedPlanQuery(q.Kind) {
		return queryScopedPlan(m.scoped, q)
	}
	if q.OperationID != "" {
		return queryScopedAuthority(m.scoped, q)
	}
	switch q.Kind {
	case "recovery":
		if q.Key != "" || q.AuthorityKey != nil || q.CollectionIdentity != nil || q.Progress != nil {
			return nil, fault.ErrInvalid
		}
		return m.scoped.recoveryPlacementState(), nil
	case "progress":
		return queryScopedProgress(m.scoped, *q.Progress)
	case "collection_check":
		return queryScopedCollection(m.scoped, q)
	case "topology":
		if q.Key != "" || q.OperationID != "" {
			return nil, fault.ErrInvalid
		}
		return queryScopedTopology(m.scoped)
	case "lock_holder":
		return m.queryLockHolderLocked(q)
	case "authority":
		return queryScopedAuthority(m.scoped, q)
	case "authority_grant":
		return m.queryAuthorityGrantLocked(q)
	case "authority_proof":
		return queryScopedAuthorityProof(m.scoped, q)
	case "authority_floors":
		if q.Key == "" || q.OperationID != "" || q.AuthorityKey != nil {
			return nil, fault.ErrInvalid
		}
		return scopedAuthorityFloorsForNode(m.scoped, q.Key), nil
	default:
		return nil, fault.ErrUnsupported
	}
}

func (m *Machine) queryAuthorityGrantLocked(q QueryRequest) (*topology.AuthorityGrant, error) {
	if q.Key != "" || q.OperationID != "" || q.AuthorityKey == nil ||
		q.AuthorityKey.ClusterID != m.scoped.Target.ClusterID || q.AuthorityKey.Revision == 0 ||
		topology.ValidateID(q.AuthorityKey.GroupID) != nil {
		return nil, fault.ErrInvalid
	}
	var grant *topology.AuthorityGrant
	if record := m.scoped.indexes.grants[*q.AuthorityKey]; record != nil {
		grant = &record.Grant
	}
	return grant, nil
}

func (m *Machine) queryLockHolderLocked(q QueryRequest) (LockRead, error) {
	if q.Key == "" || q.OperationID != "" {
		return LockRead{}, fault.ErrInvalid
	}
	result := LockRead{}
	for _, lock := range m.scoped.Locks {
		if lock.Key != q.Key {
			continue
		}
		for _, session := range m.scoped.Sessions {
			if session.SessionID != lock.SessionID {
				continue
			}
			var err error
			result.Session, err = SessionValue(session)
			if err != nil {
				return LockRead{}, err
			}
			result.Held = true
			return result, nil
		}
		return LockRead{}, fault.ErrUnreadable
	}
	return result, nil
}
