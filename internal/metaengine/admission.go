package metaengine

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

// AdmissionState describes the replicated progress of one exact node
// incarnation's admission.
type AdmissionState string

// Admission states track preparation, learner promotion and terminal admission
// outcomes.
const (
	AdmissionPending    AdmissionState = "pending"
	AdmissionReady      AdmissionState = "ready"
	AdmissionLearner    AdmissionState = "learner"
	AdmissionPromoted   AdmissionState = "promoted"
	AdmissionCompleted  AdmissionState = "completed"
	AdmissionSuperseded AdmissionState = "superseded"
	AdmissionRetired    AdmissionState = "retired"
)

// AdmissionRecord binds a process incarnation and member identity to its
// admission operator.
type AdmissionRecord struct {
	NodeName    string               `json:"node_name"`
	InstanceID  string               `json:"instance_id"`
	MemberID    uint64               `json:"member_id"`
	OperationID topology.OperationID `json:"execution_id"`
	State       AdmissionState       `json:"state"`
	// ReadmissionInstanceID retains the original retirement proof when a
	// pending admission changes process without changing its durable member or
	// operator.
	// Empty means this record is the original. Only the FSM may assign it.
	ReadmissionInstanceID string `json:"readmission_instance_id,omitempty"`
	// ReadmissionOperatorID independently preserves the original approved
	// retirement authority when the same member's admission changes owner.
	// Empty means OperationID is the origin. Only the FSM may assign it.
	ReadmissionOperatorID topology.OperationID `json:"readmission_operator_id,omitempty"`
}

func admissionKey(nodeName, instanceID string) string {
	return nodeName + "\x00" + instanceID
}

func (s AdmissionState) valid() bool {
	return s == AdmissionPending || s == AdmissionReady || s == AdmissionLearner || s == AdmissionPromoted ||
		s.terminal()
}

func (s AdmissionState) terminal() bool {
	return s == AdmissionCompleted || s == AdmissionSuperseded || s == AdmissionRetired
}

func (s AdmissionState) isPrepared() bool {
	switch s {
	case AdmissionReady,
		AdmissionLearner,
		AdmissionPromoted,
		AdmissionCompleted:
		return true
	default:
		return false
	}
}

func cloneAdmission(in AdmissionRecord) AdmissionRecord { return in }

func admissionInstanceReplacement(p BindAdmissionProposal, record AdmissionRecord,
	conflict *AdmissionRecord) (AdmissionRecord, *ApplyResult) {
	if conflict != nil {
		if !p.ReplaceInstance {
			result := rejectProposal(p.ProposalID, proposalAuthorization, "node %q is already admitted for instance %q",
				record.NodeName, conflict.InstanceID)
			return record, &result
		}
		if conflict.State != AdmissionPending {
			result := rejectProposal(p.ProposalID, proposalAuthorization,
				"instance %q admission is already %s; replace it through a fresh-ID repair", conflict.InstanceID,
				conflict.State)
			return record, &result
		}
		if conflict.MemberID == record.MemberID && conflict.OperationID == record.OperationID {
			record.ReadmissionOperatorID = conflict.ReadmissionOperatorID
			record.ReadmissionInstanceID = conflict.ReadmissionInstanceID
			if record.ReadmissionInstanceID == "" {
				record.ReadmissionInstanceID = conflict.InstanceID
			}
		}
	}
	return record, nil
}

func validateAdmissionIdentity(record AdmissionRecord) error {
	if strings.ContainsRune(record.NodeName, 0) || strings.ContainsRune(record.InstanceID, 0) ||
		strings.ContainsRune(record.ReadmissionInstanceID, 0) ||
		strings.ContainsRune(string(record.ReadmissionOperatorID), 0) {
		return errors.New("admission identity contains a separator")
	}
	if record.NodeName == "" || record.InstanceID == "" || record.MemberID == 0 {
		return errors.New("admission requires node, instance, and non-zero member identity")
	}
	if record.ReadmissionInstanceID == record.InstanceID {
		return errors.New("readmission origin must name a previous process")
	}
	if record.ReadmissionOperatorID != "" && record.ReadmissionOperatorID == topology.OperationID(record.OperationID) {
		return errors.New("readmission origin must name a previous operation")
	}
	return nil
}

func scopedLiveAdmission(state *scopedState, nodeName, exceptInstance string) *AdmissionRecord {
	for i := range state.Admissions {
		record := &state.Admissions[i]
		if record.NodeName == nodeName && record.InstanceID != exceptInstance && !record.State.terminal() {
			return record
		}
	}
	return nil
}

func hasAdmissionSequence(steps []topology.Step, desired topology.Document, nodeName string, memberID uint64) bool {
	if meta := desired.MetaGroup(); meta != nil {
		for _, r := range meta.Replicas {
			if r.NodeName == nodeName && r.ReplicaID == memberID {
				return topology.ValidAdmissionSequence(steps, nodeName, memberID, r.Role)
			}
		}
	}
	return false
}

func buildReadmissionDocument(base topology.Document, nodeName string, at time.Time) (topology.Document, error) {
	doc := base.Clone()
	node := doc.Node(nodeName)
	if node == nil || node.State != topology.NodeActive {
		return topology.Document{}, fmt.Errorf("node %q is not active", nodeName)
	}
	node.UpdatedAt = at
	for i := range doc.Groups {
		changed := false
		for j := range doc.Groups[i].Replicas {
			replica := &doc.Groups[i].Replicas[j]
			if replica.NodeName != nodeName {
				continue
			}
			id, err := doc.Groups[i].AllocateReplicaID()
			if err != nil {
				return topology.Document{}, err
			}
			replica.ReplicaID = id
			replica.UpdatedAt = at
			changed = true
		}
		if changed {
			doc.Groups[i].UpdatedAt = at
		}
	}
	doc.ParentVersion = base.Version
	doc.Version = base.Version + 1
	doc.ChangedBy = "join-reconciler"
	doc.Origin = topology.OriginReconciler
	return doc, nil
}

// Prepared reports whether an exact admitted runtime has acknowledged
// readiness.
func (s AdmissionState) Prepared() bool { return s.isPrepared() }
