package metaengine

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

type completionScope uint8

const (
	openCompletion completionScope = iota
	retainedCompletion
)

func (catalog authorityCatalog) planStepConsumed(status topology.OperationStatus,
	step topology.Step, scope completionScope) bool {
	record := catalog.planStepAuthority(status, step)
	if record == nil || scope == openCompletion && record.Closed {
		return false
	}
	for _, resource := range record.Grant.Resources {
		if !resourceConsumed(record, resource) {
			return false
		}
	}
	return true
}

func (catalog authorityCatalog) planStepAuthority(status topology.OperationStatus,
	step topology.Step) *authorityRecord {
	action := topology.AuthorizedStep{
		OperationID:          topology.OperationID(status.ID),
		Version:              status.Version,
		Attempt:              status.Attempt,
		StepSeq:              step.Seq,
		Kind:                 step.Kind,
		GroupID:              step.GroupID,
		NodeName:             step.NodeName,
		ReplicaID:            step.ReplicaID,
		TransferNodeName:     step.TransferNodeName,
		TransferReplicaID:    step.TransferReplicaID,
		ExpectedVotersDigest: step.ExpectedVotersDigest,
		ResultVotersDigest:   step.ResultVotersDigest,
	}
	return catalog.records[catalog.actions[action]]
}

func validStepKind(kind topology.StepKind) bool {
	switch kind {
	case topology.StepCreateFoundingGroup, topology.StepPrepareReplica,
		topology.StepAddLearner, topology.StepWaitCaughtUp,
		topology.StepPromoteVoter, topology.StepDemoteVoter,
		topology.StepTransferLeader, topology.StepRemoveReplica,
		topology.StepStopReplica, topology.StepCleanupReplicaState,
		topology.StepDeleteGroupState:
		return true
	default:
		return false
	}
}

func validateRepairDocument(active, repair topology.Document) error {
	if active.ClusterID != repair.ClusterID || !reflect.DeepEqual(active.Placement, repair.Placement) ||
		!reflect.DeepEqual(active.Audit, repair.Audit) {
		return errors.New("repair changes cluster placement or forced audit")
	}
	if len(active.Nodes) != len(repair.Nodes) || len(active.Groups) != len(repair.Groups) {
		return errors.New("repair changes node or group cardinality")
	}
	for i := range active.Nodes {
		a, b := active.Nodes[i], repair.Nodes[i]
		a.UpdatedAt, b.UpdatedAt = time.Time{}, time.Time{}
		if !reflect.DeepEqual(a, b) {
			return fmt.Errorf("repair changes node %q beyond an update timestamp", active.Nodes[i].Name)
		}
	}
	for i := range active.Groups {
		if err := validateRepairGroup(active.Groups[i], repair.Groups[i]); err != nil {
			return err
		}
	}
	return nil
}

// validateSingleReplicaRepair re-derives the sole document edit permitted by
// the atomic repair command. Readmission has a separate validator because it
// replaces the admitted node's meta and load identities together.
func validateSingleReplicaRepair(active, repair topology.Document) error {
	if err := validateRepairDocument(active, repair); err != nil {
		return err
	}
	if repair.ParentVersion != active.Version {
		return errors.New("repair parent is not Active")
	}
	expected := active.Clone()
	expected.Version, expected.ParentVersion = repair.Version, repair.ParentVersion
	expected.Origin, expected.ChangedBy = repair.Origin, repair.ChangedBy
	// Explicit operator mutations may stamp their document audit comment/time.
	// Automatic repair preserves these fields from Active.
	if repair.Origin == topology.OriginOperator {
		expected.Comment, expected.CreatedAt = repair.Comment, repair.CreatedAt
	}
	changed := 0
	for i, group := range active.Groups {
		for j, old := range group.Replicas {
			next := repair.Groups[i].Replicas[j]
			if old.ReplicaID == next.ReplicaID {
				continue
			}
			node := active.Node(old.NodeName)
			if group.ID == topology.MetaGroupID || old.Role != topology.ReplicaVoter || node == nil ||
				node.State != topology.NodeActive || next.ReplicaID != group.NextReplicaID ||
				next.ReplicaID == ^uint64(0) {
				return errors.New("repair must replace one exact Active load voter with its allocator identity")
			}
			changed++
			expected.Groups[i].Replicas[j].ReplicaID = next.ReplicaID
			expected.Groups[i].Replicas[j].UpdatedAt = next.UpdatedAt
			expected.Groups[i].NextReplicaID = next.ReplicaID + 1
			expected.Groups[i].UpdatedAt = repair.Groups[i].UpdatedAt
			expected.Node(old.NodeName).UpdatedAt = repair.Node(old.NodeName).UpdatedAt
		}
	}
	if changed != 1 || !reflect.DeepEqual(expected, repair) {
		return errors.New("repair changes fields outside one exact Active load voter")
	}
	return nil
}

func documentReplica(doc topology.Document, groupID string, replicaID uint64) *topology.Replica {
	group := doc.Group(groupID)
	if group == nil {
		return nil
	}
	for i := range group.Replicas {
		if group.Replicas[i].ReplicaID == replicaID {
			return &group.Replicas[i]
		}
	}
	return nil
}

func rejectVersionConflict(id string, expected, current uint64) ApplyResult {
	result := rejectProposal(id, proposalVersionConflict, "expected Latest %d, current Latest %d", expected, current)
	result.ExpectedVersion, result.CurrentVersion = expected, current
	return result
}

func rejectProposal(id string, code proposalErrorCode, format string, args ...any) ApplyResult {
	return ApplyResult{ProposalID: id, Accepted: false, Code: code, Reason: fmt.Sprintf(format, args...)}
}

func rejectProposalError(id string, code proposalErrorCode, err error) ApplyResult {
	result := rejectProposal(id, code, errorFormat, err)
	var placement *topology.PlacementError
	if errors.As(err, &placement) {
		report := placement.Report.Clone()
		result.Placement = &report
	}
	return result
}

func validatePlanStepPosition(step topology.Step, i int) error {
	if step.Seq != i+1 {
		return fmt.Errorf("plan steps must be contiguous from 1: position %d has seq %d", i+1, step.Seq)
	}
	if step.GroupID == "" {
		return fmt.Errorf("plan step %d has no group", step.Seq)
	}
	if !validStepKind(step.Kind) {
		return fmt.Errorf("plan step %d has unknown kind %q", step.Seq, step.Kind)
	}
	return nil
}

func validatePlanStepScope(step topology.Step) error {
	wholeGroup := step.Kind == topology.StepCreateFoundingGroup || step.Kind == topology.StepDeleteGroupState
	if step.TransferNodeName != "" || step.TransferReplicaID != 0 {
		if step.Kind != topology.StepDemoteVoter || step.TransferNodeName == "" || step.TransferReplicaID == 0 ||
			step.TransferReplicaID == step.ReplicaID {
			return fmt.Errorf("step %d has invalid demotion transfer identity", step.Seq)
		}
	}
	if wholeGroup && (step.NodeName != "" || step.ReplicaID != 0) {
		return fmt.Errorf("whole-group step %d carries replica identity", step.Seq)
	}
	return nil
}

func validateRepairGroup(a, b topology.Group) error {
	if a.ID != b.ID || a.Storage != b.Storage || len(a.Replicas) != len(b.Replicas) ||
		b.NextReplicaID < a.NextReplicaID || !reflect.DeepEqual(a.Placement, b.Placement) ||
		!reflect.DeepEqual(a.VoterConfig, b.VoterConfig) || !reflect.DeepEqual(a.Hosting, b.Hosting) ||
		!a.CreatedAt.Equal(b.CreatedAt) {
		return fmt.Errorf("repair changes group %q outside identity repair", a.ID)
	}
	for j := range a.Replicas {
		ra, rb := a.Replicas[j], b.Replicas[j]
		if ra.NodeName != rb.NodeName || ra.Role != rb.Role || ra.PinnedLearner != rb.PinnedLearner ||
			!ra.CreatedAt.Equal(rb.CreatedAt) {
			return fmt.Errorf("repair changes replica placement or role in group %q", a.ID)
		}
		if ra.ReplicaID != rb.ReplicaID && rb.ReplicaID < a.NextReplicaID {
			return fmt.Errorf("repair reuses replica ID %d in group %q", rb.ReplicaID, a.ID)
		}
		if rb.ReplicaID >= b.NextReplicaID {
			return fmt.Errorf("repair replica ID %d exceeds allocator in group %q", rb.ReplicaID, a.ID)
		}
	}
	return nil
}
