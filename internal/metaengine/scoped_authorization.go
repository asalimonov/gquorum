package metaengine

import (
	"context"
	"reflect"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func (s *scopedState) authorizeLocked(ctx context.Context, action topology.AuthorizedStep) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if action.StepSeq == 0 {
		return s.authorizeDirectTransfer(action)
	}
	op := s.indexes.operations[topology.OperationID(action.OperatorID)]
	if op == nil || action.StepSeq < 1 || action.StepSeq > len(op.Plan.Steps) ||
		!action.Matches(topology.OperatorID(op.Status.ID), op.Status.Attempt,
			op.Status.Version, op.Plan.Steps[action.StepSeq-1]) {
		return fault.ErrOperationAuthorization
	}
	position := slices.IndexFunc(op.Plan.Blocks, func(block topology.PlanBlock) bool {
		return block.GroupID == action.GroupID
	})
	if position < 0 {
		return fault.ErrOperationAuthorization
	}
	definition, block := op.Plan.Blocks[position], op.Status.Blocks[position]
	if slices.Contains(definition.Cleanup, action.StepSeq) {
		return s.authorizeCleanupAction(action)
	}
	row := s.indexes.groups[action.GroupID]
	if row == nil || row.Owner != op.Status.ID || block.State.Terminal() ||
		!slices.Contains(definition.Steps, action.StepSeq) {
		return fault.ErrOperationAuthorization
	}
	if block.State == topology.OperationPending || block.CurrentStep < action.StepSeq {
		return fault.New(fault.Unavailable, "receiver has not applied required block progress", nil)
	}
	if block.CurrentStep != action.StepSeq {
		return fault.ErrOperationAuthorization
	}
	return s.validateActionBarriers(op, action)
}

func (s *scopedState) authorizeDirectTransfer(action topology.AuthorizedStep) error {
	if action.OperatorID != "" || action.Attempt != 0 || action.Version == 0 ||
		action.Kind != topology.StepTransferLeader || action.TransferNodeName != "" || action.TransferReplicaID != 0 ||
		action.ExpectedVotersDigest != "" || action.ResultVotersDigest != "" {
		return fault.ErrOperationAuthorization
	}
	row := s.indexes.groups[action.GroupID]
	if row == nil || row.SourceVersion != action.Version || row.Declaration == nil || row.Deleting ||
		!reflect.DeepEqual(row.Declaration, s.Target.Group(action.GroupID)) {
		return fault.ErrOperationAuthorization
	}
	if row.Owner != "" {
		return fault.ErrUnavailable
	}
	if !slices.ContainsFunc(row.Declaration.Replicas, func(replica topology.Replica) bool {
		return replica.NodeName == action.NodeName && replica.ReplicaID == action.ReplicaID &&
			replica.Role == topology.ReplicaVoter
	}) {
		return fault.ErrOperationAuthorization
	}
	return nil
}
