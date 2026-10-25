package recoveryruntime

import (
	"context"
	"errors"

	"github.com/asalimonov/gquorum/fault"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

func (c *Coordinator) consumeGroups(ctx context.Context, operation topology.OperationID,
	record *model.ImportState) error {
	if err := c.waitImportOperation(ctx, operation); err != nil {
		return err
	}
	for _, group := range record.Groups {
		if err := c.consumeGroup(ctx, group, record); err != nil {
			return err
		}
	}
	return nil
}

func (c *Coordinator) recordGroup(ctx context.Context, proposal model.GroupConsumedProposal) error {
	err := c.config.Meta.RecordRecoveredGroup(ctx, proposal)
	if err == nil {
		return nil
	}
	record, readErr := c.readRecovery(ctx)
	if readErr != nil {
		return errors.Join(err, readErr)
	}
	if validationErr := c.validateRecord(record); validationErr != nil {
		return validationErr
	}
	for _, group := range record.Groups {
		if group.GroupID != proposal.GroupID || group.Consumed == nil {
			continue
		}
		if group.Consumed.Identity == proposal.Checkpoint.Identity &&
			group.Consumed.Checkpoint == proposal.Checkpoint.Position &&
			group.Consumed.SnapshotDigest == proposal.Checkpoint.Digest {
			return nil
		}
		return fault.ErrConflict
	}
	return err
}

func (c *Coordinator) consumeGroup(ctx context.Context, group model.ImportGroup, record *model.ImportState) error {
	ref := load.ReplicaRef{GroupID: group.GroupID, ReplicaID: group.Destination.ReplicaID}
	// The retry timer is bounded; accepted restore/checkpoint work uses its owner,
	// not the short metadata-call attempt context.
	proof, err := fault.Retry(ctx, fault.RetryOptions{}, func(context.Context) (model.CheckpointProof, error) {
		return c.config.Host.RecoveryCheckpoint(ctx, ref)
	})
	if err != nil {
		return err
	}
	if group.Consumed != nil {
		return nil
	}
	proposal := model.GroupConsumedProposal{RecoveryID: record.RecoveryID, PlanDigest: record.PlanDigest,
		GroupID: group.GroupID, Checkpoint: proof, ProposalID: record.RecoveryID + ".consume." + group.GroupID}
	recordErr := retryControl(ctx, func(attempt context.Context) error { return c.recordGroup(attempt, proposal) })
	if recordErr != nil {
		return recordErr
	}
	confirmed, err := c.readRecovery(ctx)
	if err != nil {
		return err
	}
	if err := c.validateRecord(confirmed); err != nil {
		return err
	}
	return c.config.Host.SetRecovery(ctx, confirmed, c.config.Manifest)
}

func (c *Coordinator) replayGroups(ctx context.Context, record *model.ImportState) error {
	for _, group := range record.Groups {
		ref := load.ReplicaRef{GroupID: group.GroupID, ReplicaID: group.Destination.ReplicaID}
		outcome, err := fault.Retry(ctx, fault.RetryOptions{}, func(context.Context) (error, error) {
			callErr := c.config.Host.ReplayRecovery(ctx, ref)
			var failed *model.StartFailure
			if errors.As(callErr, &failed) {
				return callErr, nil
			}
			return nil, callErr
		})
		if err != nil {
			return err
		}
		if outcome != nil {
			return outcome
		}
	}
	return nil
}
