package main

import (
	"context"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// Verify the exact approved local identity before an external process effect.
// RequestClose schedules joined shutdown and lets the callback return promptly.
func (a *application) selfRemoved(parent context.Context, reason string) error {
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	identity := a.meta.Identity()
	if identity.NodeName != a.cfg.NodeName || identity.ClusterID != a.cfg.ClusterID || identity.InstanceID != a.cfg.InstanceID || identity.MemberID == 0 {
		return fault.ErrIdentityMismatch
	}
	status, err := a.meta.Status(ctx)
	if err != nil {
		return err
	}
	removal := status.SelfRemoval
	if removal == nil || !removal.Fenced || removal.Reason != reason || removal.MemberID != identity.MemberID || removal.Version == 0 || removal.AppliedIndex == 0 {
		return fault.ErrIdentityMismatch
	}
	// The durable apply callback signals removal before the Raft loop publishes
	// its next owned status. Wait for that publication instead of treating this
	// harmless scheduling window as a permanent authorization failure.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		local := a.meta.LocalStatus()
		if a.meta.Identity() != identity || local.HostNode != identity.NodeName || local.GroupID != topology.MetaGroupID {
			return fault.ErrIdentityMismatch
		}
		if local.Applied >= removal.AppliedIndex && !slices.Contains(local.ConfState.Voters, identity.MemberID) && !slices.Contains(local.ConfState.Learners, identity.MemberID) {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	// Verify the recorded approved version, even if unrelated intent advances
	// Target while this owned callback is being scheduled.
	target, err := a.meta.Get(ctx, removal.Version)
	if err != nil {
		return err
	}
	if target.ClusterID != identity.ClusterID || target.MemberID(identity.NodeName) == identity.MemberID {
		return fault.ErrIdentityMismatch
	}
	a.logger.Info("self-removal identity verified", "node", identity.NodeName, "member_id", identity.MemberID, "instance_id", identity.InstanceID, "reason", reason)
	a.RequestClose()
	return nil
}
