//go:build functional

package operator

import (
	"context"
	"slices"

	"github.com/asalimonov/gquorum/topology"
)

func (c *controllerCoordinator) ReconcileFences(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return c.reconcileErr
}

func (c *controllerCoordinator) BeginEffect(ctx context.Context, action AuthorizedStep) (topology.AuthorityGrant, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return topology.AuthorityGrant{}, err
	}
	if c.beginErr != nil {
		return topology.AuthorityGrant{}, c.beginErr
	}
	if c.grants == nil {
		c.grants = map[AuthorizedStep]topology.AuthorityGrant{}
		c.receipts = map[topology.AuthorityKey][]topology.EffectReceipt{}
	}
	if old, found := c.grants[action]; found {
		return old.Clone(), nil
	}
	target, _ := c.store.Target(ctx)
	active, _ := c.store.Active(ctx)
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: target.ClusterID, GroupID: action.GroupID, Revision: uint64(len(c.grants) + 1)}, Purpose: topology.AuthorityEffect, OperatorID: action.OperatorID, Action: &action}
	resources := map[topology.AuthorityResource]bool{}
	targets := map[topology.AuthorityTarget]bool{}
	add := func(node string, replica uint64, host bool) {
		member := target.MemberID(node)
		if member == 0 {
			member = active.MemberID(node)
		}
		targets[topology.AuthorityTarget{NodeName: node, MemberID: member, ReplicaID: replica}] = true
		if host {
			resources[topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: action.GroupID, NodeName: node}] = true
		}
	}
	switch action.Kind {
	case StepCreateFoundingGroup, StepDeleteGroupState:
		doc := target
		if action.Kind == StepDeleteGroupState {
			doc = active
		} else {
			resources[topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: action.GroupID}] = true
		}
		if group := doc.Group(action.GroupID); group != nil {
			for _, replica := range group.Replicas {
				if action.Kind == StepDeleteGroupState || replica.Role == ReplicaVoter {
					add(replica.NodeName, replica.ReplicaID, true)
				}
			}
		}
	case StepPrepareReplica, StepStopReplica, StepCleanupReplicaState:
		add(action.NodeName, action.ReplicaID, true)
	default:
		resources[topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: action.GroupID}] = true
		add(action.NodeName, action.ReplicaID, false)
		if action.TransferReplicaID != 0 {
			add(action.TransferNodeName, action.TransferReplicaID, false)
		}
	}
	for resource := range resources {
		grant.Resources = append(grant.Resources, resource)
	}
	for target := range targets {
		grant.Targets = append(grant.Targets, target)
	}
	slices.SortFunc(grant.Resources, topology.AuthorityResource.Compare)
	slices.SortFunc(grant.Targets, topology.AuthorityTarget.Compare)
	if err := grant.Validate(); err != nil {
		return topology.AuthorityGrant{}, err
	}
	c.grants[action] = grant.Clone()
	return grant, nil
}

func (c *controllerCoordinator) EffectReceipts(ctx context.Context, grant topology.AuthorityGrant) ([]topology.EffectReceipt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	result := slices.Clone(c.receipts[grant.Key])
	for i := range result {
		result[i] = result[i].Clone()
	}
	return result, nil
}

func (c *controllerCoordinator) RecordEffect(ctx context.Context, receipt topology.EffectReceipt) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if c.receiptErr != nil {
		return c.receiptErr
	}
	grant, found := c.grants[*receipt.Action]
	if !found || receipt.Validate(grant) != nil {
		return ErrOperatorAuthorization
	}
	if !slices.ContainsFunc(c.receipts[receipt.Key], func(old topology.EffectReceipt) bool {
		return old.Resource == receipt.Resource && old.Target == receipt.Target
	}) {
		c.receipts[receipt.Key] = append(c.receipts[receipt.Key], receipt.Clone())
	}
	return nil
}

func controllerReceipts(grant topology.AuthorityGrant) []topology.EffectReceipt {
	var result []topology.EffectReceipt
	for _, resource := range grant.Resources {
		if resource.Kind == topology.AuthorityHost {
			for _, target := range grant.Targets {
				if target.NodeName == resource.NodeName {
					result = append(result, topology.EffectReceipt{Key: grant.Key, Resource: resource, Action: grant.Action, Target: target, Outcome: topology.EffectApplied, LocalSequence: grant.Key.Revision})
				}
			}
			continue
		}
		receipt := topology.EffectReceipt{Key: grant.Key, Resource: resource, Action: grant.Action, Outcome: topology.EffectApplied, AppliedIndex: grant.Key.Revision + 1, AppliedTerm: 1}
		if grant.Action.Kind == StepTransferLeader {
			receipt.Outcome = topology.EffectTransferStarted
		}
		for _, target := range grant.Targets {
			if target.NodeName == grant.Action.NodeName && target.ReplicaID == grant.Action.ReplicaID {
				receipt.Target = target
			}
		}
		result = append(result, receipt)
	}
	return result
}
