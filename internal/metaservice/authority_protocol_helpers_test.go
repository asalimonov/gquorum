//go:build functional

package metaservice

import (
	"context"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
	"testing"
)

// Protocol doubles model durable host and group evidence independently of meta.
func protocolEffectReceipts(grant topology.AuthorityGrant) []topology.EffectReceipt {
	var result []topology.EffectReceipt
	for _, resource := range grant.Resources {
		if resource.Kind == topology.AuthorityHost {
			for _, target := range grant.Targets {
				if target.NodeName == resource.NodeName {
					result = append(result, topology.EffectReceipt{Key: grant.Key, Resource: resource, Action: grant.Action, Target: target, Outcome: topology.EffectApplied, LocalSequence: grant.Key.Revision*10 + 1})
				}
			}
			continue
		}
		receipt := topology.EffectReceipt{Key: grant.Key, Resource: resource, Action: grant.Action, Outcome: topology.EffectApplied, AppliedIndex: grant.Key.Revision*10 + 1, AppliedTerm: 1}
		if grant.Action.Kind == topology.StepTransferLeader {
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

func (c *verifiedLoadController) installFence(ctx context.Context, request load.FenceRequest) (load.FenceResult, error) {
	if _, err := c.authorized(ctx, request.Grant); err != nil {
		return load.FenceResult{}, err
	}
	proof := topology.FenceEvidence{Key: request.Grant.Key, Resource: request.Resource, Receiver: request.To, ReplicaID: request.ReplicaID}
	if request.Resource.Kind == topology.AuthorityHost {
		proof.LocalSequence = request.Grant.Key.Revision * 10
	} else {
		proof.AppliedIndex, proof.AppliedTerm = request.Grant.Key.Revision*10, 1
	}
	return load.FenceResult{From: request.To, Evidence: proof}, nil
}

func (f *mutationFixture) consumeStep(t *testing.T, action topology.AuthorizedStep) {
	t.Helper()
	grant := f.command(t, metaengine.BeginEffectProposal{Action: action}).Grant
	for _, receipt := range protocolEffectReceipts(*grant) {
		f.command(t, metaengine.RecordEffectProposal{Receipt: receipt})
	}
}
