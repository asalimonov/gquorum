//go:build !functional

package metaengine

import (
	"github.com/asalimonov/gquorum/topology"
	"testing"
)

// consumeTestStep explicitly models the resource owner's durable consumption.
// index is the next free metadata log index; the return value preserves that convention.
func consumeTestStep(t *testing.T, sm *topologySM, id topology.OperatorID, index uint64) uint64 {
	t.Helper()
	ex := sm.operators[id]
	step := ex.Steps[ex.CurrentStep-1]
	if step.Kind == topology.StepWaitCaughtUp {
		return index
	}
	action := topology.AuthorizedStep{OperatorID: id, Version: ex.Version, Attempt: ex.Attempt, StepSeq: step.Seq, Kind: step.Kind, GroupID: step.GroupID, NodeName: step.NodeName, ReplicaID: step.ReplicaID, TransferNodeName: step.TransferNodeName, TransferReplicaID: step.TransferReplicaID}
	result := apply(t, sm, index, BeginEffectProposal{Action: action})
	index++
	if !result.Accepted || result.Grant == nil {
		t.Fatalf("grant step %d: %+v", step.Seq, result)
	}
	for _, receipt := range fixtureEffectReceipts(*result.Grant) {
		result := apply(t, sm, index, RecordEffectProposal{Receipt: receipt})
		index++
		if !result.Accepted {
			t.Fatalf("consume step %d: %+v", step.Seq, result)
		}
	}
	return index
}
