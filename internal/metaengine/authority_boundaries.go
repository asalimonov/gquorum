package metaengine

import "github.com/asalimonov/gquorum/topology"

func orderedFenceBoundaries(a, b topology.FenceEvidence) bool {
	if a.Resource != b.Resource || a.Key == b.Key {
		return true
	}
	if a.Key.Revision > b.Key.Revision {
		a, b = b, a
	}
	if a.Resource.Kind == topology.AuthorityGroup {
		return a.AppliedIndex < b.AppliedIndex && a.AppliedTerm <= b.AppliedTerm
	}
	return a.LocalSequence < b.LocalSequence
}
func receiptFitsFence(receipt topology.EffectReceipt, fence topology.FenceEvidence) bool {
	if receipt.Resource != fence.Resource {
		return true
	}
	if receipt.Resource.Kind == topology.AuthorityHost {
		switch {
		case receipt.Key == fence.Key:
			return receipt.LocalSequence == fence.LocalSequence
		case receipt.Key.Revision < fence.Key.Revision:
			return receipt.LocalSequence < fence.LocalSequence
		default:
			return receipt.LocalSequence > fence.LocalSequence
		}
	}
	if receipt.Key.Revision < fence.Key.Revision {
		return receipt.AppliedIndex < fence.AppliedIndex && receipt.AppliedTerm <= fence.AppliedTerm
	}
	return receipt.AppliedIndex >= fence.AppliedIndex && receipt.AppliedTerm >= fence.AppliedTerm
}
func validAuthorityBoundaries(a authorityCatalog) bool {
	var fences []topology.FenceEvidence
	var receipts []topology.EffectReceipt
	for _, r := range a.records {
		fences = append(fences, r.Fences...)
		receipts = append(receipts, r.Effects...)
	}
	for i, fence := range fences {
		for _, other := range fences[i+1:] {
			if !orderedFenceBoundaries(fence, other) {
				return false
			}
		}
		for _, receipt := range receipts {
			if !receiptFitsFence(receipt, fence) {
				return false
			}
		}
	}
	return true
}
