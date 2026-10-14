package operator

import (
	"slices"

	"github.com/asalimonov/gquorum/topology"
)

func receiptSetComplete(grant topology.AuthorityGrant, receipts []topology.EffectReceipt) (bool, error) {
	for _, receipt := range receipts {
		if receipt.Validate(grant) != nil {
			return false, ErrOperatorAuthorization
		}
	}
	for _, resource := range grant.Resources {
		if !resourceReceiptsComplete(resource, grant.Targets, receipts) {
			return false, nil
		}
	}
	return true, nil
}

func resourceReceiptsComplete(resource topology.AuthorityResource, targets []topology.AuthorityTarget,
	receipts []topology.EffectReceipt) bool {
	if resource.Kind == topology.AuthorityGroup {
		return slices.ContainsFunc(receipts, func(r topology.EffectReceipt) bool { return r.Resource == resource })
	}
	for _, target := range targets {
		if target.NodeName == resource.NodeName &&
			!slices.ContainsFunc(receipts,
				func(r topology.EffectReceipt) bool { return r.Resource == resource && r.Target == target }) {
			return false
		}
	}
	return true
}
