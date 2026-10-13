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
		if resource.Kind == topology.AuthorityGroup {
			if !slices.ContainsFunc(receipts, func(r topology.EffectReceipt) bool { return r.Resource == resource }) {
				return false, nil
			}
			continue
		}
		for _, target := range grant.Targets {
			if target.NodeName == resource.NodeName && !slices.ContainsFunc(receipts, func(r topology.EffectReceipt) bool { return r.Resource == resource && r.Target == target }) {
				return false, nil
			}
		}
	}
	return true, nil
}
