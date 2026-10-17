package operator

import (
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// ValidateEffectTarget checks the dispatch selection without trimming the grant.
// Current authority must still be confirmed before sending and by the recipient.
func ValidateEffectTarget(grant topology.AuthorityGrant, resource topology.AuthorityResource,
	target topology.AuthorityTarget) error {
	if grant.Action == nil {
		return fault.ErrOperationAuthorization
	}
	return validateDispatchTarget(grant, resource, target)
}

// ValidateFenceTarget preserves the exact boundary and recipient selection.
func ValidateFenceTarget(grant topology.AuthorityGrant, resource topology.AuthorityResource,
	target topology.AuthorityTarget) error {
	if grant.Purpose != topology.AuthorityFence {
		return fault.ErrOperationAuthorization
	}
	return validateDispatchTarget(grant, resource, target)
}

func validateDispatchTarget(grant topology.AuthorityGrant, resource topology.AuthorityResource,
	target topology.AuthorityTarget) error {
	if grant.Validate() != nil ||
		!slices.Contains(grant.Resources, resource) || !slices.Contains(grant.Targets, target) {
		return fault.ErrOperationAuthorization
	}
	if resource.Kind == topology.AuthorityHost && resource.NodeName != target.NodeName {
		return fault.ErrOperationAuthorization
	}
	return nil
}
