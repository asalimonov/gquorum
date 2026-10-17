package metaengine

import (
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// Until then its nil adapter refuses while owning-package tests exercise this path.
func queryScopedAuthority(state *scopedState, query QueryRequest) (topology.AuthorityStatus, error) {
	if state == nil {
		return topology.AuthorityStatus{}, fault.ErrUnsupported
	}
	if err := query.validateScopedAuthority(); err != nil {
		return topology.AuthorityStatus{}, err
	}
	status := topology.AuthorityStatus{OperationID: topology.OperationID(query.OperationID)}
	known := state.indexes.operations[query.OperationID] != nil
	for _, row := range state.Revocations {
		if row.OperationID != query.OperationID {
			continue
		}
		known = true
		if row.Requested {
			status.RevocationRequested = true
			for _, ob := range row.Obligations {
				status.Obligations = appendObligation(status.Obligations, ob)
			}
		}
	}
	if !known {
		return topology.AuthorityStatus{}, fault.ErrHistoryUnavailable
	}
	catalog := state.authorityCatalog()
	status.Obligations = catalog.settledObligations(status.Obligations)
	status.RevocationComplete = status.RevocationRequested &&
		revokedComplete(status.Obligations)
	return status.Clone(), nil
}
