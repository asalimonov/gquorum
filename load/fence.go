package load

import (
	"github.com/asalimonov/gquorum/topology"
)

// FenceRequest requests installation of a grant fence on an exact authority
// resource.
type FenceRequest struct {
	From      topology.Identity          `json:"from"`
	To        topology.Identity          `json:"to"`
	ReplicaID uint64                     `json:"replica_id,omitempty"`
	Resource  topology.AuthorityResource `json:"resource"`
	Grant     topology.AuthorityGrant    `json:"grant"`
}

// FenceResult returns the responding node and its installed fence evidence.
type FenceResult struct {
	From     topology.Identity      `json:"from"`
	Evidence topology.FenceEvidence `json:"evidence"`
}
