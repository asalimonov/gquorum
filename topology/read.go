package topology

// ReadView contains the three documents captured atomically after a quorum
// read barrier and local application. AppliedIndex is the local _meta index.
type ReadView struct {
	FullyConverged FullyConverged `json:"fully_converged"`
	AppliedIndex   uint64         `json:"applied_index"`
	Latest         Document       `json:"latest"`
	Target         Document       `json:"target"`
	// Removed at the complete Task78 cutover after placement/scheduler/join consumers convert.
	Active Document `json:"active"`
}
