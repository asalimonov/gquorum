package topology

// ReadView contains staged and approved intent captured after a quorum read
// barrier and local application. Per-group convergence is read separately.
type ReadView struct {
	FullyConverged FullyConverged `json:"fully_converged"`
	AppliedIndex   uint64         `json:"applied_index"`
	Latest         Document       `json:"latest"`
	Target         Document       `json:"target"`
}
