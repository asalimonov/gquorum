package topology

// ReadView contains the three documents captured atomically after a quorum
// read barrier and local application. AppliedIndex is the local _meta index.
type ReadView struct {
	AppliedIndex uint64   `json:"applied_index"`
	Latest       Document `json:"latest"`
	Target       Document `json:"target"`
	Active       Document `json:"active"`
}
