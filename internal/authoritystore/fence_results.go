package authoritystore

// FenceLookup distinguishes a retained floor from an absent group record.
type FenceLookup struct {
	Record FenceRecord
	Found  bool
}

// FenceResult reports the retained floor and whether it already matched the grant.
type FenceResult struct {
	Record FenceRecord
	Same   bool
}
