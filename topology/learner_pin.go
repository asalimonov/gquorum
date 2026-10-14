package topology

// LearnerPin sets the promotion restriction for one exact group replica.
// Pinning a voter requests guarded demotion; unpinning changes only eligibility.
type LearnerPin struct {
	Replica ReplicaKey
	Pinned  bool
}
