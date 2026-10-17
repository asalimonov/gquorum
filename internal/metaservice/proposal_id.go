package metaservice

import "crypto/rand"

// newProposalID correlates one proposal with its applied result. Durable retry
// identity comes from the proposal's exact CAS, intent or operation tuple.
func newProposalID() string { return rand.Text() }
