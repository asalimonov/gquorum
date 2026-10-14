package metaservice

// RetireOptions supplies explicit retirement justification and mutation
// controls.
type RetireOptions struct {
	Reason   string          `json:"reason"`
	Mutation MutationOptions `json:"mutation"`
}
