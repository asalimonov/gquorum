package gquorum

// RetireOptions supplies the retirement reason and topology mutation controls.
type RetireOptions struct {
	Reason   string          `json:"reason"`
	Mutation MutationOptions `json:"mutation"`
}
