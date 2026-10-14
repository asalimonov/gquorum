package topology

// EffectOutcome classifies what an acknowledged authorized effect accomplished.
type EffectOutcome string

// EffectReceipt binds durable consumption evidence to the exact grant, action
// and resource.
type EffectReceipt struct {
	Key           AuthorityKey      `json:"key"`
	Resource      AuthorityResource `json:"resource"`
	Action        *AuthorizedStep   `json:"action,omitempty"`
	AdmissionID   string            `json:"admission_id,omitempty"`
	Target        AuthorityTarget   `json:"target"`
	Outcome       EffectOutcome     `json:"outcome"`
	AppliedIndex  uint64            `json:"applied_index,omitempty"`
	AppliedTerm   uint64            `json:"applied_term,omitempty"`
	LocalSequence uint64            `json:"local_sequence,omitempty"`
}
