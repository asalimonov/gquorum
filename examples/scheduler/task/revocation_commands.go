package task

// Revocation identifies an exact attempt and expected state to revoke.
type Revocation struct {
	Key           Key    `json:"key"`
	ExpectedState string `json:"expected_state"` // assigned or running
}

// Revoke requests a batch of guarded assignment revocations.
type Revoke struct {
	Tasks []Revocation `json:"tasks"`
}
