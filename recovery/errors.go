package recovery

import "fmt"

// Refusal reports a recovery diagnostic and preserves its classified cause.
type Refusal struct {
	Issue Issue
	Cause error
}

func (e *Refusal) Error() string {
	if e.Issue.Message == "" {
		return e.Issue.Code
	}
	return fmt.Sprintf("%s: %s", e.Issue.Code, e.Issue.Message)
}

func (e *Refusal) Unwrap() error { return e.Cause }
