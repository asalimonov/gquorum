package model

import "errors"

// ErrResourceBound preserves input-size refusals independently of corruption.
// It is internal and remains wrapped by the existing fault classification.
var ErrResourceBound = errors.New("recovery input exceeds configured byte bounds")

// InputIssue distinguishes an insufficient configured bound from damaged input.
func InputIssue(issue Issue, cause error) Issue {
	if errors.Is(cause, ErrResourceBound) {
		issue.Code = "resource_bound"
		issue.Message = "Retained input exceeds configured Resources limits; " +
			"review its size and configure appropriate bounds"
	}
	return issue
}
