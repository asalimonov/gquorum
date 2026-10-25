package recoverymodel

// StartFailure retains the cause of a joined recovery startup that must not
// reacquire another application during the same Node lifetime.
type StartFailure struct{ Cause error }

func (e *StartFailure) Error() string { return e.Cause.Error() }
func (e *StartFailure) Unwrap() error { return e.Cause }
