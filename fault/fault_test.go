package fault

import (
	"errors"
	"fmt"
	"testing"
)

func TestClassificationPreservesCause(t *testing.T) {
	t.Parallel()
	cause := errors.New("disk unavailable")
	err := fmt.Errorf("proposal: %w", New(Unavailable, "retry", cause))
	if !errors.Is(err, ErrUnavailable) || !errors.Is(err, cause) || errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	var typed *Error
	if !errors.As(err, &typed) || typed.Code != Unavailable {
		t.Fatal("missing typed error")
	}
}
