//go:build !functional

package metaservice

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func TestApplyVersionDiscriminatorRejectsAmbiguousWrites(t *testing.T) {
	t.Parallel()
	for _, body := range []string{
		`{"kind":"approve","version":3}`, `{"kind":"retry","previous":"exec-v3-a1"}`,
	} {
		if _, err := decodeApplyVersionRequest([]byte(body)); err != nil {
			t.Fatal(body, err)
		}
	}
	for _, body := range []string{
		`{}`, `null`, `{"version":3}`, `{"kind":"","version":3}`, `{"kind":"retry","version":3,"previous":"exec-v3-a1"}`,
		`{"kind":"approve","version":3,"previous":"exec-v3-a1"}`,
		`{"kind":"retry"}`, `{"kind":"retry","previous":" "}`,
		`{"version":3,"plan":{}}`, `{"kind":"automatic","version":3}`, `{"version":3} {}`,
	} {
		if _, err := decodeApplyVersionRequest([]byte(body)); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal(body, err)
		}
	}
}
