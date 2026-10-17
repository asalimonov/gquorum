//go:build !functional

package metaservice

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestFollowerOperationReplyRequiresExactNonemptyID(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, data string
		want       topology.OperationID
		err        error
	}{
		{"success", `"op-v17-a2"`, "op-v17-a2", nil},
		{"empty", `""`, "", fault.ErrUnreadable},
		{"null", `null`, "", fault.ErrUnreadable},
		{"malformed", `{`, "", fault.ErrUnreadable},
		{"wrong_type", `17`, "", fault.ErrUnreadable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := forwardOperation(context.Background(), applyVersionRequest{Kind: "approve", Version: 17},
				func(context.Context, string, any) ([]byte, error) { return []byte(tc.data), nil })
			if got != tc.want || !errors.Is(err, tc.err) {
				t.Fatal(got, err)
			}
		})
	}
}
