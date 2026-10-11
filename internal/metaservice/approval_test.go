//go:build !functional

package metaservice

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// The response-validation boundary formerly required a three-member runtime.
// Keep malformed-reply coverage isolated from elections and disk scheduling.
func TestFollowerApprovalReplyRequiresExactNonemptyExecutionID(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, data string
		want       topology.OperatorID
		err        error
	}{
		{"success", `"exec-v17-a2"`, "exec-v17-a2", nil},
		{"empty", `""`, "", fault.ErrUnreadable},
		{"null", `null`, "", fault.ErrUnreadable},
		{"malformed", `{`, "", fault.ErrUnreadable},
		{"wrong_type", `17`, "", fault.ErrUnreadable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := approvalOperatorID([]byte(tc.data))
			if got != tc.want || !errors.Is(err, tc.err) {
				t.Fatal(got, err)
			}
		})
	}
}
