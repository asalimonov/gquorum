//go:build !functional

package recoverydisk

import (
	"strings"
	"testing"

	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/internal/recoverytest"
)

func TestApprovalShapeRequiresIdentityToFitNormalHeaders(t *testing.T) {
	t.Parallel()
	for _, mode := range []model.Mode{model.Rebuild, model.ResetForJoin} {
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()
			accepted := recoverytest.Accepted(model.Paths{StorageDir: "/system", LoadWALDir: "/wal"}, "bounded")
			accepted.Request.Mode = mode
			if mode == model.ResetForJoin {
				accepted.Request.ClusterID = accepted.Identity.ClusterID
				accepted.Identity.MemberID = 0
				accepted.Sources = nil
			}
			if err := ValidateApprovalShape(accepted); err != nil {
				t.Fatal(err)
			}
			accepted.Identity.ClusterID = strings.Repeat("x", int(model.HeaderMaxBytes))
			accepted.Request.ClusterID = accepted.Identity.ClusterID
			if err := ValidateApprovalShape(accepted); err == nil {
				t.Fatal("oversized future identity accepted before archive")
			}
		})
	}
}
