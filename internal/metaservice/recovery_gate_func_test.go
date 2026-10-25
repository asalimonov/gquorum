//go:build functional

package metaservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/internal/recoverytest"
)

func TestRecoveryPreImportGateBlocksJoinAndSchedulerOnAnActualLeader(t *testing.T) {
	t.Parallel()
	accepted := recoverytest.Manifest(model.Paths{StorageDir: "/system"}, "gate", time.Now().UTC(), model.Importing)
	group := singleMetaFixtureWith(t, func(options *Options) {
		options.Identity.ClusterID, options.Identity.MemberID = accepted.Identity.ClusterID, accepted.Identity.MemberID
		options.Founders[0].MemberID = accepted.Identity.MemberID
		options.Limits, options.Recovery = accepted.Rules.Limits, &accepted
	})[0]
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := group.WaitRecoveryReplay(ctx); err != nil {
		t.Fatal(err)
	}
	if !group.IsLeader() || group.state().RecoveryState() != nil {
		t.Fatal("test did not hold the pre-import leader interval")
	}
	if err := group.TransferLeader(ctx, "_meta", group.currentIdentity().NodeName); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("pre-import transfer admitted", err)
	}
	if (schedulingHost{g: group}).IsLeader() {
		t.Fatal("scheduler admitted a repair tick before import")
	}
	reply := group.handleJoin(ctx, &metapb.Header{}, []byte(`{}`))
	if err := metatransport.ReplyError(reply); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("joining request bypassed local recovery gate", err)
	}
	if group.state().Target().Version != 1 || !group.OwnSessionLive() {
		t.Fatal("gate stopped ordinary founding/session progress")
	}
}
