//go:build !functional

package metatransport

import (
	"errors"
	"fmt"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/membership"
)

func TestReplyPreservesVersionConflictAndSafetyIdentity(t *testing.T) {
	t.Parallel()
	conflict := &fault.Error{Code: fault.VersionConflict, ExpectedVersion: 9, CurrentVersion: 10, Message: "stale version"}
	var got *fault.Error
	if !errors.As(ReplyError(Reply(conflict)), &got) || got.ExpectedVersion != 9 || got.CurrentVersion != 10 {
		t.Fatal("lost CAS refusal", got)
	}
	safety := &membership.SafetyRefusal{GroupID: "_meta", Operation: "remove_replica", ReplicaID: 4, Reason: "replica liveness is unknown", Quorum: 2}
	var decoded *membership.SafetyRefusal
	if err := ReplyError(Reply(fmt.Errorf("control: %w", safety))); !errors.As(err, &decoded) || *decoded != *safety {
		t.Fatal("lost exact safety refusal", decoded, err)
	}
}
