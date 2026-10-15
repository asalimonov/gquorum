//go:build !functional

package metatransport

import (
	"errors"
	"fmt"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/membership"
	"github.com/asalimonov/gquorum/topology"
)

func TestReplyPreservesVersionConflictAndSafetyIdentity(t *testing.T) {
	t.Parallel()
	conflict := &fault.Error{Code: fault.VersionConflict, ExpectedVersion: 9, CurrentVersion: 10, RebasedTo: 12, Message: "stale version"}
	var got *fault.Error
	if !errors.As(ReplyError(Reply(conflict)), &got) || got.ExpectedVersion != 9 || got.CurrentVersion != 10 || got.RebasedTo != 12 {
		t.Fatal("lost CAS refusal", got)
	}
	safety := &membership.SafetyRefusal{GroupID: "_meta", Operation: "remove_replica", ReplicaID: 4, Reason: "replica liveness is unknown", Quorum: 2}
	var decoded *membership.SafetyRefusal
	if err := ReplyError(Reply(fmt.Errorf("control: %w", safety))); !errors.As(err, &decoded) || *decoded != *safety {
		t.Fatal("lost exact safety refusal", decoded, err)
	}
}

func TestReplyPreservesPlacementReport(t *testing.T) {
	t.Parallel()
	report := topology.PlacementReport{Version: 7, ApprovedVersion: 6, Groups: []topology.GroupPlacementReport{{GroupID: "billing", Issues: []topology.PlacementIssue{{Code: "insufficient_distinct_zones", GroupID: "billing", Message: "only two zones"}}}}}
	err := &topology.PlacementError{Code: fault.Invalid, Report: report}
	var decoded *topology.PlacementError
	if failure := ReplyError(Reply(fmt.Errorf("forwarded: %w", err))); !errors.As(failure, &decoded) || decoded.Code != fault.Invalid || decoded.Report.Version != 7 || decoded.Report.Groups[0].Issues[0].Code != "insufficient_distinct_zones" {
		t.Fatal("transport dropped placement details", decoded, failure)
	}
}
