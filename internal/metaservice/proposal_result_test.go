//go:build !functional

package metaservice

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/topology"
)

func TestProposalConflictSurvivesResultAndReplyEncoding(t *testing.T) {
	t.Parallel()
	// Exercise the replicated result decoder and the distinct leader-reply path.
	raw := []byte(`{"Code":"version_conflict","Reason":"rebased","expected_version":9,"CurrentVersion":12,"rebased_to":12}`)
	var result metaengine.ApplyResult
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &result); err != nil {
		t.Fatal(err)
	}
	wrapped := fmt.Errorf("forwarded: %w", applyResultError(result))
	err = metatransport.ReplyError(metatransport.Reply(wrapped))
	var conflict *fault.Error
	if !errors.Is(err, fault.ErrVersionConflict) || !errors.As(err, &conflict) ||
		conflict.ExpectedVersion != 9 || conflict.CurrentVersion != 12 || conflict.RebasedTo != 12 {
		t.Fatal("lost structured conflict after forwarding", err)
	}
}

func TestMutationOutcomeKeepsStageOnApprovalConflict(t *testing.T) {
	t.Parallel()
	cause := errors.New("approval interrupted")
	err := &fault.Error{Code: fault.VersionConflict, ExpectedVersion: 9, CurrentVersion: 12, RebasedTo: 12, Cause: cause}
	staged := MutationResult{Document: topology.Document{Version: 9}}
	result, failure := staged.outcome(err)
	if result.Document.Version != 9 || result.RebasedTo != 12 || result.OperationID != "" ||
		!errors.Is(failure, cause) || !errors.Is(failure, fault.ErrVersionConflict) {
		t.Fatal("approval failure discarded staged evidence or error cause", result, failure)
	}
	staged.OperatorID = topology.MakeOperatorID(9, 1)
	result, failure = staged.outcome(nil)
	if failure != nil || result.OperationID != topology.MakeOperationID(9, 1) {
		t.Fatal("accepted execution missing from mutation result", result, failure)
	}
	result.RebasedTo = 12
	result, _ = result.outcome(fault.ErrNoController)
	if result.RebasedTo != 12 {
		t.Fatal("unrelated approval error erased the retained successor")
	}
}
