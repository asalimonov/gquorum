//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestReadyBlocksRequireCurrentOwnerAndIgnoreFailedSiblings(t *testing.T) {
	t.Parallel()
	s := loadAdmissionFixture(t)
	failed := scopedTransitionFor(s.Operations[0], "shard0", 3)
	failed.State, failed.Error = topology.OperationFailed, "unreachable"
	applyScopedTransitionFixture(t, &s, failed)
	work, err := queryReadyBlocks(&s, "", math.MaxInt)
	if err != nil || len(work) != 1 || work[0].GroupID != topology.MetaGroupID {
		t.Fatal(work, err)
	}
	work[0].Operation.Plan.Steps[0].NodeName = "caller"
	if s.Operations[0].Plan.Steps[0].NodeName == "caller" {
		t.Fatal("ready item borrowed immutable plan")
	}
	s.indexes.groups[topology.MetaGroupID].Owner = ""
	if work, err = queryReadyBlocks(&s, "", 10); err != nil || len(work) != 0 {
		t.Fatal("nonowner selected", work, err)
	}
	if _, err := queryReadyBlocks(&s, "", 0); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := NewMachine(testRules(), nil).ReadyBlocks("", 1); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal("schema3 published scoped work", err)
	}
}

func TestReadyBlocksDeriveAdmissionWaitAndClearStaleDiagnostic(t *testing.T) {
	t.Parallel()
	s := loadAdmissionFixture(t)
	op := &s.Operations[0]
	op.Status.Blocks[1].Blocked = &topology.BlockedReason{Code: "dependency", Message: "old cause"}
	op.Status.Blocks[1].State = topology.OperationWaitingForReplica
	before, _ := json.Marshal(s)
	work, err := queryReadyBlocks(&s, "", 10)
	blocked := s.groupProgress("shard0").Blocked
	if err != nil || len(work) != 1 || work[0].GroupID != topology.MetaGroupID ||
		blocked == nil || blocked.Code != "dependency" || blocked.Message == "old cause" {
		t.Fatal("unprepared member became runnable or diagnosis went stale", work, blocked, err)
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("selection or diagnosis mutated durable state")
	}
	admissionPrefixReceipt(&s, 0, 1)
	s.Admissions[0].State = AdmissionReady
	prepareScopedFixture(t, &s)
	work, err = queryReadyBlocks(&s, topology.MetaGroupID, 1)
	if err != nil || len(work) != 1 || work[0].GroupID != "shard0" || s.groupProgress("shard0").Blocked != nil {
		t.Fatal("consumed preparation did not unblock exact load scope", work, err)
	}
	work, err = queryReadyBlocks(&s, "shard0", 1)
	if err != nil || len(work) != 1 || work[0].GroupID != topology.MetaGroupID {
		t.Fatal("continuation failed to wrap", work, err)
	}
}

func TestReadyBlocksReuseIssuedGrantAndIncludeClosedConsumedFinalStep(t *testing.T) {
	t.Parallel()
	s := scopedGraphFixture()
	s.Operations[1].Status.Blocks[1].State = topology.OperationRunning
	s.Operations[1].Status.State = topology.OperationRunning
	key := scopedPendingTransfer(&s)
	prepareScopedFixture(t, &s)
	for _, consumed := range []bool{false, true} {
		if consumed {
			s.indexes.grants[key].Effects = fixtureEffectReceipts(s.indexes.grants[key].Grant)
			s.indexes.grants[key].Closed = true
			prepareScopedFixture(t, &s)
		}
		work, err := queryReadyBlocks(&s, "", 1)
		if err != nil || len(work) != 1 || work[0].GroupID != "shard0" {
			t.Fatal("own issued/consumed grant blocked progress", consumed, work, err)
		}
	}
}

func TestReadyBlocksIncludeZeroStepDeletionWithoutDispatchingCleanup(t *testing.T) {
	t.Parallel()
	s, approval := readmissionDeletionFixture(t)
	applyScopedApprovalFixture(t, &s, approval)
	work, err := queryReadyBlocks(&s, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, item := range work {
		if item.GroupID == "shard0" {
			found = true
		}
	}
	if !found {
		t.Fatal("zero-step deletion stranded", work)
	}
}

func TestReadyBlocksOnlyGateRelevantUnsettledResources(t *testing.T) {
	t.Parallel()
	for _, affected := range []string{topology.MetaGroupID, "shard0"} {
		t.Run(affected, func(t *testing.T) {
			t.Parallel()
			s, approval := approvalFixture(t, affected)
			approval.Plan.Steps = []topology.Step{{Seq: 1, Kind: topology.StepTransferLeader, GroupID: affected, NodeName: "n1", ReplicaID: 1}}
			approval.Plan.Blocks[0].Steps = []int{1}
			scopedPendingTransfer(&s)
			applyScopedApprovalFixture(t, &s, approval)
			work, err := queryReadyBlocks(&s, "", 100)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, item := range work {
				found = found || item.GroupID == "shard0"
			}
			if affected == topology.MetaGroupID && !found {
				t.Fatal("independent fence blocked load work", work)
			}
			if affected == "shard0" {
				blocked := s.groupProgress("shard0").Blocked
				if found || blocked == nil || blocked.Code != "revocation_pending" {
					t.Fatal(work, blocked)
				}
			}
		})
	}
}
