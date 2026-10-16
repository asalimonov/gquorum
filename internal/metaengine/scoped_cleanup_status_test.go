//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func TestScopedCleanupStatusRequiresExactProofAndWritesOnlyChanges(t *testing.T) {
	t.Parallel()
	s := scopedCodecFixture()
	s.Cleanup[0].Collection = nil
	prepareScopedFixture(t, &s)
	debt := s.Cleanup[0].clone()
	later := debt.UpdatedAt.Add(time.Hour)
	p := RecordCleanupStatusProposal{Identity: debt.Identity, MemberID: debt.MemberID,
		RetirementKey: debt.Retired.Key, NotBefore: &later, Blocked: &topology.BlockedReason{Code: "retention"},
		At: debt.UpdatedAt.Add(time.Minute), ProposalID: "deferred"}
	body, _ := json.Marshal(p)
	result, err := applyScopedProposal(&s, s.AppliedIndex+1, "record_cleanup_status", body)
	if err != nil || !result.changed || !result.result.Accepted {
		t.Fatal("exact retention status refused", result, err)
	}
	got := s.indexes.cleanup[debt.Identity]
	if !reflect.DeepEqual(got.Removed, debt.Removed) || !reflect.DeepEqual(got.Stopped, debt.Stopped) ||
		!reflect.DeepEqual(got.Retired, debt.Retired) || *got.NotBefore != later {
		t.Fatal("diagnostic update changed lifecycle proof")
	}
	before, _ := json.Marshal(s)
	p.At = p.At.Add(time.Second)
	result, err = s.applyRecordCleanupStatus(s.AppliedIndex+1, p)
	after, _ := json.Marshal(s)
	if err != nil || result.changed || !bytes.Equal(before, after) {
		t.Fatal("repeated callback rewrote durable state", err)
	}
	for _, mutate := range []func(*RecordCleanupStatusProposal){
		func(p *RecordCleanupStatusProposal) { p.MemberID++ },
		func(p *RecordCleanupStatusProposal) { p.RetirementKey.Revision++ },
		func(p *RecordCleanupStatusProposal) { p.Identity.ReplicaID++ },
	} {
		wrong := p
		mutate(&wrong)
		if _, err := s.applyRecordCleanupStatus(s.AppliedIndex+1, wrong); err == nil {
			t.Fatal("foreign reporter/proof accepted", wrong)
		}
	}
	p.NotBefore, p.Blocked = nil, nil
	result, err = s.applyRecordCleanupStatus(s.AppliedIndex+1, p)
	if err != nil || !result.changed || s.indexes.cleanup[debt.Identity].Blocked != nil {
		t.Fatal("exact status clear failed", result, err)
	}
}
