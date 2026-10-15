//go:build !functional

package metaengine

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func TestScopedAuthorityExactCatalogAndRevocationProof(t *testing.T) {
	t.Parallel()
	good := scopedGraphFixture()
	if _, err := prepareScopedGraph(&good, testRules()); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*scopedState){
		"forged class":             func(s *scopedState) { s.AuthorityRecords[0].Class = "cleanup" },
		"cleanup fence":            func(s *scopedState) { s.AuthorityRecords[1].Class = "cleanup" },
		"changed immutable action": func(s *scopedState) { s.AuthorityRecords[0].Grant.Action.StepSeq = 2 },
		"missing floor":            func(s *scopedState) { s.AuthorityFloors = nil },
		"decreased floor":          func(s *scopedState) { s.AuthorityFloors[0].Key.Revision = 3 },
		"future revision":          func(s *scopedState) { s.AppliedIndex = 3 },
		"duplicate receipt": func(s *scopedState) {
			s.AuthorityRecords[0].Effects = append(s.AuthorityRecords[0].Effects, s.AuthorityRecords[0].Effects[0])
		},
		"refused receipt":           func(s *scopedState) { s.AuthorityRecords[0].Effects[0].Outcome = "refused" },
		"receipt after newer fence": func(s *scopedState) { s.AuthorityRecords[0].Effects[0].AppliedIndex = 5 },
		"foreign receiver":          func(s *scopedState) { s.AuthorityRecords[1].Fences[0].Receiver.InstanceID = "" },
		"missing fence predecessor": func(s *scopedState) { s.AuthorityRecords = s.AuthorityRecords[1:]; s.AuthorityRecords[0].Fences = nil },
		"scope not in plan":         func(s *scopedState) { s.Revocations[0].GroupID = "elsewhere" },
		"wrong resource scope":      func(s *scopedState) { s.Revocations[0].GroupID = "shard0" },
		"duplicate obligation": func(s *scopedState) {
			s.Revocations[0].Obligations = append(s.Revocations[0].Obligations, s.Revocations[0].Obligations[0])
		},
		"unrecorded fence proof": func(s *scopedState) { s.Revocations[0].Obligations[0].Evidence.AppliedIndex = 5 },
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			state := scopedGraphFixture()
			corrupt(&state)
			if _, err := prepareScopedGraph(&state, testRules()); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("invalid authority proof accepted", err)
			}
		})
	}
}

func TestScopedAuthorityRetainsDetachedUnresolvedDebtBeyondOldCap(t *testing.T) {
	t.Parallel()
	state := scopedCodecFixture()
	state.Rules.Limits.MaxPendingAuthorityRecords = 1
	for i := range state.Cleanup {
		state.AuthorityRecords[i].Effects = nil
		state.Cleanup[i].Retired, state.Cleanup[i].RetiredAt, state.Cleanup[i].Collection = nil, nil, nil
	}
	if _, err := prepareScopedGraph(&state, state.Rules); err != nil {
		t.Fatal("valid unresolved cleanup hit old cap", err)
	}
	state.Cleanup[0].RetireAction = nil
	if _, err := prepareScopedGraph(&state, state.Rules); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("archived unconsumed cleanup lost its exact action/debt binding", err)
	}
}
