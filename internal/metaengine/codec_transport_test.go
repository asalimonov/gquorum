package metaengine

import (
	"reflect"
	"testing"
)

func TestDecodeProposalWhitelistsCompleteCommandSet(t *testing.T) {
	t.Parallel()
	for _, want := range []any{ProposeDocument{ExpectedVersion: 9}, AdmitNode{MemberID: 8}, BeginOperatorProposal{Version: 7}, CommitRepairOperatorProposal{ExpectedVersion: 6}, TransitionOperatorProposal{ProposalID: "transition"}, ActivateTargetProposal{Version: 5}, BindAdmissionProposal{Founding: true}, CommitReadmissionProposal{ExpectedVersion: 4}, ReadyAdmissionProposal{MemberID: 3}, GrantSession{SessionID: "session", TTLSeconds: 30}, RevokeSession{Reason: "expired"}, AcquireLock{Key: "node/n1"}} {
		data, err := Encode(want)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeProposal(data)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(reflect.ValueOf(got).Elem().Interface(), want) {
			t.Fatalf("lost command fields: got %+v want %+v", got, want)
		}
	}
	for _, data := range []string{`{`, `{}`, `{"schema_version":2,"kind":"grant_session","body":{}}`, `{"schema_version":1,"kind":"arbitrary","body":{}}`, `{"schema_version":1,"kind":"grant_session","body":null}`, `{"schema_version":1,"kind":"grant_session","body":[]}`, `{"schema_version":1,"kind":"grant_session","body":{"ttl_seconds":"bad"}}`} {
		if _, err := DecodeProposal([]byte(data)); err == nil {
			t.Errorf("accepted invalid envelope %s", data)
		}
	}
}
