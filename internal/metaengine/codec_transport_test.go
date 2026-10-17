//go:build !functional

package metaengine

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func TestDecodeProposalWhitelistsCompleteCommandSet(t *testing.T) {
	t.Parallel()
	_, begin := approvalFixture(t, "shard0")
	_, repair := repairCommitFixture(t)
	proposals := []any{
		RecordReadmissionReceiptsProposal{ProposalID: "receipts"},
		BeginEffectProposal{ProposalID: "effect"}, RecordFenceProposal{ProposalID: "fence"},
		RecordEffectProposal{ProposalID: "receipt"},
		GrantTransferProposal{RequestID: "transfer", ExpectedSourceVersion: 1},
		BeginReadmissionAuthority{PreviousMemberID: 1, NodeName: "n1", InstanceID: "replacement", MemberID: 4, OperationID: "exec-v2-a1"},
		ProposeDocument{ExpectedVersion: 9}, begin, repair,
		TransitionOperationProposal{ProposalID: "transition"},
		RetryOperationProposal{Scopes: []ScopeExpectation{}, ProposalID: "retry"},
		RecordCollectionProposal{ProposalID: "collection"}, BeginCleanupEffectProposal{ProposalID: "cleanup"}, RecordCleanupStatusProposal{ProposalID: "status"},
		CommitVoterChangeProposal{}, BindAdmissionProposal{Founding: true},
		CommitReadmissionProposal{ExpectedVersion: 4}, ReadyAdmissionProposal{MemberID: 3},
		GrantSession{SessionID: "session", TTLSeconds: 30}, RevokeSession{Reason: "expired"}, AcquireLock{Key: "node/n1"},
	}
	for _, want := range proposals {
		t.Run(reflect.TypeOf(want).Name(), func(t *testing.T) {
			t.Parallel()
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
		})
	}
	for _, data := range []string{`{`, `{}`, `{"schema_version":2,"kind":"grant_session","body":{}}`, `{"schema_version":4,"kind":"arbitrary","body":{}}`, `{"schema_version":4,"kind":"grant_session","body":null}`, `{"schema_version":4,"kind":"grant_session","body":[]}`, `{"schema_version":4,"kind":"grant_session","body":{"ttl_seconds":"bad"}}`} {
		if _, err := DecodeProposal([]byte(data)); err == nil {
			t.Errorf("accepted invalid envelope %s", data)
		}
	}
}

func TestCodecRejectsRemovedCommandsInCurrentEnvelope(t *testing.T) {
	t.Parallel()
	for _, kind := range []ProposalKind{"admit_node", "begin_execution", "commit_repair_execution", "transition_execution", "activate_target"} {
		data, err := json.Marshal(envelope{SchemaVersion: metaEnvelopeSchemaVersion, Kind: kind, Body: json.RawMessage(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeProposal(data); err == nil {
			t.Fatalf("accepted removed command %s", kind)
		}
	}
	if _, err := Encode(struct{}{}); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal("encoded an unrecognized proposal", err)
	}
}
