//go:build !functional

package metaengine

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestScopedProposalPayloads(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	scopes := []ScopeExpectation{{GroupID: "work", SourceVersion: 3, Owner: "exec-v4-a1"}}
	plan := topology.Plan{Version: 5, Blocks: []topology.PlanBlock{{GroupID: "work", SourceVersion: 3}}}
	id := storage.Identity{ClusterID: "c", NodeName: "n", GroupID: "work", ReplicaID: 7}
	key := topology.AuthorityKey{ClusterID: "c", GroupID: "work", Revision: 10}
	for _, want := range []any{
		BeginOperationProposal{Version: 5, Plan: plan, Scopes: scopes, Actor: "operator", Origin: topology.OriginOperator, At: at, ProposalID: "begin"},
		RetryOperationProposal{Previous: "exec-v4-a1", Scopes: scopes, At: at, ProposalID: "retry"},
		TransitionOperationProposal{Transition: topology.OperationTransition{OperationID: "exec-v4-a2", Version: 4, Attempt: 2, GroupID: "work", StepSeq: 3, State: topology.OperationFailed, At: at}, ProposalID: "transition"},
		CommitRepairOperationProposal{Doc: topology.Document{Version: 5}, ExpectedVersion: 4, ExpectedTarget: 3, Scopes: scopes, Plan: plan, At: at, ProposalID: "repair"},
		RecordCollectionProposal{Receipt: authoritystore.CollectionReceipt{Identity: id, MemberID: 2, RetirementKey: key, Token: "opaque", LocalSequence: 8, CollectedAt: at}, Courier: topology.Identity{ClusterID: "c", NodeName: "n", MemberID: 3, InstanceID: "next"}, ReadmissionKey: &key, At: at, ProposalID: "receipt"},
		BeginCleanupEffectProposal{Identity: id, Action: topology.AuthorizedStep{OperatorID: "exec-v4-a2", Version: 4, Attempt: 2, StepSeq: 3, GroupID: "work", NodeName: "n", ReplicaID: 7, Kind: topology.StepCleanupReplicaState}, ProposalID: "cleanup"},
		RecordCleanupStatusProposal{Identity: id, MemberID: 2, RetirementKey: key, NotBefore: &at, Blocked: &topology.BlockedReason{Code: "retention"}, At: at, ProposalID: "status"},
	} {
		t.Run(reflect.TypeOf(want).Name(), func(t *testing.T) {
			t.Parallel()
			data, err := json.Marshal(want)
			if err != nil {
				t.Fatal(err)
			}
			into := reflect.New(reflect.TypeOf(want)).Interface()
			if err := json.Unmarshal(data, into); err != nil || !reflect.DeepEqual(want, reflect.ValueOf(into).Elem().Interface()) {
				t.Fatal("payload lost exact tuple or correlation", string(data), into, err)
			}
			assertScopedPayloadRefusals(t, data, into)
			if _, err := Encode(want); !errors.Is(err, fault.ErrUnsupported) {
				t.Fatal("incomplete scoped persistence was enabled", err)
			}
		})
	}
}

func assertScopedPayloadRefusals(t *testing.T, data []byte, into any) {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"proposal_id", "class", "plan", "required"} {
		changed := make(map[string]json.RawMessage, len(fields)+1)
		for key, value := range fields {
			changed[key] = value
		}
		switch field {
		case "proposal_id":
			changed[field] = json.RawMessage(`" "`)
		case "class":
			changed[field] = json.RawMessage(`"cleanup"`)
		case "plan":
			if _, exists := fields[field]; exists {
				continue
			}
			changed[field] = json.RawMessage(`{}`)
		case "required":
			for key := range changed {
				delete(changed, key)
			}
			changed["proposal_id"] = fields["proposal_id"]
		}
		bad, err := json.Marshal(changed)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(bad, into); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal("accepted malformed payload", string(bad), err)
		}
	}
}

func TestScopedFieldsCannotReachLegacyMutationHandlers(t *testing.T) {
	t.Parallel()
	m := convergedAuthorityFixture(t)
	before := m.latest.Clone()
	for i, proposal := range []any{
		ProposeDocument{ExpectedVersion: before.Version, Doc: before, Intent: &Intent{Kind: "add_node"}, ProposalID: "intent"},
		CommitVoterChangeProposal{ExpectedVersion: before.Version, ExpectedTarget: before.Version, ProposalID: "voter"},
	} {
		result := apply(t, m.topologySM, uint64(i+6), proposal)
		if result.Accepted || result.Code != proposalUnsupported || result.ProposalID == "" || !reflect.DeepEqual(m.latest, before) {
			t.Fatal("legacy FSM discarded new authority fields", result)
		}
	}
	for _, kind := range []ProposalKind{"begin_operation", "retry_operation", "transition_operation", "commit_repair_operation", "record_collection", "begin_cleanup_effect", "record_cleanup_status"} {
		if _, err := m.applyProposal(10, kind, json.RawMessage(`{}`)); !errors.Is(err, fault.ErrUnsupported) {
			t.Fatal("unimplemented command dispatched", kind, err)
		}
	}
}

func TestScopedQueryShapesCannotBeIgnored(t *testing.T) {
	t.Parallel()
	m := NewMachine(testRules(), nil)
	for _, input := range []string{
		`{"kind":"progress"}`, `{"kind":"progress","progress":null}`, `{"kind":"progress","progress":{"limit":-1}}`,
		`{"kind":"topology","progress":{}}`, `{"kind":"collection_check"}`, `{"kind":"collection_check","collection_identity":{}}`,
		`{"kind":"progress","progress":{},"operator_id":"exec-v1-a1"}`, `{"kind":"progress","progress":{"class":"cleanup"}}`,
		`{"kind":"topology","collection_identity":{"cluster_id":"c","group_id":"g","node_name":"n","replica_id":1}}`,
	} {
		if _, err := m.Query(t.Context(), []byte(input)); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal(input, err)
		}
	}
	for _, input := range []string{`{"kind":"authority","operation_id":"exec-v1-a1"}`, `{"kind":"collection_check","collection_identity":{"cluster_id":"c","group_id":"g","node_name":"n","replica_id":1}}`} {
		if _, err := m.Query(t.Context(), []byte(input)); !errors.Is(err, fault.ErrUnsupported) {
			t.Fatal("unimplemented read returned fabricated progress", input, err)
		}
	}
}
