//go:build !functional

package metaengine

import (
	"bytes"
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

// This is codec data, not a valid restore graph; Tasks 10–11 validate graph/proof semantics.
func scopedCodecFixture() scopedState {
	at := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	id := storage.Identity{ClusterID: "c", GroupID: "a", NodeName: "n", ReplicaID: 7}
	key := topology.AuthorityKey{ClusterID: "c", GroupID: "a", Revision: 8}
	return scopedState{
		SchemaVersion: scopedStateSchemaVersion, Rules: testRules(), AppliedIndex: 30, AppliedTerm: 4,
		Latest: topology.Document{Version: 4, ParentVersion: 3, RebasedFrom: 2}, Target: topology.Document{Version: 3},
		History:          []topology.Document{{Version: 2}, {Version: 1}},
		Operations:       []topology.Operation{{Status: topology.OperationStatus{ID: "exec-v3-a1", Version: 3, Attempt: 1, State: topology.OperationFailed, Blocks: []topology.BlockStatus{{GroupID: "a", State: topology.OperationFailed}}}, Plan: topology.Plan{Version: 3, Blocks: []topology.PlanBlock{{GroupID: "a", SourceVersion: 1}}}}, {Status: topology.OperationStatus{ID: "exec-v1-a1", Version: 1, Attempt: 1, State: topology.OperationSucceeded}, Plan: topology.Plan{Version: 1}}},
		Groups:           []groupConvergence{{GroupID: "live", SourceVersion: 1, Declaration: &topology.Group{ID: "live"}, CompletedAt: at}, {GroupID: "never"}, {GroupID: "tombstone", SourceVersion: 2, CompletedAt: at}},
		Nodes:            []nodeConvergence{{NodeName: "b", SourceVersion: 2, MemberID: 2, CompletedAt: at}, {NodeName: "a", Declaration: &topology.Node{Name: "a"}, SourceVersion: 1, MemberID: 1, CompletedAt: at}},
		Cleanup:          []cleanupRecord{{Identity: id, MemberID: 2, Storage: topology.StorageDefault, OperationID: "exec-v3-a1", Retired: &topology.EffectReceipt{Key: key}, RetiredAt: &at, Collection: &authoritystore.CollectionReceipt{Identity: id, MemberID: 2, RetirementKey: key, Token: "opaque", LocalSequence: 9, CollectedAt: at}, CreatedAt: at, UpdatedAt: at}, {Identity: storage.Identity{ClusterID: "c", GroupID: "b", NodeName: "n", ReplicaID: 8}, Storage: topology.StorageMemory, RetiredAt: &at}},
		Intents:          []intentRecord{{Version: 4, ExpectedVersion: 3, Kind: "add_node", Digest: "digest", NodeName: "b", SuccessorID: 2}, {Version: 2, ExpectedVersion: 1, Kind: "delete_group", GroupID: "a"}},
		FullyConverged:   topology.FullyConverged{Version: 1, At: at},
		AuthorityRecords: []scopedAuthorityRecord{{authorityRecord: authorityRecord{Grant: topology.AuthorityGrant{Key: key}, Closed: true}, Class: "cleanup"}, {authorityRecord: authorityRecord{Grant: topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: "c", GroupID: "b", Revision: 9}}}, Class: "execution"}},
		AuthorityFloors:  []resourceFloor{{Resource: topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: "a", NodeName: "n"}, Key: key}, {Resource: topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: "a"}, Key: key}},
		Revocations:      []scopedRevocationRecord{{Requested: true, OperationID: "exec-v3-a1", GroupID: "b"}, {Requested: true, OperationID: "exec-v3-a1", GroupID: "a"}},
		RetiredGroups:    []retiredGroup{{ID: "b", Version: 2}, {ID: "a", Version: 1}},
		Allocations:      []allocationRecord{{GroupID: "b", NextReplicaID: 8}, {GroupID: "a", NextReplicaID: 7}},
		Admissions:       []AdmissionRecord{{NodeName: "b", InstanceID: "i", MemberID: 2, OperatorID: "exec-v3-a1"}, {NodeName: "a", InstanceID: "i", MemberID: 1}},
		Sessions:         []sessionState{{SessionID: "b", NodeName: "b", InstanceID: "i", TTLSeconds: 30}, {SessionID: "a", NodeName: "a", InstanceID: "i", TTLSeconds: 30}},
		Locks:            []lockEntry{{Key: "b", SessionID: "b"}, {Key: "a", SessionID: "a"}}, Activated: []string{"b", "a"},
		VoterSwitches: []topology.VoterSwitchState{{GroupID: "b"}, {GroupID: "a"}},
	}
}

func TestScopedStateCanonicalRoundTripAndExactShape(t *testing.T) {
	t.Parallel()
	want := scopedCodecFixture()
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, scopedCodecFixture()) {
		t.Fatal("serialization sorted caller-owned slices")
	}
	var got scopedState
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	fields := reflect.ValueOf(&got).Elem()
	for i := range fields.NumField() {
		field := fields.Field(i)
		if field.Kind() == reflect.Slice && field.Len() > 1 {
			reflect.Swapper(field.Interface())(0, field.Len()-1)
		}
	}
	roundtrip, err := json.Marshal(got)
	if err != nil || !bytes.Equal(data, roundtrip) {
		t.Fatal("unordered snapshot arrays changed bytes", err)
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatal(err)
	}
	keys := []string{"schema_version", "rules", "latest", "target", "history", "operations", "group_convergence", "node_convergence", "cleanup", "intents", "fully_converged", "authority_records", "authority_floors", "revocations", "retired_groups", "allocations", "admissions", "sessions", "locks", "activated", "voter_switches", "applied_index", "applied_term"}
	if len(root) != len(keys) {
		t.Fatal("unexpected v4 fields", string(data))
	}
	for _, key := range keys {
		if _, found := root[key]; !found {
			t.Fatal("missing v4 field", key)
		}
	}
	if !bytes.Contains(root["revocations"], []byte(`"operation_id"`)) || bytes.Contains(root["revocations"], []byte(`"operator_id"`)) || !bytes.Contains(root["authority_records"], []byte(`"grant"`)) {
		t.Fatal("authority wrapper leaked or legacy revocation names survived", string(data))
	}
}

func TestScopedStateRejectsDuplicateKeysAtomically(t *testing.T) {
	t.Parallel()
	fixture := scopedCodecFixture()
	fields := reflect.ValueOf(fixture)
	for i := range fields.NumField() {
		if fields.Field(i).Kind() != reflect.Slice {
			continue
		}
		t.Run(fields.Type().Field(i).Name, func(t *testing.T) {
			t.Parallel()
			bad := scopedCodecFixture()
			field := reflect.ValueOf(&bad).Elem().Field(i)
			field.Set(reflect.Append(field, field.Index(0)))
			if _, err := json.Marshal(bad); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("duplicate serialized", err)
			}
			type wire scopedState
			data, err := json.Marshal(wire(bad))
			if err != nil {
				t.Fatal(err)
			}
			unchanged := scopedCodecFixture()
			if err := json.Unmarshal(data, &unchanged); !errors.Is(err, fault.ErrUnreadable) || !reflect.DeepEqual(unchanged, fixture) {
				t.Fatal("failed decode replaced retained evidence", err)
			}
		})
	}
}

func TestScopedStateTombstonesAndActivationBoundary(t *testing.T) {
	t.Parallel()
	fixture := scopedCodecFixture()
	data, err := json.Marshal(smState{SchemaVersion: scopedStateSchemaVersion, Scoped: &fixture})
	if err != nil {
		t.Fatal(err)
	}
	var got smState
	if err := json.Unmarshal(data, &got); err != nil || got.Scoped == nil {
		t.Fatal(err)
	}
	for _, group := range got.Scoped.Groups {
		if group.GroupID == "never" && (group.Declaration != nil || group.SourceVersion != 0 || !group.CompletedAt.IsZero()) || group.GroupID == "tombstone" && (group.Declaration != nil || group.SourceVersion != 2 || group.CompletedAt.IsZero()) {
			t.Fatal("nil declarations lost their distinct proof", group)
		}
	}
	if got.Scoped.Latest.RebasedFrom != 2 || got.Scoped.Cleanup[1].Collection != nil {
		t.Fatal("lost rebase provenance or fabricated memory collection")
	}
	m := convergedAuthorityFixture(t)
	before, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Restore(t.Context(), data); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal("enabled partial live v4 restoration", err)
	}
	after, err := m.Snapshot(t.Context())
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("refusal changed live state", err)
	}
	if _, err := json.Marshal(smState{SchemaVersion: metaStateSchemaVersion, Scoped: &fixture}); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("serialized scoped state with legacy schema", err)
	}
	doc := m.latest.Clone()
	doc.RebasedFrom = fixture.Latest.RebasedFrom
	result := apply(t, m.topologySM, 6, ProposeDocument{Doc: doc, ExpectedVersion: m.latest.Version})
	if result.Accepted || result.Code != proposalUnsupported || m.latest.RebasedFrom != 0 {
		t.Fatal("legacy staging accepted caller-selected rebase provenance", result)
	}
}

func TestScopedStateRefusesWrongFormatAndLegacyFields(t *testing.T) {
	t.Parallel()
	for _, input := range []string{`{"schema_version":4,"active":{}}`, `{"schema_version":4,"collection_acks":[]}`, `{"schema_version":4,"authority_records":[{"class":"other"}]}`} {
		var state scopedState
		if err := json.Unmarshal([]byte(input), &state); !errors.Is(err, fault.ErrUnreadable) {
			t.Fatal(input, err)
		}
	}
	var state scopedState
	if err := json.Unmarshal([]byte(`{"schema_version":3}`), &state); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal("old state treated as empty scoped progress", err)
	}
}
