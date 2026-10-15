//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// Documents/attempts are valid; convergence and authority proof validation follows in Tasks 11–14.
func scopedCodecFixture() scopedState {
	at := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	id := storage.Identity{ClusterID: "c", GroupID: "a", NodeName: "n", ReplicaID: 7}
	key := topology.AuthorityKey{ClusterID: "c", GroupID: "a", Revision: 8}
	base := baseDoc()
	base.ClusterID, base.Version = "c", 1
	base.Nodes[0].Name = "n"
	for i := range base.Groups {
		base.Groups[i].Replicas[0].NodeName = "n"
	}
	base.Groups[1].ID = "a"
	base.Groups[1].Replicas[0].ReplicaID, base.Groups[1].NextReplicaID = 7, 8
	live := base.Clone().Groups[1]
	live.ID, live.NextReplicaID, live.Replicas[0].ReplicaID = "live", 2, 1
	base.Groups = append(base.Groups, live)
	removed := base.Clone().Groups[1]
	removed.ID, removed.NextReplicaID, removed.Replicas[0].ReplicaID = "b", 9, 8
	removed.Storage = topology.StorageMemory
	base.Groups = append(base.Groups, removed)
	successor := base.Clone()
	successor.Groups = successor.Groups[:len(successor.Groups)-1]
	successor.Group("a").Replicas[0].ReplicaID, successor.Group("a").NextReplicaID = 8, 9
	original, approved, staged := successor.Clone(), successor.Clone(), successor.Clone()
	target, latest := successor.Clone(), successor.Clone()
	original.Version, original.ParentVersion = 2, 1
	approved.Version, approved.ParentVersion = 3, 2
	staged.Version, staged.ParentVersion = 4, 3
	target.Version, target.ParentVersion = 5, 3
	latest.Version, latest.ParentVersion, latest.RebasedFrom = 6, 5, 4
	latest.MetaGroup().NextReplicaID = 4
	latest.Group("a").NextReplicaID = 9
	state := scopedState{
		SchemaVersion: scopedStateSchemaVersion, Rules: testRules(), AppliedIndex: 30, AppliedTerm: 4,
		Latest: latest, Target: target,
		History:        []topology.Document{original, base, approved, staged},
		Operations:     []topology.Operation{{Status: topology.OperationStatus{ID: "exec-v3-a1", Version: 3, Attempt: 1, State: topology.OperationFailed, TotalBlocks: 1, Blocks: []topology.BlockStatus{{GroupID: "a", State: topology.OperationFailed, CurrentStep: 1, TotalSteps: 1}}}, Plan: topology.Plan{Version: 3, Steps: []topology.Step{{Seq: 1, Kind: topology.StepTransferLeader, GroupID: "a", NodeName: "n", ReplicaID: 8}}, Blocks: []topology.PlanBlock{{GroupID: "a", SourceVersion: 1, Steps: []int{1}}}}, StartedAt: at, UpdatedAt: at}, {Status: topology.OperationStatus{ID: "exec-v1-a1", Version: 1, Attempt: 1, State: topology.OperationSucceeded}, Plan: topology.Plan{Version: 1}, StartedAt: at, UpdatedAt: at}},
		Groups:         []groupConvergence{{GroupID: "live", SourceVersion: 1, Declaration: base.Group("live"), CompletedAt: at}, {GroupID: "never"}, {GroupID: "a", SourceVersion: 1, Declaration: base.Group("a"), CompletedAt: at}, {GroupID: "tombstone", SourceVersion: 2, CompletedAt: at}, {GroupID: topology.MetaGroupID, SourceVersion: 1, Declaration: base.MetaGroup(), CompletedAt: at}},
		Nodes:          []nodeConvergence{{NodeName: "b", SourceVersion: 2, MemberID: 0, CompletedAt: at}, {NodeName: "n", Declaration: base.Node("n"), SourceVersion: 1, MemberID: 1, CompletedAt: at}},
		Intents:        []intentRecord{{Version: 3, ExpectedVersion: 2, Kind: "delete_group", Digest: strings.Repeat("0", 64), GroupID: "b"}, {Version: 2, ExpectedVersion: 1, Kind: "set_group_placement", Digest: strings.Repeat("0", 64), GroupID: "a"}},
		FullyConverged: topology.FullyConverged{Version: 1, At: at},
		Revocations:    []scopedRevocationRecord{{Requested: true, OperationID: "exec-v3-a1", GroupID: "a"}},
		RetiredGroups:  []retiredGroup{{ID: "b", Version: 2}, {ID: "tombstone", Version: 2}},
		Allocations:    []allocationRecord{{GroupID: "b", NextReplicaID: 9}, {GroupID: "a", NextReplicaID: 9}, {GroupID: "live", NextReplicaID: 2}, {GroupID: topology.MetaGroupID, NextReplicaID: 4}},
		Admissions:     []AdmissionRecord{{NodeName: "b", InstanceID: "i", MemberID: 2, OperatorID: "exec-v3-a1"}, {NodeName: "a", InstanceID: "i", MemberID: 1}},
		Sessions:       []sessionState{{SessionID: "b", NodeName: "b", InstanceID: "i", TTLSeconds: 30}, {SessionID: "a", NodeName: "a", InstanceID: "i", TTLSeconds: 30}},
		Locks:          []lockEntry{{Key: "b", SessionID: "b"}, {Key: "a", SessionID: "a"}}, Activated: []string{"b", "a"},
		VoterSwitches: []topology.VoterSwitchState{{GroupID: "b"}, {GroupID: "a"}},
	}
	populateScopedCodecAuthority(&state, id, key, at)
	retainScopedCleanupOrigin(&state, at)
	return state
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
	if got.Scoped.Latest.RebasedFrom != 4 || got.Scoped.Cleanup[1].Collection != nil {
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
	if !result.Accepted || result.Doc.RebasedFrom != 0 || m.latest.RebasedFrom != 0 {
		t.Fatal("ordinary staging failed to normalize generated rebase provenance", result)
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
