//go:build !functional

package metaengine

import (
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestIntentFactoryCanonicalMapsButPreservesSequences(t *testing.T) {
	t.Parallel()
	a, err := NewIntent("add_node", map[string]any{"a": 1, "b": []int{1, 2}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewIntent("add_node", map[string]any{"b": []int{1, 2}, "a": 1})
	if err != nil || a != b || !validIntentFingerprint(a) {
		t.Fatal(a, b, err)
	}
	c, err := NewIntent("add_node", map[string]any{"a": 1, "b": []int{2, 1}})
	if err != nil || a == c {
		t.Fatal("sequence order lost", err)
	}
	for _, kind := range []string{"", "arbitrary_request"} {
		if _, err := NewIntent(kind, struct{}{}); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal(err)
		}
	}
	if _, err := NewIntent("add_node", nil); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := NewIntent("add_node", make(chan int)); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
}

func TestRetainedMutationLostResponseSurvivesRebaseWithoutAllocation(t *testing.T) {
	t.Parallel()
	s, repair := rebaseFixture(t)
	intent, err := NewIntent("set_group_placement", map[string]string{"purpose": "retain"})
	if err != nil {
		t.Fatal(err)
	}
	intent.Version, intent.ExpectedVersion, intent.GroupID = 2, 1, "shard0"
	s.Intents = []Intent{intent}
	prepareScopedFixture(t, &s)
	pair, err := s.prepareRepairDocuments(repair, s.Latest.CreatedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	s.installRepairDocuments(pair)
	prepareScopedFixture(t, &s)
	before, _ := json.Marshal(s)
	request := intent
	request.Version, request.ExpectedVersion = 0, 0
	for range 2 {
		got, err := retainedMutation(ProposeDocument{ExpectedVersion: 1, Intent: &request}, s.Latest.Version, s.indexes)
		if err != nil || !got.Accepted || got.Doc.Version != 2 || got.RebasedTo != 4 || got.Doc.Group("b").Replicas[0].ReplicaID != 1 {
			t.Fatal("replay lost original acceptance", got, err)
		}
		got.Doc.Node("n3").Labels["modified"] = "caller"
	}
	after, _ := json.Marshal(s)
	if string(before) != string(after) {
		t.Fatal("replay allocated, changed lineage or exposed mutable state")
	}
	changed := request
	changed.Digest = string(make([]byte, 64))
	if _, err := retainedMutation(ProposeDocument{ExpectedVersion: 1, Intent: &changed}, 4, s.indexes); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	changed = request
	changed.NodeName = "other"
	if _, err := retainedMutation(ProposeDocument{ExpectedVersion: 1, Intent: &changed}, 4, s.indexes); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal(err)
	}
	changed = request
	changed.PreviousID = 19
	if _, err := retainedMutation(ProposeDocument{ExpectedVersion: 1, Intent: &changed}, 4, s.indexes); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal(err)
	}
	changed = request
	changed.SuccessorID = 20
	if _, err := retainedMutation(ProposeDocument{ExpectedVersion: 1, Intent: &changed}, 4, s.indexes); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal(err)
	}
	delete(s.indexes.intents, intentKey{1, request.Kind, request.Digest})
	if _, err := retainedMutation(ProposeDocument{ExpectedVersion: 1, Intent: &request}, 4, s.indexes); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal(err)
	}
	delete(s.indexes.documents, 2)
	if _, err := retainedMutation(ProposeDocument{ExpectedVersion: 1, Intent: &request}, 4, s.indexes); !errors.Is(err, fault.ErrHistoryUnavailable) {
		t.Fatal(err)
	}
}

func TestRetainedRawStageComparesFullCanonicalContent(t *testing.T) {
	t.Parallel()
	s, _ := rebaseFixture(t)
	accepted := s.Latest.Clone()
	request := accepted.Clone()
	request.Version = 1
	slices.Reverse(request.Nodes)
	slices.Reverse(request.Groups)
	for i := range request.Groups {
		slices.Reverse(request.Groups[i].Replicas)
	}
	before := request.Clone()
	got, err := retainedMutation(ProposeDocument{Doc: request, ExpectedVersion: 1}, 2, s.indexes)
	if err != nil || !got.Accepted || !reflect.DeepEqual(got.Doc, accepted) || !reflect.DeepEqual(request, before) {
		t.Fatal("canonical replay failed or mutated input", got, err)
	}
	for name, edit := range map[string]func(*topology.Document){
		"allocator": func(d *topology.Document) { d.Group("b").NextReplicaID++ },
		"identity":  func(d *topology.Document) { d.Group("b").Replicas[0].ReplicaID++ },
		"audit":     func(d *topology.Document) { d.Audit.Reason = "different" },
		"comment":   func(d *topology.Document) { d.Comment = "different" },
		"clock":     func(d *topology.Document) { d.CreatedAt = d.CreatedAt.Add(time.Second) },
		"parent":    func(d *topology.Document) { d.ParentVersion = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			doc := request.Clone()
			edit(&doc)
			_, err := retainedMutation(ProposeDocument{Doc: doc, ExpectedVersion: 1}, 2, s.indexes)
			if !errors.Is(err, fault.ErrVersionConflict) {
				t.Fatal(err)
			}
		})
	}
}
