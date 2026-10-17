//go:build !functional

package topology

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
)

func TestProgressQueryValidation(t *testing.T) {
	t.Parallel()
	for _, limit := range []int{0, 1, defaultProgressLimit, maxProgressLimit} {
		if err := (ProgressQuery{Limit: limit, GroupID: MetaGroupID, NodeName: "n"}).Validate(); err != nil {
			t.Fatal(limit, err)
		}
	}
	for _, query := range []ProgressQuery{
		{Limit: -1}, {Limit: maxProgressLimit + 1}, {GroupID: "../elsewhere"},
		{NodeName: " "}, {NodeName: "n\x00"}, {OperationID: " "}, {OperationID: "exec\x00"},
	} {
		if err := query.Validate(); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal("invalid filter/limit accepted", query, err)
		}
	}
}

func TestProgressCloneOwnsEveryRow(t *testing.T) {
	t.Parallel()
	at := time.Unix(10, 0).UTC()
	group := Group{ID: "work", Labels: map[string]string{"a": "b"}, Replicas: []Replica{{ReplicaID: 1}},
		Placement: &PlacementRule{NodeSelector: map[string]string{"zone": "a"}},
		Hosting:   &HostingRule{Nodes: []string{"n"}}, VoterConfig: &VoterConfig{}}
	node := Node{Name: "n", Labels: map[string]string{"zone": "a"}}
	blocked := &BlockedReason{Code: BlockedDependency}
	page := ProgressPage{AppliedIndex: 4, ReadAt: at,
		Groups: []GroupProgress{{GroupID: "work", Desired: &group, Approved: &group, Converged: &group,
			Blocked: blocked, Observation: &GroupStatus{Reporter: Identity{MemberID: 1, InstanceID: "instance"},
				ReporterReplicaID: 7, ReceivedAt: at, SourceVersion: 2, LiveVoters: map[uint64]bool{1: true}}}},
		Nodes:      []NodeProgress{{NodeName: "n", Desired: &node, Approved: &node, Converged: &node, Blocked: blocked}},
		Operations: []OperationStatus{{Blocks: []BlockStatus{{Blocked: blocked}}}},
		Cleanup: []CleanupProgress{{RemovedAt: &at, StoppedAt: &at, RetiredAt: &at, CollectedAt: &at,
			NotBefore: &at, Blocked: blocked}},
	}
	before := page.Clone()
	owned := page.Clone()
	if !reflect.DeepEqual(page, owned) {
		t.Fatal("clone changed source evidence")
	}
	for _, declaration := range []*Group{owned.Groups[0].Desired, owned.Groups[0].Approved, owned.Groups[0].Converged} {
		declaration.Labels["a"], declaration.Placement.NodeSelector["zone"] = "changed", "changed"
		declaration.Replicas[0].ReplicaID++
		declaration.Hosting.Nodes[0] = "changed"
	}
	for _, declaration := range []*Node{owned.Nodes[0].Desired, owned.Nodes[0].Approved, owned.Nodes[0].Converged} {
		declaration.Labels["zone"] = "changed"
	}
	owned.Groups[0].Observation.LiveVoters[1] = false
	owned.Groups[0].Observation.Reporter.MemberID++
	owned.Groups[0].Observation.ReporterReplicaID++
	for _, reason := range []*BlockedReason{owned.Groups[0].Blocked, owned.Nodes[0].Blocked,
		owned.Operations[0].Blocks[0].Blocked, owned.Cleanup[0].Blocked} {
		reason.Code = BlockedUnconfirmed
	}
	cleanup := owned.Cleanup[0]
	for _, timestamp := range []*time.Time{cleanup.RemovedAt, cleanup.StoppedAt, cleanup.RetiredAt,
		cleanup.CollectedAt, cleanup.NotBefore} {
		*timestamp = at.Add(time.Hour)
	}
	if !reflect.DeepEqual(page, before) || !reflect.DeepEqual((ProgressPage{}).Clone(), ProgressPage{}) {
		t.Fatal("clone shares mutable evidence or fabricates absent evidence")
	}
}
