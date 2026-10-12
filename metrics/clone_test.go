//go:build !functional

package metrics

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSnapshotClonesEveryNestedSlice(t *testing.T) {
	t.Parallel()
	r := ReplicaObservation{Sample: ReplicaSample{Voters: []uint64{1}, Learners: []uint64{2}, Progress: []PeerProgress{{ReplicaID: 2}}}, Evidence: Evidence{Reasons: []string{"standby_window"}}}
	s := ClusterSnapshot{Local: []ReplicaObservation{r}, Groups: []GroupObservation{{Replicas: []ReplicaObservation{r}}}}
	x := s.Clone()
	x.Local[0].Sample.Voters[0] = 99
	x.Groups[0].Replicas[0].Sample.Learners[0] = 99
	x.Groups[0].Replicas[0].Sample.Progress[0].ReplicaID = 99
	x.Groups[0].Replicas[0].Evidence.Reasons[0] = "changed"
	if s.Local[0].Sample.Voters[0] != 1 || s.Groups[0].Replicas[0].Sample.Learners[0] != 2 || s.Groups[0].Replicas[0].Sample.Progress[0].ReplicaID != 2 || s.Groups[0].Replicas[0].Evidence.Reasons[0] != "standby_window" {
		t.Fatal("clone shares nested data")
	}
	zero := (ClusterSnapshot{Local: []ReplicaObservation{{}}}).Clone()
	b, err := json.Marshal(zero)
	if err != nil || strings.Contains(string(b), "null") || strings.Contains(string(b), `"state":""`) {
		t.Fatal(string(b), err)
	}
}
