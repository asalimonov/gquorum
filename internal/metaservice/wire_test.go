//go:build !functional

package metaservice

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/asalimonov/gquorum/topology"
)

func TestMetaControlKeepsLegacyAuthorizationTuple(t *testing.T) {
	t.Parallel()
	a := topology.AuthorizedStep{OperatorID: "exec-v4-a2", Version: 4, Attempt: 2, StepSeq: 3, Kind: topology.StepAddLearner, GroupID: topology.MetaGroupID, NodeName: "n2", ReplicaID: 7}
	legacy := []byte(`{"execution_id":"exec-v4-a2","version":4,"attempt":2,"step_seq":3,"kind":"add_learner","group_id":"_meta","node_name":"n2","replica_id":7}`)
	got, err := json.Marshal(toWireAuthorization(a))
	if err != nil || !bytes.Equal(got, legacy) {
		t.Fatalf("meta authorization changed: %s/%v", got, err)
	}
	var read wireAuthorization
	if err := json.Unmarshal(legacy, &read); err != nil {
		t.Fatal(err)
	}
	if read.public() != a || !read.public().Matches(a.OperatorID, a.Attempt, a.Version, topology.Step{Seq: a.StepSeq, Kind: a.Kind, GroupID: a.GroupID, NodeName: a.NodeName, ReplicaID: a.ReplicaID}) {
		t.Fatal("wire conversion lost exact authorization scope")
	}
	guarded := a
	guarded.ExpectedVotersDigest = topology.VoterSetDigest([]uint64{1})
	guarded.ResultVotersDigest = topology.VoterSetDigest([]uint64{1})
	encoded, err := json.Marshal(toWireAuthorization(guarded))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &read); err != nil || read.public() != guarded {
		t.Fatalf("wire conversion lost voter proof: %s %v", encoded, err)
	}
	public, err := json.Marshal(a)
	if err != nil || !bytes.Contains(public, []byte(`"operator_id"`)) || bytes.Contains(public, []byte(`"execution_id"`)) {
		t.Fatalf("public authorization leaked stored vocabulary: %s/%v", public, err)
	}
}

func TestJoinReadyKeepsLegacyReplyKey(t *testing.T) {
	t.Parallel()
	legacy := []byte(`{"NodeName":"n1","InstanceID":"process","Address":"127.0.0.1:1","MemberID":1,"ExecutionID":"exec-v1-a1"}`)
	var request readyRequest
	if err := json.Unmarshal(legacy, &request); err != nil {
		t.Fatal(err)
	}
	if request.OperatorID != "exec-v1-a1" || request.MemberID != 1 {
		t.Fatal("join readiness lost approval identity", request)
	}
	got, err := json.Marshal(request)
	if err != nil || !bytes.Equal(got, legacy) {
		t.Fatalf("join readiness encoding changed: %s/%v", got, err)
	}
}
