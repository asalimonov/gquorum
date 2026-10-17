//go:build !functional

package metaservice

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/asalimonov/gquorum/topology"
)

func TestAuthorizationTupleUsesOperationIdentity(t *testing.T) {
	t.Parallel()
	a := topology.AuthorizedStep{OperationID: "exec-v4-a2", Version: 4, Attempt: 2, StepSeq: 3, Kind: topology.StepAddLearner, GroupID: topology.MetaGroupID, NodeName: "n2", ReplicaID: 7,
		ExpectedVotersDigest: topology.VoterSetDigest([]uint64{1}), ResultVotersDigest: topology.VoterSetDigest([]uint64{1})}
	encoded, err := json.Marshal(a)
	if err != nil || !bytes.Contains(encoded, []byte(`"operation_id"`)) || bytes.Contains(encoded, []byte(`"operator_id"`)) || bytes.Contains(encoded, []byte(`"execution_id"`)) {
		t.Fatalf("unexpected authorization vocabulary: %s/%v", encoded, err)
	}
	var read topology.AuthorizedStep
	if err := json.Unmarshal(encoded, &read); err != nil || read != a {
		t.Fatalf("authorization lost exact scope or voter proof: %+v/%v", read, err)
	}
	step := topology.Step{Seq: a.StepSeq, Kind: a.Kind, GroupID: a.GroupID, NodeName: a.NodeName, ReplicaID: a.ReplicaID,
		ExpectedVotersDigest: a.ExpectedVotersDigest, ResultVotersDigest: a.ResultVotersDigest}
	if !read.Matches(a.OperationID, a.Attempt, a.Version, step) || read.Matches("another-operation", a.Attempt, a.Version, step) {
		t.Fatal("authorization does not bind the complete operation tuple")
	}
}

func TestJoinReadyKeepsLegacyReplyKey(t *testing.T) {
	t.Parallel()
	legacy := []byte(`{"NodeName":"n1","InstanceID":"process","Address":"127.0.0.1:1","MemberID":1,"ExecutionID":"exec-v1-a1"}`)
	var request readyRequest
	if err := json.Unmarshal(legacy, &request); err != nil {
		t.Fatal(err)
	}
	if request.OperationID != "exec-v1-a1" || request.MemberID != 1 {
		t.Fatal("join readiness lost approval identity", request)
	}
	got, err := json.Marshal(request)
	if err != nil || !bytes.Equal(got, legacy) {
		t.Fatalf("join readiness encoding changed: %s/%v", got, err)
	}
}
