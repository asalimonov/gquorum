//go:build !functional

package metaengine

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestMetadataQueriesReturnOwnedAppliedState(t *testing.T) {
	t.Parallel()
	m := NewMachine(testRules(), nil)
	m.latest = topology.Document{Version: 3, Nodes: []topology.Node{{Name: "n1", Labels: map[string]string{"zone": "a"}}}}
	m.target = topology.Document{Version: 2}
	m.active = topology.Document{Version: 1}
	apply(t, m.topologySM, 8, GrantSession{SessionID: "s", NodeName: "n1", InstanceID: "i", TTLSeconds: 3})
	apply(t, m.topologySM, 9, AcquireLock{Key: "key", SessionID: "s"})
	r, err := m.Query(t.Context(), []byte(`{"kind":"topology"}`))
	if err != nil {
		t.Fatal(err)
	}
	var view topology.ReadView
	if err := json.Unmarshal(r.Data, &view); err != nil {
		t.Fatal(err)
	}
	if view.AppliedIndex != 9 || view.Latest.Version != 3 || view.Target.Version != 2 || view.Active.Version != 1 {
		t.Fatal(view)
	}
	view.Latest.Nodes[0].Labels["zone"] = "changed"
	if m.latest.Nodes[0].Labels["zone"] != "a" {
		t.Fatal("aliased query result")
	}
	r, err = m.Query(t.Context(), []byte(`{"kind":"lock_holder","key":"key"}`))
	if err != nil {
		t.Fatal(err)
	}
	var held LockRead
	if err := json.Unmarshal(r.Data, &held); err != nil {
		t.Fatal(err)
	}
	if !held.Held || held.Session.ID != "s" || held.Session.TTL != 3*time.Second {
		t.Fatal(held)
	}
	apply(t, m.topologySM, 10, RevokeSession{SessionID: "s"})
	r, err = m.Query(t.Context(), []byte(`{"kind":"lock_holder","key":"key"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(r.Data, &held); err != nil || held.Held {
		t.Fatal(held, err)
	}
}
func TestMetadataQueryProtocolRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	m := NewMachine(testRules(), nil)
	for _, input := range []string{`{`, `{"kind":"topology"} {}`, `{"kind":"topology","extra":1}`, `{"kind":"topology","key":"x"}`, `{"kind":"lock_holder"}`} {
		if _, err := m.Query(t.Context(), []byte(input)); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal(input, err)
		}
	}
	if _, err := m.Query(t.Context(), []byte(`{"kind":"future"}`)); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal(err)
	}
	m.locks["broken"] = "absent"
	if _, err := m.Query(t.Context(), []byte(`{"kind":"lock_holder","key":"broken"}`)); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal(err)
	}
}

func TestGrantQueryReturnsExactHistoricalRecordAndRejectsFutureKey(t *testing.T) {
	t.Parallel()
	m, action := authorityFixture(t, 32)
	grant := apply(t, m.topologySM, 4, BeginEffectProposal{Action: action}).Grant
	if grant == nil {
		t.Fatal("grant not committed")
	}
	_ = supersedeAuthority(t, m, 5)
	query := func(key topology.AuthorityKey) *topology.AuthorityGrant {
		t.Helper()
		data, err := json.Marshal(QueryRequest{Kind: "authority_grant", AuthorityKey: &key})
		if err != nil {
			t.Fatal(err)
		}
		result, err := m.Query(t.Context(), data)
		if err != nil {
			t.Fatal(err)
		}
		var found *topology.AuthorityGrant
		if err := json.Unmarshal(result.Data, &found); err != nil {
			t.Fatal(err)
		}
		return found
	}
	if got := query(grant.Key); got == nil || !got.Equal(*grant) {
		t.Fatal("supersession erased immutable grant identity", got)
	}
	future := grant.Key
	future.Revision += 100
	if got := query(future); got != nil {
		t.Fatal("future grant inferred from approved intent", got)
	}
	for _, q := range []QueryRequest{
		{Kind: "authority_grant"},
		{Kind: "authority_grant", AuthorityKey: &grant.Key, Key: "unexpected"},
		{Kind: "authority_grant", AuthorityKey: &grant.Key, OperatorID: action.OperatorID},
		{Kind: "topology", AuthorityKey: &grant.Key},
	} {
		data, err := json.Marshal(q)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := m.Query(t.Context(), data); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal(q, err)
		}
	}
}

func TestAuthorityProofCapturesGrantReceiptsAndCurrentCourier(t *testing.T) {
	t.Parallel()
	m, action := authorityFixture(t, 1024)
	grant := apply(t, m.topologySM, 4, BeginEffectProposal{Action: action}).Grant
	m.active = m.target.Clone()
	apply(t, m.topologySM, 5, GrantSession{SessionID: "courier", NodeName: "n1", InstanceID: "current", TTLSeconds: 10})
	apply(t, m.topologySM, 6, AcquireLock{Key: "node/n1", SessionID: "courier"})
	receipt := groupEffectProof(*grant, 10)
	if r := apply(t, m.topologySM, 7, RecordEffectProposal{Receipt: receipt}); !r.Accepted {
		t.Fatal(r)
	}
	data, err := json.Marshal(QueryRequest{Kind: "authority_proof", AuthorityKey: &grant.Key, Key: "n1"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := m.Query(t.Context(), data)
	if err != nil {
		t.Fatal(err)
	}
	var proof AuthorityProof
	if err = json.Unmarshal(result.Data, &proof); err != nil {
		t.Fatal(err)
	}
	if proof.Grant == nil || !proof.Grant.Equal(*grant) || len(proof.Receipts) != 1 || proof.Courier != (topology.Identity{ClusterID: grant.Key.ClusterID, NodeName: "n1", MemberID: 1, InstanceID: "current"}) {
		t.Fatal(proof)
	}
	proof.Grant.Action.NodeName = "tampered"
	proof.Receipts[0].Action.NodeName = "tampered"
	if err = m.ValidateGrant(t.Context(), *grant); err != nil {
		t.Fatal("query result aliases catalog", err)
	}
	apply(t, m.topologySM, 8, RevokeSession{SessionID: "courier"})
	result, err = m.Query(t.Context(), data)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(result.Data, &proof); err != nil || proof.Courier.InstanceID != "" {
		t.Fatal("revoked courier stayed authorized", proof, err)
	}
	for _, q := range []QueryRequest{{Kind: "authority_proof"}, {Kind: "authority_proof", AuthorityKey: &grant.Key, OperatorID: action.OperatorID}} {
		data, _ := json.Marshal(q)
		if _, err := m.Query(t.Context(), data); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal(q, err)
		}
	}
}
