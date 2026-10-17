//go:build !functional

package metaengine

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/topology"
)

func TestMetadataQueriesReturnOwnedAppliedState(t *testing.T) {
	t.Parallel()
	s, _ := rebaseFixture(t)
	m := mustMachine(t, s.Rules, nil)
	m.scoped = &s
	applyQueryCommand(t, m, GrantSession{SessionID: "s", NodeName: "n1", InstanceID: "i", TTLSeconds: 3})
	applyQueryCommand(t, m, AcquireLock{Key: "key", SessionID: "s"})
	r, err := m.Query(t.Context(), []byte(`{"kind":"topology"}`))
	if err != nil {
		t.Fatal(err)
	}
	var view topology.ReadView
	if err := json.Unmarshal(r.Data, &view); err != nil {
		t.Fatal(err)
	}
	if view.AppliedIndex != 12 || view.Latest.Version != 2 || view.Target.Version != 1 {
		t.Fatal(view)
	}
	view.Latest.Group("shard0").Labels["owner"] = "changed"
	if m.scoped.Latest.Group("shard0").Labels["owner"] != "app" {
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
	applyQueryCommand(t, m, RevokeSession{SessionID: "s"})
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
	m := mustMachine(t, testRules(), nil)
	for _, input := range []string{`{`, `{"kind":"topology"} {}`, `{"kind":"topology","extra":1}`, `{"kind":"topology","key":"x"}`, `{"kind":"lock_holder"}`, `{"kind":"authority","operator_id":"exec-v1-a1"}`, `{"kind":"authority","operation_id":"exec-v1-a1","operator_id":"exec-v1-a1"}`} {
		if _, err := m.Query(t.Context(), []byte(input)); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal(input, err)
		}
	}
	if _, err := m.Query(t.Context(), []byte(`{"kind":"future"}`)); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal(err)
	}
	m.scoped.Locks = []lockEntry{{Key: "broken", SessionID: "absent"}}
	if _, err := m.Query(t.Context(), []byte(`{"kind":"lock_holder","key":"broken"}`)); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal(err)
	}
}

func TestScopedAuthorityFloorsQueryReturnsOwnedHostGrants(t *testing.T) {
	t.Parallel()
	m := mustMachine(t, testRules(), nil)
	state := scopedCodecFixture()
	m.scoped = &state
	result, err := m.Query(t.Context(), []byte(`{"kind":"authority_floors","key":"n"}`))
	if err != nil {
		t.Fatal(err)
	}
	var got []topology.AuthorityGrant
	if err := json.Unmarshal(result.Data, &got); err != nil {
		t.Fatal(err)
	}
	want := scopedAuthorityFloorsForNode(&state, "n")
	if len(got) != len(want) {
		t.Fatalf("host floors count = %d, want %d", len(got), len(want))
	}
	for i := range got {
		if !got[i].Equal(want[i]) {
			t.Fatalf("host floor %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	if len(got) != 0 {
		got[0].Targets[0].NodeName = "mutated"
		if state.AuthorityRecords[0].Grant.Targets[0].NodeName == "mutated" {
			t.Fatal("authority floor query aliases replicated grant")
		}
	}
}

func TestGrantQueryReturnsExactHistoricalRecordAndRejectsFutureKey(t *testing.T) {
	t.Parallel()
	s := scopedGraphFixture()
	prepareScopedFixture(t, &s)
	m := mustMachine(t, s.Rules, nil)
	m.scoped = &s
	grant := &s.AuthorityRecords[0].Grant
	action := grant.Action
	// The fixture retains this failed-attempt grant behind a newer fence and retry.
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
	// Receivers use the local cache before a confirmed remote lookup. Closing
	// authority cannot erase its immutable identity or force that extra read.
	cached, found := m.AuthorityGrant(grant.Key)
	if !found || !cached.Equal(*grant) {
		t.Fatal("local cache lost retained scoped grant", cached, found)
	}
	if err := m.ValidateGrant(t.Context(), cached); err != nil {
		t.Fatal(err)
	}
	cached.Targets[0].NodeName = "caller mutation"
	if err := m.ValidateGrant(t.Context(), cached); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("altered grant changed or matched retained authority", err)
	}
	future := grant.Key
	future.Revision += 100
	if got := query(future); got != nil {
		t.Fatal("future grant inferred from approved intent", got)
	}
	for _, q := range []QueryRequest{
		{Kind: "authority_grant"},
		{Kind: "authority_grant", AuthorityKey: &grant.Key, Key: "unexpected"},
		{Kind: "authority_grant", AuthorityKey: &grant.Key, OperationID: action.OperationID},
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
	s := scopedGraphFixture()
	prepareScopedFixture(t, &s)
	m := mustMachine(t, s.Rules, nil)
	m.scoped = &s
	grant := s.AuthorityRecords[0].Grant.Clone()
	action := grant.Action
	applyQueryCommand(t, m, GrantSession{SessionID: "courier", NodeName: "n1", InstanceID: "current", TTLSeconds: 10})
	applyQueryCommand(t, m, AcquireLock{Key: "node/n1", SessionID: "courier"})
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
	if proof.Grant == nil || !proof.Grant.Equal(grant) || len(proof.Receipts) != 1 || proof.Courier != (topology.Identity{ClusterID: grant.Key.ClusterID, NodeName: "n1", MemberID: 1, InstanceID: "current"}) {
		t.Fatal(proof)
	}
	proof.Grant.Action.NodeName = "tampered"
	proof.Receipts[0].Action.NodeName = "tampered"
	if !s.indexes.grants[grant.Key].Grant.Equal(grant) || s.indexes.grants[grant.Key].Effects[0].Action.NodeName != "n1" {
		t.Fatal("query result aliases catalog")
	}
	applyQueryCommand(t, m, RevokeSession{SessionID: "courier"})
	result, err = m.Query(t.Context(), data)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(result.Data, &proof); err != nil || proof.Courier.InstanceID != "" {
		t.Fatal("revoked courier stayed authorized", proof, err)
	}
	for _, q := range []QueryRequest{{Kind: "authority_proof"}, {Kind: "authority_proof", AuthorityKey: &grant.Key, OperationID: action.OperationID}} {
		data, _ := json.Marshal(q)
		if _, err := m.Query(t.Context(), data); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal(q, err)
		}
	}
}

func applyQueryCommand(t *testing.T, m *Machine, proposal any) {
	t.Helper()
	result, err := m.Apply(t.Context(), raftengine.Command{Index: m.scoped.AppliedIndex + 1,
		Term: m.scoped.AppliedTerm, Data: mustEncode(t, proposal)})
	var applied ApplyResult
	if err != nil || json.Unmarshal(result.Data, &applied) != nil || !applied.Accepted {
		t.Fatal("query fixture command refused", applied, err)
	}
}
