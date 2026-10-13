//go:build !functional

package raftstate

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func codecGrant() topology.AuthorityGrant {
	return topology.AuthorityGrant{
		Key:     topology.AuthorityKey{ClusterID: "cluster", GroupID: "orders", Revision: 100},
		Purpose: topology.AuthorityEffect, OperatorID: topology.MakeOperatorID(2, 1),
		Action:    &topology.AuthorizedStep{OperatorID: topology.MakeOperatorID(2, 1), Version: 2, Attempt: 1, StepSeq: 1, Kind: topology.StepAddLearner, GroupID: "orders", NodeName: "b", ReplicaID: 4},
		Resources: []topology.AuthorityResource{{Kind: topology.AuthorityGroup, GroupID: "orders"}},
		Targets:   []topology.AuthorityTarget{{NodeName: "a", MemberID: 1, ReplicaID: 3}, {NodeName: "b", MemberID: 2, ReplicaID: 4}},
	}
}
func codecSnapshot() Snapshot {
	g := codecGrant()
	return Snapshot{Schema: Schema, ClusterID: "cluster", GroupID: "orders", Application: []byte("entire application"), Authority: AuthorityState{
		Grant: &g, InstallIndex: 3, InstallTerm: 2,
		Receipt: &topology.EffectReceipt{Key: g.Key, Resource: g.Resources[0], Action: g.Clone().Action, Target: g.Targets[1], Outcome: topology.EffectApplied, AppliedIndex: 4, AppliedTerm: 2},
	}}
}
func TestCompleteSnapshotRoundTripAndBoundaries(t *testing.T) {
	t.Parallel()
	s := codecSnapshot()
	data, err := EncodeSnapshot(s)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeSnapshot(data, "cluster", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Application, s.Application) || !got.Authority.Grant.Equal(*s.Authority.Grant) {
		t.Fatal("snapshot lost application or authority")
	}
	if err := ValidateAuthority(got.Authority, 4); err != nil {
		t.Fatal(err)
	} // meta revision 100 is a different log
	if err := ValidateAuthority(got.Authority, 3); err == nil {
		t.Fatal("future effect receipt accepted")
	}
	clone := got.Authority.Clone()
	clone.Grant.Action.StepSeq++
	clone.Receipt.Action.StepSeq++
	if got.Authority.Grant.Action.StepSeq != 1 || got.Authority.Receipt.Action.StepSeq != 1 {
		t.Fatal("authority snapshot aliases")
	}
	if _, err := DecodeSnapshot(data, "other", "orders"); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	got.Application[0] = 'x'
	again, err := DecodeSnapshot(data, "cluster", "orders")
	if err != nil || again.Application[0] != 'e' {
		t.Fatal("decode aliases input or sibling state", err)
	}
}
func TestSnapshotRejectsMissingForeignOrContradictoryAuthority(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func(*Snapshot){
		"missing grant":              func(s *Snapshot) { s.Authority.Grant = nil },
		"missing install index":      func(s *Snapshot) { s.Authority.InstallIndex = 0 },
		"effect before install":      func(s *Snapshot) { s.Authority.Receipt.AppliedIndex = 2 },
		"effect term before install": func(s *Snapshot) { s.Authority.Receipt.AppliedTerm = 1 },
		"altered action":             func(s *Snapshot) { s.Authority.Receipt.Action.ReplicaID++ },
		"empty application":          func(s *Snapshot) { s.Application = nil },
		"foreign group":              func(s *Snapshot) { s.GroupID = "other" },
		"old schema":                 func(s *Snapshot) { s.Schema = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := codecSnapshot()
			change(&s)
			data, err := json.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeSnapshot(data, s.ClusterID, s.GroupID); err == nil {
				t.Fatal("corrupt snapshot accepted")
			}
		})
	}
	for _, data := range [][]byte{[]byte(`{"schema_version":1,"state":"old"}`), []byte(`{"schema":2}`), []byte(`null`), []byte(`[]`)} {
		if _, err := DecodeSnapshot(data, "cluster", "orders"); err == nil {
			t.Fatal("legacy/incomplete snapshot accepted")
		}
	}
	data, _ := EncodeSnapshot(codecSnapshot())
	if _, err := DecodeSnapshot(append(data, []byte(` {}`)...), "cluster", "orders"); err == nil {
		t.Fatal("trailing record accepted")
	}
}
func TestEntryKindsCannotSmuggleAdministrativeAuthority(t *testing.T) {
	t.Parallel()
	g := codecGrant()
	app := Entry{Schema: Schema, Kind: Application, Correlation: "call", RequestID: "command", Data: []byte("payload")}
	data, err := EncodeEntry(app)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeEntry(data); err != nil || got.RequestID != "command" {
		t.Fatal(got, err)
	}
	app.Grant = &g
	if _, err := EncodeEntry(app); err == nil {
		t.Fatal("application smuggled grant")
	}
	install := Entry{Schema: Schema, Kind: InstallAuthority, Correlation: "call", Grant: &g}
	if _, err := EncodeEntry(install); err != nil {
		t.Fatal(err)
	}
	config := install
	config.Kind = ManagedConfiguration
	config.Action = g.Clone().Action
	data, err = EncodeEntry(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeEntry(data); err != nil {
		t.Fatal(err)
	}
	config.Action.StepSeq++
	if _, err := EncodeEntry(config); err == nil {
		t.Fatal("configuration rebound grant")
	}
	config.Action = g.Clone().Action
	config.Kind = TransferIntent
	if _, err := EncodeEntry(config); err == nil {
		t.Fatal("configuration treated as transfer")
	}
	standalone := Entry{Schema: Schema, Kind: StandaloneConfiguration, Correlation: "call", Data: []byte("caller context")}
	if _, err := EncodeEntry(standalone); err != nil {
		t.Fatal(err)
	}
	standalone.Grant = &g
	if _, err := EncodeEntry(standalone); err == nil {
		t.Fatal("standalone bypass carries grant")
	}
	if _, err := DecodeEntry([]byte(`{"schema":1,"kind":"application","correlation":"call","request_id":"cmd"}`)); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal(err)
	}
}

func TestDemotionTransferSnapshotRequiresExactGrantAndAppliedBoundary(t *testing.T) {
	t.Parallel()
	grant := codecGrant()
	grant.Action.Kind = topology.StepDemoteVoter
	grant.Action.TransferNodeName, grant.Action.TransferReplicaID = "a", 3
	state := AuthorityState{Grant: &grant, InstallIndex: 3, InstallTerm: 2, DemotionTransferIndex: 4, DemotionTransferTerm: 2}
	if err := ValidateAuthority(state, 4); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*AuthorityState){
		func(s *AuthorityState) { s.Grant = nil },
		func(s *AuthorityState) { s.DemotionTransferIndex = 0 },
		func(s *AuthorityState) { s.DemotionTransferTerm = 0 },
		func(s *AuthorityState) { s.DemotionTransferIndex = 3 },
		func(s *AuthorityState) { s.DemotionTransferIndex = 5 },
		func(s *AuthorityState) { s.Grant.Action.TransferReplicaID, s.Grant.Action.TransferNodeName = 0, "" },
		func(s *AuthorityState) { s.Grant.Action.Kind = topology.StepRemoveReplica },
		func(s *AuthorityState) {
			s.Receipt = &topology.EffectReceipt{Key: grant.Key, Resource: grant.Resources[0], Action: grant.Action, Target: grant.Targets[1], Outcome: topology.EffectApplied, AppliedIndex: 4, AppliedTerm: 2}
		},
	} {
		bad := state.Clone()
		mutate(&bad)
		if err := ValidateAuthority(bad, 4); err == nil {
			t.Fatal("invalid demotion prerequisite survived snapshot validation", bad)
		}
	}
}
