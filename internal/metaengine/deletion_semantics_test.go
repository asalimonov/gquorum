//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func TestDeletionSemanticsCommandPreservesLegacyBytes(t *testing.T) {
	t.Parallel()
	_, proposal := deletionApprovalFixture(t)
	legacy, err := Encode(proposal)
	if err != nil || bytes.Contains(legacy, []byte("deletion_semantics")) {
		t.Fatal("legacy command shape changed", string(legacy), err)
	}
	for _, semantics := range []DeletionSemantics{DeletionSemanticsLegacy, DeletionSemanticsDetached} {
		p := proposal
		p.DeletionSemantics = semantics
		data, err := Encode(p)
		if err != nil {
			t.Fatal(err)
		}
		value, err := DecodeProposal(data)
		if err != nil {
			t.Fatal(err)
		}
		decoded := value.(*BeginOperationProposal)
		if decoded.DeletionSemantics != semantics || !decoded.Plan.Equal(proposal.Plan) {
			t.Fatal("command lost its immutable interpretation", decoded)
		}
		roundtrip, err := Encode(*decoded)
		if err != nil || !bytes.Equal(data, roundtrip) {
			t.Fatal("command bytes changed on decode/encode", err)
		}
	}
	// Explicit zero is allowed; re-encoding omits it just like an old writer.
	explicit := bytes.Replace(legacy, []byte("\"body\":{"), []byte("\"body\":{\"deletion_semantics\":0,"), 1)
	value, err := DecodeProposal(explicit)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := Encode(*value.(*BeginOperationProposal))
	if err != nil || !bytes.Equal(legacy, canonical) {
		t.Fatal("explicit legacy marker changed canonical bytes", err)
	}
	proposal.DeletionSemantics = 2
	unsupported, err := Encode(proposal)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeProposal(unsupported); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal("unknown approval interpretation accepted", err)
	}
}

func TestDeletionSemanticsDecodesUnmarkedHistoricalEnvelope(t *testing.T) {
	t.Parallel()
	// This pre-marker command literal is independent of the current encoder.
	const command = `{"schema_version":4,"kind":"begin_operation","body":{"version":1,"plan":{"version":1,` +
		`"steps":[],"blocks":[]},"scopes":[],"actor":"admin","origin":"api","at":"2026-10-17T0` +
		`0:00:00Z","proposal_id":"legacy"}}`
	decoded, err := DecodeProposal([]byte(command))
	if err != nil {
		t.Fatal(err)
	}
	proposal := decoded.(*BeginOperationProposal)
	if proposal.DeletionSemantics != DeletionSemanticsLegacy {
		t.Fatal("unmarked command acquired new deletion semantics")
	}
	encoded, err := Encode(*proposal)
	if err != nil || string(encoded) != command {
		t.Fatal("historical approval bytes changed", string(encoded), err)
	}
}

func TestDeletionSemanticsActivationWaitsForCompleteApprovalPath(t *testing.T) {
	t.Parallel()
	s, proposal := deletionApprovalFixture(t)
	before, _ := json.Marshal(s)
	proposal.DeletionSemantics = DeletionSemanticsDetached
	if _, err := prepareScopedApproval(&s, s.Latest, proposal, explicitApproval); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal("new command silently used legacy approval", err)
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("unselected interpretation mutated state")
	}
}
