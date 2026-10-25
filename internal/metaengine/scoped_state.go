package metaengine

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	recoveryplacement "github.com/asalimonov/gquorum/internal/recovery/placement"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

// scopedState is the complete replicated schema-4 metadata checkpoint.
// Derived indexes are rebuilt and validated before live installation.
const scopedStateSchemaVersion = 4

type scopedState struct {
	// Derived only; never trusted from a snapshot or independently persisted.
	indexes *scopedIndexes
	// Ephemeral engine-supplied context for one committed application entry.
	// Convergence persists the resulting proof; snapshots cannot provide this.
	nativeConf       *raftpb.ConfState
	importPlacement  *recoveryplacement.Context  // owned atomic-import analysis only; never serialized/published
	SchemaVersion    uint32                      `json:"schema_version"`
	Rules            topology.Rules              `json:"rules"`
	Latest           topology.Document           `json:"latest"`
	Target           topology.Document           `json:"target"`
	History          []topology.Document         `json:"history"`
	Operations       []topology.Operation        `json:"operations"`
	Groups           []groupConvergence          `json:"group_convergence"`
	Nodes            []nodeConvergence           `json:"node_convergence"`
	Cleanup          []cleanupRecord             `json:"cleanup"`
	Intents          []Intent                    `json:"intents"`
	FullyConverged   topology.FullyConverged     `json:"fully_converged"`
	AuthorityRecords []scopedAuthorityRecord     `json:"authority_records,omitempty"`
	AuthorityFloors  []resourceFloor             `json:"authority_floors,omitempty"`
	Revocations      []scopedRevocationRecord    `json:"revocations,omitempty"`
	RetiredGroups    []retiredGroup              `json:"retired_groups"`
	Allocations      []allocationRecord          `json:"allocations"`
	Admissions       []AdmissionRecord           `json:"admissions,omitempty"`
	Sessions         []sessionState              `json:"sessions"`
	Locks            []lockEntry                 `json:"locks"`
	Activated        []string                    `json:"activated,omitempty"`
	VoterSwitches    []topology.VoterSwitchState `json:"voter_switches,omitempty"`
	AppliedIndex     uint64                      `json:"applied_index"`
	AppliedTerm      uint64                      `json:"applied_term"`
	Recovery         *model.ImportState          `json:"recovery,omitempty"`
}

// The shared grant/evidence shape is flattened in JSON. Class is derived by
// the FSM; callers cannot select the authority class.
type scopedAuthorityRecord struct {
	authorityRecord
	Class string `json:"class"`
}

type scopedRevocationRecord struct {
	Requested   bool                       `json:"requested"`
	OperationID topology.OperationID       `json:"operation_id"`
	GroupID     string                     `json:"group_id"`
	Obligations []topology.FenceObligation `json:"obligations"`
}

func (s scopedState) MarshalJSON() ([]byte, error) {
	owned, err := s.canonical()
	if err != nil {
		return nil, err
	}
	type wire scopedState
	return json.Marshal(wire(owned))
}

func (s *scopedState) UnmarshalJSON(data []byte) error {
	type wire scopedState
	var decoded wire
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&decoded); err != nil {
		return fault.New(fault.Unreadable, "invalid scoped metadata state", err)
	}
	if d.Decode(new(any)) != io.EOF {
		return fault.ErrUnreadable
	}
	owned, err := scopedState(decoded).canonical()
	if err != nil {
		return err
	}
	owned.indexes, err = prepareScopedGraph(&owned, owned.Rules)
	if err != nil {
		return err
	}
	*s = owned
	return nil
}
