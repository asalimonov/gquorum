package raftstate

import (
	"encoding/json"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

type EntryKind string

const (
	Application             EntryKind = "application"
	InstallAuthority        EntryKind = "install_authority"
	ObserveEffect           EntryKind = "observe_effect"
	TransferIntent          EntryKind = "transfer_intent"
	ManagedConfiguration    EntryKind = "managed_configuration"
	StandaloneConfiguration EntryKind = "standalone_configuration"
)

type Entry struct {
	Schema      uint32                   `json:"schema"`
	Kind        EntryKind                `json:"kind"`
	Correlation string                   `json:"correlation,omitempty"`
	RequestID   string                   `json:"request_id,omitempty"`
	Data        []byte                   `json:"data,omitempty"`
	Grant       *topology.AuthorityGrant `json:"grant,omitempty"`
	Action      *topology.AuthorizedStep `json:"action,omitempty"`
}

func EncodeEntry(e Entry) ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(e)
}
func DecodeEntry(data []byte) (Entry, error) {
	var e Entry
	if err := decode(data, &e); err != nil {
		return Entry{}, err
	}
	if err := e.Validate(); err != nil {
		return Entry{}, err
	}
	return e, nil
}

// Validate checks payload shape. The engine additionally restricts which kinds
// may be proposed by an application and which Raft entry type can carry them.
func (e Entry) Validate() error {
	if e.Schema != Schema {
		return fault.New(fault.Unsupported, "unsupported engine entry schema", nil)
	}
	if !validText(e.Correlation) {
		return fault.New(fault.Unreadable, "entry lacks correlation", nil)
	}
	switch e.Kind {
	case Application:
		if !validText(e.RequestID) || e.Grant != nil || e.Action != nil {
			return fault.New(fault.Unreadable, "application entry contains invalid authority or request ID", nil)
		}
		return nil
	case StandaloneConfiguration:
		if e.RequestID != "" || e.Grant != nil || e.Action != nil {
			return fault.New(fault.Unreadable, "standalone entry contains managed authority", nil)
		}
		return nil
	case InstallAuthority, ObserveEffect, TransferIntent, ManagedConfiguration:
	default:
		return fault.New(fault.Unsupported, "unknown engine entry kind", nil)
	}
	if e.RequestID != "" || len(e.Data) != 0 || e.Grant == nil {
		return fault.New(fault.Unreadable, "administrative entry lacks exclusive grant payload", nil)
	}
	if err := e.Grant.Validate(); err != nil {
		return fault.New(fault.Unreadable, "invalid administrative grant", err)
	}
	resource := topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: e.Grant.Key.GroupID}
	if !slices.Contains(e.Grant.Resources, resource) {
		return fault.New(fault.Unreadable, "administrative entry has no group resource", nil)
	}
	if e.Kind == InstallAuthority {
		if e.Action != nil {
			return fault.New(fault.Unreadable, "install entry cannot execute an action", nil)
		}
		return nil
	}
	if e.Action == nil || e.Grant.Action == nil || *e.Action != *e.Grant.Action {
		return fault.New(fault.Unreadable, "administrative entry action differs from grant", nil)
	}
	switch e.Kind {
	case TransferIntent:
		if e.Action.Kind != topology.StepTransferLeader && (e.Action.Kind != topology.StepDemoteVoter || e.Action.TransferReplicaID == 0 || e.Action.TransferReplicaID == e.Action.ReplicaID) {
			return fault.New(fault.Unreadable, "non-transfer intent", nil)
		}
	case ManagedConfiguration:
		switch e.Action.Kind {
		case topology.StepAddLearner, topology.StepPromoteVoter, topology.StepDemoteVoter, topology.StepRemoveReplica:
		default:
			return fault.New(fault.Unreadable, "non-membership configuration context", nil)
		}
	}
	return nil
}
