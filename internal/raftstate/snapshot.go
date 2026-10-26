// Package raftstate encodes complete engine snapshots and administrative log
// records without depending on the engine, metadata machine or storage owner.
package raftstate

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"slices"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// Schema identifies the current complete engine snapshot and log-record
// encoding.
const Schema uint32 = 2

// AuthorityState preserves installed grant and effect evidence within a group's
// applied log.
type AuthorityState struct {
	Grant                 *topology.AuthorityGrant `json:"grant,omitempty"`
	InstallIndex          uint64                   `json:"install_index"`
	InstallTerm           uint64                   `json:"install_term"`
	Receipt               *topology.EffectReceipt  `json:"receipt,omitempty"`
	DemotionTransferIndex uint64                   `json:"demotion_transfer_index,omitempty"`
	DemotionTransferTerm  uint64                   `json:"demotion_transfer_term,omitempty"`
}

// Clone copies the installed grant and optional effect receipt.
func (s AuthorityState) Clone() AuthorityState {
	if s.Grant != nil {
		g := s.Grant.Clone()
		s.Grant = &g
	}
	if s.Receipt != nil {
		r := s.Receipt.Clone()
		s.Receipt = &r
	}
	return s
}

// Snapshot contains exact group identity, complete application state and
// retained engine authority.
type Snapshot struct {
	Schema      uint32         `json:"schema"`
	ClusterID   string         `json:"cluster_id"`
	GroupID     string         `json:"group_id"`
	Application []byte         `json:"application"`
	Authority   AuthorityState `json:"authority"`
}

// ValidateAuthority checks group-owned authority against its local applied
// boundary. Grant revisions belong to _meta's log, not this group's index
// space.
func ValidateAuthority(s AuthorityState, applied uint64) error {
	if s.Grant == nil {
		if s.InstallIndex != 0 || s.InstallTerm != 0 || s.Receipt != nil || s.DemotionTransferIndex != 0 ||
			s.DemotionTransferTerm != 0 {
			return fault.New(fault.Unreadable, "authority state lacks its grant", nil)
		}
		return nil
	}
	if err := s.Grant.Validate(); err != nil {
		return fault.New(fault.Unreadable, "invalid installed authority", err)
	}
	resource := topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: s.Grant.Key.GroupID}
	if s.InstallIndex == 0 || s.InstallTerm == 0 || s.InstallIndex > applied ||
		!slices.Contains(s.Grant.Resources, resource) {
		return fault.New(fault.Unreadable, "group authority outside applied boundary", nil)
	}
	if err := s.validateReceipt(applied, resource); err != nil {
		return err
	}
	return s.validateDemotionTransfer(applied)
}

func (s AuthorityState) validateReceipt(applied uint64, resource topology.AuthorityResource) error {
	if s.Receipt != nil {
		if err := s.Receipt.Validate(*s.Grant); err != nil {
			return fault.New(fault.Unreadable, "invalid authority consumption", err)
		}
		if s.Receipt.Resource != resource || s.Receipt.AppliedIndex < s.InstallIndex ||
			s.Receipt.AppliedIndex > applied || s.Receipt.AppliedTerm < s.InstallTerm {
			return fault.New(fault.Unreadable, "authority receipt outside installed/applied boundary", nil)
		}
	}
	return nil
}

func (s AuthorityState) validateDemotionTransfer(applied uint64) error {
	if s.DemotionTransferIndex != 0 || s.DemotionTransferTerm != 0 {
		a := s.Grant.Action
		if a == nil || a.Kind != topology.StepDemoteVoter || a.TransferReplicaID == 0 ||
			a.TransferReplicaID == a.ReplicaID || s.DemotionTransferIndex <= s.InstallIndex ||
			s.DemotionTransferIndex > applied || s.DemotionTransferTerm < s.InstallTerm {
			return fault.New(fault.Unreadable, "invalid demotion transfer intent", nil)
		}
		if s.Receipt != nil && (s.Receipt.AppliedIndex <= s.DemotionTransferIndex ||
			s.Receipt.AppliedTerm < s.DemotionTransferTerm) {
			return fault.New(fault.Unreadable, "demotion receipt precedes its transfer intent", nil)
		}
	}
	return nil
}

// EncodeSnapshot validates and encodes complete application and authority
// state.
func EncodeSnapshot(s Snapshot) ([]byte, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	return json.Marshal(s)
}

// DecodeSnapshot rejects legacy raw application snapshots. Callers separately
// validate authority against the Raft snapshot metadata's applied index.
func DecodeSnapshot(data []byte, cluster, group string) (Snapshot, error) {
	var s Snapshot
	if err := decodeSnapshotEnvelope(data, &s); err != nil {
		return Snapshot{}, err
	}
	if err := s.validate(); err != nil {
		return Snapshot{}, err
	}
	if s.ClusterID != cluster || s.GroupID != group {
		return Snapshot{}, fault.ErrIdentityMismatch
	}
	return s, nil
}
func (s Snapshot) validate() error {
	if s.Schema != Schema {
		return fault.New(fault.Unsupported, "unsupported engine snapshot schema", nil)
	}
	if len(s.Application) == 0 || !validText(s.ClusterID) || topology.ValidateID(s.GroupID) != nil {
		return fault.New(fault.Unreadable, "snapshot lacks identity or complete application state", nil)
	}
	if err := ValidateAuthority(s.Authority, math.MaxUint64); err != nil {
		return err
	}
	if s.Authority.Grant != nil && (s.Authority.Grant.Key.ClusterID != s.ClusterID ||
		s.Authority.Grant.Key.GroupID != s.GroupID) {
		return fault.New(fault.IdentityMismatch, "snapshot contains foreign authority", nil)
	}
	return nil
}

func validText(s string) bool {
	return strings.TrimSpace(s) != "" && !strings.ContainsRune(s, 0)
}

func decode(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fault.New(fault.Unreadable, "invalid engine record", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fault.New(fault.Unreadable, "trailing engine record data", err)
	}
	return nil
}
