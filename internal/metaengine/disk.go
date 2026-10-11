package metaengine

import (
	"encoding/json"
	"github.com/asalimonov/gquorum/topology"
	"time"
)

// These schema-1 records deliberately retain their legacy JSON vocabulary.
// Public Operator names do not change Raft commands, snapshots or reply keys.
type diskOperatorStatus struct {
	ID            topology.OperatorID    `json:"id"`
	Version       uint64                 `json:"version"`
	Attempt       uint32                 `json:"attempt"`
	State         topology.OperatorState `json:"state"`
	CurrentStep   int                    `json:"current_step"`
	TotalSteps    int                    `json:"total_steps"`
	GroupID       string                 `json:"group_id,omitempty"`
	NodeName      string                 `json:"node_name,omitempty"`
	WaitingReason string                 `json:"waiting_reason,omitempty"`
	ErrorCode     string                 `json:"error_code,omitempty"`
	Error         string                 `json:"error,omitempty"`
}
type diskOperator struct {
	Status    diskOperatorStatus `json:"status"`
	Plan      topology.Plan      `json:"plan"`
	Actor     string             `json:"actor"`
	Origin    topology.Origin    `json:"origin"`
	StartedAt time.Time          `json:"started_at"`
	UpdatedAt time.Time          `json:"updated_at"`
}
type diskAdmission struct {
	NodeName    string              `json:"node_name"`
	InstanceID  string              `json:"instance_id"`
	MemberID    uint64              `json:"member_id"`
	ExecutionID topology.OperatorID `json:"execution_id"`
	State       AdmissionState      `json:"state"`
}
type metaDiskState struct {
	SchemaVersion uint32              `json:"schema_version"`
	Rules         topology.Rules      `json:"rules"`
	RetiredGroups []retiredGroup      `json:"retired_groups"`
	Allocations   []allocationRecord  `json:"allocations"`
	Latest        topology.Document   `json:"latest"`
	Target        topology.Document   `json:"target"`
	Active        topology.Document   `json:"active"`
	History       []topology.Document `json:"history"`
	Executions    []diskOperator      `json:"executions"`
	Admissions    []diskAdmission     `json:"admissions,omitempty"`
	Sessions      []sessionState      `json:"sessions"`
	Locks         []lockEntry         `json:"locks"`
	Activated     []string            `json:"activated,omitempty"` // sorted node names that ever activated
	AppliedIndex  uint64              `json:"applied_index"`
	AppliedTerm   uint64              `json:"applied_term"`
}
type diskApplyResult struct {
	ProposalID     string
	CurrentVersion uint64
	Accepted       bool
	Reason         string
	Code           proposalErrorCode
	Dropped        bool
	Doc            topology.Document
	ExecutionID    topology.OperatorID
	Admission      diskAdmission
}
type diskOperatorTransition struct {
	ExecutionID   topology.OperatorID    `json:"execution_id"`
	Version       uint64                 `json:"version"`
	Attempt       uint32                 `json:"attempt"`
	StepSeq       int                    `json:"step_seq"`
	State         topology.OperatorState `json:"state"`
	GroupID       string                 `json:"group_id,omitempty"`
	NodeName      string                 `json:"node_name,omitempty"`
	WaitingReason string                 `json:"waiting_reason,omitempty"`
	Error         string                 `json:"error,omitempty"`
	At            time.Time              `json:"at"`
}

func toDiskOperator(v topology.Operator) diskOperator {
	return diskOperator{Status: diskOperatorStatus(v.Status), Plan: v.Plan, Actor: v.Actor, Origin: v.Origin, StartedAt: v.StartedAt, UpdatedAt: v.UpdatedAt}
}
func (d diskOperator) public() topology.Operator {
	return topology.Operator{Status: topology.OperatorStatus(d.Status), Plan: d.Plan, Actor: d.Actor, Origin: d.Origin, StartedAt: d.StartedAt, UpdatedAt: d.UpdatedAt}
}
func toDiskAdmission(a AdmissionRecord) diskAdmission {
	return diskAdmission{NodeName: a.NodeName, InstanceID: a.InstanceID, MemberID: a.MemberID, ExecutionID: a.OperatorID, State: a.State}
}
func (a diskAdmission) public() AdmissionRecord {
	return AdmissionRecord{NodeName: a.NodeName, InstanceID: a.InstanceID, MemberID: a.MemberID, OperatorID: a.ExecutionID, State: a.State}
}
func toDiskTransition(t topology.OperatorTransition) diskOperatorTransition {
	return diskOperatorTransition{ExecutionID: t.OperatorID, Version: t.Version, Attempt: t.Attempt, StepSeq: t.StepSeq, State: t.State, GroupID: t.GroupID, NodeName: t.NodeName, WaitingReason: t.WaitingReason, Error: t.Error, At: t.At}
}
func (t diskOperatorTransition) public() topology.OperatorTransition {
	return topology.OperatorTransition{OperatorID: t.ExecutionID, Version: t.Version, Attempt: t.Attempt, StepSeq: t.StepSeq, State: t.State, GroupID: t.GroupID, NodeName: t.NodeName, WaitingReason: t.WaitingReason, Error: t.Error, At: t.At}
}

func (s smState) MarshalJSON() ([]byte, error) {
	d := metaDiskState{SchemaVersion: s.SchemaVersion, Rules: s.Rules, RetiredGroups: s.RetiredGroups, Allocations: s.Allocations, Latest: s.Latest, Target: s.Target, Active: s.Active, History: s.History, Sessions: s.Sessions, Locks: s.Locks, Activated: s.Activated, AppliedIndex: s.AppliedIndex, AppliedTerm: s.AppliedTerm}
	if s.Operators != nil {
		d.Executions = make([]diskOperator, len(s.Operators))
		for i, v := range s.Operators {
			d.Executions[i] = toDiskOperator(v)
		}
	}
	if s.Admissions != nil {
		d.Admissions = make([]diskAdmission, len(s.Admissions))
		for i, v := range s.Admissions {
			d.Admissions[i] = toDiskAdmission(v)
		}
	}
	return json.Marshal(d)
}
func (s *smState) UnmarshalJSON(data []byte) error {
	var d metaDiskState
	if err := json.Unmarshal(data, &d); err != nil {
		return err
	}
	out := smState{SchemaVersion: d.SchemaVersion, Rules: d.Rules, RetiredGroups: d.RetiredGroups, Allocations: d.Allocations, Latest: d.Latest, Target: d.Target, Active: d.Active, History: d.History, Sessions: d.Sessions, Locks: d.Locks, Activated: d.Activated, AppliedIndex: d.AppliedIndex, AppliedTerm: d.AppliedTerm}
	if d.Executions != nil {
		out.Operators = make([]topology.Operator, len(d.Executions))
		for i, v := range d.Executions {
			out.Operators[i] = v.public()
		}
	}
	if d.Admissions != nil {
		out.Admissions = make([]AdmissionRecord, len(d.Admissions))
		for i, v := range d.Admissions {
			out.Admissions[i] = v.public()
		}
	}
	*s = out
	return nil
}
func (r ApplyResult) MarshalJSON() ([]byte, error) {
	return json.Marshal(diskApplyResult{ProposalID: r.ProposalID, CurrentVersion: r.CurrentVersion, Accepted: r.Accepted, Reason: r.Reason, Code: r.Code, Dropped: r.Dropped, Doc: r.Doc, ExecutionID: r.OperatorID, Admission: toDiskAdmission(r.Admission)})
}
func (r *ApplyResult) UnmarshalJSON(data []byte) error {
	var d diskApplyResult
	if err := json.Unmarshal(data, &d); err != nil {
		return err
	}
	*r = ApplyResult{ProposalID: d.ProposalID, CurrentVersion: d.CurrentVersion, Accepted: d.Accepted, Reason: d.Reason, Code: d.Code, Dropped: d.Dropped, Doc: d.Doc, OperatorID: d.ExecutionID, Admission: d.Admission.public()}
	return nil
}
func (p TransitionOperatorProposal) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Transition diskOperatorTransition `json:"transition"`
		ProposalID string                 `json:"proposal_id"`
	}{toDiskTransition(p.Transition), p.ProposalID})
}
func (p *TransitionOperatorProposal) UnmarshalJSON(data []byte) error {
	var d struct {
		Transition diskOperatorTransition `json:"transition"`
		ProposalID string                 `json:"proposal_id"`
	}
	if err := json.Unmarshal(data, &d); err != nil {
		return err
	}
	*p = TransitionOperatorProposal{Transition: d.Transition.public(), ProposalID: d.ProposalID}
	return nil
}

type diskActivateTarget struct {
	ExecutionID topology.OperatorID `json:"execution_id"`
	Version     uint64              `json:"version"`
	Attempt     uint32              `json:"attempt"`
	ProposalID  string              `json:"proposal_id"`
}

func (p ActivateTargetProposal) MarshalJSON() ([]byte, error) {
	return json.Marshal(diskActivateTarget{ExecutionID: p.OperatorID, Version: p.Version, Attempt: p.Attempt, ProposalID: p.ProposalID})
}
func (p *ActivateTargetProposal) UnmarshalJSON(data []byte) error {
	var d diskActivateTarget
	if err := json.Unmarshal(data, &d); err != nil {
		return err
	}
	*p = ActivateTargetProposal{OperatorID: d.ExecutionID, Version: d.Version, Attempt: d.Attempt, ProposalID: d.ProposalID}
	return nil
}

type diskReadyAdmission struct {
	NodeName    string              `json:"node_name"`
	InstanceID  string              `json:"instance_id"`
	MemberID    uint64              `json:"member_id"`
	ExecutionID topology.OperatorID `json:"execution_id"`
	ProposalID  string              `json:"proposal_id"`
}

func (p ReadyAdmissionProposal) MarshalJSON() ([]byte, error) {
	return json.Marshal(diskReadyAdmission{NodeName: p.NodeName, InstanceID: p.InstanceID, MemberID: p.MemberID, ExecutionID: p.OperatorID, ProposalID: p.ProposalID})
}
func (p *ReadyAdmissionProposal) UnmarshalJSON(data []byte) error {
	var d diskReadyAdmission
	if err := json.Unmarshal(data, &d); err != nil {
		return err
	}
	*p = ReadyAdmissionProposal{NodeName: d.NodeName, InstanceID: d.InstanceID, MemberID: d.MemberID, OperatorID: d.ExecutionID, ProposalID: d.ProposalID}
	return nil
}
