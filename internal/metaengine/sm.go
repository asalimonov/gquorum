package metaengine

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

var errDurablePersist = errors.New("meta: durable state persist failed")

type statePersister interface {
	SaveState(data []byte) error
}

type ApplyResult struct {
	ProposalID     string
	CurrentVersion uint64
	Accepted       bool
	Reason         string
	Code           proposalErrorCode
	Dropped        bool
	Doc            topology.Document
	OperatorID     topology.OperatorID
	Admission      AdmissionRecord
}

type proposalErrorCode string

const (
	proposalVersionConflict proposalErrorCode = "version_conflict"
	proposalAuthorization   proposalErrorCode = "authorization"
	proposalSuperseded      proposalErrorCode = "superseded"
	proposalGuardrail       proposalErrorCode = "guardrail"
)

type operatorRecord struct {
	ID            topology.OperatorID    `json:"id,omitempty"`
	Version       uint64                 `json:"version"`
	Attempt       uint32                 `json:"attempt,omitempty"`
	State         topology.OperatorState `json:"state"`
	Steps         []topology.Step        `json:"steps,omitempty"`
	CurrentStep   int                    `json:"current_step,omitempty"`
	GroupID       string                 `json:"group_id,omitempty"`
	NodeName      string                 `json:"node_name,omitempty"`
	WaitingReason string                 `json:"waiting_reason,omitempty"`
	Error         string                 `json:"error,omitempty"`
	Origin        topology.Origin        `json:"origin,omitempty"`
	Actor         string                 `json:"actor,omitempty"`
	StartedAt     time.Time              `json:"started_at,omitempty"`
	UpdatedAt     time.Time              `json:"updated_at,omitempty"`
}

type topologySM struct {
	mu        sync.RWMutex
	persister statePersister
	rules     topology.Rules

	latest     topology.Document
	target     topology.Document
	active     topology.Document
	history    []topology.Document
	operators  map[topology.OperatorID]*operatorRecord
	admissions map[string]*AdmissionRecord

	sessions map[string]*sessionState // sessionID → session
	locks    map[string]string        // lock key → holding sessionID

	retired     map[string]uint64
	allocations map[string]uint64
	activated   map[string]struct{} // node name → has activated at least once

	appliedIndex uint64
	appliedTerm  uint64
}

func newTopologySM(persister statePersister, rules topology.Rules) *topologySM {
	return &topologySM{
		persister:   persister,
		rules:       rules,
		retired:     make(map[string]uint64),
		allocations: make(map[string]uint64),
		operators:   make(map[topology.OperatorID]*operatorRecord),
		admissions:  make(map[string]*AdmissionRecord),
		sessions:    make(map[string]*sessionState),
		locks:       make(map[string]string),
		activated:   make(map[string]struct{}),
	}
}

type smState struct {
	SchemaVersion uint32              `json:"schema_version"`
	Rules         topology.Rules      `json:"rules"`
	RetiredGroups []retiredGroup      `json:"retired_groups"`
	Allocations   []allocationRecord  `json:"allocations"`
	Latest        topology.Document   `json:"latest"`
	Target        topology.Document   `json:"target"`
	Active        topology.Document   `json:"active"`
	History       []topology.Document `json:"history"`
	Operators     []topology.Operator `json:"executions"`
	Admissions    []AdmissionRecord   `json:"admissions,omitempty"`
	Sessions      []sessionState      `json:"sessions"`
	Locks         []lockEntry         `json:"locks"`
	Activated     []string            `json:"activated,omitempty"` // sorted node names that ever activated
	AppliedIndex  uint64              `json:"applied_index"`
	AppliedTerm   uint64              `json:"applied_term"`
}

func (sm *topologySM) Apply(index, term uint64, data []byte) (ApplyResult, error) {
	kind, body, err := decodeEnvelope(data)
	if err != nil {
		return ApplyResult{}, err
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if index <= sm.appliedIndex {
		return ApplyResult{}, nil
	}

	var (
		res     ApplyResult
		changed bool
	)
	switch kind {
	case KindProposeDocument:
		var p ProposeDocument
		if err := json.Unmarshal(body, &p); err != nil {
			return ApplyResult{}, fmt.Errorf("meta: unmarshal propose_document: %w", err)
		}
		res, changed = sm.applyProposeDocument(p)
	case KindAdmitNode:
		var p AdmitNode
		if err := json.Unmarshal(body, &p); err != nil {
			return ApplyResult{}, fmt.Errorf("meta: unmarshal admit_node: %w", err)
		}
		res, changed = sm.applyAdmitNode(p)
	case KindBeginOperator:
		var p BeginOperatorProposal
		if err := json.Unmarshal(body, &p); err != nil {
			return ApplyResult{}, fmt.Errorf("meta: unmarshal begin_execution: %w", err)
		}
		res, changed = sm.applyBeginOperator(p)
	case KindCommitRepairOperator:
		var p CommitRepairOperatorProposal
		if err := json.Unmarshal(body, &p); err != nil {
			return ApplyResult{}, fmt.Errorf("meta: unmarshal commit_repair_execution: %w", err)
		}
		res, changed = sm.applyCommitRepairOperator(p)
	case KindTransitionOperator:
		var p TransitionOperatorProposal
		if err := json.Unmarshal(body, &p); err != nil {
			return ApplyResult{}, fmt.Errorf("meta: unmarshal transition_execution: %w", err)
		}
		res, changed = sm.applyTransitionOperator(p)
	case KindActivateTarget:
		var p ActivateTargetProposal
		if err := json.Unmarshal(body, &p); err != nil {
			return ApplyResult{}, fmt.Errorf("meta: unmarshal activate_target: %w", err)
		}
		res, changed = sm.applyActivateTarget(p)
	case KindBindAdmission:
		var p BindAdmissionProposal
		if err := json.Unmarshal(body, &p); err != nil {
			return ApplyResult{}, fmt.Errorf("meta: unmarshal bind_admission: %w", err)
		}
		res, changed = sm.applyBindAdmission(p)
	case KindCommitReadmission:
		var p CommitReadmissionProposal
		if err := json.Unmarshal(body, &p); err != nil {
			return ApplyResult{}, fmt.Errorf("meta: unmarshal commit_readmission: %w", err)
		}
		res, changed = sm.applyCommitReadmission(p)
	case KindReadyAdmission:
		var p ReadyAdmissionProposal
		if err := json.Unmarshal(body, &p); err != nil {
			return ApplyResult{}, fmt.Errorf("meta: unmarshal ready_admission: %w", err)
		}
		res, changed = sm.applyReadyAdmission(p)
	case KindGrantSession:
		var p GrantSession
		if err := json.Unmarshal(body, &p); err != nil {
			return ApplyResult{}, fmt.Errorf("meta: unmarshal grant_session: %w", err)
		}
		res, changed = sm.applyGrantSession(p)
	case KindRevokeSession:
		var p RevokeSession
		if err := json.Unmarshal(body, &p); err != nil {
			return ApplyResult{}, fmt.Errorf("meta: unmarshal revoke_session: %w", err)
		}
		res, changed = sm.applyRevokeSession(p)
	case KindAcquireLock:
		var p AcquireLock
		if err := json.Unmarshal(body, &p); err != nil {
			return ApplyResult{}, fmt.Errorf("meta: unmarshal acquire_lock: %w", err)
		}
		res, changed = sm.applyAcquireLock(p)
	default:
		return ApplyResult{}, fmt.Errorf("meta: unknown proposal kind %q", kind)
	}

	res.CurrentVersion = sm.latest.Version
	sm.appliedIndex = index
	sm.appliedTerm = term
	if changed {
		if err := sm.persistLocked(); err != nil {
			return ApplyResult{}, fmt.Errorf("%w: %w", errDurablePersist, err)
		}
	}
	return res, nil
}

func (sm *topologySM) applyProposeDocument(p ProposeDocument) (ApplyResult, bool) {
	if p.ExpectedVersion != sm.latest.Version {
		return ApplyResult{
			ProposalID: p.ProposalID,
			Accepted:   false,
			Reason: fmt.Sprintf("version conflict: expected %d, latest %d",
				p.ExpectedVersion, sm.latest.Version),
			Code: proposalVersionConflict,
		}, false
	}
	doc := p.Doc.Clone()
	doc.Version = sm.latest.Version + 1
	if err := sm.validateDocument(doc); err != nil {
		return ApplyResult{ProposalID: p.ProposalID, Accepted: false, Reason: err.Error(), Code: proposalGuardrail}, false
	}
	if err := sm.commitLatestLocked(&doc); err != nil {
		return rejectProposal(p.ProposalID, proposalGuardrail, "%v", err), false
	}
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Doc: doc.Clone()}, true
}

func (sm *topologySM) applyAdmitNode(p AdmitNode) (ApplyResult, bool) {
	if existing := sm.latest.Node(p.Node.Name); existing != nil {
		if !p.ReplaceExisting {
			return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Doc: sm.latest.Clone()}, false
		}
		doc := sm.latest.Clone()
		node := doc.Node(p.Node.Name)
		node.State = topology.NodeActive
		node.UpdatedAt = p.At
		for i := range doc.Groups {
			changed := false
			for j := range doc.Groups[i].Replicas {
				if doc.Groups[i].Replicas[j].NodeName == p.Node.Name {
					id, err := doc.Groups[i].AllocateReplicaID()
					if err != nil {
						return rejectProposal(p.ProposalID, proposalGuardrail, "%v", err), false
					}
					doc.Groups[i].Replicas[j].ReplicaID = id
					doc.Groups[i].Replicas[j].UpdatedAt = p.At
					changed = true
				}
			}
			if changed {
				doc.Groups[i].UpdatedAt = p.At
			}
		}
		doc.Version = sm.latest.Version + 1
		if err := sm.validateDocument(doc); err != nil {
			return ApplyResult{ProposalID: p.ProposalID, Accepted: false, Reason: err.Error()}, false
		}
		if err := sm.commitLatestLocked(&doc); err != nil {
			return rejectProposal(p.ProposalID, proposalGuardrail, "%v", err), false
		}
		return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Doc: doc.Clone()}, true
	}
	doc := sm.latest.Clone()
	node := p.Node
	if node.State == "" {
		node.State = topology.NodeActive
	}
	node.CreatedAt = p.At
	node.UpdatedAt = p.At
	doc.Nodes = append(doc.Nodes, node)
	if err := doc.SetMemberID(node.Name, p.MemberID, p.At); err != nil {
		return rejectProposal(p.ProposalID, proposalGuardrail, "%v", err), false
	}
	if mg := doc.MetaGroup(); mg != nil {
		mg.UpdatedAt = p.At
	}
	doc.Version = sm.latest.Version + 1
	if err := sm.validateDocument(doc); err != nil {
		return ApplyResult{ProposalID: p.ProposalID, Accepted: false, Reason: err.Error()}, false
	}
	if err := sm.commitLatestLocked(&doc); err != nil {
		return rejectProposal(p.ProposalID, proposalGuardrail, "%v", err), false
	}
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Doc: doc.Clone()}, true
}

const historyRetention = 128

func (sm *topologySM) commitLatestLocked(doc *topology.Document) error {
	canonicalizeDocument(doc)
	history, err := sm.nextHistoryLocked(*doc)
	if err != nil {
		return err
	}
	sm.rememberAllocations(*doc)
	sm.latest = doc.Clone()
	sm.history = history
	sm.pruneOperatorsLocked()
	return nil
}

func (sm *topologySM) persistLocked() error {
	if sm.persister == nil {
		return nil
	}
	data, err := sm.marshalLocked()
	if err != nil {
		return err
	}
	return sm.persister.SaveState(data)
}

func (sm *topologySM) marshalLocked() ([]byte, error) {
	records := make([]operatorRecord, 0, len(sm.operators))
	for _, ex := range sm.operators {
		records = append(records, cloneOperatorRecord(*ex))
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].Version != records[j].Version {
			return records[i].Version < records[j].Version
		}
		return records[i].Attempt < records[j].Attempt
	})
	execs := make([]topology.Operator, 0, len(records))
	for _, rec := range records {
		execs = append(execs, operatorRecordView(rec))
	}
	activated := make([]string, 0, len(sm.activated))
	for name := range sm.activated {
		activated = append(activated, name)
	}
	sort.Strings(activated)
	return json.Marshal(smState{
		SchemaVersion: 1, Rules: sm.rules, RetiredGroups: sm.retiredSnapshot(), Allocations: sm.allocationSnapshot(),
		Latest:       sm.latest,
		Target:       sm.target,
		Active:       sm.active,
		History:      sm.history,
		Operators:    execs,
		Admissions:   sm.admissionSnapshotLocked(),
		Sessions:     sm.sessionSnapshotLocked(),
		Locks:        sm.lockSnapshotLocked(),
		Activated:    activated,
		AppliedIndex: sm.appliedIndex,
		AppliedTerm:  sm.appliedTerm,
	})
}

func (sm *topologySM) Snapshot() ([]byte, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.marshalLocked()
}

func (sm *topologySM) Restore(data []byte) error {
	var st smState
	if err := json.Unmarshal(data, &st); err != nil {
		return fmt.Errorf("meta: unmarshal sm state: %w", err)
	}
	if err := validateRestoredState(st, sm.rules); err != nil {
		return err
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.retired = make(map[string]uint64, len(st.RetiredGroups))
	for _, r := range st.RetiredGroups {
		sm.retired[r.ID] = r.Version
	}
	sm.allocations = make(map[string]uint64, len(st.Allocations))
	for _, a := range st.Allocations {
		sm.allocations[a.GroupID] = a.NextReplicaID
	}
	sm.latest = st.Latest.Clone()
	sm.target = st.Target.Clone()
	sm.active = st.Active.Clone()
	sm.history = st.History
	sm.operators = make(map[topology.OperatorID]*operatorRecord, len(st.Operators))
	for i := range st.Operators {
		rec := operatorFromView(st.Operators[i])
		sm.operators[rec.ID] = &rec
	}
	sm.admissions = make(map[string]*AdmissionRecord, len(st.Admissions))
	for i := range st.Admissions {
		record := st.Admissions[i]
		sm.admissions[admissionKey(record.NodeName, record.InstanceID)] = &record
	}
	sm.sessions = make(map[string]*sessionState, len(st.Sessions))
	for i := range st.Sessions {
		s := st.Sessions[i]
		sm.sessions[s.SessionID] = &s
	}
	sm.locks = make(map[string]string, len(st.Locks))
	for _, l := range st.Locks {
		sm.locks[l.Key] = l.SessionID
	}
	sm.activated = make(map[string]struct{}, len(st.Activated))
	for _, name := range st.Activated {
		sm.activated[name] = struct{}{}
	}
	sm.appliedIndex = st.AppliedIndex
	sm.appliedTerm = st.AppliedTerm
	return nil
}

func (sm *topologySM) HasActivated(name string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	_, ok := sm.activated[name]
	return ok
}

func (sm *topologySM) Current() topology.Document {
	return sm.Latest()
}

func (sm *topologySM) Latest() topology.Document {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.latest.Clone()
}

func (sm *topologySM) Target() topology.Document {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.target.Clone()
}

func (sm *topologySM) Active() topology.Document {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.active.Clone()
}

func (sm *topologySM) History(offset, limit int) []topology.Document {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if offset < 0 {
		offset = 0
	}
	n := len(sm.history) - offset
	if n <= 0 {
		return nil
	}
	if limit > 0 && limit < n {
		n = limit
	}
	out := make([]topology.Document, 0, n)
	for i := range n {
		out = append(out, sm.history[len(sm.history)-1-offset-i].Clone())
	}
	return out
}

func (sm *topologySM) ExclusionVersion(name string) uint64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	excludedIdx := -1
	for i := len(sm.history) - 1; i >= 0; i-- {
		if n := sm.history[i].Node(name); n != nil && n.State == topology.NodeExcluded {
			excludedIdx = i
			continue
		}
		if excludedIdx >= 0 {
			break
		}
	}
	if excludedIdx <= 0 {
		return 0
	}
	return sm.history[excludedIdx].Version
}

func (sm *topologySM) Get(version uint64) (topology.Document, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for i := range sm.history {
		if sm.history[i].Version == version {
			return sm.history[i].Clone(), true
		}
	}
	return topology.Document{}, false
}

func (sm *topologySM) Operator(id topology.OperatorID) (topology.OperatorStatus, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	ex, ok := sm.operators[id]
	if !ok {
		return topology.OperatorStatus{}, false
	}
	return operatorStatus(*ex), true
}

func (sm *topologySM) OperatorRecord(id topology.OperatorID) (topology.Operator, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	ex, ok := sm.operators[id]
	if !ok {
		return topology.Operator{}, false
	}
	return operatorRecordView(*ex), true
}

func (sm *topologySM) CurrentOperator() (topology.Operator, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	var current *operatorRecord
	for _, ex := range sm.operators {
		if current == nil || ex.Version > current.Version ||
			(ex.Version == current.Version && ex.Attempt > current.Attempt) {
			current = ex
		}
	}
	if current == nil {
		return topology.Operator{}, false
	}
	return operatorRecordView(*current), true
}

func (sm *topologySM) AppliedState() (index, term uint64) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.appliedIndex, sm.appliedTerm
}
