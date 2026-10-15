package metaengine

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

var errDurablePersist = errors.New("meta: durable state persist failed")

type statePersister interface {
	SaveState(data []byte) error
}

// ApplyResult carries the deterministic acceptance, refusal and evidence from a
// metadata proposal.
type ApplyResult struct {
	ExpectedVersion uint64                    `json:"expected_version,omitempty"`
	RebasedTo       uint64                    `json:"rebased_to,omitempty"`
	Grants          []topology.AuthorityGrant `json:"grants,omitempty"`
	ProposalID      string
	CurrentVersion  uint64
	Accepted        bool
	Reason          string
	Code            proposalErrorCode
	Dropped         bool
	Doc             topology.Document
	OperatorID      topology.OperatorID
	Grant           *topology.AuthorityGrant `json:"grant,omitempty"`
	Admission       AdmissionRecord
	Placement       *topology.PlacementReport `json:"placement,omitempty"`
}

const (
	errorFormat            = "%v"
	metaStateSchemaVersion = 3
)

type proposalErrorCode string

const (
	proposalVersionConflict  proposalErrorCode = "version_conflict"
	proposalAuthorization    proposalErrorCode = "authorization"
	proposalSuperseded       proposalErrorCode = "superseded"
	proposalGuardrail        proposalErrorCode = "guardrail"
	proposalOverloaded       proposalErrorCode = "overloaded"
	proposalAuthorityPending proposalErrorCode = "authority_pending"
	proposalAuthorityFenced  proposalErrorCode = "authority_fenced"
	proposalUnsupported      proposalErrorCode = "unsupported"
)

type operatorRecord struct {
	ID            topology.OperatorID      `json:"id,omitempty"`
	Version       uint64                   `json:"version"`
	Attempt       uint32                   `json:"attempt,omitempty"`
	State         topology.OperatorState   `json:"state"`
	Steps         []topology.Step          `json:"steps,omitempty"`
	VoterBases    []topology.PlanVoterBase `json:"voter_bases,omitempty"`
	CurrentStep   int                      `json:"current_step,omitempty"`
	GroupID       string                   `json:"group_id,omitempty"`
	NodeName      string                   `json:"node_name,omitempty"`
	WaitingReason string                   `json:"waiting_reason,omitempty"`
	Error         string                   `json:"error,omitempty"`
	Origin        topology.Origin          `json:"origin,omitempty"`
	Actor         string                   `json:"actor,omitempty"`
	StartedAt     time.Time                `json:"started_at,omitempty"`
	UpdatedAt     time.Time                `json:"updated_at,omitempty"`
}

type topologySM struct {
	mu        sync.RWMutex
	persister statePersister
	rules     topology.Rules

	latest        topology.Document
	target        topology.Document
	active        topology.Document
	history       []topology.Document
	operators     map[topology.OperatorID]*operatorRecord
	admissions    map[string]*AdmissionRecord
	voterSwitches map[string]topology.VoterSwitchState

	sessions map[string]*sessionState // sessionID → session
	locks    map[string]string        // lock key → holding sessionID

	retired     map[string]uint64
	allocations map[string]uint64
	activated   map[string]struct{} // node name → has activated at least once

	authority  authorityCatalog
	metaOwners map[uint64]string // rebuilt on mutation/restore; no per-frame catalog scans

	appliedIndex uint64
	appliedTerm  uint64
}

func newTopologySM(persister statePersister, rules topology.Rules) *topologySM {
	return &topologySM{
		persister:     persister,
		authority:     newAuthorityCatalog(),
		rules:         rules,
		retired:       make(map[string]uint64),
		allocations:   make(map[string]uint64),
		operators:     make(map[topology.OperatorID]*operatorRecord),
		admissions:    make(map[string]*AdmissionRecord),
		voterSwitches: make(map[string]topology.VoterSwitchState),
		sessions:      make(map[string]*sessionState),
		locks:         make(map[string]string),
		activated:     make(map[string]struct{}),
	}
}

type smState struct {
	Scoped           *scopedState                `json:"-"` // isolated v4 codec; replaces this wrapper at Task 58
	AuthorityRecords []authorityRecord           `json:"authority_records,omitempty"`
	AuthorityFloors  []resourceFloor             `json:"authority_floors,omitempty"`
	Revocations      []revocationRecord          `json:"revocations,omitempty"`
	SchemaVersion    uint32                      `json:"schema_version"`
	Rules            topology.Rules              `json:"rules"`
	RetiredGroups    []retiredGroup              `json:"retired_groups"`
	Allocations      []allocationRecord          `json:"allocations"`
	Latest           topology.Document           `json:"latest"`
	Target           topology.Document           `json:"target"`
	Active           topology.Document           `json:"active"`
	History          []topology.Document         `json:"history"`
	Operators        []topology.Operator         `json:"executions"`
	Admissions       []AdmissionRecord           `json:"admissions,omitempty"`
	Sessions         []sessionState              `json:"sessions"`
	Locks            []lockEntry                 `json:"locks"`
	Activated        []string                    `json:"activated,omitempty"` // sorted node names that ever activated
	VoterSwitches    []topology.VoterSwitchState `json:"voter_switches,omitempty"`
	AppliedIndex     uint64                      `json:"applied_index"`
	AppliedTerm      uint64                      `json:"applied_term"`
}

func (sm *topologySM) Apply(index, term uint64, data []byte) (ApplyResult, error) {
	decoded, err := decodeEnvelope(data)
	kind, body := decoded.Kind, decoded.Body
	if err != nil {
		return ApplyResult{}, err
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if index <= sm.appliedIndex {
		return ApplyResult{}, nil
	}

	outcome, err := sm.applyProposal(index, kind, body)
	if err != nil {
		return ApplyResult{}, err
	}
	res, changed := outcome.result, outcome.changed

	res.CurrentVersion = sm.latest.Version
	sm.appliedIndex = index
	sm.appliedTerm = term
	if changed {
		sm.metaOwners = sm.metaMemberOwnersLocked()
		if err := sm.persistLocked(); err != nil {
			return ApplyResult{}, fmt.Errorf("%w: %w", errDurablePersist, err)
		}
	}
	return res, nil
}

func (sm *topologySM) applyProposeDocument(p ProposeDocument) (ApplyResult, bool) {
	// Rebase provenance is always FSM-derived; raw staging cannot assert it.
	if p.Doc.RebasedFrom != 0 {
		return rejectProposalError(p.ProposalID, proposalGuardrail, fault.ErrInvalid), false
	}
	// Remove at Task 58: the current format cannot retain typed intent provenance.
	if p.Intent != nil {
		return rejectProposalError(p.ProposalID, proposalUnsupported, fault.ErrUnsupported), false
	}
	if p.ExpectedVersion != sm.latest.Version {
		return rejectVersionConflict(p.ProposalID, p.ExpectedVersion, sm.latest.Version), false
	}
	doc := p.Doc.Clone()
	doc.Version = sm.latest.Version + 1
	if err := sm.validateDocument(doc); err != nil {
		return rejectProposalError(p.ProposalID, proposalGuardrail, err), false
	}
	report, err := placementResult(sm.target, doc, topology.AffectedPlacementGroups(sm.latest, doc))
	if err != nil {
		return rejectProposalError(p.ProposalID, proposalGuardrail, err), false
	}
	if err := sm.commitLatestLocked(&doc); err != nil {
		return rejectProposal(p.ProposalID, proposalGuardrail, errorFormat, err), false
	}
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Doc: doc.Clone(), Placement: report}, true
}

func (sm *topologySM) applyAdmitNode(p AdmitNode) (ApplyResult, bool) {
	if existing := sm.latest.Node(p.Node.Name); existing != nil {
		if !p.ReplaceExisting {
			return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Doc: sm.latest.Clone()}, false
		}
		return sm.replaceAdmittedNode(p)
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
		return rejectProposal(p.ProposalID, proposalGuardrail, errorFormat, err), false
	}
	if mg := doc.MetaGroup(); mg != nil {
		mg.UpdatedAt = p.At
	}
	doc.Version = sm.latest.Version + 1
	if err := sm.validateDocument(doc); err != nil {
		return rejectProposalError(p.ProposalID, proposalGuardrail, err), false
	}
	if err := sm.commitLatestLocked(&doc); err != nil {
		return rejectProposal(p.ProposalID, proposalGuardrail, errorFormat, err), false
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
	slices.SortFunc(records, func(a, b operatorRecord) int {
		return cmp.Or(cmp.Compare(a.Version, b.Version), cmp.Compare(a.Attempt, b.Attempt))
	})
	execs := make([]topology.Operator, 0, len(records))
	for _, rec := range records {
		execs = append(execs, operatorRecordView(rec))
	}
	activated := make([]string, 0, len(sm.activated))
	for name := range sm.activated {
		activated = append(activated, name)
	}
	slices.Sort(activated)
	authority := sm.authority.snapshot()
	return json.Marshal(smState{
		AuthorityRecords: authority.records, AuthorityFloors: authority.floors, Revocations: authority.revocations,
		SchemaVersion: metaStateSchemaVersion, Rules: sm.rules,
		RetiredGroups: sm.retiredSnapshot(), Allocations: sm.allocationSnapshot(),
		Latest:        sm.latest,
		Target:        sm.target,
		Active:        sm.active,
		History:       sm.history,
		Operators:     execs,
		Admissions:    sm.admissionSnapshotLocked(),
		Sessions:      sm.sessionSnapshotLocked(),
		Locks:         sm.lockSnapshotLocked(),
		Activated:     activated,
		VoterSwitches: sm.voterSwitchSnapshotLocked(),
		AppliedIndex:  sm.appliedIndex,
		AppliedTerm:   sm.appliedTerm,
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
	authority, err := restoreAuthority(st)
	if err != nil {
		return err
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.authority = authority
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
	sm.voterSwitches = make(map[string]topology.VoterSwitchState, len(st.VoterSwitches))
	for _, state := range st.VoterSwitches {
		sm.voterSwitches[state.GroupID] = state.Clone()
	}
	for i := range st.Admissions {
		record := st.Admissions[i]
		sm.admissions[admissionKey(record.NodeName, record.InstanceID)] = &record
	}
	sm.restoreSessionsLocked(st)
	sm.appliedIndex = st.AppliedIndex
	sm.appliedTerm = st.AppliedTerm
	sm.metaOwners = sm.metaMemberOwnersLocked()
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
	position := max(offset, 0)
	n := len(sm.history) - position
	if n <= 0 {
		return nil
	}
	if limit > 0 && limit < n {
		n = limit
	}
	out := make([]topology.Document, 0, n)
	for i := range n {
		out = append(out, sm.history[len(sm.history)-1-position-i].Clone())
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
		if current == nil || ex.Version > current.Version || (ex.Version == current.Version &&
			ex.Attempt > current.Attempt) {
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

func (sm *topologySM) replaceAdmittedNode(p AdmitNode) (ApplyResult, bool) {
	doc := sm.latest.Clone()
	node := doc.Node(p.Node.Name)
	node.State = topology.NodeActive
	node.UpdatedAt = p.At
	if err := reseatNodeReplicas(&doc, p); err != nil {
		return rejectProposal(p.ProposalID, proposalGuardrail, errorFormat, err), false
	}
	doc.Version = sm.latest.Version + 1
	if err := sm.validateDocument(doc); err != nil {
		return rejectProposalError(p.ProposalID, proposalGuardrail, err), false
	}
	if err := sm.commitLatestLocked(&doc); err != nil {
		return rejectProposal(p.ProposalID, proposalGuardrail, errorFormat, err), false
	}
	return ApplyResult{ProposalID: p.ProposalID, Accepted: true, Doc: doc.Clone()}, true
}

func reseatNodeReplicas(doc *topology.Document, p AdmitNode) error {
	for i := range doc.Groups {
		changed := false
		for j := range doc.Groups[i].Replicas {
			if doc.Groups[i].Replicas[j].NodeName == p.Node.Name {
				id, err := doc.Groups[i].AllocateReplicaID()
				if err != nil {
					return err
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
	return nil
}

func (sm *topologySM) restoreSessionsLocked(st smState) {
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
}
