package metaengine

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"math"
	"slices"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/internal/recoveryplacement"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

// RecoveryState returns owned replicated provenance, never local replay observations.
func (m *Machine) RecoveryState() *model.ImportState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.scoped.recoveryPlacementState()
}

func validRecoveryDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && strings.ToLower(value) == value
}

func (s *scopedState) applyRecoveryImport(index uint64, p model.ImportProposal) (proposalApplication, error) {
	digest, err := model.ImportRequestDigest(p)
	if err != nil {
		return proposalApplication{}, err
	}
	if s.Recovery != nil {
		if s.Recovery.RequestDigest != digest {
			return proposalApplication{}, fault.ErrConflict
		}
		return proposalApplication{result: scopedApprovalResult(p.ProposalID,
			s.indexes.first[s.Recovery.Version])}, nil
	}
	if err := s.validateImportFounding(p); err != nil {
		return proposalApplication{}, err
	}
	next := *s
	candidate, err := prepareImportDefinitions(p, digest)
	if err != nil {
		return proposalApplication{}, err
	}
	doc, record := candidate.document, candidate.record
	next.Recovery = record
	placement, err := recoveryplacement.FromImport(record, doc)
	if err != nil {
		return proposalApplication{}, err
	}
	next.importPlacement = &placement
	if err := next.installImportFloors(p); err != nil {
		return proposalApplication{}, err
	}
	if err := next.validateStagedDocument(doc); err != nil {
		return proposalApplication{}, err
	}
	next.installStagedDocument(doc, nil)
	next.indexes, err = next.buildIndexes()
	if err != nil {
		return proposalApplication{}, err
	}
	approval, err := next.prepareRecoveryApproval(index, p, placement)
	if err != nil {
		return proposalApplication{}, err
	}
	approved, err := prepareScopedApproval(&next, next.Latest, approval, explicitApproval)
	if err != nil {
		return proposalApplication{}, err
	}
	approved.AppliedIndex = index
	approved.importPlacement = nil
	approved.indexes, err = prepareScopedGraph(approved, approved.Rules)
	if err != nil {
		return proposalApplication{}, err
	}
	*s = *approved
	return proposalApplication{changed: true, result: scopedApprovalResult(p.ProposalID, s.indexes.first[2])}, nil
}

func (s *scopedState) prepareRecoveryApproval(index uint64, p model.ImportProposal,
	placement recoveryplacement.Context) (BeginOperationProposal, error) {
	input := s.planningDeclarations()
	input.Native = map[string]topology.NativeState{topology.MetaGroupID: {
		GroupID: topology.MetaGroupID, ReplicaID: p.Survivor.ReplicaID, Applied: index, ConfState: *s.nativeConf,
	}}
	plan, err := recoveryplacement.BuildPlan(placement, input, s.Latest, topology.PlanOptions{Rules: s.Rules})
	if err != nil {
		return BeginOperationProposal{}, err
	}
	return BeginOperationProposal{
		Version: 2, Plan: plan, At: p.At, Actor: p.Actor, Origin: topology.OriginOperator,
		ProposalID: p.ProposalID, Scopes: s.capturePlanScopes(topology.AffectedGroups(s.Target, s.Latest)),
	}, nil
}

func (s *scopedState) validateImportFounding(p model.ImportProposal) error {
	if err := validateImportRequest(p); err != nil {
		return err
	}
	if s.Latest.Version != 1 || s.Target.Version != 1 || len(s.Target.Groups) != 1 ||
		s.Target.Origin != topology.OriginFounding || s.Target.ClusterID != p.Survivor.ClusterID {
		return fault.ErrOperationAuthorization
	}
	meta := s.Target.MetaGroup()
	if meta == nil || len(meta.Replicas) != 1 || meta.Replicas[0].NodeName != p.Survivor.NodeName ||
		meta.Replicas[0].ReplicaID != p.Survivor.ReplicaID || meta.Replicas[0].Role != topology.ReplicaVoter {
		return fault.ErrIdentityMismatch
	}
	if !exactRecoveryNative(s.nativeConf, p.Survivor.ReplicaID) {
		return fault.ErrIdentityMismatch
	}
	if err := s.validateRecoveryFoundingProof(p); err != nil {
		return err
	}
	if err := p.ApprovedSource.Validate(s.Rules); err != nil {
		return err
	}
	if p.ApprovedSource.Version == 0 || p.ApprovedSource.ClusterID != p.Source.ClusterID ||
		p.ApprovedSource.Node(p.Source.NodeName) == nil {
		return fault.ErrIdentityMismatch
	}
	return nil
}

func validateImportRequest(p model.ImportProposal) error {
	if p.ProposalID == "" || p.ExpectedVersion != 1 || p.Schema != model.SchemaVersion ||
		p.At.IsZero() || strings.TrimSpace(p.Actor) == "" || strings.TrimSpace(p.Comment) == "" ||
		model.ValidateRecoveryID(p.RecoveryID) != nil || !validRecoveryDigest(p.PlanDigest) {
		return fault.ErrInvalid
	}
	if p.Survivor.GroupID != topology.MetaGroupID || p.Source.InstanceID != "" || p.Source.MemberID == 0 ||
		p.Survivor.ReplicaID <= p.Source.MemberID || p.Source.ClusterID == p.Survivor.ClusterID ||
		p.Source.NodeName != p.Survivor.NodeName {
		return fault.ErrOperationAuthorization
	}
	return nil
}

func exactRecoveryNative(conf *raftpb.ConfState, id uint64) bool {
	return conf != nil && len(conf.Voters) == 1 && conf.Voters[0] == id &&
		len(conf.Learners) == 0 && len(conf.VotersOutgoing) == 0 && len(conf.LearnersNext) == 0 && !conf.AutoLeave
}

func (s *scopedState) validateRecoveryFoundingProof(p model.ImportProposal) error {
	operation := s.indexes.operations[s.indexes.first[1]]
	if operation == nil || operation.Origin != topology.OriginFounding ||
		operation.Status.State != topology.OperationSucceeded || !slices.Contains(s.Activated,
		p.Survivor.NodeName) {
		return fault.ErrOperationAuthorization
	}
	for _, admission := range s.Admissions {
		if admission.NodeName == p.Survivor.NodeName && admission.MemberID == p.Survivor.ReplicaID &&
			admission.State == AdmissionCompleted {
			return nil
		}
	}
	return fault.ErrOperationAuthorization
}

type recoveryImportCandidate struct {
	document topology.Document
	record   *model.ImportState
}

type recoveryImportInputs struct {
	floors  map[string]uint64
	sources map[string]model.SeedSource
}

func importDefinitionInputs(p model.ImportProposal) (recoveryImportInputs, error) {
	inputs := recoveryImportInputs{floors: make(map[string]uint64), sources: make(map[string]model.SeedSource)}
	for _, floor := range p.Floors {
		if inputs.floors[floor.GroupID] != 0 || floor.NextID == 0 || topology.ValidateID(floor.GroupID) != nil {
			return recoveryImportInputs{}, fault.ErrInvalid
		}
		inputs.floors[floor.GroupID] = floor.NextID
	}
	if inputs.floors[topology.MetaGroupID] != p.Survivor.ReplicaID {
		return recoveryImportInputs{}, fault.ErrIdentityMismatch
	}
	for _, source := range p.Sources {
		if source.SourceVersion > p.ApprovedSource.Version || inputs.sources[source.GroupID].GroupID != "" ||
			topology.ValidateLoadID(source.GroupID) != nil || model.ValidateSeedSource(source, p.Source) != nil {
			return recoveryImportInputs{}, fault.ErrInvalid
		}
		inputs.sources[source.GroupID] = source
	}
	if len(inputs.sources) != len(p.ApprovedSource.LoadGroups()) {
		return recoveryImportInputs{}, fault.ErrInvalid
	}
	return inputs, nil
}

func prepareImportDefinitions(p model.ImportProposal, digest string) (recoveryImportCandidate, error) {
	inputs, err := importDefinitionInputs(p)
	if err != nil {
		return recoveryImportCandidate{}, err
	}
	doc := p.ApprovedSource.Clone()
	doc.ClusterID, doc.Version, doc.ParentVersion, doc.RebasedFrom = p.Survivor.ClusterID, 2, 1, 0
	doc.Origin, doc.ChangedBy, doc.Comment = topology.OriginOperator, p.Actor, p.Comment
	doc.CreatedAt, doc.Audit = p.At, nil
	for i := range doc.Nodes {
		doc.Nodes[i].State = topology.NodeExcluded
		if doc.Nodes[i].Name == p.Survivor.NodeName {
			doc.Nodes[i].State = topology.NodeActive
		}
	}
	record := &model.ImportState{
		Schema: model.SchemaVersion, RecoveryID: p.RecoveryID, PlanDigest: p.PlanDigest, RequestDigest: digest,
		Source: p.Source, Survivor: p.Survivor, Version: 2, Phase: "importing", AcceptedAt: p.At,
	}
	for i := range doc.Groups {
		group := &doc.Groups[i]
		id := inputs.floors[group.ID]
		if err := replaceRecoveryReplicas(group, id, p); err != nil {
			return recoveryImportCandidate{}, err
		}
		if group.ID != topology.MetaGroupID {
			source, found := inputs.sources[group.ID]
			if !found {
				return recoveryImportCandidate{}, fault.ErrInvalid
			}
			record.Groups = append(record.Groups, model.ImportGroup{
				GroupID: group.ID, Source: source, Destination: storage.Identity{
					ClusterID: doc.ClusterID, NodeName: p.Survivor.NodeName, GroupID: group.ID, ReplicaID: id,
				},
			})
		}
		policy, err := recoveryplacement.PolicyDigest(doc, group.ID)
		if err != nil {
			return recoveryImportCandidate{}, err
		}
		record.Policies = append(record.Policies, model.PolicyState{GroupID: group.ID, PolicyDigest: policy})
	}
	canonicalizeDocument(&doc)
	slices.SortFunc(record.Groups, func(a, b model.ImportGroup) int { return cmp.Compare(a.GroupID, b.GroupID) })
	slices.SortFunc(record.Policies, func(a, b model.PolicyState) int { return cmp.Compare(a.GroupID, b.GroupID) })
	return recoveryImportCandidate{document: doc, record: record}, nil
}

func replaceRecoveryReplicas(group *topology.Group, id uint64, p model.ImportProposal) error {
	if id == 0 || id == math.MaxUint64 || group.Storage != topology.StorageDefault {
		return fault.ErrUnsupported
	}
	if id < group.NextReplicaID {
		return fault.ErrIdentityMismatch
	}
	for _, replica := range group.Replicas {
		if id <= replica.ReplicaID {
			return fault.ErrIdentityMismatch
		}
	}
	group.Replicas = []topology.Replica{{
		NodeName: p.Survivor.NodeName, ReplicaID: id, Role: topology.ReplicaVoter, CreatedAt: p.At, UpdatedAt: p.At,
	}}
	group.NextReplicaID, group.CreatedAt, group.UpdatedAt = id+1, p.At, p.At
	return nil
}

func (s *scopedState) installImportFloors(p model.ImportProposal) error {
	floors := s.Allocations
	s.Allocations = slices.Clone(floors)
	for _, incoming := range p.Floors {
		found := false
		for i := range s.Allocations {
			if s.Allocations[i].GroupID == incoming.GroupID {
				s.Allocations[i].NextReplicaID = max(s.Allocations[i].NextReplicaID, incoming.NextID)
				found = true
				break
			}
		}
		if !found {
			s.Allocations = append(s.Allocations, allocationRecord{GroupID: incoming.GroupID,
				NextReplicaID: incoming.NextID})
		}
	}
	s.RetiredGroups = slices.Clone(s.RetiredGroups)
	seen := make(map[string]bool)
	for _, group := range p.Retired {
		if seen[group] || topology.ValidateLoadID(group) != nil || p.ApprovedSource.Group(group) != nil {
			return fault.ErrInvalid
		}
		seen[group] = true
		found := slices.ContainsFunc(p.Floors,
			func(floor model.AllocationFloor) bool { return floor.GroupID == group })
		if !found {
			return fault.ErrInvalid
		}
		s.RetiredGroups = append(s.RetiredGroups, retiredGroup{ID: group, Version: 1})
	}
	return nil
}
