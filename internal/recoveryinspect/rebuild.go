package recoveryinspect

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftengine"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/internal/walbackend"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// InspectRebuild requires one validated local donor for every approved group.
func InspectRebuild(ctx context.Context, state SourceState, request model.Request) (model.AcceptedPlan, error) {
	accepted := baseAccepted(state, request)
	wal := walInventory{state: &state, accepted: &accepted, sources: make(map[string]model.SeedSource),
		reports: make(map[string]model.GroupRecovery)}
	wal.addRequiredAssertions()
	wal.addMetadataReport()
	if err := systemInventory(ctx, &state, &accepted); err != nil {
		return accepted, err
	}
	if err := wal.scan(ctx); err != nil {
		return accepted, err
	}
	wal.finishCoverage()
	result, err := finishInspection(state, accepted)
	return result, errors.Join(err, wal.cause)
}

// InspectReset archives proven ownership without promising application replay.
func InspectReset(ctx context.Context, state SourceState, request model.Request) (model.AcceptedPlan, error) {
	accepted := baseAccepted(state, request)
	wal := walInventory{state: &state, accepted: &accepted}
	if request.ClusterID == state.identity.ClusterID {
		return accepted, fault.ErrIdentityMismatch
	}
	if err := systemInventory(ctx, &state, &accepted); err != nil {
		return accepted, err
	}
	if err := wal.scan(ctx); err != nil {
		return accepted, err
	}
	result, err := finishInspection(state, accepted)
	return result, errors.Join(err, wal.cause)
}

func baseAccepted(state SourceState, request model.Request) model.AcceptedPlan {
	return model.AcceptedPlan{Request: request, Paths: state.paths, Resources: state.resources,
		Source: state.identity,
		Rules:  state.inventory.Rules, Approved: state.inventory.Approved.Clone(),
		Floors:  slices.Clone(state.inventory.Floors),
		Retired: slices.Clone(state.inventory.Retired), Archived: state.inventory.Archived}
}

func finishInspection(state SourceState, input model.AcceptedPlan) (model.AcceptedPlan, error) {
	accepted := model.CanonicalAccepted(input)
	fingerprint, err := sourceFingerprint(state, accepted)
	if err != nil {
		return accepted, err
	}
	accepted.Fingerprint = fingerprint
	if accepted.Request.Mode == model.ResetForJoin {
		accepted.Identity = topology.Identity{NodeName: state.identity.NodeName,
			ClusterID: accepted.Request.ClusterID}
	} else {
		floor := uint64(0)
		for _, entry := range accepted.Floors {
			if entry.GroupID == topology.MetaGroupID {
				floor = entry.NextID
			}
		}
		accepted.Identity, err = DeriveIdentity(state.identity, accepted.Request.RecoveryID, fingerprint,
			accepted.Request.ClusterID, floor)
		if err != nil {
			return accepted, err
		}
	}
	for _, issue := range accepted.Issues {
		if issue.Blocking {
			return accepted, fault.New(fault.Unavailable, "source inspection contains blockers", nil)
		}
	}
	return accepted, nil
}

func (w *walInventory) donor(ctx context.Context, version uint64, fact *localArtifact) error {
	name, id := fact.Name, fact.Identity
	group := w.accepted.Approved.Group(id.GroupID)
	if group.Storage != topology.StorageDefault {
		return nil
	}
	evidence, err := walbackend.ReadRecovery(ctx, w.state.paths.LoadWALDir, id.GroupID, model.ReadBounds{
		MaxRecordBytes: w.state.resources.MaxRecordBytes, MaxGroupBytes: w.state.resources.MaxGroupBytes,
	})
	if err != nil {
		w.failed("unreadable_group", id.GroupID, "required WAL snapshot or committed prefix is unreadable", err)
		w.reports[id.GroupID] = model.GroupRecovery{GroupID: id.GroupID, Source: id}
		return ctx.Err()
	}
	if evidence.Identity != id {
		w.issue("contradictory_replica", id.GroupID, "WAL donor identity changed during inspection")
		return nil
	}
	fact.Digest = evidence.Digest
	source := seedSource(evidence.Recovery, model.ReplicaEvidence{Identity: id,
		MemberID: w.state.identity.MemberID, SourceVersion: version}, evidence.Digest)
	source.LastLog = evidence.LastLog
	source.Bytes = evidence.Bytes
	report := groupReport(source)
	w.reports[id.GroupID] = report
	for _, issue := range evidence.Issues {
		issue.GroupID = id.GroupID
		w.accepted.Issues = append(w.accepted.Issues, issue)
	}
	if evidence.TornTail != nil {
		w.accepted.Issues = append(w.accepted.Issues, model.Issue{Code: "torn_final_tail",
			GroupID: id.GroupID,
			Message: fmt.Sprintf("torn WAL %s at offset %d; retained commit %d unchanged",
				evidence.TornTail.File, evidence.TornTail.Offset, source.Commit.Index),
			Remedy: model.RemedyInvestigate})
	}
	prefix, err := model.CommittedPrefix(evidence.Recovery)
	if err == nil {
		_, err = raftengine.Replay(ctx, raftengine.ReplayOptions{Identity: id,
			MemberID: w.state.identity.MemberID, InstanceID: "offline-inspection"}, prefix,
			raftengine.DiscardMachine{})
	}
	if err != nil {
		w.failed("invalid_group_history", id.GroupID,
			"source native membership or authority replay is invalid", err)
		report.Usable = false
		w.reports[id.GroupID] = report
		return ctx.Err()
	}
	if err := eligibleSurvivor(*w.state, id.GroupID); err != nil {
		w.issue("ineligible_survivor", id.GroupID, err.Error())
		report.Usable = false
		w.reports[id.GroupID] = report
		return nil
	}
	w.sources[id.GroupID] = source
	w.accepted.Archive = append(w.accepted.Archive, model.ArchiveEntry{Root: "wal", Name: name,
		Kind: "directory", Classification: "source", EvidenceDigest: evidence.Digest})
	return nil
}

func seedSource(retained storage.Recovery, binding model.ReplicaEvidence, digest string) model.SeedSource {
	id, member, version := binding.Identity, binding.MemberID, binding.SourceVersion
	snapshot := retained.Snapshot.Metadata
	commit := model.Position{Index: retained.HardState.Commit, Term: snapshot.Term}
	last := model.Position{Index: snapshot.Index, Term: snapshot.Term}
	for _, entry := range retained.Entries {
		last = model.Position{Index: entry.Index, Term: entry.Term}
		if entry.Index == commit.Index {
			commit.Term = entry.Term
		}
	}
	return model.SeedSource{GroupID: id.GroupID, Identity: id, MemberID: member, SourceVersion: version,
		Snapshot: model.Position{Index: snapshot.Index, Term: snapshot.Term}, Commit: commit, LastLog: last,
		HardStateTerm: retained.HardState.Term, Digest: digest, Bytes: recoveryBytes(retained)}
}

func groupReport(source model.SeedSource) model.GroupRecovery {
	return model.GroupRecovery{GroupID: source.GroupID, Source: source.Identity, Snapshot: source.Snapshot,
		Commit: source.Commit, LastLog: source.LastLog, Bytes: source.Bytes, Usable: true}
}

func eligibleSurvivor(state SourceState, group string) error {
	doc := state.inventory.Approved
	node := doc.Node(state.identity.NodeName)
	if node == nil {
		return fault.New(fault.Invalid, "survivor is absent from approved node definitions", nil)
	}
	if group != topology.MetaGroupID && topology.NodeReserved(*node) {
		return fault.New(fault.Invalid, "reserved survivor cannot acquire a fresh load identity", nil)
	}
	report, err := topology.EvaluatePlacement(topology.PlacementInput{Approved: doc, Desired: doc, GroupID: group})
	if err != nil {
		return err
	}
	for _, candidate := range report.Candidates {
		if candidate.NodeName == node.Name && candidate.CanVote {
			return nil
		}
	}
	return fault.New(fault.Invalid,
		"survivor cannot vote under approved placement, type, hosting or learner restrictions", nil)
}

func (w *walInventory) addRequiredAssertions() {
	for _, id := range w.accepted.Request.RequiredGroups {
		if w.accepted.Approved.Group(id) == nil {
			w.issue("required_group_absent", id, "asserted group is absent from approved Target")
			w.accepted.Groups = append(w.accepted.Groups, model.GroupRecovery{GroupID: id})
		}
	}
}

func (w *walInventory) addMetadataReport() {
	metaID := storage.Identity{ClusterID: w.state.identity.ClusterID, NodeName: w.state.identity.NodeName,
		GroupID: topology.MetaGroupID, ReplicaID: w.state.identity.MemberID}
	meta := seedSource(w.state.metadata, model.ReplicaEvidence{Identity: metaID,
		MemberID: w.state.identity.MemberID, SourceVersion: w.accepted.Approved.Version}, w.state.metadataDigest)
	w.accepted.Sources = append(w.accepted.Sources, meta)
	metaReport := groupReport(meta)
	proof, proofErr := selectReplicaEvidence(w.state.inventory.Replicas, metaID)
	if proofErr != nil || proof.MemberID != w.state.identity.MemberID ||
		(proof.Classification != "approved" && proof.Classification != "proven") {
		w.issue("invalid_metadata_owner", topology.MetaGroupID,
			"metadata pin has no exact approved or proven member identity")
		metaReport.Usable = false
	}
	if err := eligibleSurvivor(*w.state, topology.MetaGroupID); err != nil {
		w.issue("ineligible_survivor", topology.MetaGroupID, err.Error())
		metaReport.Usable = false
	}
	w.accepted.Groups = append(w.accepted.Groups, metaReport)
}

func (w *walInventory) finishCoverage() {
	floors := make(map[string]uint64, len(w.accepted.Floors))
	for _, floor := range w.accepted.Floors {
		floors[floor.GroupID] = floor.NextID
	}
	for _, group := range w.accepted.Approved.LoadGroups() {
		if floors[group.ID] == math.MaxUint64 {
			w.issue("identity_exhausted", group.ID,
				"replica allocation floor has no fresh successor identity")
		}
		report, found := w.reports[group.ID]
		if !found {
			report = model.GroupRecovery{GroupID: group.ID}
		}
		if group.Storage != topology.StorageDefault {
			w.failed("unsupported_storage", group.ID, "rebuild requires a durable Node-owned WAL group",
				fault.ErrUnsupported)
		}
		if source, ok := w.sources[group.ID]; ok {
			w.accepted.Sources = append(w.accepted.Sources, source)
		} else {
			w.issue("missing_group_coverage", group.ID,
				"survivor has no usable approved or proven local WAL donor")
		}
		w.accepted.Groups = append(w.accepted.Groups, report)
	}
}
