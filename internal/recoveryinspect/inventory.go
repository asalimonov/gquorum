package recoveryinspect

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recoverydisk"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/internal/walbackend"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

const classRetired = "retired"
const classDeleted = "approved_deleted"

type localArtifact struct {
	Root           string
	Name           string
	Classification string
	Identity       storage.Identity
	Digest         string
}

type walInventory struct {
	state    *SourceState
	accepted *model.AcceptedPlan
	sources  map[string]model.SeedSource
	reports  map[string]model.GroupRecovery
	cause    error
}

func scanDirectory(ctx context.Context, path string, maxNames uint64, visit func(os.DirEntry) error) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	remaining := maxNames
	const page = 128
	for {
		entries, readErr := dir.ReadDir(page)
		if err := visitInventoryPage(ctx, entries, &remaining, visit); err != nil {
			return err
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func (w *walInventory) scan(ctx context.Context) error {
	if w.state.paths.LoadWALDir == "" {
		return nil
	}
	return scanDirectory(ctx, w.state.paths.LoadWALDir, w.state.resources.MaxMetadataBytes,
		func(entry os.DirEntry) error {
			if entry.Name() == recoverydisk.LockName || entry.Name() == recoverydisk.Namespace {
				return nil
			}
			fact := localArtifact{Root: "wal", Name: entry.Name(), Classification: "unrecognized"}
			defer func() { w.state.local = append(w.state.local, fact) }()
			err := w.state.owner.ValidateArtifact(ctx, "wal", entry.Name(), w.state.resources.MaxMetadataBytes)
			if err != nil {
				w.failed("unsafe_artifact", "",
					"WAL inventory contains a link, mount, special file or unreadable artifact: "+entry.Name(), err)
				return ctx.Err()
			}
			if strings.HasSuffix(entry.Name(), ".collection") {
				return w.collection(ctx, entry.Name(), &fact)
			}
			if !entry.IsDir() {
				w.issue("unrecognized_local_replica", "", "unrecognized regular file in WAL root: "+entry.Name())
				return nil
			}
			return w.replica(ctx, entry.Name(), &fact)
		})
}

func (w *walInventory) replica(ctx context.Context, name string, fact *localArtifact) error {
	group := name
	quarantined := false
	if parsed, ok := walbackend.QuarantineGroup(name); ok {
		group, quarantined = parsed, true
	}
	if topology.ValidateLoadID(group) != nil {
		w.issue("unrecognized_local_replica", "", "unrecognized WAL directory: "+name)
		return nil
	}
	id, err := walbackend.ReadReplicaPin(ctx, filepath.Join(w.state.paths.LoadWALDir, name), group)
	if err != nil {
		w.failed("invalid_replica_owner", group, "replica pin is missing, foreign or unreadable: "+name, err)
		return ctx.Err()
	}
	fact.Identity = id
	if id.NodeName != w.state.identity.NodeName || id.ClusterID != w.state.identity.ClusterID {
		w.issue("foreign_replica", group, "WAL owner differs from the metadata survivor: "+name)
		return nil
	}
	qualified, err := classifyIdentity(*w.state, id)
	if err != nil {
		w.failed("unrecognized_local_replica", group,
			"no unambiguous approved or proven identity for WAL: "+name, err)
		if !quarantined {
			return w.unrecognizedPositions(ctx, id, fact)
		}
		return nil
	}
	classification, version := qualified.Classification, qualified.SourceVersion
	if quarantined && classification != classRetired && classification != classDeleted {
		w.issue("contradictory_replica", group, "quarantined data lacks exact retirement evidence: "+name)
		return nil
	}
	fact.Classification = classification
	if classification != classDeleted && classification != classRetired &&
		w.accepted.Request.Mode == model.Rebuild &&
		!quarantined && w.accepted.Approved.Group(group) != nil {
		return w.donor(ctx, version, fact)
	}
	digest, err := w.state.owner.ArtifactDigest(ctx, "wal", name, w.state.resources.MaxMetadataBytes)
	if err != nil {
		w.failed("unreadable_artifact", group, "cannot fingerprint owned retained WAL: "+name, err)
		return ctx.Err()
	}
	fact.Digest = digest
	w.accepted.Archive = append(w.accepted.Archive, model.ArchiveEntry{Root: "wal", Name: name,
		Kind: "directory", Classification: classification, EvidenceDigest: digest})
	return nil
}

func (w *walInventory) unrecognizedPositions(ctx context.Context, id storage.Identity, fact *localArtifact) error {
	evidence, err := walbackend.ReadRecovery(ctx, w.state.paths.LoadWALDir, id.GroupID, model.ReadBounds{
		MaxRecordBytes: w.state.resources.MaxRecordBytes, MaxGroupBytes: w.state.resources.MaxGroupBytes})
	if err != nil {
		w.cause = errors.Join(w.cause, err)
		return ctx.Err()
	}
	fact.Digest = evidence.Digest
	source := seedSource(evidence.Recovery, model.ReplicaEvidence{Identity: id,
		MemberID: w.state.identity.MemberID}, evidence.Digest)
	report := groupReport(source)
	report.LastLog = evidence.LastLog
	report.Usable = false
	w.accepted.Groups = append(w.accepted.Groups, report)
	return nil
}

func classifyIdentity(state SourceState, id storage.Identity) (model.ReplicaEvidence, error) {
	selected, err := selectReplicaEvidence(state.inventory.Replicas, id)
	if err != nil {
		return model.ReplicaEvidence{}, err
	}
	selected.Identity = id
	if selected.Classification == classRetired || selected.Classification == classDeleted {
		return selected, nil
	}
	if retiredByAuthority(state.authority, id) {
		selected.Classification = classRetired
		return selected, nil
	}
	if selected.Classification != "" && slices.Contains(state.inventory.Retired, id.GroupID) {
		selected.Classification = classDeleted
		return selected, nil
	}
	if selected.Classification == "" || selected.MemberID != state.identity.MemberID ||
		selected.SourceVersion == 0 {
		return model.ReplicaEvidence{}, fault.ErrIdentityMismatch
	}
	ledger, found := state.ledger[id.GroupID]
	if !found || ledger.NodeName != id.NodeName || ledger.ClusterID != id.ClusterID ||
		ledger.ReplicaID != id.ReplicaID {
		return model.ReplicaEvidence{}, fault.ErrIdentityMismatch
	}
	return selected, nil
}

func selectReplicaEvidence(candidates []model.ReplicaEvidence, id storage.Identity) (model.ReplicaEvidence, error) {
	var selected model.ReplicaEvidence
	for _, candidate := range candidates {
		if candidate.Identity != id {
			continue
		}
		if selected.MemberID != 0 && selected.MemberID != candidate.MemberID {
			return model.ReplicaEvidence{}, fault.ErrIdentityMismatch
		}
		version := max(selected.SourceVersion, candidate.SourceVersion)
		if classRank(candidate.Classification) > classRank(selected.Classification) {
			selected = candidate
		}
		selected.SourceVersion = version
	}
	return selected, nil
}

const (
	rankUnknown = iota
	rankPredecessor
	rankProven
	rankApproved
	rankRetired
	rankDeleted
)

func classRank(class string) int {
	switch class {
	case classDeleted:
		return rankDeleted
	case classRetired:
		return rankRetired
	case "approved":
		return rankApproved
	case "proven":
		return rankProven
	case "pending_predecessor":
		return rankPredecessor
	default:
		return rankUnknown
	}
}

func retiredByAuthority(authority model.AuthorityEvidence, id storage.Identity) bool {
	for _, effect := range authority.Effects {
		action := effect.Grant.Action
		if action == nil || effect.State != "complete" || effect.Receipt == nil {
			continue
		}
		if action.Kind != topology.StepCleanupReplicaState && action.Kind != topology.StepDeleteGroupState {
			continue
		}
		if effect.Grant.Key.ClusterID == id.ClusterID && effect.Grant.Key.GroupID == id.GroupID &&
			effect.Target.NodeName == id.NodeName && effect.Target.ReplicaID == id.ReplicaID {
			return true
		}
	}
	return false
}

func (w *walInventory) collection(ctx context.Context, name string, fact *localArtifact) error {
	record, err := walbackend.ReadCollectionEvidence(ctx, w.state.paths.LoadWALDir, name)
	if err != nil {
		w.failed("invalid_collection", "", "invalid collection marker: "+name, err)
		return ctx.Err()
	}
	found := false
	for _, proof := range w.state.authority.Collections {
		if proof.Retired == record && proof.MemberID > 0 && proof.MemberID <= w.state.identity.MemberID &&
			record.Identity.NodeName == w.state.identity.NodeName &&
			record.Identity.ClusterID == w.state.identity.ClusterID {
			found = true
			break
		}
	}
	if !found {
		w.issue("unproven_collection", record.Identity.GroupID,
			"collection marker lacks exact retained journal proof: "+name)
		return nil
	}
	digest, err := w.state.owner.ArtifactDigest(ctx, "wal", name, w.state.resources.MaxMetadataBytes)
	if err != nil {
		return err
	}
	fact.Identity, fact.Classification, fact.Digest = record.Identity, "collection", digest
	w.accepted.Archive = append(w.accepted.Archive, model.ArchiveEntry{Root: "wal", Name: name,
		Kind: "regular_file", Classification: "collection", EvidenceDigest: digest})
	return nil
}

func (w *walInventory) issue(code, group, message string) {
	groupID := group
	if topology.ValidateID(groupID) != nil {
		groupID = ""
	}
	w.accepted.Issues = append(w.accepted.Issues, model.Issue{Code: code, GroupID: groupID, Message: message,
		Blocking: true, Remedy: model.RemedyInvestigate})
}

func visitInventoryPage(ctx context.Context, entries []os.DirEntry, remaining *uint64,
	visit func(os.DirEntry) error) error {
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		size := uint64(len(entry.Name()) + 1)
		if size > *remaining {
			return fault.New(fault.Unreadable, "root inventory exceeds recovery name budget", nil)
		}
		*remaining -= size
		if err := visit(entry); err != nil {
			return err
		}
	}
	return nil
}

func (w *walInventory) failed(code, group, message string, cause error) {
	issue := model.InputIssue(model.Issue{Code: code, GroupID: group, Message: message}, cause)
	w.issue(issue.Code, group, issue.Message)
	w.cause = errors.Join(w.cause, cause)
}
