// Package testutil builds pure, complete recovery records for owning tests.
package testutil

import (
	"crypto/sha256"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/internal/recovery/model"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

const fixtureName = "n1"
const fixtureTail = 3
const fixtureSourceBytes = 100
const fixtureMaxVoters = 3

// Accepted constructs metadata-only evidence; it performs no I/O or callbacks.
func Accepted(paths model.Paths, id string) model.AcceptedPlan {
	source := topology.Identity{NodeName: fixtureName, ClusterID: "cluster", MemberID: 1}
	resources, _ := model.ResolveResources(model.Resources{})
	return model.AcceptedPlan{Request: model.Request{RecoveryID: id,
		Mode: model.Rebuild, Actor: "operator", Comment: "lost majority"},
		Paths: paths, Resources: resources, Source: source, Identity: topology.Identity{NodeName: fixtureName,
			ClusterID: "recovered", MemberID: 2}, Fingerprint: strings.Repeat("a", sha256.Size*2),
		Rules: topology.Rules{SchemaVersion: 1, Limits: (topology.Limits{}).WithDefaults()},
		Approved: topology.Document{Version: 1, ClusterID: "cluster", Origin: topology.OriginFounding,
			Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: fixtureMaxVoters},
			Nodes:     []topology.Node{{Name: fixtureName, Type: "worker", State: topology.NodeActive}},
			Groups: []topology.Group{{ID: topology.MetaGroupID, NextReplicaID: 2,
				Replicas: []topology.Replica{{NodeName: fixtureName, ReplicaID: 1, Role: topology.ReplicaVoter}}}}},
		Floors: []model.AllocationFloor{{GroupID: topology.MetaGroupID, NextID: 2}},
		Sources: []model.SeedSource{{GroupID: topology.MetaGroupID,
			Identity: storage.Identity{NodeName: fixtureName, ClusterID: "cluster", GroupID: topology.MetaGroupID,
				ReplicaID: 1}, MemberID: 1, SourceVersion: 1, Snapshot: model.Position{Index: 1,
				Term: 1}, Commit: model.Position{Index: 2, Term: 1},
			LastLog: model.Position{Index: fixtureTail, Term: 1}, HardStateTerm: 1,
			Digest: strings.Repeat("b", sha256.Size*2), Bytes: fixtureSourceBytes}},
		Archive: []model.ArchiveEntry{{Root: "system", Name: "_authority.bolt",
			Kind: "regular_file", Classification: "source", EvidenceDigest: strings.Repeat("c", sha256.Size*2)},
			{Root: "system", Name: "_meta.bolt", Kind: "regular_file", Classification: "source",
				EvidenceDigest: strings.Repeat("d", sha256.Size*2)}}}
}

// Manifest constructs a semantically complete record at the named phase.
func Manifest(paths model.Paths, id string, at time.Time,
	phase model.Phase) model.LocalManifest {
	p := Accepted(paths, id)
	digest, _ := model.DigestAccepted(p)
	name, _ := model.BackupName(at, id)
	r := model.LocalManifest{Schema: 1, Revision: 1, Request: p.Request,
		Acceptance: model.Acceptance{PlanDigest: digest, AcknowledgeDataLoss: true,
			OldDeploymentFenced: true}, Paths: p.Paths, Resources: p.Resources, Source: p.Source,
		Identity: p.Identity, Fingerprint: p.Fingerprint, PlanDigest: digest, Rules: p.Rules,
		Approved: p.Approved, Floors: p.Floors, Sources: p.Sources, Archive: p.Archive,
		BackupName: name, AcceptedAt: at, Phase: phase,
		Files: model.FilesProgress{Backup: model.HookProgress{State: "pending"},
			Undo: model.HookProgress{State: "pending"}}}
	if phase != model.Approved && phase != model.Files {
		r.Files.Backup.State = "complete"
	}
	if phase == model.AwaitingStart || phase == model.Importing ||
		phase == model.Validating || phase == model.Complete {
		for i := range r.Archive {
			r.Archive[i].Moved = true
		}
	}
	if phase == model.Importing || phase == model.Validating || phase == model.Complete {
		r.SuccessorStartedAt = &at
	}
	if phase == model.Complete {
		r.CompletedAt = &at
	}
	return r
}
