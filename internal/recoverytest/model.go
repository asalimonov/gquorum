// Package recoverytest builds pure, complete recovery records for owning tests.
package recoverytest

import (
	"crypto/sha256"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

const fixtureName = "n1"
const fixtureTail = 3
const fixtureSourceBytes = 100
const fixtureMaxVoters = 3

// Accepted constructs metadata-only evidence; it performs no I/O or callbacks.
func Accepted(paths recoverymodel.Paths, id string) recoverymodel.AcceptedPlan {
	source := topology.Identity{NodeName: fixtureName, ClusterID: "cluster", MemberID: 1}
	resources, _ := recoverymodel.ResolveResources(recoverymodel.Resources{})
	return recoverymodel.AcceptedPlan{Request: recoverymodel.Request{RecoveryID: id,
		Mode: recoverymodel.Rebuild, Actor: "operator", Comment: "lost majority"},
		Paths: paths, Resources: resources, Source: source, Identity: topology.Identity{NodeName: fixtureName,
			ClusterID: "recovered", MemberID: 2}, Fingerprint: strings.Repeat("a", sha256.Size*2),
		Rules: topology.Rules{SchemaVersion: 1, Limits: (topology.Limits{}).WithDefaults()},
		Approved: topology.Document{Version: 1, ClusterID: "cluster", Origin: topology.OriginFounding,
			Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: fixtureMaxVoters},
			Nodes:     []topology.Node{{Name: fixtureName, Type: "worker", State: topology.NodeActive}},
			Groups: []topology.Group{{ID: topology.MetaGroupID, NextReplicaID: 2,
				Replicas: []topology.Replica{{NodeName: fixtureName, ReplicaID: 1, Role: topology.ReplicaVoter}}}}},
		Floors: []recoverymodel.AllocationFloor{{GroupID: topology.MetaGroupID, NextID: 2}},
		Sources: []recoverymodel.SeedSource{{GroupID: topology.MetaGroupID,
			Identity: storage.Identity{NodeName: fixtureName, ClusterID: "cluster", GroupID: topology.MetaGroupID,
				ReplicaID: 1}, MemberID: 1, SourceVersion: 1, Snapshot: recoverymodel.Position{Index: 1,
				Term: 1}, Commit: recoverymodel.Position{Index: 2, Term: 1},
			LastLog: recoverymodel.Position{Index: fixtureTail, Term: 1}, HardStateTerm: 1,
			Digest: strings.Repeat("b", sha256.Size*2), Bytes: fixtureSourceBytes}},
		Archive: []recoverymodel.ArchiveEntry{{Root: "system", Name: "_authority.bolt",
			Kind: "regular_file", Classification: "source", EvidenceDigest: strings.Repeat("c", sha256.Size*2)},
			{Root: "system", Name: "_meta.bolt", Kind: "regular_file", Classification: "source",
				EvidenceDigest: strings.Repeat("d", sha256.Size*2)}}}
}

// Manifest constructs a semantically complete record at the named phase.
func Manifest(paths recoverymodel.Paths, id string, at time.Time,
	phase recoverymodel.Phase) recoverymodel.LocalManifest {
	p := Accepted(paths, id)
	digest, _ := recoverymodel.DigestAccepted(p)
	name, _ := recoverymodel.BackupName(at, id)
	r := recoverymodel.LocalManifest{Schema: 1, Revision: 1, Request: p.Request,
		Acceptance: recoverymodel.Acceptance{PlanDigest: digest, AcknowledgeDataLoss: true,
			OldDeploymentFenced: true}, Paths: p.Paths, Resources: p.Resources, Source: p.Source,
		Identity: p.Identity, Fingerprint: p.Fingerprint, PlanDigest: digest, Rules: p.Rules,
		Approved: p.Approved, Floors: p.Floors, Sources: p.Sources, Archive: p.Archive,
		BackupName: name, AcceptedAt: at, Phase: phase,
		Files: recoverymodel.FilesProgress{Backup: recoverymodel.HookProgress{State: "pending"},
			Undo: recoverymodel.HookProgress{State: "pending"}}}
	if phase != recoverymodel.Approved && phase != recoverymodel.Files {
		r.Files.Backup.State = "complete"
	}
	if phase == recoverymodel.AwaitingStart || phase == recoverymodel.Importing ||
		phase == recoverymodel.Validating || phase == recoverymodel.Complete {
		for i := range r.Archive {
			r.Archive[i].Moved = true
		}
	}
	if phase == recoverymodel.Importing || phase == recoverymodel.Validating || phase == recoverymodel.Complete {
		r.SuccessorStartedAt = &at
	}
	if phase == recoverymodel.Complete {
		r.CompletedAt = &at
	}
	return r
}
