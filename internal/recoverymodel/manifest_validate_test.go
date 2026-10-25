//go:build !functional

package recoverymodel_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/internal/recoverytest"
)

func manifest() model.LocalManifest {
	return recoverytest.Manifest(model.Paths{StorageDir: "/system", LoadWALDir: "/wal"}, "incident", time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC), model.Approved)
}
func seal(r *model.LocalManifest) {
	digest, _ := model.DigestAccepted(model.AcceptedFromManifest(*r))
	r.PlanDigest = digest
	r.Acceptance.PlanDigest = digest
}

func TestManifestSemanticRefusals(t *testing.T) {
	t.Parallel()
	base := manifest()
	if err := model.ValidateManifest(base); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*model.LocalManifest){
		"latched bound smaller than source": func(r *model.LocalManifest) { r.Resources.MaxMetadataBytes = 1 },
		"schema":                            func(r *model.LocalManifest) { r.Schema = 2 },
		"zero revision":                     func(r *model.LocalManifest) { r.Revision = 0 },
		"missing ID":                        func(r *model.LocalManifest) { r.Request.RecoveryID = "" },
		"name":                              func(r *model.LocalManifest) { r.BackupName = "renamed" },
		"resource defaults unlatched":       func(r *model.LocalManifest) { r.Resources.MaxGroupBytes = 0 },
		"no acceptance":                     func(r *model.LocalManifest) { r.Acceptance.AcknowledgeDataLoss = false },
		"no fencing":                        func(r *model.LocalManifest) { r.Acceptance.OldDeploymentFenced = false },
		"phase":                             func(r *model.LocalManifest) { r.Phase = "invented" },
		"new process in identity":           func(r *model.LocalManifest) { r.Identity.InstanceID = "process" },
		"same lineage":                      func(r *model.LocalManifest) { r.Identity.ClusterID = r.Source.ClusterID },
		"relative roots":                    func(r *model.LocalManifest) { r.Paths.StorageDir = "relative" },
		"reverse roots":                     func(r *model.LocalManifest) { r.Paths.LoadWALDir = "/" },
		"reserved roots":                    func(r *model.LocalManifest) { r.Paths.StorageDir = "/system/@gquorum-recovery/x" },
		"missing group":                     func(r *model.LocalManifest) { r.Sources = nil },
		"duplicate source":                  func(r *model.LocalManifest) { r.Sources = append(r.Sources, r.Sources[0]) },
		"commit beyond tail":                func(r *model.LocalManifest) { r.Sources[0].Commit.Index = 4 },
		"descending term":                   func(r *model.LocalManifest) { r.Sources[0].Commit.Term = 2 },
		"missing snapshot":                  func(r *model.LocalManifest) { r.Sources[0].Snapshot.Index = 0 },
		"foreign donor":                     func(r *model.LocalManifest) { r.Sources[0].Identity.NodeName = "other" },
		"future source":                     func(r *model.LocalManifest) { r.Sources[0].SourceVersion = 2 },
		"missing system archive":            func(r *model.LocalManifest) { r.Archive = r.Archive[:1] },
		"duplicate archive":                 func(r *model.LocalManifest) { r.Archive = append(r.Archive, r.Archive[0]) },
		"unsafe archive":                    func(r *model.LocalManifest) { r.Archive[0].Name = "../foreign" },
		"foreign system file":               func(r *model.LocalManifest) { r.Archive[0].Name = "application.db" },
		"moved before boundary":             func(r *model.LocalManifest) { r.Archive[0].Moved = true },
		"old allocator":                     func(r *model.LocalManifest) { r.Floors[0].NextID = 1 },
		"false completion":                  func(r *model.LocalManifest) { r.Phase = model.Complete },
		"premature successor":               func(r *model.LocalManifest) { r.SuccessorStartedAt = &r.AcceptedAt },
		"missing cancellation audit":        func(r *model.LocalManifest) { r.Phase = model.Cancelling },
		"unknown hook state":                func(r *model.LocalManifest) { r.Files.Backup.State = "maybe" },
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r := model.CloneManifest(base)
			edit(&r)
			seal(&r)
			if err := model.ValidateManifest(r); err == nil {
				t.Fatal("contradictory journal accepted")
			}
		})
	}
}
func TestManifestCASAndWriteOnceProgress(t *testing.T) {
	t.Parallel()
	old := manifest()
	next := model.CloneManifest(old)
	next.Revision++
	next.Phase = model.Files
	if err := model.ValidateManifestUpdate(old, next); err != nil {
		t.Fatal(err)
	}
	changed := model.CloneManifest(next)
	changed.Resources.MaxGroupBytes++
	seal(&changed)
	if err := model.ValidateManifestUpdate(old, changed); !errors.Is(err, fault.ErrConflict) {
		t.Fatal("changed acceptance allowed", err)
	}
	changed = model.CloneManifest(old)
	changed.Revision++
	changed.Phase = model.Archiving
	changed.Files.Backup.State = "complete"
	if err := model.ValidateManifestUpdate(old, changed); err == nil {
		t.Fatal("archive boundary skipped file phase")
	}
	old.Revision = math.MaxUint64
	next.Revision = 0
	if err := model.ValidateManifestUpdate(old, next); !errors.Is(err, fault.ErrConflict) {
		t.Fatal("revision overflow accepted", err)
	}
	r := manifest()
	r.Phase = model.RollingBack
	r.Rollback = &model.ActionAudit{Actor: "operator", Reason: "reserved", At: r.AcceptedAt}
	r.Archive[0].Restored = true
	data, err := model.EncodeManifest(r)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := model.DecodeManifest(data)
	if err != nil || restored.Rollback == nil || !restored.Archive[0].Restored {
		t.Fatal("reserved schema lost", err)
	}
	restored.Revision++
	if err := model.ValidateManifestUpdate(r, restored); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal("reserved rollback became executable", err)
	}
	if status := model.StatusFromManifest(r); status.Complete || status.Issue == nil || status.Issue.Code != "unsupported_rollback" {
		t.Fatal(status)
	}
}
func TestExactSchemaKeysAndCanonicalDigest(t *testing.T) {
	t.Parallel()
	r := manifest()
	data, err := model.EncodeManifest(r)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{
		bytes.Replace(data, []byte(`"schema":1`), []byte(`"schema":1,"Schema":1`), 1),
		bytes.Replace(data, []byte(`"revision":1`), []byte(`"revision":1,"revision":2`), 1),
		append(append([]byte(nil), data...), []byte(` {}`)...),
	} {
		if _, err := model.DecodeManifest(bad); err == nil {
			t.Fatal("ambiguous record accepted")
		}
	}
	p := model.AcceptedFromManifest(r)
	before, err := model.DigestAccepted(p)
	if err != nil {
		t.Fatal(err)
	}
	p.Resources.MaxMetadataBytes++
	p.Resources.MaxGroupBytes++
	p.Archive[0], p.Archive[1] = p.Archive[1], p.Archive[0]
	after, err := model.DigestAccepted(p)
	if err != nil || before != after {
		t.Fatal("resources/order changed recovered data digest", err)
	}
	p.Archive[0].EvidenceDigest = r.Fingerprint
	changed, _ := model.DigestAccepted(p)
	if changed == before {
		t.Fatal("inventory content not bound")
	}
	for _, bad := range []string{`{"schema":2}`, `{"schema":1,"recovery_id":"../foreign"}`, `{"schema":1,"RecoveryID":"x"}`} {
		if _, err := model.DecodeCurrent([]byte(bad)); err == nil {
			t.Fatal("invalid current header accepted", bad)
		}
	}
	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
}

func TestManifestRetainsVerifiedCollectionMarkerInventory(t *testing.T) {
	t.Parallel()
	r := manifest()
	r.Archive = append(r.Archive, model.ArchiveEntry{Root: "wal", Name: "group.retired.collection", Kind: "regular_file", Classification: "collection", EvidenceDigest: r.Fingerprint})
	seal(&r)
	if err := model.ValidateManifest(r); err != nil {
		t.Fatal("collection marker file was not representable", err)
	}
	r.Archive[len(r.Archive)-1].Name = "unknown-file"
	seal(&r)
	if err := model.ValidateManifest(r); err == nil {
		t.Fatal("unknown WAL file adopted as a collection marker")
	}
}
