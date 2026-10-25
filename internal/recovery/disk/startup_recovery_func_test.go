//go:build functional

package disk

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	recoverytest "github.com/asalimonov/gquorum/internal/recovery/testutil"
	"github.com/asalimonov/gquorum/topology"
)

func TestRecoverySuccessorMarkerPublicationFailuresNeverAcquireDestination(t *testing.T) {
	t.Parallel()
	for _, point := range []string{"manifest.temp_sync", "manifest.rename", "manifest.parent_sync"} {
		t.Run(point, func(t *testing.T) {
			t.Parallel()
			owner := journalOwner(t)
			journal, _, _ := acceptedJournal(t, owner, "marker", false)
			mustJournal(t, journal.SetPhase(t.Context(), model.Files, nil))
			mustJournal(t, journal.MarkFiles(t.Context(), model.FilesBackup, "complete", 0))
			mustJournal(t, journal.SetPhase(t.Context(), model.Archiving, nil))
			for _, entry := range journal.View().Archive {
				mustJournal(t, journal.MarkArchive(t.Context(), entry.Root, entry.Name))
				mustJournal(t, os.Remove(filepath.Join(owner.Paths().StorageDir, entry.Name)))
			}
			mustJournal(t, journal.SetPhase(t.Context(), model.AwaitingStart, nil))
			failure := errors.New("marker sync failed")
			owner.boundary = func(boundary string) error {
				if boundary == point {
					return failure
				}
				return nil
			}
			if err := journal.MarkSuccessorStarted(t.Context(), time.Now().UTC()); !errors.Is(err, failure) {
				t.Fatal(err)
			}
			for _, name := range []string{"_meta.bolt", "_authority.bolt"} {
				if _, err := os.Stat(filepath.Join(owner.Paths().StorageDir, name)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("failed marker acquired B", name, err)
				}
			}
			owner.boundary = nil
			reopened, err := Open(t.Context(), owner, "marker")
			if err != nil {
				t.Fatal(err)
			}
			mustJournal(t, reopened.MarkSuccessorStarted(t.Context(), time.Now().UTC()))
			if reopened.View().SuccessorStartedAt == nil {
				t.Fatal("durable marker retry was lost")
			}
		})
	}
}

func TestRecoveryMarkJoinedBindsExactGrantedIdentityAndKeepsHistoricalRoots(t *testing.T) {
	t.Parallel()
	owner := journalOwner(t)
	accepted := recoverytest.Accepted(owner.Paths(), "joined")
	accepted.Request.Mode, accepted.Request.ClusterID = model.ResetForJoin, accepted.Identity.ClusterID
	accepted.Identity.MemberID, accepted.Sources = 0, nil
	digest, err := model.DigestAccepted(accepted)
	mustJournal(t, err)
	journal, err := Create(t.Context(), owner, accepted, model.Acceptance{PlanDigest: digest, AcknowledgeDataLoss: true})
	mustJournal(t, err)
	mustJournal(t, journal.SetPhase(t.Context(), model.Files, nil))
	mustJournal(t, journal.MarkFiles(t.Context(), model.FilesBackup, "complete", 0))
	mustJournal(t, journal.SetPhase(t.Context(), model.Archiving, nil))
	for _, entry := range journal.View().Archive {
		mustJournal(t, journal.MarkArchive(t.Context(), entry.Root, entry.Name))
	}
	mustJournal(t, journal.SetPhase(t.Context(), model.JoinReady, nil))
	identity := topology.Identity{NodeName: "n1", ClusterID: accepted.Identity.ClusterID, MemberID: 99, InstanceID: "fresh"}
	if err := journal.MarkJoined(t.Context(), identity); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("join preceded successor marker", err)
	}
	mustJournal(t, journal.MarkSuccessorStarted(t.Context(), owner.clock()))
	paths := owner.Paths()
	lease, err := owner.TakeWALLease()
	mustJournal(t, err)
	t.Cleanup(func() { mustJournal(t, lease.Close()) })
	mustJournal(t, journal.MarkJoined(t.Context(), identity))
	if owner.Paths() != paths || journal.View().JoinedIdentity.MemberID != 99 ||
		journal.View().JoinedIdentity.InstanceID != "" {
		t.Fatal("lease handoff lost root or acquired identity")
	}
	identity.InstanceID = "another process"
	mustJournal(t, journal.MarkJoined(t.Context(), identity))
	identity.MemberID++
	if err := journal.MarkJoined(t.Context(), identity); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
}
