//go:build functional

package disk

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/internal/recovery/model"
)

func TestJournalHistorySharesRevisionAndSurvivesLostAcknowledgement(t *testing.T) {
	t.Parallel()
	owner := journalOwner(t)
	journal, _, _ := acceptedJournal(t, owner, "history", true)
	before := journal.View()
	if len(before.History) != 1 {
		t.Fatal("approval not recorded", before.History)
	}
	injected := errors.New("lost journal acknowledgement")
	owner.boundary = func(point string) error {
		if point == "manifest.parent_sync" {
			return injected
		}
		return nil
	}
	err := journal.SetPhase(t.Context(), model.Files, nil)
	if !errors.Is(err, injected) {
		t.Fatal(err)
	}
	owner.boundary = nil
	reopened, err := Open(t.Context(), owner, "history")
	mustJournal(t, err)
	after := reopened.View()
	if after.Revision != before.Revision+1 || len(after.History) != 2 || after.Phase != model.Files {
		t.Fatal("history needed a separate revision or was lost", after)
	}
	mustJournal(t, reopened.SetPhase(t.Context(), model.Files, nil))
	if !reflect.DeepEqual(after.History, reopened.View().History) {
		t.Fatal("resume duplicated committed event")
	}
	mustJournal(t, reopened.MarkFiles(t.Context(), model.FilesBackup, "running", 1))
	mustJournal(t, reopened.MarkFiles(t.Context(), model.FilesBackup, "complete", 1))
	list, err := List(context.Background(), owner.Paths())
	mustJournal(t, err)
	if len(list) != 1 || len(model.StatusFromManifest(list[0]).History) != 4 {
		t.Fatal("list lost bounded history", list)
	}
	copied := reopened.View()
	copied.History[0].Message = "modified"
	if reopened.View().History[0].Message == "modified" {
		t.Fatal("journal view aliased history")
	}
}
