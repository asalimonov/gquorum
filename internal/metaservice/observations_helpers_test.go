package metaservice

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/asalimonov/gquorum/events"
)

func eventFixture(t *testing.T) (*events.Bus, *events.Journal) {
	t.Helper()
	journal, err := events.OpenJournal(events.JournalOptions{Path: filepath.Join(t.TempDir(), "events.bolt")})
	if err != nil {
		t.Fatal(err)
	}
	bus, err := events.NewBus(events.BusOptions{Journal: journal, Buffer: 256})
	if err != nil {
		journal.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := bus.Close(context.Background()); err != nil {
			t.Error(err)
		}
		if err := journal.Close(); err != nil {
			t.Error(err)
		}
	})
	return bus, journal
}

func journalEvents(t *testing.T, j *events.Journal, f events.Filter) []events.Event {
	t.Helper()
	e, err := j.List(context.Background(), events.Query{Limit: 256, Filter: f})
	if err != nil {
		t.Fatal(err)
	}
	return e
}
