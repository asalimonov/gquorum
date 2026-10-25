package recoverymodel

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/asalimonov/gquorum/fault"
)

// HistoryMaxEvents and HistoryMessageBytes bound operator progress diagnostics.
const (
	HistoryMaxEvents    = 256
	HistoryMessageBytes = 512
	historyEventBytes   = 4096
	diagnosticBytes     = 4096
)

// CleanDiagnostic produces bounded single-line UTF-8 without control characters.
func CleanDiagnostic(value string, limit int) string {
	if limit <= 0 {
		return ""
	}
	clean := strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, strings.ToValidUTF8(value, "�"))
	clean = strings.TrimSpace(clean)
	if len(clean) > limit {
		clean = clean[:limit]
		for !utf8.ValidString(clean) {
			clean = clean[:len(clean)-1]
		}
	}
	return strings.TrimSpace(clean)
}

func historyIssue(value *Issue) *Issue {
	if value == nil {
		return nil
	}
	issue := *value
	issue.Message = CleanDiagnostic(issue.Message, HistoryMessageBytes)
	return &issue
}

// StartHistory records the approval already established by the manifest.
func StartHistory(value *LocalManifest) {
	if len(value.History) != 0 {
		return
	}
	value.History = []StatusEvent{{At: value.AcceptedAt, Phase: Approved, Message: "Recovery approved"}}
}

// AdvanceHistory derives only new semantic transitions in the same manifest CAS.
func AdvanceHistory(old LocalManifest, next *LocalManifest, at time.Time) {
	next.History = CloneHistory(old.History)
	StartHistory(next)
	emit := historyAppender(next, at)
	if len(next.Seeds) == 0 && old.Phase == Importing && next.Phase == Validating {
		emit("Metadata import committed", "", nil)
	}
	if old.Phase != next.Phase {
		emit(phaseMessage(next.Phase), "", nil)
	}
	if old.SuccessorStartedAt == nil && next.SuccessorStartedAt != nil {
		emit("Successor startup begun", "", nil)
	}
	historyFiles(old.Files, next.Files, emit)
	historyArchive(old.Archive, next.Archive, emit)
	historySeeds(old, next, emit)
	if !reflect.DeepEqual(old.Failure, next.Failure) {
		if next.Failure != nil {
			emit("Recovery operation failed: "+next.Failure.Message, next.Failure.GroupID, next.Failure)
		} else {
			emit("Recovery resumed", "", nil)
		}
	}
}

func historyAppender(next *LocalManifest, at time.Time) historyEmitter {
	return func(message, group string, issue *Issue) {
		observed := at
		last := next.History[len(next.History)-1].At
		if observed.Before(last) {
			observed = last
		}
		event := StatusEvent{At: observed.UTC(), Phase: next.Phase,
			Message: CleanDiagnostic(message, HistoryMessageBytes), GroupID: group, Issue: historyIssue(issue)}
		next.History = append(next.History, event)
		if len(next.History) > HistoryMaxEvents {
			retained := next.History[len(next.History)-(HistoryMaxEvents-1):]
			next.History = append([]StatusEvent{next.History[0]}, retained...)
		}
	}
}
func historyArchive(old, next []ArchiveEntry, emit historyEmitter) {
	for i, entry := range next {
		if i < len(old) && !old[i].Moved && entry.Moved {
			group := ""
			if entry.Root == archiveWAL && entry.Kind == "directory" {
				group = entry.Name
			}
			emit("Archived library data "+entry.Name, group, nil)
		}
	}
}

type historyEmitter func(string, string, *Issue)

func historyFiles(old, next FilesProgress, emit historyEmitter) {
	for _, entry := range []struct {
		name      string
		old, next HookProgress
	}{
		{"Application files backup", old.Backup, next.Backup}, {"Application files undo", old.Undo, next.Undo}} {
		if entry.old == entry.next {
			continue
		}
		if entry.next.State == stateRunning {
			emit(entry.name+" started", "", nil)
		}
		if entry.next.State == stateComplete {
			emit(entry.name+" finished", "", nil)
		}
	}
}
func historySeeds(old LocalManifest, next *LocalManifest, emit historyEmitter) {
	first := true
	for _, seed := range old.Seeds {
		if seed.State != statePending {
			first = false
			break
		}
	}
	for i, seed := range next.Seeds {
		if i >= len(old.Seeds) || old.Seeds[i] == seed {
			continue
		}
		if first && old.Phase == Importing {
			emit("Metadata import committed", "", nil)
			first = false
		}
		message := "Recovered replica " + seed.State
		if seed.Checkpoint.Index != 0 {
			message += fmt.Sprintf(" through index %d, term %d", seed.Checkpoint.Index, seed.Checkpoint.Term)
		}
		emit(message, seed.GroupID, nil)
	}
}
func phaseMessage(phase Phase) string {
	messages := map[Phase]string{
		Approved:      "Recovery approved",
		Files:         "Application file preparation begun",
		Cancelling:    "Cancellation requested",
		Cancelled:     "Recovery cancelled",
		Archiving:     "Library backup begun",
		AwaitingStart: "Library backup complete; recovered node may start",
		JoinReady:     "Library backup complete; node may join destination",
		Joined:        "Joined recovered cluster",
		Importing:     "Metadata import and reconstruction begun",
		Validating:    "Replicated import complete; validating application data",
		Complete:      "Recovery complete"}

	if message := messages[phase]; message != "" {
		return message
	}
	return "Recovery phase: " + string(phase)
}

// ValidateHistory accepts legacy absence and bounds newly recorded observations.
func ValidateHistory(value LocalManifest) error {
	if len(value.History) > HistoryMaxEvents {
		return fault.ErrInvalid
	}
	var previous time.Time
	for i, event := range value.History {
		if err := validateHistoryEvent(event, value.AcceptedAt, previous); err != nil {
			return err
		}
		if i == 0 && (event.Phase != Approved || !event.At.Equal(value.AcceptedAt)) {
			return fault.ErrInvalid
		}

		previous = event.At
	}
	return nil
}
func validateHistoryEvent(event StatusEvent, accepted, previous time.Time) error {
	if event.Message == "" || len(event.Message) > HistoryMessageBytes ||
		CleanDiagnostic(event.Message, HistoryMessageBytes) != event.Message {
		return fault.ErrInvalid
	}
	if event.At.IsZero() || event.At.Before(accepted) || event.At.Before(previous) || !knownHistoryPhase(event.Phase) {
		return fault.ErrInvalid
	}
	if event.Issue != nil {
		if len(event.Issue.Message) > HistoryMessageBytes ||
			CleanDiagnostic(event.Issue.Message, HistoryMessageBytes) != event.Issue.Message {
			return fault.ErrInvalid
		}
		if err := validateIssue(*event.Issue); err != nil {
			return err
		}
	}
	encoded, err := json.Marshal(event)
	if err != nil || len(encoded) > historyEventBytes {
		return fault.ErrInvalid
	}
	return nil
}

func knownHistoryPhase(phase Phase) bool {
	switch phase {
	case Approved, Files, Cancelling, Cancelled, Archiving, AwaitingStart, Importing, Validating, Complete,
		JoinReady, Joined, RollingBack, RolledBack:
		return true
	default:
		return false
	}
}

// LimitFailure preserves a bounded diagnostic in mutable journal progress.
func LimitFailure(issue Issue) Issue {
	issue.Message = CleanDiagnostic(issue.Message, diagnosticBytes)
	if issue.Message == "" {
		issue.Message = "Recovery operation failed"
	}
	return issue
}
