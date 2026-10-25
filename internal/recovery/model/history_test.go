//go:build !functional

package model_test

import (
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/asalimonov/gquorum/internal/recovery/model"
)

func TestHistoryIsBoundedOwnedAndExcludedFromAcceptance(t *testing.T) {
	t.Parallel()
	original := manifest()
	digest, err := model.DigestAccepted(model.AcceptedFromManifest(original))
	if err != nil {
		t.Fatal(err)
	}
	current := model.CloneManifest(original)
	model.StartHistory(&current)
	for i := range model.HistoryMaxEvents + 20 {
		next := model.CloneManifest(current)
		next.Failure = &model.Issue{Code: "fault", Message: strings.Repeat("é", 400) + string(rune('a'+i%26))}
		model.AdvanceHistory(current, &next, current.AcceptedAt.Add(time.Duration(i+1)*time.Second))
		current = next
	}
	if len(current.History) != model.HistoryMaxEvents || current.History[0].Message != "Recovery approved" {
		t.Fatal("history did not retain approval and recent events", len(current.History))
	}
	if err := model.ValidateHistory(current); err != nil {
		t.Fatal(err)
	}
	for _, event := range current.History {
		if len(event.Message) > model.HistoryMessageBytes || !utf8.ValidString(event.Message) ||
			event.Issue != nil && len(event.Issue.Message) > model.HistoryMessageBytes {
			t.Fatal(event)
		}
	}
	after, err := model.DigestAccepted(model.AcceptedFromManifest(current))
	if err != nil || digest != after || model.SameAcceptance(original, current) != nil {
		t.Fatal("history changed approval", err)
	}
	encoded, err := model.EncodeManifest(current)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := model.DecodeManifest(encoded)
	if err != nil || !reflect.DeepEqual(current, restored) {
		t.Fatal("history did not round trip", err)
	}
	observation := model.StatusFromManifest(current)
	copy := model.CloneStatus(observation)
	copy.History[len(copy.History)-1].Issue.Message = "changed"
	if observation.History[len(observation.History)-1].Issue.Message == "changed" || current.History[len(current.History)-1].Issue.Message == "changed" {
		t.Fatal("history diagnostic shared mutable ownership")
	}
}

func TestHistorySemanticRetriesAndClockRegressionDoNotDuplicateOrReorder(t *testing.T) {
	t.Parallel()
	old := manifest()
	next := model.CloneManifest(old)
	next.Phase = model.Files
	model.AdvanceHistory(old, &next, old.AcceptedAt.Add(time.Second))
	count := len(next.History)
	repeated := model.CloneManifest(next)
	model.AdvanceHistory(next, &repeated, old.AcceptedAt.Add(-time.Hour))
	if len(repeated.History) != count {
		t.Fatal("idempotent retry duplicated history", repeated.History)
	}
	changed := model.CloneManifest(repeated)
	changed.Files.Backup.State = "running"
	changed.Files.Backup.Attempt = 1
	model.AdvanceHistory(repeated, &changed, old.AcceptedAt.Add(-time.Hour))
	if changed.History[len(changed.History)-1].At.Before(repeated.History[len(repeated.History)-1].At) {
		t.Fatal("clock regression reordered events")
	}
	legacy := manifest()
	encoded, err := model.EncodeManifest(legacy)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := model.DecodeManifest(encoded)
	if err != nil || len(decoded.History) != 0 {
		t.Fatal("legacy absence changed", err)
	}
}

func TestHistoryRejectsMalformedAndOversizedDiagnostics(t *testing.T) {
	t.Parallel()
	for _, shape := range []string{"count", "message", "control", "approval", "time", "phase", "event bytes"} {
		t.Run(shape, func(t *testing.T) {
			t.Parallel()
			value := manifest()
			model.StartHistory(&value)
			event := value.History[0]
			switch shape {
			case "count":
				for range model.HistoryMaxEvents {
					value.History = append(value.History, event)
				}
			case "message":
				value.History[0].Message = strings.Repeat("a", model.HistoryMessageBytes+1)
			case "control":
				value.History[0].Message = "unsafe\nline"
			case "approval":
				value.History[0].Phase = model.Complete
			case "time":
				value.History[0].At = value.AcceptedAt.Add(-time.Second)
			case "phase":
				event.Phase = "unknown"
				value.History = append(value.History, event)
			case "event bytes":
				event.GroupID = strings.Repeat("a", 8192)
				value.History = append(value.History, event)
			}
			if err := model.ValidateHistory(value); err == nil {
				t.Fatal("malformed history accepted", shape)
			}
		})
	}
}
