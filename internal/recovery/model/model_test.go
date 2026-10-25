//go:build !functional

package model

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestRequestAndResourceValidation(t *testing.T) {
	t.Parallel()
	base := Request{Mode: Rebuild, Actor: "operator", Comment: "permanent loss"}
	if err := ValidateRequest(base); err != nil {
		t.Fatal("Inspect must be able to generate its ID", err)
	}
	for _, id := range []string{"", ".", "..", "../other", "a/b", "a\\b", "current.json", "manifest.json", "a\x00", "é", string(make([]byte, 129))} {
		if err := ValidateRecoveryID(id); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal("unsafe ID accepted", id, err)
		}
	}
	for _, change := range []func(*Request){
		func(r *Request) { r.Mode = "unknown" },
		func(r *Request) { r.Actor = " " },
		func(r *Request) { r.Comment = "\x00" },
		func(r *Request) { r.RequiredGroups = []string{"a", "a"} },
		func(r *Request) { r.RequiredGroups = []string{"a/b"} },
		func(r *Request) { r.Mode = ResetForJoin },
		func(r *Request) { r.Mode = ResetForJoin; r.ClusterID = "b"; r.RequiredGroups = []string{"a"} },
	} {
		r := base
		change(&r)
		if err := ValidateRequest(r); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal("invalid request accepted", r, err)
		}
	}
	r := base
	r.Mode, r.ClusterID = ResetForJoin, "b"
	if err := ValidateRequest(r); err != nil {
		t.Fatal(err)
	}
	limits, err := ResolveResources(Resources{})
	if err != nil || limits != (Resources{64 << 20, 1 << 30, 128 << 20}) {
		t.Fatal(limits, err)
	}
	if _, err := ResolveResources(Resources{MaxMetadataBytes: math.MaxUint64}); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("overflowing bound accepted", err)
	}
	at := time.Date(2026, 10, 25, 3, 4, 5, 0, time.FixedZone("caller", 3600))
	name, err := BackupName(at, "rcv-123")
	if err != nil || name != "20261025T020405Z-rcv-123" {
		t.Fatal("backup name did not use UTC acceptance time", name, err)
	}
}

func TestStrictCodecRejectsAmbiguousRecords(t *testing.T) {
	t.Parallel()
	for _, data := range []string{
		`{"schema":1,"schema":2}`,
		`{"schema":1,"\u0073chema":2}`,
		`{"schema":1,"unknown":true}`,
		`{"schema":1} {"schema":1}`,
		`{"schema":1} trailing`,
		`{"schema":-1}`,
		`{"schema":1.1}`,
		`{"schema":`,
	} {
		var record Current
		if err := DecodeStrict([]byte(data), &record, HeaderMaxBytes); !errors.Is(err, fault.ErrUnreadable) {
			t.Fatal("ambiguous record accepted", data, err)
		}
	}
	var record Current
	if err := DecodeStrict([]byte(`{"schema":1}`), &record, 1); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("byte limit ignored", err)
	}
	var position Position
	if err := DecodeStrict([]byte(`{"index":9007199254740993,"term":2}`), &position, HeaderMaxBytes); err != nil || position.Index != 9007199254740993 {
		t.Fatal("position lost integer precision", position, err)
	}
	var nested struct {
		Child map[string]int `json:"child"`
	}
	if err := DecodeStrict([]byte(`{"child":{"a":1,"a":2}}`), &nested, HeaderMaxBytes); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("nested duplicate accepted", err)
	}
	data, err := EncodeCanonical(map[string]any{"z": 2, "a": 1})
	if err != nil || string(data) != `{"a":1,"z":2}` {
		t.Fatal("unstable map encoding", string(data), err)
	}
}

func TestJournalAndReportsOwnTheirMutableValues(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 10, 25, 2, 0, 0, 0, time.UTC)
	manifest := LocalManifest{Schema: 1, Request: Request{RequiredGroups: []string{"a"}},
		Approved: topology.Document{Nodes: []topology.Node{{Name: "n1", Labels: map[string]string{"az": "a"}}}},
		Archive:  []ArchiveEntry{{Name: "a"}}, Cancellation: &ActionAudit{Actor: "operator"},
		CompletedAt: &at, Failure: &Issue{Code: "original"}}
	copy := CloneManifest(manifest)
	copy.Request.RequiredGroups[0] = "changed"
	copy.Approved.Nodes[0].Labels["az"] = "changed"
	copy.Archive[0].Moved = true
	copy.Cancellation.Actor = "changed"
	copy.Failure.Code = "changed"
	if manifest.Request.RequiredGroups[0] != "a" || manifest.Approved.Nodes[0].Labels["az"] != "a" || manifest.Archive[0].Moved || manifest.Cancellation.Actor != "operator" || manifest.Failure.Code != "original" {
		t.Fatal("journal clone shared caller state")
	}
	if copy.CompletedAt == manifest.CompletedAt || !copy.CompletedAt.Equal(*manifest.CompletedAt) {
		t.Fatal("completion timestamp was not an owned copy")
	}
	data, err := EncodeCanonical(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var restored LocalManifest
	if err := DecodeStrict(data, &restored, ManifestMaxBytes); err != nil || !reflect.DeepEqual(manifest, restored) {
		t.Fatal("private schema did not round trip", err)
	}
	status := Status{Groups: []GroupProgress{{Issue: &Issue{Code: "original"}}}}
	view := CloneStatus(status)
	view.Groups[0].Issue.Code = "changed"
	if status.Groups[0].Issue.Code != "original" {
		t.Fatal("report shared diagnostics")
	}
}
